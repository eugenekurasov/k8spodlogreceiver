package k8spodlogreceiver

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	apiWatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/k8sconfig"
	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/metadata"
	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/retry"
	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/watch"
)

// podGVR is the GroupVersionResource the pod Observer watches.
var podGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}

const (
	eventTypeAdded   = "added"
	eventTypeDeleted = "deleted"

	reasonRBACDenied = "rbac_denied"
	reasonPodGone    = "pod_gone"
	reasonOther      = "other"

	maxLineSize = 1024 * 1024
)

type logsReceiver struct {
	cfg      *Config
	settings receiver.Settings
	consumer consumer.Logs
	// kubernetes.Interface instead of *kubernetes.Clientset so tests can
	// inject fake.NewSimpleClientset() without a real API server.
	clientset kubernetes.Interface
	// dynamicClient drives pod discovery through the watch.Observer. It shares
	// httpClient's transport (built from the same rest.Config) so Shutdown's
	// CloseIdleConnections() covers it too.
	dynamicClient  dynamic.Interface
	httpClient     *http.Client
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	mu             sync.Mutex
	activeStreams  map[string]context.CancelFunc
	terminatedPods map[string]struct{}
	//It is a field so tests can substitute a no-op without a real API server.
	startStream func(ctx context.Context, namespace, podName, podUID, containerName, key string)
	obsrep      *receiverhelper.ObsReport
	telemetry   *metadata.TelemetryBuilder
}

func newLogsReceiver(settings receiver.Settings, cfg *Config, c consumer.Logs) (receiver.Logs, error) {
	obsrep, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             settings.ID,
		Transport:              "http",
		ReceiverCreateSettings: settings,
	})
	if err != nil {
		return nil, fmt.Errorf("k8spodlogreceiver: building obsreport: %w", err)
	}

	telemetryBuilder, err := metadata.NewTelemetryBuilder(settings.TelemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("k8spodlogreceiver: building telemetry: %w", err)
	}

	r := &logsReceiver{
		cfg:            cfg,
		settings:       settings,
		consumer:       c,
		activeStreams:  make(map[string]context.CancelFunc),
		terminatedPods: make(map[string]struct{}),
		obsrep:         obsrep,
		telemetry:      telemetryBuilder,
	}
	r.startStream = r.streamContainerLogs
	return r, nil
}

func (r *logsReceiver) Start(ctx context.Context, _ component.Host) error {
	restCfg, err := k8sconfig.CreateRestConfig(r.cfg.APIConfig)
	if err != nil {
		return fmt.Errorf("k8spodlogreceiver: building kube client config: %w", err)
	}

	httpClient, err := rest.HTTPClientFor(restCfg)
	if err != nil {
		return fmt.Errorf("k8spodlogreceiver: building kube HTTP client: %w", err)
	}
	r.httpClient = httpClient

	clientset, err := kubernetes.NewForConfigAndClient(restCfg, httpClient)
	if err != nil {
		return fmt.Errorf("k8spodlogreceiver: %w (%v)", errNoRBACHint, err)
	}
	r.clientset = clientset

	dynamicClient, err := dynamic.NewForConfigAndClient(restCfg, httpClient)
	if err != nil {
		return fmt.Errorf("k8spodlogreceiver: building dynamic client: %w", err)
	}
	r.dynamicClient = dynamicClient

	ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	if err := r.startPodObserver(ctx); err != nil {
		return fmt.Errorf("k8spodlogreceiver: starting pod observer: %w", err)
	}

	return nil
}

func (r *logsReceiver) Shutdown(context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	if r.httpClient != nil {
		// Not r.httpClient.CloseIdleConnections(): rest.HTTPClientFor wraps the
		// *http.Transport in RoundTrippers (userAgent, auth) that don't implement
		// CloseIdleConnections, and http.Client only forwards the call to the
		// top-level transport — so the plain call is a no-op and the idle keep-
		// alive conns' HTTP/2 read-loop goroutines leak. utilnet.CloseIdleConnectionsFor
		// unwraps the RoundTripper chain to reach the real transport.
		utilnet.CloseIdleConnectionsFor(r.httpClient.Transport)
	}
	return nil
}

func (r *logsReceiver) startPodObserver(ctx context.Context) error {
	observer, err := watch.New(
		r.dynamicClient,
		watch.Config{
			Gvr:                 podGVR,
			Namespaces:          r.cfg.Namespaces,
			LabelSelector:       r.cfg.PodLabelSelector,
			IncludeInitialState: true,
			// Bookmarks carry only a resourceVersion, no pod payload — drop them.
			Exclude: map[apiWatch.EventType]bool{apiWatch.Bookmark: true},
		},
		r.settings.Logger,
		func(event *apiWatch.Event) { r.handlePodEvent(ctx, event) },
	)
	if err != nil {
		return err
	}

	observer.Start(ctx, &r.wg)
	return nil
}

func (r *logsReceiver) handlePodEvent(ctx context.Context, event *apiWatch.Event) {
	u, ok := event.Object.(*unstructured.Unstructured)
	if !ok {
		return
	}
	pod := &corev1.Pod{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, pod); err != nil {
		r.settings.Logger.Warn("failed to convert watch event object to Pod", zap.Error(err))
		return
	}

	switch event.Type {
	case apiWatch.Added:
		r.onPodAdded(ctx, pod)
	case apiWatch.Modified:
		r.markPodPhase(pod)
		r.ensureStreams(ctx, pod)
	case apiWatch.Deleted:
		r.onPodDeleted(pod)
	}
}

func (r *logsReceiver) onPodAdded(ctx context.Context, pod *corev1.Pod) {
	if r.telemetry != nil {
		r.telemetry.PodDiscoveryEventsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("event_type", eventTypeAdded)))
	}

	r.markPodPhase(pod)
	r.ensureStreams(ctx, pod)
}

func (r *logsReceiver) ensureStreams(ctx context.Context, pod *corev1.Pod) {
	for _, container := range pod.Spec.Containers {
		key := pod.Namespace + "/" + pod.Name + "/" + container.Name

		r.mu.Lock()
		if _, exists := r.activeStreams[key]; exists {
			r.mu.Unlock()
			continue
		}
		streamCtx, streamCancel := context.WithCancel(ctx)
		r.activeStreams[key] = streamCancel
		r.mu.Unlock()

		r.wg.Add(1)
		go r.startStream(streamCtx, pod.Namespace, pod.Name, string(pod.UID), container.Name, key)
	}

	r.recordActiveStreams(ctx)
}

func (r *logsReceiver) onPodDeleted(pod *corev1.Pod) {
	ctx := context.Background()
	if r.telemetry != nil {
		r.telemetry.PodDiscoveryEventsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("event_type", eventTypeDeleted)))
	}

	r.mu.Lock()
	for _, container := range pod.Spec.Containers {
		key := pod.Namespace + "/" + pod.Name + "/" + container.Name
		if cancel, ok := r.activeStreams[key]; ok {
			cancel()
			delete(r.activeStreams, key)
		}
	}
	delete(r.terminatedPods, pod.Namespace+"/"+pod.Name)
	r.mu.Unlock()

	r.recordActiveStreams(ctx)
}

func (r *logsReceiver) markPodPhase(pod *corev1.Pod) {
	if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
		return
	}
	r.mu.Lock()
	if r.terminatedPods == nil {
		r.terminatedPods = make(map[string]struct{})
	}
	r.terminatedPods[pod.Namespace+"/"+pod.Name] = struct{}{}
	r.mu.Unlock()
}

func (r *logsReceiver) isPodTerminal(namespace, podName string) bool {
	r.mu.Lock()
	_, terminal := r.terminatedPods[namespace+"/"+podName]
	r.mu.Unlock()
	return terminal
}

func (r *logsReceiver) recordActiveStreams(ctx context.Context) {
	if r.telemetry == nil {
		return
	}
	r.mu.Lock()
	count := int64(len(r.activeStreams))
	r.mu.Unlock()
	r.telemetry.ActiveLogStreams.Record(ctx, count)
}

// Refactoring needed
func (r *logsReceiver) streamContainerLogs(ctx context.Context, namespace, podName, podUID, containerName, key string) {
	defer r.wg.Done()
	defer func() {
		r.mu.Lock()
		delete(r.activeStreams, key)
		r.mu.Unlock()
	}()

	logger := r.settings.Logger.With(
		zap.String("namespace", namespace),
		zap.String("pod", podName),
		zap.String("container", containerName),
		zap.String("podUID", podUID),
	)

	backoff := r.cfg.ReconnectBackoff.InitialInterval
	sinceSeconds := r.cfg.SinceSeconds
	var sinceTime *metav1.Time
	var lastSeenTimestamp time.Time
	var reconnectStartTime time.Time
	firstAttempt := true

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !firstAttempt && r.telemetry != nil {
			r.telemetry.LogConnectionReconnectsTotal.Add(ctx, 1)
		}
		firstAttempt = false

		opts := &corev1.PodLogOptions{
			Container:  containerName,
			Follow:     true,
			Timestamps: true,
		}
		if sinceTime != nil {
			opts.SinceTime = sinceTime
		} else {
			opts.SinceSeconds = sinceSeconds
		}

		req := r.clientset.CoreV1().Pods(namespace).GetLogs(podName, opts)
		stream, err := req.Stream(ctx)
		if err != nil {
			if reconnectStartTime.IsZero() {
				reconnectStartTime = time.Now()
			}
			if r.telemetry != nil {
				r.telemetry.LogConnectionErrorsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", classifyStreamError(err))))
			}
			logger.Warn("log stream failed, will retry", zap.Error(err), zap.Duration("backoff", backoff))

			// MaxElapsedTime == 0 means retry indefinitely.
			if r.cfg.ReconnectBackoff.MaxElapsedTime > 0 && time.Since(reconnectStartTime) > r.cfg.ReconnectBackoff.MaxElapsedTime {
				logger.Info("max reconnect elapsed time exceeded, stopping stream", zap.Duration("max_elapsed_time", r.cfg.ReconnectBackoff.MaxElapsedTime))
				return
			}

			if !retry.SleepOrDone(ctx, backoff) {
				return
			}
			backoff = retry.NextBackoff(backoff, r.cfg.ReconnectBackoff.MaxInterval)
			continue
		}

		reconnectStartTime = time.Time{}
		backoff = r.cfg.ReconnectBackoff.InitialInterval

		lastTS, scanErr := r.streamConnection(ctx, stream, batchMeta{
			namespace:     namespace,
			podName:       podName,
			podUID:        podUID,
			containerName: containerName,
		})
		_ = stream.Close()
		if !lastTS.IsZero() {
			lastSeenTimestamp = lastTS
		}

		if scanErr != nil {
			if scanErr == bufio.ErrTooLong {
				logger.Warn("log line exceeded max size, discarded rest of line before reconnecting", zap.Int("max_bytes", maxLineSize))
			} else {
				logger.Debug("log stream ended, reconnecting", zap.Error(scanErr))
			}
		}

		if !lastSeenTimestamp.IsZero() {
			t := metav1.NewTime(lastSeenTimestamp)
			sinceTime = &t
		}

		if r.isPodTerminal(namespace, podName) {
			logger.Debug("pod is in a terminal phase, stopping log stream")
			return
		}

		if !retry.SleepOrDone(ctx, backoff) {
			return
		}
	}
}

// batchMeta identifies the container stream a batch of log records belongs to.
// Every record in a single stream shares the same resource attributes, so they
// are set once per batch rather than once per line.
type batchMeta struct {
	namespace     string
	podName       string
	podUID        string
	containerName string
}

// scannedLine is a single log line handed from the reader goroutine to the
// batching loop, carrying its already-parsed leading timestamp.
type scannedLine struct {
	line string
	ts   time.Time
}

// logBatch accumulates log records for one container stream into a single
// plog.Logs so many lines are forwarded with one ConsumeLogs call.
type logBatch struct {
	logs    plog.Logs
	records plog.LogRecordSlice
	count   int
}

func newLogBatch(m batchMeta) *logBatch {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()

	res := rl.Resource()
	res.Attributes().PutStr("k8s.namespace.name", m.namespace)
	res.Attributes().PutStr("k8s.pod.name", m.podName)
	res.Attributes().PutStr("k8s.pod.uid", m.podUID)
	res.Attributes().PutStr("k8s.container.name", m.containerName)

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName(metadata.ScopeName)

	return &logBatch{logs: logs, records: sl.LogRecords()}
}

func (b *logBatch) append(line string, ts time.Time) {
	lr := b.records.AppendEmpty()
	if ts.IsZero() {
		ts = time.Now()
	}
	lr.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	lr.Body().SetStr(line)
	b.count++
}

func (r *logsReceiver) streamConnection(ctx context.Context, stream io.Reader, m batchMeta) (lastTS time.Time, scanErr error) {
	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineSize)

	maxBatch := r.batchSize()
	flushInterval := r.flushInterval()

	lineCh := make(chan scannedLine, maxBatch)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		defer close(lineCh)
		for scanner.Scan() {
			line := scanner.Text()
			lineCh <- scannedLine{line: line, ts: parseLeadingTimestamp(line)}
		}
		if scanner.Err() == bufio.ErrTooLong {
			discardOversizedLine(stream)
		}
		scanErr = scanner.Err()
	}()

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	// batchMaxTS is the newest timestamp in the current, not-yet-flushed batch.
	// lastTS (the returned resume cursor) only advances to it once the batch is
	// accepted by the consumer — a failed ConsumeLogs must NOT move the cursor,
	// or the whole dropped batch would be skipped on reconnect (data loss).
	// Advancing only on success keeps the reconnect at-least-once: a failed
	// batch is fully re-read (duplicates) rather than lost.
	var batchMaxTS time.Time
	batch := newLogBatch(m)
	flush := func() {
		if batch.count == 0 {
			return
		}
		if r.consumeBatch(batch.logs, batch.count) && !batchMaxTS.IsZero() {
			lastTS = batchMaxTS
		}
		batch = newLogBatch(m)
		batchMaxTS = time.Time{}
	}

	for {
		select {
		case item, ok := <-lineCh:
			if !ok {
				flush()
				<-readerDone // scanErr is fully written before readerDone closes
				return lastTS, scanErr
			}
			batch.append(item.line, item.ts)
			if !item.ts.IsZero() {
				batchMaxTS = item.ts
			}
			if batch.count >= maxBatch {
				flush()
				ticker.Reset(flushInterval)
			}
		case <-ticker.C:
			flush()
		}
	}
}

// consumeBatch forwards one batch to the pipeline, accounting for all count
// records in a single obsreport span. It reports whether the consumer accepted
// the batch (err == nil), so the caller can decide whether to advance the
// reconnect cursor.
func (r *logsReceiver) consumeBatch(logs plog.Logs, count int) bool {
	consumeCtx := context.Background()
	if r.obsrep != nil {
		consumeCtx = r.obsrep.StartLogsOp(consumeCtx)
	}
	err := r.consumer.ConsumeLogs(consumeCtx, logs)
	if r.obsrep != nil {
		r.obsrep.EndLogsOp(consumeCtx, "k8s_podlog", count, err)
	}
	if err != nil {
		r.settings.Logger.Error("failed to forward log records to pipeline", zap.Error(err))
		return false
	}
	return true
}

// emitLogLine forwards a single line as a one-record batch. Retained as a thin
// wrapper over the batch primitives for callers/tests that emit one line.
func (r *logsReceiver) emitLogLine(namespace, podName, podUID, containerName, line string, ts time.Time) {
	b := newLogBatch(batchMeta{
		namespace:     namespace,
		podName:       podName,
		podUID:        podUID,
		containerName: containerName,
	})
	b.append(line, ts)
	r.consumeBatch(b.logs, b.count)
}

// batchSize / flushInterval return the effective batching parameters, falling
// back to package defaults so a receiver built without a fully-populated Config
// (as some unit tests do) still behaves sanely.
func (r *logsReceiver) batchSize() int {
	if r.cfg != nil && r.cfg.MaxBatchSize > 0 {
		return r.cfg.MaxBatchSize
	}
	return defaultMaxBatchSize
}

func (r *logsReceiver) flushInterval() time.Duration {
	if r.cfg != nil && r.cfg.FlushInterval > 0 {
		return r.cfg.FlushInterval
	}
	return defaultFlushInterval
}

// Candidate to be moved to a utility or other package.
func classifyStreamError(err error) string {
	switch {
	case apierrors.IsForbidden(err):
		return reasonRBACDenied
	case apierrors.IsNotFound(err):
		return reasonPodGone
	default:
		return reasonOther
	}
}

// Candidate to be moved to a utility or other package.
func discardOversizedLine(r io.Reader) {
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		if err != nil || n == 0 {
			return
		}
		for i := 0; i < n; i++ {
			if buf[i] == '\n' {
				return
			}
		}
	}
}

// Candidate to be moved to a utility or other package.
func parseLeadingTimestamp(line string) time.Time {
	idx := strings.IndexByte(line, ' ')
	if idx < 0 {
		return time.Time{}
	}
	ts, err := time.Parse(time.RFC3339Nano, line[:idx])
	if err != nil {
		return time.Time{}
	}
	return ts
}
