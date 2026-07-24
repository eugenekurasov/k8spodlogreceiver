package k8spodlogreceiver

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
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
	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/logline"
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
	dynamicClient        dynamic.Interface
	httpClient           *http.Client
	cancel               context.CancelFunc
	wg                   sync.WaitGroup
	mu                   sync.Mutex
	activeStreams        map[string]context.CancelFunc
	terminatedContainers map[string]struct{}
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
		cfg:                  cfg,
		settings:             settings,
		consumer:             c,
		activeStreams:        make(map[string]context.CancelFunc),
		terminatedContainers: make(map[string]struct{}),
		obsrep:               obsrep,
		telemetry:            telemetryBuilder,
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
		r.markContainerStates(pod)
		r.ensureStreams(ctx, pod)
	case apiWatch.Deleted:
		r.onPodDeleted(pod)
	}
}

func (r *logsReceiver) onPodAdded(ctx context.Context, pod *corev1.Pod) {
	if r.telemetry != nil {
		r.telemetry.PodDiscoveryEventsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("event_type", eventTypeAdded)))
	}

	r.markContainerStates(pod)
	r.ensureStreams(ctx, pod)
}

func (r *logsReceiver) ensureStreams(ctx context.Context, pod *corev1.Pod) {
	containers := make([]corev1.Container, 0, len(pod.Spec.InitContainers)+len(pod.Spec.Containers))
	containers = append(containers, pod.Spec.InitContainers...)
	containers = append(containers, pod.Spec.Containers...)

	for _, container := range containers {
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
	for _, container := range append(append([]corev1.Container(nil), pod.Spec.InitContainers...), pod.Spec.Containers...) {
		key := pod.Namespace + "/" + pod.Name + "/" + container.Name
		if cancel, ok := r.activeStreams[key]; ok {
			cancel()
			delete(r.activeStreams, key)
		}
		delete(r.terminatedContainers, key)
	}
	r.mu.Unlock()

	r.recordActiveStreams(ctx)
}

func (r *logsReceiver) markContainerStates(pod *corev1.Pod) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminatedContainers == nil {
		r.terminatedContainers = make(map[string]struct{})
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if containerIsTerminal(pod.Spec.RestartPolicy, cs, false, nil) {
			r.terminatedContainers[pod.Namespace+"/"+pod.Name+"/"+cs.Name] = struct{}{}
		}
	}
	for _, cs := range pod.Status.InitContainerStatuses {
		if containerIsTerminal(pod.Spec.RestartPolicy, cs, true, initContainerRestartPolicy(pod, cs.Name)) {
			r.terminatedContainers[pod.Namespace+"/"+pod.Name+"/"+cs.Name] = struct{}{}
		}
	}
}

func containerIsTerminal(podPolicy corev1.RestartPolicy, cs corev1.ContainerStatus, isInit bool, ownPolicy *corev1.ContainerRestartPolicy) bool {
	term := cs.State.Terminated
	if term == nil {
		return false
	}
	if ownPolicy != nil && *ownPolicy == corev1.ContainerRestartPolicyAlways {
		return false
	}
	if isInit {
		return term.ExitCode == 0 || podPolicy == corev1.RestartPolicyNever
	}
	switch podPolicy {
	case corev1.RestartPolicyNever:
		return true
	case corev1.RestartPolicyOnFailure:
		return term.ExitCode == 0
	default:
		return false
	}
}

func initContainerRestartPolicy(pod *corev1.Pod, name string) *corev1.ContainerRestartPolicy {
	for i := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[i].Name == name {
			return pod.Spec.InitContainers[i].RestartPolicy
		}
	}
	return nil
}

func (r *logsReceiver) isContainerTerminal(key string) bool {
	r.mu.Lock()
	_, terminal := r.terminatedContainers[key]
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

		lastTS, scanErr := r.streamConnection(ctx, stream, logline.Meta{
			Namespace:     namespace,
			PodName:       podName,
			PodUID:        podUID,
			ContainerName: containerName,
		})
		_ = stream.Close()
		if !lastTS.IsZero() {
			lastSeenTimestamp = lastTS
		}

		if scanErr != nil {
			logger.Debug("log stream ended, reconnecting", zap.Error(scanErr))
		}

		if !lastSeenTimestamp.IsZero() {
			t := metav1.NewTime(lastSeenTimestamp)
			sinceTime = &t
		}

		if r.isContainerTerminal(key) {
			if scanErr != nil {
				r.drainTerminalLogs(ctx, logline.Meta{
					Namespace:     namespace,
					PodName:       podName,
					PodUID:        podUID,
					ContainerName: containerName,
				}, &lastSeenTimestamp, logger)
			}
			logger.Debug("container terminated, stopping log stream")
			return
		}

		if !retry.SleepOrDone(ctx, backoff) {
			return
		}
	}
}

func (r *logsReceiver) drainTerminalLogs(ctx context.Context, m logline.Meta, lastSeenTimestamp *time.Time, logger *zap.Logger) {
	opts := &corev1.PodLogOptions{
		Container:  m.ContainerName,
		Follow:     false,
		Timestamps: true,
	}
	if !lastSeenTimestamp.IsZero() {
		t := metav1.NewTime(*lastSeenTimestamp)
		opts.SinceTime = &t
	}

	req := r.clientset.CoreV1().Pods(m.Namespace).GetLogs(m.PodName, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		logger.Debug("final drain of terminal pod logs failed", zap.Error(err))
		return
	}
	defer func() { _ = stream.Close() }()

	lastTS, _ := r.streamConnection(ctx, stream, m)
	if !lastTS.IsZero() {
		*lastSeenTimestamp = lastTS
	}
}

func (r *logsReceiver) streamConnection(ctx context.Context, stream io.Reader, m logline.Meta) (lastTS time.Time, scanErr error) {
	maxBatch := r.batchSize()
	flushInterval := r.flushInterval()

	behavior := r.logSizeBehavior()
	maxSize := r.maxLogSize()
	onOversize := func() {
		r.settings.Logger.Warn("log line exceeded max size",
			zap.Int("max_bytes", maxSize),
			zap.Stringer("behavior", behavior),
		)
	}
	scanner := logline.NewScanner(stream, maxSize, behavior, onOversize)

	lineCh := make(chan logline.Line, maxBatch)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		defer close(lineCh)
		for scanner.Scan() {
			lineCh <- scanner.Line()
		}
		scanErr = scanner.Err()
	}()

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	var batchMaxTS time.Time
	batch := logline.NewBatch(m)
	flush := func() {
		if batch.Count() == 0 {
			return
		}
		if r.consumeBatch(ctx, batch.Logs(), batch.Count()) && !batchMaxTS.IsZero() {
			lastTS = batchMaxTS
		}
		batch = logline.NewBatch(m)
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
			batch.Append(item.Body, item.Timestamp)
			if !item.Timestamp.IsZero() {
				batchMaxTS = item.Timestamp
			}
			if batch.Count() >= maxBatch {
				flush()
				ticker.Reset(flushInterval)
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (r *logsReceiver) consumeBatch(ctx context.Context, logs plog.Logs, count int) bool {
	consumeCtx := ctx
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

func (r *logsReceiver) emitLogLine(ctx context.Context, namespace, podName, podUID, containerName, line string, ts time.Time) {
	b := logline.NewBatch(logline.Meta{
		Namespace:     namespace,
		PodName:       podName,
		PodUID:        podUID,
		ContainerName: containerName,
	})
	b.Append(line, ts)
	r.consumeBatch(ctx, b.Logs(), b.Count())
}

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

func (r *logsReceiver) maxLogSize() int {
	if r.cfg != nil && r.cfg.MaxLogSize > 0 {
		return r.cfg.MaxLogSize
	}
	return defaultMaxLogSize
}

func (r *logsReceiver) logSizeBehavior() logline.Behavior {
	if r.cfg == nil {
		return logline.BehaviorSplit
	}

	b, err := logline.ParseBehavior(r.cfg.MaxLogSizeBehavior)
	if err != nil {
		return logline.BehaviorSplit
	}
	return b
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
