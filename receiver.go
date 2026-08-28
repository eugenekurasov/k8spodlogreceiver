package k8spodlogreceiver

import (
	"context"
	"errors"
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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	apiWatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/consumerretry"
	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/k8sconfig"
	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/logline"
	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/metadata"
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

	nextConsumer := consumerretry.NewLogs(cfg.RetryOnFailure, settings.Logger, c)

	r := &logsReceiver{
		cfg:                  cfg,
		settings:             settings,
		consumer:             nextConsumer,
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

	r.startPodObserver(ctx)
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

func (r *logsReceiver) startPodObserver(ctx context.Context) {
	observer := watch.New(
		r.dynamicClient,
		watch.Config{
			Gvr:           podGVR,
			Namespaces:    r.cfg.Namespaces,
			LabelSelector: r.cfg.PodLabelSelector,
			// Bookmarks carry only a resourceVersion, no pod payload — drop them.
			Exclude: map[apiWatch.EventType]bool{apiWatch.Bookmark: true},
		},
		r.settings.Logger,
		func(event *apiWatch.Event) { r.handlePodEvent(ctx, event) },
	)

	observer.Start(ctx, &r.wg)
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

// streamKey identifies one container's log stream in activeStreams and
// terminatedContainers.
func streamKey(namespace, podName, containerName string) string {
	return namespace + "/" + podName + "/" + containerName
}

// podContainers lists every container the receiver should stream: init
// containers first, then regular ones.
func podContainers(pod *corev1.Pod) []corev1.Container {
	containers := make([]corev1.Container, 0, len(pod.Spec.InitContainers)+len(pod.Spec.Containers))
	containers = append(containers, pod.Spec.InitContainers...)
	return append(containers, pod.Spec.Containers...)
}

// containerHasStarted reports whether a container has ever started — i.e.
// whether the kubelet can have logs for it. Streaming a container that is
// still waiting (ContainerCreating, image pull) would only churn through
// "is waiting to start" connect errors, so ensureStreams skips it; the
// Modified event emitted when it starts running picks it up.
func containerHasStarted(pod *corev1.Pod, name string) bool {
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
		for i := range statuses {
			cs := &statuses[i]
			if cs.Name != name {
				continue
			}
			// LastTerminationState covers a restarting container (e.g.
			// CrashLoopBackOff): currently Waiting, but a previous run left
			// logs worth reading.
			return cs.State.Running != nil || cs.State.Terminated != nil ||
				cs.LastTerminationState.Terminated != nil
		}
	}
	return false
}

func (r *logsReceiver) ensureStreams(ctx context.Context, pod *corev1.Pod) {
	for _, container := range podContainers(pod) {
		if !containerHasStarted(pod, container.Name) {
			continue
		}
		key := streamKey(pod.Namespace, pod.Name, container.Name)

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
	for _, container := range podContainers(pod) {
		key := streamKey(pod.Namespace, pod.Name, container.Name)
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
	for _, cs := range pod.Status.ContainerStatuses {
		if containerIsTerminal(pod.Spec.RestartPolicy, cs, false, nil) {
			r.terminatedContainers[streamKey(pod.Namespace, pod.Name, cs.Name)] = struct{}{}
		}
	}
	for _, cs := range pod.Status.InitContainerStatuses {
		if containerIsTerminal(pod.Spec.RestartPolicy, cs, true, initContainerRestartPolicy(pod, cs.Name)) {
			r.terminatedContainers[streamKey(pod.Namespace, pod.Name, cs.Name)] = struct{}{}
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

func (r *logsReceiver) streamContainerLogs(ctx context.Context, namespace, podName, podUID, containerName, key string) {
	defer r.wg.Done()
	defer func() {
		r.mu.Lock()
		delete(r.activeStreams, key)
		r.mu.Unlock()
	}()

	r.newContainerStream(namespace, podName, podUID, containerName, key).run(ctx)
}

// newContainerStream wires a containerStream with everything it needs from
// the receiver, so the stream itself holds no reference back to it.
func (r *logsReceiver) newContainerStream(namespace, podName, podUID, containerName, key string) *containerStream {
	return &containerStream{
		client:    r.clientset,
		telemetry: r.telemetry,
		meta: logline.Meta{
			Namespace:     namespace,
			PodName:       podName,
			PodUID:        podUID,
			ContainerName: containerName,
		},
		logger: r.settings.Logger.With(
			zap.String("namespace", namespace),
			zap.String("pod", podName),
			zap.String("container", containerName),
			zap.String("podUID", podUID),
		),
		sinceSeconds: r.cfg.SinceSeconds,
		backoffCfg:   r.cfg.ReconnectBackoff,
		consume:      r.streamConnection,
		isTerminal:   func() bool { return r.isContainerTerminal(key) },
		backoff:      r.cfg.ReconnectBackoff.InitialInterval,
		firstAttempt: true,
	}
}

var errPipelineRefused = errors.New("pipeline refused a batch; reconnecting to re-read it")

func (r *logsReceiver) streamConnection(ctx context.Context, stream io.Reader, m logline.Meta) (lastTS time.Time, _ error) {
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
	// done unblocks the reader goroutine if we return while it is parked on a
	// send into a full lineCh (e.g. the pipeline refused a batch); without it
	// the goroutine would leak. It does not interrupt a blocked Read — the
	// caller's stream.Close() takes care of that.
	done := make(chan struct{})
	defer close(done)
	var readErr error
	go func() {
		defer close(lineCh)
		for scanner.Scan() {
			select {
			case lineCh <- scanner.Line():
			case <-done:
				return
			}
		}
		// Written before close(lineCh) fires, so the receive of the closed
		// channel below is safe to order the read of readErr after it.
		readErr = scanner.Err()
	}()

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	var batchMaxTS time.Time
	batch := logline.NewBatch(m)

	flush := func() bool {
		if batch.Count() == 0 {
			return true
		}
		delivered := r.consumeBatch(ctx, batch.Logs(), batch.Count())
		if delivered && !batchMaxTS.IsZero() {
			lastTS = batchMaxTS
		}
		batch = logline.NewBatch(m)
		batchMaxTS = time.Time{}
		return delivered
	}

	for {
		select {
		case item, ok := <-lineCh:
			if !ok {
				flush() // stream is over; nothing left to re-read from
				return lastTS, readErr
			}
			batch.Append(item.Body, item.Timestamp)
			if !item.Timestamp.IsZero() {
				batchMaxTS = item.Timestamp
			}
			if batch.Count() >= maxBatch {
				if !flush() {
					return lastTS, errPipelineRefused
				}
				ticker.Reset(flushInterval)
			}
		case <-ticker.C:
			if !flush() {
				return lastTS, errPipelineRefused
			}
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
