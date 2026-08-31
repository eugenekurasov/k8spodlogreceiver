// Copyright 2026 Yevhenii Kurasov
// SPDX-License-Identifier: Apache-2.0

package k8spodlogreceiver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/eugenekurasov/k8spodlogreceiver/internal/consumerretry"
	"github.com/eugenekurasov/k8spodlogreceiver/internal/k8sconfig"
	"github.com/eugenekurasov/k8spodlogreceiver/internal/logline"
	"github.com/eugenekurasov/k8spodlogreceiver/internal/metadata"
	"github.com/eugenekurasov/k8spodlogreceiver/internal/poddiscovery"
)

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
	clientset     kubernetes.Interface
	httpClient    *http.Client
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	mu            sync.Mutex
	activeStreams map[string]streamHandle
	// nextStreamGen hands each started stream a unique generation, so a
	// stream that is winding down can only release its own bookkeeping.
	nextStreamGen        uint64
	terminatedContainers map[string]struct{}
	// drainedContainers marks containers whose stream has already run to
	// completion while the container was terminal — it has been drained, so
	// nothing more will ever be written to it. It is what stops the next
	// resync from reopening a stream for a dead container, over and over
	// until the pod is deleted. Being terminal alone is not enough: a
	// container that had already exited when the receiver first saw it (a
	// completed init container, a short Job) still has logs nobody has read.
	drainedContainers map[string]struct{}
	// cursors is where each container's progress lives, along with its
	// persistence and expiry; see cursorStore. It has its own lock and is
	// safe to use without holding r.mu — r.mu is still taken around it where
	// a write has to be fenced against the receiver's own state.
	cursors *cursorStore
	// restartCounts caches the restart count for each container, keyed by
	// streamKey. Updated on each pod discovery event.
	restartCounts map[string]int32
	//It is a field so tests can substitute a no-op without a real API server.
	startStream func(ctx context.Context, namespace, podName, podUID, containerName, nodeName, key string, gen uint64)
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
		activeStreams:        make(map[string]streamHandle),
		terminatedContainers: make(map[string]struct{}),
		drainedContainers:    make(map[string]struct{}),
		// Memory-only until Start resolves the configured storage extension.
		cursors:       newCursorStore(nil, settings.Logger),
		restartCounts: make(map[string]int32),
		obsrep:        obsrep,
		telemetry:     telemetryBuilder,
	}
	r.startStream = r.streamContainerLogs
	return r, nil
}

func (r *logsReceiver) Start(ctx context.Context, host component.Host) error {
	client, err := openCursorStorage(ctx, host, r.cfg.StorageID, r.settings.ID)
	if err != nil {
		return fmt.Errorf("k8spodlogreceiver: %w", err)
	}
	// Replaced rather than mutated: nothing can have touched the memory-only
	// store yet, since no stream exists before this point.
	r.cursors = newCursorStore(client, r.settings.Logger)
	r.cursors.load(ctx)

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

	runCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	if err := r.startPodDiscovery(runCtx); err != nil {
		cancel()
		return fmt.Errorf("k8spodlogreceiver: starting pod discovery: %w", err)
	}

	r.wg.Add(1)
	go r.runCursorPruning(runCtx)

	if r.cursors.persists() {
		r.wg.Add(1)
		go r.runCursorFlush(runCtx)
	}

	r.settings.Logger.Info("started collecting pod logs",
		zap.String("since", sinceForLog(r.cfg.SinceSeconds)),
		zap.Int("max_batch_size", r.batchSize()),
		zap.Duration("flush_interval", r.flushInterval()),
		zap.Int("max_log_size", r.maxLogSize()),
		zap.Stringer("max_log_size_behavior", r.logSizeBehavior()))
	return nil
}

// sinceForLog renders the three states of SinceSeconds, which a plain int64
// field cannot express: unset reads as "full history", not as zero.
func sinceForLog(sinceSeconds *int64) string {
	if sinceSeconds == nil {
		return "full available history"
	}
	return (time.Duration(*sinceSeconds) * time.Second).String()
}

func (r *logsReceiver) Shutdown(ctx context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}

	r.mu.Lock()
	draining := len(r.activeStreams)
	r.mu.Unlock()
	r.settings.Logger.Info("stopping pod log collection", zap.Int("draining_streams", draining))

	r.wg.Wait()

	// Written after every stream has stopped, so the persisted set includes
	// each container's final delivered line. ctx here is the shutdown context,
	// not the cancelled run context, so the write is still allowed to happen.
	r.cursors.flush(ctx)
	if err := r.cursors.close(ctx); err != nil {
		r.settings.Logger.Warn("closing cursor storage", zap.Error(err))
	}

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

func (r *logsReceiver) startPodDiscovery(ctx context.Context) error {
	discovery := poddiscovery.New(
		r.clientset,
		poddiscovery.Config{
			Namespaces:    r.cfg.Namespaces,
			LabelSelector: r.cfg.PodLabelSelector,
			ResyncPeriod:  r.podResyncPeriod(),
		},
		r.settings.Logger,
		poddiscovery.Handler{
			OnAdd: r.onPodAdded,
			// A resync delivers every cached pod here, which is what restarts
			// a stream that gave up (e.g. ReconnectBackoff.MaxElapsedTime):
			// both calls below are idempotent.
			OnUpdate: func(updateCtx context.Context, pod *corev1.Pod) {
				r.recordPodUIDSeen(string(pod.UID))
				r.markContainerStates(pod)
				r.ensureStreams(updateCtx, pod)
			},
			OnDelete: r.onPodDeleted,
		},
	)
	return discovery.Start(ctx, &r.wg)
}

func (r *logsReceiver) onPodAdded(ctx context.Context, pod *corev1.Pod) {
	if r.telemetry != nil {
		r.telemetry.PodDiscoveryEventsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("event_type", eventTypeAdded)))
	}

	r.recordPodUIDSeen(string(pod.UID))
	r.markContainerStates(pod)
	r.ensureStreams(ctx, pod)
}

// streamHandle is the bookkeeping for one running stream: how to stop it, and
// which generation of that stream it is. The generation exists because a key
// can legitimately be reused by a *later* stream for the same pod and
// container — an informer that loses its watch reports a
// DeletedFinalStateUnknown tombstone, and the following relist re-adds the very
// same pod, UID included. The delete tears the old stream down and the re-add
// starts a new one, while the old goroutine is still unwinding. Without a
// generation its deferred cleanup would evict the replacement's entry, leaving
// a stream that nothing tracks: no cancel on pod deletion, and a duplicate
// started by the next sync that finds the key missing.
type streamHandle struct {
	cancel context.CancelFunc
	gen    uint64
}

// releaseStream drops the bookkeeping for a finished stream, but only if the
// entry still belongs to it. See streamHandle for why that check is needed.
//
// The gauge is re-recorded here and not only on pod events, because a stream
// can end on its own: a terminal container, an exhausted MaxElapsedTime, a
// cancelled parent. Without this, active_log_streams would stay overstated
// until the next add/delete for some pod happened to correct it.
func (r *logsReceiver) releaseStream(ctx context.Context, key string, gen uint64) {
	r.mu.Lock()
	released := false
	if h, ok := r.activeStreams[key]; ok && h.gen == gen {
		delete(r.activeStreams, key)
		released = true
		// The stream reached the end of a container that will never write
		// again, so there is nothing left for a resync to reopen. Recorded
		// only here, where the stream is known to have finished: a stream
		// that stopped for any other reason (an exhausted MaxElapsedTime, a
		// cancelled parent) must still be revived by the next resync.
		if _, terminal := r.terminatedContainers[key]; terminal {
			r.drainedContainers[key] = struct{}{}
		}
	}
	r.mu.Unlock()

	if released {
		// ctx is this stream's own context and is usually already cancelled by
		// the time the cleanup runs; the gauge must be recorded regardless.
		r.recordActiveStreams(context.WithoutCancel(ctx))
	}
}

// streamKey identifies one container's log stream in activeStreams and
// terminatedContainers.
//
// The pod UID is part of the key, not just the name. A pod recreated under the
// same name (a StatefulSet replacement, a rerun Job, a re-applied bare pod) is
// a different pod, and sharing a key with its predecessor causes two distinct
// faults: the outgoing stream's deferred cleanup would delete the incoming
// pod's entry, leaving an untracked goroutine that a later sync would then
// duplicate; and a terminal marker left by the old container would suppress
// streaming for the new one entirely.
func streamKey(namespace, podName, podUID, containerName string) string {
	return namespace + "/" + podName + "/" + podUID + "/" + containerName
}

// podUIDFromStreamKey recovers the pod UID from a key built by streamKey, and
// returns "" for anything else. It is how a cursor restored from storage — the
// only place keys outlive the pod they came from — is matched back to its pod.
//
// None of the four segments can contain a slash: namespaces, pod names and
// container names are DNS labels, and a UID is hexadecimal.
func podUIDFromStreamKey(key string) string {
	parts := strings.Split(key, "/")
	if len(parts) != 4 {
		return ""
	}
	return parts[2]
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
		key := streamKey(pod.Namespace, pod.Name, string(pod.UID), container.Name)

		r.mu.Lock()
		if _, exists := r.activeStreams[key]; exists {
			r.mu.Unlock()
			continue
		}
		if _, drained := r.drainedContainers[key]; drained {
			r.mu.Unlock()
			continue
		}
		streamCtx, streamCancel := context.WithCancel(ctx)
		r.nextStreamGen++
		gen := r.nextStreamGen
		r.activeStreams[key] = streamHandle{cancel: streamCancel, gen: gen}
		r.mu.Unlock()

		r.wg.Add(1)
		go r.startStream(streamCtx, pod.Namespace, pod.Name, string(pod.UID), container.Name, pod.Spec.NodeName, key, gen)
	}

	r.recordActiveStreams(ctx)
}

// onPodDeleted stops the pod's streams. inferred says whether the delete was
// reported by the API server or only deduced by the informer after a broken
// watch; see poddiscovery.Handler.OnDelete. A real delete means the pod is
// gone and its cursors are dead weight. An inferred one means the very same
// pod is probably about to be relisted and re-added, so the cursors are kept
// and the replacement streams resume instead of re-reading the backfill.
func (r *logsReceiver) onPodDeleted(pod *corev1.Pod, inferred bool) {
	ctx := context.Background()
	if r.telemetry != nil {
		r.telemetry.PodDiscoveryEventsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("event_type", eventTypeDeleted)))
	}

	r.mu.Lock()
	for _, container := range podContainers(pod) {
		key := streamKey(pod.Namespace, pod.Name, string(pod.UID), container.Name)
		if h, ok := r.activeStreams[key]; ok {
			h.cancel()
			delete(r.activeStreams, key)
		}
		delete(r.terminatedContainers, key)
		delete(r.drainedContainers, key)
		if !inferred {
			r.cursors.forget(key)
		}
	}
	r.mu.Unlock()

	r.recordActiveStreams(ctx)
}

func (r *logsReceiver) markContainerStates(pod *corev1.Pod) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, cs := range pod.Status.ContainerStatuses {
		key := streamKey(pod.Namespace, pod.Name, string(pod.UID), cs.Name)
		r.restartCounts[key] = cs.RestartCount
		if containerIsTerminal(pod.Spec.RestartPolicy, cs, false, nil) {
			r.terminatedContainers[key] = struct{}{}
		}
	}
	for _, cs := range pod.Status.InitContainerStatuses {
		key := streamKey(pod.Namespace, pod.Name, string(pod.UID), cs.Name)
		r.restartCounts[key] = cs.RestartCount
		if containerIsTerminal(pod.Spec.RestartPolicy, cs, true, initContainerRestartPolicy(pod, cs.Name)) {
			r.terminatedContainers[key] = struct{}{}
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

// cursorFor returns the last delivered timestamp for a container, zero if none.
func (r *logsReceiver) cursorFor(key string) time.Time {
	return r.cursors.get(key)
}

// advanceCursor records the last line delivered for a container, on behalf of
// the stream generation gen.
//
// The write is fenced on that generation for the same reason releaseStream is:
// a cancelled stream keeps running until it notices, and flushes its final
// batch on the way out. Without the fence that late write would resurrect the
// cursor of a pod that was really deleted — growing the persisted set forever
// with entries for pods that no longer exist — or overwrite the cursor now
// owned by a replacement stream.
//
// Zero timestamps and out-of-order writes are the store's business; see
// cursorStore.advance.
func (r *logsReceiver) advanceCursor(key string, gen uint64, ts time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	h, ok := r.activeStreams[key]
	if !ok || h.gen != gen {
		// This stream no longer owns the key: it was deleted or replaced.
		return
	}
	// Called under r.mu so the fence and the write cannot be interleaved with
	// a delete. The store never calls back here, so nothing can deadlock on it.
	r.cursors.advance(key, ts)
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

func (r *logsReceiver) getRestartCount(key string) int32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.restartCounts[key]
}

// recordPodUIDSeen marks a pod UID as recently seen, deferring the expiry of
// its containers' cursors.
func (r *logsReceiver) recordPodUIDSeen(podUID string) {
	r.cursors.markPodSeen(podUID)
}

// runCursorPruning expires the cursors of pods that are long gone, until ctx
// is cancelled.
func (r *logsReceiver) runCursorPruning(ctx context.Context) {
	defer r.wg.Done()

	ticker := time.NewTicker(cursorPruneInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.pruneStaleCursors()
		}
	}
}

// pruneStaleCursors expires the cursors of pods the informer has not reported
// for cursorStaleAfter, and drops the receiver's own per-container state for
// the same keys so the two cannot drift apart.
func (r *logsReceiver) pruneStaleCursors() {
	pruned := r.cursors.prune(time.Now().Add(-cursorStaleAfter))
	if len(pruned) == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, key := range pruned {
		delete(r.restartCounts, key)
		delete(r.terminatedContainers, key)
		delete(r.drainedContainers, key)
	}
}

// runCursorFlush persists cursors periodically until ctx is cancelled.
func (r *logsReceiver) runCursorFlush(ctx context.Context) {
	defer r.wg.Done()
	r.cursors.runFlushLoop(ctx)
}

func (r *logsReceiver) streamContainerLogs(ctx context.Context, namespace, podName, podUID, containerName, nodeName, key string, gen uint64) {
	defer r.wg.Done()
	defer r.releaseStream(ctx, key, gen)

	r.newContainerStream(namespace, podName, podUID, containerName, nodeName, key, gen).run(ctx)
}

// newContainerStream wires a containerStream with everything it needs from
// the receiver, so the stream itself holds no reference back to it.
func (r *logsReceiver) newContainerStream(namespace, podName, podUID, containerName, nodeName, key string, gen uint64) *containerStream {
	return &containerStream{
		client:    r.clientset,
		telemetry: r.telemetry,
		meta: logline.Meta{
			Namespace:     namespace,
			PodName:       podName,
			PodUID:        podUID,
			ContainerName: containerName,
			NodeName:      nodeName,
			RestartCount:  r.getRestartCount(key),
		},
		logger: r.settings.Logger.With(
			zap.String("namespace", namespace),
			zap.String("pod", podName),
			zap.String("container", containerName),
			zap.String("podUID", podUID),
		),
		resumeFrom:        r.cursorFor(key),
		onDelivered:       func(ts time.Time) { r.advanceCursor(key, gen, ts) },
		sinceSeconds:      r.cfg.SinceSeconds,
		backoffCfg:        r.cfg.ReconnectBackoff,
		maxStreamLifetime: r.cfg.MaxStreamLifetime,
		consume:           r.streamConnection,
		isTerminal:        func() bool { return r.isContainerTerminal(key) },
		backoff:           r.cfg.ReconnectBackoff.InitialInterval,
		firstAttempt:      true,
	}
}

var errPipelineRefused = errors.New("pipeline refused a batch; reconnecting to re-read it")

func (r *logsReceiver) streamConnection(ctx context.Context, stream io.Reader, m logline.Meta, onProgress func(time.Time)) (lastTS time.Time, _ error) {
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
		outcome := r.consumeBatch(ctx, batch.Logs(), batch.Count())
		// A dropped batch advances the cursor exactly like a delivered one:
		// those records are gone either way, and leaving the cursor behind
		// them would make the next connection re-read them forever.
		if outcome != batchRefused && !batchMaxTS.IsZero() {
			lastTS = batchMaxTS
			// Reported per delivered batch, not once the connection ends: a
			// healthy stream stays open for max_stream_lifetime, and a cursor
			// that only advanced on disconnect would persist an hour-old
			// position and re-read all of it after an abrupt restart.
			if onProgress != nil {
				onProgress(lastTS)
			}
		}
		batch = logline.NewBatch(m)
		batchMaxTS = time.Time{}
		return outcome != batchRefused
	}

	for {
		select {
		case item, ok := <-lineCh:
			if !ok {
				// EOF, so readErr is usually nil — but a refused final batch
				// must still be reported as one. Reporting it as a clean end
				// would drop those lines for good: the caller only reconnects
				// (live container) or drains the rest of a terminal
				// container's log (follow) when the stream ended in error.
				if !flush() {
					return lastTS, errPipelineRefused
				}
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

// batchOutcome is what the pipeline did with one batch, which decides whether
// the stream may move past it.
type batchOutcome int

const (
	// batchDelivered: accepted, the cursor may advance.
	batchDelivered batchOutcome = iota
	// batchRefused: rejected for now — a recoverable error, or retries that
	// ran out of time. Re-reading it after a reconnect is the only way it can
	// still be delivered, so the cursor must not move.
	batchRefused
	// batchDropped: rejected for good (consumererror.IsPermanent). Re-reading
	// it would produce the identical error, so the cursor moves past it.
	batchDropped
)

func (r *logsReceiver) consumeBatch(ctx context.Context, logs plog.Logs, count int) batchOutcome {
	consumeCtx := ctx
	if r.obsrep != nil {
		consumeCtx = r.obsrep.StartLogsOp(consumeCtx)
	}
	err := r.consumer.ConsumeLogs(consumeCtx, logs)
	if r.obsrep != nil {
		r.obsrep.EndLogsOp(consumeCtx, "k8s_pod_log", count, err)
	}
	if err == nil {
		return batchDelivered
	}

	// A permanent error is the pipeline saying the data itself is
	// unacceptable, not that it is busy — a processor rejecting a malformed
	// record, an exporter refusing a payload it can never encode. Reconnecting
	// re-reads the very same records and earns the very same error, forever:
	// the connect succeeds, so the reconnect backoff is reset every round and
	// the receiver spins at roughly one request per second per container
	// without ever making progress. Dropping the batch and moving past it is
	// the only outcome that terminates. It is already counted as refused
	// records by obsrep.
	if consumererror.IsPermanent(err) {
		r.settings.Logger.Error("pipeline permanently refused log records; dropping them and moving on",
			zap.Error(err),
			zap.Int("dropped_records", count),
		)
		return batchDropped
	}

	r.settings.Logger.Error("failed to forward log records to pipeline", zap.Error(err))
	return batchRefused
}

func (r *logsReceiver) podResyncPeriod() time.Duration {
	if r.cfg == nil || r.cfg.PodResyncPeriod == nil {
		return defaultPodResyncPeriod
	}
	return *r.cfg.PodResyncPeriod
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
