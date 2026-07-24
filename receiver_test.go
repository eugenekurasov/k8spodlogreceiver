package k8spodlogreceiver

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/collector/receiver/receivertest"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/logline"
	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/metadata"
)

func newTestReceiver() *logsReceiver {
	r := &logsReceiver{
		cfg:           createDefaultConfig().(*Config),
		activeStreams: make(map[string]context.CancelFunc),
	}
	r.startStream = func(_ context.Context, _, _, _, _, _ string) {
		defer r.wg.Done()
	}
	return r
}

func makePod(ns, name string, containers ...string) *corev1.Pod {
	specs := make([]corev1.Container, len(containers))
	for i, c := range containers {
		specs[i] = corev1.Container{Name: c}
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       corev1.PodSpec{Containers: specs},
	}
}

// podEvent wraps a typed pod as an Observer watch event, mirroring how the
// Observer delivers objects (as *unstructured.Unstructured).
func podEvent(t watch.EventType, pod *corev1.Pod) *watch.Event {
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
	if err != nil {
		panic(err)
	}
	return &watch.Event{Type: t, Object: &unstructured.Unstructured{Object: obj}}
}

func TestOnPodAdded_RegistersStreamsPerContainer(t *testing.T) {
	r := newTestReceiver()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.onPodAdded(ctx, makePod("payments", "app-abc", "api", "sidecar"))

	r.mu.Lock()
	_, hasAPI := r.activeStreams["payments/app-abc/api"]
	_, hasSidecar := r.activeStreams["payments/app-abc/sidecar"]
	r.mu.Unlock()

	assert.True(t, hasAPI, "expected stream for 'api' container")
	assert.True(t, hasSidecar, "expected stream for 'sidecar' container")

	cancel()
	r.wg.Wait()
}

// TestModifiedEventStartsStreamsForNeverAddedPod covers the watch-restart (410)
// gap: a pod created while the watch was disconnected is never re-emitted as
// Added, so streams must still start when its first Modified event arrives.
func TestModifiedEventStartsStreamsForNeverAddedPod(t *testing.T) {
	r := newTestReceiver()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// No prior onPodAdded — simulate a pod first seen via Modified.
	r.handlePodEvent(ctx, podEvent(watch.Modified, makePod("payments", "app-abc", "api")))

	r.mu.Lock()
	_, hasAPI := r.activeStreams["payments/app-abc/api"]
	r.mu.Unlock()
	assert.True(t, hasAPI, "Modified event must start streams for a pod that was never Added")

	cancel()
	r.wg.Wait()
}

func TestOnPodAdded_Deduplicates(t *testing.T) {
	r := newTestReceiver()

	var calls int
	var mu sync.Mutex
	r.startStream = func(ctx context.Context, _, _, _, _, _ string) {
		mu.Lock()
		calls++
		mu.Unlock()
		defer r.wg.Done()
		<-ctx.Done()
	}

	ctx, cancel := context.WithCancel(context.Background())
	pod := makePod("default", "worker", "app")
	r.onPodAdded(ctx, pod)
	r.onPodAdded(ctx, pod)

	cancel()
	r.wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, calls, "duplicate onPodAdded must not start a second stream")
}

func TestOnPodDeleted_CancelsAndRemovesStream(t *testing.T) {
	cancelled := false
	r := &logsReceiver{
		activeStreams: map[string]context.CancelFunc{
			"default/worker/app": func() { cancelled = true },
		},
	}

	r.onPodDeleted(makePod("default", "worker", "app"))

	assert.True(t, cancelled, "cancel func must be called on pod delete")

	r.mu.Lock()
	_, stillPresent := r.activeStreams["default/worker/app"]
	r.mu.Unlock()
	assert.False(t, stillPresent, "entry must be removed from activeStreams")
}

func TestOnPodDeleted_UnknownPod_NoPanic(t *testing.T) {
	r := &logsReceiver{
		activeStreams: make(map[string]context.CancelFunc),
	}
	require.NotPanics(t, func() {
		r.onPodDeleted(makePod("default", "ghost", "app"))
	})
}

func TestOnPodDeleted_MultiContainer(t *testing.T) {
	cancelledA, cancelledB := false, false
	r := &logsReceiver{
		activeStreams: map[string]context.CancelFunc{
			"ns/pod/a": func() { cancelledA = true },
			"ns/pod/b": func() { cancelledB = true },
		},
	}

	r.onPodDeleted(makePod("ns", "pod", "a", "b"))

	assert.True(t, cancelledA, "container-a stream must be cancelled")
	assert.True(t, cancelledB, "container-b stream must be cancelled")
}

func terminatedStatus(name string, exitCode int32) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:  name,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: exitCode}},
	}
}

func runningStatus(name string) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:  name,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	}
}

func TestMarkContainerStates_MultiContainerJob(t *testing.T) {
	r := newTestReceiver()
	pod := makePod("batch", "job-abc", "worker", "helper")
	pod.Spec.RestartPolicy = corev1.RestartPolicyNever
	pod.Status.Phase = corev1.PodRunning // still Running: helper hasn't exited
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		terminatedStatus("worker", 0),
		runningStatus("helper"),
	}

	r.markContainerStates(pod)

	assert.True(t, r.isContainerTerminal("batch/job-abc/worker"), "finished container must be terminal even though pod phase is Running")
	assert.False(t, r.isContainerTerminal("batch/job-abc/helper"), "still-running container must not be terminal")
}

func TestMarkContainerStates_RestartPolicyAlways_NeverTerminal(t *testing.T) {
	r := newTestReceiver()
	// A CrashLooping container: momentarily Terminated but will be restarted.
	pod := makePod("default", "app-abc", "app")
	pod.Spec.RestartPolicy = corev1.RestartPolicyAlways
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{terminatedStatus("app", 1)}

	r.markContainerStates(pod)

	assert.False(t, r.isContainerTerminal("default/app-abc/app"), "Always container must keep following restarts")
}

func TestMarkContainerStates_OnFailure(t *testing.T) {
	r := newTestReceiver()
	pod := makePod("default", "job-of", "task")
	pod.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{terminatedStatus("task", 0)}
	r.markContainerStates(pod)
	assert.True(t, r.isContainerTerminal("default/job-of/task"), "OnFailure + exit 0 is terminal")

	r2 := newTestReceiver()
	podFail := makePod("default", "job-of", "task")
	podFail.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
	podFail.Status.ContainerStatuses = []corev1.ContainerStatus{terminatedStatus("task", 2)}
	r2.markContainerStates(podFail)
	assert.False(t, r2.isContainerTerminal("default/job-of/task"), "OnFailure + non-zero exit will restart, not terminal")
}

func TestMarkContainerStates_NativeSidecar(t *testing.T) {
	r := newTestReceiver()
	always := corev1.ContainerRestartPolicyAlways
	pod := makePod("mesh", "app-abc", "app")
	pod.Spec.RestartPolicy = corev1.RestartPolicyNever
	pod.Spec.InitContainers = []corev1.Container{{Name: "proxy", RestartPolicy: &always}}
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{terminatedStatus("proxy", 0)}

	r.markContainerStates(pod)

	assert.False(t, r.isContainerTerminal("mesh/app-abc/proxy"), "native sidecar must not be terminal on a Terminated status")
}

func TestMarkContainerStates_RegularInitContainer(t *testing.T) {
	r := newTestReceiver()
	pod := makePod("default", "app-abc", "app")
	pod.Spec.RestartPolicy = corev1.RestartPolicyAlways
	pod.Spec.InitContainers = []corev1.Container{{Name: "migrate"}} // no per-container policy
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{terminatedStatus("migrate", 0)}

	r.markContainerStates(pod)

	assert.True(t, r.isContainerTerminal("default/app-abc/migrate"), "completed regular init container is terminal")
}

func TestIsContainerTerminal_Unknown_ReturnsFalse(t *testing.T) {
	r := newTestReceiver()
	assert.False(t, r.isContainerTerminal("default/never-seen/app"))
}

func TestEnsureStreams_CoversInitContainers(t *testing.T) {
	r := newTestReceiver()
	always := corev1.ContainerRestartPolicyAlways
	pod := makePod("mesh", "app-abc", "app")
	pod.Spec.InitContainers = []corev1.Container{
		{Name: "migrate"},
		{Name: "proxy", RestartPolicy: &always},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.onPodAdded(ctx, pod)

	r.mu.Lock()
	_, hasApp := r.activeStreams["mesh/app-abc/app"]
	_, hasMigrate := r.activeStreams["mesh/app-abc/migrate"]
	_, hasProxy := r.activeStreams["mesh/app-abc/proxy"]
	r.mu.Unlock()

	assert.True(t, hasApp, "main container stream expected")
	assert.True(t, hasMigrate, "regular init container stream expected")
	assert.True(t, hasProxy, "native sidecar stream expected")

	cancel()
	r.wg.Wait()
}

func TestOnPodDeleted_ClearsTerminatedContainerEntries(t *testing.T) {
	r := newTestReceiver()
	pod := makePod("default", "job-abc", "app")
	pod.Spec.RestartPolicy = corev1.RestartPolicyNever
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{terminatedStatus("app", 0)}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.onPodAdded(ctx, pod)
	require.True(t, r.isContainerTerminal("default/job-abc/app"))

	r.onPodDeleted(pod)
	assert.False(t, r.isContainerTerminal("default/job-abc/app"), "terminatedContainers entry must be cleared on delete")

	cancel()
	r.wg.Wait()
}

func TestOnPodAddedDeleted_RecordsActiveStreamsWithRealTelemetry(t *testing.T) {
	tb, err := metadata.NewTelemetryBuilder(receivertest.NewNopSettings(metadata.Type).TelemetrySettings)
	require.NoError(t, err)

	r := newTestReceiver()
	r.telemetry = tb

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NotPanics(t, func() {
		r.onPodAdded(ctx, makePod("payments", "app-abc", "api", "sidecar"))
	})
	r.mu.Lock()
	assert.Len(t, r.activeStreams, 2)
	r.mu.Unlock()

	require.NotPanics(t, func() {
		r.onPodDeleted(makePod("payments", "app-abc", "api", "sidecar"))
	})
	r.mu.Lock()
	assert.Empty(t, r.activeStreams)
	r.mu.Unlock()

	cancel()
	r.wg.Wait()
}

// ---- stream tests ----

// ---- emitLogLine ----

func TestEmitLogLine_PopulatesResourceAttributes(t *testing.T) {
	sink := &consumertest.LogsSink{}
	r := &logsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: sink,
	}

	r.emitLogLine(context.Background(), "payments", "app-abc", "abc-123-uid", "api", "hello world", time.Time{})

	require.Len(t, sink.AllLogs(), 1)
	rl := sink.AllLogs()[0].ResourceLogs().At(0)
	attrs := rl.Resource().Attributes().AsRaw()
	assert.Equal(t, "payments", attrs["k8s.namespace.name"])
	assert.Equal(t, "app-abc", attrs["k8s.pod.name"])
	assert.Equal(t, "abc-123-uid", attrs["k8s.pod.uid"])
	assert.Equal(t, "api", attrs["k8s.container.name"])
	assert.Equal(t, metadata.ScopeName, rl.ScopeLogs().At(0).Scope().Name())
}

func TestEmitLogLine_PopulatesBody(t *testing.T) {
	sink := &consumertest.LogsSink{}
	r := &logsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: sink,
	}

	r.emitLogLine(context.Background(), "ns", "pod", "uid", "c", "log line content", time.Time{})

	require.Len(t, sink.AllLogs(), 1)
	body := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str()
	assert.Equal(t, "log line content", body)
}

func TestEmitLogLine_FallsBackToNowWhenTimestampZero(t *testing.T) {
	sink := &consumertest.LogsSink{}
	r := &logsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: sink,
	}

	before := time.Now()
	r.emitLogLine(context.Background(), "ns", "pod", "uid", "c", "ts test", time.Time{})
	after := time.Now()

	require.Len(t, sink.AllLogs(), 1)
	ts := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Timestamp().AsTime()
	assert.True(t, !ts.Before(before) && !ts.After(after), "timestamp should be between before and after")
}

func TestEmitLogLine_UsesProvidedTimestamp(t *testing.T) {
	sink := &consumertest.LogsSink{}
	r := &logsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: sink,
	}

	want := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	r.emitLogLine(context.Background(), "ns", "pod", "uid", "c", "ts test", want)

	require.Len(t, sink.AllLogs(), 1)
	got := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Timestamp().AsTime()
	assert.True(t, got.Equal(want), "expected %v, got %v", want, got)
}

// ---- streamConnection batching ----

func TestStreamConnection_BatchesLinesBySize(t *testing.T) {
	sink := &consumertest.LogsSink{}
	r := &logsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: sink,
		cfg:      &Config{MaxBatchSize: 3, FlushInterval: time.Hour},
	}

	// 6 lines with a batch size of 3 → 2 ConsumeLogs calls of 3 records each.
	// FlushInterval is set huge so only the size threshold triggers flushes.
	var b strings.Builder
	for i := 0; i < 6; i++ {
		b.WriteString("line\n")
	}
	_, scanErr := r.streamConnection(context.Background(), strings.NewReader(b.String()),
		logline.Meta{Namespace: "ns", PodName: "pod", PodUID: "uid", ContainerName: "c"})

	require.NoError(t, scanErr)
	assert.Equal(t, 2, len(sink.AllLogs()), "6 lines / batch 3 should produce 2 batches")
	assert.Equal(t, 6, sink.LogRecordCount(), "all 6 records must be forwarded")
	for _, ld := range sink.AllLogs() {
		assert.Equal(t, 3, ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().Len())
	}
}

func TestStreamConnection_FlushesPartialBatchByInterval(t *testing.T) {
	sink := &consumertest.LogsSink{}
	r := &logsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: sink,
		// Batch size far larger than the input so only the timer can flush.
		cfg: &Config{MaxBatchSize: 1000, FlushInterval: 20 * time.Millisecond},
	}

	// A single line that never fills the batch must still be flushed once the
	// interval elapses, then the stream is closed (EOF) which flushes again a
	// no-op. Use a pipe so the stream stays open past the first line.
	pr, pw := io.Pipe()
	go func() {
		_, _ = io.WriteString(pw, "solo line\n")
		time.Sleep(80 * time.Millisecond) // outlive at least one flush tick
		_ = pw.Close()
	}()

	_, scanErr := r.streamConnection(context.Background(), pr,
		logline.Meta{Namespace: "ns", PodName: "pod", PodUID: "uid", ContainerName: "c"})

	require.NoError(t, scanErr)
	require.GreaterOrEqual(t, len(sink.AllLogs()), 1, "partial batch must be flushed by the interval")
	assert.Equal(t, 1, sink.LogRecordCount(), "the single line must be delivered exactly once")
}

func TestStreamConnection_AdvancesCursorToLastTimestampOnSuccess(t *testing.T) {
	sink := &consumertest.LogsSink{}
	r := &logsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: sink,
		cfg:      &Config{MaxBatchSize: 10, FlushInterval: time.Hour},
	}

	input := "2024-01-15T10:00:04.900000000Z a\n2024-01-15T10:00:05.800000000Z b\n"
	lastTS, _ := r.streamConnection(context.Background(), strings.NewReader(input),
		logline.Meta{Namespace: "ns", PodName: "pod", PodUID: "uid", ContainerName: "c"})

	want, err := time.Parse(time.RFC3339Nano, "2024-01-15T10:00:05.800000000Z")
	require.NoError(t, err)
	assert.True(t, lastTS.Equal(want), "cursor must advance to the newest delivered line, got %v", lastTS)
}

// TestStreamConnection_StripsTimestampPrefixFromBody guards against the leading
// RFC3339 timestamp (emitted because PodLogOptions.Timestamps is true) being
// duplicated into both the record Timestamp and its Body. The emitLogLine tests
// do not exercise this because they bypass the scan/parse path.
func TestStreamConnection_StripsTimestampPrefixFromBody(t *testing.T) {
	sink := &consumertest.LogsSink{}
	r := &logsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: sink,
		cfg:      &Config{MaxBatchSize: 10, FlushInterval: time.Hour},
	}

	input := "2024-01-15T10:00:04.900000000Z hello world\n"
	_, scanErr := r.streamConnection(context.Background(), strings.NewReader(input),
		logline.Meta{Namespace: "ns", PodName: "pod", PodUID: "uid", ContainerName: "c"})

	require.NoError(t, scanErr)
	require.Equal(t, 1, sink.LogRecordCount())
	rec := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, "hello world", rec.Body().Str(), "the timestamp prefix must not appear in the body")

	want, err := time.Parse(time.RFC3339Nano, "2024-01-15T10:00:04.900000000Z")
	require.NoError(t, err)
	assert.True(t, rec.Timestamp().AsTime().Equal(want), "the parsed timestamp must still populate the record Timestamp")
}

// TestStreamConnection_FailedConsumeDoesNotAdvanceCursor guards the reconnect
// invariant: if the consumer rejects a batch it is dropped, so the resume
// cursor must stay put — otherwise the dropped lines would be skipped (lost)
// on reconnect instead of re-read.
func TestStreamConnection_FailedConsumeDoesNotAdvanceCursor(t *testing.T) {
	r := &logsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: consumertest.NewErr(assert.AnError),
		cfg:      &Config{MaxBatchSize: 2, FlushInterval: time.Hour},
	}

	input := "2024-01-15T10:00:04.900000000Z a\n2024-01-15T10:00:05.800000000Z b\n"
	lastTS, _ := r.streamConnection(context.Background(), strings.NewReader(input),
		logline.Meta{Namespace: "ns", PodName: "pod", PodUID: "uid", ContainerName: "c"})

	assert.True(t, lastTS.IsZero(), "cursor must not advance when the consumer rejects the batch")
}

// collectBodies flattens every log record body the sink received, in order.
func collectBodies(sink *consumertest.LogsSink) []string {
	var bodies []string
	for _, ld := range sink.AllLogs() {
		recs := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
		for i := 0; i < recs.Len(); i++ {
			bodies = append(bodies, recs.At(i).Body().Str())
		}
	}
	return bodies
}

func TestStreamConnection_SplitsOversizedLineByDefault(t *testing.T) {
	sink := &consumertest.LogsSink{}
	r := &logsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: sink,
		// Cap chosen above the ~32-byte timestamped lines but below the big line,
		// so only the middle line is oversized; behavior defaults to split.
		cfg: &Config{MaxBatchSize: 10, FlushInterval: time.Hour, MaxLogSize: 40},
	}

	oversized := strings.Repeat("x", 100) // 100 > 40 -> chunks of 40 + 40 + 20
	input := "2024-01-15T10:00:04.900000000Z a\n" +
		oversized + "\n" +
		"2024-01-15T10:00:05.800000000Z c\n"

	lastTS, scanErr := r.streamConnection(context.Background(), strings.NewReader(input),
		logline.Meta{Namespace: "ns", PodName: "pod", PodUID: "uid", ContainerName: "c"})

	require.NoError(t, scanErr, "oversized line must be handled in-stream, not surfaced as an error that triggers a reconnect")

	bodies := collectBodies(sink)
	assert.Equal(t, 5, len(bodies), "2 small lines + 3 split chunks of the oversized line")

	// Split loses nothing: the chunk bodies must reconstruct the whole line.
	var reconstructed string
	for _, b := range bodies {
		if strings.HasPrefix(b, "x") {
			reconstructed += b
			assert.LessOrEqual(t, len(b), 40, "no split chunk may exceed max_log_size")
		}
	}
	assert.Equal(t, oversized, reconstructed, "split must preserve the entire oversized line, losing nothing")

	// The cursor must advance to the line after the oversized one (the split
	// chunks carry no timestamp of their own).
	want, err := time.Parse(time.RFC3339Nano, "2024-01-15T10:00:05.800000000Z")
	require.NoError(t, err)
	assert.True(t, lastTS.Equal(want), "cursor must advance to the line after the oversized one, got %v", lastTS)
}

// With max_log_size_behavior=truncate an oversized line keeps its head (the
// first max_log_size bytes) and drops the remainder up to the next newline.
func TestStreamConnection_TruncatesOversizedLineWhenConfigured(t *testing.T) {
	sink := &consumertest.LogsSink{}
	r := &logsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: sink,
		cfg: &Config{
			MaxBatchSize:       10,
			FlushInterval:      time.Hour,
			MaxLogSize:         40,
			MaxLogSizeBehavior: "truncate",
		},
	}

	oversized := strings.Repeat("x", 100)
	input := "2024-01-15T10:00:04.900000000Z a\n" +
		oversized + "\n" +
		"2024-01-15T10:00:05.800000000Z c\n"

	lastTS, scanErr := r.streamConnection(context.Background(), strings.NewReader(input),
		logline.Meta{Namespace: "ns", PodName: "pod", PodUID: "uid", ContainerName: "c"})

	require.NoError(t, scanErr)

	bodies := collectBodies(sink)
	assert.Equal(t, 3, len(bodies), "2 small lines + the truncated head of the oversized line")
	assert.Contains(t, bodies, strings.Repeat("x", 40), "the first max_log_size bytes of the oversized line must be kept")
	for _, b := range bodies {
		assert.LessOrEqual(t, len(b), 40, "no record may exceed max_log_size")
	}

	want, err := time.Parse(time.RFC3339Nano, "2024-01-15T10:00:05.800000000Z")
	require.NoError(t, err)
	assert.True(t, lastTS.Equal(want), "cursor must advance to the line after the oversized one, got %v", lastTS)
}

func TestStreamConnection_CancelStopsAndFlushes(t *testing.T) {
	sink := &consumertest.LogsSink{}
	r := &logsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: sink,
		cfg:      &Config{MaxBatchSize: 1000, FlushInterval: time.Hour},
	}

	ctx, cancel := context.WithCancel(context.Background())
	pr, pw := io.Pipe()
	go func() {
		_, _ = io.WriteString(pw, "before cancel\n")
		time.Sleep(20 * time.Millisecond)
		cancel()       // aborts the in-flight read the way a real ctx cancel does
		_ = pw.Close() // unblock the scanner's Read
	}()

	// Must return promptly (reader goroutine exits) and not deadlock.
	done := make(chan struct{})
	go func() {
		_, _ = r.streamConnection(ctx, pr, logline.Meta{Namespace: "ns", PodName: "pod", PodUID: "uid", ContainerName: "c"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("streamConnection did not return after cancel — possible goroutine leak")
	}
}

type blockingConsumer struct {
	entered chan struct{}
	once    sync.Once
}

func (c *blockingConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *blockingConsumer) ConsumeLogs(ctx context.Context, _ plog.Logs) error {
	c.once.Do(func() { close(c.entered) })
	<-ctx.Done()
	return ctx.Err()
}

func TestStreamConnection_ConsumeUnblocksOnContextCancel(t *testing.T) {
	c := &blockingConsumer{entered: make(chan struct{})}
	r := &logsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: c,
		// Batch size 1 flushes on the first line, entering the blocked
		// ConsumeLogs immediately — the exact Shutdown deadlock scenario.
		cfg: &Config{MaxBatchSize: 1, FlushInterval: time.Hour},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pr, pw := io.Pipe()
	go func() { _, _ = io.WriteString(pw, "line\n") }()

	done := make(chan struct{})
	go func() {
		_, _ = r.streamConnection(ctx, pr, logline.Meta{Namespace: "ns", PodName: "pod", PodUID: "uid", ContainerName: "c"})
		close(done)
	}()

	select {
	case <-c.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("ConsumeLogs was never reached — batch never flushed")
	}

	cancel()       // the Shutdown r.cancel()
	_ = pw.Close() // model req.Stream(ctx)'s Read aborting once ctx is cancelled

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("streamConnection did not return after ctx cancel — ConsumeLogs ignored shutdown (deadlock)")
	}
}

// ---- classifyStreamError ----

func TestClassifyStreamError_Forbidden(t *testing.T) {
	err := apierrors.NewForbidden(schema.GroupResource{Group: "", Resource: "pods/log"}, "mypod", assert.AnError)
	assert.Equal(t, reasonRBACDenied, classifyStreamError(err))
}

func TestClassifyStreamError_NotFound(t *testing.T) {
	err := apierrors.NewNotFound(schema.GroupResource{Group: "", Resource: "pods"}, "mypod")
	assert.Equal(t, reasonPodGone, classifyStreamError(err))
}

func TestClassifyStreamError_Other(t *testing.T) {
	assert.Equal(t, reasonOther, classifyStreamError(context.DeadlineExceeded))
}

// ---- emitLogLine with real obsrep/telemetry wired ----

func TestEmitLogLine_WithRealObsreportAndTelemetry_NoPanic(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	obsrep, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             settings.ID,
		Transport:              "http",
		ReceiverCreateSettings: settings,
	})
	require.NoError(t, err)
	tb, err := metadata.NewTelemetryBuilder(settings.TelemetrySettings)
	require.NoError(t, err)

	sink := &consumertest.LogsSink{}
	r := &logsReceiver{
		settings:  settings,
		consumer:  sink,
		obsrep:    obsrep,
		telemetry: tb,
	}

	require.NotPanics(t, func() {
		r.emitLogLine(context.Background(), "ns", "pod", "uid", "c", "hello", time.Time{})
	})
	require.Len(t, sink.AllLogs(), 1)
}
