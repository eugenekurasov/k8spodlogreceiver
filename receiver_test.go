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
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/collector/receiver/receivertest"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"

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

func makePodWithPhase(ns, name string, phase corev1.PodPhase, containers ...string) *corev1.Pod {
	pod := makePod(ns, name, containers...)
	pod.Status.Phase = phase
	return pod
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

func TestMarkPodPhase_SucceededMarksTerminal(t *testing.T) {
	r := newTestReceiver()
	r.markPodPhase(makePodWithPhase("default", "job-abc", corev1.PodSucceeded, "app"))
	assert.True(t, r.isPodTerminal("default", "job-abc"))
}

func TestMarkPodPhase_FailedMarksTerminal(t *testing.T) {
	r := newTestReceiver()
	r.markPodPhase(makePodWithPhase("default", "job-abc", corev1.PodFailed, "app"))
	assert.True(t, r.isPodTerminal("default", "job-abc"))
}

func TestMarkPodPhase_RunningDoesNotMarkTerminal(t *testing.T) {
	r := newTestReceiver()
	r.markPodPhase(makePodWithPhase("default", "app-abc", corev1.PodRunning, "app"))
	assert.False(t, r.isPodTerminal("default", "app-abc"))
}

func TestIsPodTerminal_UnknownPod_ReturnsFalse(t *testing.T) {
	r := newTestReceiver()
	assert.False(t, r.isPodTerminal("default", "never-seen"))
}

func TestOnPodAdded_AlreadyTerminalPod_MarkedTerminalButStreamsStillStart(t *testing.T) {
	r := newTestReceiver()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Simulates the receiver discovering a pod that already completed
	// before it was ever seen (e.g. a Job that finished before the receiver
	// started, or before the pod was added to this receiver's watch).
	r.onPodAdded(ctx, makePodWithPhase("batch", "job-xyz", corev1.PodSucceeded, "worker"))

	assert.True(t, r.isPodTerminal("batch", "job-xyz"), "pod discovered already-terminal must be marked terminal")

	r.mu.Lock()
	_, hasStream := r.activeStreams["batch/job-xyz/worker"]
	r.mu.Unlock()
	assert.True(t, hasStream, "a stream must still be started once, to pick up any existing log content")

	cancel()
	r.wg.Wait()
}

func TestOnPodDeleted_ClearsTerminatedPodEntry(t *testing.T) {
	r := newTestReceiver()
	pod := makePodWithPhase("default", "job-abc", corev1.PodSucceeded, "app")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.onPodAdded(ctx, pod)
	require.True(t, r.isPodTerminal("default", "job-abc"))

	r.onPodDeleted(pod)
	assert.False(t, r.isPodTerminal("default", "job-abc"), "terminatedPods entry must be cleared on delete")

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

	r.emitLogLine("payments", "app-abc", "abc-123-uid", "api", "hello world", time.Time{})

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

	r.emitLogLine("ns", "pod", "uid", "c", "log line content", time.Time{})

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
	r.emitLogLine("ns", "pod", "uid", "c", "ts test", time.Time{})
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
	r.emitLogLine("ns", "pod", "uid", "c", "ts test", want)

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
		batchMeta{namespace: "ns", podName: "pod", podUID: "uid", containerName: "c"})

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
		batchMeta{namespace: "ns", podName: "pod", podUID: "uid", containerName: "c"})

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
		batchMeta{namespace: "ns", podName: "pod", podUID: "uid", containerName: "c"})

	want, err := time.Parse(time.RFC3339Nano, "2024-01-15T10:00:05.800000000Z")
	require.NoError(t, err)
	assert.True(t, lastTS.Equal(want), "cursor must advance to the newest delivered line, got %v", lastTS)
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
		batchMeta{namespace: "ns", podName: "pod", podUID: "uid", containerName: "c"})

	assert.True(t, lastTS.IsZero(), "cursor must not advance when the consumer rejects the batch")
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
		_, _ = r.streamConnection(ctx, pr, batchMeta{namespace: "ns", podName: "pod", podUID: "uid", containerName: "c"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("streamConnection did not return after cancel — possible goroutine leak")
	}
}

// ---- parseLeadingTimestamp ----

func TestParseLeadingTimestamp_ParsesValidTimestamp(t *testing.T) {
	ts := parseLeadingTimestamp("2024-01-15T10:30:00.123456789Z log line content")
	want, err := time.Parse(time.RFC3339Nano, "2024-01-15T10:30:00.123456789Z")
	require.NoError(t, err)
	assert.True(t, ts.Equal(want))
}

func TestParseLeadingTimestamp_NoSpace(t *testing.T) {
	ts := parseLeadingTimestamp("nospacehere")
	assert.True(t, ts.IsZero())
}

func TestParseLeadingTimestamp_UnparseableTimestamp(t *testing.T) {
	ts := parseLeadingTimestamp("not-a-timestamp rest of line")
	assert.True(t, ts.IsZero())
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
		r.emitLogLine("ns", "pod", "uid", "c", "hello", time.Time{})
	})
	require.Len(t, sink.AllLogs(), 1)
}

// ---- discardOversizedLine ----

// chunkedReader simulates a network stream that returns data in chunks.
type chunkedReader struct {
	data      []byte
	chunkSize int
	pos       int
}

func newChunkedReader(data []byte, chunkSize int) *chunkedReader {
	return &chunkedReader{data: data, chunkSize: chunkSize, pos: 0}
}

func (cr *chunkedReader) Read(p []byte) (int, error) {
	if cr.pos >= len(cr.data) {
		return 0, io.EOF
	}
	n := cr.chunkSize
	if n > len(p) {
		n = len(p)
	}
	if cr.pos+n > len(cr.data) {
		n = len(cr.data) - cr.pos
	}
	copy(p, cr.data[cr.pos:cr.pos+n])
	cr.pos += n
	return n, nil
}

func TestDiscardOversizedLine_SkipsRestOfLine(t *testing.T) {
	// Verify that discardOversizedLine reads until it finds a newline without error.
	data := strings.Repeat("x", 5000) + "\n" + "next line\n"
	reader := newChunkedReader([]byte(data), 1024)

	// Should not panic or error
	discardOversizedLine(reader)
}

func TestDiscardOversizedLine_HandlesMultipleChunks(t *testing.T) {
	// Verify that discardOversizedLine handles data spanning multiple buffer chunks.
	data := strings.Repeat("x", 64*1024) + "\n" + "after\n"
	reader := newChunkedReader([]byte(data), 8192)

	// Should not panic or error
	discardOversizedLine(reader)
}
