package k8spodlogreceiver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/metadata"
)

// ---- nextBackoff ----

func TestNextBackoff_Doubles(t *testing.T) {
	assert.Equal(t, 2*time.Second, nextBackoff(1*time.Second, 30*time.Second))
	assert.Equal(t, 4*time.Second, nextBackoff(2*time.Second, 30*time.Second))
}

func TestNextBackoff_CapsAtMax(t *testing.T) {
	assert.Equal(t, 30*time.Second, nextBackoff(20*time.Second, 30*time.Second))
	assert.Equal(t, 30*time.Second, nextBackoff(30*time.Second, 30*time.Second))
	assert.Equal(t, 30*time.Second, nextBackoff(100*time.Second, 30*time.Second))
}

// ---- sleepOrDone ----

func TestSleepOrDone_WaitsFullDuration(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	result := sleepOrDone(ctx, 50*time.Millisecond)
	assert.True(t, result, "should return true when timer fires")
	assert.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond)
}

func TestSleepOrDone_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	result := sleepOrDone(ctx, 10*time.Second)
	assert.False(t, result, "should return false on cancelled context")
}

func TestSleepOrDone_CancelDuringWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	result := sleepOrDone(ctx, 10*time.Second)
	assert.False(t, result)
	assert.Less(t, time.Since(start), 2*time.Second, "should unblock quickly on cancel")
}

// ---- emitLogLine ----

func TestEmitLogLine_PopulatesResourceAttributes(t *testing.T) {
	sink := &consumertest.LogsSink{}
	r := &logsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: sink,
	}

	r.emitLogLine("payments", "app-abc", "api", "hello world")

	require.Len(t, sink.AllLogs(), 1)
	rl := sink.AllLogs()[0].ResourceLogs().At(0)
	attrs := rl.Resource().Attributes().AsRaw()
	assert.Equal(t, "payments", attrs["k8s.namespace.name"])
	assert.Equal(t, "app-abc", attrs["k8s.pod.name"])
	assert.Equal(t, "api", attrs["k8s.container.name"])
	assert.Equal(t, metadata.ScopeName, rl.ScopeLogs().At(0).Scope().Name())
}

func TestEmitLogLine_PopulatesBody(t *testing.T) {
	sink := &consumertest.LogsSink{}
	r := &logsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: sink,
	}

	r.emitLogLine("ns", "pod", "c", "log line content")

	require.Len(t, sink.AllLogs(), 1)
	body := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str()
	assert.Equal(t, "log line content", body)
}

func TestEmitLogLine_SetsTimestamp(t *testing.T) {
	sink := &consumertest.LogsSink{}
	r := &logsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: sink,
	}

	before := time.Now()
	r.emitLogLine("ns", "pod", "c", "ts test")
	after := time.Now()

	require.Len(t, sink.AllLogs(), 1)
	ts := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Timestamp().AsTime()
	assert.True(t, !ts.Before(before) && !ts.After(after), "timestamp should be between before and after")
}
