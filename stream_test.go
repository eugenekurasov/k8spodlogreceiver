package k8spodlogreceiver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/collector/receiver/receivertest"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/metadata"
)

// nextBackoff/sleepOrDone moved to internal/retry (retry.NextBackoff,
// retry.SleepOrDone) — see internal/retry/retry_test.go for their tests.
// Shared with internal/watch/observer.go's getResourceVersion retry path.

// ---- emitLogLine ----

func TestEmitLogLine_PopulatesResourceAttributes(t *testing.T) {
	sink := &consumertest.LogsSink{}
	r := &logsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: sink,
	}

	r.emitLogLine("payments", "app-abc", "api", "hello world", time.Time{})

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

	r.emitLogLine("ns", "pod", "c", "log line content", time.Time{})

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
	r.emitLogLine("ns", "pod", "c", "ts test", time.Time{})
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
	r.emitLogLine("ns", "pod", "c", "ts test", want)

	require.Len(t, sink.AllLogs(), 1)
	got := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Timestamp().AsTime()
	assert.True(t, got.Equal(want), "expected %v, got %v", want, got)
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
	assert.Equal(t, metadata.ReasonRBACDenied, classifyStreamError(err))
}

func TestClassifyStreamError_NotFound(t *testing.T) {
	err := apierrors.NewNotFound(schema.GroupResource{Group: "", Resource: "pods"}, "mypod")
	assert.Equal(t, metadata.ReasonPodGone, classifyStreamError(err))
}

func TestClassifyStreamError_Other(t *testing.T) {
	assert.Equal(t, metadata.ReasonOther, classifyStreamError(context.DeadlineExceeded))
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
		r.emitLogLine("ns", "pod", "c", "hello", time.Time{})
	})
	require.Len(t, sink.AllLogs(), 1)
}
