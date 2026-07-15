package metadata

// Hand-authored, not mdatagen-generated (unlike generated_logs.go and
// generated_status.go in this package). mdatagen (the OTel Collector's
// metadata code generator, go.opentelemetry.io/collector/cmd/mdatagen)
// can't be invoked standalone here: its go.mod carries replace directives
// that `go run module@version` refuses to honor outside the
// opentelemetry-collector monorepo's own build tooling.
//
// The instruments and naming below follow the same conventions real
// mdatagen output uses elsewhere in opentelemetry-collector-contrib (see
// e.g. receiver/kafkareceiver or receiver/fluentforwardreceiver's
// internal/metadata/generated_telemetry.go). Keep this in sync with the
// `telemetry:` section of ../../metadata.yaml; if mdatagen ever becomes
// runnable in this environment, this file should be replaced by its output.

import (
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/metric"
)

// Reason values for the "reason" attribute on StreamErrorsTotal.
const (
	ReasonRBACDenied = "rbac_denied"
	ReasonPodGone    = "pod_gone"
	ReasonOther      = "other"
)

// EventType values for the "event_type" attribute on InformerEventsTotal.
const (
	EventTypeAdded   = "added"
	EventTypeDeleted = "deleted"
)

// TelemetryBuilder holds this receiver's component-specific internal
// metrics (as opposed to the standard otelcol_receiver_accepted_log_records
// / otelcol_receiver_refused_log_records pair, which come from
// receiverhelper.ObsReport in receiver.go, not from here).
type TelemetryBuilder struct {
	ActiveStreams         metric.Int64Gauge
	StreamReconnectsTotal metric.Int64Counter
	StreamErrorsTotal     metric.Int64Counter
	InformerEventsTotal   metric.Int64Counter
}

func NewTelemetryBuilder(settings component.TelemetrySettings) (*TelemetryBuilder, error) {
	meter := settings.MeterProvider.Meter(ScopeName)

	var errs error
	tb := &TelemetryBuilder{}

	var err error
	tb.ActiveStreams, err = meter.Int64Gauge(
		"otelcol_active_streams",
		metric.WithDescription("Number of pod/container log streams currently being tailed [Development]"),
		metric.WithUnit("1"),
	)
	errs = errors.Join(errs, err)

	tb.StreamReconnectsTotal, err = meter.Int64Counter(
		"otelcol_stream_reconnects_total",
		metric.WithDescription("Number of times a pod/container log stream was re-established after the initial connection [Development]"),
		metric.WithUnit("1"),
	)
	errs = errors.Join(errs, err)

	tb.StreamErrorsTotal, err = meter.Int64Counter(
		"otelcol_stream_errors_total",
		metric.WithDescription("Number of log stream connection errors, by reason [Development]"),
		metric.WithUnit("1"),
	)
	errs = errors.Join(errs, err)

	tb.InformerEventsTotal, err = meter.Int64Counter(
		"otelcol_informer_events_total",
		metric.WithDescription("Number of pod add/delete events observed by the discovery informer (debug) [Development]"),
		metric.WithUnit("1"),
	)
	errs = errors.Join(errs, err)

	return tb, errs
}
