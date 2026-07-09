package k8sapilogreceiver

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8sapilogreceiver/internal/metadata"
)

// NewFactory creates a factory for the k8sapilog receiver, following the
// standard OpenTelemetry Collector Contrib component conventions (see
// receiver/filelogreceiver for a reference implementation of the same
// pattern applied to a different transport).
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		APIConfig: APIConfig{
			InCluster: true,
		},
		SinceSeconds: 300,
		ReconnectBackoff: ReconnectBackoffConfig{
			InitialInterval: 1 * time.Second,
			MaxInterval:     30 * time.Second,
			MaxElapsedTime:  5 * time.Minute,
		},
		RateLimit: RateLimitConfig{
			QPS:   5,
			Burst: 10,
		},
	}
}

func createLogsReceiver(
	_ context.Context,
	settings receiver.Settings,
	cfg component.Config,
	consumer consumer.Logs,
) (receiver.Logs, error) {
	rCfg := cfg.(*Config)
	return newLogsReceiver(settings, rCfg, consumer)
}
