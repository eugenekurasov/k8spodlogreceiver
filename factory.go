package k8spodlogreceiver

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/logline"
	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/metadata"
)

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
			AuthType: AuthTypeServiceAccount,
		},
		SinceSeconds: nil, // full available history by default; set explicitly to bound it
		ReconnectBackoff: ReconnectBackoffConfig{
			InitialInterval: 1 * time.Second,
			MaxInterval:     30 * time.Second,
			MaxElapsedTime:  5 * time.Minute,
		},
		MaxBatchSize:       defaultMaxBatchSize,
		FlushInterval:      defaultFlushInterval,
		MaxLogSize:         defaultMaxLogSize,
		MaxLogSizeBehavior: logline.BehaviorSplitName,
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
