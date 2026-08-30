// Copyright 2026 Yevhenii Kurasov
// SPDX-License-Identifier: Apache-2.0

package k8spodlogreceiver

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/eugenekurasov/k8spodlogreceiver/internal/consumerretry"
	"github.com/eugenekurasov/k8spodlogreceiver/internal/logline"
	"github.com/eugenekurasov/k8spodlogreceiver/internal/metadata"
)

func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	// A per-call copy: a shared package-level pointer would let one
	// receiver's config mutation leak into every other instance.
	sinceSeconds := defaultSinceSeconds
	return &Config{
		APIConfig: APIConfig{
			AuthType: AuthTypeServiceAccount,
		},
		SinceSeconds:    &sinceSeconds, // bounded backfill; `since_seconds: null` opts into full history
		PodResyncPeriod: nil, // defaultPodResyncPeriod; set to 0 to disable resyncs
		ReconnectBackoff: ReconnectBackoffConfig{
			InitialInterval: 1 * time.Second,
			MaxInterval:     30 * time.Second,
			MaxElapsedTime:  5 * time.Minute,
		},
		RetryOnFailure:     consumerretry.NewDefaultConfig(),
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
