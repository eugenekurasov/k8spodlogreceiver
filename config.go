// Package k8spodlogreceiver implements an OpenTelemetry Collector receiver
// that streams Kubernetes pod logs via the Kubernetes API server
// (kubectl logs-style), avoiding the need for hostPath mounts or
// DaemonSet node-level filesystem access.
package k8spodlogreceiver

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/k8sconfig"
	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/logline"
)

// Config defines the user-facing configuration for the k8spodlog receiver.
type Config struct {
	// APIConfig controls how the receiver authenticates to the Kubernetes
	// API server: ServiceAccount (default, in-cluster) or an explicit
	// kubeconfig for local development/testing.
	APIConfig APIConfig `mapstructure:"api_config"`

	// Namespaces restricts log collection to specific namespaces.
	// Empty means "all namespaces visible to the ServiceAccount's RBAC".
	Namespaces []string `mapstructure:"namespaces"`

	// PodLabelSelector filters which pods are watched, e.g.
	// "app.kubernetes.io/part-of=payments".
	PodLabelSelector string `mapstructure:"pod_label_selector"`

	// SinceSeconds bounds how far back into existing logs to read when a
	// new pod/container is first discovered (mirrors `kubectl logs
	// --since`). Three states, distinguished via the pointer so "not set"
	// and "explicitly zero" don't collide:
	//   - nil (key absent from config): full available log history
	//     (whatever the kubelet still has retained), no bound.
	//   - pointer to 0: fresh logs only, no historical backfill.
	//   - pointer to N > 0: last N seconds of history.
	// Set an explicit bound in production to avoid a thundering-herd
	// re-read of full available log history on collector restart.
	SinceSeconds *int64 `mapstructure:"since_seconds"`

	// ReconnectBackoff controls the retry/backoff behavior when a log
	// stream from the kubelet is interrupted (rotation, pod restart,
	// transient API server errors).
	ReconnectBackoff ReconnectBackoffConfig `mapstructure:"reconnect_backoff"`

	// MaxBatchSize is the maximum number of log records coalesced into a
	// single plog.Logs / ConsumeLogs call per container stream. A container
	// emitting at, say, 10k lines/sec would otherwise trigger 10k separate
	// pipeline pushes per second; batching amortizes that per-line overhead.
	// A partially-filled batch is still flushed after FlushInterval, so this
	// bound never adds unbounded latency. Zero means "use the default"
	// (defaultMaxBatchSize); negative is rejected by Validate.
	MaxBatchSize int `mapstructure:"max_batch_size"`

	// FlushInterval bounds how long a partially-filled batch waits before it
	// is forwarded, so a low-volume stream isn't held back until it happens
	// to accumulate MaxBatchSize lines. Zero means "use the default"
	// (defaultFlushInterval); negative is rejected by Validate.
	FlushInterval time.Duration `mapstructure:"flush_interval"`

	// MaxLogSize is the maximum size, in bytes, of a single emitted log record
	// body. A physical log line longer than this is handled per
	// MaxLogSizeBehavior rather than being dropped. Zero means "use the
	// default" (defaultMaxLogSize); negative is rejected by Validate.
	MaxLogSize int `mapstructure:"max_log_size"`

	// MaxLogSizeBehavior controls what happens to a log line longer than
	// MaxLogSize. "split" (the default) preserves all data by emitting the line
	// as consecutive MaxLogSize-sized records; "truncate" emits the first
	// MaxLogSize bytes and drops the remainder of that line. Empty means the
	// default. Mirrors the filelog receiver's max_log_size_behavior.
	MaxLogSizeBehavior string `mapstructure:"max_log_size_behavior"`
}

const (
	// defaultMaxBatchSize / defaultFlushInterval are the batching defaults
	// applied by the factory and used as a fallback when a receiver is
	// constructed without a fully-populated Config (e.g. in unit tests).
	defaultMaxBatchSize  = 1000
	defaultFlushInterval = 200 * time.Millisecond

	// defaultMaxLogSize is the fallback per-record size cap (1 MiB), matching
	// the filelog receiver's default max_log_size.
	defaultMaxLogSize = 1024 * 1024
)

// APIConfig controls how the receiver authenticates to the API server.
// Alias (not a new type) for k8sconfig.APIConfig, whose fields carry the
// mapstructure tags that make up this section's user-facing schema — see
// internal/k8sconfig for the field docs and where this is applied.
type APIConfig = k8sconfig.APIConfig

// AuthType and its values are aliased from internal/k8sconfig so callers in
// this package (factory defaults, tests) don't need to import it directly.
type AuthType = k8sconfig.AuthType

const (
	AuthTypeNone           = k8sconfig.AuthTypeNone
	AuthTypeServiceAccount = k8sconfig.AuthTypeServiceAccount
	AuthTypeKubeConfig     = k8sconfig.AuthTypeKubeConfig
)

// DefaultKubeAPIQPS is client-go's own built-in default queries-per-second
// limit, documented here for operators tuning KubeAPIQPS. See
// internal/k8sconfig for where this is actually applied.
const DefaultKubeAPIQPS = k8sconfig.DefaultKubeAPIQPS

// DefaultKubeAPIBurst is client-go's own built-in default burst limit,
// documented here for operators tuning KubeAPIBurst. See internal/k8sconfig
// for where this is actually applied.
const DefaultKubeAPIBurst = k8sconfig.DefaultKubeAPIBurst

// ReconnectBackoffConfig configures exponential backoff for stream
// reconnection.
type ReconnectBackoffConfig struct {
	InitialInterval time.Duration `mapstructure:"initial_interval"`
	MaxInterval     time.Duration `mapstructure:"max_interval"`
	MaxElapsedTime  time.Duration `mapstructure:"max_elapsed_time"`
}

var (
	errNoRBACHint = errors.New(
		"k8spodlogreceiver: ensure the ServiceAccount has RBAC permission " +
			"for resources: [\"pods\", \"pods/log\"], verbs: [\"get\", \"list\", \"watch\"]",
	)
)

// Validate checks the receiver configuration for obvious misconfigurations
// before the collector starts.
func (cfg *Config) Validate() error {
	if cfg.SinceSeconds != nil && *cfg.SinceSeconds < 0 {
		return errors.New("k8spodlogreceiver: since_seconds must be >= 0")
	}
	if cfg.ReconnectBackoff.InitialInterval < 0 {
		return errors.New("k8spodlogreceiver: reconnect_backoff.initial_interval must be >= 0")
	}
	if cfg.ReconnectBackoff.MaxInterval < 0 {
		return errors.New("k8spodlogreceiver: reconnect_backoff.max_interval must be >= 0")
	}
	if cfg.ReconnectBackoff.MaxElapsedTime < 0 {
		return errors.New("k8spodlogreceiver: reconnect_backoff.max_elapsed_time must be >= 0")
	}
	if cfg.MaxBatchSize < 0 {
		return errors.New("k8spodlogreceiver: max_batch_size must be >= 0")
	}
	if cfg.FlushInterval < 0 {
		return errors.New("k8spodlogreceiver: flush_interval must be >= 0")
	}
	if cfg.MaxLogSize < 0 {
		return errors.New("k8spodlogreceiver: max_log_size must be >= 0")
	}
	if _, err := logline.ParseBehavior(cfg.MaxLogSizeBehavior); err != nil {
		return fmt.Errorf("k8spodlogreceiver: max_log_size_behavior %w", err)
	}
	return cfg.APIConfig.Validate()
}

var _ component.Config = (*Config)(nil)
