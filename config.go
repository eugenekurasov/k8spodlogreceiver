// Package k8spodlogreceiver implements an OpenTelemetry Collector receiver
// that streams Kubernetes pod logs via the Kubernetes API server
// (kubectl logs-style), avoiding the need for hostPath mounts or
// DaemonSet node-level filesystem access.
package k8spodlogreceiver

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/filter"
)

// Config defines the user-facing configuration for the k8spodlog receiver.
type Config struct {
	// APIConfig controls how the receiver talks to the Kubernetes API
	// server: in-cluster (default) or an explicit kubeconfig path for
	// local development/testing.
	APIConfig APIConfig `mapstructure:"api_config"`

	// Namespaces restricts log collection to specific namespaces.
	// Empty means "all namespaces visible to the ServiceAccount's RBAC".
	Namespaces []string `mapstructure:"namespaces"`

	// Filtered namespace for log collection,
	// Can be helfpull if needed excluded couple namespace instead
	ExcludeNamespaces []filter.Config `mapstructure:"exclude_namespaces"`

	// PodLabelSelector filters which pods are watched, e.g.
	// "app.kubernetes.io/part-of=payments".
	PodLabelSelector string `mapstructure:"pod_label_selector"`

	// SinceSeconds bounds how far back into existing logs to read when a
	// new pod/container is first discovered (mirrors `kubectl logs
	// --since`). Prevents a thundering-herd re-read of full log history
	// on collector restart.
	SinceSeconds int64 `mapstructure:"since_seconds"`

	// ReconnectBackoff controls the retry/backoff behavior when a log
	// stream from the kubelet is interrupted (rotation, pod restart,
	// transient API server errors).
	ReconnectBackoff ReconnectBackoffConfig `mapstructure:"reconnect_backoff"`
}

// APIConfig controls how the receiver authenticates to the API server.
type APIConfig struct {
	// InCluster uses the pod's mounted ServiceAccount token (standard
	// production mode). Default: true.
	InCluster bool `mapstructure:"in_cluster"`

	// KubeconfigPath is used only when InCluster is false (local dev).
	KubeconfigPath string `mapstructure:"kubeconfig_path"`
}

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
	if cfg.SinceSeconds < 0 {
		return errors.New("k8spodlogreceiver: since_seconds must be >= 0")
	}
	return nil
}

var _ component.Config = (*Config)(nil)
