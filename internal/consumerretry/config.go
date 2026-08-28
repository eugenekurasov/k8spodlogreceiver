package consumerretry

import (
	"errors"
	"time"
)

// Config defines configuration for retrying batches in case of receiving a
// retryable error from a downstream consumer. If the retryable error doesn't
// provide a delay, exponential backoff is applied.
type Config struct {
	// Enabled indicates whether to retry sending logs in case of receiving a
	// retryable error from a downstream consumer. Default is true — see the
	// package doc in logs.go for why this diverges from upstream.
	Enabled bool `mapstructure:"enabled"`
	// InitialInterval the time to wait after the first failure before retrying.
	// Default value is 1 second.
	InitialInterval time.Duration `mapstructure:"initial_interval"`
	// MaxInterval is the upper bound on backoff interval. Once this value is
	// reached the delay between consecutive retries will always be MaxInterval.
	// Default value is 30 seconds.
	MaxInterval time.Duration `mapstructure:"max_interval"`
	// MaxElapsedTime is the maximum amount of time (including retries) spent
	// trying to send a logs batch to a downstream consumer. Once this value is
	// reached, the data is discarded. It never stops if MaxElapsedTime == 0.
	// Default value is 5 minutes.
	//
	// Note that retrying blocks the caller, so a long value here trades
	// delivery latency for durability.
	MaxElapsedTime time.Duration `mapstructure:"max_elapsed_time"`
}

// NewDefaultConfig returns the default Config.
//
// Enabled is true here where upstream defaults it to false; see logs.go.
func NewDefaultConfig() Config {
	return Config{
		Enabled:         true,
		InitialInterval: 1 * time.Second,
		MaxInterval:     30 * time.Second,
		MaxElapsedTime:  5 * time.Minute,
	}
}

// Validate checks the retry settings for obvious misconfigurations. Upstream's
// Config has no Validate; it is added here to match how this module's other
// config sections (see internal/k8sconfig) report bad values at startup rather
// than behaving oddly at runtime.
func (cfg Config) Validate() error {
	if cfg.InitialInterval < 0 {
		return errors.New("retry_on_failure.initial_interval must be >= 0")
	}
	if cfg.MaxInterval < 0 {
		return errors.New("retry_on_failure.max_interval must be >= 0")
	}
	if cfg.MaxElapsedTime < 0 {
		return errors.New("retry_on_failure.max_elapsed_time must be >= 0")
	}
	return nil
}
