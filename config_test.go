package k8sapilogreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate_Valid(t *testing.T) {
	cfg := &Config{
		SinceSeconds: 0,
		RateLimit:    RateLimitConfig{QPS: 5, Burst: 10},
	}
	require.NoError(t, cfg.Validate())
}

func TestConfigValidate_QPS(t *testing.T) {
	tests := []struct {
		name    string
		qps     float32
		wantErr bool
	}{
		{"positive", 1, false},
		{"zero", 0, true},
		{"negative", -1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{RateLimit: RateLimitConfig{QPS: tt.qps, Burst: 1}}
			err := cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfigValidate_NegativeSinceSeconds(t *testing.T) {
	cfg := &Config{
		SinceSeconds: -1,
		RateLimit:    RateLimitConfig{QPS: 5, Burst: 10},
	}
	assert.Error(t, cfg.Validate())
}

func TestConfigValidate_ZeroSinceSeconds(t *testing.T) {
	cfg := &Config{
		SinceSeconds: 0,
		RateLimit:    RateLimitConfig{QPS: 5, Burst: 10},
	}
	assert.NoError(t, cfg.Validate())
}
