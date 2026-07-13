package k8spodlogreceiver

import (
	"testing"

	"go.opentelemetry.io/collector/component/componenttest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate_Valid(t *testing.T) {
	cfg := &Config{
		SinceSeconds: 0,
	}
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
	require.NoError(t, cfg.Validate())
}

func TestConfigValidate_NegativeSinceSeconds(t *testing.T) {
	cfg := &Config{
		SinceSeconds: -1,
	}
	assert.Error(t, cfg.Validate())
}

func TestConfigValidate_ZeroSinceSeconds(t *testing.T) {
	cfg := &Config{
		SinceSeconds: 0,
	}
	assert.NoError(t, cfg.Validate())
}
