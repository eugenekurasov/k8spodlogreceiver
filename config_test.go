package k8spodlogreceiver

import (
	"testing"

	"go.opentelemetry.io/collector/component/componenttest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate_Valid(t *testing.T) {
	cfg := &Config{
		SinceSeconds: nil,
	}
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
	require.NoError(t, cfg.Validate())
}

func TestConfigValidate_NegativeSinceSeconds(t *testing.T) {
	negative := int64(-1)
	cfg := &Config{
		SinceSeconds: &negative,
	}
	assert.Error(t, cfg.Validate())
}

func TestConfigValidate_ZeroSinceSeconds(t *testing.T) {
	zero := int64(0)
	cfg := &Config{
		SinceSeconds: &zero,
	}
	assert.NoError(t, cfg.Validate())
}

func TestConfigValidate_NilSinceSeconds(t *testing.T) {
	cfg := &Config{
		SinceSeconds: nil,
	}
	assert.NoError(t, cfg.Validate())
}
