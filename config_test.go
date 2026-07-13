package k8spodlogreceiver

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap/confmaptest"

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

// TestLoadConfig_Testdata loads testdata/config.yaml the same way the
// collector does at startup, guarding against config-example drift: the
// keys used here must actually decode into Config (mapstructure tags,
// list syntax) rather than being silently ignored in favor of defaults.
func TestLoadConfig_Testdata(t *testing.T) {
	cm, err := confmaptest.LoadConf("testdata/config.yaml")
	require.NoError(t, err)

	sub, err := cm.Sub("receivers::k8s_podlog")
	require.NoError(t, err)

	// Unmarshal into a zero-value Config, not createDefaultConfig()'s
	// output: a mistyped mapstructure key (e.g. camelCase vs snake_case)
	// silently leaves the field at its Go zero value instead of erroring,
	// and a zero-value default would mask that by looking identical to a
	// correctly-parsed value that happens to match the default.
	cfg := &Config{}
	require.NoError(t, sub.Unmarshal(cfg))
	require.NoError(t, cfg.Validate())

	assert.Equal(t, []string{"payment", "api-gateway"}, cfg.Namespaces)
	assert.Equal(t, "app.kubernetes.io/part-of=payments", cfg.PodLabelSelector)
	require.NotNil(t, cfg.SinceSeconds)
	assert.Equal(t, int64(300), *cfg.SinceSeconds)
	assert.True(t, cfg.APIConfig.InCluster)
	assert.Equal(t, 1*time.Second, cfg.ReconnectBackoff.InitialInterval)
	assert.Equal(t, 30*time.Second, cfg.ReconnectBackoff.MaxInterval)
	assert.Equal(t, 5*time.Minute, cfg.ReconnectBackoff.MaxElapsedTime)
}
