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
		APIConfig:    APIConfig{AuthType: AuthTypeServiceAccount},
		SinceSeconds: nil,
	}
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
	require.NoError(t, cfg.Validate())
}

func TestConfigValidate_NegativeSinceSeconds(t *testing.T) {
	negative := int64(-1)
	cfg := &Config{
		APIConfig:    APIConfig{AuthType: AuthTypeServiceAccount},
		SinceSeconds: &negative,
	}
	assert.Error(t, cfg.Validate())
}

func TestConfigValidate_ZeroSinceSeconds(t *testing.T) {
	zero := int64(0)
	cfg := &Config{
		APIConfig:    APIConfig{AuthType: AuthTypeServiceAccount},
		SinceSeconds: &zero,
	}
	assert.NoError(t, cfg.Validate())
}

func TestConfigValidate_NilSinceSeconds(t *testing.T) {
	cfg := &Config{
		APIConfig:    APIConfig{AuthType: AuthTypeServiceAccount},
		SinceSeconds: nil,
	}
	assert.NoError(t, cfg.Validate())
}

func TestConfigValidate_InvalidAuthType(t *testing.T) {
	cfg := &Config{APIConfig: APIConfig{AuthType: "bogus"}}
	assert.Error(t, cfg.Validate())
}

func TestConfigValidate_NegativeKubeAPIQPS(t *testing.T) {
	cfg := &Config{APIConfig: APIConfig{AuthType: AuthTypeServiceAccount, KubeAPIQPS: -1}}
	assert.Error(t, cfg.Validate())
}

func TestConfigValidate_NegativeKubeAPIBurst(t *testing.T) {
	cfg := &Config{APIConfig: APIConfig{AuthType: AuthTypeServiceAccount, KubeAPIBurst: -1}}
	assert.Error(t, cfg.Validate())
}

func TestConfigValidate_ZeroKubeAPIQPSAndBurst(t *testing.T) {
	// Zero means "use client-go's own default", not "invalid".
	cfg := &Config{APIConfig: APIConfig{AuthType: AuthTypeServiceAccount, KubeAPIQPS: 0, KubeAPIBurst: 0}}
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
	assert.Equal(t, AuthTypeServiceAccount, cfg.APIConfig.AuthType)
	assert.Equal(t, float32(20), cfg.APIConfig.KubeAPIQPS)
	assert.Equal(t, 40, cfg.APIConfig.KubeAPIBurst)
	assert.Equal(t, 1*time.Second, cfg.ReconnectBackoff.InitialInterval)
	assert.Equal(t, 30*time.Second, cfg.ReconnectBackoff.MaxInterval)
	assert.Equal(t, 5*time.Minute, cfg.ReconnectBackoff.MaxElapsedTime)
}
