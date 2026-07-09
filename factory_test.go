package k8sapilogreceiver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8sapilogreceiver/internal/metadata"
)

func TestNewFactory_Type(t *testing.T) {
	f := NewFactory()
	require.NotNil(t, f)
	assert.Equal(t, metadata.Type, f.Type())
}

func TestCreateDefaultConfig_Defaults(t *testing.T) {
	cfg := createDefaultConfig().(*Config)

	assert.True(t, cfg.APIConfig.InCluster)
	assert.Equal(t, int64(300), cfg.SinceSeconds)
	assert.InEpsilon(t, float32(5), cfg.RateLimit.QPS, 0.001)
	assert.Equal(t, 10, cfg.RateLimit.Burst)
	assert.Equal(t, 1*time.Second, cfg.ReconnectBackoff.InitialInterval)
	assert.Equal(t, 30*time.Second, cfg.ReconnectBackoff.MaxInterval)
	assert.Equal(t, 5*time.Minute, cfg.ReconnectBackoff.MaxElapsedTime)
}

func TestCreateDefaultConfig_Validates(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.NoError(t, cfg.Validate())
}

func TestCreateLogsReceiver(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig()
	r, err := f.CreateLogs(context.Background(), receivertest.NewNopSettings(f.Type()), cfg, &consumertest.LogsSink{})
	require.NoError(t, err)
	require.NotNil(t, r)
}
