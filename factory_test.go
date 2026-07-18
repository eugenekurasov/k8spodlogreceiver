package k8spodlogreceiver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/metadata"
)

func TestNewFactory_Type(t *testing.T) {
	f := NewFactory()
	require.NotNil(t, f)
	assert.Equal(t, metadata.Type, f.Type())
}

func TestCreateDefaultConfig_Defaults(t *testing.T) {
	cfg := createDefaultConfig().(*Config)

	assert.Equal(t, AuthTypeServiceAccount, cfg.APIConfig.AuthType)
	assert.Nil(t, cfg.SinceSeconds)
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
