// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package k8sconfig

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testdataKubeconfig = "testdata/kubeconfig-nontls.yaml"

func TestCreateRestConfig_AppliesKubeAPIQPSAndBurst(t *testing.T) {
	restCfg, err := CreateRestConfig(APIConfig{
		AuthType:       AuthTypeKubeConfig,
		KubeconfigPath: testdataKubeconfig,
		KubeAPIQPS:     42,
		KubeAPIBurst:   99,
	})
	require.NoError(t, err)
	assert.Equal(t, float32(42), restCfg.QPS)
	assert.Equal(t, 99, restCfg.Burst)
}

func TestCreateRestConfig_ZeroQPSAndBurstLeaveClientGoDefaults(t *testing.T) {
	restCfg, err := CreateRestConfig(APIConfig{
		AuthType:       AuthTypeKubeConfig,
		KubeconfigPath: testdataKubeconfig,
	})
	require.NoError(t, err)
	assert.Zero(t, restCfg.QPS, "unset qps must not override client-go's own default")
	assert.Zero(t, restCfg.Burst, "unset burst must not override client-go's own default")
}

func TestCreateRestConfig_StripsSystemProxy(t *testing.T) {
	restCfg, err := CreateRestConfig(APIConfig{
		AuthType:       AuthTypeKubeConfig,
		KubeconfigPath: testdataKubeconfig,
	})
	require.NoError(t, err)
	require.NotNil(t, restCfg.WrapTransport)

	transport := &http.Transport{Proxy: http.ProxyFromEnvironment}
	wrapped := restCfg.WrapTransport(transport)
	wrappedTransport, ok := wrapped.(*http.Transport)
	require.True(t, ok)
	assert.Nil(t, wrappedTransport.Proxy, "system proxy must be stripped for in-cluster API calls")
}

func TestCreateRestConfigAppliesRateLimits(t *testing.T) {
	// AuthTypeNone requires only KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT.
	t.Setenv("KUBERNETES_SERVICE_HOST", "127.0.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "6443")

	tests := []struct {
		desc          string
		cfg           APIConfig
		expectedQPS   float32
		expectedBurst int
	}{
		{
			desc:          "zero values do not override rest.Config (client-go applies its own defaults at request time)",
			cfg:           APIConfig{AuthType: AuthTypeNone, KubeAPIQPS: 0, KubeAPIBurst: 0},
			expectedQPS:   0,
			expectedBurst: 0,
		},
		{
			desc:          "custom qps and burst are written to rest.Config",
			cfg:           APIConfig{AuthType: AuthTypeNone, KubeAPIQPS: 100, KubeAPIBurst: 200},
			expectedQPS:   100,
			expectedBurst: 200,
		},
		{
			desc:          "only qps set, burst unchanged",
			cfg:           APIConfig{AuthType: AuthTypeNone, KubeAPIQPS: 50},
			expectedQPS:   50,
			expectedBurst: 0,
		},
		{
			desc:          "only burst set, qps unchanged",
			cfg:           APIConfig{AuthType: AuthTypeNone, KubeAPIBurst: 50},
			expectedQPS:   0,
			expectedBurst: 50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			rc, err := CreateRestConfig(tt.cfg)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedQPS, rc.QPS)
			assert.Equal(t, tt.expectedBurst, rc.Burst)
		})
	}
}

func TestCreateRestConfig_AuthTypeNoneRequiresServiceEnvVars(t *testing.T) {
	_, err := CreateRestConfig(APIConfig{AuthType: AuthTypeNone})
	assert.Error(t, err)
}

func TestCreateRestConfig_InvalidAuthType(t *testing.T) {
	_, err := CreateRestConfig(APIConfig{AuthType: "bogus"})
	assert.Error(t, err)
}

func TestAPIConfigValidate_InvalidAuthType(t *testing.T) {
	err := APIConfig{AuthType: "bogus"}.Validate()
	assert.Error(t, err)
}

func TestAPIConfigValidate_NegativeKubeAPIQPS(t *testing.T) {
	err := APIConfig{AuthType: AuthTypeServiceAccount, KubeAPIQPS: -1}.Validate()
	assert.Error(t, err)
}

func TestAPIConfigValidate_NegativeKubeAPIBurst(t *testing.T) {
	err := APIConfig{AuthType: AuthTypeServiceAccount, KubeAPIBurst: -1}.Validate()
	assert.Error(t, err)
}

func TestAPIConfigValidate_Valid(t *testing.T) {
	err := APIConfig{AuthType: AuthTypeServiceAccount, KubeAPIQPS: 20, KubeAPIBurst: 40}.Validate()
	assert.NoError(t, err)
}
