// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package k8sconfig builds a *rest.Config for talking to the Kubernetes API
// server.
//
// Adapted from opentelemetry-collector-contrib's internal/k8sconfig package
// (APIConfig, CreateRestConfig):
//
//	https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/internal/k8sconfig/config.go
//
// That package lives under an `internal/` path, so Go's package visibility
// rules only allow it to be imported from within the opentelemetry-collector-contrib
// module tree — it cannot be imported from this repo. Adapted rather than
// copied unchanged: upstream's AuthType has a fourth value, AuthTypeTLS,
// that its own CreateRestConfig doesn't implement (a documented TODO
// upstream); we omit it rather than expose a config value that silently
// nil-pointer-panics. We also add KubeconfigPath (AuthTypeKubeConfig can
// point at an explicit kubeconfig file, not just the standard
// KUBECONFIG-env/~/.kube/config loading chain upstream relies on).
package k8sconfig // import "github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/k8sconfig"

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// DefaultKubeAPIQPS is client-go's own built-in default queries-per-second
// limit to the Kubernetes API. Matches
// opentelemetry-collector-contrib's internal/k8sconfig.DefaultKubeAPIQPS.
const DefaultKubeAPIQPS float32 = 5

// DefaultKubeAPIBurst is client-go's own built-in default burst limit.
// Matches opentelemetry-collector-contrib's internal/k8sconfig.DefaultKubeAPIBurst.
const DefaultKubeAPIBurst int = 10

// AuthType describes how to authenticate to the Kubernetes API server.
type AuthType string

const (
	// AuthTypeNone builds the API host from the KUBERNETES_SERVICE_HOST /
	// KUBERNETES_SERVICE_PORT env vars with no client credentials — for
	// talking to an unauthenticated proxy in front of the API server
	// (e.g. `kubectl proxy`), not for production use.
	AuthTypeNone AuthType = "none"
	// AuthTypeServiceAccount uses the pod's mounted ServiceAccount token
	// (standard production mode).
	AuthTypeServiceAccount AuthType = "serviceAccount"
	// AuthTypeKubeConfig uses KubeconfigPath if set, otherwise the
	// standard kubeconfig-loading chain (KUBECONFIG env, then
	// ~/.kube/config), matching kubectl's own behavior.
	AuthTypeKubeConfig AuthType = "kubeConfig"
)

var validAuthTypes = map[AuthType]bool{
	AuthTypeNone:           true,
	AuthTypeServiceAccount: true,
	AuthTypeKubeConfig:     true,
}

// APIConfig controls how the receiver authenticates to the API server.
// Aliased as k8spodlogreceiver.APIConfig so its mapstructure tags stay the
// user-facing config schema; this is the single definition.
type APIConfig struct {
	// AuthType selects how to authenticate: "serviceAccount" (production
	// default), "kubeConfig" (local development), or "none" (unauthenticated,
	// e.g. against `kubectl proxy`).
	AuthType AuthType `mapstructure:"auth_type"`

	// KubeconfigPath is used only when AuthType is "kubeConfig". If empty,
	// falls back to the standard kubeconfig-loading chain.
	KubeconfigPath string `mapstructure:"kubeconfig_path"`

	// KubeAPIQPS is the maximum number of queries per second to the
	// Kubernetes API. Zero (default) leaves client-go's own built-in
	// default in effect (5 QPS) — see DefaultKubeAPIQPS. Increase this if
	// you see "client-side throttling" warnings in the collector logs;
	// each active pod/container stream only briefly touches the apiserver
	// on connect/reconnect (long-running GetLogs streams are exempt from
	// server-side inflight limits), so throughput scales with reconnect
	// rate, not with how many streams are held open concurrently.
	KubeAPIQPS float32 `mapstructure:"kube_api_qps"`

	// KubeAPIBurst is the maximum burst of requests to the Kubernetes API.
	// Zero (default) leaves client-go's own built-in default in effect (10
	// burst) — see DefaultKubeAPIBurst, used alongside KubeAPIQPS above.
	KubeAPIBurst int `mapstructure:"kube_api_burst"`
}

// Validate checks the API config for obvious misconfigurations before the
// collector starts.
func (c APIConfig) Validate() error {
	if !validAuthTypes[c.AuthType] {
		return fmt.Errorf("k8spodlogreceiver: invalid auth_type: %q", c.AuthType)
	}
	if c.KubeAPIQPS < 0 {
		return errors.New("k8spodlogreceiver: kube_api_qps must be >= 0")
	}
	if c.KubeAPIBurst < 0 {
		return errors.New("k8spodlogreceiver: kube_api_burst must be >= 0")
	}
	return nil
}

// CreateRestConfig builds a *rest.Config for the Kubernetes API server.
// Named and shaped to match upstream's CreateRestConfig for easier
// cross-reference.
//
// apiConf.KubeAPIQPS/KubeAPIBurst of 0 leave client-go's own defaults
// (DefaultKubeAPIQPS / DefaultKubeAPIBurst) in effect.
func CreateRestConfig(apiConf APIConfig) (*rest.Config, error) {
	var cfg *rest.Config
	var err error

	switch apiConf.AuthType {
	case AuthTypeServiceAccount:
		cfg, err = rest.InClusterConfig()
	case AuthTypeKubeConfig:
		if apiConf.KubeconfigPath != "" {
			cfg, err = clientcmd.BuildConfigFromFlags("", apiConf.KubeconfigPath)
		} else {
			cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
				clientcmd.NewDefaultClientConfigLoadingRules(),
				nil,
			).ClientConfig()
		}
	case AuthTypeNone:
		host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
		if host == "" || port == "" {
			return nil, errors.New("k8spodlogreceiver: KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT must be set for auth_type=none")
		}
		cfg = &rest.Config{Host: "https://" + net.JoinHostPort(host, port)}
		cfg.Insecure = true
	default:
		return nil, fmt.Errorf("k8spodlogreceiver: invalid auth_type: %q", apiConf.AuthType)
	}
	if err != nil {
		return nil, err
	}

	// Don't use system proxy settings since the API is local to the
	// cluster; a stray HTTP_PROXY/NO_PROXY env var could otherwise
	// silently reroute or break in-cluster API calls.
	cfg.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		if t, ok := rt.(*http.Transport); ok {
			t.Proxy = nil
		}
		return rt
	}

	if apiConf.KubeAPIQPS > 0 {
		cfg.QPS = apiConf.KubeAPIQPS
	}
	if apiConf.KubeAPIBurst > 0 {
		cfg.Burst = apiConf.KubeAPIBurst
	}

	return cfg, nil
}
