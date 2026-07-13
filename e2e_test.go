//go:build e2e

package k8spodlogreceiver

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

// kubeconfig returns the kubeconfig path from KUBECONFIG env or ~/.kube/config,
// and skips the test if the file does not exist.
func kubeconfig(t *testing.T) string {
	t.Helper()
	if kp := os.Getenv("KUBECONFIG"); kp != "" {
		return kp
	}
	home, err := os.UserHomeDir()
	require.NoError(t, err)
	kp := filepath.Join(home, ".kube", "config")
	if _, err := os.Stat(kp); err != nil {
		t.Skipf("no kubeconfig at %s — set KUBECONFIG to run e2e tests", kp)
	}
	return kp
}

// startReceiver creates and starts the receiver with the given config.
// It skips the test (rather than failing) if the cluster is unreachable,
// so CI without a cluster stays green.
// Returns the log sink and a shutdown function the caller must defer.
func startReceiver(t *testing.T, cfg *Config) (*consumertest.LogsSink, func()) {
	t.Helper()
	factory := NewFactory()
	sink := &consumertest.LogsSink{}
	recv, err := factory.CreateLogs(
		context.Background(),
		receivertest.NewNopSettings(factory.Type()),
		cfg,
		sink,
	)
	require.NoError(t, err)

	if err := recv.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Skipf("cluster unreachable, skipping e2e test: %v", err)
	}
	return sink, func() {
		assert.NoError(t, recv.Shutdown(context.Background()))
	}
}

func baseConfig(t *testing.T) *Config {
	t.Helper()
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.APIConfig.InCluster = false
	cfg.APIConfig.KubeconfigPath = kubeconfig(t)
	cfg.SinceSeconds = 300
	return cfg
}

// TestE2E_ReceivesLogs verifies end-to-end: the receiver connects,
// discovers pods, and emits log records with the three required resource attributes.
func TestE2E_ReceivesLogs(t *testing.T) {
	sink, shutdown := startReceiver(t, baseConfig(t))
	defer shutdown()

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() > 0
	}, 30*time.Second, 500*time.Millisecond, "no log records received within 30s")

	attrs := sink.AllLogs()[0].ResourceLogs().At(0).Resource().Attributes().AsRaw()
	assert.NotEmpty(t, attrs["k8s.namespace.name"])
	assert.NotEmpty(t, attrs["k8s.pod.name"])
	assert.NotEmpty(t, attrs["k8s.container.name"])
}

// TestE2E_NamespaceFilter verifies that setting Namespaces restricts
// log collection to only the specified namespace.
func TestE2E_NamespaceFilter(t *testing.T) {
	cfg := baseConfig(t)
	cfg.Namespaces = []string{"kube-system"}

	sink, shutdown := startReceiver(t, cfg)
	defer shutdown()

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() > 0
	}, 30*time.Second, 500*time.Millisecond, "no log records received from kube-system within 30s")

	for _, ld := range sink.AllLogs() {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			ns, _ := ld.ResourceLogs().At(i).Resource().Attributes().Get("k8s.namespace.name")
			assert.Equal(t, "kube-system", ns.Str(), "namespace filter leaked logs from another namespace")
		}
	}
}

// TestE2E_Shutdown verifies that Shutdown cancels all active streams
// and returns without hanging.
func TestE2E_Shutdown(t *testing.T) {
	sink, shutdown := startReceiver(t, baseConfig(t))

	// Wait for at least one stream to become active before shutting down.
	require.Eventually(t, func() bool {
		return sink.LogRecordCount() > 0
	}, 30*time.Second, 500*time.Millisecond, "no streams became active before shutdown test")

	done := make(chan struct{})
	go func() {
		shutdown()
		close(done)
	}()

	select {
	case <-done:
		// clean shutdown
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown did not return within 10s — possible goroutine leak")
	}
}
