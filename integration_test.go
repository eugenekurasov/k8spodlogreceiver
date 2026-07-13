//go:build integration

package k8spodlogreceiver

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/metadata"
)

const (
	integrationNamespace = "k8spodlog-inttest"
	integrationPodName   = "log-emitter"
	integrationMarker    = "k8spodlog-integration-marker"
	integrationTimeout   = 60 * time.Second
)

// TestIntegration_LogsArrive starts the receiver against a real kind cluster,
// creates a pod that emits a known marker line, and asserts the line arrives
// at the consumer with correct resource attributes.
//
// Run with: go test -v -mod=vendor -tags integration -timeout 120s ./...
func TestIntegration_LogsArrive(t *testing.T) {
	// Uses KUBECONFIG env (set by helm/kind-action in CI) or ~/.kube/config locally.
	restCfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), nil,
	).ClientConfig()
	require.NoError(t, err, "load kubeconfig — is a cluster running?")

	k8sClient, err := kubernetes.NewForConfig(restCfg)
	require.NoError(t, err)

	ctx := context.Background()

	_, err = k8sClient.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: integrationNamespace},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = k8sClient.CoreV1().Namespaces().Delete(
			context.Background(), integrationNamespace, metav1.DeleteOptions{},
		)
	})

	_, err = k8sClient.CoreV1().Pods(integrationNamespace).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      integrationPodName,
			Namespace: integrationNamespace,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:  "emitter",
				Image: "busybox:1.36",
				// Print the marker once, then sleep so the log stream stays open.
				Command: []string{"sh", "-c", `echo "` + integrationMarker + `"; sleep 600`},
			}},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	sink := new(consumertest.LogsSink)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.APIConfig = APIConfig{
		InCluster: false, // uses default kubeconfig; kind sets it in CI
	}
	cfg.Namespaces = []string{integrationNamespace}
	// SinceSeconds left nil (factory default): full available history, so the marker
	// printed once at pod startup is still read even if the receiver's
	// stream attaches after the line was already written.

	recv, err := factory.CreateLogs(ctx, receivertest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, recv.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() { _ = recv.Shutdown(context.Background()) })

	// Verify the full path: informer → stream → consumer.
	// Checking only for no-error from Start() is not sufficient — the receiver
	// could start successfully but silently fail to stream logs.
	assert.Eventually(t, func() bool {
		for _, ld := range sink.AllLogs() {
			for i := 0; i < ld.ResourceLogs().Len(); i++ {
				rl := ld.ResourceLogs().At(i)
				attrs := rl.Resource().Attributes()

				ns, _ := attrs.Get("k8s.namespace.name")
				pod, _ := attrs.Get("k8s.pod.name")
				container, _ := attrs.Get("k8s.container.name")

				if ns.Str() != integrationNamespace ||
					pod.Str() != integrationPodName ||
					container.Str() != "emitter" {
					continue
				}

				for j := 0; j < rl.ScopeLogs().Len(); j++ {
					sl := rl.ScopeLogs().At(j)
					for k := 0; k < sl.LogRecords().Len(); k++ {
						if strings.Contains(sl.LogRecords().At(k).Body().Str(), integrationMarker) {
							return true
						}
					}
				}
			}
		}
		return false
	}, integrationTimeout, 500*time.Millisecond,
		"log record with marker %q never arrived — check receiver logs for stream errors", integrationMarker)
}
