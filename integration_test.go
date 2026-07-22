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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/metadata"
)

const (
	integrationNamespace = "k8spodlog-inttest"
	integrationPodName   = "log-emitter"
	integrationMarker    = "k8spodlog-integration-marker"
	// 120s rather than a tighter bound: under CI resource contention (three
	// kind clusters spinning up concurrently via Docker-in-Docker on a
	// shared runner), image pull/scheduling for the test pod can occasionally
	// take longer than a minute even though the receiver itself is working.
	integrationTimeout = 120 * time.Second
)

// TestIntegration_LogsArrive starts the receiver against a real kind cluster,
// creates a pod that emits a known marker line, and asserts the line arrives
// at the consumer with correct resource attributes.
//
// Run with: go test -v -mod=vendor -tags integration -timeout 180s ./...
func TestIntegration_LogsArrive(t *testing.T) {
	// Uses KUBECONFIG env (set by helm/kind-action in CI) or ~/.kube/config locally.
	restCfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), nil,
	).ClientConfig()
	require.NoError(t, err, "load kubeconfig — is a cluster running?")

	// Built via an explicit http.Client (rather than kubernetes.NewForConfig,
	// which hides it) so idle keep-alive connections can be force-closed in
	// t.Cleanup — otherwise their HTTP/2 read-loop goroutines outlive the
	// test and trip goleak.
	httpClient, err := rest.HTTPClientFor(restCfg)
	require.NoError(t, err)
	// httpClient.CloseIdleConnections() alone is a no-op here: rest.HTTPClientFor
	// wraps the *http.Transport in RoundTrippers (userAgent, auth) that don't
	// implement CloseIdleConnections, and http.Client only forwards the call to
	// the top-level transport. utilnet.CloseIdleConnectionsFor unwraps the chain
	// to reach the real transport. Without this the idle conns' HTTP/2 read-loop
	// goroutines outlive the test and trip goleak.
	t.Cleanup(func() { utilnet.CloseIdleConnectionsFor(httpClient.Transport) })

	k8sClient, err := kubernetes.NewForConfigAndClient(restCfg, httpClient)
	require.NoError(t, err)

	ctx := context.Background()

	// Namespace deletion is asynchronous (finalizers + pod termination), and the
	// t.Cleanup below does not wait for it to finish. A rerun that starts before
	// the previous namespace is fully gone would otherwise fail Create with
	// "object is being deleted: namespaces ... already exists". Delete any
	// leftover namespace and wait until it disappears before recreating.
	nsClient := k8sClient.CoreV1().Namespaces()
	_ = nsClient.Delete(ctx, integrationNamespace, metav1.DeleteOptions{})
	require.Eventually(t, func() bool {
		_, getErr := nsClient.Get(ctx, integrationNamespace, metav1.GetOptions{})
		return apierrors.IsNotFound(getErr)
	}, 90*time.Second, 500*time.Millisecond,
		"leftover namespace %q never finished terminating", integrationNamespace)

	_, err = nsClient.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: integrationNamespace},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = nsClient.Delete(
			context.Background(), integrationNamespace, metav1.DeleteOptions{},
		)
	})

	createdPod, err := k8sClient.CoreV1().Pods(integrationNamespace).Create(ctx, &corev1.Pod{
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
		AuthType: AuthTypeKubeConfig, // uses default kubeconfig; kind sets it in CI
	}
	cfg.Namespaces = []string{integrationNamespace}
	// SinceSeconds left nil (factory default): full available history, so the marker
	// printed once at pod startup is still read even if the receiver's
	// stream attaches after the line was already written.

	recv, err := factory.CreateLogs(ctx, receivertest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, recv.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() { _ = recv.Shutdown(context.Background()) })

	// Verify the full path: watch → stream → consumer.
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
				podUID, _ := attrs.Get("k8s.pod.uid")

				if ns.Str() != integrationNamespace ||
					pod.Str() != integrationPodName ||
					container.Str() != "emitter" ||
					podUID.Str() != string(createdPod.UID) {
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
