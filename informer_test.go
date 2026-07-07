package k8sapilogreceiver

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// newTestReceiver creates a minimal logsReceiver for unit tests.
// The default startStream is a no-op that immediately calls wg.Done,
// so tests that don't need real streaming work without a Kubernetes API server.
func newTestReceiver() *logsReceiver {
	r := &logsReceiver{
		cfg:          createDefaultConfig().(*Config),
		activeStreams: make(map[string]context.CancelFunc),
	}
	r.startStream = func(_ context.Context, _, _, _, _ string) {
		defer r.wg.Done()
	}
	return r
}

func makePod(ns, name string, containers ...string) *corev1.Pod {
	specs := make([]corev1.Container, len(containers))
	for i, c := range containers {
		specs[i] = corev1.Container{Name: c}
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       corev1.PodSpec{Containers: specs},
	}
}

func TestOnPodAdded_RegistersStreamsPerContainer(t *testing.T) {
	r := newTestReceiver()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.onPodAdded(ctx, makePod("payments", "app-abc", "api", "sidecar"))

	r.mu.Lock()
	_, hasAPI := r.activeStreams["payments/app-abc/api"]
	_, hasSidecar := r.activeStreams["payments/app-abc/sidecar"]
	r.mu.Unlock()

	assert.True(t, hasAPI, "expected stream for 'api' container")
	assert.True(t, hasSidecar, "expected stream for 'sidecar' container")

	cancel()
	r.wg.Wait()
}

func TestOnPodAdded_Deduplicates(t *testing.T) {
	r := newTestReceiver()

	var calls int
	var mu sync.Mutex
	// Override with a startStream that holds until ctx is cancelled, so the
	// first stream is still "active" when the second onPodAdded arrives.
	r.startStream = func(ctx context.Context, _, _, _, _ string) {
		mu.Lock()
		calls++
		mu.Unlock()
		defer r.wg.Done()
		<-ctx.Done()
	}

	ctx, cancel := context.WithCancel(context.Background())
	pod := makePod("default", "worker", "app")
	r.onPodAdded(ctx, pod)
	r.onPodAdded(ctx, pod) // duplicate — stream already active, must be ignored

	cancel()
	r.wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, calls, "duplicate onPodAdded must not start a second stream")
}

func TestOnPodDeleted_CancelsAndRemovesStream(t *testing.T) {
	cancelled := false
	r := &logsReceiver{
		activeStreams: map[string]context.CancelFunc{
			"default/worker/app": func() { cancelled = true },
		},
	}

	r.onPodDeleted(makePod("default", "worker", "app"))

	assert.True(t, cancelled, "cancel func must be called on pod delete")

	r.mu.Lock()
	_, stillPresent := r.activeStreams["default/worker/app"]
	r.mu.Unlock()
	assert.False(t, stillPresent, "entry must be removed from activeStreams")
}

func TestOnPodDeleted_UnknownPod_NoPanic(t *testing.T) {
	r := &logsReceiver{
		activeStreams: make(map[string]context.CancelFunc),
	}
	require.NotPanics(t, func() {
		r.onPodDeleted(makePod("default", "ghost", "app"))
	})
}

func TestOnPodDeleted_MultiContainer(t *testing.T) {
	cancelledA, cancelledB := false, false
	r := &logsReceiver{
		activeStreams: map[string]context.CancelFunc{
			"ns/pod/a": func() { cancelledA = true },
			"ns/pod/b": func() { cancelledB = true },
		},
	}

	r.onPodDeleted(makePod("ns", "pod", "a", "b"))

	assert.True(t, cancelledA, "container-a stream must be cancelled")
	assert.True(t, cancelledB, "container-b stream must be cancelled")
}
