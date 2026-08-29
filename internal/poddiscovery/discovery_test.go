package poddiscovery

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

func pod(namespace, name string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
}

// recorder collects what a Discovery reports, keyed "<namespace>/<name>".
type recorder struct {
	added   chan string
	updated chan string
	deleted chan string
}

func newRecorder() *recorder {
	return &recorder{
		added:   make(chan string, 16),
		updated: make(chan string, 16),
		deleted: make(chan string, 16),
	}
}

func (r *recorder) handler() Handler {
	key := func(p *corev1.Pod) string { return p.Namespace + "/" + p.Name }
	return Handler{
		OnAdd:    func(_ context.Context, p *corev1.Pod) { r.added <- key(p) },
		OnUpdate: func(_ context.Context, p *corev1.Pod) { r.updated <- key(p) },
		OnDelete: func(p *corev1.Pod) { r.deleted <- key(p) },
	}
}

func awaitKey(t *testing.T, ch chan string, what string) string {
	t.Helper()
	select {
	case key := <-ch:
		return key
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return ""
	}
}

// start runs a Discovery over objects and stops it when the test ends.
func start(t *testing.T, cfg Config, objects ...runtime.Object) (*fake.Clientset, *recorder) {
	t.Helper()

	client := fake.NewSimpleClientset(objects...)
	rec := newRecorder()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	require.NoError(t, New(client, cfg, zap.NewNop(), rec.handler()).Start(ctx, &wg))
	return client, rec
}

func TestDiscovery_ReportsInitialListAsAdds(t *testing.T) {
	_, rec := start(t, Config{Namespaces: []string{"payments"}}, pod("payments", "app-abc"))

	assert.Equal(t, "payments/app-abc", awaitKey(t, rec.added, "the initial-list pod"),
		"a pod that exists before Start must be reported through the initial listing")
}

func TestDiscovery_ReportsCreatesAndDeletes(t *testing.T) {
	client, rec := start(t, Config{Namespaces: []string{"payments"}})
	ctx := context.Background()

	_, err := client.CoreV1().Pods("payments").Create(ctx, pod("payments", "app-abc"), metav1.CreateOptions{})
	require.NoError(t, err)
	assert.Equal(t, "payments/app-abc", awaitKey(t, rec.added, "the created pod"))

	require.NoError(t, client.CoreV1().Pods("payments").Delete(ctx, "app-abc", metav1.DeleteOptions{}))
	assert.Equal(t, "payments/app-abc", awaitKey(t, rec.deleted, "the deleted pod"))
}

func TestDiscovery_HonoursNamespaceScope(t *testing.T) {
	client, rec := start(t, Config{Namespaces: []string{"payments"}})
	ctx := context.Background()

	_, err := client.CoreV1().Pods("other").Create(ctx, pod("other", "ignored"), metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = client.CoreV1().Pods("payments").Create(ctx, pod("payments", "watched"), metav1.CreateOptions{})
	require.NoError(t, err)

	assert.Equal(t, "payments/watched", awaitKey(t, rec.added, "the in-scope pod"),
		"a pod outside the configured namespaces must not be reported")
}

func TestDiscovery_WatchesAllNamespacesWhenNoneConfigured(t *testing.T) {
	client, rec := start(t, Config{})
	ctx := context.Background()

	_, err := client.CoreV1().Pods("anywhere").Create(ctx, pod("anywhere", "app-abc"), metav1.CreateOptions{})
	require.NoError(t, err)

	assert.Equal(t, "anywhere/app-abc", awaitKey(t, rec.added, "the pod in an unlisted namespace"))
}

func TestDiscovery_StartRejectsIncompleteHandler(t *testing.T) {
	d := New(fake.NewSimpleClientset(), Config{}, zap.NewNop(), Handler{
		OnAdd: func(context.Context, *corev1.Pod) {},
	})

	var wg sync.WaitGroup
	err := d.Start(context.Background(), &wg)

	require.Error(t, err, "a partially-filled Handler must be rejected, not panic on the first event")
	assert.Contains(t, err.Error(), "must all be set")
}

// TestPodFrom_UnwrapsTombstone covers the missed-delete path: when a delete is
// only inferred from a re-listing, the informer wraps the last cached pod in a
// DeletedFinalStateUnknown. Failing to unwrap it would silently drop the
// delete — the cacheless observer this package replaced could not report it at
// all.
func TestPodFrom_UnwrapsTombstone(t *testing.T) {
	want := pod("payments", "app-abc")

	got, ok := podFrom(cache.DeletedFinalStateUnknown{Key: "payments/app-abc", Obj: want})

	require.True(t, ok)
	assert.Equal(t, want, got)
}

func TestPodFrom_RejectsNonPod(t *testing.T) {
	_, ok := podFrom(&corev1.Service{})
	assert.False(t, ok, "a non-pod object must be reported as such, not silently coerced")
}

func TestTrimPod_DropsManagedFields(t *testing.T) {
	p := pod("payments", "app-abc")
	p.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "kubelet"}}

	trimmed, err := trimPod(p)

	require.NoError(t, err)
	assert.Nil(t, trimmed.(*corev1.Pod).ManagedFields)
	assert.Equal(t, "app-abc", trimmed.(*corev1.Pod).Name, "trimming must not disturb fields consumers read")
}
