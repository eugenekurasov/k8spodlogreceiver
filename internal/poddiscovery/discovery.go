// Package poddiscovery watches the Kubernetes API for pods and reports their
// lifecycle to a handler. It owns the client-go informer machinery and
// nothing else: it does not know what the caller does with a pod.
package poddiscovery // import "github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/poddiscovery"

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apiWatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// Config scopes what is watched.
type Config struct {
	// Namespaces to watch. Empty means every namespace the client's RBAC
	// allows.
	Namespaces []string

	// LabelSelector filters watched pods, e.g. "app.kubernetes.io/name=api".
	LabelSelector string

	// ResyncPeriod is how often every cached pod is re-delivered to OnUpdate.
	// Resyncs are served from the local cache and cost no API traffic. Zero
	// disables them.
	ResyncPeriod time.Duration
}

// Handler receives pod lifecycle callbacks. All three are required; Start
// rejects a partially-filled Handler rather than panicking on the first event.
//
// Callbacks must be idempotent. OnAdd fires for every pod in the initial
// listing and again whenever an informer re-lists after its watch breaks, and
// OnUpdate fires for every cached pod on each resync — so both are called
// repeatedly for pods the handler has already seen.
type Handler struct {
	OnAdd    func(ctx context.Context, pod *corev1.Pod)
	OnUpdate func(ctx context.Context, pod *corev1.Pod)

	// OnDelete also fires for a delete the informer only inferred from a
	// re-listing (its watch missed the event); the pod passed is then the last
	// state the informer had cached.
	OnDelete func(pod *corev1.Pod)
}

// Discovery runs one shared informer per configured namespace.
type Discovery struct {
	client  kubernetes.Interface
	cfg     Config
	logger  *zap.Logger
	handler Handler
}

func New(client kubernetes.Interface, cfg Config, logger *zap.Logger, handler Handler) *Discovery {
	return &Discovery{client: client, cfg: cfg, logger: logger, handler: handler}
}

// Start runs the informers until ctx is cancelled, tracking each one in wg.
// Every informer is built before any is started, so a construction failure
// leaves nothing running.
func (d *Discovery) Start(ctx context.Context, wg *sync.WaitGroup) error {
	if d.handler.OnAdd == nil || d.handler.OnUpdate == nil || d.handler.OnDelete == nil {
		return errors.New("poddiscovery: OnAdd, OnUpdate and OnDelete must all be set")
	}

	namespaces := d.cfg.Namespaces
	if len(namespaces) == 0 {
		namespaces = []string{metav1.NamespaceAll}
	}

	informers := make([]cache.SharedIndexInformer, 0, len(namespaces))
	for _, namespace := range namespaces {
		informer, err := d.newInformer(ctx, namespace)
		if err != nil {
			return err
		}
		informers = append(informers, informer)
	}

	d.logger.Info("watching pods",
		zap.Strings("namespaces", loggableNamespaces(namespaces)),
		zap.String("label_selector", d.cfg.LabelSelector),
		zap.Duration("resync_period", d.cfg.ResyncPeriod))

	for _, informer := range informers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			informer.RunWithContext(ctx)
		}()
	}

	// Report the initial listing once, so a silent start can be told apart
	// from one that never reached the API server. Runs in the background:
	// Start must not block the collector's startup on a reachable cluster.
	wg.Add(1)
	go func() {
		defer wg.Done()
		synced := make([]cache.InformerSynced, len(informers))
		for i, informer := range informers {
			synced[i] = informer.HasSynced
		}
		if !cache.WaitForCacheSync(ctx.Done(), synced...) {
			return // ctx cancelled during shutdown
		}
		d.logger.Info("initial pod listing complete", zap.Int("pods", d.cachedPods(informers)))
	}()

	return nil
}

func (d *Discovery) cachedPods(informers []cache.SharedIndexInformer) int {
	total := 0
	for _, informer := range informers {
		total += len(informer.GetStore().ListKeys())
	}
	return total
}

// loggableNamespaces renders the watch scope for logs: the informer uses ""
// to mean every namespace, which reads as an empty entry otherwise.
func loggableNamespaces(namespaces []string) []string {
	out := make([]string, len(namespaces))
	for i, namespace := range namespaces {
		if namespace == metav1.NamespaceAll {
			out[i] = "<all>"
			continue
		}
		out[i] = namespace
	}
	return out
}

func (d *Discovery) newInformer(ctx context.Context, namespace string) (cache.SharedIndexInformer, error) {
	pods := d.client.CoreV1().Pods(namespace)
	// The typed client is used rather than cache.NewFilteredListWatchFromClient
	// (which needs a RESTClient the fake clientset does not provide) — this
	// keeps discovery drivable by fake.NewSimpleClientset in tests, and hands
	// the handler *corev1.Pod directly, with no unstructured decoding.
	lw := podListWatch{&cache.ListWatch{
		ListWithContextFunc: func(listCtx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
			opts.LabelSelector = d.cfg.LabelSelector
			return pods.List(listCtx, opts)
		},
		WatchFuncWithContext: func(watchCtx context.Context, opts metav1.ListOptions) (apiWatch.Interface, error) {
			opts.LabelSelector = d.cfg.LabelSelector
			return pods.Watch(watchCtx, opts)
		},
	}}

	informer := cache.NewSharedIndexInformer(lw, &corev1.Pod{}, d.cfg.ResyncPeriod, cache.Indexers{})

	if err := informer.SetTransform(trimPod); err != nil {
		return nil, fmt.Errorf("setting pod cache transform: %w", err)
	}
	if err := informer.SetWatchErrorHandlerWithContext(d.logWatchError(namespace)); err != nil {
		return nil, fmt.Errorf("setting pod watch error handler: %w", err)
	}
	deliver := func(obj any, to func(*corev1.Pod)) {
		pod, ok := podFrom(obj)
		if !ok {
			d.logger.Warn("informer delivered an object that is not a pod",
				zap.String("namespace", namespace))
			return
		}
		to(pod)
	}

	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			deliver(obj, func(pod *corev1.Pod) { d.handler.OnAdd(ctx, pod) })
		},
		UpdateFunc: func(_, newObj any) {
			deliver(newObj, func(pod *corev1.Pod) { d.handler.OnUpdate(ctx, pod) })
		},
		DeleteFunc: func(obj any) {
			deliver(obj, d.handler.OnDelete)
		},
	}); err != nil {
		return nil, fmt.Errorf("registering pod event handler: %w", err)
	}

	return informer, nil
}

// podListWatch opts out of WatchList (streaming list) semantics, which
// client-go's WatchListClient gate turns on by default. Where the API server
// supports it, a streaming list is the better primitive; where it does not,
// the reflector waits ~10s for a bookmark that never comes and only then
// falls back to a plain List — and it retries that on every re-list, since
// the fallback is not sticky. This receiver deliberately stays on the
// List/Watch semantics that have been stable since Kubernetes 1.0 (see the
// compatibility section of the receiver's README), so startup behaves
// identically on every supported cluster version. The paging the reflector
// already does keeps a plain List from spiking memory. Drop this wrapper once
// the minimum supported Kubernetes version has streaming lists.
type podListWatch struct {
	*cache.ListWatch
}

func (podListWatch) IsWatchListSemanticsUnSupported() bool { return true }

// podFrom unwraps what an informer hands a handler. A delete the informer
// only inferred from a re-listing arrives wrapped in a
// DeletedFinalStateUnknown tombstone holding the last cached state.
func podFrom(obj any) (*corev1.Pod, bool) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	pod, ok := obj.(*corev1.Pod)
	return pod, ok
}

// trimPod drops fields no consumer reads before a pod enters the informer's
// cache. ManagedFields is pure API-server bookkeeping and is routinely the
// largest part of a pod object.
func trimPod(obj any) (any, error) {
	if pod, ok := podFrom(obj); ok {
		pod.ManagedFields = nil
	}
	return obj, nil
}

// logWatchError reports a broken watch. The informer recovers on its own — it
// re-lists and resumes, reconciling the refreshed state against its cache —
// so this only decides how loudly to say so.
func (d *Discovery) logWatchError(namespace string) func(context.Context, *cache.Reflector, error) {
	return func(_ context.Context, _ *cache.Reflector, err error) {
		switch {
		case errors.Is(err, context.Canceled):
			// Shutdown.
		case apierrors.IsResourceExpired(err) || apierrors.IsGone(err):
			// The watch resourceVersion was compacted away by etcd; routine.
			d.logger.Debug("pod watch expired, informer is re-listing",
				zap.String("namespace", namespace), zap.Error(err))
		default:
			d.logger.Warn("pod watch failed, informer will retry",
				zap.String("namespace", namespace), zap.Error(err))
		}
	}
}
