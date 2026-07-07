package k8sapilogreceiver

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// runPodInformer watches pods matching the configured namespace/label
// selector and starts/stops a log-streaming goroutine per container as
// pods come and go. Using an informer (rather than polling) keeps API
// server load low and reacts to pod lifecycle events promptly.
func (r *logsReceiver) runPodInformer(ctx context.Context) {
	defer r.wg.Done()

	queue := workqueue.NewTypedRateLimitingQueue[string](
		workqueue.DefaultTypedControllerRateLimiter[string](),
	)
	defer queue.ShutDown()

	listOpts := func(opts *metav1.ListOptions) {
		opts.LabelSelector = r.cfg.PodLabelSelector
	}

	namespaces := r.cfg.Namespaces
	if len(namespaces) == 0 {
		namespaces = []string{metav1.NamespaceAll}
	}

	for _, ns := range namespaces {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
					listOpts(&options)
					return r.clientset.CoreV1().Pods(ns).List(ctx, options)
				},
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					listOpts(&options)
					return r.clientset.CoreV1().Pods(ns).Watch(ctx, options)
				},
			},
			&corev1.Pod{},
			30*time.Second,
			cache.Indexers{},
		)

		_, _ = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				pod := obj.(*corev1.Pod)
				r.onPodAdded(ctx, pod)
			},
			DeleteFunc: func(obj interface{}) {
				pod, ok := obj.(*corev1.Pod)
				if !ok {
					return
				}
				r.onPodDeleted(pod)
			},
		})

		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			informer.Run(ctx.Done())
		}()
	}

	<-ctx.Done()
}

func (r *logsReceiver) onPodAdded(ctx context.Context, pod *corev1.Pod) {
	for _, container := range pod.Spec.Containers {
		key := pod.Namespace + "/" + pod.Name + "/" + container.Name

		r.mu.Lock()
		if _, exists := r.activeStreams[key]; exists {
			r.mu.Unlock()
			continue
		}
		streamCtx, streamCancel := context.WithCancel(ctx)
		r.activeStreams[key] = streamCancel
		r.mu.Unlock()

		r.wg.Add(1)
		go r.startStream(streamCtx, pod.Namespace, pod.Name, container.Name, key)
	}
}

func (r *logsReceiver) onPodDeleted(pod *corev1.Pod) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, container := range pod.Spec.Containers {
		key := pod.Namespace + "/" + pod.Name + "/" + container.Name
		if cancel, ok := r.activeStreams[key]; ok {
			cancel()
			delete(r.activeStreams, key)
		}
	}
}
