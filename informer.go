package k8spodlogreceiver

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/metadata"
)

func (r *logsReceiver) runPodInformer(ctx context.Context) {
	defer r.wg.Done()

	namespaces := r.cfg.Namespaces
	if len(namespaces) == 0 {
		namespaces = []string{metav1.NamespaceAll}
	}

	tweakOpts := informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
		opts.LabelSelector = r.cfg.PodLabelSelector
	})

	for _, ns := range namespaces {
		var factory informers.SharedInformerFactory
		if ns == metav1.NamespaceAll {
			factory = informers.NewSharedInformerFactoryWithOptions(r.clientset, 30*time.Second, tweakOpts)
		} else {
			factory = informers.NewSharedInformerFactoryWithOptions(r.clientset, 30*time.Second, informers.WithNamespace(ns), tweakOpts)
		}

		podInformer := factory.Core().V1().Pods().Informer()
		_, _ = podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
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
			podInformer.Run(ctx.Done())
		}()
	}

	<-ctx.Done()
}

func (r *logsReceiver) onPodAdded(ctx context.Context, pod *corev1.Pod) {
	if r.telemetry != nil {
		r.telemetry.InformerEventsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("event_type", metadata.EventTypeAdded)))
	}

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

	r.recordActiveStreams(ctx)
}

func (r *logsReceiver) onPodDeleted(pod *corev1.Pod) {
	ctx := context.Background()
	if r.telemetry != nil {
		r.telemetry.InformerEventsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("event_type", metadata.EventTypeDeleted)))
	}

	r.mu.Lock()
	for _, container := range pod.Spec.Containers {
		key := pod.Namespace + "/" + pod.Name + "/" + container.Name
		if cancel, ok := r.activeStreams[key]; ok {
			cancel()
			delete(r.activeStreams, key)
		}
	}
	r.mu.Unlock()

	r.recordActiveStreams(ctx)
}

// recordActiveStreams reports the current number of tailed pod/container
// log streams to the active_streams gauge.
func (r *logsReceiver) recordActiveStreams(ctx context.Context) {
	if r.telemetry == nil {
		return
	}
	r.mu.Lock()
	count := int64(len(r.activeStreams))
	r.mu.Unlock()
	r.telemetry.ActiveStreams.Record(ctx, count)
}
