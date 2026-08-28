// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// This file is copied, with adaptations, from
// opentelemetry-collector-contrib's internal/k8sinventory/watch package:
//
//	https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/internal/k8sinventory/watch/observer_test.go
//
// The checkpointer/storage-persistence tests were dropped because this copy
// removed the upstream checkpointer (see observer.go header). Config no longer
// embeds k8sinventory.Config (fields are flattened here; the unused
// IncludeInitialState / ResourceVersion / FieldSelector knobs were removed
// along with getResourceVersion and its tests), New no longer takes a
// storage.Client and returns no error — the tests below track those
// signatures. The initial state is always emitted now, so event counts
// include pods that exist before Start.

package watch // import "github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/watch"

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apiWatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/fake"
	k8s_testing "k8s.io/client-go/testing"
)

func TestObserver(t *testing.T) {
	mockClient := newMockDynamicClient()
	mockClient.createPods(
		generatePod("pod1", "default", map[string]any{
			"environment": "production",
		}, "1"),
	)

	cfg := Config{
		Gvr: schema.GroupVersionResource{
			Group:    "",
			Version:  "v1",
			Resource: "pods",
		},
		Namespaces: []string{"default"},
	}

	// Buffered: the always-on initial state emits pod1 before this test
	// starts draining, and an unbuffered send would stall the observer so the
	// watch would only register after createPods below (losing those events).
	receivedEventsChan := make(chan *apiWatch.Event, 10)

	obs := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	wg := sync.WaitGroup{}

	stopChan := obs.Start(t.Context(), &wg)

	time.Sleep(time.Millisecond * 100)

	mockClient.createPods(
		generatePod("pod2", "default", map[string]any{
			"environment": "test",
		}, "2"),
		generatePod("pod3", "default_ignore", map[string]any{
			"environment": "production",
		}, "3"),
		generatePod("pod4", "default", map[string]any{
			"environment": "production",
		}, "4"),
	)

	// pod1 existed before Start, so the always-on initial state adds one
	// synthetic Added on top of the two watch events (pod3 is in an
	// unwatched namespace).
	verifyReceivedEvents(t, 3, receivedEventsChan, stopChan)

	wg.Wait()
}

func TestObserverWithInitialState(t *testing.T) {
	mockClient := newMockDynamicClient()
	mockClient.createPods(
		generatePod("pod1", "default", map[string]any{
			"environment": "production",
		}, "1"),
	)

	cfg := Config{
		Gvr: schema.GroupVersionResource{
			Group:    "",
			Version:  "v1",
			Resource: "pods",
		},
		Namespaces: []string{"default"},
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	wg := sync.WaitGroup{}

	stopChan := obs.Start(t.Context(), &wg)

	verifyReceivedEvents(t, 1, receivedEventsChan, stopChan)

	wg.Wait()
}

func TestObserverExcludeDelete(t *testing.T) {
	mockClient := newMockDynamicClient()

	cfg := Config{
		Gvr: schema.GroupVersionResource{
			Group:    "",
			Version:  "v1",
			Resource: "pods",
		},
		Namespaces: []string{"default"},
		Exclude: map[apiWatch.EventType]bool{
			apiWatch.Deleted: true,
		},
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	wg := sync.WaitGroup{}

	stopChan := obs.Start(t.Context(), &wg)

	<-time.After(time.Millisecond * 100)

	pod := generatePod("pod1", "default", map[string]any{
		"environment": "production",
	}, "1")

	// create and delete the pod - only the creation event should be received
	mockClient.createPods(pod)
	mockClient.deletePods(pod)

	verifyReceivedEvents(t, 1, receivedEventsChan, stopChan)

	wg.Wait()
}

func TestObserverEmptyNamespaces(t *testing.T) {
	mockClient := newMockDynamicClient()

	cfg := Config{
		Gvr: schema.GroupVersionResource{
			Group:    "",
			Version:  "v1",
			Resource: "pods",
		},
		Namespaces: []string{}, // empty to watch all namespaces
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	wg := sync.WaitGroup{}

	stopChan := obs.Start(t.Context(), &wg)

	time.Sleep(time.Millisecond * 100)

	mockClient.createPods(
		generatePod("pod1", "default", map[string]any{"env": "test"}, "1"),
		generatePod("pod2", "other", map[string]any{"env": "prod"}, "2"),
	)

	verifyReceivedEvents(t, 2, receivedEventsChan, stopChan)

	wg.Wait()
}

func TestObserverMultipleNamespaces(t *testing.T) {
	mockClient := newMockDynamicClient()

	cfg := Config{
		Gvr: schema.GroupVersionResource{
			Group:    "",
			Version:  "v1",
			Resource: "pods",
		},
		Namespaces: []string{"default", "other"},
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	wg := sync.WaitGroup{}

	stopChan := obs.Start(t.Context(), &wg)

	time.Sleep(time.Millisecond * 100)

	mockClient.createPods(
		generatePod("pod1", "default", map[string]any{"env": "test"}, "1"),
		generatePod("pod2", "other", map[string]any{"env": "prod"}, "2"),
		generatePod("pod3", "ignored", map[string]any{"env": "dev"}, "3"),
	)

	verifyReceivedEvents(t, 2, receivedEventsChan, stopChan)

	wg.Wait()
}

func TestObserverWithSelectors(t *testing.T) {
	mockClient := newMockDynamicClient()

	cfg := Config{
		Gvr: schema.GroupVersionResource{
			Group:    "",
			Version:  "v1",
			Resource: "pods",
		},
		Namespaces:    []string{"default"},
		LabelSelector: "environment=test",
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	wg := sync.WaitGroup{}

	stopChan := obs.Start(t.Context(), &wg)

	time.Sleep(time.Millisecond * 100)

	// Since fake client doesn't filter, it will return all, but the code path is covered
	mockClient.createPods(
		generatePod("pod1", "default", map[string]any{"environment": "test"}, "1"),
		generatePod("pod2", "default", map[string]any{"environment": "prod"}, "2"),
	)

	verifyReceivedEvents(t, 2, receivedEventsChan, stopChan)

	wg.Wait()
}

func TestObserverInitialStateError(t *testing.T) {
	mockClient := newMockDynamicClient()

	// Make list return error for initial state
	mockClient.client.(*fake.FakeDynamicClient).PrependReactor("list", "pods", func(_ k8s_testing.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("mock list error")
	})

	cfg := Config{
		Gvr: schema.GroupVersionResource{
			Group:    "",
			Version:  "v1",
			Resource: "pods",
		},
		Namespaces: []string{"default"},
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	wg := sync.WaitGroup{}

	// Unlike contrib (which cancels and gives up on a List error), this copy
	// backs off and retries the state re-List indefinitely. That retry loop is
	// upstream of doWatch, so closing stopChan can't interrupt it — the only way
	// to stop the observer here is to cancel its context.
	ctx, cancel := context.WithCancel(t.Context())
	stopChan := obs.Start(ctx, &wg)

	time.Sleep(time.Millisecond * 100)

	// No events should be received due to error
	select {
	case <-receivedEventsChan:
		t.Fatal("unexpected event received")
	case <-time.After(100 * time.Millisecond):
		// ok
	}

	cancel()
	close(stopChan)

	wg.Wait()
}

func TestObserverInitialStateNoObjects(t *testing.T) {
	mockClient := newMockDynamicClient()

	cfg := Config{
		Gvr: schema.GroupVersionResource{
			Group:    "",
			Version:  "v1",
			Resource: "pods",
		},
		Namespaces: []string{"default"},
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	wg := sync.WaitGroup{}

	stopChan := obs.Start(t.Context(), &wg)

	time.Sleep(time.Millisecond * 100)

	// No events since no objects
	select {
	case <-receivedEventsChan:
		t.Fatal("unexpected event received")
	case <-time.After(100 * time.Millisecond):
		// ok
	}

	close(stopChan)

	wg.Wait()
}

// TestSendInitialStateReturnsListRV verifies that sendInitialState returns the
// list's own ResourceVersion, not just the highest individual object RV.
// The list RV is always >= any individual object RV and is the correct starting
// point for the subsequent watch to avoid a race window between two List calls.
// TestObserverReplaysInitialStateAfter410 guards the divergence from contrib
// documented in the observer.go header: after a 410 Gone the observer must
// re-emit the current state as synthetic Added events, not just resume the
// watch from a fresh resourceVersion. A pod created during the blind window
// only surfaces through that replay.
func TestObserverReplaysInitialStateAfter410(t *testing.T) {
	mockClient := newMockDynamicClient()
	mockClient.createPods(generatePod("pod1", "default", nil, "1"))

	// The first watch attempt delivers a 410 Gone error event; later attempts
	// fall through to the fake's default watch reactor.
	var watchCalls atomic.Int32
	mockClient.client.(*fake.FakeDynamicClient).PrependWatchReactor("pods", func(_ k8s_testing.Action) (bool, apiWatch.Interface, error) {
		if watchCalls.Add(1) > 1 {
			return false, nil, nil
		}
		fw := apiWatch.NewRaceFreeFake()
		fw.Error(&v1.Status{Code: http.StatusGone, Reason: v1.StatusReasonGone})
		return true, fw, nil
	})

	cfg := Config{
		Gvr:        schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Namespaces: []string{"default"},
	}

	receivedEventsChan := make(chan *apiWatch.Event, 10)
	obs := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	wg := sync.WaitGroup{}
	stopChan := obs.Start(t.Context(), &wg)

	// Two Added events for the same pod: the initial state, then the replay
	// forced by the 410.
	for i := 0; i < 2; i++ {
		select {
		case ev := <-receivedEventsChan:
			assert.Equal(t, apiWatch.Added, ev.Type)
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for event %d", i+1)
		}
	}

	close(stopChan)
	wg.Wait()
}

func TestSendInitialStateReturnsListRV(t *testing.T) {
	mockClient := newMockDynamicClient()
	// Set list RV to "999", higher than any individual object RV below.
	mockClient.setListResourceVersion("999")
	mockClient.createPods(
		generatePod("pod1", "default", nil, "100"),
		generatePod("pod2", "default", nil, "200"),
	)

	cfg := Config{
		Gvr:        schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Namespaces: []string{"default"},
	}

	obs := New(mockClient, cfg, zap.NewNop(), nil)

	resource := mockClient.Resource(cfg.Gvr)
	listRV := obs.sendInitialState(t.Context(), resource.Namespace("default"), "default")
	assert.Equal(t, "999", listRV, "sendInitialState should return the list's own ResourceVersion")
}

func verifyReceivedEvents(t *testing.T, numEvents int, receivedEventsChan chan *apiWatch.Event, stopChan chan struct{}) {
	receivedEvents := 0

	exit := false
	for {
		select {
		case <-receivedEventsChan:
			receivedEvents++
			if receivedEvents == numEvents {
				exit = true
			}
		case <-time.After(10 * time.Second):
			t.Log("timed out waiting for expected events")
			t.Fail()
			exit = true
		}
		if exit {
			break
		}
	}

	close(stopChan)
}

type mockDynamicClient struct {
	client dynamic.Interface
}

func (c mockDynamicClient) Resource(resource schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return c.client.Resource(resource)
}

func newMockDynamicClient() mockDynamicClient {
	scheme := runtime.NewScheme()
	objs := []runtime.Object{}

	gvrToListKind := map[schema.GroupVersionResource]string{
		{Group: "", Version: "v1", Resource: "pods"}: "PodList",
	}

	fakeClient := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind, objs...)
	return mockDynamicClient{
		client: fakeClient,
	}
}

func (c mockDynamicClient) createPods(objects ...*unstructured.Unstructured) {
	pods := c.client.Resource(schema.GroupVersionResource{
		Version:  "v1",
		Resource: "pods",
	})
	for _, pod := range objects {
		_, _ = pods.Namespace(pod.GetNamespace()).Create(context.Background(), pod, v1.CreateOptions{})
	}
}

func (c mockDynamicClient) deletePods(objects ...*unstructured.Unstructured) {
	pods := c.client.Resource(schema.GroupVersionResource{
		Version:  "v1",
		Resource: "pods",
	})
	for _, pod := range objects {
		_ = pods.Namespace(pod.GetNamespace()).Delete(context.Background(), pod.GetName(), v1.DeleteOptions{})
	}
}

// setListResourceVersion creates a new mock client with a custom List reactor
func (c *mockDynamicClient) setListResourceVersion(resourceVersion string) {
	scheme := runtime.NewScheme()
	gvrToListKind := map[schema.GroupVersionResource]string{
		{Group: "", Version: "v1", Resource: "pods"}: "PodList",
	}

	fakeClient := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind)

	// Add reactor to set resourceVersion on list operations
	fakeClient.PrependReactor("list", "*", func(_ k8s_testing.Action) (handled bool, ret runtime.Object, err error) {
		// Don't handle, let default action occur
		return false, nil, nil
	})

	fakeClient.PrependWatchReactor("*", func(_ k8s_testing.Action) (handled bool, ret apiWatch.Interface, err error) {
		// Don't handle, let default action occur
		return false, nil, nil
	})

	// Wrap to intercept List calls
	c.client = &listResourceVersionInterceptor{
		Interface:       fakeClient,
		resourceVersion: resourceVersion,
	}
}

// listResourceVersionInterceptor wraps a dynamic client to set resourceVersion on List results
type listResourceVersionInterceptor struct {
	dynamic.Interface
	resourceVersion string
}

func (l *listResourceVersionInterceptor) Resource(resource schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return &namespacedResourceInterceptor{
		NamespaceableResourceInterface: l.Interface.Resource(resource),
		resourceVersion:                l.resourceVersion,
	}
}

type namespacedResourceInterceptor struct {
	dynamic.NamespaceableResourceInterface
	resourceVersion string
}

func (n *namespacedResourceInterceptor) Namespace(ns string) dynamic.ResourceInterface {
	return &resourceInterceptor{
		ResourceInterface: n.NamespaceableResourceInterface.Namespace(ns),
		resourceVersion:   n.resourceVersion,
	}
}

func (n *namespacedResourceInterceptor) List(ctx context.Context, opts v1.ListOptions) (*unstructured.UnstructuredList, error) {
	list, err := n.NamespaceableResourceInterface.List(ctx, opts)
	if err == nil && list != nil {
		list.SetResourceVersion(n.resourceVersion)
	}
	return list, err
}

type resourceInterceptor struct {
	dynamic.ResourceInterface
	resourceVersion string
}

func (r *resourceInterceptor) List(ctx context.Context, opts v1.ListOptions) (*unstructured.UnstructuredList, error) {
	list, err := r.ResourceInterface.List(ctx, opts)
	if err == nil && list != nil {
		list.SetResourceVersion(r.resourceVersion)
	}
	return list, err
}

func generatePod(name, namespace string, labels map[string]any, resourceVersion string) *unstructured.Unstructured {
	pod := unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pods",
			"metadata": map[string]any{
				"namespace": namespace,
				"name":      name,
				"labels":    labels,
			},
		},
	}

	pod.SetResourceVersion(resourceVersion)
	return &pod
}
