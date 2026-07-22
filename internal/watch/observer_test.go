// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// This file is copied, with adaptations, from
// opentelemetry-collector-contrib's internal/k8sinventory/watch package:
//
//	https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/internal/k8sinventory/watch/observer_test.go
//
// The checkpointer/storage-persistence tests were dropped because this copy
// removed the upstream checkpointer (see observer.go header). Config no longer
// embeds k8sinventory.Config (fields are flattened here), New no longer takes a
// storage.Client, and sendInitialState/getResourceVersion lost their
// checkpointer-only parameters — the tests below track those signatures.

package watch // import "github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/watch"

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	receivedEventsChan := make(chan *apiWatch.Event)

	obs, err := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	require.NoError(t, err)

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

	verifyReceivedEvents(t, 2, receivedEventsChan, stopChan)

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
		Namespaces:          []string{"default"},
		IncludeInitialState: true,
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs, err := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	require.NoError(t, err)

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
		Namespaces:          []string{"default"},
		IncludeInitialState: true,
		Exclude: map[apiWatch.EventType]bool{
			apiWatch.Deleted: true,
		},
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs, err := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	require.NoError(t, err)

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

	obs, err := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	require.NoError(t, err)

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

	obs, err := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	require.NoError(t, err)

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
		Namespaces:      []string{"default"},
		LabelSelector:   "environment=test",
		FieldSelector:   "",
		ResourceVersion: "",
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs, err := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	require.NoError(t, err)

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
		Namespaces:          []string{"default"},
		IncludeInitialState: true,
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs, err := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	require.NoError(t, err)

	wg := sync.WaitGroup{}

	// Unlike contrib (which cancels and gives up on a List error), this copy
	// backs off and retries getResourceVersion indefinitely. That retry loop is
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
		Namespaces:          []string{"default"},
		IncludeInitialState: true,
	}

	receivedEventsChan := make(chan *apiWatch.Event)

	obs, err := New(mockClient, cfg, zap.NewNop(), func(event *apiWatch.Event) {
		receivedEventsChan <- event
	})

	require.NoError(t, err)

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
func TestSendInitialStateReturnsListRV(t *testing.T) {
	mockClient := newMockDynamicClient()
	// Set list RV to "999", higher than any individual object RV below.
	mockClient.setListResourceVersion("999")
	mockClient.createPods(
		generatePod("pod1", "default", nil, "100"),
		generatePod("pod2", "default", nil, "200"),
	)

	cfg := Config{
		Gvr:                 schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Namespaces:          []string{"default"},
		IncludeInitialState: true,
	}

	obs, err := New(mockClient, cfg, zap.NewNop(), nil)
	require.NoError(t, err)

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

func TestGetResourceVersion(t *testing.T) {
	// The checkpointer/persistence source was removed in this copy, so only the
	// config-resource-version and list-derived paths remain.

	t.Run("with config resource version", func(t *testing.T) {
		tests := []struct {
			name            string
			configVersion   string
			listVersion     string
			expectedVersion string
		}{
			{
				name:            "config RV set - use it",
				configVersion:   "150",
				listVersion:     "100",
				expectedVersion: "150",
			},
			{
				name:            "config RV is zero - fall back to list",
				configVersion:   "0",
				listVersion:     "100",
				expectedVersion: "100",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				mockClient := newMockDynamicClient()
				if tt.listVersion != "" {
					mockClient.setListResourceVersion(tt.listVersion)
				}

				cfg := Config{
					Gvr:             schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
					Namespaces:      []string{"default"},
					ResourceVersion: tt.configVersion,
				}

				obs, err := New(mockClient, cfg, zap.NewNop(), nil)
				require.NoError(t, err)

				resource := mockClient.Resource(cfg.Gvr)
				version, err := obs.getResourceVersion(t.Context(), resource.Namespace("default"))
				require.NoError(t, err)
				assert.Equal(t, tt.expectedVersion, version)
			})
		}
	})

	t.Run("from list (no config)", func(t *testing.T) {
		tests := []struct {
			name            string
			listVersion     string
			expectedVersion string
		}{
			{
				name:            "list version returned",
				listVersion:     "100",
				expectedVersion: "100",
			},
			{
				name:            "empty list version - use default",
				listVersion:     "",
				expectedVersion: "1",
			},
			{
				name:            "zero list version - use default",
				listVersion:     "0",
				expectedVersion: "1",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				mockClient := newMockDynamicClient()
				if tt.listVersion != "" {
					mockClient.setListResourceVersion(tt.listVersion)
				}

				cfg := Config{
					Gvr:        schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
					Namespaces: []string{"default"},
				}

				obs, err := New(mockClient, cfg, zap.NewNop(), nil)
				require.NoError(t, err)

				resource := mockClient.Resource(cfg.Gvr)
				version, err := obs.getResourceVersion(t.Context(), resource.Namespace("default"))
				require.NoError(t, err)
				assert.Equal(t, tt.expectedVersion, version)
			})
		}
	})
}
