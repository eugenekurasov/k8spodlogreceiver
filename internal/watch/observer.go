// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// This file is copied, with adaptations, from
// opentelemetry-collector-contrib's internal/k8sinventory/watch package:
//
//	https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/internal/k8sinventory/watch/observer.go
//
// That package lives under an `internal/` path, so Go's package visibility
// rules only allow it to be imported from within the opentelemetry-collector-contrib
// module tree — it cannot be imported from this repo. It is copied here
// instead of reimplemented, per Apache-2.0's permission to redistribute with
// attribution.
//
// Deliberate divergences from the upstream copy:
//
//   - The storage-extension checkpointer was dropped (see AGENTS.md for why a
//     committed discovery RV is unsafe for this receiver).
//   - The initial state is always emitted, and a watch restart (usually a
//     410 Gone) re-emits the full current state as synthetic Added events
//     instead of re-listing only for a fresh resourceVersion. Upstream
//     discards the listed objects, so anything created during the blind
//     window is never emitted; here that would mean a pod whose log stream
//     never starts. Consumers must handle Added idempotently.
//   - The unused knobs were removed with their code paths: the
//     IncludeInitialState flag (always on now), and the ResourceVersion /
//     FieldSelector config fields (with getResourceVersion and
//     fetchListResourceVersion, which only served them).
//

package watch // import "github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/watch"

import (
	"context"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	apiWatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/watch"

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/retry"
)

const (
	defaultResourceVersion = "1"

	// Backoff applied when the state re-List fails (a real error, e.g.
	// apiserver unreachable) — the routine 410 Gone restart itself retries
	// immediately.
	relistBackoffInitial = 1 * time.Second
	relistBackoffMax     = 30 * time.Second
)

type Config struct {
	Gvr           schema.GroupVersionResource
	Namespaces    []string
	LabelSelector string

	Exclude map[apiWatch.EventType]bool
}

type Observer struct {
	config Config

	client dynamic.Interface
	logger *zap.Logger

	handleWatchEventFunc func(event *apiWatch.Event)
}

func New(client dynamic.Interface, config Config, logger *zap.Logger, handleWatchEventFunc func(event *apiWatch.Event)) *Observer {
	return &Observer{
		client:               client,
		config:               config,
		logger:               logger,
		handleWatchEventFunc: handleWatchEventFunc,
	}
}

func (o *Observer) Start(ctx context.Context, wg *sync.WaitGroup) chan struct{} {
	resource := o.client.Resource(o.config.Gvr)
	o.logger.Info("Started collecting",
		zap.Any("gvr", o.config.Gvr),
		zap.Any("mode", "watch"),
		zap.Any("namespaces", o.config.Namespaces))

	stopperChan := make(chan struct{})

	if len(o.config.Namespaces) == 0 {
		wg.Add(1)
		go o.startWatch(ctx, resource, "", stopperChan, wg)
	} else {
		for _, ns := range o.config.Namespaces {
			wg.Add(1)
			go o.startWatch(ctx, resource.Namespace(ns), ns, stopperChan, wg)
		}
	}

	return stopperChan
}

func (o *Observer) startWatch(ctx context.Context, resource dynamic.ResourceInterface, namespace string, stopperChan chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	watchFunc := func(watchCtx context.Context, options metav1.ListOptions) (apiWatch.Interface, error) {
		options.LabelSelector = o.config.LabelSelector
		return resource.Watch(watchCtx, options)
	}

	cancelCtx, cancel := context.WithCancel(ctx)

	// initialListRV holds the list resourceVersion of the startup
	// sendInitialState; the first iteration reuses it as the watch starting
	// point, avoiding a second List() and the race window between two
	// listings. It is "" when that List failed — then the first iteration
	// falls into the re-List branch below like any restart.
	initialListRV := o.sendInitialState(ctx, resource, namespace)

	backoff := relistBackoffInitial

	wait.UntilWithContext(cancelCtx, func(newCtx context.Context) {
		var resourceVersion string
		if initialListRV != "" {
			resourceVersion = initialListRV
			initialListRV = ""
		} else {
			// Restarting after a broken watch — usually a 410 Gone, meaning
			// our resourceVersion was compacted away. The events missed while
			// blind are gone from etcd and can never be replayed, so re-List
			// and re-emit the current state as synthetic Added events instead
			// of only grabbing a fresh resourceVersion (see the divergence
			// note in the file header). Consumers must treat Added
			// idempotently.
			resourceVersion = o.sendInitialState(newCtx, resource, namespace)
			if resourceVersion == "" {
				// List failed (already logged); back off instead of
				// hot-looping on an unreachable apiserver.
				if !retry.SleepOrDone(newCtx, backoff) {
					cancel()
					return
				}
				backoff = retry.NextBackoff(backoff, relistBackoffMax)
				return
			}
		}
		backoff = relistBackoffInitial

		if done := o.doWatch(ctx, resourceVersion, watchFunc, stopperChan); done {
			cancel()
		}
	}, 0)
}

// sendInitialState sends the current state of objects as synthetic Added events
// and returns the list's own ResourceVersion, which the caller uses as the watch
// starting point to avoid a redundant List() call.
func (o *Observer) sendInitialState(ctx context.Context, resource dynamic.ResourceInterface, namespace string) string {
	o.logger.Info("sending initial state",
		zap.String("resource", o.config.Gvr.String()),
		zap.Strings("namespaces", o.config.Namespaces))

	listOption := metav1.ListOptions{
		LabelSelector: o.config.LabelSelector,
	}

	objects, err := resource.List(ctx, listOption)
	if err != nil {
		o.logger.Error("error in listing objects for initial state",
			zap.String("resource", o.config.Gvr.String()),
			zap.Error(err))
		return ""
	}

	listRV := objects.GetResourceVersion()
	if listRV == "" || listRV == "0" {
		// A watch cannot start from an empty resourceVersion, and callers
		// use "" to mean "the List itself failed".
		listRV = defaultResourceVersion
	}

	if len(objects.Items) == 0 {
		o.logger.Debug("no objects found for initial state",
			zap.String("resource", o.config.Gvr.String()))
		return listRV
	}

	for i := range objects.Items {
		if o.handleWatchEventFunc != nil {
			o.handleWatchEventFunc(&apiWatch.Event{
				Type:   apiWatch.Added,
				Object: &objects.Items[i],
			})
		}
	}

	o.logger.Info("initial state sent",
		zap.String("namespace", namespace),
		zap.String("list_rv", listRV),
		zap.String("resource", o.config.Gvr.String()),
		zap.Int("object_count", len(objects.Items)))
	return listRV
}

func (o *Observer) doWatch(ctx context.Context, resourceVersion string, watchFunc func(watchCtx context.Context, options metav1.ListOptions) (apiWatch.Interface, error), stopperChan chan struct{}) bool {
	watcher, err := watch.NewRetryWatcherWithContext(ctx, resourceVersion, &cache.ListWatch{
		WatchFuncWithContext: watchFunc,
	})
	if err != nil {
		o.logger.Error("error in watching object",
			zap.String("resource", o.config.Gvr.String()),
			zap.Error(err))
		return false
	}

	defer watcher.Stop()
	res := watcher.ResultChan()
	for {
		select {
		case data, ok := <-res:
			if data.Type == apiWatch.Error {
				errObject := apierrors.FromObject(data.Object)
				//nolint:errorlint
				if errObject.(*apierrors.StatusError).ErrStatus.Code == http.StatusGone {
					o.logger.Info("received a 410, grabbing new resource version",
						zap.Any("data", data))
					// we received a 410 so we need to restart
					return false
				}
			}

			if !ok {
				o.logger.Warn("Watch channel closed unexpectedly",
					zap.String("resource", o.config.Gvr.String()))
				return true
			}

			if o.config.Exclude[data.Type] {
				o.logger.Debug("dropping excluded data",
					zap.String("type", string(data.Type)))
				continue
			}

			if o.handleWatchEventFunc != nil {
				o.handleWatchEventFunc(&data)
			}

		case <-stopperChan:
			watcher.Stop()
			return true
		}
	}
}
