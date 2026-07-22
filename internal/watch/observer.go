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

package watch // import "github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/watch"

import (
	"context"
	"errors"
	"fmt"
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

	// Backoff applied specifically when getResourceVersion fails (a real
	// error, e.g. apiserver unreachable) — not applied to the routine 410
	// Gone restart path, which must keep retrying immediately.
	getResourceVersionBackoffInitial = 1 * time.Second
	getResourceVersionBackoffMax     = 30 * time.Second
)

type Config struct {
	Gvr             schema.GroupVersionResource
	Namespaces      []string
	LabelSelector   string
	FieldSelector   string
	ResourceVersion string

	IncludeInitialState bool
	Exclude             map[apiWatch.EventType]bool
}

type Observer struct {
	config Config

	client dynamic.Interface
	logger *zap.Logger

	handleWatchEventFunc func(event *apiWatch.Event)
}

func New(client dynamic.Interface, config Config, logger *zap.Logger, handleWatchEventFunc func(event *apiWatch.Event)) (*Observer, error) {
	return &Observer{
		client:               client,
		config:               config,
		logger:               logger,
		handleWatchEventFunc: handleWatchEventFunc,
	}, nil
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
		options.FieldSelector = o.config.FieldSelector
		options.LabelSelector = o.config.LabelSelector
		return resource.Watch(watchCtx, options)
	}

	cancelCtx, cancel := context.WithCancel(ctx)

	// initialListRV holds the list resourceVersion returned by sendInitialState.
	// It is used as the watch starting point on the first iteration, eliminating
	// a second List() call and closing the race window between the two listings.
	// It is cleared after the first iteration so subsequent restarts (e.g. after
	// a 410 Gone) fall back to getResourceVersion() as normal.
	var initialListRV string
	if o.config.IncludeInitialState {
		initialListRV = o.sendInitialState(ctx, resource, namespace)
	}

	backoff := getResourceVersionBackoffInitial

	wait.UntilWithContext(cancelCtx, func(newCtx context.Context) {
		var resourceVersion string
		if initialListRV != "" {
			// First iteration: reuse the list RV from sendInitialState directly,
			// avoiding a redundant List() call and the race window it creates.
			resourceVersion = initialListRV
			initialListRV = ""
		} else {
			var err error
			resourceVersion, err = o.getResourceVersion(newCtx, resource)
			if err != nil {
				o.logger.Error("could not retrieve a resourceVersion, will retry",
					zap.String("resource", o.config.Gvr.String()),
					zap.String("namespace", namespace),
					zap.Duration("backoff", backoff),
					zap.Error(err))
				// A real error (e.g. apiserver unreachable), not a routine
				// 410 — back off instead of hot-looping or giving up.
				if !retry.SleepOrDone(newCtx, backoff) {
					cancel()
					return
				}
				backoff = retry.NextBackoff(backoff, getResourceVersionBackoffMax)
				return
			}
		}
		backoff = getResourceVersionBackoffInitial

		done := o.doWatch(ctx, resourceVersion, watchFunc, stopperChan)
		if done {
			cancel()
			return
		}

		// need to restart with a fresh resource version
		o.config.ResourceVersion = ""
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
		FieldSelector: o.config.FieldSelector,
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

func (o *Observer) fetchListResourceVersion(ctx context.Context, resource dynamic.ResourceInterface) (string, error) {
	objects, err := resource.List(ctx, metav1.ListOptions{
		FieldSelector: o.config.FieldSelector,
		LabelSelector: o.config.LabelSelector,
	})
	if err != nil {
		return "", fmt.Errorf("could not perform initial list for watch on %s, %w", o.config.Gvr.String(), err)
	}
	if objects == nil {
		return "", errors.New("nil objects returned, this is an error in the k8s observer")
	}

	listVersion := objects.GetResourceVersion()

	// If we still don't have a resourceVersion, use default
	if listVersion == "" || listVersion == "0" {
		listVersion = defaultResourceVersion
	}

	return listVersion, nil
}

func (o *Observer) getResourceVersion(ctx context.Context, resource dynamic.ResourceInterface) (string, error) {
	configVersion := o.config.ResourceVersion
	if configVersion != "" && configVersion != "0" {
		return configVersion, nil
	}
	return o.fetchListResourceVersion(ctx, resource)
}
