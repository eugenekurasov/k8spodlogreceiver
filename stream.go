// Copyright 2026 Yevhenii Kurasov
// SPDX-License-Identifier: Apache-2.0

package k8spodlogreceiver

import (
	"context"
	"errors"
	"io"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/eugenekurasov/k8spodlogreceiver/internal/logline"
	"github.com/eugenekurasov/k8spodlogreceiver/internal/metadata"
	"github.com/eugenekurasov/k8spodlogreceiver/internal/retry"
)

// streamLifetimeJitter is how much of maxStreamLifetime each connection may
// randomly give up, so that connections opened together drift apart instead of
// recycling in lockstep. 10% of the 1h default spreads a herd over ~6 minutes
// while keeping recycles rare.
const streamLifetimeJitter = 0.1

// containerStream owns the log-follow loop of a single container: open a
// stream, forward its lines to the pipeline, reconnect with backoff, and stop
// once the container is terminal or ctx is cancelled.
//
// It holds no reference back to the receiver; everything it needs is injected
// at construction (see logsReceiver.newContainerStream).
type containerStream struct {
	client    kubernetes.Interface
	telemetry *metadata.TelemetryBuilder
	logger    *zap.Logger
	meta      logline.Meta

	// sinceSeconds bounds the initial read; nil means the full log.
	sinceSeconds *int64
	backoffCfg   ReconnectBackoffConfig
	// maxStreamLifetime caps how long one connection is followed before it is
	// deliberately closed and reopened; 0 disables the cap.
	maxStreamLifetime time.Duration

	// consume forwards one open stream's lines to the pipeline and returns the
	// timestamp of the last delivered line (logsReceiver.streamConnection).
	consume func(ctx context.Context, stream io.Reader, m logline.Meta, onProgress func(time.Time)) (time.Time, error)
	// isTerminal reports whether this container has been marked terminated.
	isTerminal func() bool
	// onDelivered publishes the cursor so it outlives this stream: a
	// replacement started after a watch break resumes from it.
	onDelivered func(time.Time)

	// backoff is the wait before the next connect attempt; it grows while
	// connects fail and resets to InitialInterval once one succeeds.
	backoff time.Duration
	// resumeFrom is the timestamp of the last line delivered to the pipeline.
	// While zero the stream starts from sinceSeconds; afterwards each
	// reconnect resumes right after that line.
	resumeFrom time.Time
	// retryingSince is when the current run of failed connects began, and is
	// zero while connects succeed. Measured against MaxElapsedTime.
	retryingSince time.Time
	firstAttempt  bool
}

func (s *containerStream) run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		s.countReconnect(ctx)

		// connCtx bounds this one connection. ctx still bounds the stream as a
		// whole, so an expired connCtx means "recycle", not "stop".
		connCtx, connCancel := s.connectionContext(ctx)

		stream, err := s.open(connCtx)
		if err != nil {
			recycled := s.lifetimeReached(ctx, connCtx)
			connCancel()
			if recycled {
				continue
			}
			if !s.retryAfterConnectError(ctx, err) {
				return
			}
			continue
		}

		keepGoing := s.follow(ctx, connCtx, stream)
		recycled := s.lifetimeReached(ctx, connCtx)
		connCancel()

		if !keepGoing {
			return
		}
		if recycled {
			// A deliberate recycle, not a failure: reconnect at once rather
			// than waiting out a backoff earned by errors.
			s.logger.Debug("stream lifetime reached, reconnecting",
				zap.Duration("max_stream_lifetime", s.maxStreamLifetime),
				zap.Time("resume_from", s.resumeFrom),
			)
			continue
		}
		if !retry.SleepOrDone(ctx, retry.Jitter(s.backoff)) {
			return
		}
	}
}

// connectionContext derives the context for a single connection, applying
// maxStreamLifetime when it is set.
//
// The cap exists because a `pods/log?follow=true` connection can stop
// delivering while remaining open — the socket stays healthy, so nothing
// errors and no reconnect is triggered, and the stream goes silently mute.
// Recycling the connection on a fixed schedule bounds how long that can last.
//
// The cap is jittered down by up to streamLifetimeJitter so that streams
// started together do not all recycle at the same instant, hour after hour;
// see retry.Spread.
func (s *containerStream) connectionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.maxStreamLifetime <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, retry.Spread(s.maxStreamLifetime, streamLifetimeJitter))
}

// lifetimeReached reports whether this connection ended because its lifetime
// cap expired, as opposed to the stream as a whole being cancelled.
func (s *containerStream) lifetimeReached(ctx, connCtx context.Context) bool {
	return ctx.Err() == nil && errors.Is(connCtx.Err(), context.DeadlineExceeded)
}

// countReconnect counts every connect attempt except the very first one, which
// is the initial connection rather than a reconnect.
func (s *containerStream) countReconnect(ctx context.Context) {
	if s.firstAttempt {
		s.firstAttempt = false
		return
	}
	if s.telemetry != nil {
		s.telemetry.LogConnectionReconnectsTotal.Add(ctx, 1)
	}
}

func (s *containerStream) open(ctx context.Context) (io.ReadCloser, error) {
	opts := &corev1.PodLogOptions{
		Container:  s.meta.ContainerName,
		Follow:     true,
		Timestamps: true,
	}
	opts.SinceTime, opts.SinceSeconds = streamStartPoint(s.resumeFrom, s.sinceSeconds, time.Now())

	req := s.client.CoreV1().Pods(s.meta.Namespace).GetLogs(s.meta.PodName, opts)
	return req.Stream(ctx)
}

// streamStartPoint decides where a stream begins reading, as the pair of
// mutually exclusive PodLogOptions fields the API accepts.
//
// The sinceSeconds == 0 case is the subtle one. Config documents it as "fresh
// logs only, no historical backfill", but it cannot be passed through: the API
// server rejects it outright with
//
//	PodLogOptions is invalid: sinceSeconds: Invalid value: 0: must be greater than 0
//
// which would fail every connect attempt and deliver nothing. sinceTime=now is
// the equivalent the API does accept, so "no backfill" is expressed that way.
func streamStartPoint(resumeFrom time.Time, sinceSeconds *int64, now time.Time) (*metav1.Time, *int64) {
	switch {
	case !resumeFrom.IsZero():
		// Reconnect: resume just after the last line already delivered.
		t := metav1.NewTime(resumeFrom)
		return &t, nil
	case sinceSeconds != nil && *sinceSeconds == 0:
		t := metav1.NewTime(now)
		return &t, nil
	default:
		// A nil sinceSeconds leaves both unset: full retained history.
		return nil, sinceSeconds
	}
}

// retryAfterConnectError reports whether another connect should be attempted.
// It returns false when ctx is cancelled or when connects have been failing
// for longer than MaxElapsedTime (0 means retry indefinitely).
func (s *containerStream) retryAfterConnectError(ctx context.Context, err error) bool {
	if s.retryingSince.IsZero() {
		s.retryingSince = time.Now()
	}
	if s.telemetry != nil {
		s.telemetry.LogConnectionErrorsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", classifyStreamError(err))))
	}
	s.logger.Warn("log stream failed, will retry", zap.Error(err), zap.Duration("backoff", s.backoff))

	if s.backoffCfg.MaxElapsedTime > 0 && time.Since(s.retryingSince) > s.backoffCfg.MaxElapsedTime {
		s.logger.Info("max reconnect elapsed time exceeded, stopping stream", zap.Duration("max_elapsed_time", s.backoffCfg.MaxElapsedTime))
		return false
	}

	if !retry.SleepOrDone(ctx, retry.Jitter(s.backoff)) {
		return false
	}
	s.backoff = retry.NextBackoff(s.backoff, s.backoffCfg.MaxInterval)
	return true
}

// follow drains one open stream and reports whether the loop should reconnect.
// follow reads one open stream to completion. connCtx bounds the read itself
// and may expire early through maxStreamLifetime; ctx bounds the stream as a
// whole and is what the terminal drain below must use, so a lifetime cap that
// happens to expire as the container terminates does not cost the final lines.
func (s *containerStream) follow(ctx, connCtx context.Context, stream io.ReadCloser) bool {
	s.retryingSince = time.Time{}
	s.backoff = s.backoffCfg.InitialInterval

	lastTS, scanErr := s.consume(connCtx, stream, s.meta, s.onDelivered)
	_ = stream.Close()
	if !lastTS.IsZero() {
		s.resumeFrom = lastTS
		if s.onDelivered != nil {
			s.onDelivered(lastTS)
		}
	}

	switch {
	case errors.Is(scanErr, errPipelineRefused):
		s.logger.Warn("pipeline refused a batch, reconnecting to re-read it",
			zap.Time("resume_from", s.resumeFrom),
		)
	case scanErr != nil:
		s.logger.Debug("log stream ended, reconnecting", zap.Error(scanErr))
	}

	if !s.isTerminal() {
		return true
	}

	// The container is gone: if the stream broke rather than reaching EOF,
	// one non-follow read picks up whatever was written after the last
	// delivered line.
	if scanErr != nil {
		s.drainTerminalLogs(ctx)
	}
	s.logger.Debug("container terminated, stopping log stream")
	return false
}

// drainTerminalLogs does one non-follow read of a terminated container's logs
// to pick up lines written after resumeFrom that the broken stream missed.
func (s *containerStream) drainTerminalLogs(ctx context.Context) {
	opts := &corev1.PodLogOptions{
		Container:  s.meta.ContainerName,
		Follow:     false,
		Timestamps: true,
	}
	if !s.resumeFrom.IsZero() {
		t := metav1.NewTime(s.resumeFrom)
		opts.SinceTime = &t
	}

	req := s.client.CoreV1().Pods(s.meta.Namespace).GetLogs(s.meta.PodName, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		s.logger.Debug("final drain of terminal pod logs failed", zap.Error(err))
		return
	}
	defer func() { _ = stream.Close() }()

	lastTS, _ := s.consume(ctx, stream, s.meta, s.onDelivered)
	if !lastTS.IsZero() {
		s.resumeFrom = lastTS
	}
}
