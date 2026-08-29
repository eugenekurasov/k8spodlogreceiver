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

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/logline"
	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/metadata"
	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/retry"
)

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

	// consume forwards one open stream's lines to the pipeline and returns the
	// timestamp of the last delivered line (logsReceiver.streamConnection).
	consume func(ctx context.Context, stream io.Reader, m logline.Meta) (time.Time, error)
	// isTerminal reports whether this container has been marked terminated.
	isTerminal func() bool

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

		stream, err := s.open(ctx)
		if err != nil {
			if !s.retryAfterConnectError(ctx, err) {
				return
			}
			continue
		}

		if !s.follow(ctx, stream) {
			return
		}
		if !retry.SleepOrDone(ctx, s.backoff) {
			return
		}
	}
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

	if !retry.SleepOrDone(ctx, s.backoff) {
		return false
	}
	s.backoff = retry.NextBackoff(s.backoff, s.backoffCfg.MaxInterval)
	return true
}

// follow drains one open stream and reports whether the loop should reconnect.
func (s *containerStream) follow(ctx context.Context, stream io.ReadCloser) bool {
	s.retryingSince = time.Time{}
	s.backoff = s.backoffCfg.InitialInterval

	lastTS, scanErr := s.consume(ctx, stream, s.meta)
	_ = stream.Close()
	if !lastTS.IsZero() {
		s.resumeFrom = lastTS
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

	lastTS, _ := s.consume(ctx, stream, s.meta)
	if !lastTS.IsZero() {
		s.resumeFrom = lastTS
	}
}
