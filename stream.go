package k8spodlogreceiver

import (
	"bufio"
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/metadata"
	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/retry"
)

// streamContainerLogs tails a single container's logs via the API server
// (equivalent to `kubectl logs -f <pod> -c <container>`), converts each
// raw line into a plog.Logs record unchanged (timestamp prefix kept intact
// in the body), and forwards it to the next consumer in the pipeline.
// Reconnects with backoff on stream errors (rotation, container restart,
// transient API errors). On reconnect it resumes from the timestamp of the
// last line it actually processed (via SinceTime), rather than jumping to
// "now", so lines logged during the backoff window aren't silently
// dropped. This can occasionally re-deliver the last line seen before a
// disconnect (SinceTime's boundary is inclusive); that's intentional — a
// rare duplicate is preferable to a silent gap, and duplicates can be
// cleaned up downstream (e.g. logdedupprocessor).
func (r *logsReceiver) streamContainerLogs(ctx context.Context, namespace, podName, containerName, key string) {
	defer r.wg.Done()
	defer func() {
		r.mu.Lock()
		delete(r.activeStreams, key)
		r.mu.Unlock()
	}()

	logger := r.settings.Logger.With(
		zap.String("namespace", namespace),
		zap.String("pod", podName),
		zap.String("container", containerName),
	)

	backoff := r.cfg.ReconnectBackoff.InitialInterval
	sinceSeconds := r.cfg.SinceSeconds
	var sinceTime *metav1.Time
	var lastSeenTimestamp time.Time
	firstAttempt := true

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !firstAttempt && r.telemetry != nil {
			r.telemetry.StreamReconnectsTotal.Add(ctx, 1)
		}
		firstAttempt = false

		opts := &corev1.PodLogOptions{
			Container:  containerName,
			Follow:     true,
			Timestamps: true,
		}
		// SinceSeconds and SinceTime are mutually exclusive; once a
		// reconnect has a real bookmark (sinceTime), it takes over from
		// the initial config-provided SinceSeconds.
		if sinceTime != nil {
			opts.SinceTime = sinceTime
		} else {
			opts.SinceSeconds = sinceSeconds
		}

		req := r.clientset.CoreV1().Pods(namespace).GetLogs(podName, opts)
		stream, err := req.Stream(ctx)
		if err != nil {
			if r.telemetry != nil {
				r.telemetry.StreamErrorsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", classifyStreamError(err))))
			}
			logger.Warn("log stream failed, will retry", zap.Error(err), zap.Duration("backoff", backoff))
			if !retry.SleepOrDone(ctx, backoff) {
				return
			}
			backoff = retry.NextBackoff(backoff, r.cfg.ReconnectBackoff.MaxInterval)
			continue
		}

		// Successful (re)connect: reset backoff.
		backoff = r.cfg.ReconnectBackoff.InitialInterval

		scanner := bufio.NewScanner(stream)
		for scanner.Scan() {
			line := scanner.Text()
			ts := parseLeadingTimestamp(line)
			if !ts.IsZero() {
				lastSeenTimestamp = ts
			}
			r.emitLogLine(namespace, podName, containerName, line, ts)
		}
		_ = stream.Close()

		if err := scanner.Err(); err != nil {
			logger.Debug("log stream ended, reconnecting", zap.Error(err))
		}

		// Bookmark the last line we actually processed so the next
		// reconnect resumes from there instead of from "now". If no line
		// was ever parsed (e.g. the stream closed before producing any
		// output), leave the previous since* value in place rather than
		// advancing it.
		if !lastSeenTimestamp.IsZero() {
			t := metav1.NewTime(lastSeenTimestamp)
			sinceTime = &t
		}

		if !retry.SleepOrDone(ctx, backoff) {
			return
		}
	}
}

// classifyStreamError buckets a failed log-stream connection attempt into a
// coarse reason for the stream_errors_total metric — specifically so an RBAC
// misconfiguration (403, which affects every pod and needs an operator fix)
// is distinguishable from a pod that simply disappeared before the stream
// could be established (404, expected churn under normal scheduling).
func classifyStreamError(err error) string {
	switch {
	case apierrors.IsForbidden(err):
		return metadata.ReasonRBACDenied
	case apierrors.IsNotFound(err):
		return metadata.ReasonPodGone
	default:
		return metadata.ReasonOther
	}
}

// parseLeadingTimestamp extracts the RFC3339Nano timestamp Kubernetes
// prepends to each line when Timestamps: true is set, without modifying the
// line itself — the full raw line (timestamp prefix included) is kept as
// the emitted log body; this is only used for the reconnect bookmark and
// the record's structured Timestamp field. Returns the zero Time if the
// line doesn't start with a parseable timestamp (unexpected format, partial
// write, etc.), so the caller can fall back to wall-clock time.
func parseLeadingTimestamp(line string) time.Time {
	idx := strings.IndexByte(line, ' ')
	if idx < 0 {
		return time.Time{}
	}
	ts, err := time.Parse(time.RFC3339Nano, line[:idx])
	if err != nil {
		return time.Time{}
	}
	return ts
}

// emitLogLine converts a single log line (the full raw line as received,
// timestamp prefix included) into a plog.Logs record and forwards it to the
// next consumer. ts is the container's actual emission time (parsed from
// the log line); if it's the zero Time (unparseable or timestamps
// disabled), the current wall-clock time is used instead.
func (r *logsReceiver) emitLogLine(namespace, podName, containerName, line string, ts time.Time) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()

	res := rl.Resource()
	res.Attributes().PutStr("k8s.namespace.name", namespace)
	res.Attributes().PutStr("k8s.pod.name", podName)
	res.Attributes().PutStr("k8s.container.name", containerName)

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName(metadata.ScopeName)
	lr := sl.LogRecords().AppendEmpty()
	if ts.IsZero() {
		ts = time.Now()
	}
	lr.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	lr.Body().SetStr(line)

	consumeCtx := context.Background()
	if r.obsrep != nil {
		consumeCtx = r.obsrep.StartLogsOp(consumeCtx)
	}
	err := r.consumer.ConsumeLogs(consumeCtx, logs)
	if r.obsrep != nil {
		r.obsrep.EndLogsOp(consumeCtx, "k8s_podlog", 1, err)
	}
	if err != nil {
		r.settings.Logger.Error("failed to forward log record to pipeline", zap.Error(err))
	}
}
