package k8sapilogreceiver

import (
	"bufio"
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	corev1 "k8s.io/api/core/v1"
)

// streamContainerLogs tails a single container's logs via the API server
// (equivalent to `kubectl logs -f <pod> -c <container>`), converts each
// line into a plog.Logs record, and forwards it to the next consumer in
// the pipeline. Reconnects with backoff on stream errors (rotation,
// container restart, transient API errors) using sinceSeconds to avoid
// re-reading already-processed lines where possible.
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
	var sinceSeconds *int64 = &r.cfg.SinceSeconds

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		opts := &corev1.PodLogOptions{
			Container:    containerName,
			Follow:       true,
			SinceSeconds: sinceSeconds,
			Timestamps:   true,
		}

		req := r.clientset.CoreV1().Pods(namespace).GetLogs(podName, opts)
		stream, err := req.Stream(ctx)
		if err != nil {
			logger.Warn("log stream failed, will retry", zap.Error(err), zap.Duration("backoff", backoff))
			if !sleepOrDone(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, r.cfg.ReconnectBackoff.MaxInterval)
			continue
		}

		// Successful (re)connect: reset backoff, and after the first
		// successful connection avoid re-requesting old history on
		// subsequent reconnects within this process lifetime.
		backoff = r.cfg.ReconnectBackoff.InitialInterval
		zero := int64(0)
		sinceSeconds = &zero

		scanner := bufio.NewScanner(stream)
		for scanner.Scan() {
			r.emitLogLine(namespace, podName, containerName, scanner.Text())
		}
		_ = stream.Close()

		if err := scanner.Err(); err != nil {
			logger.Debug("log stream ended, reconnecting", zap.Error(err))
		}

		if !sleepOrDone(ctx, backoff) {
			return
		}
	}
}

func (r *logsReceiver) emitLogLine(namespace, podName, containerName, line string) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()

	res := rl.Resource()
	res.Attributes().PutStr("k8s.namespace.name", namespace)
	res.Attributes().PutStr("k8s.pod.name", podName)
	res.Attributes().PutStr("k8s.container.name", containerName)

	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	lr.Body().SetStr(line)

	if err := r.consumer.ConsumeLogs(context.Background(), logs); err != nil {
		r.settings.Logger.Error("failed to forward log record to pipeline", zap.Error(err))
	}
}

// sleepOrDone blocks for duration d and returns true, or returns false
// immediately if ctx is cancelled first.
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// nextBackoff doubles current, capped at max.
func nextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}
