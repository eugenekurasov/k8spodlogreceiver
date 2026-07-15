// Package retry provides the small backoff/sleep primitives shared between
// stream.go's log-reconnect loop and internal/watch/observer.go's
// object-watch retry loop.
//
// It deliberately does not provide a single "run this loop with backoff"
// wrapper: the two call sites disagree on what a "failed" attempt means.
// In stream.go, any reconnect failure is abnormal and should back off. In
// observer.go, a restart is triggered by a 410 Gone (the watch's
// resourceVersion expired) — routine, expected behavior for any long-lived
// watch, not a failure — so it must keep restarting with no delay. Forcing
// both through one "success/failure" abstraction would make a healthy,
// hours-old watch's routine 410 climb an exponential backoff for no reason.
// Sharing just the primitives below avoids that trap while still removing
// the duplication between the two retry loops.
package retry

import (
	"context"
	"time"
)

// SleepOrDone blocks for duration d and returns true, or returns false
// immediately if ctx is cancelled first.
func SleepOrDone(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// NextBackoff doubles current, capped at max.
func NextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}
