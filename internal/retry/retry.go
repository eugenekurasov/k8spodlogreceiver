// Copyright 2026 Yevhenii Kurasov
// SPDX-License-Identifier: Apache-2.0

package retry

import (
	"context"
	"math/rand/v2"
	"time"
)

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

func NextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}

func Jitter(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(rand.Int64N(int64(half)))
}

func Spread(d time.Duration, frac float64) time.Duration {
	if d <= 0 || frac <= 0 {
		return d
	}
	if frac > 1 {
		frac = 1
	}
	span := int64(float64(d) * frac)
	if span <= 0 {
		return d
	}
	return d - time.Duration(rand.Int64N(span))
}
