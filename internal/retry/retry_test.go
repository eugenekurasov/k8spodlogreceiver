// Copyright 2026 Yevhenii Kurasov
// SPDX-License-Identifier: Apache-2.0

package retry

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNextBackoff_Doubles(t *testing.T) {
	assert.Equal(t, 2*time.Second, NextBackoff(1*time.Second, 30*time.Second))
	assert.Equal(t, 4*time.Second, NextBackoff(2*time.Second, 30*time.Second))
}

func TestNextBackoff_CapsAtMax(t *testing.T) {
	assert.Equal(t, 30*time.Second, NextBackoff(20*time.Second, 30*time.Second))
	assert.Equal(t, 30*time.Second, NextBackoff(30*time.Second, 30*time.Second))
	assert.Equal(t, 30*time.Second, NextBackoff(100*time.Second, 30*time.Second))
}

func TestSleepOrDone_WaitsFullDuration(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	result := SleepOrDone(ctx, 50*time.Millisecond)
	assert.True(t, result, "should return true when timer fires")
	assert.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond)
}

func TestSleepOrDone_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	result := SleepOrDone(ctx, 10*time.Second)
	assert.False(t, result, "should return false on cancelled context")
}

func TestSleepOrDone_CancelDuringWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	result := SleepOrDone(ctx, 10*time.Second)
	assert.False(t, result)
	assert.Less(t, time.Since(start), 2*time.Second, "should unblock quickly on cancel")
}

func TestJitter_StaysWithinTheUpperHalfOfTheInterval(t *testing.T) {
	const d = 8 * time.Second
	for i := 0; i < 2000; i++ {
		got := Jitter(d)
		require.GreaterOrEqual(t, got, d/2, "jitter must not collapse the backoff to near zero")
		require.Less(t, got, d, "jitter must not exceed the interval it spreads")
	}
}

// The point of jitter: streams that fail together must not retry together.
func TestJitter_SpreadsRetriesAcrossTheInterval(t *testing.T) {
	const d = 8 * time.Second
	buckets := map[int]int{}
	for i := 0; i < 2000; i++ {
		// which eighth of the interval did we land in
		buckets[int(Jitter(d)*8/d)]++
	}
	assert.GreaterOrEqual(t, len(buckets), 3,
		"a lockstep implementation would put every retry in one bucket, got %v", buckets)
}

func TestJitter_DegenerateIntervalsAreSafe(t *testing.T) {
	assert.Equal(t, time.Duration(0), Jitter(0))
	assert.Equal(t, time.Duration(1), Jitter(1), "an interval too small to halve is returned as-is")
	assert.Equal(t, -time.Second, Jitter(-time.Second))
}

// Many streams back off at once, so the shared source must be safe to call
// concurrently. Run with -race.
func TestJitter_IsSafeUnderConcurrency(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = Jitter(time.Second)
			}
		}()
	}
	wg.Wait()
}

// The ladder itself must stay deterministic: it is carried between attempts,
// so randomising it would compound.
func TestNextBackoff_IsDeterministic(t *testing.T) {
	for i := 0; i < 100; i++ {
		assert.Equal(t, 2*time.Second, NextBackoff(time.Second, time.Minute))
	}
}

// Spread must stay inside [d*(1-frac), d]: the value it returns is used as a
// deadline that is documented as a cap, so it may only ever be shortened.
func TestSpread_StaysWithinBoundsAndNeverExceedsD(t *testing.T) {
	const d = time.Hour
	for i := 0; i < 1000; i++ {
		got := Spread(d, 0.1)
		assert.LessOrEqual(t, got, d, "the cap must remain an upper bound")
		assert.Greater(t, got, d-time.Duration(float64(d)*0.1)-time.Nanosecond)
	}
}

// The point of spreading is desynchronisation: identical inputs must not
// produce the same deadline for every stream.
func TestSpread_ProducesDistinctValues(t *testing.T) {
	seen := make(map[time.Duration]struct{})
	for i := 0; i < 100; i++ {
		seen[Spread(time.Hour, 0.1)] = struct{}{}
	}
	assert.Greater(t, len(seen), 1, "every stream would recycle in lockstep")
}

func TestSpread_DegenerateInputsAreSafe(t *testing.T) {
	assert.Equal(t, time.Duration(0), Spread(0, 0.1))
	assert.Equal(t, -time.Second, Spread(-time.Second, 0.1))
	assert.Equal(t, time.Hour, Spread(time.Hour, 0), "no fraction means no spread")
	assert.Equal(t, time.Hour, Spread(time.Hour, -1))
	// A fraction that rounds down to a zero-width span leaves d untouched.
	assert.Equal(t, time.Duration(1), Spread(1, 0.1))
	// frac is clamped, so the result stays non-negative.
	assert.GreaterOrEqual(t, Spread(time.Hour, 5), time.Duration(0))
}
