// Copyright 2026 Yevhenii Kurasov
// SPDX-License-Identifier: Apache-2.0

package k8spodlogreceiver

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/eugenekurasov/k8spodlogreceiver/internal/logline"
)

func lifetimeStream(t *testing.T, lifetime time.Duration, consume func(context.Context, io.Reader, logline.Meta, func(time.Time)) (time.Time, error)) *containerStream {
	t.Helper()
	return &containerStream{
		client:            fake.NewSimpleClientset(),
		logger:            zap.NewNop(),
		meta:              logline.Meta{Namespace: "ns", PodName: "pod", PodUID: "uid", ContainerName: "c"},
		backoffCfg:        ReconnectBackoffConfig{InitialInterval: time.Millisecond, MaxInterval: time.Millisecond},
		maxStreamLifetime: lifetime,
		consume:           consume,
		isTerminal:        func() bool { return false },
		backoff:           time.Millisecond,
		firstAttempt:      true,
	}
}

// A connection that stays open but stops delivering must be recycled, so the
// receiver reconnects instead of following a mute stream forever.
func TestRun_RecyclesConnectionWhenLifetimeElapses(t *testing.T) {
	var mu sync.Mutex
	connections := 0

	s := lifetimeStream(t, 30*time.Millisecond, func(ctx context.Context, _ io.Reader, _ logline.Meta, _ func(time.Time)) (time.Time, error) {
		mu.Lock()
		connections++
		mu.Unlock()
		<-ctx.Done() // mute stream: never returns on its own
		return time.Time{}, ctx.Err()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	s.run(ctx)

	mu.Lock()
	defer mu.Unlock()
	assert.Greater(t, connections, 1, "a mute connection must be recycled, not followed indefinitely")
}

// Without a cap the same mute connection is followed until the stream itself is
// cancelled — this is what the cap exists to prevent.
func TestRun_WithoutLifetimeCapFollowsMuteConnection(t *testing.T) {
	var mu sync.Mutex
	connections := 0

	s := lifetimeStream(t, 0, func(ctx context.Context, _ io.Reader, _ logline.Meta, _ func(time.Time)) (time.Time, error) {
		mu.Lock()
		connections++
		mu.Unlock()
		<-ctx.Done()
		return time.Time{}, ctx.Err()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	s.run(ctx)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, connections, "with the cap disabled the connection is never recycled")
}

// A recycle must resume from the last delivered line, not re-read from the
// configured backfill window.
func TestRun_RecycleResumesFromCursor(t *testing.T) {
	delivered := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	var seenResume []time.Time

	// declared first so the consume closure can read the cursor it observes
	var s *containerStream
	s = lifetimeStream(t, 25*time.Millisecond, func(ctx context.Context, _ io.Reader, _ logline.Meta, _ func(time.Time)) (time.Time, error) {
		mu.Lock()
		seenResume = append(seenResume, s.resumeFrom)
		mu.Unlock()
		<-ctx.Done()
		return delivered, ctx.Err()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 130*time.Millisecond)
	defer cancel()
	s.run(ctx)

	mu.Lock()
	defer mu.Unlock()
	require.Greater(t, len(seenResume), 1, "expected at least one recycle")
	assert.True(t, seenResume[0].IsZero(), "first connection starts from the backfill window")
	for i, ts := range seenResume[1:] {
		assert.Equal(t, delivered, ts, "reconnect %d must resume from the last delivered line", i+1)
	}
}

func TestLifetimeReached_DistinguishesRecycleFromShutdown(t *testing.T) {
	s := &containerStream{}

	t.Run("deadline exceeded while the stream is live is a recycle", func(t *testing.T) {
		ctx := context.Background()
		connCtx, cancel := context.WithTimeout(ctx, time.Nanosecond)
		defer cancel()
		<-connCtx.Done()
		assert.True(t, s.lifetimeReached(ctx, connCtx))
	})

	t.Run("parent cancellation is a shutdown, not a recycle", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		connCtx, connCancel := context.WithTimeout(ctx, time.Hour)
		defer connCancel()
		cancel()
		<-connCtx.Done()
		assert.False(t, s.lifetimeReached(ctx, connCtx))
	})
}

func TestConnectionContext_CapDisabledHasNoDeadline(t *testing.T) {
	s := &containerStream{maxStreamLifetime: 0}
	connCtx, cancel := s.connectionContext(context.Background())
	defer cancel()
	_, hasDeadline := connCtx.Deadline()
	assert.False(t, hasDeadline)

	s2 := &containerStream{maxStreamLifetime: time.Hour}
	connCtx2, cancel2 := s2.connectionContext(context.Background())
	defer cancel2()
	_, hasDeadline2 := connCtx2.Deadline()
	assert.True(t, hasDeadline2)
}

// The lifetime cap must be jittered: without it every stream opened at the same
// moment — a receiver start, a node reboot, a rollout — recycles at the very
// same instant, every hour, and the API server sees a periodic burst of
// reconnects instead of a steady trickle.
func TestConnectionContext_JittersTheLifetimeCap(t *testing.T) {
	s := &containerStream{maxStreamLifetime: time.Hour}

	deadlines := make(map[time.Duration]struct{})
	for i := 0; i < 100; i++ {
		start := time.Now()
		connCtx, cancel := s.connectionContext(context.Background())
		deadline, ok := connCtx.Deadline()
		cancel()
		require.True(t, ok)

		remaining := deadline.Sub(start)
		assert.LessOrEqual(t, remaining, time.Hour, "the cap must remain an upper bound")
		assert.Greater(t, remaining, time.Hour-time.Duration(float64(time.Hour)*streamLifetimeJitter)-time.Second,
			"the jitter must stay small enough that recycles remain rare")
		deadlines[remaining.Round(time.Millisecond)] = struct{}{}
	}
	assert.Greater(t, len(deadlines), 1, "identical caps would recycle in lockstep")
}
