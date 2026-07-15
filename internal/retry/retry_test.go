package retry

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
