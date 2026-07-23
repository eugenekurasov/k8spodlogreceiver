package logline

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatch_AppendBuildsResourceAndRecords(t *testing.T) {
	b := NewBatch(Meta{
		Namespace:     "payments",
		PodName:       "app-abc",
		PodUID:        "abc-123-uid",
		ContainerName: "api",
	})

	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	b.Append("hello world", ts)
	require.Equal(t, 1, b.Count())

	logs := b.Logs()
	require.Equal(t, 1, logs.ResourceLogs().Len())
	rl := logs.ResourceLogs().At(0)

	attrs := rl.Resource().Attributes()
	ns, ok := attrs.Get("k8s.namespace.name")
	require.True(t, ok)
	assert.Equal(t, "payments", ns.Str())
	pod, _ := attrs.Get("k8s.pod.name")
	assert.Equal(t, "app-abc", pod.Str())
	uid, _ := attrs.Get("k8s.pod.uid")
	assert.Equal(t, "abc-123-uid", uid.Str())
	c, _ := attrs.Get("k8s.container.name")
	assert.Equal(t, "api", c.Str())

	rec := rl.ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, "hello world", rec.Body().Str())
	assert.True(t, rec.Timestamp().AsTime().Equal(ts))
}

// A zero timestamp is backfilled with wall-clock time so no record is emitted
// with an unset timestamp.
func TestBatch_AppendZeroTimestampBackfilled(t *testing.T) {
	b := NewBatch(Meta{})
	before := time.Now()
	b.Append("line", time.Time{})
	after := time.Now()

	rec := b.Logs().ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	got := rec.Timestamp().AsTime()
	assert.False(t, got.Before(before))
	assert.False(t, got.After(after))
}
