// Copyright 2026 Yevhenii Kurasov
// SPDX-License-Identifier: Apache-2.0

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

	// Deliberately literal, not semconv constants: these names are the wire
	// contract every downstream consumer matches on, so bumping the semconv
	// import must fail here rather than silently rename what is emitted.
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

// The full attribute set, including the two the test above does not cover, and
// the schema URL that says which version of the conventions they follow.
func TestBatch_ResourceCarriesFullSemanticConventions(t *testing.T) {
	b := NewBatch(Meta{
		Namespace:     "payments",
		PodName:       "app-abc",
		PodUID:        "abc-123-uid",
		ContainerName: "api",
		NodeName:      "node-7",
		RestartCount:  4,
	})

	rl := b.Logs().ResourceLogs().At(0)
	assert.Equal(t, "https://opentelemetry.io/schemas/1.43.0", rl.SchemaUrl(),
		"records must declare the conventions version they follow")

	attrs := rl.Resource().Attributes()
	node, ok := attrs.Get("k8s.node.name")
	require.True(t, ok)
	assert.Equal(t, "node-7", node.Str())

	restarts, ok := attrs.Get("k8s.container.restart_count")
	require.True(t, ok)
	assert.Equal(t, int64(4), restarts.Int())

	assert.Equal(t, 6, attrs.Len(), "no attribute may be added without documenting it in the README")
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
