// Copyright 2026 Yevhenii Kurasov
// SPDX-License-Identifier: Apache-2.0

package cursor

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.uber.org/zap"
)

var receiverID = component.MustNewID("k8s_pod_log")

// testKey and testPodUIDOf stand in for the receiver's key layout. The store
// treats keys as opaque and asks podUIDOf for the one thing expiry needs.
func testKey(podUID, container string) string { return podUID + "/" + container }

func testPodUIDOf(key string) string {
	podUID, _, _ := strings.Cut(key, "/")
	return podUID
}

// memoryStorage is a storage.Client backed by a map, standing in for a real
// storage extension such as file_storage.
type memoryStorage struct {
	mu     sync.Mutex
	data   map[string][]byte
	closed bool
}

func newMemoryStorage() *memoryStorage {
	return &memoryStorage{data: map[string][]byte{}}
}

func (m *memoryStorage) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[key], nil
}

func (m *memoryStorage) Set(_ context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
	return nil
}

func (m *memoryStorage) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *memoryStorage) Batch(_ context.Context, _ ...*storage.Operation) error { return nil }

func (m *memoryStorage) Close(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func testStore(t *testing.T, client storage.Client) *Store {
	t.Helper()
	return NewStore(client, zap.NewNop(), testPodUIDOf)
}

// The point of persistence: a restarted collector resumes where it stopped
// rather than re-reading the backfill window for every container.
func TestCursors_SurviveARestart(t *testing.T) {
	persisted := newMemoryStorage()
	delivered := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	key := testKey("uid-1", "api")

	before := testStore(t, persisted)
	before.Advance(key, Cursor{TS: delivered})
	before.Flush(context.Background())

	// A fresh process, same storage.
	after := testStore(t, persisted)
	require.True(t, after.Get(key).IsZero(), "a new store starts empty")
	after.Load(context.Background())

	assert.Equal(t, delivered.UTC(), after.Get(key).TS.UTC(),
		"the restarted receiver must resume from the persisted cursor")
}

// The stored format gained a record count; the values an older version wrote
// must still restore, or an upgrade throws the whole set away and every
// container re-reads its backfill window at once.
func TestCursors_RestoreTheTimestampOnlyFormatOfOlderVersions(t *testing.T) {
	persisted := newMemoryStorage()
	key := testKey("uid-1", "api")
	require.NoError(t, persisted.Set(context.Background(), storageKey,
		[]byte(`{"`+key+`":"2026-08-30T12:00:00.5Z"}`)))

	s := testStore(t, persisted)
	s.Load(context.Background())

	got := s.Get(key)
	assert.Equal(t, time.Date(2026, 8, 30, 12, 0, 0, 500000000, time.UTC), got.TS.UTC())
	assert.Zero(t, got.Delivered,
		"nothing is known about that second, so it is re-read once rather than skipped")
}

// A cursor written now must come back with both halves, or every restart
// replays the second the count exists to trim.
func TestCursors_PersistTheRecordCountToo(t *testing.T) {
	persisted := newMemoryStorage()
	key := testKey("uid-1", "api")
	want := Cursor{TS: time.Date(2026, 8, 30, 12, 0, 0, 500000000, time.UTC), Delivered: 3}

	before := testStore(t, persisted)
	before.Advance(key, want)
	before.Flush(context.Background())

	after := testStore(t, persisted)
	after.Load(context.Background())

	got := after.Get(key)
	assert.True(t, want.TS.Equal(got.TS))
	assert.Equal(t, want.Delivered, got.Delivered)
}

func TestCursors_NoStorageIsMemoryOnly(t *testing.T) {
	s := testStore(t, nil)
	key := testKey("uid", "c")
	s.Advance(key, Cursor{TS: time.Now()})

	assert.False(t, s.Persists())
	require.NotPanics(t, func() {
		s.Flush(context.Background())
		s.Load(context.Background())
		require.NoError(t, s.Close(context.Background()))
	}, "no storage configured must be a no-op, not a failure")
	assert.False(t, s.Get(key).IsZero(), "cursors still work, they just do not survive a restart")
}

// Storage is best-effort: unreadable state must not stop the receiver, it just
// falls back to the configured backfill window.
func TestCursors_CorruptStateFallsBackToBackfill(t *testing.T) {
	persisted := newMemoryStorage()
	require.NoError(t, persisted.Set(context.Background(), storageKey, []byte("{not json")))

	s := testStore(t, persisted)
	require.NotPanics(t, func() { s.Load(context.Background()) })
	assert.Zero(t, s.Size(), "corrupt state is discarded rather than partially applied")
}

// A cursor only ever moves forward: a late write from a stream that is still
// unwinding must not rewind a position, and a line the kubelet handed over
// without a timestamp must not clear one.
func TestCursors_AdvanceOnlyMovesForward(t *testing.T) {
	s := testStore(t, nil)
	key := testKey("uid", "c")
	later := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)

	s.Advance(key, Cursor{TS: later})
	s.Advance(key, Cursor{TS: later.Add(-time.Hour)})
	assert.Equal(t, later, s.Get(key).TS, "an out-of-order write must not rewind the cursor")

	s.Advance(key, Cursor{TS: time.Time{}})
	assert.Equal(t, later, s.Get(key).TS, "a zero timestamp must not clear the cursor")
}

func TestCursors_ForgetDropsOnlyThatContainer(t *testing.T) {
	s := testStore(t, nil)
	gone := testKey("uid", "gone")
	kept := testKey("uid", "kept")
	now := time.Now()
	s.Advance(gone, Cursor{TS: now})
	s.Advance(kept, Cursor{TS: now})

	s.Forget(gone)
	assert.True(t, s.Get(gone).IsZero())
	assert.False(t, s.Get(kept).IsZero())
}

// Expiry is what bounds the store: a collector that runs for months must not
// keep a cursor for every container that has ever existed on the node.
func TestCursors_PruneDropsOnlyLongUnseenPods(t *testing.T) {
	s := testStore(t, nil)

	live := testKey("uid-live", "c")
	deadOne := testKey("uid-dead", "app")
	deadTwo := testKey("uid-dead", "sidecar")
	for _, key := range []string{live, deadOne, deadTwo} {
		s.Advance(key, Cursor{TS: time.Now()})
	}
	s.MarkPodSeen("uid-live")

	// uid-dead was last seen three hours ago; uid-live just now.
	s.mu.Lock()
	s.lastSeenPods["uid-dead"] = time.Now().Add(-3 * time.Hour)
	s.mu.Unlock()

	pruned := s.Prune(time.Now().Add(-StaleAfter))

	assert.ElementsMatch(t, []string{deadOne, deadTwo}, pruned,
		"every container of the expired pod must be reported, so the caller can drop its own state")
	assert.False(t, s.Get(live).IsZero(), "a pod still being reported must keep its cursor")
	assert.True(t, s.Get(deadOne).IsZero())
	assert.True(t, s.Get(deadTwo).IsZero())

	s.mu.Lock()
	_, stillTracked := s.lastSeenPods["uid-dead"]
	s.mu.Unlock()
	assert.False(t, stillTracked, "the expired pod itself must be forgotten too")
}

func TestCursors_PruneOfNothingIsANoOp(t *testing.T) {
	s := testStore(t, nil)
	key := testKey("uid", "c")
	s.Advance(key, Cursor{TS: time.Now()})
	s.MarkPodSeen("uid")

	assert.Nil(t, s.Prune(time.Now().Add(-StaleAfter)))
	assert.False(t, s.Get(key).IsZero())
}

func TestOpenCursorStorage_UnconfiguredExtensionIsAnError(t *testing.T) {
	id := component.MustNewID("file_storage")

	_, err := OpenStorage(context.Background(), &emptyHost{}, &id, receiverID)
	require.Error(t, err, "naming a storage extension that is not configured must fail loudly at Start")
	assert.Contains(t, err.Error(), "not configured")
}

func TestOpenCursorStorage_NoneConfiguredReturnsNilClient(t *testing.T) {
	client, err := OpenStorage(context.Background(), &emptyHost{}, nil, receiverID)
	require.NoError(t, err)
	assert.Nil(t, client)
}

type emptyHost struct{}

func (h *emptyHost) GetExtensions() map[component.ID]component.Component { return nil }
