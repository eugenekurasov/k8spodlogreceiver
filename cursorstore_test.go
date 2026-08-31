// Copyright 2026 Yevhenii Kurasov
// SPDX-License-Identifier: Apache-2.0

package k8spodlogreceiver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/eugenekurasov/k8spodlogreceiver/internal/metadata"
)

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

func testCursorStore(t *testing.T, client storage.Client) *cursorStore {
	t.Helper()
	return newCursorStore(client, zap.NewNop())
}

// The point of persistence: a restarted collector resumes where it stopped
// rather than re-reading the backfill window for every container.
func TestCursors_SurviveARestart(t *testing.T) {
	persisted := newMemoryStorage()
	delivered := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	key := streamKey("payments", "api-0", "uid-1", "api")

	before := testCursorStore(t, persisted)
	before.advance(key, delivered)
	before.flush(context.Background())

	// A fresh process, same storage.
	after := testCursorStore(t, persisted)
	require.True(t, after.get(key).IsZero(), "a new store starts empty")
	after.load(context.Background())

	assert.Equal(t, delivered.UTC(), after.get(key).UTC(),
		"the restarted receiver must resume from the persisted cursor")
}

func TestCursors_NoStorageIsMemoryOnly(t *testing.T) {
	s := testCursorStore(t, nil)
	key := streamKey("ns", "pod", "uid", "c")
	s.advance(key, time.Now())

	assert.False(t, s.persists())
	require.NotPanics(t, func() {
		s.flush(context.Background())
		s.load(context.Background())
		require.NoError(t, s.close(context.Background()))
	}, "no storage configured must be a no-op, not a failure")
	assert.False(t, s.get(key).IsZero(), "cursors still work, they just do not survive a restart")
}

// Storage is best-effort: unreadable state must not stop the receiver, it just
// falls back to the configured backfill window.
func TestCursors_CorruptStateFallsBackToBackfill(t *testing.T) {
	persisted := newMemoryStorage()
	require.NoError(t, persisted.Set(context.Background(), cursorsStorageKey, []byte("{not json")))

	s := testCursorStore(t, persisted)
	require.NotPanics(t, func() { s.load(context.Background()) })
	assert.Zero(t, s.size(), "corrupt state is discarded rather than partially applied")
}

// A cursor only ever moves forward: a late write from a stream that is still
// unwinding must not rewind a position, and a line the kubelet handed over
// without a timestamp must not clear one.
func TestCursors_AdvanceOnlyMovesForward(t *testing.T) {
	s := testCursorStore(t, nil)
	key := streamKey("ns", "pod", "uid", "c")
	later := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)

	s.advance(key, later)
	s.advance(key, later.Add(-time.Hour))
	assert.Equal(t, later, s.get(key), "an out-of-order write must not rewind the cursor")

	s.advance(key, time.Time{})
	assert.Equal(t, later, s.get(key), "a zero timestamp must not clear the cursor")
}

func TestCursors_ForgetDropsOnlyThatContainer(t *testing.T) {
	s := testCursorStore(t, nil)
	gone := streamKey("ns", "pod", "uid", "gone")
	kept := streamKey("ns", "pod", "uid", "kept")
	now := time.Now()
	s.advance(gone, now)
	s.advance(kept, now)

	s.forget(gone)
	assert.True(t, s.get(gone).IsZero())
	assert.False(t, s.get(kept).IsZero())
}

// Expiry is what bounds the store: a collector that runs for months must not
// keep a cursor for every container that has ever existed on the node.
func TestCursors_PruneDropsOnlyLongUnseenPods(t *testing.T) {
	s := testCursorStore(t, nil)

	live := streamKey("ns", "live", "uid-live", "c")
	deadOne := streamKey("ns", "dead", "uid-dead", "app")
	deadTwo := streamKey("ns", "dead", "uid-dead", "sidecar")
	for _, key := range []string{live, deadOne, deadTwo} {
		s.advance(key, time.Now())
	}
	s.markPodSeen("uid-live")

	// uid-dead was last seen three hours ago; uid-live just now.
	s.mu.Lock()
	s.lastSeenPods["uid-dead"] = time.Now().Add(-3 * time.Hour)
	s.mu.Unlock()

	pruned := s.prune(time.Now().Add(-cursorStaleAfter))

	assert.ElementsMatch(t, []string{deadOne, deadTwo}, pruned,
		"every container of the expired pod must be reported, so the caller can drop its own state")
	assert.False(t, s.get(live).IsZero(), "a pod still being reported must keep its cursor")
	assert.True(t, s.get(deadOne).IsZero())
	assert.True(t, s.get(deadTwo).IsZero())

	s.mu.Lock()
	_, stillTracked := s.lastSeenPods["uid-dead"]
	s.mu.Unlock()
	assert.False(t, stillTracked, "the expired pod itself must be forgotten too")
}

// A cursor restored from storage belongs to a pod that may never be reported
// again — that is exactly the entry expiry exists for, and matching it back to
// its pod is pure string work on the key.
func TestCursors_PruneMatchesRestoredKeysRegardlessOfNameLengths(t *testing.T) {
	s := testCursorStore(t, nil)

	// Container names of different lengths: an offset-based match would only
	// line up for one of them.
	short := streamKey("ns", "pod", "uid-dead", "c")
	long := streamKey("ns", "pod", "uid-dead", "a-much-longer-container-name")
	s.advance(short, time.Now())
	s.advance(long, time.Now())

	s.mu.Lock()
	s.lastSeenPods["uid-dead"] = time.Now().Add(-3 * time.Hour)
	s.mu.Unlock()

	pruned := s.prune(time.Now().Add(-cursorStaleAfter))
	assert.ElementsMatch(t, []string{short, long}, pruned)
	assert.Zero(t, s.size())
}

func TestCursors_PruneOfNothingIsANoOp(t *testing.T) {
	s := testCursorStore(t, nil)
	key := streamKey("ns", "pod", "uid", "c")
	s.advance(key, time.Now())
	s.markPodSeen("uid")

	assert.Nil(t, s.prune(time.Now().Add(-cursorStaleAfter)))
	assert.False(t, s.get(key).IsZero())
}

func TestPodUIDFromStreamKey(t *testing.T) {
	assert.Equal(t, "uid-1", podUIDFromStreamKey(streamKey("ns", "pod", "uid-1", "c")))
	assert.Empty(t, podUIDFromStreamKey("not-a-key"), "anything not built by streamKey matches no pod")
	assert.Empty(t, podUIDFromStreamKey("ns/pod/uid/c/extra"))
}

// The receiver drops its own per-container state for exactly the keys the
// store expired, so the two cannot drift apart.
func TestPruneStaleCursors_ClearsReceiverStateForExpiredKeys(t *testing.T) {
	r := newTestReceiver()
	key := streamKey("ns", "pod", "uid-dead", "c")

	r.cursors.advance(key, time.Now())
	r.mu.Lock()
	r.restartCounts[key] = 3
	r.terminatedContainers[key] = struct{}{}
	r.drainedContainers[key] = struct{}{}
	r.mu.Unlock()

	r.cursors.mu.Lock()
	r.cursors.lastSeenPods["uid-dead"] = time.Now().Add(-3 * time.Hour)
	r.cursors.mu.Unlock()

	r.pruneStaleCursors()

	assert.True(t, r.cursorFor(key).IsZero())
	r.mu.Lock()
	defer r.mu.Unlock()
	assert.NotContains(t, r.restartCounts, key)
	assert.NotContains(t, r.terminatedContainers, key)
	assert.NotContains(t, r.drainedContainers, key)
}

func TestOpenCursorStorage_UnconfiguredExtensionIsAnError(t *testing.T) {
	id := component.MustNewID("file_storage")
	settings := receivertest.NewNopSettings(metadata.Type)

	_, err := openCursorStorage(context.Background(), &emptyHost{}, &id, settings.ID)
	require.Error(t, err, "naming a storage extension that is not configured must fail loudly at Start")
	assert.Contains(t, err.Error(), "not configured")
}

func TestOpenCursorStorage_NoneConfiguredReturnsNilClient(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	client, err := openCursorStorage(context.Background(), &emptyHost{}, nil, settings.ID)
	require.NoError(t, err)
	assert.Nil(t, client)
}

type emptyHost struct{}

func (h *emptyHost) GetExtensions() map[component.ID]component.Component { return nil }
