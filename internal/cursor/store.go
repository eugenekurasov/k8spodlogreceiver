// Copyright 2026 Yevhenii Kurasov
// SPDX-License-Identifier: Apache-2.0

package cursor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.uber.org/zap"
)

// storageKey is the single key the whole cursor set is stored under.
//
// One blob rather than a key per container: the storage.Client interface has
// no way to enumerate keys, so per-container keys could be written but never
// read back without a separate index. The set is small — one position per
// container — so a single value stays well inside what a storage extension is
// built for.
const storageKey = "cursors"

// flushInterval is how often cursors are written out while running. The
// cost of losing up to this much progress on an abrupt kill is a re-read of
// that window, which is bounded and produces duplicates rather than gaps.
const flushInterval = 30 * time.Second

// StaleAfter is how long a pod may go unseen before its cursors are
// dropped. It is far longer than any resync period, so a pod that still exists
// is always refreshed well before it expires.
const StaleAfter = 2 * time.Hour

// PruneInterval is how often expiry is checked.
const PruneInterval = 1 * time.Minute

// Store is the set of "how far we have read" positions, one per container,
// plus their persistence and expiry. See Cursor for what a position is.
//
// It exists so a stream that is restarted resumes where its predecessor
// stopped rather than re-reading SinceSeconds. That is why it outlives the
// streams themselves: an informer that loses its watch reports every pod as
// deleted-then-added, and without this each container would re-read its whole
// backfill window at once.
//
// It holds no reference back to the receiver — the dependency runs one way.
// The receiver reads and advances cursors through it; nothing here reaches
// back. In particular the store knows nothing about stream generations:
// fencing a write against a replaced stream is the receiver's business, and it
// does that before calling Advance. Keys are the receiver's too: they are
// opaque here, and podUIDOf is the one thing the store needs to know about
// them.
type Store struct {
	// client persists the set across collector restarts. A nil client keeps
	// cursors in memory only, which is the whole behaviour of an unconfigured
	// storage extension: every method below degrades to a no-op.
	client storage.Client
	logger *zap.Logger
	// podUIDOf recovers the pod a key belongs to, which is what expiry is
	// measured against. It must return "" for a key it does not recognise,
	// which then never expires.
	podUIDOf func(key string) string

	mu      sync.Mutex
	cursors map[string]Cursor
	// lastSeenPods is when each pod UID was last reported by the informer. It
	// is what expiry is measured against: cursors are keyed per container, but
	// pods are what actually come and go.
	lastSeenPods map[string]time.Time
}

// NewStore returns an empty store. A nil client keeps cursors in memory only.
func NewStore(client storage.Client, logger *zap.Logger, podUIDOf func(key string) string) *Store {
	return &Store{
		client:       client,
		logger:       logger,
		podUIDOf:     podUIDOf,
		cursors:      make(map[string]Cursor),
		lastSeenPods: make(map[string]time.Time),
	}
}

// Persists reports whether cursors survive a collector restart.
func (s *Store) Persists() bool { return s.client != nil }

// Get returns how far a container has been read, the zero cursor if it has
// never been read at all.
func (s *Store) Get(key string) Cursor {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursors[key]
}

// Advance records how far a container has been read. It only ever moves a
// cursor forward — including within a single timestamp, where the record
// count is what moves — so an out-of-order write cannot rewind one, and a
// zero position, which is what a line the kubelet gave us without a timestamp
// produces, is ignored rather than clearing the position.
func (s *Store) Advance(key string, c Cursor) {
	if c.IsZero() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.After(s.cursors[key]) {
		s.cursors[key] = c
	}
}

// Forget drops a container's cursor, for a pod that is gone for good.
func (s *Store) Forget(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.cursors, key)
}

// MarkPodSeen records that a pod still exists, deferring expiry of its
// containers' cursors.
func (s *Store) MarkPodSeen(podUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSeenPods[podUID] = time.Now()
}

// Prune drops the cursors of every pod not seen since cutoff and returns the
// keys it removed, so the caller can drop whatever else it keys the same way.
//
// Expiry is what bounds the store. Without it a long-lived collector
// accumulates a cursor for every container that has ever run on the node —
// including the ones restored from storage, whose pods may have been deleted
// while the collector was down and are therefore never reported again.
func (s *Store) Prune(cutoff time.Time) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	stale := make(map[string]struct{})
	for podUID, lastSeen := range s.lastSeenPods {
		if lastSeen.Before(cutoff) {
			stale[podUID] = struct{}{}
			delete(s.lastSeenPods, podUID)
		}
	}
	if len(stale) == 0 {
		return nil
	}

	var pruned []string
	for key := range s.cursors {
		if _, ok := stale[s.podUIDOf(key)]; ok {
			delete(s.cursors, key)
			pruned = append(pruned, key)
		}
	}

	if len(pruned) > 0 {
		s.logger.Debug("pruned stale cursors",
			zap.Int("cursor_entries_removed", len(pruned)),
			zap.Int("stale_pod_uids", len(stale)))
	}
	return pruned
}

// Load seeds the in-memory cursors from storage. A missing or corrupt value is
// not fatal: the receiver starts from the configured backfill window instead,
// which is what it would do without persistence at all.
func (s *Store) Load(ctx context.Context) {
	if s.client == nil {
		return
	}
	raw, err := s.client.Get(ctx, storageKey)
	if err != nil {
		s.logger.Warn("could not read stored cursors, starting from since_seconds", zap.Error(err))
		return
	}
	if len(raw) == 0 {
		return
	}

	stored := map[string]Cursor{}
	if err := json.Unmarshal(raw, &stored); err != nil {
		s.logger.Warn("stored cursors are unreadable, starting from since_seconds", zap.Error(err))
		return
	}

	s.mu.Lock()
	for k, v := range stored {
		s.cursors[k] = v
	}
	restored := len(s.cursors)
	s.mu.Unlock()
	s.logger.Info("restored log cursors from storage", zap.Int("containers", restored))
}

// Flush writes the current cursor set out.
func (s *Store) Flush(ctx context.Context) {
	if s.client == nil {
		return
	}
	s.mu.Lock()
	snapshot := make(map[string]Cursor, len(s.cursors))
	for k, v := range s.cursors {
		snapshot[k] = v
	}
	s.mu.Unlock()

	raw, err := json.Marshal(snapshot)
	if err != nil {
		s.logger.Warn("could not encode cursors", zap.Error(err))
		return
	}
	if err := s.client.Set(ctx, storageKey, raw); err != nil {
		s.logger.Warn("could not persist cursors", zap.Error(err))
	}
}

// RunFlushLoop persists cursors periodically until ctx is cancelled. The final
// write is done at shutdown by the caller, not here, so it still happens when
// the collector stops.
func (s *Store) RunFlushLoop(ctx context.Context) {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Flush(ctx)
		}
	}
}

// Close releases the storage client, if any.
func (s *Store) Close(ctx context.Context) error {
	if s.client == nil {
		return nil
	}
	return s.client.Close(ctx)
}

// Size reports how many cursors are held, for logging and tests.
func (s *Store) Size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.cursors)
}

// OpenStorage resolves the configured storage extension and returns a
// client for it. It returns a nil client when no storage is configured, which
// leaves cursors in memory only.
func OpenStorage(ctx context.Context, host component.Host, storageID *component.ID, receiverID component.ID) (storage.Client, error) {
	if storageID == nil {
		return nil, nil
	}

	ext, ok := host.GetExtensions()[*storageID]
	if !ok {
		return nil, fmt.Errorf("storage extension %q is not configured on this collector", storageID)
	}
	storageExt, ok := ext.(storage.Extension)
	if !ok {
		return nil, fmt.Errorf("extension %q is not a storage extension", storageID)
	}
	return storageExt.GetClient(ctx, component.KindReceiver, receiverID, "")
}
