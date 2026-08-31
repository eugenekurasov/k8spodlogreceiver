// Copyright 2026 Yevhenii Kurasov
// SPDX-License-Identifier: Apache-2.0

package k8spodlogreceiver

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

// cursorsStorageKey is the single key the whole cursor set is stored under.
//
// One blob rather than a key per container: the storage.Client interface has
// no way to enumerate keys, so per-container keys could be written but never
// read back without a separate index. The set is small — one timestamp per
// container — so a single value stays well inside what a storage extension is
// built for.
const cursorsStorageKey = "cursors"

// cursorFlushInterval is how often cursors are written out while running. The
// cost of losing up to this much progress on an abrupt kill is a re-read of
// that window, which is bounded and produces duplicates rather than gaps.
const cursorFlushInterval = 30 * time.Second

// cursorStaleAfter is how long a pod may go unseen before its cursors are
// dropped. It is far longer than any resync period, so a pod that still exists
// is always refreshed well before it expires.
const cursorStaleAfter = 2 * time.Hour

// cursorPruneInterval is how often expiry is checked.
const cursorPruneInterval = 1 * time.Minute

// cursorStore is the set of "last line delivered" timestamps, one per
// container, plus their persistence and expiry.
//
// It exists so a stream that is restarted resumes where its predecessor
// stopped rather than re-reading SinceSeconds. That is why it outlives the
// streams themselves: an informer that loses its watch reports every pod as
// deleted-then-added, and without this each container would re-read its whole
// backfill window at once.
//
// Like containerStream, it holds no reference back to the receiver — the
// dependency runs one way. The receiver reads and advances cursors through it;
// nothing here reaches back. In particular the store knows nothing about
// stream generations: fencing a write against a replaced stream is the
// receiver's business, and it does that before calling advance.
type cursorStore struct {
	// client persists the set across collector restarts. A nil client keeps
	// cursors in memory only, which is the whole behaviour of an unconfigured
	// storage extension: every method below degrades to a no-op.
	client storage.Client
	logger *zap.Logger

	mu      sync.Mutex
	cursors map[string]time.Time
	// lastSeenPods is when each pod UID was last reported by the informer. It
	// is what expiry is measured against: cursors are keyed per container, but
	// pods are what actually come and go.
	lastSeenPods map[string]time.Time
}

func newCursorStore(client storage.Client, logger *zap.Logger) *cursorStore {
	return &cursorStore{
		client:       client,
		logger:       logger,
		cursors:      make(map[string]time.Time),
		lastSeenPods: make(map[string]time.Time),
	}
}

// persists reports whether cursors survive a collector restart.
func (s *cursorStore) persists() bool { return s.client != nil }

// get returns the last delivered timestamp for a container, zero if none.
func (s *cursorStore) get(key string) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursors[key]
}

// advance records the last line delivered for a container. It only ever moves
// a cursor forward, so an out-of-order write cannot rewind one, and a zero
// timestamp — a line the kubelet gave us without one — is ignored rather than
// clearing the position.
func (s *cursorStore) advance(key string, ts time.Time) {
	if ts.IsZero() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if ts.After(s.cursors[key]) {
		s.cursors[key] = ts
	}
}

// forget drops a container's cursor, for a pod that is gone for good.
func (s *cursorStore) forget(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.cursors, key)
}

// markPodSeen records that a pod still exists, deferring expiry of its
// containers' cursors.
func (s *cursorStore) markPodSeen(podUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSeenPods[podUID] = time.Now()
}

// prune drops the cursors of every pod not seen since cutoff and returns the
// keys it removed, so the caller can drop whatever else it keys the same way.
//
// Expiry is what bounds the store. Without it a long-lived collector
// accumulates a cursor for every container that has ever run on the node —
// including the ones restored from storage, whose pods may have been deleted
// while the collector was down and are therefore never reported again.
func (s *cursorStore) prune(cutoff time.Time) []string {
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
		if _, ok := stale[podUIDFromStreamKey(key)]; ok {
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

// load seeds the in-memory cursors from storage. A missing or corrupt value is
// not fatal: the receiver starts from the configured backfill window instead,
// which is what it would do without persistence at all.
func (s *cursorStore) load(ctx context.Context) {
	if s.client == nil {
		return
	}
	raw, err := s.client.Get(ctx, cursorsStorageKey)
	if err != nil {
		s.logger.Warn("could not read stored cursors, starting from since_seconds", zap.Error(err))
		return
	}
	if len(raw) == 0 {
		return
	}

	stored := map[string]time.Time{}
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

// flush writes the current cursor set out.
func (s *cursorStore) flush(ctx context.Context) {
	if s.client == nil {
		return
	}
	s.mu.Lock()
	snapshot := make(map[string]time.Time, len(s.cursors))
	for k, v := range s.cursors {
		snapshot[k] = v
	}
	s.mu.Unlock()

	raw, err := json.Marshal(snapshot)
	if err != nil {
		s.logger.Warn("could not encode cursors", zap.Error(err))
		return
	}
	if err := s.client.Set(ctx, cursorsStorageKey, raw); err != nil {
		s.logger.Warn("could not persist cursors", zap.Error(err))
	}
}

// runFlushLoop persists cursors periodically until ctx is cancelled. The final
// write is done at shutdown by the caller, not here, so it still happens when
// the collector stops.
func (s *cursorStore) runFlushLoop(ctx context.Context) {
	ticker := time.NewTicker(cursorFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.flush(ctx)
		}
	}
}

// close releases the storage client, if any.
func (s *cursorStore) close(ctx context.Context) error {
	if s.client == nil {
		return nil
	}
	return s.client.Close(ctx)
}

// size reports how many cursors are held, for logging and tests.
func (s *cursorStore) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.cursors)
}

// openCursorStorage resolves the configured storage extension and returns a
// client for it. It returns a nil client when no storage is configured, which
// leaves cursors in memory only.
func openCursorStorage(ctx context.Context, host component.Host, storageID *component.ID, receiverID component.ID) (storage.Client, error) {
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
