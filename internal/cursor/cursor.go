// Copyright 2026 Yevhenii Kurasov
// SPDX-License-Identifier: Apache-2.0

// Package cursor tracks how far each container's log has been read, so a
// reconnect or a collector restart resumes where the last one stopped instead
// of re-reading the backfill window. It holds the position itself (Cursor),
// the filter that trims the overlap a reconnect is re-served (Filter), and
// the per-container set with its persistence and expiry (Store).
package cursor

import (
	"encoding/json"
	"time"
)

// Cursor is how far one container's log has been read: the timestamp of the
// last record handed to the pipeline, and how many records carrying exactly
// that timestamp were handed over.
//
// The counter is what makes the position exact. A timestamp alone cannot say
// where inside its own second — or even its own nanosecond, for lines the
// kubelet stamps identically — the last delivered record sat, and a reconnect
// always re-reads from the start of that second; see Filter. Counting
// the records already delivered at the cursor's timestamp turns "somewhere in
// this second" into "after exactly these records".
type Cursor struct {
	TS time.Time `json:"ts"`
	// Delivered counts only records that carry TS, so it resets to 1 as soon
	// as the timestamp moves on and never grows without bound.
	Delivered int `json:"delivered"`
}

// IsZero reports whether the cursor holds no position at all, which is what a
// container the receiver has never read looks like.
func (c Cursor) IsZero() bool { return c.TS.IsZero() }

// After reports whether c is strictly further along than other. Within one
// timestamp that is the counter, so the position still moves forward while a
// burst of records shares a single instant.
func (c Cursor) After(other Cursor) bool {
	if c.TS.Equal(other.TS) {
		return c.Delivered > other.Delivered
	}
	return c.TS.After(other.TS)
}

// Record folds one record into the cursor and reports whether the position
// moved. A record the kubelet handed over without a timestamp cannot be
// placed, and one older than the cursor is already behind it, so neither
// counts.
func (c *Cursor) Record(ts time.Time) bool {
	switch {
	case ts.IsZero():
		return false
	case ts.After(c.TS):
		c.TS, c.Delivered = ts, 1
	case ts.Equal(c.TS):
		c.Delivered++
	default:
		return false
	}
	return true
}

// UnmarshalJSON also accepts the bare RFC3339 string that earlier versions
// persisted, so an upgrade resumes from stored cursors instead of discarding
// the whole set as corrupt. Such a value restores with a zero counter, which
// re-delivers its second once — the duplicate this type exists to remove, and
// never a gap.
func (c *Cursor) UnmarshalJSON(raw []byte) error {
	if len(raw) > 0 && raw[0] == '"' {
		c.Delivered = 0
		return c.TS.UnmarshalJSON(raw)
	}
	// A distinct type, or this method would call itself.
	type wire Cursor
	var w wire
	if err := json.Unmarshal(raw, &w); err != nil {
		return err
	}
	*c = Cursor(w)
	return nil
}
