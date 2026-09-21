// Copyright 2026 Yevhenii Kurasov
// SPDX-License-Identifier: Apache-2.0

package cursor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The whole point of the record count: a reconnect is served the cursor's
// entire second, and only the part of it that was never delivered may pass.
func TestResumeFilter_DropsExactlyWhatWasAlreadyDelivered(t *testing.T) {
	at := func(s string) time.Time { return ts(t, s) }
	f := NewFilter(Cursor{TS: at("2026-08-14T10:00:01.200000000Z"), Delivered: 2})

	assert.False(t, f.Keep(at("2026-08-14T10:00:01.000000000Z")), "earlier in the replayed second")
	assert.False(t, f.Keep(at("2026-08-14T10:00:01.200000000Z")), "first record at the cursor")
	assert.False(t, f.Keep(at("2026-08-14T10:00:01.200000000Z")), "second record at the cursor")
	assert.True(t, f.Keep(at("2026-08-14T10:00:01.200000000Z")),
		"the third record at the cursor was never delivered")
	assert.True(t, f.Keep(at("2026-08-14T10:00:01.900000000Z")))
}

// A container with no cursor has nothing to de-duplicate against, and every
// record it reads is new.
func TestResumeFilter_ZeroCursorKeepsEverything(t *testing.T) {
	f := NewFilter(Cursor{})
	assert.True(t, f.Keep(ts(t, "2026-08-14T10:00:01.000000000Z")))
	assert.True(t, f.Keep(time.Time{}))
}

// Past the cursor's second nothing can be a re-read, so the filter retires —
// including for a record the kubelet then hands over out of order, which is a
// line we have genuinely never seen.
func TestResumeFilter_RetiresPastTheCursor(t *testing.T) {
	at := func(s string) time.Time { return ts(t, s) }
	f := NewFilter(Cursor{TS: at("2026-08-14T10:00:01.200000000Z"), Delivered: 5})

	require.True(t, f.Keep(at("2026-08-14T10:00:02.000000000Z")))
	assert.True(t, f.done)
	assert.True(t, f.Keep(at("2026-08-14T10:00:00.000000000Z")),
		"once past the cursor the filter is off for the rest of the connection")
}

// A record with no timestamp cannot be shown to be a re-read — the
// continuation chunks of a split oversized line are the common case. Keeping
// it costs a duplicate; dropping it would lose the line, which is the worse
// half of the trade this receiver makes everywhere else.
func TestResumeFilter_UnstampedRecordIsKept(t *testing.T) {
	at := func(s string) time.Time { return ts(t, s) }
	f := NewFilter(Cursor{TS: at("2026-08-14T10:00:01.200000000Z"), Delivered: 2})

	assert.True(t, f.Keep(time.Time{}))
	assert.False(t, f.Keep(at("2026-08-14T10:00:01.200000000Z")),
		"an unstamped record must not consume the skip budget of a stamped one")
}
