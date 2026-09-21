// Copyright 2026 Yevhenii Kurasov
// SPDX-License-Identifier: Apache-2.0

package cursor

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ts(t *testing.T, s string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, s)
	require.NoError(t, err)
	return parsed
}

// Within one timestamp the record count is the position, so it is what orders
// two cursors — otherwise a burst of records sharing an instant would look
// like no progress at all.
func TestCursor_AfterComparesCountWithinATimestamp(t *testing.T) {
	at := ts(t, "2026-08-14T10:00:01.200000000Z")

	assert.True(t, Cursor{TS: at, Delivered: 3}.After(Cursor{TS: at, Delivered: 2}))
	assert.False(t, Cursor{TS: at, Delivered: 2}.After(Cursor{TS: at, Delivered: 3}))
	assert.False(t, Cursor{TS: at, Delivered: 2}.After(Cursor{TS: at, Delivered: 2}))
	assert.True(t, Cursor{TS: at.Add(time.Nanosecond)}.After(Cursor{TS: at, Delivered: 99}),
		"a later timestamp wins regardless of the counts")
}

func TestCursor_RecordCountsPerTimestamp(t *testing.T) {
	at := func(s string) time.Time { return ts(t, s) }
	var c Cursor

	require.True(t, c.Record(at("2026-08-14T10:00:01.200000000Z")))
	require.True(t, c.Record(at("2026-08-14T10:00:01.200000000Z")))
	assert.Equal(t, Cursor{TS: at("2026-08-14T10:00:01.200000000Z"), Delivered: 2}, c)

	assert.False(t, c.Record(time.Time{}), "an unstamped record cannot be placed")
	assert.False(t, c.Record(at("2026-08-14T10:00:01.100000000Z")), "an out-of-order record is already behind")
	assert.Equal(t, Cursor{TS: at("2026-08-14T10:00:01.200000000Z"), Delivered: 2}, c)

	require.True(t, c.Record(at("2026-08-14T10:00:01.300000000Z")))
	assert.Equal(t, Cursor{TS: at("2026-08-14T10:00:01.300000000Z"), Delivered: 1},
		c, "the count belongs to the timestamp and restarts with it")
}

// Cursors persisted by a version that stored only a timestamp must still be
// usable, or upgrading discards the whole set as corrupt and every container
// re-reads its backfill window at once.
func TestCursor_UnmarshalAcceptsTheOlderTimestampOnlyFormat(t *testing.T) {
	var c Cursor
	require.NoError(t, json.Unmarshal([]byte(`"2026-08-14T10:00:01.2Z"`), &c))

	assert.True(t, c.TS.Equal(ts(t, "2026-08-14T10:00:01.200000000Z")))
	assert.Zero(t, c.Delivered,
		"nothing is known about that second, so it is re-read once rather than skipped")
}

func TestCursor_JSONRoundTripKeepsBothHalves(t *testing.T) {
	want := Cursor{TS: ts(t, "2026-08-14T10:00:01.200000000Z"), Delivered: 7}

	raw, err := json.Marshal(want)
	require.NoError(t, err)

	var got Cursor
	require.NoError(t, json.Unmarshal(raw, &got))
	assert.True(t, want.TS.Equal(got.TS))
	assert.Equal(t, want.Delivered, got.Delivered)
}
