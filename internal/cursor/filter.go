// Copyright 2026 Yevhenii Kurasov
// SPDX-License-Identifier: Apache-2.0

package cursor

import "time"

// Filter drops the records a previous connection already delivered.
//
// A reconnect asks the kubelet for logs since the cursor's timestamp, but
// PodLogOptions carries that as a metav1.Time, which serialises to RFC3339
// with whole-second precision, and the kubelet treats the resulting second as
// inclusive. Every reconnect therefore re-reads from the start of the cursor's
// second rather than from the line after the cursor. This is not an error
// path: with the default max_stream_lifetime a perfectly healthy stream
// recycles once an hour and re-reads up to a second of its own log each time.
//
// Rounding the request up to the next second instead would trade those
// duplicates for real gaps inside it, turning at-least-once into
// at-most-once. A byte offset, the exact answer filelog has, does not exist
// over the API server. So the request stays as it is and the overlap is
// discarded here, where the full nanosecond timestamp each line carries is
// available.
type Filter struct {
	ts time.Time
	// skip is how many records carrying ts are still to be dropped.
	skip int
	// done is set by the first record past ts. Nothing after that can be a
	// re-read, so the rest of the connection is forwarded unexamined —
	// which is every record but the handful in the overlapping second.
	done bool
}

// NewFilter returns a filter for a connection that resumes from, which is the
// zero Cursor for a container that has never been read.
func NewFilter(from Cursor) Filter {
	return Filter{ts: from.TS, skip: from.Delivered, done: from.IsZero()}
}

// Keep reports whether a record with this timestamp should be forwarded.
func (f *Filter) Keep(ts time.Time) bool {
	switch {
	case f.done:
		return true
	case ts.IsZero():
		// Nothing to compare against, so the record cannot be shown to be a
		// re-read. The continuation chunks of a split oversized line are the
		// usual source, and they are re-delivered on every reconnect as a
		// result. Forwarding them costs a duplicate; dropping them would cost
		// the tail of a line outright, which is the worse of the two.
		return true
	case ts.Before(f.ts):
		return false
	case ts.After(f.ts):
		f.done = true
		return true
	case f.skip > 0:
		f.skip--
		return false
	default:
		return true
	}
}
