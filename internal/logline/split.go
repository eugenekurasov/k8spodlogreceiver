package logline

import (
	"bufio"
	"bytes"
	"fmt"
)

// Behavior controls what happens when a single log line is longer than the
// configured maximum record size.
type Behavior int

const (
	// BehaviorSplit preserves all data: an oversized line is emitted as
	// consecutive maxSize-byte records, so nothing is dropped. This is the
	// default and mirrors the filelog receiver's "split" max_log_size_behavior.
	BehaviorSplit Behavior = iota

	// BehaviorTruncate emits the first maxSize bytes of an oversized line and
	// drops the remainder up to the next newline, mirroring the filelog
	// receiver's "truncate" max_log_size_behavior.
	BehaviorTruncate
)

// String names of the behaviors, matching the filelog receiver's config values
// so operators can reuse familiar terminology.
const (
	BehaviorSplitName    = "split"
	BehaviorTruncateName = "truncate"
)

// String returns the config name of the behavior, so it can be used with
// zap.Stringer and in error/log messages.
func (b Behavior) String() string {
	if b == BehaviorTruncate {
		return BehaviorTruncateName
	}
	return BehaviorSplitName
}

// ParseBehavior maps a config string to a Behavior. An empty string selects the
// default (BehaviorSplit). Unknown values are rejected.
func ParseBehavior(s string) (Behavior, error) {
	switch s {
	case "", BehaviorSplitName:
		return BehaviorSplit, nil
	case BehaviorTruncateName:
		return BehaviorTruncate, nil
	default:
		return 0, fmt.Errorf("must be %q or %q", BehaviorSplitName, BehaviorTruncateName)
	}
}

// SplitFunc returns a bufio.SplitFunc that tokenizes a newline-delimited stream
// into records no larger than maxSize. Unlike a plain bufio.ScanLines (which
// surfaces bufio.ErrTooLong and forces the caller to discard the whole line),
// this never loses the head of an oversized line:
//
//   - BehaviorSplit chops the line into consecutive maxSize-byte records.
//   - BehaviorTruncate emits the first maxSize bytes, then discards the rest of
//     the line up to the next newline.
//
// onOversize, if non-nil, is called once each time a line is chopped at the
// size boundary (useful for a warning log or a dropped/truncated metric). The
// returned func is stateful (it tracks the truncate skip position) and must not
// be shared across concurrent scanners.
func SplitFunc(maxSize int, behavior Behavior, onOversize func()) bufio.SplitFunc {
	// skipping is only used by BehaviorTruncate: once the head of an oversized
	// line has been emitted, we discard bytes until the next newline.
	var skipping bool

	return func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if behavior == BehaviorTruncate && skipping {
			if i := bytes.IndexByte(data, '\n'); i >= 0 {
				skipping = false
				return i + 1, nil, nil // consume through the newline, emit nothing
			}
			if atEOF {
				skipping = false
				return len(data), nil, nil
			}
			if len(data) >= maxSize {
				return len(data), nil, nil // buffer full of tail bytes; drop and keep skipping
			}
			return 0, nil, nil // need more data to find the newline
		}

		advance, token, err = bufio.ScanLines(data, atEOF)
		if advance == 0 && token == nil && err == nil && len(data) >= maxSize {
			// No newline within maxSize bytes: emit a full-size record instead
			// of waiting (which would eventually surface bufio.ErrTooLong).
			if onOversize != nil {
				onOversize()
			}
			if behavior == BehaviorTruncate {
				skipping = true
			}
			return maxSize, data[:maxSize], nil
		}
		return advance, token, err
	}
}
