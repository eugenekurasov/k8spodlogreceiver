package logline

import (
	"bufio"
	"io"
)

// initialBufSize is the starting size of the scanner's read buffer. It grows on
// demand up to maxSize, so small lines (the common case) never allocate the full
// maximum. Matches the previous receiver read-buffer size.
const initialBufSize = 64 * 1024

// Scanner reads a newline-delimited pod log stream, enforcing a maximum record
// size, and yields parsed Lines. It is a thin wrapper over bufio.Scanner wired
// with SplitFunc, so oversized lines are split or truncated rather than
// surfaced as errors.
type Scanner struct {
	sc *bufio.Scanner
}

func NewScanner(r io.Reader, maxSize int, behavior Behavior, onOversize func()) *Scanner {
	sc := bufio.NewScanner(r)
	// The initial buffer must not exceed the max token size, or bufio.Scanner
	// can read past maxSize and report ErrTooLong before our SplitFunc runs.
	initial := initialBufSize
	if maxSize < initial {
		initial = maxSize
	}
	sc.Buffer(make([]byte, 0, initial), maxSize)
	sc.Split(SplitFunc(maxSize, behavior, onOversize))
	return &Scanner{sc: sc}
}

func (s *Scanner) Scan() bool { return s.sc.Scan() }

func (s *Scanner) Line() Line {
	ts, body := ParseLeadingTimestamp(s.sc.Text())
	return Line{Body: body, Timestamp: ts}
}

func (s *Scanner) Err() error { return s.sc.Err() }
