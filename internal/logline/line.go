// It is deliberately small and stanza-independent: this receiver streams via
// the API server rather than tailing files, so it needs only a slice of what
// pkg/stanza provides.
package logline

import (
	"strings"
	"time"
)

type Line struct {
	Body      string
	Timestamp time.Time
}

func ParseLeadingTimestamp(line string) (time.Time, string) {
	idx := strings.IndexByte(line, ' ')
	if idx < 0 {
		return time.Time{}, line
	}
	ts, err := time.Parse(time.RFC3339Nano, line[:idx])
	if err != nil {
		return time.Time{}, line
	}
	return ts, line[idx+1:]
}
