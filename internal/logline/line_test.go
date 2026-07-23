package logline

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseLeadingTimestamp_ParsesValidTimestamp(t *testing.T) {
	ts, body := ParseLeadingTimestamp("2024-01-15T10:30:00.123456789Z log line content")
	want, err := time.Parse(time.RFC3339Nano, "2024-01-15T10:30:00.123456789Z")
	require.NoError(t, err)
	assert.True(t, ts.Equal(want))
	assert.Equal(t, "log line content", body, "the timestamp prefix must be stripped from the body")
}

func TestParseLeadingTimestamp_NoSpace(t *testing.T) {
	ts, body := ParseLeadingTimestamp("nospacehere")
	assert.True(t, ts.IsZero())
	assert.Equal(t, "nospacehere", body, "a line with no prefix is returned unchanged")
}

func TestParseLeadingTimestamp_UnparseableTimestamp(t *testing.T) {
	ts, body := ParseLeadingTimestamp("not-a-timestamp rest of line")
	assert.True(t, ts.IsZero())
	assert.Equal(t, "not-a-timestamp rest of line", body, "a line without a valid timestamp is returned unchanged")
}
