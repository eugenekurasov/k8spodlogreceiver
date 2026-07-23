package logline

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chunkedReader simulates a network stream that returns data in small chunks, so
// the scanner must reassemble lines across multiple Read calls.
type chunkedReader struct {
	data      []byte
	chunkSize int
	pos       int
}

func newChunkedReader(data string, chunkSize int) *chunkedReader {
	return &chunkedReader{data: []byte(data), chunkSize: chunkSize}
}

func (cr *chunkedReader) Read(p []byte) (int, error) {
	if cr.pos >= len(cr.data) {
		return 0, io.EOF
	}
	n := cr.chunkSize
	if n > len(p) {
		n = len(p)
	}
	if cr.pos+n > len(cr.data) {
		n = len(cr.data) - cr.pos
	}
	copy(p, cr.data[cr.pos:cr.pos+n])
	cr.pos += n
	return n, nil
}

// scanAll drains a Scanner into a slice of line bodies.
func scanAll(t *testing.T, s *Scanner) []string {
	t.Helper()
	var out []string
	for s.Scan() {
		out = append(out, s.Line().Body)
	}
	require.NoError(t, s.Err())
	return out
}

func TestScanner_NormalLinesUnaffected(t *testing.T) {
	s := NewScanner(strings.NewReader("alpha\nbeta\ngamma\n"), 1024, BehaviorSplit, nil)
	assert.Equal(t, []string{"alpha", "beta", "gamma"}, scanAll(t, s))
}

func TestScanner_SplitPreservesAllData(t *testing.T) {
	big := strings.Repeat("x", 25)
	var oversizeCalls int
	s := NewScanner(strings.NewReader(big+"\n"), 10, BehaviorSplit, func() { oversizeCalls++ })

	got := scanAll(t, s)
	assert.Equal(t, []string{"xxxxxxxxxx", "xxxxxxxxxx", "xxxxx"}, got, "split must chop into max-size records and lose nothing")
	assert.Equal(t, big, strings.Join(got, ""), "concatenated chunks must reconstruct the original line")
	// 25 bytes at a 10-byte cap is chopped at offsets 10 and 20; the final 5-byte
	// remainder ends at a newline, so it's a normal token, not a boundary chop.
	assert.Equal(t, 2, oversizeCalls, "onOversize fires once per boundary chop")
}

func TestScanner_TruncateKeepsHeadDropsTail(t *testing.T) {
	big := strings.Repeat("x", 25)
	input := big + "\n" + "after\n"
	var oversizeCalls int
	s := NewScanner(strings.NewReader(input), 10, BehaviorTruncate, func() { oversizeCalls++ })

	got := scanAll(t, s)
	assert.Equal(t, []string{"xxxxxxxxxx", "after"}, got, "truncate keeps the first max-size bytes and drops the rest of the line")
	assert.Equal(t, 1, oversizeCalls, "onOversize fires once for the truncated line")
}

// The scanner must never surface bufio.ErrTooLong, even when an oversized line
// arrives across many tiny reads.
func TestScanner_SplitAcrossChunkedReads(t *testing.T) {
	big := strings.Repeat("y", 100)
	r := newChunkedReader(big+"\n"+"tail\n", 7)
	s := NewScanner(r, 16, BehaviorSplit, nil)

	got := scanAll(t, s)
	require.NotEmpty(t, got)
	assert.Equal(t, "tail", got[len(got)-1])
	var reconstructed string
	for _, b := range got[:len(got)-1] {
		assert.LessOrEqual(t, len(b), 16)
		reconstructed += b
	}
	assert.Equal(t, big, reconstructed)
}

func TestScanner_TruncateAcrossChunkedReads(t *testing.T) {
	big := strings.Repeat("z", 100)
	r := newChunkedReader(big+"\n"+"tail\n", 7)
	s := NewScanner(r, 16, BehaviorTruncate, nil)

	got := scanAll(t, s)
	assert.Equal(t, []string{strings.Repeat("z", 16), "tail"}, got, "multi-chunk oversized line must keep only its head and preserve the next line")
}

func TestParseBehavior(t *testing.T) {
	for in, want := range map[string]Behavior{
		"":         BehaviorSplit,
		"split":    BehaviorSplit,
		"truncate": BehaviorTruncate,
	} {
		got, err := ParseBehavior(in)
		require.NoError(t, err, "input %q", in)
		assert.Equal(t, want, got, "input %q", in)
	}

	_, err := ParseBehavior("nonsense")
	require.Error(t, err)
}
