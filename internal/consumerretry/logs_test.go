package consumerretry

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errRefused stands in for memory_limiter's ErrDataRefused: a plain error,
// deliberately not wrapped as permanent, because that is exactly how
// memory_limiter reports backpressure.
var errRefused = errors.New("data refused due to high memory usage")

// scriptedConsumer returns errs[i] on call i, then nil for every later call,
// recording how many records each call carried.
type scriptedConsumer struct {
	errs      []error
	calls     int
	seenCount []int
}

func (s *scriptedConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (s *scriptedConsumer) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	s.seenCount = append(s.seenCount, ld.LogRecordCount())
	s.calls++
	if s.calls <= len(s.errs) {
		return s.errs[s.calls-1]
	}
	return nil
}

func testRetryCfg() Config {
	return Config{
		Enabled:         true,
		InitialInterval: time.Millisecond,
		MaxInterval:     2 * time.Millisecond,
		MaxElapsedTime:  time.Minute,
	}
}

func logsWithRecords(n int) plog.Logs {
	ld := plog.NewLogs()
	rec := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for range n {
		rec.AppendEmpty().Body().SetStr("line")
	}
	return ld
}

// Disabled passes the call straight through and surfaces the error to the
// caller without retrying — the batch is dropped, which is the behavior the
// logged hint warns about.
func TestConsumeLogs_DisabledPassesThroughWithoutRetrying(t *testing.T) {
	next := &scriptedConsumer{errs: []error{errRefused}}
	lc := NewLogs(Config{Enabled: false}, zap.NewNop(), next)

	err := lc.ConsumeLogs(context.Background(), logsWithRecords(1))
	require.ErrorIs(t, err, errRefused)
	assert.Equal(t, 1, next.calls, "disabled must not retry")
}

func TestConsumeLogs_DisabledReturnsNilOnSuccess(t *testing.T) {
	next := consumertest.NewNop()
	lc := NewLogs(Config{Enabled: false}, zap.NewNop(), next)
	assert.NoError(t, lc.ConsumeLogs(context.Background(), logsWithRecords(1)))
}

// The core case this wrapper exists for: memory_limiter refuses, then accepts.
// The batch must be delivered, not dropped.
func TestConsumeLogs_RetriesRecoverableErrorUntilAccepted(t *testing.T) {
	next := &scriptedConsumer{errs: []error{errRefused, errRefused}}
	rc := NewLogs(testRetryCfg(), zap.NewNop(), next)

	require.NoError(t, rc.ConsumeLogs(context.Background(), logsWithRecords(3)))
	assert.Equal(t, 3, next.calls, "should retry twice then succeed")
	assert.Equal(t, []int{3, 3, 3}, next.seenCount, "the same batch is re-sent each time")
}

// A permanent error means the data itself is unacceptable; retrying can never
// help, so it must be dropped on the first attempt.
func TestConsumeLogs_DropsPermanentErrorWithoutRetrying(t *testing.T) {
	permanent := consumererror.NewPermanent(errors.New("malformed"))
	next := &scriptedConsumer{errs: []error{permanent}}
	rc := NewLogs(testRetryCfg(), zap.NewNop(), next)

	err := rc.ConsumeLogs(context.Background(), logsWithRecords(1))
	require.Error(t, err)
	assert.True(t, consumererror.IsPermanent(err))
	assert.Equal(t, 1, next.calls, "permanent errors must not be retried")
}

// When downstream reports that only part of the batch still needs delivering,
// the retry must carry that remainder rather than re-sending everything.
func TestConsumeLogs_RetriesOnlyPartialData(t *testing.T) {
	remainder := logsWithRecords(2)
	next := &scriptedConsumer{errs: []error{consumererror.NewLogs(errRefused, remainder)}}
	rc := NewLogs(testRetryCfg(), zap.NewNop(), next)

	require.NoError(t, rc.ConsumeLogs(context.Background(), logsWithRecords(5)))
	require.Equal(t, 2, next.calls)
	assert.Equal(t, []int{5, 2}, next.seenCount, "second attempt carries only the remainder")
}

// A pipeline that never recovers must not block a stream forever.
func TestConsumeLogs_GivesUpAfterMaxElapsedTime(t *testing.T) {
	cfg := testRetryCfg()
	cfg.MaxElapsedTime = 20 * time.Millisecond
	next := &scriptedConsumer{errs: make([]error, 0)}
	// Refuse every call.
	for range 10000 {
		next.errs = append(next.errs, errRefused)
	}
	rc := NewLogs(cfg, zap.NewNop(), next)

	err := rc.ConsumeLogs(context.Background(), logsWithRecords(1))
	require.ErrorIs(t, err, errRefused)
	assert.Greater(t, next.calls, 1, "should have retried before giving up")
}

// Shutdown must interrupt a backoff sleep rather than hold the stream open for
// the full MaxElapsedTime.
func TestConsumeLogs_StopsOnContextCancel(t *testing.T) {
	cfg := testRetryCfg()
	cfg.InitialInterval = time.Hour
	cfg.MaxElapsedTime = 0 // retry indefinitely
	next := &scriptedConsumer{errs: []error{errRefused}}
	rc := NewLogs(cfg, zap.NewNop(), next)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := rc.ConsumeLogs(ctx, logsWithRecords(1))
	require.ErrorIs(t, err, errRefused, "the refusal is wrapped, not swallowed")
	assert.Equal(t, 1, next.calls)
}

// MaxElapsedTime == 0 means retry indefinitely, matching ReconnectBackoff's
// convention — it must not be read as "give up immediately".
func TestConsumeLogs_ZeroMaxElapsedTimeRetriesIndefinitely(t *testing.T) {
	cfg := testRetryCfg()
	cfg.MaxElapsedTime = 0
	next := &scriptedConsumer{errs: []error{errRefused, errRefused, errRefused}}
	rc := NewLogs(cfg, zap.NewNop(), next)

	require.NoError(t, rc.ConsumeLogs(context.Background(), logsWithRecords(1)))
	assert.Equal(t, 4, next.calls)
}

func TestConfigValidate(t *testing.T) {
	require.NoError(t, NewDefaultConfig().Validate())

	for name, cfg := range map[string]Config{
		"negative initial_interval": {InitialInterval: -time.Second},
		"negative max_interval":     {MaxInterval: -time.Second},
		"negative max_elapsed_time": {MaxElapsedTime: -time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Error(t, cfg.Validate())
		})
	}
}

// Enabled-by-default is a deliberate divergence from upstream; see the package
// doc in logs.go. Pin it so it is not "tidied" back to match contrib.
func TestNewDefaultConfig_EnabledByDefault(t *testing.T) {
	assert.True(t, NewDefaultConfig().Enabled)
}
