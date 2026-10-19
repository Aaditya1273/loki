package builder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/loki/v3/pkg/dataobj"
	"github.com/grafana/loki/v3/pkg/dataobj/logsobj"
	"github.com/grafana/loki/v3/pkg/dataobj/metastore"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/scratch"

	"github.com/grafana/loki/pkg/push"
)

var (
	// A builder configuration to be used in tests.
	testBuilderCfg = logsobj.BuilderBaseConfig{
		TargetPageSize:          2048,
		MaxPageRows:             10,
		TargetObjectSize:        1 << 22, // 4 MiB
		TargetSectionSize:       1 << 22, // 4 MiB
		BufferSize:              2048 * 8,
		SectionStripeMergeLimit: 2,
	}
)

func TestProcessor_BuilderMaxAge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			ctx            = t.Context()
			reg            = prometheus.NewRegistry()
			m              = newTestMultiBuilder()
			flushCommitter = &mockFlushCommitter{}
			proc           = newProcessor(m, nil, flushCommitter, 5*time.Minute, 30*time.Minute, log.NewNopLogger(), reg)
		)

		// Since no records have been pushed, the first append time should be zero,
		// and no flush should have occurred.
		require.True(t, proc.firstAppend.IsZero())
		require.Equal(t, 0, flushCommitter.flushes)

		// Process a record containing some log lines. No flush should occur because
		// the builder has not reached the maximum age.
		require.NoError(t, proc.processRecord(ctx, newTestRecord(t, "tenant1", time.Now())))

		// The first append time should be set to the current time, but no flush
		// should have occurred.
		require.Equal(t, time.Now(), proc.firstAppend)
		require.Equal(t, time.Now(), proc.lastAppend)
		require.Equal(t, 0, flushCommitter.flushes)

		// Advance time past the maximum age. A flush should occur, and the
		// the log lines from the record should be appended to the next data
		// object, not the one that was just flushed.
		time.Sleep(31 * time.Minute)
		require.NoError(t, proc.processRecord(ctx, newTestRecord(t, "tenant1", time.Now())))

		// The last flushed time should be updated to the current time, and so should
		// the first append time to reflect the start of the new data object.
		require.Equal(t, time.Now(), proc.firstAppend)
		require.Equal(t, time.Now(), proc.lastAppend)
		require.NotEqual(t, proc.builder.GetEstimatedSize(), 0)
		require.NoError(t, proc.waitPendingFlush())
		require.Equal(t, 1, flushCommitter.flushes)

		// Advance time one last time and push some more logs. No flush should
		// occur because the next builder has not reached the maximum age.
		expectedLastFlushed := time.Now()
		time.Sleep(time.Minute)
		require.NoError(t, proc.processRecord(ctx, newTestRecord(t, "tenant1", time.Now())))
		require.Equal(t, expectedLastFlushed, proc.firstAppend)
		require.Equal(t, time.Now(), proc.lastAppend)
		require.Equal(t, 1, flushCommitter.flushes)
	})
}

func TestProcessor_FlushesWhenBuilderFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			ctx            = t.Context()
			reg            = prometheus.NewRegistry()
			m              = newTestMultiBuilder()
			flushCommitter = &mockFlushCommitter{}
			proc           = newProcessor(m, nil, flushCommitter, 5*time.Minute, 30*time.Minute, log.NewNopLogger(), reg)
		)

		// While the builder is not full, processing a record should append
		// without flushing.
		require.NoError(t, proc.processRecord(ctx, newTestRecord(t, "tenant", time.Now())))
		require.Equal(t, 0, flushCommitter.flushes)
		require.Equal(t, time.Now(), proc.firstAppend)

		// Mark the builder as full. The next record should trigger a flush
		// before it is appended, starting a new data object.
		m.forceFull = true
		flushedAt := time.Now()
		time.Sleep(time.Minute)
		require.NoError(t, proc.processRecord(ctx, newTestRecord(t, "tenant", time.Now())))

		// Exactly one flush should have occurred, and the record should still
		// have been appended afterwards, resetting firstAppend to the current
		// time rather than the time of the first append.
		require.NoError(t, proc.waitPendingFlush())
		require.Equal(t, 1, flushCommitter.flushes)
		require.NotEqual(t, flushedAt, proc.firstAppend)
		require.Equal(t, time.Now(), proc.firstAppend)
		require.Equal(t, time.Now(), proc.lastAppend)
	})
}

func TestPartitionProcessor_IdleFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			ctx            = t.Context()
			reg            = prometheus.NewRegistry()
			m              = newTestMultiBuilder()
			flushCommitter = &mockFlushCommitter{}
			proc           = newProcessor(m, nil, flushCommitter, 5*time.Minute, 30*time.Minute, log.NewNopLogger(), reg)
		)

		// The idle flush timeout has not been exceeded, no flush should occur.
		flushed, err := proc.idleFlush(ctx)
		require.NoError(t, err)
		require.False(t, flushed)
		require.Equal(t, 0, flushCommitter.flushes)

		// Advance time past the idle flush time. No flush should occur because
		// the builder is empty.
		time.Sleep(6 * time.Minute)
		flushed, err = proc.idleFlush(ctx)
		require.NoError(t, err)
		require.False(t, flushed)
		require.Equal(t, 0, flushCommitter.flushes)

		// Process a record containing some log lines. No flush should occur because
		// when log lines are appended to the builder it resets the idle timeout.
		require.NoError(t, proc.processRecord(ctx, newTestRecord(t, "tenant1", time.Now())))
		require.False(t, proc.lastAppend.IsZero())
		flushed, err = proc.idleFlush(ctx)
		require.NoError(t, err)
		require.False(t, flushed)
		require.Equal(t, 0, flushCommitter.flushes)

		// Advance time past the idle timeout. A flush should occur.
		time.Sleep(6 * time.Minute)
		flushed, err = proc.idleFlush(ctx)
		require.NoError(t, err)
		require.True(t, flushed)
		require.NoError(t, proc.waitPendingFlush())
		require.Equal(t, 1, flushCommitter.flushes)
	})
}

// failureFlusher is a special flusher that always fails.
type failureFlusher struct{}

func (f *failureFlusher) Flush(_ context.Context, _ builder, _ string) (*dataobj.Object, io.Closer, string, error) {
	return nil, nil, "", errors.New("mock error")
}

func (f *failureFlusher) Upload(_ context.Context, _ *dataobj.Object, _ string) error {
	return errors.New("mock error")
}

type failureFlushCommitter struct{}

func (m *failureFlushCommitter) Flush(_ context.Context, _ []builder, _ string, _ int64) error {
	return errors.New("mock error")
}

// fixedErrFlushCommitter always returns the configured error. It is used to
// verify how the processor classifies flush failures (recoverable vs. not).
type fixedErrFlushCommitter struct {
	err error
}

func (m *fixedErrFlushCommitter) Flush(_ context.Context, _ []builder, _ string, _ int64) error {
	return m.err
}

func TestPartitionProcessor_Flush(t *testing.T) {
	t.Run("should succeed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var (
				ctx            = t.Context()
				reg            = prometheus.NewRegistry()
				m              = newTestMultiBuilder()
				flushCommitter = &mockFlushCommitter{}
				proc           = newProcessor(m, nil, flushCommitter, 5*time.Minute, 30*time.Minute, log.NewNopLogger(), reg)
			)

			// No flush should have occurred.
			require.Equal(t, 0, flushCommitter.flushes)

			// Process a record containing some log lines. No flush should occur.
			rec1 := newTestRecord(t, "tenant", time.Now())
			require.NoError(t, proc.processRecord(ctx, rec1))
			require.Equal(t, time.Now(), proc.firstAppend)
			require.Equal(t, time.Now(), proc.lastAppend)
			require.Equal(t, rec1.Offset, proc.lastOffset)

			// Advance time and force a flush.
			time.Sleep(time.Second)
			require.NoError(t, proc.flush(ctx, "test"))
			require.NoError(t, proc.waitPendingFlush())
			require.Equal(t, 1, flushCommitter.flushes)

			// The following fields should be reset at the end of every flush.
			require.True(t, proc.firstAppend.IsZero())
			require.True(t, proc.lastAppend.IsZero())
		})
	})

	t.Run("should fail", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var (
				ctx            = t.Context()
				reg            = prometheus.NewRegistry()
				m              = newTestMultiBuilder()
				flushCommitter = &failureFlushCommitter{}
				proc           = newProcessor(m, nil, flushCommitter, 5*time.Minute, 30*time.Minute, log.NewNopLogger(), reg)
			)

			// Process a record containing some log lines. No flush should occur.
			rec := newTestRecord(t, "tenant", time.Now())
			require.NoError(t, proc.processRecord(ctx, rec))
			require.Equal(t, time.Now(), proc.firstAppend)
			require.Equal(t, time.Now(), proc.lastAppend)
			require.Equal(t, rec.Offset, proc.lastOffset)

			// Advance time and force a flush. The batch flushes in the
			// background and fails. Because the flusher is not re-entrant, a
			// non-cancellation failure is escalated to an unrecoverable error so
			// the partition processor restarts and replays from Kafka.
			time.Sleep(time.Second)
			require.NoError(t, proc.flush(ctx, "forced"))
			err := proc.waitPendingFlush()
			require.ErrorIs(t, err, errUnrecoverableFlush)
			require.ErrorContains(t, err, "mock error")

			// Despite the failure, the following fields should still be reset.
			require.True(t, proc.firstAppend.IsZero())
			require.True(t, proc.lastAppend.IsZero())
		})
	})

	t.Run("should not escalate a canceled flush", func(t *testing.T) {
		// During shutdown the flush context is canceled. A canceled flush must
		// be surfaced as-is rather than escalated to errUnrecoverableFlush, so
		// the processor shuts down gracefully instead of restarting.
		synctest.Test(t, func(t *testing.T) {
			var (
				ctx            = t.Context()
				reg            = prometheus.NewRegistry()
				m              = newTestMultiBuilder()
				flushCommitter = &fixedErrFlushCommitter{err: context.Canceled}
				proc           = newProcessor(m, nil, flushCommitter, 5*time.Minute, 30*time.Minute, log.NewNopLogger(), reg)
			)

			require.NoError(t, proc.processRecord(ctx, newTestRecord(t, "tenant", time.Now())))

			time.Sleep(time.Second)
			require.NoError(t, proc.flush(ctx, "forced"))
			err := proc.waitPendingFlush()
			require.ErrorIs(t, err, context.Canceled)
			require.NotErrorIs(t, err, errUnrecoverableFlush)
		})
	})
}

func TestPartitionProcessor_FlushSplitsAcrossTenantsAndWindows(t *testing.T) {
	// Records whose entries span multiple tenants and TOC windows must be
	// flushed as one builder per tenant and window, while still committing a
	// single Kafka offset.
	synctest.Test(t, func(t *testing.T) {
		var (
			ctx            = t.Context()
			reg            = prometheus.NewRegistry()
			m              = newTestMultiBuilder()
			flushCommitter = &mockFlushCommitter{}
			proc           = newProcessor(m, nil, flushCommitter, 5*time.Minute, 30*time.Minute, log.NewNopLogger(), reg)
		)

		w1 := time.Date(2026, time.April, 17, 0, 0, 0, 0, time.UTC)
		w2 := w1.Add(metastore.MetastoreWindowSize)
		w3 := w2.Add(metastore.MetastoreWindowSize)
		require.NoError(t, proc.processRecord(ctx, newTestMultiWindowRecord(t, "tenant-a", []time.Time{w1, w2, w3})))
		rec := newTestMultiWindowRecord(t, "tenant-b", []time.Time{w1, w2})
		rec.Offset = 1
		require.NoError(t, proc.processRecord(ctx, rec))

		// Three windows for tenant-a and two for tenant-b.
		require.Len(t, m.getBuilders(), 5)

		require.NoError(t, proc.flush(ctx, "forced"))
		require.NoError(t, proc.waitPendingFlush())
		require.Equal(t, 1, flushCommitter.flushes)
		require.Equal(t, 5, flushCommitter.lastBuilderCount)
		require.Equal(t, rec.Offset, flushCommitter.lastOffset)

		// The multi-builder is reset after a successful flush.
		require.Empty(t, m.getBuilders())
		require.Equal(t, 0, m.GetEstimatedSize())
	})
}

func TestProcessor_ConsumeDuringFlush(t *testing.T) {
	newProc := func(records chan *kgo.Record) (*processor, *mockMultiBuilder, *gatedFlushCommitter) {
		m := newTestMultiBuilder()
		flushCommitter := newGatedFlushCommitter()
		proc := newProcessor(m, records, flushCommitter, 5*time.Minute, 30*time.Minute, log.NewNopLogger(), prometheus.NewRegistry())
		return proc, m, flushCommitter
	}
	// waitRun fails the test if Run does not return within a minute of fake
	// time. The idle flush timeout is longer, so an idle flush cannot hide a
	// Run that does not return.
	waitRun := func(t *testing.T, ran <-chan error) error {
		select {
		case err := <-ran:
			return err
		case <-time.After(time.Minute):
			t.Fatal("Run did not return")
			return nil
		}
	}
	newRecord := func(t *testing.T, offset int64) *kgo.Record {
		rec := newTestRecord(t, "tenant", time.Now())
		rec.Offset = offset
		return rec
	}

	t.Run("appends records while the previous batch flushes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			proc, m, flushCommitter := newProc(nil)

			require.NoError(t, proc.processRecord(ctx, newRecord(t, 1)))
			m.forceFull = true
			require.NoError(t, proc.processRecord(ctx, newRecord(t, 2)))
			synctest.Wait()
			require.Equal(t, []int64{1}, flushCommitter.startedOffsets())

			m.forceFull = false
			require.NoError(t, proc.processRecord(ctx, newRecord(t, 3)))
			require.Equal(t, int64(3), proc.lastOffset)
			require.Len(t, m.getBuilders(), 1)
			require.Equal(t, time.Now(), proc.firstAppend)

			flushCommitter.results <- nil
			require.NoError(t, proc.waitPendingFlush())
		})
	})

	t.Run("starts the next flush only after the previous batch commits", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			proc, m, flushCommitter := newProc(nil)

			require.NoError(t, proc.processRecord(ctx, newRecord(t, 1)))
			m.forceFull = true
			require.NoError(t, proc.processRecord(ctx, newRecord(t, 2)))

			processed := make(chan error)
			go func() { processed <- proc.processRecord(ctx, newRecord(t, 3)) }()
			synctest.Wait()
			require.Equal(t, []int64{1}, flushCommitter.startedOffsets())

			flushCommitter.results <- nil
			require.NoError(t, <-processed)
			synctest.Wait()
			require.Equal(t, []int64{1, 2}, flushCommitter.startedOffsets())

			flushCommitter.results <- nil
			require.NoError(t, proc.waitPendingFlush())
		})
	})

	t.Run("returns the error of a failed background flush from the next flush", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			proc, m, flushCommitter := newProc(nil)

			require.NoError(t, proc.processRecord(ctx, newRecord(t, 1)))
			m.forceFull = true
			require.NoError(t, proc.processRecord(ctx, newRecord(t, 2)))
			flushCommitter.results <- errors.New("mock error")
			synctest.Wait()

			err := proc.processRecord(ctx, newRecord(t, 3))
			require.ErrorIs(t, err, errUnrecoverableFlush)
			require.ErrorContains(t, err, "mock error")
			require.Equal(t, []int64{1}, flushCommitter.startedOffsets())
		})
	})

	t.Run("stops Run when a background flush fails", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			records := make(chan *kgo.Record)
			proc, m, flushCommitter := newProc(records)

			ran := make(chan error)
			go func() { ran <- proc.Run(t.Context()) }()
			records <- newRecord(t, 1)
			synctest.Wait()
			m.forceFull = true
			records <- newRecord(t, 2)
			synctest.Wait()

			flushCommitter.results <- errors.New("mock error")
			err := waitRun(t, ran)
			require.ErrorIs(t, err, errUnrecoverableFlush)
			require.ErrorContains(t, err, "mock error")
		})
	})

	t.Run("waits for the background flush before Run returns", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			records := make(chan *kgo.Record)
			proc, m, flushCommitter := newProc(records)

			ran := make(chan error, 1)
			go func() { ran <- proc.Run(t.Context()) }()
			records <- newRecord(t, 1)
			synctest.Wait()
			m.forceFull = true
			records <- newRecord(t, 2)
			synctest.Wait()

			close(records)
			synctest.Wait()
			require.Empty(t, ran)

			flushCommitter.results <- nil
			require.NoError(t, waitRun(t, ran))
		})
	})
}

// newTestBuilder returns a new logsobj.Builder with registered metrics.
func newTestBuilder(t *testing.T, reg prometheus.Registerer) *logsobj.Builder {
	m := logsobj.NewBuilderMetrics()
	require.NoError(t, m.Register(reg))
	b, err := logsobj.NewBuilder(testBuilderCfg, scratch.NewMemory(), m, log.NewNopLogger(), nil)
	require.NoError(t, err)
	return b
}

// newTestRecord returns a new record containing the stream.
func newTestRecord(t *testing.T, tenant string, now time.Time) *kgo.Record {
	rec := kgo.Record{
		Key:       []byte(tenant),
		Timestamp: now,
	}
	stream := logproto.Stream{
		Labels: `{foo="bar"}`,
		Entries: []push.Entry{{
			Timestamp: now,
			Line:      "baz",
		}},
	}
	var err error
	rec.Value, err = stream.Marshal()
	require.NoError(t, err)
	return &rec
}

// newTestMultiWindowRecord returns a record whose stream contains one entry
// per provided timestamp. It is used to exercise records that span multiple
// TOC windows. The record timestamp is the first window.
func newTestMultiWindowRecord(t *testing.T, tenant string, windows []time.Time) *kgo.Record {
	require.NotEmpty(t, windows)
	rec := kgo.Record{
		Key:       []byte(tenant),
		Timestamp: windows[0],
	}
	stream := logproto.Stream{Labels: `{foo="bar"}`}
	for i, w := range windows {
		stream.Entries = append(stream.Entries, push.Entry{
			Timestamp: w.Add(time.Minute),
			Line:      fmt.Sprintf("line-%d", i),
		})
	}
	var err error
	rec.Value, err = stream.Marshal()
	require.NoError(t, err)
	return &rec
}
