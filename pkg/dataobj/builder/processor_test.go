package builder

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/loki/v3/pkg/dataobj"
	"github.com/grafana/loki/v3/pkg/dataobj/index"
	"github.com/grafana/loki/v3/pkg/dataobj/index/indexobj"
	"github.com/grafana/loki/v3/pkg/dataobj/logsobj"
	"github.com/grafana/loki/v3/pkg/dataobj/metastore"
	dataobj_uploader "github.com/grafana/loki/v3/pkg/dataobj/uploader"
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

			// flush resets these fields when it hands off the batch.
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

			// flush reset these fields when it handed off the batch, before
			// the batch failed.
			require.True(t, proc.firstAppend.IsZero())
			require.True(t, proc.lastAppend.IsZero())
		})
	})

	t.Run("does not escalate a failure after shutdown cancels the context", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var (
				ctx, cancel    = context.WithCancel(t.Context())
				reg            = prometheus.NewRegistry()
				m              = newTestMultiBuilder()
				flushCommitter = &fixedErrFlushCommitter{err: errors.New("mock error")}
				proc           = newProcessor(m, nil, flushCommitter, 5*time.Minute, 30*time.Minute, log.NewNopLogger(), reg)
			)

			require.NoError(t, proc.processRecord(ctx, newTestRecord(t, "tenant", time.Now())))

			cancel()
			require.NoError(t, proc.flush(ctx, "forced"))
			err := proc.waitPendingFlush()
			require.ErrorContains(t, err, "mock error")
			require.NotErrorIs(t, err, errUnrecoverableFlush)
		})
	})

	t.Run("escalates a canceled error while the context is live", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var (
				ctx            = t.Context()
				reg            = prometheus.NewRegistry()
				m              = newTestMultiBuilder()
				flushCommitter = &fixedErrFlushCommitter{err: context.Canceled}
				proc           = newProcessor(m, nil, flushCommitter, 5*time.Minute, 30*time.Minute, log.NewNopLogger(), reg)
			)

			require.NoError(t, proc.processRecord(ctx, newTestRecord(t, "tenant", time.Now())))

			require.NoError(t, proc.flush(ctx, "forced"))
			err := proc.waitPendingFlush()
			require.ErrorIs(t, err, errUnrecoverableFlush)
			require.ErrorIs(t, err, context.Canceled)
			require.ErrorContains(t, err, "forced flush up to offset 0")
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

		// flush resets the multi-builder when it hands off the batch.
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

	t.Run("reports the size of the flushing batch and the time spent waiting for it", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			proc, m, flushCommitter := newProc(nil)

			require.NoError(t, proc.processRecord(ctx, newRecord(t, 1)))
			firstSize := float64(m.GetEstimatedSize())
			m.forceFull = true
			require.NoError(t, proc.processRecord(ctx, newRecord(t, 2)))
			secondSize := float64(m.GetEstimatedSize())
			require.Equal(t, firstSize, testutil.ToFloat64(proc.metrics.flushingSizeEstimate))
			require.Equal(t, secondSize, testutil.ToFloat64(proc.metrics.sizeEstimate))

			processed := make(chan error)
			go func() { processed <- proc.processRecord(ctx, newRecord(t, 3)) }()
			time.Sleep(10 * time.Second)
			require.Equal(t, firstSize, testutil.ToFloat64(proc.metrics.flushingSizeEstimate))
			require.Equal(t, secondSize, testutil.ToFloat64(proc.metrics.sizeEstimate))

			flushCommitter.results <- nil
			require.NoError(t, <-processed)
			require.Equal(t, 10.0, testutil.ToFloat64(proc.metrics.flushWaitSeconds))
			require.Equal(t, secondSize, testutil.ToFloat64(proc.metrics.flushingSizeEstimate))

			flushCommitter.results <- nil
			require.NoError(t, proc.waitPendingFlush())
			require.Zero(t, testutil.ToFloat64(proc.metrics.flushingSizeEstimate))
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

	t.Run("keeps the flushing builders apart from the builders that receive new records", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			proc, m, flushCommitter := newProc(nil)

			require.NoError(t, proc.processRecord(ctx, newRecord(t, 1)))
			m.forceFull = true
			require.NoError(t, proc.processRecord(ctx, newRecord(t, 2)))
			m.forceFull = false
			synctest.Wait()

			flushing := flushCommitter.startedBuilders()[0]
			require.Len(t, flushing, 1)
			flushingSize := flushing[0].GetEstimatedSize()

			// The records have the tenant and window of the flushing builder.
			// They must go to a new builder all the same.
			for offset := int64(3); offset < 10; offset++ {
				require.NoError(t, proc.processRecord(ctx, newRecord(t, offset)))
			}
			require.Equal(t, flushingSize, flushing[0].GetEstimatedSize())
			require.Len(t, m.getBuilders(), 1)
			require.NotSame(t, flushing[0], m.getBuilders()[0])

			flushCommitter.results <- nil
			require.NoError(t, proc.waitPendingFlush())
		})
	})

	t.Run("stops reading records while the next batch waits for the previous flush", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			records := make(chan *kgo.Record)
			proc, m, flushCommitter := newProc(records)

			ran := make(chan error, 1)
			go func() { ran <- proc.Run(t.Context()) }()
			records <- newRecord(t, 1)
			synctest.Wait()
			m.forceFull = true
			records <- newRecord(t, 2)
			records <- newRecord(t, 3)
			synctest.Wait()

			select {
			case records <- newRecord(t, 4):
				t.Fatal("Run read a record while the next batch waited for the previous flush")
			default:
			}
			require.Equal(t, []int64{1}, flushCommitter.startedOffsets())

			m.forceFull = false
			flushCommitter.results <- nil
			records <- newRecord(t, 4)
			synctest.Wait()
			require.Equal(t, []int64{1, 2}, flushCommitter.startedOffsets())
			require.Equal(t, 1, flushCommitter.maxConcurrentFlushes())

			flushCommitter.results <- nil
			close(records)
			require.NoError(t, waitRun(t, ran))
		})
	})

	t.Run("starts an idle flush only after the previous batch commits", func(t *testing.T) {
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
			m.forceFull = false

			// The idle flush timeout is 5 minutes. The first timer fires at
			// exactly 5 minutes, which is not past the timeout, so the idle
			// flush starts at the second timer.
			time.Sleep(11 * time.Minute)
			synctest.Wait()
			require.Equal(t, []int64{1}, flushCommitter.startedOffsets())

			flushCommitter.results <- nil
			synctest.Wait()
			require.Equal(t, []int64{1, 2}, flushCommitter.startedOffsets())
			require.Equal(t, []string{flushReasonBuilderFull, flushReasonIdle}, flushCommitter.startedReasons())

			flushCommitter.results <- nil
			close(records)
			require.NoError(t, waitRun(t, ran))
		})
	})

	t.Run("starts no later flush when Run stops while a batch waits for the previous flush", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			records := make(chan *kgo.Record)
			proc, m, flushCommitter := newProc(records)

			ran := make(chan error, 1)
			go func() { ran <- proc.Run(ctx) }()
			records <- newRecord(t, 1)
			synctest.Wait()
			m.forceFull = true
			records <- newRecord(t, 2)
			records <- newRecord(t, 3)
			synctest.Wait()

			cancel()
			require.NoError(t, waitRun(t, ran))
			require.Equal(t, []int64{1}, flushCommitter.startedOffsets())
		})
	})

	t.Run("builds the same data objects and indexes as a flush that blocks consumption", func(t *testing.T) {
		batches := newTestBatches(t, 3, 300)

		serialObjects, serialOffsets := flushWithRealStack(t, batches, true)
		objects, offsets := flushWithRealStack(t, batches, false)

		require.NotEmpty(t, serialObjects)
		require.Equal(t, serialOffsets, offsets)
		require.Equal(t, slices.Sorted(maps.Keys(serialObjects)), slices.Sorted(maps.Keys(objects)))
		for name, data := range serialObjects {
			require.Equal(t, data, objects[name], "object %s", name)
		}
	})
}

// newTestBatches returns n batches of size records each, with offsets that
// increase across batches. The records belong to three tenants and span two
// ToC windows. The entries of each stream are out of order, so the sort has
// work to do.
func newTestBatches(t *testing.T, n, size int) [][]*kgo.Record {
	base := time.Date(2026, time.October, 1, 6, 0, 0, 0, time.UTC)
	var (
		batches [][]*kgo.Record
		offset  int64
	)
	for b := range n {
		var batch []*kgo.Record
		for i := range size {
			ts := base.Add(time.Duration(b*size+(i*37)%size) * time.Minute)
			stream := logproto.Stream{
				Labels:  fmt.Sprintf(`{app="app-%d", pod="pod-%d"}`, i%4, i%7),
				Entries: []push.Entry{{Timestamp: ts, Line: fmt.Sprintf("level=info batch=%d line=%d", b, i)}},
			}
			value, err := stream.Marshal()
			require.NoError(t, err)
			batch = append(batch, &kgo.Record{
				Key:       []byte(fmt.Sprintf("tenant-%d", i%3)),
				Value:     value,
				Timestamp: ts,
				Offset:    offset,
			})
			offset++
		}
		batches = append(batches, batch)
	}
	return batches
}

// flushWithRealStack appends each batch to a processor and flushes it as one
// batch. The flush uses the production sort, upload, index and ToC writer
// with an in-memory bucket. It returns the contents of the bucket and the
// committed offsets.
//
// If serial is true, it waits for each flush before it appends the next
// batch. The processor read no records during a flush before batches flushed
// in the background.
func flushWithRealStack(t *testing.T, batches [][]*kgo.Record, serial bool) (map[string][]byte, []int64) {
	var cfg Config
	cfg.RegisterFlags(flag.NewFlagSet("test", flag.PanicOnError))
	cfg.LogsobjBuilder.DropDuplicates = true

	var (
		ctx          = t.Context()
		logger       = log.NewNopLogger()
		reg          = prometheus.NewRegistry()
		scratchStore = scratch.NewMemory()
		bucket       = objstore.NewInMemBucket()
		idxBucket    = objstore.NewPrefixedBucket(bucket, "index/")
		committer    = &mockCommitter{}
	)
	factory, err := logsobj.NewBuilderFactory(cfg.LogsobjBuilder, scratchStore, logsobj.NewBuilderMetrics(), logger, nil)
	require.NoError(t, err)
	indexobjMetrics := indexobj.NewBuilderMetrics(reg)
	indexer, err := index.NewSimpleIndexer(cfg.IndexobjBuilder, scratchStore, logger, idxBucket, index.NewIndexerMetrics(reg), indexobjMetrics, index.NewCalculatorMetrics(reg))
	require.NoError(t, err)
	var (
		uploader       = dataobj_uploader.New(dataobj_uploader.Config{SHAPrefixSize: 2}, bucket, logger)
		flusher        = newFlusher(logsobj.NewSorter(factory, reg), uploader, logger, reg)
		tocWriter      = metastore.NewTableOfContentsWriter(idxBucket, metastore.DefaultTocWriterBackoffConfig, metastore.DefaultTocBuilderConfig, logger, metastore.NewTocWriterMetrics(reg), indexobjMetrics)
		flushCommitter = newFlushCommitter(flusher, committer, indexer, tocWriter, 0, 1, logger, reg)
		proc           = newProcessor(NewMultiObjectBuilder(factory, int(cfg.LogsobjBuilder.TargetObjectSize)), nil, flushCommitter, time.Hour, 0, logger, reg)
	)

	for _, batch := range batches {
		for _, rec := range batch {
			require.NoError(t, proc.processRecord(ctx, rec))
		}
		require.NoError(t, proc.flush(ctx, "test"))
		if serial {
			require.NoError(t, proc.waitPendingFlush())
		}
	}
	require.NoError(t, proc.waitPendingFlush())
	return maps.Clone(bucket.Objects()), committer.offsets
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
