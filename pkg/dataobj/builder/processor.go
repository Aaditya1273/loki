package builder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/loki/v3/pkg/dataobj"
	"github.com/grafana/loki/v3/pkg/kafka"
	"github.com/grafana/loki/v3/pkg/logproto"
)

type multiBuilder interface {
	Append(tenant string, stream logproto.Stream, recTime time.Time) error
	GetEstimatedSize() int
	IsFull() bool
	Reset()
	getBuilders() []builder
}

// A builder allows mocking of [logsobj.Builder] in tests.
type builder interface {
	Append(tenant string, stream logproto.Stream, recTime time.Time) error
	GetEstimatedSize() int
	IsFull() bool
	Flush() (*dataobj.Object, io.Closer, error)
	TimeRanges() []dataobj.TimeRange
	GetEarliestRecordTime() time.Time
	CopyAndSort(ctx context.Context, obj *dataobj.Object) (*dataobj.Object, io.Closer, error)
}

// A flushCommitter allows mocking of flushes in tests.
type flushCommitter interface {
	Flush(ctx context.Context, builder []builder, reason string, offset int64) error
}

// A processor receives records and builds data objects from them.
type processor struct {
	*services.BasicService
	builder        multiBuilder
	decoder        *kafka.Decoder
	records        chan *kgo.Record
	flushCommitter flushCommitter

	// lastOffset contains the offset of the last record appended to the data object
	// builder. It is used to commit the correct offset after a flush.
	lastOffset int64

	// idleFlushTimeout is the maximum amount of time to wait for more data. If no
	// records are received within this timeout, the current data object builder
	// is flushed. Most of the time, this timeout occurs when a partition is
	// marked as inactive in preparation for a scale down.
	idleFlushTimeout time.Duration

	// maxBuilderAge is the maximum age a data object builder can reach before it
	// must be flushed. This happens if a partition does not receive enough data
	// to reach the target size within the allocated time. We would rather flush
	// a small data object and compact it later then wait for more data to arrive
	// and delay querying.
	maxBuilderAge time.Duration

	// firstAppend tracks the wall clock time of the first append to the data
	// object builder. It is used to know if the builder has exceeded the
	// maximum age. It must be reset after each flush.
	firstAppend time.Time

	// lastAppend tracks the wall clock time of the last append to the data
	// object builder. It is used to know if the builder has exceeded the
	// idle timeout. It must be reset after each flush.
	lastAppend time.Time

	// pending receives the result of the batch that flushes in the
	// background. It is nil when no batch flushes. Only the goroutine that
	// calls processRecord, idleFlush and flush reads or writes the field.
	pending chan error

	metrics *metrics
	logger  log.Logger
}

func newProcessor(
	builder multiBuilder,
	records chan *kgo.Record,
	flushCommitter flushCommitter,
	idleFlushTimeout time.Duration,
	maxBuilderAge time.Duration,
	logger log.Logger,
	reg prometheus.Registerer,
) *processor {
	decoder, err := kafka.NewDecoder()
	if err != nil {
		panic(err)
	}
	p := &processor{
		builder:          builder,
		decoder:          decoder,
		records:          records,
		flushCommitter:   flushCommitter,
		idleFlushTimeout: idleFlushTimeout,
		maxBuilderAge:    maxBuilderAge,
		metrics:          newMetrics(reg),
		logger:           logger,
	}
	p.BasicService = services.NewBasicService(p.starting, p.running, p.stopping).WithName("dataobj-builder-processor")
	return p
}

// starting implements [services.StartingFn].
func (p *processor) starting(_ context.Context) error {
	return nil
}

// running implements [services.RunningFn].
func (p *processor) running(ctx context.Context) error {
	return p.Run(ctx)
}

// stopping implements [services.StoppingFn].
func (p *processor) stopping(_ error) error {
	return nil
}

var errUnrecoverableFlush = fmt.Errorf("unrecoverable flush failure")

func (p *processor) Run(ctx context.Context) (returnErr error) {
	defer level.Info(p.logger).Log("msg", "stopped partition processor")
	level.Info(p.logger).Log("msg", "started partition processor")

	// A background flush must not outlive the processor. It uses ctx, so it
	// stops soon after a shutdown cancels ctx.
	defer func() {
		if err := p.checkFlushResult(p.waitPendingFlush()); returnErr == nil {
			returnErr = err
		}
	}()

	for {
		select {
		case <-ctx.Done():
			level.Info(p.logger).Log("msg", "context canceled")
			// We don't return ctx.Err() here as it manifests as a service failure
			// when stopping the service.
			return nil
		case err := <-p.pending:
			// A failure stops the processor now, not at the next flush, so it
			// does not append records that it can never commit.
			p.pending = nil
			if err := p.checkFlushResult(err); err != nil {
				return err
			}
		case rec, ok := <-p.records:
			if !ok {
				level.Info(p.logger).Log("msg", "channel closed")
				return nil
			}
			if err := p.processRecord(ctx, rec); err != nil {
				if errors.Is(err, errUnrecoverableFlush) {
					// restarting to re-read Kafka records
					level.Error(p.logger).Log("msg", "terminating after unrecoverable flush", "err", err)
					return err
				}
				level.Error(p.logger).Log("msg", "failed to process record", "err", err)
				p.observeRecordErr(rec)
			}
		case <-time.After(p.idleFlushTimeout):
			// This partition is idle, flush it.
			p.metrics.setConsumptionLag(0)
			if _, err := p.idleFlush(ctx); err != nil {
				if errors.Is(err, errUnrecoverableFlush) {
					// restarting to re-read Kafka records
					level.Error(p.logger).Log("msg", "terminating after unrecoverable idle flush", "err", err)
					return err
				}
				level.Error(p.logger).Log("msg", "failed to idle flush", "err", err)
			}
		}
	}
}

func (p *processor) processRecord(ctx context.Context, rec *kgo.Record) error {
	// We use the current time to calculate a number of metrics, such as
	// consumption lag, the age of the data object builder, etc.
	now := time.Now()
	p.observeRecord(rec, now)

	// Try to decode the stream in the record.
	tenant := string(rec.Key)
	stream, err := p.decoder.DecodeWithoutLabels(rec.Value)
	if err != nil {
		// This is an unrecoverable error and no amount of retries will fix it.
		return fmt.Errorf("failed to decode stream: %w", err)
	}

	if p.shouldFlushDueToMaxAge() {
		if err := p.flush(ctx, flushReasonMaxAge); err != nil {
			return fmt.Errorf("failed to flush: %w", err)
		}
	}

	if p.builder.IsFull() {
		if err := p.flush(ctx, flushReasonBuilderFull); err != nil {
			return fmt.Errorf("failed to flush: %w", err)
		}
	}

	if err := p.builder.Append(tenant, stream, rec.Timestamp); err != nil {
		return fmt.Errorf("failed to append stream: %w", err)
	}
	p.metrics.sizeEstimate.Set(float64(p.builder.GetEstimatedSize()))

	if p.firstAppend.IsZero() {
		p.firstAppend = now
	}
	p.lastAppend = now
	p.lastOffset = rec.Offset

	return nil
}

func (p *processor) shouldFlushDueToMaxAge() bool {
	return p.maxBuilderAge > 0 &&
		p.builder.GetEstimatedSize() > 0 &&
		!p.firstAppend.IsZero() &&
		time.Since(p.firstAppend) > p.maxBuilderAge
}

// idleFlush flushes the partition if it has exceeded the idle flush timeout.
// It returns true if the partition was flushed, false with a non-nil error
// if the partition could not be flushed, and false with a nil error if
// the partition has not exceeded the timeout.
func (p *processor) idleFlush(ctx context.Context) (bool, error) {
	if !p.needsIdleFlush() {
		return false, nil
	}
	if err := p.flush(ctx, flushReasonIdle); err != nil {
		return false, err
	}
	return true, nil
}

// needsIdleFlush returns true if the partition has exceeded the idle timeout
// and the builder has some data buffered.
func (p *processor) needsIdleFlush() bool {
	if p.lastAppend.IsZero() {
		return false
	}
	// This is a safety check to make sure we never flush empty data objects.
	// It should never happen that lastAppend is non-zero while the builder
	// is empty.
	if p.builder.GetEstimatedSize() == 0 {
		return false
	}
	return time.Since(p.lastAppend) > p.idleFlushTimeout
}

// flush hands the appended records to a background goroutine, which flushes
// them as one batch and commits the offset of the last record. The processor
// keeps appending records while the batch flushes. The error of the batch
// comes back from the next flush, or from [processor.Run].
//
// flush first waits for the previous batch, so batches commit their offsets
// in order, and at most two batches are in memory.
func (p *processor) flush(ctx context.Context, reason string) error {
	builders, offset := p.builder.getBuilders(), p.lastOffset

	// Reset the state to prepare for building the next data objects. Reset
	// drops the builders from p.builder, and the batch keeps them.
	p.firstAppend = time.Time{}
	p.lastAppend = time.Time{}
	p.builder.Reset()
	p.metrics.sizeEstimate.Set(0)

	if err := p.waitPendingFlush(); err != nil {
		return err
	}

	// The buffer lets the goroutine exit before anyone reads the result.
	pending := make(chan error, 1)
	p.pending = pending
	go func() { pending <- p.flushBatch(ctx, builders, reason, offset) }()
	return nil
}

func (p *processor) flushBatch(ctx context.Context, builders []builder, reason string, offset int64) error {
	err := p.flushCommitter.Flush(ctx, builders, reason, offset)
	if err == nil {
		return nil
	}

	// A canceled ctx means that the processor stops. Every later flush uses
	// the same ctx, so it cannot commit a later offset, and a restart reads
	// this batch again from the last commit.
	if ctx.Err() != nil {
		return err
	}

	// The builders of this batch are already reset, so the processor cannot
	// flush them again. The error is unrecoverable even when it wraps
	// context.Canceled: if the processor went on, the next batch would commit
	// a later offset and skip the records of this one.
	return fmt.Errorf("%w: %s flush up to offset %d: %w", errUnrecoverableFlush, reason, offset, err)
}

// waitPendingFlush waits for the background flush to end and returns its
// error. It returns nil when no flush runs in the background.
func (p *processor) waitPendingFlush() error {
	if p.pending == nil {
		return nil
	}
	err := <-p.pending
	p.pending = nil
	return err
}

// checkFlushResult returns err if a background flush failed with an
// unrecoverable error, and nil otherwise.
func (p *processor) checkFlushResult(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, errUnrecoverableFlush):
		level.Error(p.logger).Log("msg", "terminating after unrecoverable background flush", "err", err)
		return err
	default:
		level.Info(p.logger).Log("msg", "background flush stopped by shutdown", "err", err)
		return nil
	}
}

func (p *processor) observeRecord(rec *kgo.Record, now time.Time) {
	p.metrics.records.Inc()
	p.metrics.receivedBytes.Add(float64(len(rec.Value)))
	p.metrics.setLastOffset(rec.Offset)
	p.metrics.setConsumptionLag(now.Sub(rec.Timestamp))
}

func (p *processor) observeRecordErr(rec *kgo.Record) {
	p.metrics.recordFailures.Inc()
	p.metrics.discardedBytes.Add(float64(len(rec.Value)))
}
