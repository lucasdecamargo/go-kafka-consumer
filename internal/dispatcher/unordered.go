package dispatcher

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	gobreaker "github.com/sony/gobreaker/v2"

	"github.com/lucasdecamargo/go-kafka-consumer/internal/circuit"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/offset"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/types"
)

// ErrBackpressure is returned by Send() when the Dispatcher's internal
// channel is at capacity. The poll loop should pause the partition.
var ErrBackpressure = errors.New("dispatcher: at capacity")

// batch represents an assembled batch of messages from a single partition,
// ready to be dispatched to a worker.
type batch struct {
	partition int32
	maxOffset int64
	messages  []types.Message
}

// partitionBuffer holds buffered messages for a single partition that have
// not yet been assembled into a batch, and tracks whether the partition
// has an in-flight batch.
type partitionBuffer struct {
	messages []types.Message
	inFlight bool
	// lingerTimer fires to flush partial batches at low throughput.
	lingerTimer *time.Timer
}

// UnorderedDispatcher implements the Dispatcher interface with no message
// ordering guarantees. Messages are dispatched to a shared worker pool
// for maximum throughput.
//
// See ADR-0003 for the dispatcher design and ADR-0007 for circuit breaker
// integration.
type UnorderedDispatcher struct {
	cfg         Config
	processor   types.BatchProcessor
	coordinator offset.Coordinator
	logger      *slog.Logger
	dlqProducer DLQProducer

	// batchCh is the bounded channel that queues assembled batches for
	// workers. Its capacity (ChannelCap) determines backpressure.
	batchCh chan batch

	// readyCh signals the poll loop that capacity is available after a
	// backpressure event.
	readyCh chan struct{}

	// circuitStateCh emits circuit breaker state transitions to the poll
	// loop so it can enter/exit degraded mode.
	circuitStateCh chan circuit.State

	// cb is the circuit breaker that wraps each BatchProcessor attempt.
	cb *gobreaker.CircuitBreaker[any]

	// mu protects partitions map and closed flag.
	mu         sync.Mutex
	partitions map[int32]*partitionBuffer
	closed     bool

	// cancelRoot cancels the root context, triggering graceful shutdown.
	// Set during Run/start, used by panic recovery.
	cancelRoot context.CancelFunc

	// wg tracks in-flight workers for graceful shutdown.
	wg sync.WaitGroup

	// lingerMu protects linger timer operations to avoid races between
	// Send() (poll loop goroutine) and dispatchNext (worker goroutine
	// callback via completePartition).
	lingerMu sync.Mutex
}

// NewUnorderedDispatcher creates a new UnorderedDispatcher with the given
// configuration, processor, offset coordinator, and optional dependencies.
func NewUnorderedDispatcher(
	cfg Config,
	processor types.BatchProcessor,
	coordinator offset.Coordinator,
	opts ...UnorderedOption,
) (*UnorderedDispatcher, error) {
	if processor == nil {
		return nil, errors.New("dispatcher: batch processor must not be nil")
	}
	if coordinator == nil {
		return nil, errors.New("dispatcher: offset coordinator must not be nil")
	}

	o := defaultUnorderedOptions()
	for _, opt := range opts {
		opt(&o)
	}

	d := &UnorderedDispatcher{
		cfg:            cfg,
		processor:      processor,
		coordinator:    coordinator,
		logger:         o.logger,
		dlqProducer:    o.dlqProducer,
		batchCh:        make(chan batch, cfg.ChannelCap),
		readyCh:        make(chan struct{}, 1),
		circuitStateCh: make(chan circuit.State, 3),
		partitions:     make(map[int32]*partitionBuffer),
	}

	// Configure the circuit breaker.
	d.cb = gobreaker.NewCircuitBreaker[any](gobreaker.Settings{
		Name: "dispatcher",
		// MaxRequests is the number of requests allowed in HalfOpen state.
		MaxRequests: uint32(cfg.CBMaxRequests),
		// Interval is the cyclic period of the Closed state for clearing
		// the internal counts. If 0, internal counts are never cleared.
		Interval: cfg.CBInterval,
		// Timeout is the duration of the Open state, after which the
		// circuit transitions to HalfOpen.
		Timeout: cfg.CBOpenTimeout,
		// ReadyToTrip is called with a copy of Counts when a request fails
		// in the Closed state. It determines whether the circuit should trip.
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			if counts.Requests < uint32(cfg.CBMinRequests) {
				return false
			}
			failureRate := float64(counts.TotalFailures) / float64(counts.Requests)
			return failureRate >= cfg.CBFailureThreshold
		},
		// OnStateChange emits state transitions to the poll loop.
		OnStateChange: func(name string, from, to gobreaker.State) {
			newState := gobreakerStateToCircuit(to)
			d.logger.Info("circuit breaker state changed",
				slog.String("from", from.String()),
				slog.String("to", to.String()),
			)
			// Non-blocking send — channel has capacity 3 for buffering
			// rapid transitions.
			select {
			case d.circuitStateCh <- newState:
			default:
				d.logger.Warn("circuit state change channel full, dropping notification",
					slog.String("state", to.String()),
				)
			}
		},
	})

	return d, nil
}

// Start launches the worker pool. Must be called before Send(). The context
// is used for worker lifecycle — canceling it triggers graceful shutdown.
func (d *UnorderedDispatcher) Start(ctx context.Context) {
	ctx, d.cancelRoot = context.WithCancel(ctx)

	for range d.cfg.WorkerCount {
		d.wg.Add(1)
		go d.worker(ctx)
	}
}

// Send delivers messages from the poll loop to the Dispatcher for batch
// assembly. Messages are buffered per partition. When a batch is ready
// (size threshold reached) and the partition has no in-flight batch, the
// batch is dispatched to the worker channel.
//
// Returns ErrBackpressure if the worker channel is at capacity.
func (d *UnorderedDispatcher) Send(ctx context.Context, partition int32, msgs []types.Message) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return errors.New("dispatcher: closed")
	}

	pb, ok := d.partitions[partition]
	if !ok {
		pb = &partitionBuffer{
			messages: make([]types.Message, 0, d.cfg.BatchSize),
		}
		d.partitions[partition] = pb
	}

	pb.messages = append(pb.messages, msgs...)
	d.mu.Unlock()

	// Try to dispatch batches for this partition.
	return d.tryDispatch(ctx, partition)
}

// Ready returns a channel that is signaled when capacity becomes available
// after a backpressure event.
func (d *UnorderedDispatcher) Ready() <-chan struct{} {
	return d.readyCh
}

// CircuitStateChanged returns a channel that emits the new state on every
// circuit breaker transition.
func (d *UnorderedDispatcher) CircuitStateChanged() <-chan circuit.State {
	return d.circuitStateCh
}

// OnPartitionsAssigned notifies the Dispatcher of newly assigned partitions.
// For the unordered dispatcher, this initializes per-partition buffers.
func (d *UnorderedDispatcher) OnPartitionsAssigned(partitions []Partition) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, p := range partitions {
		if _, exists := d.partitions[p.Partition]; !exists {
			d.partitions[p.Partition] = &partitionBuffer{
				messages: make([]types.Message, 0, d.cfg.BatchSize),
			}
			d.logger.Debug("partition assigned",
				slog.Int("partition", int(p.Partition)),
				slog.String("topic", p.Topic),
			)
		}
	}
}

// OnPartitionsRevoked notifies the Dispatcher that partitions are being
// revoked. It clears buffered messages for the revoked partitions and
// stops their linger timers. In-flight batches for revoked partitions
// will complete naturally — the OffsetCoordinator tracks their offsets.
func (d *UnorderedDispatcher) OnPartitionsRevoked(partitions []Partition) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, p := range partitions {
		if pb, exists := d.partitions[p.Partition]; exists {
			d.lingerMu.Lock()
			if pb.lingerTimer != nil {
				pb.lingerTimer.Stop()
				pb.lingerTimer = nil
			}
			d.lingerMu.Unlock()

			// Clear buffered (not yet dispatched) messages.
			pb.messages = pb.messages[:0]

			d.logger.Debug("partition revoked",
				slog.Int("partition", int(p.Partition)),
				slog.String("topic", p.Topic),
			)
		}
	}
}

// Close initiates graceful shutdown. It stops accepting new messages,
// waits for in-flight workers to complete (bounded by the context
// deadline), and releases resources.
func (d *UnorderedDispatcher) Close(ctx context.Context) error {
	d.mu.Lock()
	d.closed = true

	// Stop all linger timers.
	for _, pb := range d.partitions {
		d.lingerMu.Lock()
		if pb.lingerTimer != nil {
			pb.lingerTimer.Stop()
			pb.lingerTimer = nil
		}
		d.lingerMu.Unlock()
	}
	d.mu.Unlock()

	// Close the batch channel so workers drain and exit.
	close(d.batchCh)

	// Wait for workers, bounded by context deadline.
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		d.logger.Info("dispatcher: all workers drained")
		return nil
	case <-ctx.Done():
		d.logger.Warn("dispatcher: shutdown deadline exceeded, abandoning in-flight batches")
		return ctx.Err()
	}
}

// tryDispatch attempts to dispatch a batch for the given partition.
// Called from Send() and from worker completion (via completePartition).
func (d *UnorderedDispatcher) tryDispatch(ctx context.Context, partition int32) error {
	d.mu.Lock()
	pb, ok := d.partitions[partition]
	if !ok || pb.inFlight || len(pb.messages) == 0 {
		d.mu.Unlock()
		return nil
	}

	// Check if we have enough messages for a full batch.
	if len(pb.messages) >= d.cfg.BatchSize {
		b := d.assembleBatch(pb, partition)
		pb.inFlight = true
		d.mu.Unlock()

		return d.dispatchBatch(ctx, b)
	}

	// Not enough for a full batch — start/reset the linger timer.
	d.startLingerTimer(ctx, partition)
	d.mu.Unlock()
	return nil
}

// assembleBatch slices off up to BatchSize messages from the partition
// buffer and returns a batch. Caller must hold d.mu.
func (d *UnorderedDispatcher) assembleBatch(pb *partitionBuffer, partition int32) batch {
	size := d.cfg.BatchSize
	if size > len(pb.messages) {
		size = len(pb.messages)
	}

	msgs := make([]types.Message, size)
	copy(msgs, pb.messages[:size])

	// Shift remaining messages to the front.
	remaining := copy(pb.messages, pb.messages[size:])
	pb.messages = pb.messages[:remaining]

	maxOffset := msgs[len(msgs)-1].Offset

	// Stop linger timer since we're dispatching.
	d.lingerMu.Lock()
	if pb.lingerTimer != nil {
		pb.lingerTimer.Stop()
		pb.lingerTimer = nil
	}
	d.lingerMu.Unlock()

	return batch{
		partition: partition,
		maxOffset: maxOffset,
		messages:  msgs,
	}
}

// dispatchBatch sends a batch to the worker channel. Returns ErrBackpressure
// if the channel is full.
func (d *UnorderedDispatcher) dispatchBatch(_ context.Context, b batch) error {
	d.coordinator.BatchDispatched(b.partition, b.maxOffset)

	select {
	case d.batchCh <- b:
		d.logger.Debug("batch dispatched",
			slog.Int("partition", int(b.partition)),
			slog.Int64("max_offset", b.maxOffset),
			slog.Int("size", len(b.messages)),
		)
		return nil
	default:
		// Channel full — undo the dispatch tracking.
		// The OffsetCoordinator expects BatchComplete for every
		// BatchDispatched, so we must complete it before returning.
		d.coordinator.BatchComplete(b.partition, b.maxOffset)

		// Put messages back into the buffer.
		d.mu.Lock()
		pb := d.partitions[b.partition]
		pb.inFlight = false
		pb.messages = append(b.messages, pb.messages...)
		d.mu.Unlock()

		return ErrBackpressure
	}
}

// startLingerTimer starts or resets the linger timer for a partition.
// When it fires, a partial batch is dispatched. Caller must hold d.mu.
func (d *UnorderedDispatcher) startLingerTimer(ctx context.Context, partition int32) {
	d.lingerMu.Lock()
	defer d.lingerMu.Unlock()

	pb := d.partitions[partition]
	if pb.lingerTimer != nil {
		return // Timer already running.
	}

	pb.lingerTimer = time.AfterFunc(d.cfg.LingerTime, func() {
		d.lingerMu.Lock()
		// Clear the timer reference since it has fired.
		d.mu.Lock()
		lPb, ok := d.partitions[partition]
		if ok {
			lPb.lingerTimer = nil
		}
		d.mu.Unlock()
		d.lingerMu.Unlock()

		d.flushPartition(ctx, partition)
	})
}

// flushPartition dispatches a partial batch for the given partition
// regardless of batch size. Called by the linger timer.
func (d *UnorderedDispatcher) flushPartition(ctx context.Context, partition int32) {
	d.mu.Lock()
	pb, ok := d.partitions[partition]
	if !ok || pb.inFlight || len(pb.messages) == 0 {
		d.mu.Unlock()
		return
	}

	b := d.assembleBatch(pb, partition)
	pb.inFlight = true
	d.mu.Unlock()

	if err := d.dispatchBatch(ctx, b); err != nil {
		d.logger.Warn("linger flush: backpressure, batch returned to buffer",
			slog.Int("partition", int(partition)),
		)
	}
}

// worker is the main goroutine for processing batches. Each worker
// loops on the batch channel, processes batches with retries and
// circuit breaker protection, and handles panics.
func (d *UnorderedDispatcher) worker(ctx context.Context) {
	// wg.Done MUST execute even on panic — deferred first (LIFO: runs last).
	defer d.wg.Done()

	// Panic recovery — deferred second (LIFO: runs before wg.Done).
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			d.logger.Error("worker panic recovered — triggering graceful shutdown",
				slog.Any("panic", r),
				slog.String("stack", string(stack)),
			)
			// Trigger graceful shutdown. cancelRoot is idempotent.
			if d.cancelRoot != nil {
				d.cancelRoot()
			}
		}
	}()

	for b := range d.batchCh {
		d.processBatch(ctx, b)
	}
}

// processBatch handles a single batch: classification, retries, circuit
// breaker, DLQ routing, and offset completion.
func (d *UnorderedDispatcher) processBatch(ctx context.Context, b batch) {
	var lastErr error

	for attempt := 0; attempt <= d.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := backoff(attempt - 1)
			d.logger.Debug("retrying batch",
				slog.Int("partition", int(b.partition)),
				slog.Int64("max_offset", b.maxOffset),
				slog.Int("attempt", attempt),
				slog.Duration("backoff", delay),
			)
			if err := sleepWithContext(ctx, delay); err != nil {
				// Context canceled during backoff — batch not completed.
				// It will be reprocessed on restart.
				d.logger.Warn("retry backoff interrupted by shutdown",
					slog.Int("partition", int(b.partition)),
				)
				return
			}
		}

		// Execute through the circuit breaker.
		// Non-retryable errors are returned as the result value (not the
		// error value) so gobreaker does not count them as failures.
		result, cbErr := d.cb.Execute(func() (any, error) {
			err := d.processor(ctx, b.messages)
			if err != nil {
				if types.IsNonRetryable(err) {
					// Return as result, not error — CB sees success.
					return &nonRetryableWrapper{err: err}, nil
				}
				// Transient error — CB counts as failure.
				return nil, err
			}
			return nil, nil
		})

		// Check if a non-retryable error was returned via the result.
		// Non-retryable errors bypass the CB (returned as result, not error).
		if nrw, ok := result.(*nonRetryableWrapper); ok {
			d.logger.Warn("non-retryable error, routing to DLQ",
				slog.Int("partition", int(b.partition)),
				slog.Int64("max_offset", b.maxOffset),
				slog.String("error", nrw.err.Error()),
			)
			d.sendToDLQ(ctx, b, nrw.err)
			d.coordinator.BatchComplete(b.partition, b.maxOffset)
			d.onBatchDone(b.partition, ctx)
			return
		}

		if cbErr == nil {
			// Success — processor returned nil, no CB error.
			d.coordinator.BatchComplete(b.partition, b.maxOffset)
			d.onBatchDone(b.partition, ctx)
			return
		}

		// Check if the circuit breaker is open.
		if errors.Is(cbErr, gobreaker.ErrOpenState) || errors.Is(cbErr, gobreaker.ErrTooManyRequests) {
			// Circuit is open — hold the batch, don't count as a retry.
			// Wait and try again without incrementing attempt.
			d.logger.Debug("circuit open, holding batch",
				slog.Int("partition", int(b.partition)),
				slog.Int64("max_offset", b.maxOffset),
			)
			if err := sleepWithContext(ctx, backoff(attempt)); err != nil {
				return // Shutdown.
			}
			attempt-- // Don't count this as a retry.
			continue
		}

		// Transient error — will retry (loop continues).
		lastErr = cbErr
		d.logger.Debug("transient error",
			slog.Int("partition", int(b.partition)),
			slog.Int64("max_offset", b.maxOffset),
			slog.Int("attempt", attempt),
			slog.String("error", cbErr.Error()),
		)
	}

	// Retries exhausted. Check circuit state to decide DLQ vs hold.
	if d.cb.State() == gobreaker.StateOpen {
		// Circuit is open — hold the batch for reprocessing after recovery.
		// The worker blocks here until the circuit closes or shutdown.
		d.logger.Warn("retries exhausted but circuit open, holding batch",
			slog.Int("partition", int(b.partition)),
			slog.Int64("max_offset", b.maxOffset),
		)
		d.holdUntilCircuitCloses(ctx, b)
		return
	}

	// Circuit is closed — this is a per-batch failure, route to DLQ.
	d.logger.Warn("retries exhausted, routing to DLQ",
		slog.Int("partition", int(b.partition)),
		slog.Int64("max_offset", b.maxOffset),
		slog.Int("retries", d.cfg.MaxRetries),
		slog.String("last_error", lastErr.Error()),
	)
	d.sendToDLQ(ctx, b, lastErr)
	d.coordinator.BatchComplete(b.partition, b.maxOffset)
	d.onBatchDone(b.partition, ctx)
}

// holdUntilCircuitCloses blocks the worker until the circuit breaker
// transitions to Closed or HalfOpen, then retries the batch. If the
// context is canceled (shutdown), the batch is abandoned.
func (d *UnorderedDispatcher) holdUntilCircuitCloses(ctx context.Context, b batch) {
	for {
		// Wait with backoff, checking context.
		if err := sleepWithContext(ctx, d.cfg.CBOpenTimeout/2); err != nil {
			return // Shutdown — batch abandoned, reprocessed on restart.
		}

		state := d.cb.State()
		if state == gobreaker.StateClosed || state == gobreaker.StateHalfOpen {
			// Circuit recovered — retry the batch.
			d.logger.Info("circuit recovered, retrying held batch",
				slog.Int("partition", int(b.partition)),
				slog.Int64("max_offset", b.maxOffset),
			)
			d.processBatch(ctx, b)
			return
		}
	}
}

// sendToDLQ produces the failed batch to the dead letter queue.
// If no DLQ producer is configured, the batch is logged and dropped.
func (d *UnorderedDispatcher) sendToDLQ(ctx context.Context, b batch, reason error) {
	if d.dlqProducer == nil {
		d.logger.Error("no DLQ producer configured, dropping failed batch",
			slog.Int("partition", int(b.partition)),
			slog.Int64("max_offset", b.maxOffset),
			slog.Int("messages", len(b.messages)),
			slog.String("reason", reason.Error()),
		)
		return
	}

	if err := d.dlqProducer.Produce(ctx, b.messages, reason); err != nil {
		d.logger.Error("failed to produce to DLQ",
			slog.Int("partition", int(b.partition)),
			slog.Int64("max_offset", b.maxOffset),
			slog.String("error", err.Error()),
		)
	}
}

// onBatchDone is called after a batch is fully processed (success or DLQ).
// It marks the partition as no longer in-flight and dispatches the next
// batch if buffered messages are available. Signals Ready if the channel
// was at capacity.
func (d *UnorderedDispatcher) onBatchDone(partition int32, ctx context.Context) {
	d.mu.Lock()
	pb, ok := d.partitions[partition]
	if ok {
		pb.inFlight = false
	}
	hasPending := ok && len(pb.messages) > 0
	d.mu.Unlock()

	// Signal readiness — non-blocking.
	select {
	case d.readyCh <- struct{}{}:
	default:
	}

	// Dispatch next batch for this partition if messages are buffered.
	if hasPending {
		_ = d.tryDispatch(ctx, partition)
	}
}

// nonRetryableWrapper wraps a non-retryable error so it can bypass the
// circuit breaker's failure counting. The circuit breaker sees this as
// a successful execution (not counted as a failure).
type nonRetryableWrapper struct {
	err error
}

func (e *nonRetryableWrapper) Error() string {
	return e.err.Error()
}

func (e *nonRetryableWrapper) Unwrap() error {
	return e.err
}

// gobreakerStateToCircuit converts a gobreaker.State to our circuit.State.
func gobreakerStateToCircuit(s gobreaker.State) circuit.State {
	switch s {
	case gobreaker.StateClosed:
		return circuit.Closed
	case gobreaker.StateOpen:
		return circuit.Open
	case gobreaker.StateHalfOpen:
		return circuit.HalfOpen
	default:
		return circuit.Closed
	}
}
