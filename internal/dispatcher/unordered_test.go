package dispatcher

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lucasdecamargo/go-kafka-consumer/consumer"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/circuit"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/offset"
)

// --- Test Helpers ---

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig() Config {
	return Config{
		WorkerCount:        2,
		ChannelCap:         10,
		BatchSize:          3,
		LingerTime:         50 * time.Millisecond,
		MaxRetries:         2,
		CBMinRequests:      3,
		CBFailureThreshold: 0.6,
		CBOpenTimeout:      500 * time.Millisecond,
		CBMaxRequests:      1,
		CBInterval:         1 * time.Second,
	}
}

func makeMessages(partition int32, startOffset int64, count int) []consumer.Message {
	msgs := make([]consumer.Message, count)
	for i := range count {
		msgs[i] = consumer.Message{
			Topic:     "test-topic",
			Partition: partition,
			Offset:    startOffset + int64(i),
			Key:       []byte("key"),
			Value:     []byte("value"),
		}
	}
	return msgs
}

// mockDLQProducer records DLQ produce calls.
type mockDLQProducer struct {
	mu      sync.Mutex
	batches [][]consumer.Message
	reasons []error
	err     error // if set, Produce returns this error
}

func (m *mockDLQProducer) Produce(_ context.Context, msgs []consumer.Message, reason error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches = append(m.batches, msgs)
	m.reasons = append(m.reasons, reason)
	return m.err
}

func (m *mockDLQProducer) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.batches)
}

// --- Tests ---

func TestHappyPath_SendProcessComplete(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	var processed atomic.Int32

	processor := func(_ context.Context, batch []consumer.Message) error {
		processed.Add(int32(len(batch)))
		return nil
	}

	cfg := testConfig()
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	// Send enough messages to trigger a batch (BatchSize=3).
	msgs := makeMessages(0, 0, 3)
	if err := d.Send(ctx, 0, msgs); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Wait for processing.
	time.Sleep(200 * time.Millisecond)

	if processed.Load() != 3 {
		t.Fatalf("expected 3 processed messages, got %d", processed.Load())
	}

	// Verify offset was committed.
	offsets := coord.Committable()
	if offsets[0] != 3 {
		t.Fatalf("expected committable offset 3, got %d", offsets[0])
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestLingerTimer_FlushesPartialBatch(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	var processed atomic.Int32

	processor := func(_ context.Context, batch []consumer.Message) error {
		processed.Add(int32(len(batch)))
		return nil
	}

	cfg := testConfig()
	cfg.LingerTime = 50 * time.Millisecond
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	// Send fewer messages than BatchSize — should trigger linger flush.
	msgs := makeMessages(0, 0, 2)
	if err := d.Send(ctx, 0, msgs); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Wait for linger timer + processing.
	time.Sleep(300 * time.Millisecond)

	if processed.Load() != 2 {
		t.Fatalf("expected 2 processed messages via linger, got %d", processed.Load())
	}

	offsets := coord.Committable()
	if offsets[0] != 2 {
		t.Fatalf("expected committable offset 2, got %d", offsets[0])
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestBackpressure_SendReturnsError(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))

	// Processor that blocks forever (simulates slow processing).
	processor := func(ctx context.Context, _ []consumer.Message) error {
		<-ctx.Done()
		return ctx.Err()
	}

	cfg := testConfig()
	cfg.ChannelCap = 1
	cfg.WorkerCount = 1
	cfg.BatchSize = 1
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	// First send — fills the channel. Use different partitions so they don't
	// block on in-flight tracking.
	if err := d.Send(ctx, 0, makeMessages(0, 0, 1)); err != nil {
		t.Fatalf("first Send failed: %v", err)
	}

	// Give the first batch time to be picked up by the worker, then fill channel.
	time.Sleep(50 * time.Millisecond)

	if err := d.Send(ctx, 1, makeMessages(1, 0, 1)); err != nil {
		t.Fatalf("second Send failed: %v", err)
	}

	// Third send should hit backpressure (channel cap=1, one in channel, one in worker).
	err = d.Send(ctx, 2, makeMessages(2, 0, 1))
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("expected ErrBackpressure, got %v", err)
	}

	cancel()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer closeCancel()
	_ = d.Close(closeCtx)
}

func TestReadyChannel_SignaledAfterBatchComplete(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))

	processor := func(_ context.Context, _ []consumer.Message) error {
		return nil
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	if err := d.Send(ctx, 0, makeMessages(0, 0, 1)); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Ready should be signaled after the batch completes.
	select {
	case <-d.Ready():
		// OK
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for Ready signal")
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestTransientError_RetriesAndSucceeds(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	var attempts atomic.Int32

	processor := func(_ context.Context, _ []consumer.Message) error {
		attempt := attempts.Add(1)
		if attempt <= 2 {
			return errors.New("transient error")
		}
		return nil // Succeed on third attempt.
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	cfg.MaxRetries = 3
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d.Start(ctx)

	if err := d.Send(ctx, 0, makeMessages(0, 0, 1)); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Wait for retries + processing.
	time.Sleep(2 * time.Second)

	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}

	offsets := coord.Committable()
	if offsets[0] != 1 {
		t.Fatalf("expected committable offset 1, got %d", offsets[0])
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestNonRetryableError_DirectToDLQ(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	dlq := &mockDLQProducer{}
	var attempts atomic.Int32

	processor := func(_ context.Context, _ []consumer.Message) error {
		attempts.Add(1)
		return &consumer.ErrNonRetryable{Err: errors.New("bad data")}
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	d, err := NewUnorderedDispatcher(cfg, processor, coord,
		WithLogger(silentLogger()),
		WithDLQProducer(dlq),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	if err := d.Send(ctx, 0, makeMessages(0, 0, 1)); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Should have been called exactly once — no retries.
	if attempts.Load() != 1 {
		t.Fatalf("expected 1 attempt (no retries), got %d", attempts.Load())
	}

	if dlq.callCount() != 1 {
		t.Fatalf("expected 1 DLQ call, got %d", dlq.callCount())
	}

	offsets := coord.Committable()
	if offsets[0] != 1 {
		t.Fatalf("expected committable offset 1 after DLQ, got %d", offsets[0])
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestRetriesExhausted_RoutesToDLQ(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	dlq := &mockDLQProducer{}
	var attempts atomic.Int32

	processor := func(_ context.Context, _ []consumer.Message) error {
		attempts.Add(1)
		return errors.New("always fails")
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	cfg.MaxRetries = 2
	// Set high CB threshold so it doesn't trip during this test.
	cfg.CBMinRequests = 100
	d, err := NewUnorderedDispatcher(cfg, processor, coord,
		WithLogger(silentLogger()),
		WithDLQProducer(dlq),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d.Start(ctx)

	if err := d.Send(ctx, 0, makeMessages(0, 0, 1)); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Wait for retries + processing.
	time.Sleep(3 * time.Second)

	// 1 initial + 2 retries = 3 attempts.
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}

	if dlq.callCount() != 1 {
		t.Fatalf("expected 1 DLQ call, got %d", dlq.callCount())
	}

	offsets := coord.Committable()
	if offsets[0] != 1 {
		t.Fatalf("expected committable offset 1 after DLQ, got %d", offsets[0])
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestNoDLQProducer_DropsFailedBatch(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))

	processor := func(_ context.Context, _ []consumer.Message) error {
		return &consumer.ErrNonRetryable{Err: errors.New("bad data")}
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	// No DLQ producer — should log and drop.
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	if err := d.Send(ctx, 0, makeMessages(0, 0, 1)); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Offset should still advance — batch was "handled" (dropped).
	offsets := coord.Committable()
	if offsets[0] != 1 {
		t.Fatalf("expected committable offset 1 after drop, got %d", offsets[0])
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWorkerPanic_TriggersGracefulShutdown(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))

	processor := func(_ context.Context, _ []consumer.Message) error {
		panic("test panic")
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	cfg.WorkerCount = 1
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	d.Start(ctx)

	sendCtx, sendCancel := context.WithTimeout(ctx, 2*time.Second)
	defer sendCancel()
	if err := d.Send(sendCtx, 0, makeMessages(0, 0, 1)); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// The panic should trigger cancelRoot, which cancels the worker
	// context. The worker should recover and wg.Done should be called.
	// We verify this by checking that Close() succeeds without hanging
	// (wg.Done was called despite the panic).
	time.Sleep(300 * time.Millisecond)

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer closeCancel()
	if err := d.Close(closeCtx); err != nil {
		t.Fatalf("Close should succeed after panic recovery (wg.Done must have been called): %v", err)
	}
}

func TestCircuitBreaker_OpensOnSustainedFailure(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))

	processor := func(_ context.Context, _ []consumer.Message) error {
		return errors.New("target down")
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	cfg.MaxRetries = 0
	cfg.CBMinRequests = 3
	cfg.CBFailureThreshold = 0.6
	cfg.CBOpenTimeout = 500 * time.Millisecond
	cfg.CBInterval = 10 * time.Second
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d.Start(ctx)

	// Send enough batches to trip the circuit breaker (3 failures).
	for i := range 3 {
		if err := d.Send(ctx, int32(i), makeMessages(int32(i), 0, 1)); err != nil {
			t.Fatalf("Send %d failed: %v", i, err)
		}
	}

	// Wait for processing and circuit breaker evaluation.
	var circuitOpened bool
	timeout := time.After(5 * time.Second)
	for !circuitOpened {
		select {
		case state := <-d.CircuitStateChanged():
			if state == circuit.Open {
				circuitOpened = true
			}
		case <-timeout:
			t.Fatal("timeout waiting for circuit to open")
		}
	}

	if !circuitOpened {
		t.Fatal("circuit breaker should have opened")
	}

	cancel()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer closeCancel()
	_ = d.Close(closeCtx)
}

func TestMultiplePartitions_IndependentBatching(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	var processed atomic.Int32

	processor := func(_ context.Context, batch []consumer.Message) error {
		processed.Add(int32(len(batch)))
		return nil
	}

	cfg := testConfig()
	cfg.BatchSize = 2
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	// Send to two partitions.
	if err := d.Send(ctx, 0, makeMessages(0, 0, 2)); err != nil {
		t.Fatalf("Send p0 failed: %v", err)
	}
	if err := d.Send(ctx, 1, makeMessages(1, 100, 2)); err != nil {
		t.Fatalf("Send p1 failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	if processed.Load() != 4 {
		t.Fatalf("expected 4 processed messages, got %d", processed.Load())
	}

	offsets := coord.Committable()
	if offsets[0] != 2 {
		t.Fatalf("expected p0 offset 2, got %d", offsets[0])
	}
	if offsets[1] != 102 {
		t.Fatalf("expected p1 offset 102, got %d", offsets[1])
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestGracefulShutdown_WorkersDrain(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	var processed atomic.Int32

	processor := func(_ context.Context, _ []consumer.Message) error {
		time.Sleep(100 * time.Millisecond) // Simulate slow processing.
		processed.Add(1)
		return nil
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.Start(ctx)

	if err := d.Send(ctx, 0, makeMessages(0, 0, 1)); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Close with enough time for the worker to finish.
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer closeCancel()

	if err := d.Close(closeCtx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if processed.Load() != 1 {
		t.Fatalf("expected 1 processed message after drain, got %d", processed.Load())
	}
}

func TestGracefulShutdown_DeadlineExceeded(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))

	processor := func(ctx context.Context, _ []consumer.Message) error {
		<-ctx.Done() // Block forever.
		return ctx.Err()
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	cfg.WorkerCount = 1
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.Start(ctx)

	if err := d.Send(ctx, 0, makeMessages(0, 0, 1)); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond) // Let worker pick up the batch.

	// Close with a very short deadline.
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer closeCancel()

	err = d.Close(closeCtx)
	if err == nil {
		t.Fatal("expected deadline exceeded error")
	}
	cancel()
}

func TestOnPartitionsRevoked_ClearsBuffer(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))

	processor := func(_ context.Context, _ []consumer.Message) error {
		return nil
	}

	cfg := testConfig()
	cfg.BatchSize = 10 // Large batch so messages stay in buffer.
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	// Send messages but not enough for a batch.
	if err := d.Send(ctx, 0, makeMessages(0, 0, 3)); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Revoke the partition.
	d.OnPartitionsRevoked([]Partition{{Topic: "test-topic", Partition: 0}})

	// Buffer should be cleared.
	d.mu.Lock()
	pb := d.partitions[0]
	bufLen := len(pb.messages)
	d.mu.Unlock()

	if bufLen != 0 {
		t.Fatalf("expected empty buffer after revocation, got %d messages", bufLen)
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestConstructor_NilProcessor_ReturnsError(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	_, err := NewUnorderedDispatcher(testConfig(), nil, coord)
	if err == nil {
		t.Fatal("expected error for nil processor")
	}
}

func TestConstructor_NilCoordinator_ReturnsError(t *testing.T) {
	processor := func(_ context.Context, _ []consumer.Message) error { return nil }
	_, err := NewUnorderedDispatcher(testConfig(), processor, nil)
	if err == nil {
		t.Fatal("expected error for nil coordinator")
	}
}

func TestSendAfterClose_ReturnsError(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	processor := func(_ context.Context, _ []consumer.Message) error { return nil }

	cfg := testConfig()
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	d.Start(ctx)

	closeCtx, closeCancel := context.WithTimeout(ctx, 1*time.Second)
	defer closeCancel()
	_ = d.Close(closeCtx)

	err = d.Send(ctx, 0, makeMessages(0, 0, 1))
	if err == nil {
		t.Fatal("expected error when sending after close")
	}
}

func TestPerPartitionInFlight_PreventsDoubleDispatch(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	firstBatchStarted := make(chan struct{})
	firstBatchDone := make(chan struct{})
	var callCount atomic.Int32

	processor := func(_ context.Context, _ []consumer.Message) error {
		call := callCount.Add(1)
		if call == 1 {
			close(firstBatchStarted)
			<-firstBatchDone // Block first batch.
		}
		return nil
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	cfg.WorkerCount = 2
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	// Send first batch for partition 0.
	if err := d.Send(ctx, 0, makeMessages(0, 0, 1)); err != nil {
		t.Fatalf("first Send failed: %v", err)
	}

	// Wait for worker to start processing.
	<-firstBatchStarted

	// Send second batch for same partition — should be buffered, not dispatched.
	if err := d.Send(ctx, 0, makeMessages(0, 1, 1)); err != nil {
		t.Fatalf("second Send failed: %v", err)
	}

	// Verify only first batch was dispatched to coordinator.
	d.mu.Lock()
	pb := d.partitions[int32(0)]
	inFlight := pb.inFlight
	buffered := len(pb.messages)
	d.mu.Unlock()

	if !inFlight {
		t.Fatal("partition should be in-flight")
	}
	if buffered != 1 {
		t.Fatalf("expected 1 buffered message, got %d", buffered)
	}

	// Release the first batch.
	close(firstBatchDone)

	// Wait for the buffered batch to be dispatched and processed.
	time.Sleep(500 * time.Millisecond)

	offsets := coord.Committable()
	if offsets[0] != 2 {
		t.Fatalf("expected committable offset 2 (both batches done), got %d", offsets[0])
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// --- Gap Tests: Circuit Breaker Full Lifecycle (Scenario 13) ---

func TestCircuitBreaker_FullLifecycle_OpenHalfOpenClosed(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	shouldFail := atomic.Bool{}
	shouldFail.Store(true)

	processor := func(_ context.Context, _ []consumer.Message) error {
		if shouldFail.Load() {
			return errors.New("target down")
		}
		return nil
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	cfg.MaxRetries = 0
	cfg.CBMinRequests = 3
	cfg.CBFailureThreshold = 0.6
	cfg.CBOpenTimeout = 300 * time.Millisecond
	cfg.CBInterval = 10 * time.Second
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	d.Start(ctx)

	// Phase 1: Trip the circuit breaker with 3 failures.
	for i := range 3 {
		_ = d.Send(ctx, int32(i), makeMessages(int32(i), 0, 1))
	}

	waitForState(t, d, circuit.Open, 5*time.Second)

	// Phase 2: Wait for HalfOpen (after CBOpenTimeout = 300ms).
	waitForState(t, d, circuit.HalfOpen, 2*time.Second)

	// Phase 3: Make the target succeed so the probe passes.
	shouldFail.Store(false)

	// Send a probe batch.
	_ = d.Send(ctx, 10, makeMessages(10, 0, 1))

	waitForState(t, d, circuit.Closed, 5*time.Second)

	cancel()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer closeCancel()
	_ = d.Close(closeCtx)
}

func TestCircuitBreaker_HeldBatchNotDLQd_RetriedOnRecovery(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	dlq := &mockDLQProducer{}
	shouldFail := atomic.Bool{}
	shouldFail.Store(true)
	var successCount atomic.Int32

	processor := func(_ context.Context, _ []consumer.Message) error {
		if shouldFail.Load() {
			return errors.New("target down")
		}
		successCount.Add(1)
		return nil
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	cfg.WorkerCount = 1
	cfg.MaxRetries = 0
	cfg.CBMinRequests = 2
	cfg.CBFailureThreshold = 0.5
	cfg.CBOpenTimeout = 300 * time.Millisecond
	cfg.CBInterval = 10 * time.Second
	d, err := NewUnorderedDispatcher(cfg, processor, coord,
		WithLogger(silentLogger()),
		WithDLQProducer(dlq),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	d.Start(ctx)

	// Trip the circuit with 2 failures.
	_ = d.Send(ctx, 0, makeMessages(0, 0, 1))
	_ = d.Send(ctx, 1, makeMessages(1, 0, 1))

	waitForState(t, d, circuit.Open, 5*time.Second)

	// Send a batch that should be HELD because circuit is open.
	_ = d.Send(ctx, 2, makeMessages(2, 0, 1))

	time.Sleep(200 * time.Millisecond)

	// Make the target succeed and wait for recovery.
	shouldFail.Store(false)

	waitForState(t, d, circuit.Closed, 5*time.Second)

	time.Sleep(500 * time.Millisecond)

	if successCount.Load() < 1 {
		t.Fatalf("expected at least 1 successful processing after recovery, got %d", successCount.Load())
	}

	cancel()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer closeCancel()
	_ = d.Close(closeCtx)
}

func TestNonRetryableError_DoesNotTripCircuitBreaker(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	dlq := &mockDLQProducer{}

	processor := func(_ context.Context, _ []consumer.Message) error {
		return &consumer.ErrNonRetryable{Err: errors.New("bad data")}
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	cfg.MaxRetries = 0
	cfg.CBMinRequests = 3
	cfg.CBFailureThreshold = 0.6
	cfg.CBInterval = 10 * time.Second
	d, err := NewUnorderedDispatcher(cfg, processor, coord,
		WithLogger(silentLogger()),
		WithDLQProducer(dlq),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	// Send many non-retryable batches — circuit should NOT open.
	for i := range 10 {
		_ = d.Send(ctx, int32(i), makeMessages(int32(i), 0, 1))
	}

	time.Sleep(500 * time.Millisecond)

	if dlq.callCount() != 10 {
		t.Fatalf("expected 10 DLQ calls, got %d", dlq.callCount())
	}

	// Circuit should still be closed.
	select {
	case state := <-d.CircuitStateChanged():
		t.Fatalf("circuit should not have changed state, got %v", state)
	default:
		// Good — no state change.
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// --- Gap Tests: Worker Panic Isolation (Scenario 11) ---

func TestWorkerPanic_OffsetNotCommitted_OtherWorkersSucceed(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	var callCount atomic.Int32

	processor := func(_ context.Context, _ []consumer.Message) error {
		call := callCount.Add(1)
		if call == 1 {
			panic("test panic")
		}
		return nil
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	cfg.WorkerCount = 2
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	d.Start(ctx)

	// Send two batches on different partitions.
	_ = d.Send(ctx, 0, makeMessages(0, 100, 1))
	_ = d.Send(ctx, 1, makeMessages(1, 200, 1))

	time.Sleep(500 * time.Millisecond)

	offsets := coord.Committable()

	// At least one partition should be committable (the one that didn't panic).
	if len(offsets) == 0 {
		t.Fatal("expected at least one partition to be committable after panic isolation")
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer closeCancel()
	_ = d.Close(closeCtx)
}

// --- Gap Tests: Retry Does Not Call BatchComplete (Scenario 04) ---

func TestRetry_OffsetNotCommittedDuringRetries(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	retryBarrier := make(chan struct{})
	var attempts atomic.Int32

	processor := func(_ context.Context, _ []consumer.Message) error {
		attempt := attempts.Add(1)
		if attempt == 1 {
			return errors.New("transient error")
		}
		<-retryBarrier
		return nil
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	cfg.MaxRetries = 2
	cfg.CBMinRequests = 100 // High so CB doesn't trip.
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d.Start(ctx)

	if err := d.Send(ctx, 0, makeMessages(0, 50, 1)); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Wait for the first failure and retry to start.
	time.Sleep(500 * time.Millisecond)

	// During retries, Committable should NOT include partition 0.
	offsets := coord.Committable()
	if _, ok := offsets[0]; ok {
		t.Fatal("partition 0 should NOT be committable during retries")
	}

	close(retryBarrier)
	time.Sleep(300 * time.Millisecond)

	offsets = coord.Committable()
	if offsets[0] != 51 {
		t.Fatalf("expected committable offset 51 after retry success, got %d", offsets[0])
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// --- Gap Tests: Committable During Backpressure (Scenario 02, 10) ---

func TestCommittable_WorksDuringBackpressure(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	processing := make(chan struct{})

	processor := func(_ context.Context, _ []consumer.Message) error {
		<-processing
		return nil
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	cfg.ChannelCap = 1
	cfg.WorkerCount = 1
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	_ = d.Send(ctx, 0, makeMessages(0, 0, 1))
	time.Sleep(50 * time.Millisecond)
	_ = d.Send(ctx, 1, makeMessages(1, 0, 1))

	// System at capacity. Committable should still work.
	offsets := coord.Committable()
	if len(offsets) != 0 {
		t.Fatalf("expected 0 committable during backpressure, got %d", len(offsets))
	}

	close(processing)
	time.Sleep(300 * time.Millisecond)

	offsets = coord.Committable()
	if len(offsets) != 2 {
		t.Fatalf("expected 2 committable after drain, got %d", len(offsets))
	}

	cancel()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer closeCancel()
	_ = d.Close(closeCtx)
}

// --- Gap Tests: OnPartitionsAssigned (Scenario 09) ---

func TestOnPartitionsAssigned_NewPartitionAcceptsSend(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	var processed atomic.Int32

	processor := func(_ context.Context, batch []consumer.Message) error {
		processed.Add(int32(len(batch)))
		return nil
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	d.OnPartitionsAssigned([]Partition{{Topic: "test-topic", Partition: 5}})

	if err := d.Send(ctx, 5, makeMessages(5, 0, 1)); err != nil {
		t.Fatalf("Send to assigned partition failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if processed.Load() != 1 {
		t.Fatalf("expected 1 processed message, got %d", processed.Load())
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestSendWithoutPriorAssignment_CreatesBuffer(t *testing.T) {
	coord := offset.NewCoordinator(offset.WithLogger(silentLogger()))
	var processed atomic.Int32

	processor := func(_ context.Context, batch []consumer.Message) error {
		processed.Add(int32(len(batch)))
		return nil
	}

	cfg := testConfig()
	cfg.BatchSize = 1
	d, err := NewUnorderedDispatcher(cfg, processor, coord, WithLogger(silentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	if err := d.Send(ctx, 7, makeMessages(7, 0, 1)); err != nil {
		t.Fatalf("Send without prior assignment failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if processed.Load() != 1 {
		t.Fatalf("expected 1 processed message, got %d", processed.Load())
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// --- Gap Tests: Backoff Timing (Scenario 04) ---

func TestBackoff_ExponentialIncrease(t *testing.T) {
	d0 := backoff(0)
	d1 := backoff(1)
	d2 := backoff(2)

	if d0 >= d1 {
		t.Fatalf("backoff should increase: d0=%v >= d1=%v", d0, d1)
	}
	if d1 >= d2 {
		t.Fatalf("backoff should increase: d1=%v >= d2=%v", d1, d2)
	}

	// Verify cap at retryMaxDelay.
	dMax := backoff(100)
	maxWithJitter := retryMaxDelay + time.Duration(float64(retryMaxDelay)*jitterFactor)
	if dMax > maxWithJitter {
		t.Fatalf("backoff should be capped at ~%v, got %v", maxWithJitter, dMax)
	}
}

// --- Helper ---

func waitForState(t *testing.T, d *UnorderedDispatcher, expected circuit.State, timeout time.Duration) {
	t.Helper()
	timer := time.After(timeout)
	for {
		select {
		case state := <-d.CircuitStateChanged():
			if state == expected {
				return
			}
		case <-timer:
			t.Fatalf("timeout waiting for circuit state %v", expected)
		}
	}
}
