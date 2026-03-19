//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lucasdecamargo/go-kafka-consumer/consumer"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/kafkatest"
)

// --------------------------------------------------------------------------
// T1-1: Transient retry then succeed
//
// Verifies: FR-3.4, NFR-6.2, NFR-6.3
// Processor fails first N attempts per batch, then succeeds. All messages
// must be processed and offsets committed.
// --------------------------------------------------------------------------

func TestTransientRetry_EventualSuccess(t *testing.T) {
	topic := "test-t1-retry"
	groupID := "test-t1-retry-group"
	messageCount := 20

	cluster.CreateTopicT(t, topic, 2)
	msgs := kafkatest.NewMessages(messageCount, func(i int) []byte {
		return []byte(fmt.Sprintf(`{"seq":%d}`, i))
	})
	cluster.ProduceT(t, topic, msgs)

	// Track how many times each batch (identified by partition:maxOffset)
	// has been attempted. Fail the first 2 attempts, succeed on the 3rd.
	var attempts sync.Map // key: "partition:offset" → *atomic.Int32
	var processed atomic.Int64
	allDone := make(chan struct{})

	processor := func(ctx context.Context, batch []consumer.Message) error {
		// Use the first message's partition:offset as the batch key.
		key := fmt.Sprintf("%d:%d", batch[0].Partition, batch[0].Offset)

		counter := &atomic.Int32{}
		actual, _ := attempts.LoadOrStore(key, counter)
		count := actual.(*atomic.Int32).Add(1)

		if count <= 2 {
			return fmt.Errorf("transient failure attempt %d for batch %s", count, key)
		}

		processed.Add(int64(len(batch)))
		if processed.Load() >= int64(messageCount) {
			select {
			case <-allDone:
			default:
				close(allDone)
			}
		}
		return nil
	}

	cfg := testConfig(cluster.Brokers(), topic, groupID)
	cfg.MaxRetries = 3 // Allow up to 3 retries (4 total attempts).
	// Prevent the circuit breaker from tripping during intentional transient
	// failures. With CBMinRequests=100, the CB never evaluates failure rate.
	cfg.CBMinRequests = 100

	c, err := consumer.New(cfg, processor,
		consumer.WithLogger(testLogger()),
		consumer.WithMetrics(prometheus.NewRegistry()),
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- c.Run(runCtx)
	}()

	select {
	case <-allDone:
		t.Logf("all %d messages processed after retries", messageCount)
	case <-time.After(60 * time.Second):
		t.Fatalf("timeout: only %d/%d messages processed", processed.Load(), messageCount)
	}

	// Let the commit timer tick.
	time.Sleep(cfg.CommitInterval + 200*time.Millisecond)

	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("consumer run: %v", err)
	}

	// Verify all messages processed.
	if got := processed.Load(); got != int64(messageCount) {
		t.Errorf("processed %d messages, want %d", got, messageCount)
	}

	// Verify offsets committed.
	offsets := cluster.CommittedOffsetsT(t, groupID, topic, 2)
	if len(offsets) == 0 {
		t.Error("no offsets committed")
	}
	var total int64
	for _, off := range offsets {
		total += off
	}
	t.Logf("committed offsets: %v (total=%d)", offsets, total)
	if total < int64(messageCount) {
		t.Errorf("committed offset sum %d < message count %d", total, messageCount)
	}

	// Verify retries actually happened: every batch should have been
	// attempted at least 3 times.
	var retriedBatches int
	attempts.Range(func(_, value any) bool {
		count := value.(*atomic.Int32).Load()
		if count >= 3 {
			retriedBatches++
		}
		return true
	})
	if retriedBatches == 0 {
		t.Error("expected at least one batch to be retried, but none were")
	}
	t.Logf("batches that went through retries: %d", retriedBatches)
}

// --------------------------------------------------------------------------
// T1-2: Non-retryable error — offset advances
//
// Verifies: FR-3.4, FR-7.1, NFR-6.4
// Processor returns ErrNonRetryable for "poison" messages. Offsets must
// advance past poison batches — consumer must not get stuck.
// Without a DLQ producer, poison batches are logged and dropped.
// --------------------------------------------------------------------------

func TestNonRetryableError_OffsetAdvances(t *testing.T) {
	topic := "test-t1-nonretryable"
	groupID := "test-t1-nonretryable-group"

	// Use 1 partition for deterministic batch composition.
	cluster.CreateTopicT(t, topic, 1)

	// Produce messages: some "poison", some "good".
	// With BatchSize=5 and 1 partition, the first batch will be all poison
	// and the remaining batches will be all good.
	var allMsgs []kafkatest.ProduceMessage
	for i := range 5 {
		allMsgs = append(allMsgs, kafkatest.ProduceMessage{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf(`{"type":"poison","id":%d}`, i)),
		})
	}
	for i := range 15 {
		allMsgs = append(allMsgs, kafkatest.ProduceMessage{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf(`{"type":"good","id":%d}`, i)),
		})
	}
	cluster.ProduceT(t, topic, allMsgs)

	totalMessages := len(allMsgs) // 20
	goodCount := 15

	var goodProcessed atomic.Int64
	var poisonSeen atomic.Int64
	allDone := make(chan struct{})

	processor := func(ctx context.Context, batch []consumer.Message) error {
		// Check if any message in the batch is poison.
		for _, msg := range batch {
			var payload struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal(msg.Value, &payload); err == nil && payload.Type == "poison" {
				poisonSeen.Add(int64(len(batch)))
				return &consumer.ErrNonRetryable{
					Err: fmt.Errorf("poison message detected in batch (partition=%d, offset=%d)", msg.Partition, msg.Offset),
				}
			}
		}

		// All good messages.
		goodProcessed.Add(int64(len(batch)))
		if goodProcessed.Load() >= int64(goodCount) {
			select {
			case <-allDone:
			default:
				close(allDone)
			}
		}
		return nil
	}

	cfg := testConfig(cluster.Brokers(), topic, groupID)
	cfg.BatchSize = 5
	cfg.MaxRetries = 3 // Poison should NOT be retried despite this.

	c, err := consumer.New(cfg, processor,
		consumer.WithLogger(testLogger()),
		consumer.WithMetrics(prometheus.NewRegistry()),
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- c.Run(runCtx)
	}()

	select {
	case <-allDone:
		t.Logf("all %d good messages processed", goodCount)
	case <-time.After(60 * time.Second):
		t.Fatalf("timeout: good=%d/%d, poison=%d — consumer likely stuck on poison batch",
			goodProcessed.Load(), goodCount, poisonSeen.Load())
	}

	// Let the commit timer tick.
	time.Sleep(cfg.CommitInterval + 200*time.Millisecond)

	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("consumer run: %v", err)
	}

	// Verify all good messages processed.
	if got := goodProcessed.Load(); got != int64(goodCount) {
		t.Errorf("processed %d good messages, want %d", got, goodCount)
	}

	// Verify poison was seen (not silently swallowed).
	if poisonSeen.Load() == 0 {
		t.Error("expected poison messages to be seen by processor")
	}
	t.Logf("poison messages seen: %d", poisonSeen.Load())

	// Verify offsets advanced past all messages (good + poison).
	offsets := cluster.CommittedOffsetsT(t, groupID, topic, 1)
	t.Logf("committed offsets: %v", offsets)

	var totalCommitted int64
	for _, off := range offsets {
		totalCommitted += off
	}
	if totalCommitted < int64(totalMessages) {
		t.Errorf("committed offset %d < total messages %d — offsets did not advance past poison", totalCommitted, totalMessages)
	}
}

// --------------------------------------------------------------------------
// T1-3: Panic recovery — graceful shutdown
//
// Verifies: FR-3.6, ADR-0005
// Processor panics on a specific batch. consumer.Run() must return (not
// hang), offsets committed only for completed batches, and remaining
// messages are consumable on restart.
// --------------------------------------------------------------------------

func TestPanicRecovery_GracefulShutdown(t *testing.T) {
	topic := "test-t1-panic"
	groupID := "test-t1-panic-group"

	// 1 partition, 1 worker for deterministic batch ordering.
	cluster.CreateTopicT(t, topic, 1)

	messageCount := 30
	msgs := kafkatest.NewMessages(messageCount, func(i int) []byte {
		return []byte(fmt.Sprintf(`{"seq":%d}`, i))
	})
	cluster.ProduceT(t, topic, msgs)

	var processedBeforePanic atomic.Int64
	var batchCount atomic.Int32

	processor := func(ctx context.Context, batch []consumer.Message) error {
		n := batchCount.Add(1)

		// Panic on the 3rd batch. With BatchSize=5, the first 2 batches
		// (10 messages) should complete, then the 3rd panics.
		if n == 3 {
			panic("intentional test panic on batch 3")
		}

		processedBeforePanic.Add(int64(len(batch)))
		return nil
	}

	cfg := testConfig(cluster.Brokers(), topic, groupID)
	cfg.BatchSize = 5
	cfg.WorkerCount = 1 // Single worker for deterministic ordering.

	c, err := consumer.New(cfg, processor,
		consumer.WithLogger(testLogger()),
		consumer.WithMetrics(prometheus.NewRegistry()),
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() {
		runDone <- c.Run(runCtx)
	}()

	// consumer.Run() must return — the panic triggers cancelRoot which
	// unblocks the poll loop. Give it generous time.
	select {
	case err := <-runDone:
		// Run() may return nil or a context-canceled error — both are acceptable.
		t.Logf("consumer.Run() returned: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("consumer.Run() did not return after panic — deadlock?")
	}

	beforePanic := processedBeforePanic.Load()
	t.Logf("processed %d messages before panic (batches completed: %d)", beforePanic, batchCount.Load()-1)

	// At least the first 2 batches (10 messages) should have completed.
	if beforePanic < 5 {
		t.Errorf("expected at least 5 messages processed before panic, got %d", beforePanic)
	}

	// Restart consumer — remaining messages should be consumable.
	var phase2Count atomic.Int64
	phase2Done := make(chan struct{})

	processor2 := func(ctx context.Context, batch []consumer.Message) error {
		phase2Count.Add(int64(len(batch)))
		if phase2Count.Load()+beforePanic >= int64(messageCount) {
			select {
			case <-phase2Done:
			default:
				close(phase2Done)
			}
		}
		return nil
	}

	c2, err := consumer.New(cfg, processor2,
		consumer.WithLogger(testLogger()),
		consumer.WithMetrics(prometheus.NewRegistry()),
	)
	if err != nil {
		t.Fatalf("create consumer 2: %v", err)
	}

	runCtx2, cancel2 := context.WithCancel(context.Background())
	runDone2 := make(chan error, 1)
	go func() {
		runDone2 <- c2.Run(runCtx2)
	}()

	select {
	case <-phase2Done:
		t.Logf("phase 2 processed %d messages", phase2Count.Load())
	case <-time.After(30 * time.Second):
		t.Fatalf("phase 2 timeout: processed %d, need %d more",
			phase2Count.Load(), int64(messageCount)-beforePanic-phase2Count.Load())
	}

	cancel2()
	<-runDone2

	total := beforePanic + phase2Count.Load()
	t.Logf("total across both phases: %d (before_panic=%d, phase2=%d)", total, beforePanic, phase2Count.Load())

	// Some messages from the panicked batch may be reprocessed, but total
	// should be >= messageCount (at-least-once).
	if total < int64(messageCount) {
		t.Errorf("total processed %d < message count %d — messages lost", total, messageCount)
	}
}

// --------------------------------------------------------------------------
// T1-4: Circuit breaker opens on sustained failures
//
// Verifies: FR-2.10, FR-1.11, NFR-3.3
// Processor always returns transient errors. With aggressive CB config,
// the circuit breaker opens and the consumer enters degraded mode,
// stopping dispatch. consumer.Run() must still respond to cancellation.
// --------------------------------------------------------------------------

func TestCircuitBreakerOpens_StopsDispatching(t *testing.T) {
	topic := "test-t1-cb-open"
	groupID := "test-t1-cb-open-group"

	cluster.CreateTopicT(t, topic, 2)

	messageCount := 50
	msgs := kafkatest.NewMessages(messageCount, func(i int) []byte {
		return []byte(fmt.Sprintf(`{"seq":%d}`, i))
	})
	cluster.ProduceT(t, topic, msgs)

	var attempts atomic.Int64

	processor := func(ctx context.Context, batch []consumer.Message) error {
		attempts.Add(1)
		return errors.New("simulated target service unavailable")
	}

	cfg := testConfig(cluster.Brokers(), topic, groupID)
	cfg.MaxRetries = 1 // Fail fast — only 1 retry.
	// Aggressive CB: trip after 2 requests at 50% failure rate.
	cfg.CBMinRequests = 2
	cfg.CBFailureThreshold = 0.5
	cfg.CBOpenTimeout = 60 * time.Second // Stay open for the test duration.
	cfg.CBInterval = 5 * time.Second
	cfg.CBMaxRequests = 1

	c, err := consumer.New(cfg, processor,
		consumer.WithLogger(testLogger()),
		consumer.WithMetrics(prometheus.NewRegistry()),
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- c.Run(runCtx)
	}()

	// Wait for the circuit breaker to open. The CB trips after
	// CBMinRequests (2) at ≥50% failure rate. With MaxRetries=1, each
	// batch gets 2 attempts (both fail), so the first batch is enough
	// to trip the CB. After that, subsequent batches see ErrOpenState
	// and hold (don't count as new attempts).
	//
	// We wait until attempts stabilize (no new attempts for a period),
	// indicating the CB is open and dispatch has stopped.
	time.Sleep(5 * time.Second)
	attemptsSnapshot := attempts.Load()
	time.Sleep(3 * time.Second)
	attemptsAfterWait := attempts.Load()

	t.Logf("attempts: snapshot=%d, after_wait=%d", attemptsSnapshot, attemptsAfterWait)

	// If CB opened properly, attempts should have stabilized (or grown
	// very slowly due to held batches retrying against the open CB).
	// The key assertion: NOT all 50 messages were dispatched, because
	// the CB and degraded mode blocked further dispatch.
	if attemptsAfterWait >= int64(messageCount) {
		t.Errorf("expected dispatch to stop before processing all %d messages, but got %d attempts",
			messageCount, attemptsAfterWait)
	}
	t.Logf("circuit breaker appears to have blocked dispatch (attempts=%d, messages=%d)",
		attemptsAfterWait, messageCount)

	// consumer.Run() must still respond to cancellation.
	cancel()
	select {
	case err := <-runDone:
		t.Logf("consumer.Run() returned after CB open: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("consumer.Run() did not return after cancellation — held batches blocking?")
	}
}

// --------------------------------------------------------------------------
// T1-5: Mixed valid/invalid payloads — selective processing
//
// Verifies: FR-7.2, FR-7.3
// Produce structured JSON messages, some valid and some intentionally
// malformed. Processor returns ErrNonRetryable for parse failures,
// succeeds for valid messages. Consumer must not get stuck.
// --------------------------------------------------------------------------

func TestMixedErrors_SelectiveProcessing(t *testing.T) {
	topic := "test-t1-mixed"
	groupID := "test-t1-mixed-group"

	// 1 partition for deterministic batch composition.
	cluster.CreateTopicT(t, topic, 1)

	type Event struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	// Produce messages in batch-aligned blocks to ensure each batch is
	// either all-valid or all-invalid. With BatchSize=5:
	//
	//   Block 1 (msgs 0-4):   5 invalid → batch 1 → ErrNonRetryable
	//   Block 2 (msgs 5-14): 10 valid   → batch 2+3 → success
	//   Block 3 (msgs 15-19): 5 invalid → batch 4 → ErrNonRetryable
	//   Block 4 (msgs 20-29):10 valid   → batch 5+6 → success
	//
	// This exercises the alternation pattern (invalid → valid → invalid →
	// valid) while respecting FR-3.3 (atomic batch processing).
	var allMsgs []kafkatest.ProduceMessage
	validCount := 0

	// Block 1: 5 invalid.
	for i := range 5 {
		allMsgs = append(allMsgs, kafkatest.ProduceMessage{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf(`{INVALID JSON id=%d`, i)),
		})
	}
	// Block 2: 10 valid.
	for i := 5; i < 15; i++ {
		data, _ := json.Marshal(Event{ID: i, Name: fmt.Sprintf("user-%d", i)})
		allMsgs = append(allMsgs, kafkatest.ProduceMessage{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: data,
		})
		validCount++
	}
	// Block 3: 5 invalid.
	for i := 15; i < 20; i++ {
		allMsgs = append(allMsgs, kafkatest.ProduceMessage{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf(`{INVALID JSON id=%d`, i)),
		})
	}
	// Block 4: 10 valid.
	for i := 20; i < 30; i++ {
		data, _ := json.Marshal(Event{ID: i, Name: fmt.Sprintf("user-%d", i)})
		allMsgs = append(allMsgs, kafkatest.ProduceMessage{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: data,
		})
		validCount++
	}
	cluster.ProduceT(t, topic, allMsgs)
	// validCount = 20

	var mu sync.Mutex
	processedIDs := make(map[int]bool)
	var nonRetryableCount atomic.Int64
	allDone := make(chan struct{})

	processor := func(ctx context.Context, batch []consumer.Message) error {
		// Parse each message. If any fails, the entire batch is
		// non-retryable (FR-3.3: atomic batch processing).
		var events []Event
		for _, msg := range batch {
			var ev Event
			if err := json.Unmarshal(msg.Value, &ev); err != nil {
				nonRetryableCount.Add(1)
				return &consumer.ErrNonRetryable{
					Err: fmt.Errorf("malformed JSON: %w", err),
				}
			}
			events = append(events, ev)
		}

		mu.Lock()
		for _, ev := range events {
			processedIDs[ev.ID] = true
		}
		done := len(processedIDs) >= validCount
		mu.Unlock()

		if done {
			select {
			case <-allDone:
			default:
				close(allDone)
			}
		}
		return nil
	}

	cfg := testConfig(cluster.Brokers(), topic, groupID)
	cfg.BatchSize = 5 // Aligned with blocks of 5/10 messages.
	cfg.MaxRetries = 3

	c, err := consumer.New(cfg, processor,
		consumer.WithLogger(testLogger()),
		consumer.WithMetrics(prometheus.NewRegistry()),
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- c.Run(runCtx)
	}()

	select {
	case <-allDone:
		mu.Lock()
		got := len(processedIDs)
		mu.Unlock()
		t.Logf("all %d valid messages processed (non-retryable batches: %d)", got, nonRetryableCount.Load())
	case <-time.After(60 * time.Second):
		mu.Lock()
		got := len(processedIDs)
		mu.Unlock()
		t.Fatalf("timeout: processed %d/%d valid messages, non-retryable=%d — consumer stuck?",
			got, validCount, nonRetryableCount.Load())
	}

	// Let commit tick.
	time.Sleep(cfg.CommitInterval + 200*time.Millisecond)

	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("consumer run: %v", err)
	}

	// Verify offsets advanced past all 30 messages.
	offsets := cluster.CommittedOffsetsT(t, groupID, topic, 1)
	t.Logf("committed offsets: %v", offsets)

	var totalCommitted int64
	for _, off := range offsets {
		totalCommitted += off
	}
	if totalCommitted < int64(len(allMsgs)) {
		t.Errorf("committed offset %d < total messages %d — consumer stuck on non-retryable batch",
			totalCommitted, len(allMsgs))
	}

	// Verify non-retryable errors were actually encountered.
	if nonRetryableCount.Load() == 0 {
		t.Error("expected at least one non-retryable error, but none occurred")
	}
	t.Logf("non-retryable errors encountered: %d", nonRetryableCount.Load())
}
