//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lucasdecamargo/go-kafka-consumer/consumer"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/kafkatest"
)

// --------------------------------------------------------------------------
// DLQ-1: Non-retryable errors route to DLQ with metadata headers
//
// Verifies: FR-4.1, FR-4.2, FR-4.3
// Processor returns ErrNonRetryable for "poison" messages. Those messages
// must appear in the DLQ topic with correct error metadata headers.
// --------------------------------------------------------------------------

func TestDLQ_NonRetryableMessages_RouteToTopic(t *testing.T) {
	sourceTopic := "test-dlq-source"
	dlqTopic := "test-dlq-dest"
	groupID := "test-dlq-group"

	// 1 partition for deterministic batch composition.
	cluster.CreateTopicT(t, sourceTopic, 1)
	cluster.CreateTopicT(t, dlqTopic, 1)

	// Produce: 5 poison + 15 good = 20 messages.
	// With BatchSize=5, batch 1 is all poison, batches 2-4 are all good.
	var allMsgs []kafkatest.ProduceMessage
	for i := range 5 {
		allMsgs = append(allMsgs, kafkatest.ProduceMessage{
			Key:   []byte(fmt.Sprintf("poison-%d", i)),
			Value: []byte(fmt.Sprintf(`{"type":"poison","id":%d}`, i)),
		})
	}
	for i := range 15 {
		allMsgs = append(allMsgs, kafkatest.ProduceMessage{
			Key:   []byte(fmt.Sprintf("good-%d", i)),
			Value: []byte(fmt.Sprintf(`{"type":"good","id":%d}`, i)),
		})
	}
	cluster.ProduceT(t, sourceTopic, allMsgs)

	var goodProcessed atomic.Int64
	allDone := make(chan struct{})

	processor := func(ctx context.Context, batch []consumer.Message) error {
		for _, msg := range batch {
			if len(msg.Key) > 0 && string(msg.Key[:6]) == "poison" {
				return &consumer.ErrNonRetryable{
					Err: fmt.Errorf("poison payload rejected"),
				}
			}
		}
		goodProcessed.Add(int64(len(batch)))
		if goodProcessed.Load() >= 15 {
			select {
			case <-allDone:
			default:
				close(allDone)
			}
		}
		return nil
	}

	cfg := testConfig(cluster.Brokers(), sourceTopic, groupID)
	cfg.BatchSize = 5
	cfg.DLQTopic = dlqTopic

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
		t.Logf("all 15 good messages processed")
	case <-time.After(60 * time.Second):
		t.Fatalf("timeout: only %d/15 good messages processed", goodProcessed.Load())
	}

	// Let commit tick.
	time.Sleep(cfg.CommitInterval + 200*time.Millisecond)

	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("consumer run: %v", err)
	}

	// Verify DLQ topic received the poison messages.
	dlqMsgs, err := cluster.ConsumeAll(
		context.Background(),
		dlqTopic,
		"dlq-verify-group",
		10, // max messages to read
		10*time.Second,
	)
	if err != nil {
		t.Fatalf("consume DLQ: %v", err)
	}

	t.Logf("DLQ messages received: %d", len(dlqMsgs))

	if len(dlqMsgs) != 5 {
		t.Errorf("expected 5 DLQ messages, got %d", len(dlqMsgs))
	}

	// Verify each DLQ message has the required metadata headers (FR-4.2).
	for _, msg := range dlqMsgs {
		headers := make(map[string]string)
		for _, h := range msg.Headers {
			headers[h.Key] = string(h.Value)
		}

		// Check required DLQ metadata headers.
		requiredHeaders := []string{
			"dlq.source.topic",
			"dlq.source.partition",
			"dlq.source.offset",
			"dlq.error.reason",
			"dlq.error.timestamp",
		}
		for _, h := range requiredHeaders {
			if _, ok := headers[h]; !ok {
				t.Errorf("DLQ message (key=%s) missing header %q", msg.Key, h)
			}
		}

		// Verify source topic.
		if got := headers["dlq.source.topic"]; got != sourceTopic {
			t.Errorf("dlq.source.topic = %q, want %q", got, sourceTopic)
		}

		// Verify error reason is present.
		if reason := headers["dlq.error.reason"]; reason == "" {
			t.Error("dlq.error.reason is empty")
		}

		t.Logf("DLQ msg: key=%s, source_topic=%s, partition=%s, offset=%s, reason=%s",
			msg.Key,
			headers["dlq.source.topic"],
			headers["dlq.source.partition"],
			headers["dlq.source.offset"],
			headers["dlq.error.reason"],
		)
	}

	// Verify offsets on source topic advanced past all 20 messages.
	offsets := cluster.CommittedOffsetsT(t, groupID, sourceTopic, 1)
	t.Logf("source topic committed offsets: %v", offsets)
	var totalCommitted int64
	for _, off := range offsets {
		totalCommitted += off
	}
	if totalCommitted < int64(len(allMsgs)) {
		t.Errorf("committed offset %d < total messages %d", totalCommitted, len(allMsgs))
	}
}

// --------------------------------------------------------------------------
// DLQ-2: Retries exhausted + circuit breaker closed → DLQ
//
// Verifies: FR-4.1(b), NFR-6.5
// Processor always returns transient errors. After MaxRetries exhausted
// and CB still closed, messages are sent to DLQ.
// --------------------------------------------------------------------------

func TestDLQ_RetriesExhausted_RoutesToDLQ(t *testing.T) {
	sourceTopic := "test-dlq-retries"
	dlqTopic := "test-dlq-retries-dest"
	groupID := "test-dlq-retries-group"

	cluster.CreateTopicT(t, sourceTopic, 1)
	cluster.CreateTopicT(t, dlqTopic, 1)

	// Produce 5 messages — one batch worth.
	msgs := kafkatest.NewMessages(5, func(i int) []byte {
		return []byte(fmt.Sprintf(`{"id":%d}`, i))
	})
	cluster.ProduceT(t, sourceTopic, msgs)

	var attempts atomic.Int64

	processor := func(ctx context.Context, batch []consumer.Message) error {
		attempts.Add(1)
		return fmt.Errorf("always-failing transient error")
	}

	cfg := testConfig(cluster.Brokers(), sourceTopic, groupID)
	cfg.BatchSize = 5
	cfg.MaxRetries = 2 // 3 total attempts.
	cfg.DLQTopic = dlqTopic
	// Prevent CB from opening — we want retries exhausted with CB closed.
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

	// Wait long enough for retries + DLQ produce.
	// MaxRetries=2: 3 attempts with ~100ms, ~200ms backoff = ~1s per batch.
	time.Sleep(5 * time.Second)

	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("consumer run: %v", err)
	}

	t.Logf("total processor attempts: %d", attempts.Load())

	// Verify DLQ received the failed batch.
	dlqMsgs, err := cluster.ConsumeAll(
		context.Background(),
		dlqTopic,
		"dlq-retries-verify",
		10,
		10*time.Second,
	)
	if err != nil {
		t.Fatalf("consume DLQ: %v", err)
	}

	t.Logf("DLQ messages: %d", len(dlqMsgs))

	if len(dlqMsgs) != 5 {
		t.Errorf("expected 5 DLQ messages (one batch), got %d", len(dlqMsgs))
	}

	// Verify attempts: should be exactly 3 (initial + 2 retries) for the
	// first batch. May be more if additional batches were dispatched.
	if attempts.Load() < 3 {
		t.Errorf("expected at least 3 attempts (1 + MaxRetries=2), got %d", attempts.Load())
	}

	// Verify offsets advanced (FR-4.3: offset committed after DLQ).
	offsets := cluster.CommittedOffsetsT(t, groupID, sourceTopic, 1)
	t.Logf("source offsets: %v", offsets)
	var totalCommitted int64
	for _, off := range offsets {
		totalCommitted += off
	}
	if totalCommitted < 5 {
		t.Errorf("offsets did not advance past DLQ'd batch: committed=%d, expected>=5", totalCommitted)
	}
}
