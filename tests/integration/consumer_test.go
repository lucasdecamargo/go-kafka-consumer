//go:build integration

package integration

import (
	"context"
	"encoding/json"
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
// Scenario 1: Happy path — produce N messages, consume, verify all processed.
// --------------------------------------------------------------------------

func TestHappyPath_AllMessagesProcessed(t *testing.T) {
	topic := "test-happy-path"
	groupID := "test-happy-group"
	messageCount := 50

	cluster.CreateTopicT(t, topic, 3)
	msgs := kafkatest.NewMessages(messageCount, func(i int) []byte {
		return []byte(fmt.Sprintf(`{"id":%d}`, i))
	})
	cluster.ProduceT(t, topic, msgs)

	// Track processed messages.
	var processed atomic.Int64
	allDone := make(chan struct{})

	processor := func(ctx context.Context, batch []consumer.Message) error {
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
	reg := prometheus.NewRegistry()

	c, err := consumer.New(cfg, processor,
		consumer.WithLogger(testLogger()),
		consumer.WithMetrics(reg),
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
		t.Logf("all %d messages processed", messageCount)
	case <-time.After(60 * time.Second):
		t.Fatalf("timeout: only %d/%d messages processed", processed.Load(), messageCount)
	}

	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("consumer run: %v", err)
	}

	if got := processed.Load(); got != int64(messageCount) {
		t.Errorf("processed %d messages, want %d", got, messageCount)
	}

	offsets := cluster.CommittedOffsetsT(t, groupID, topic, 3)
	if len(offsets) == 0 {
		t.Error("no offsets committed")
	}
	var totalCommitted int64
	for _, off := range offsets {
		totalCommitted += off
	}
	t.Logf("committed offsets: %v (total=%d)", offsets, totalCommitted)
	if totalCommitted < int64(messageCount) {
		t.Errorf("committed offset total %d < message count %d", totalCommitted, messageCount)
	}
}

// --------------------------------------------------------------------------
// Scenario 2: Backpressure — slow processor, verify no message loss.
// --------------------------------------------------------------------------

func TestBackpressure_NoMessageLoss(t *testing.T) {
	topic := "test-backpressure"
	groupID := "test-backpressure-group"
	messageCount := 30

	cluster.CreateTopicT(t, topic, 2)
	msgs := kafkatest.NewMessages(messageCount, func(i int) []byte {
		return []byte(fmt.Sprintf("message-%d", i))
	})
	cluster.ProduceT(t, topic, msgs)

	var mu sync.Mutex
	seen := make(map[string]bool)
	allDone := make(chan struct{})

	processor := func(ctx context.Context, batch []consumer.Message) error {
		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		for _, msg := range batch {
			key := fmt.Sprintf("%d:%d", msg.Partition, msg.Offset)
			seen[key] = true
		}
		count := len(seen)
		mu.Unlock()

		if count >= messageCount {
			select {
			case <-allDone:
			default:
				close(allDone)
			}
		}
		return nil
	}

	cfg := testConfig(cluster.Brokers(), topic, groupID)
	cfg.ChannelCap = 2
	cfg.BatchSize = 3

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
		t.Logf("all %d messages processed under backpressure", messageCount)
	case <-time.After(90 * time.Second):
		mu.Lock()
		got := len(seen)
		mu.Unlock()
		t.Fatalf("timeout: only %d/%d messages processed", got, messageCount)
	}

	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("consumer run: %v", err)
	}

	mu.Lock()
	got := len(seen)
	mu.Unlock()
	if got != messageCount {
		t.Errorf("processed %d unique messages, want %d", got, messageCount)
	}
}

// --------------------------------------------------------------------------
// Scenario 3: Graceful shutdown — cancel context, verify offsets committed.
// --------------------------------------------------------------------------

func TestGracefulShutdown_OffsetsCommitted(t *testing.T) {
	topic := "test-shutdown"
	groupID := "test-shutdown-group"
	messageCount := 20

	cluster.CreateTopicT(t, topic, 2)
	msgs := kafkatest.NewMessages(messageCount, func(i int) []byte {
		return []byte(fmt.Sprintf("message-%d", i))
	})
	cluster.ProduceT(t, topic, msgs)

	var processed atomic.Int64
	started := make(chan struct{})

	processor := func(ctx context.Context, batch []consumer.Message) error {
		processed.Add(int64(len(batch)))
		select {
		case <-started:
		default:
			close(started)
		}
		return nil
	}

	cfg := testConfig(cluster.Brokers(), topic, groupID)

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
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for processing to start")
	}

	// Let the commit timer tick at least once.
	time.Sleep(cfg.CommitInterval + 200*time.Millisecond)

	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("consumer run: %v", err)
	}

	processedCount := processed.Load()
	t.Logf("processed %d messages before shutdown", processedCount)

	offsets := cluster.CommittedOffsetsT(t, groupID, topic, 2)
	t.Logf("committed offsets after shutdown: %v", offsets)

	if len(offsets) == 0 {
		t.Error("no offsets committed after graceful shutdown")
	}

	// Restart consumer — verify it does not reprocess already-committed messages.
	var reprocessed atomic.Int64

	processor2 := func(ctx context.Context, batch []consumer.Message) error {
		reprocessed.Add(int64(len(batch)))
		return nil
	}

	c2, err := consumer.New(cfg, processor2,
		consumer.WithLogger(testLogger()),
		consumer.WithMetrics(prometheus.NewRegistry()),
	)
	if err != nil {
		t.Fatalf("create consumer 2: %v", err)
	}

	runCtx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	runDone2 := make(chan error, 1)
	go func() {
		runDone2 <- c2.Run(runCtx2)
	}()

	<-runDone2

	remaining := int64(messageCount) - processedCount
	reprocessedCount := reprocessed.Load()
	t.Logf("reprocessed %d messages on restart (remaining=%d)", reprocessedCount, remaining)

	maxExpected := remaining + int64(cfg.BatchSize)
	if reprocessedCount > maxExpected {
		t.Errorf("reprocessed %d messages, expected at most %d (remaining=%d + batch_size=%d)",
			reprocessedCount, maxExpected, remaining, cfg.BatchSize)
	}
}

// --------------------------------------------------------------------------
// Scenario 4: Offset commit verification — restart after processing,
//             verify no duplicate processing beyond batch boundary.
// --------------------------------------------------------------------------

func TestOffsetCommit_NoDuplicatesBeyondBatch(t *testing.T) {
	topic := "test-offsets"
	groupID := "test-offsets-group"
	messageCount := 40

	cluster.CreateTopicT(t, topic, 2)
	msgs := kafkatest.NewMessages(messageCount, func(i int) []byte {
		return []byte(fmt.Sprintf("message-%d", i))
	})
	cluster.ProduceT(t, topic, msgs)

	// Phase 1: Consume all messages.
	var phase1Count atomic.Int64
	phase1Done := make(chan struct{})

	processor1 := func(ctx context.Context, batch []consumer.Message) error {
		phase1Count.Add(int64(len(batch)))
		if phase1Count.Load() >= int64(messageCount) {
			select {
			case <-phase1Done:
			default:
				close(phase1Done)
			}
		}
		return nil
	}

	cfg := testConfig(cluster.Brokers(), topic, groupID)

	c1, err := consumer.New(cfg, processor1,
		consumer.WithLogger(testLogger()),
		consumer.WithMetrics(prometheus.NewRegistry()),
	)
	if err != nil {
		t.Fatalf("create consumer 1: %v", err)
	}

	runCtx1, cancel1 := context.WithCancel(context.Background())
	runDone1 := make(chan error, 1)
	go func() {
		runDone1 <- c1.Run(runCtx1)
	}()

	select {
	case <-phase1Done:
	case <-time.After(60 * time.Second):
		t.Fatalf("phase 1 timeout: processed %d/%d", phase1Count.Load(), messageCount)
	}

	// Wait for commit to happen.
	time.Sleep(cfg.CommitInterval + 200*time.Millisecond)

	cancel1()
	<-runDone1

	offsets := cluster.CommittedOffsetsT(t, groupID, topic, 2)
	t.Logf("committed offsets after phase 1: %v", offsets)

	// Phase 2: Restart — should see zero or very few messages.
	var phase2Count atomic.Int64

	processor2 := func(ctx context.Context, batch []consumer.Message) error {
		phase2Count.Add(int64(len(batch)))
		return nil
	}

	c2, err := consumer.New(cfg, processor2,
		consumer.WithLogger(testLogger()),
		consumer.WithMetrics(prometheus.NewRegistry()),
	)
	if err != nil {
		t.Fatalf("create consumer 2: %v", err)
	}

	runCtx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	runDone2 := make(chan error, 1)
	go func() {
		runDone2 <- c2.Run(runCtx2)
	}()

	<-runDone2

	reprocessed := phase2Count.Load()
	t.Logf("phase 2 reprocessed: %d messages", reprocessed)

	if reprocessed > int64(cfg.BatchSize) {
		t.Errorf("reprocessed %d messages on restart, expected at most %d (batch size)",
			reprocessed, cfg.BatchSize)
	}
}

// --------------------------------------------------------------------------
// Scenario 5: Multiple partitions — verify all partitions are consumed.
// --------------------------------------------------------------------------

func TestMultiplePartitions_AllConsumed(t *testing.T) {
	topic := "test-multi-partition"
	groupID := "test-multi-partition-group"
	numPartitions := 4
	messageCount := 100

	cluster.CreateTopicT(t, topic, numPartitions)
	msgs := kafkatest.NewMessages(messageCount, func(i int) []byte {
		return []byte(fmt.Sprintf("message-%d", i))
	})
	cluster.ProduceT(t, topic, msgs)

	var mu sync.Mutex
	partitionsSeen := make(map[int32]int)
	var totalProcessed atomic.Int64
	allDone := make(chan struct{})

	processor := func(ctx context.Context, batch []consumer.Message) error {
		mu.Lock()
		for _, msg := range batch {
			partitionsSeen[msg.Partition]++
		}
		mu.Unlock()

		totalProcessed.Add(int64(len(batch)))
		if totalProcessed.Load() >= int64(messageCount) {
			select {
			case <-allDone:
			default:
				close(allDone)
			}
		}
		return nil
	}

	cfg := testConfig(cluster.Brokers(), topic, groupID)
	cfg.WorkerCount = 4
	cfg.ChannelCap = 50

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
	case <-time.After(60 * time.Second):
		mu.Lock()
		t.Logf("partitions seen at timeout: %v", partitionsSeen)
		mu.Unlock()
		t.Fatalf("timeout: processed %d/%d", totalProcessed.Load(), messageCount)
	}

	cancel()
	<-runDone

	mu.Lock()
	defer mu.Unlock()

	t.Logf("partitions seen: %v", partitionsSeen)

	if totalProcessed.Load() != int64(messageCount) {
		t.Errorf("processed %d messages, want %d", totalProcessed.Load(), messageCount)
	}

	// With 100 messages across 4 partitions with round-robin keys,
	// we should see messages from multiple partitions.
	if len(partitionsSeen) < 2 {
		t.Errorf("only saw %d partitions, expected messages across multiple partitions", len(partitionsSeen))
	}
}

// --------------------------------------------------------------------------
// Scenario 6: Custom messages — produce structured JSON, verify content
//             is received intact.
// --------------------------------------------------------------------------

func TestCustomMessages_StructuredJSON(t *testing.T) {
	topic := "test-custom-json"
	groupID := "test-custom-json-group"

	cluster.CreateTopicT(t, topic, 1)

	type Event struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	events := []Event{
		{ID: 1, Name: "alice"},
		{ID: 2, Name: "bob"},
		{ID: 3, Name: "charlie"},
	}

	msgs := make([]kafkatest.ProduceMessage, len(events))
	for i, ev := range events {
		data, _ := json.Marshal(ev)
		msgs[i] = kafkatest.ProduceMessage{
			Key:   []byte(fmt.Sprintf("user-%d", ev.ID)),
			Value: data,
			Headers: []kafkatest.Header{
				{Key: "event-type", Value: []byte("user-created")},
				{Key: "version", Value: []byte("v1")},
			},
		}
	}
	cluster.ProduceT(t, topic, msgs)

	var mu sync.Mutex
	received := make([]Event, 0, len(events))
	allDone := make(chan struct{})

	processor := func(ctx context.Context, batch []consumer.Message) error {
		mu.Lock()
		defer mu.Unlock()
		for _, msg := range batch {
			var ev Event
			if err := json.Unmarshal(msg.Value, &ev); err != nil {
				t.Errorf("unmarshal message: %v", err)
				continue
			}
			received = append(received, ev)
		}
		if len(received) >= len(events) {
			select {
			case <-allDone:
			default:
				close(allDone)
			}
		}
		return nil
	}

	cfg := testConfig(cluster.Brokers(), topic, groupID)

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
		t.Logf("all %d structured messages received", len(events))
	case <-time.After(30 * time.Second):
		mu.Lock()
		got := len(received)
		mu.Unlock()
		t.Fatalf("timeout: only %d/%d messages received", got, len(events))
	}

	cancel()
	<-runDone

	mu.Lock()
	defer mu.Unlock()

	if len(received) != len(events) {
		t.Fatalf("received %d events, want %d", len(received), len(events))
	}

	// Verify content matches (order may differ).
	idSet := make(map[int]string)
	for _, ev := range received {
		idSet[ev.ID] = ev.Name
	}
	for _, ev := range events {
		if name, ok := idSet[ev.ID]; !ok || name != ev.Name {
			t.Errorf("missing or mismatched event: want {id:%d name:%s}, got name=%q", ev.ID, ev.Name, name)
		}
	}
}

// --------------------------------------------------------------------------
// Scenario 7: Malformed messages — produce invalid JSON, verify processor
//             receives raw bytes (framework does not parse).
// --------------------------------------------------------------------------

func TestMalformedMessages_ReceivedAsIs(t *testing.T) {
	topic := "test-malformed"
	groupID := "test-malformed-group"

	cluster.CreateTopicT(t, topic, 1)

	msgs := []kafkatest.ProduceMessage{
		{Key: []byte("good"), Value: []byte(`{"valid":"json"}`)},
		{Key: []byte("bad"), Value: []byte(`{invalid json!!!}`)},
		{Key: []byte("empty"), Value: []byte("")},
		{Key: []byte("binary"), Value: []byte{0x00, 0xFF, 0xFE}},
	}
	cluster.ProduceT(t, topic, msgs)

	var mu sync.Mutex
	received := make(map[string][]byte)
	allDone := make(chan struct{})

	processor := func(ctx context.Context, batch []consumer.Message) error {
		mu.Lock()
		defer mu.Unlock()
		for _, msg := range batch {
			received[string(msg.Key)] = msg.Value
		}
		if len(received) >= len(msgs) {
			select {
			case <-allDone:
			default:
				close(allDone)
			}
		}
		return nil
	}

	cfg := testConfig(cluster.Brokers(), topic, groupID)

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
		t.Logf("all %d messages received (including malformed)", len(msgs))
	case <-time.After(30 * time.Second):
		mu.Lock()
		got := len(received)
		mu.Unlock()
		t.Fatalf("timeout: only %d/%d messages received", got, len(msgs))
	}

	cancel()
	<-runDone

	mu.Lock()
	defer mu.Unlock()

	// Framework delivers raw bytes — no parsing, no filtering.
	for _, m := range msgs {
		val, ok := received[string(m.Key)]
		if !ok {
			t.Errorf("message with key %q not received", m.Key)
			continue
		}
		if string(val) != string(m.Value) {
			t.Errorf("key %q: got value %q, want %q", m.Key, val, m.Value)
		}
	}
}

// --------------------------------------------------------------------------
// Scenario 8: Headers preserved — verify Kafka headers survive the
//             framework pipeline.
// --------------------------------------------------------------------------

func TestHeaders_PreservedThroughPipeline(t *testing.T) {
	topic := "test-headers"
	groupID := "test-headers-group"

	cluster.CreateTopicT(t, topic, 1)

	msgs := []kafkatest.ProduceMessage{{
		Key:   []byte("with-headers"),
		Value: []byte("payload"),
		Headers: []kafkatest.Header{
			{Key: "trace-id", Value: []byte("abc-123")},
			{Key: "content-type", Value: []byte("application/json")},
		},
	}}
	cluster.ProduceT(t, topic, msgs)

	headersDone := make(chan map[string]string, 1)

	processor := func(ctx context.Context, batch []consumer.Message) error {
		for _, msg := range batch {
			hdrs := make(map[string]string)
			for _, h := range msg.Headers {
				hdrs[h.Key] = string(h.Value)
			}
			select {
			case headersDone <- hdrs:
			default:
			}
		}
		return nil
	}

	cfg := testConfig(cluster.Brokers(), topic, groupID)

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
	case hdrs := <-headersDone:
		if hdrs["trace-id"] != "abc-123" {
			t.Errorf("trace-id header: got %q, want %q", hdrs["trace-id"], "abc-123")
		}
		if hdrs["content-type"] != "application/json" {
			t.Errorf("content-type header: got %q, want %q", hdrs["content-type"], "application/json")
		}
		t.Logf("headers verified: %v", hdrs)
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for message with headers")
	}

	cancel()
	<-runDone
}
