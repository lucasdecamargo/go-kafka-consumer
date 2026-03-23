//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lucasdecamargo/go-kafka-consumer/consumer"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/kafkatest"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/metrics"
)

// TestConsumerLag_MetricUpdated validates the end-to-end consumer lag
// instrumentation pipeline:
//
//  1. librdkafka emits a *kafka.Stats event every LagReportInterval.
//  2. The Kafka adapter's handleEvent parses the JSON payload and calls
//     LagMetrics.Update() with the group ID and stats string.
//  3. kafka_consumer_lag{group, topic, partition} is set for every
//     partition whose lag is known (>= 0).
//
// Two assertions are made in sequence:
//
//   - Phase 1 — non-zero lag is observed while most messages remain in
//     Kafka. Backpressure (ChannelCap=2, WorkerCount=1, slow processor)
//     caps in-flight messages at ~15, leaving ~75 of 90 messages unpolled
//     when the first stats event fires. The gauge must reflect that.
//
//   - Phase 2 — after all messages are processed and offsets committed,
//     the gauge must reach zero within one CommitInterval + one
//     LagReportInterval.
func TestConsumerLag_MetricUpdated(t *testing.T) {
	const (
		topic      = "test-lag-metric"
		groupID    = "test-lag-group"
		partitions = 3
		msgCount   = 90 // 30 per partition
	)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Produce all messages before the consumer starts so lag is non-zero
	// from the moment the group establishes its first committed offset.
	cluster.CreateTopicT(t, topic, partitions)
	cluster.ProduceT(t, topic, kafkatest.NewMessages(msgCount, func(i int) []byte {
		return []byte(fmt.Sprintf("value-%04d", i))
	}))

	// Isolated registry — avoids metric collisions with other tests that
	// also call consumer.WithMetrics.
	reg := prometheus.NewRegistry()

	// Backpressure configuration:
	//   ChannelCap=2 fills the dispatch channel after ~2 batches.
	//   WorkerCount=1 means a single worker drains it slowly.
	//   processor sleep=200ms → ~5 messages/s throughput.
	//   At saturation: 15 messages in flight, ~75 remain in Kafka (paused).
	//   CommitInterval=500ms is shorter than LagReportInterval=1s, so the
	//   first commit happens before the first stats event — ensuring the
	//   committed offset is known and consumer_lag is not -1 (unknown).
	cfg := testConfig(cluster.Brokers(), topic, groupID)
	cfg.LagReportInterval = 1 * time.Second
	cfg.BatchSize = 5
	cfg.ChannelCap = 2
	cfg.WorkerCount = 1
	cfg.CommitInterval = 500 * time.Millisecond

	var processed atomic.Int64
	processingDone := make(chan struct{})
	var once sync.Once

	c, err := consumer.New(cfg,
		func(_ context.Context, batch []consumer.Message) error {
			// Slow processor — keeps the dispatch channel full long enough for
			// the first stats event to fire with non-zero per-partition lag.
			time.Sleep(200 * time.Millisecond)
			if processed.Add(int64(len(batch))) >= int64(msgCount) {
				once.Do(func() { close(processingDone) })
			}
			return nil
		},
		consumer.WithLogger(testLogger()),
		consumer.WithMetrics(reg),
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()

	// ── Phase 1: observe non-zero lag during backpressured consumption ──────
	//
	// Poll for up to 10 seconds. The first stats event fires at ~1s.
	// Before that point the committed offset is set by the 500ms
	// CommitInterval, so consumer_lag will not be -1 (unknown) when
	// stats fires.
	t.Log("phase 1: waiting for non-zero consumer lag...")

	var phase1OK bool

	lagCheckDeadline := time.Now().Add(10 * time.Second)
	for !phase1OK && time.Now().Before(lagCheckDeadline) {
		if total := lagTotal(t, reg, groupID, topic, partitions); total > 0 {
			phase1OK = true
			t.Logf("phase 1 OK: observed lag = %.0f messages across %d partitions",
				total, partitions)
		} else {
			time.Sleep(200 * time.Millisecond)
		}
	}

	if !phase1OK {
		t.Error("phase 1 FAIL: kafka_consumer_lag never exceeded 0 " +
			"during backpressured consumption — stats callback may not be wired")
	}

	// ── Phase 2: lag converges to 0 after full processing + commit ──────────
	t.Log("phase 2: waiting for all messages to be processed...")

	select {
	case <-processingDone:
		t.Logf("phase 2: all %d messages processed; waiting for final commit and stats cycle", msgCount)
	case <-ctx.Done():
		t.Fatalf("phase 2: timed out — processed %d/%d messages", processed.Load(), msgCount)
	}

	// Allow one full CommitInterval (offsets committed) and one full
	// LagReportInterval (stats event fires with updated committed_offset)
	// plus a 500ms scheduling buffer.
	drainWait := cfg.CommitInterval + cfg.LagReportInterval + 500*time.Millisecond
	t.Logf("phase 2: sleeping %s for commit + stats cycle...", drainWait)
	time.Sleep(drainWait)

	if lag := lagTotal(t, reg, groupID, topic, partitions); lag != 0 {
		t.Errorf("phase 2 FAIL: expected kafka_consumer_lag = 0 after full consumption, got %.0f", lag)
	} else {
		t.Log("phase 2 OK: kafka_consumer_lag = 0 ✓")
	}

	// Shut down cleanly; the test context is already live so Run() won't
	// return until we cancel.
	cancel()

	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("consumer.Run returned unexpected error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consumer did not shut down within 15s")
	}
}

// lagTotal sums kafka_consumer_lag across all numeric partition IDs for
// the given topic and group. Partitions whose lag value is -1 (unknown)
// are already excluded by LagMetrics.Update — the gauge simply won't have
// a label set for them — so getGaugeValue returns 0, which is safe to sum.
func lagTotal(t *testing.T, g prometheus.Gatherer, groupID, topic string, partitions int) float64 {
	t.Helper()
	var total float64
	for p := range partitions {
		total += getGaugeValue(t, g, metrics.ConsumerLag,
			"group", groupID,
			"topic", topic,
			"partition", fmt.Sprintf("%d", p),
		)
	}
	return total
}
