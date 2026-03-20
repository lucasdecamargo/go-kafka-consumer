//go:build e2ebench

// Package e2e contains end-to-end benchmarks that exercise the full consumer
// framework against a real 3-broker Kafka cluster (via Docker Compose).
//
// Prerequisites:
//
//	docker compose -f benchmarks/docker-compose.bench.yml up -d --wait
//
// Run with:
//
//	go test -tags e2ebench -bench=. -benchmem -timeout=30m ./benchmarks/e2e/
package e2e

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/lucasdecamargo/go-kafka-consumer/consumer"
)

// benchBrokers is the bootstrap servers string for the benchmark cluster.
// Matches the EXTERNAL listeners in docker-compose.bench.yml.
const benchBrokers = "localhost:19092,localhost:19093,localhost:19094"

// discardLogger returns a logger that discards all output.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// benchConfig returns a consumer.Config tuned for benchmarking with the
// given topic and group. Configuration knobs can be overridden by the caller.
func benchConfig(topic, groupID string) consumer.Config {
	cfg := consumer.DefaultConfig()
	cfg.Brokers = []string{benchBrokers}
	cfg.Topics = []string{topic}
	cfg.GroupID = groupID
	cfg.WorkerCount = 4
	cfg.ChannelCap = 100
	cfg.BatchSize = 50
	cfg.LingerTime = 100 * time.Millisecond
	cfg.PollInterval = 50 * time.Millisecond
	cfg.CommitInterval = 1 * time.Second
	cfg.ShutdownTimeout = 15 * time.Second
	cfg.HealthAddr = "" // Disable HTTP server in benchmarks.
	return cfg
}

// createTopic creates a Kafka topic with the given partition count and
// replication factor 3. Skips if the topic already exists.
func createTopic(b *testing.B, topic string, partitions int) {
	b.Helper()

	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": benchBrokers,
	})
	if err != nil {
		b.Fatalf("create admin client: %v", err)
	}
	defer admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}})
	if err != nil {
		b.Fatalf("create topic %s: %v", topic, err)
	}
	for _, r := range results {
		if r.Error.Code() != kafka.ErrNoError && r.Error.Code() != kafka.ErrTopicAlreadyExists {
			b.Fatalf("create topic %s: %v", topic, r.Error)
		}
	}

	// Wait briefly for metadata propagation (fast with RF=1).
	time.Sleep(500 * time.Millisecond)
}

// produceMessages bulk-produces count messages with the given valueSize to
// the topic. Messages are distributed across partitions by the broker.
func produceMessages(b *testing.B, topic string, count, valueSize int) {
	b.Helper()

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": benchBrokers,
		"acks":              "all",
		"linger.ms":         5,
		"batch.size":        1048576,
	})
	if err != nil {
		b.Fatalf("create producer: %v", err)
	}

	// Drain delivery reports in background.
	var deliveryErrors atomic.Int64
	go func() {
		for e := range p.Events() {
			if msg, ok := e.(*kafka.Message); ok && msg.TopicPartition.Error != nil {
				deliveryErrors.Add(1)
			}
		}
	}()

	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	topicStr := topic
	for i := range count {
		key := []byte(fmt.Sprintf("key-%d", i))
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topicStr, Partition: kafka.PartitionAny},
			Key:            key,
			Value:          value,
		}, nil)
		if err != nil {
			p.Close()
			b.Fatalf("produce message %d: %v", i, err)
		}
	}

	remaining := p.Flush(30 * 1000)
	p.Close()

	if remaining > 0 {
		b.Fatalf("producer flush: %d messages still in queue", remaining)
	}
	if errs := deliveryErrors.Load(); errs > 0 {
		b.Fatalf("producer: %d delivery errors", errs)
	}

	b.Logf("produced %d messages (%d B each) to %s", count, valueSize, topic)
}

// runConsumerBenchmark starts a consumer, waits until targetCount messages
// are processed, then stops and reports throughput metrics on the benchmark.
func runConsumerBenchmark(b *testing.B, cfg consumer.Config, targetCount int64, valueSize int) {
	b.Helper()

	var processed atomic.Int64
	allDone := make(chan struct{})

	processor := func(_ context.Context, batch []consumer.Message) error {
		n := processed.Add(int64(len(batch)))
		if n >= targetCount {
			select {
			case <-allDone:
			default:
				close(allDone)
			}
		}
		return nil
	}

	reg := prometheus.NewRegistry()
	c, err := consumer.New(cfg, processor,
		consumer.WithLogger(discardLogger()),
		consumer.WithMetrics(reg),
	)
	if err != nil {
		b.Fatalf("create consumer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)

	start := time.Now()

	go func() {
		runDone <- c.Run(ctx)
	}()

	select {
	case <-allDone:
		// Success.
	case err := <-runDone:
		b.Fatalf("consumer exited early (processed %d/%d): %v", processed.Load(), targetCount, err)
	case <-time.After(2 * time.Minute):
		cancel()
		<-runDone
		b.Fatalf("timeout: processed %d/%d messages", processed.Load(), targetCount)
	}

	elapsed := time.Since(start)

	cancel()
	if err := <-runDone; err != nil {
		b.Logf("consumer shutdown: %v", err)
	}

	// Report custom metrics.
	recordsPerSec := float64(targetCount) / elapsed.Seconds()
	mbPerSec := recordsPerSec * float64(valueSize) / (1024 * 1024)
	b.ReportMetric(recordsPerSec, "records/sec")
	b.ReportMetric(mbPerSec, "MB/sec")
	b.ReportMetric(elapsed.Seconds(), "wall-sec")
}

// benchRunWithLatency is like runConsumerBenchmark but injects a fixed
// sleep into the processor to simulate target service latency.
func benchRunWithLatency(b *testing.B, cfg consumer.Config, targetCount int64, valueSize int, latency time.Duration) {
	b.Helper()

	var processed atomic.Int64
	allDone := make(chan struct{})

	processor := func(ctx context.Context, batch []consumer.Message) error {
		if latency > 0 {
			timer := time.NewTimer(latency)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}
		n := processed.Add(int64(len(batch)))
		if n >= targetCount {
			select {
			case <-allDone:
			default:
				close(allDone)
			}
		}
		return nil
	}

	reg := prometheus.NewRegistry()
	c, err := consumer.New(cfg, processor,
		consumer.WithLogger(discardLogger()),
		consumer.WithMetrics(reg),
	)
	if err != nil {
		b.Fatalf("create consumer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)

	start := time.Now()

	go func() {
		runDone <- c.Run(ctx)
	}()

	select {
	case <-allDone:
	case err := <-runDone:
		b.Fatalf("consumer exited early (processed %d/%d): %v", processed.Load(), targetCount, err)
	case <-time.After(2 * time.Minute):
		cancel()
		<-runDone
		b.Fatalf("timeout: processed %d/%d messages", processed.Load(), targetCount)
	}

	elapsed := time.Since(start)

	cancel()
	if err := <-runDone; err != nil {
		b.Logf("consumer shutdown: %v", err)
	}

	recordsPerSec := float64(targetCount) / elapsed.Seconds()
	mbPerSec := recordsPerSec * float64(valueSize) / (1024 * 1024)
	b.ReportMetric(recordsPerSec, "records/sec")
	b.ReportMetric(mbPerSec, "MB/sec")
	b.ReportMetric(elapsed.Seconds(), "wall-sec")
}

// uniqueGroupID returns a group ID that is unique per benchmark run to
// avoid offset state contamination between runs.
func uniqueGroupID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// TestMain checks that the benchmark cluster is reachable before running.
func TestMain(m *testing.M) {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": benchBrokers,
		"socket.timeout.ms": 5000,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "SKIP: cannot create admin client: %v\n", err)
		fmt.Fprintf(os.Stderr, "Start the cluster: docker compose -f benchmarks/docker-compose.bench.yml up -d\n")
		os.Exit(0)
	}

	meta, err := admin.GetMetadata(nil, true, 5000)
	admin.Close()
	if err != nil || len(meta.Brokers) == 0 {
		fmt.Fprintf(os.Stderr, "SKIP: benchmark cluster not reachable at %s\n", benchBrokers)
		fmt.Fprintf(os.Stderr, "Start the cluster: docker compose -f benchmarks/docker-compose.bench.yml up -d\n")
		os.Exit(0)
	}

	// Note: topics are created with replication factor 1 for benchmark speed.
	// The 3-broker cluster is available for manual experiments with higher RF.

	fmt.Fprintf(os.Stderr, "benchmark cluster: %d broker(s) reachable\n", len(meta.Brokers))
	os.Exit(m.Run())
}
