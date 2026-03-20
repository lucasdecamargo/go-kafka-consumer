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
	dto "github.com/prometheus/client_model/go"

	"errors"

	"github.com/lucasdecamargo/go-kafka-consumer/consumer"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/kafkatest"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/metrics"
)

// --------------------------------------------------------------------------
// Helpers for reading metrics from an isolated prometheus.Registry.
// --------------------------------------------------------------------------

// getCounterValue returns the value of a counter metric with the given name
// and optional label pairs (key, value, key, value, ...).
func getCounterValue(t *testing.T, g prometheus.Gatherer, name string, labels ...string) float64 {
	t.Helper()
	return getMetricValue(t, g, name, labels...)
}

// getGaugeValue returns the value of a gauge metric with the given name
// and optional label pairs.
func getGaugeValue(t *testing.T, g prometheus.Gatherer, name string, labels ...string) float64 {
	t.Helper()
	return getMetricValue(t, g, name, labels...)
}

// getMetricValue reads the numeric value of a counter or gauge.
func getMetricValue(t *testing.T, g prometheus.Gatherer, name string, labels ...string) float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	lMap := labelsToMap(labels)
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if !matchLabels(m, lMap) {
				continue
			}
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
			if g := m.GetGauge(); g != nil {
				return g.GetValue()
			}
		}
	}
	return 0
}

func labelsToMap(pairs []string) map[string]string {
	m := make(map[string]string, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		m[pairs[i]] = pairs[i+1]
	}
	return m
}

func matchLabels(m *dto.Metric, want map[string]string) bool {
	if len(want) == 0 {
		return true
	}
	have := make(map[string]string, len(m.GetLabel()))
	for _, l := range m.GetLabel() {
		have[l.GetName()] = l.GetValue()
	}
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

// sumCounterAcrossPartitions sums a counter metric across all partition labels.
func sumCounterAcrossPartitions(t *testing.T, g prometheus.Gatherer, name string, extraLabels ...string) float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	lMap := labelsToMap(extraLabels)
	var total float64
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if matchLabels(m, lMap) {
				if c := m.GetCounter(); c != nil {
					total += c.GetValue()
				}
			}
		}
	}
	return total
}

// sumHistogramCountAcrossPartitions sums histogram sample counts across partitions.
func sumHistogramCountAcrossPartitions(t *testing.T, g prometheus.Gatherer, name string, extraLabels ...string) uint64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	lMap := labelsToMap(extraLabels)
	var total uint64
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if matchLabels(m, lMap) {
				if h := m.GetHistogram(); h != nil {
					total += h.GetSampleCount()
				}
			}
		}
	}
	return total
}

// --------------------------------------------------------------------------
// Test: Message throughput counters
// --------------------------------------------------------------------------

func TestMetrics_MessageThroughput(t *testing.T) {
	topic := "test-metrics-throughput"
	groupID := "test-metrics-throughput-group"
	messageCount := 30

	cluster.CreateTopicT(t, topic, 1)
	msgs := kafkatest.NewMessages(messageCount, func(i int) []byte {
		return []byte(fmt.Sprintf(`{"id":%d}`, i))
	})
	cluster.ProduceT(t, topic, msgs)

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
	cfg.WorkerCount = 1
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
	go func() { runDone <- c.Run(runCtx) }()

	select {
	case <-allDone:
	case <-time.After(60 * time.Second):
		t.Fatalf("timeout: %d/%d processed", processed.Load(), messageCount)
	}

	cancel()
	<-runDone

	// Assert messages_polled_total == messageCount (summed across partitions).
	polled := sumCounterAcrossPartitions(t, reg, metrics.MessagesPolledTotal)
	if polled != float64(messageCount) {
		t.Errorf("messages_polled_total = %v, want %v", polled, messageCount)
	}

	// Assert messages_processed_total{status="success"} == messageCount.
	processedMetric := sumCounterAcrossPartitions(t, reg,
		metrics.MessagesProcessedTotal, "status", "success")
	if processedMetric != float64(messageCount) {
		t.Errorf("messages_processed_total{success} = %v, want %v", processedMetric, messageCount)
	}

	// Assert partition label is present (partition "0" since 1 partition).
	p0Polled := getCounterValue(t, reg, metrics.MessagesPolledTotal, "partition", "0")
	if p0Polled != float64(messageCount) {
		t.Errorf("messages_polled_total{partition=0} = %v, want %v", p0Polled, messageCount)
	}
}

// --------------------------------------------------------------------------
// Test: Processing latency histograms
// --------------------------------------------------------------------------

func TestMetrics_ProcessingLatency(t *testing.T) {
	topic := "test-metrics-latency"
	groupID := "test-metrics-latency-group"
	messageCount := 10

	cluster.CreateTopicT(t, topic, 1)
	msgs := kafkatest.NewMessages(messageCount, func(i int) []byte {
		return []byte(fmt.Sprintf(`{"id":%d}`, i))
	})
	cluster.ProduceT(t, topic, msgs)

	var processed atomic.Int64
	allDone := make(chan struct{})

	processor := func(ctx context.Context, batch []consumer.Message) error {
		time.Sleep(50 * time.Millisecond) // Simulate processing time.
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
	cfg.WorkerCount = 1
	cfg.BatchSize = 5
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
	go func() { runDone <- c.Run(runCtx) }()

	select {
	case <-allDone:
	case <-time.After(60 * time.Second):
		t.Fatalf("timeout: %d/%d processed", processed.Load(), messageCount)
	}

	cancel()
	<-runDone

	// processing_time_seconds should have observations for all messages.
	ptCount := sumHistogramCountAcrossPartitions(t, reg, metrics.ProcessingTimeSeconds)
	if ptCount == 0 {
		t.Error("processing_time_seconds has no observations")
	}

	// message_delay_seconds should have one observation per message.
	mdCount := sumHistogramCountAcrossPartitions(t, reg, metrics.MessageDelaySeconds)
	if mdCount != uint64(messageCount) {
		t.Errorf("message_delay_seconds count = %d, want %d", mdCount, messageCount)
	}

	// record_age_seconds should have one observation per message.
	raCount := sumHistogramCountAcrossPartitions(t, reg, metrics.RecordAgeSeconds)
	if raCount != uint64(messageCount) {
		t.Errorf("record_age_seconds count = %d, want %d", raCount, messageCount)
	}

	// Validate ordering: record_age >= message_delay >= processing_time.
	// We can't check exact values, but we verify all three are recorded.
	t.Logf("processing_time observations: %d, message_delay: %d, record_age: %d",
		ptCount, mdCount, raCount)
}

// --------------------------------------------------------------------------
// Test: DLQ counters with partition label
// --------------------------------------------------------------------------

func TestMetrics_DLQCounters(t *testing.T) {
	topic := "test-metrics-dlq"
	dlqTopic := "test-metrics-dlq-dead"
	groupID := "test-metrics-dlq-group"
	messageCount := 10

	cluster.CreateTopicT(t, topic, 1)
	cluster.CreateTopicT(t, dlqTopic, 1)
	msgs := kafkatest.NewMessages(messageCount, func(i int) []byte {
		return []byte(fmt.Sprintf(`{"id":%d}`, i))
	})
	cluster.ProduceT(t, topic, msgs)

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
		return &consumer.ErrNonRetryable{Err: errors.New("bad data")}
	}

	cfg := testConfig(cluster.Brokers(), topic, groupID)
	cfg.WorkerCount = 1
	cfg.BatchSize = 5
	cfg.DLQTopic = dlqTopic
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
	go func() { runDone <- c.Run(runCtx) }()

	select {
	case <-allDone:
	case <-time.After(60 * time.Second):
		t.Fatalf("timeout: %d/%d processed", processed.Load(), messageCount)
	}

	// Wait for DLQ production to complete.
	time.Sleep(2 * time.Second)
	cancel()
	<-runDone

	// Assert non_retryable processed count.
	nrCount := sumCounterAcrossPartitions(t, reg,
		metrics.MessagesProcessedTotal, "status", "non_retryable")
	if nrCount != float64(messageCount) {
		t.Errorf("messages_processed{non_retryable} = %v, want %v", nrCount, messageCount)
	}

	// Assert DLQ counter.
	dlqCount := sumCounterAcrossPartitions(t, reg,
		metrics.DLQMessagesTotal, "reason", "non_retryable")
	if dlqCount != float64(messageCount) {
		t.Errorf("dlq_messages_total{non_retryable} = %v, want %v", dlqCount, messageCount)
	}

	// Assert partition label is present on DLQ counter.
	dlqP0 := getCounterValue(t, reg, metrics.DLQMessagesTotal,
		"reason", "non_retryable", "partition", "0")
	if dlqP0 != float64(messageCount) {
		t.Errorf("dlq_messages_total{partition=0} = %v, want %v", dlqP0, messageCount)
	}
}

// --------------------------------------------------------------------------
// Test: Circuit breaker gauge
// --------------------------------------------------------------------------

func TestMetrics_CircuitBreakerGauge(t *testing.T) {
	topic := "test-metrics-cb"
	groupID := "test-metrics-cb-group"
	messageCount := 50

	cluster.CreateTopicT(t, topic, 1)
	msgs := kafkatest.NewMessages(messageCount, func(i int) []byte {
		return []byte(fmt.Sprintf(`{"id":%d}`, i))
	})
	cluster.ProduceT(t, topic, msgs)

	processor := func(ctx context.Context, batch []consumer.Message) error {
		return fmt.Errorf("always fail")
	}

	cfg := testConfig(cluster.Brokers(), topic, groupID)
	cfg.WorkerCount = 1
	cfg.BatchSize = 5
	cfg.MaxRetries = 0
	cfg.CBMinRequests = 3
	cfg.CBFailureThreshold = 0.6
	cfg.CBOpenTimeout = 30 * time.Second // Stay open long enough to observe.
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
	go func() { runDone <- c.Run(runCtx) }()

	// Wait for CB to trip. The gauge may oscillate between open (2) and
	// half-open (1) as gobreaker probes. Track the max value observed —
	// any value > 0 proves the gauge is being updated by state transitions.
	var maxCBState float64
	deadline := time.After(15 * time.Second)
loop:
	for {
		cbState := getGaugeValue(t, reg, metrics.CircuitBreakerState)
		if cbState > maxCBState {
			maxCBState = cbState
		}
		if maxCBState >= 2 {
			break
		}
		select {
		case <-deadline:
			break loop
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	if maxCBState < 1 {
		t.Fatalf("circuit breaker never tripped, max_state=%v", maxCBState)
	}

	t.Logf("circuit breaker max state observed = %v", maxCBState)

	cancel()
	<-runDone
}

// --------------------------------------------------------------------------
// Test: Commit offset metrics
// --------------------------------------------------------------------------

func TestMetrics_CommitOffsets(t *testing.T) {
	topic := "test-metrics-commits"
	groupID := "test-metrics-commits-group"
	messageCount := 20

	cluster.CreateTopicT(t, topic, 1)
	msgs := kafkatest.NewMessages(messageCount, func(i int) []byte {
		return []byte(fmt.Sprintf(`{"id":%d}`, i))
	})
	cluster.ProduceT(t, topic, msgs)

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
	cfg.WorkerCount = 1
	cfg.CommitInterval = 200 * time.Millisecond
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
	go func() { runDone <- c.Run(runCtx) }()

	select {
	case <-allDone:
	case <-time.After(60 * time.Second):
		t.Fatalf("timeout: %d/%d processed", processed.Load(), messageCount)
	}

	// Wait for at least one commit cycle.
	time.Sleep(1 * time.Second)

	cancel()
	<-runDone

	// commits_total should be > 0.
	commits := sumCounterAcrossPartitions(t, reg, metrics.CommitsTotal)
	if commits == 0 {
		t.Error("commits_total = 0, expected > 0")
	}

	// last_committed_offset{partition="0"} should be == messageCount (next-to-fetch).
	lastOffset := getGaugeValue(t, reg, metrics.LastCommittedOffset, "partition", "0")
	if lastOffset != float64(messageCount) {
		t.Errorf("last_committed_offset{0} = %v, want %v", lastOffset, messageCount)
	}

	t.Logf("commits_total = %v, last_committed_offset{0} = %v", commits, lastOffset)
}

// --------------------------------------------------------------------------
// Test: Inflight messages returns to zero
// --------------------------------------------------------------------------

func TestMetrics_InflightMessages(t *testing.T) {
	topic := "test-metrics-inflight"
	groupID := "test-metrics-inflight-group"
	messageCount := 20

	cluster.CreateTopicT(t, topic, 1)
	msgs := kafkatest.NewMessages(messageCount, func(i int) []byte {
		return []byte(fmt.Sprintf(`{"id":%d}`, i))
	})
	cluster.ProduceT(t, topic, msgs)

	var processed atomic.Int64
	allDone := make(chan struct{})
	var mu sync.Mutex
	var maxInflight float64

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
	cfg.WorkerCount = 2
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
	go func() { runDone <- c.Run(runCtx) }()

	// Sample inflight while consuming.
	go func() {
		for {
			select {
			case <-allDone:
				return
			default:
				v := getGaugeValue(t, reg, metrics.InflightMessages, "partition", "0")
				mu.Lock()
				if v > maxInflight {
					maxInflight = v
				}
				mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	select {
	case <-allDone:
	case <-time.After(60 * time.Second):
		t.Fatalf("timeout: %d/%d processed", processed.Load(), messageCount)
	}

	// Wait for all batch completions to propagate.
	time.Sleep(500 * time.Millisecond)

	cancel()
	<-runDone

	// After shutdown, inflight should be 0.
	inflight := getGaugeValue(t, reg, metrics.InflightMessages, "partition", "0")
	if inflight != 0 {
		t.Errorf("inflight_messages{0} = %v after shutdown, want 0", inflight)
	}

	mu.Lock()
	t.Logf("max inflight observed during consumption: %v", maxInflight)
	mu.Unlock()
}
