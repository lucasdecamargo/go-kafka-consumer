//go:build e2ebench

package e2e

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

const (
	// defaultMessageCount is the number of messages per sub-benchmark.
	// Large enough to amortize startup cost, small enough to finish quickly.
	defaultMessageCount = 10000

	// defaultPartitions is the partition count for benchmark topics.
	defaultPartitions = 12
)

// topicSeq generates unique topic names across benchmark invocations.
// The Go benchmark framework may re-invoke the function body; each
// invocation must use a fresh topic to avoid consuming stale data.
var topicSeq atomic.Int64

func uniqueTopic(prefix string) string {
	return fmt.Sprintf("bench-%s-%d", prefix, topicSeq.Add(1))
}

// BenchmarkThroughput_MessageSize measures consumer throughput at varying
// message payload sizes. Isolates serialization and memory pressure effects.
func BenchmarkThroughput_MessageSize(b *testing.B) {
	for _, valueSize := range []int{100, 512, 1024, 10240} {
		b.Run(fmt.Sprintf("payload=%dB", valueSize), func(b *testing.B) {
			topic := uniqueTopic(fmt.Sprintf("msgsize-%d", valueSize))
			createTopic(b, topic, defaultPartitions)
			produceMessages(b, topic, defaultMessageCount, valueSize)

			cfg := benchConfig(topic, uniqueGroupID("msgsize"))

			b.ResetTimer()
			runConsumerBenchmark(b, cfg, int64(defaultMessageCount), valueSize)
		})
	}
}

// BenchmarkThroughput_Workers measures consumer throughput at varying
// worker pool sizes. Reveals the sweet spot for parallelism.
func BenchmarkThroughput_Workers(b *testing.B) {
	const valueSize = 1024

	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			topic := uniqueTopic(fmt.Sprintf("workers-%d", workers))
			createTopic(b, topic, defaultPartitions)
			produceMessages(b, topic, defaultMessageCount, valueSize)

			cfg := benchConfig(topic, uniqueGroupID("workers"))
			cfg.WorkerCount = workers

			b.ResetTimer()
			runConsumerBenchmark(b, cfg, int64(defaultMessageCount), valueSize)
		})
	}
}

// BenchmarkThroughput_BatchSize measures consumer throughput at varying
// batch sizes. Reveals the trade-off between batch overhead and latency.
func BenchmarkThroughput_BatchSize(b *testing.B) {
	const valueSize = 1024

	for _, batchSize := range []int{10, 50, 100, 500} {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			topic := uniqueTopic(fmt.Sprintf("batch-%d", batchSize))
			createTopic(b, topic, defaultPartitions)
			produceMessages(b, topic, defaultMessageCount, valueSize)

			cfg := benchConfig(topic, uniqueGroupID("batch"))
			cfg.BatchSize = batchSize

			b.ResetTimer()
			runConsumerBenchmark(b, cfg, int64(defaultMessageCount), valueSize)
		})
	}
}

// BenchmarkThroughput_ProcessorLatency measures consumer throughput with
// simulated target service latency. Reveals how well the worker pool
// absorbs processor wait time.
func BenchmarkThroughput_ProcessorLatency(b *testing.B) {
	const valueSize = 1024

	for _, latency := range []time.Duration{0, time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond} {
		name := "0ms"
		if latency > 0 {
			name = latency.String()
		}
		b.Run(fmt.Sprintf("latency=%s", name), func(b *testing.B) {
			topic := uniqueTopic(fmt.Sprintf("latency-%s", name))
			createTopic(b, topic, defaultPartitions)
			produceMessages(b, topic, defaultMessageCount, valueSize)

			cfg := benchConfig(topic, uniqueGroupID("latency"))

			b.ResetTimer()
			benchRunWithLatency(b, cfg, int64(defaultMessageCount), valueSize, latency)
		})
	}
}

// BenchmarkThroughput_ChannelCap measures consumer throughput at varying
// internal channel capacities. Reveals the buffer size sweet spot.
func BenchmarkThroughput_ChannelCap(b *testing.B) {
	const valueSize = 1024

	for _, chanCap := range []int{10, 100, 500, 1000} {
		b.Run(fmt.Sprintf("cap=%d", chanCap), func(b *testing.B) {
			topic := uniqueTopic(fmt.Sprintf("chancap-%d", chanCap))
			createTopic(b, topic, defaultPartitions)
			produceMessages(b, topic, defaultMessageCount, valueSize)

			cfg := benchConfig(topic, uniqueGroupID("chancap"))
			cfg.ChannelCap = chanCap

			b.ResetTimer()
			runConsumerBenchmark(b, cfg, int64(defaultMessageCount), valueSize)
		})
	}
}
