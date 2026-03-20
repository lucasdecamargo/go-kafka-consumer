// Package benchutil provides shared utilities for benchmark tests across
// internal packages. All helpers are designed for benchmark setup — they
// create lightweight, minimal implementations that satisfy interface
// contracts without production overhead.
package benchutil

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lucasdecamargo/go-kafka-consumer/internal/metrics"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/types"
)

// NewMessages creates count messages for the given partition with a payload
// of valueSize bytes. Messages have sequential offsets and realistic
// timestamps suitable for benchmarking.
func NewMessages(partition int32, count, valueSize int) []types.Message {
	now := time.Now()
	value := make([]byte, valueSize)
	// Fill with non-zero data to avoid zero-page optimizations.
	for i := range value {
		value[i] = byte(i % 256)
	}

	msgs := make([]types.Message, count)
	for i := range msgs {
		v := make([]byte, valueSize)
		copy(v, value)
		msgs[i] = types.Message{
			Topic:     "bench-topic",
			Partition: partition,
			Offset:    int64(i),
			Key:       []byte("key"),
			Value:     v,
			Timestamp: now.Add(-time.Duration(count-i) * time.Millisecond),
			PolledAt:  now,
		}
	}
	return msgs
}

// NoopProcessor returns a BatchProcessor that returns nil immediately.
func NoopProcessor() types.BatchProcessor {
	return func(_ context.Context, _ []types.Message) error {
		return nil
	}
}

// FixedLatencyProcessor returns a BatchProcessor that sleeps for the given
// duration, simulating target service latency.
func FixedLatencyProcessor(d time.Duration) types.BatchProcessor {
	return func(ctx context.Context, _ []types.Message) error {
		if d <= 0 {
			return nil
		}
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}
}

// DiscardDispatcherMetrics returns DispatcherMetrics registered against a
// throwaway Prometheus registry. Satisfies the metrics dependency without
// polluting the default registry.
func DiscardDispatcherMetrics() *metrics.DispatcherMetrics {
	return metrics.NewDispatcherMetrics(prometheus.NewRegistry())
}

// DiscardPollLoopMetrics returns PollLoopMetrics registered against a
// throwaway Prometheus registry.
func DiscardPollLoopMetrics() *metrics.PollLoopMetrics {
	return metrics.NewPollLoopMetrics(prometheus.NewRegistry())
}
