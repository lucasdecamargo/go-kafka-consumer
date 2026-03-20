package dispatcher

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lucasdecamargo/go-kafka-consumer/internal/benchutil"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/offset"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/types"
)

// BenchmarkAssembleBatch measures the time and allocations to slice messages
// from a partition buffer into a batch. This isolates the allocation cost
// of the copy + shift pattern used in assembleBatch.
func BenchmarkAssembleBatch(b *testing.B) {
	for _, size := range []int{10, 50, 100, 500} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			cfg := DefaultConfig()
			cfg.BatchSize = size

			d := &UnorderedDispatcher{
				cfg:        cfg,
				partitions: make(map[int32]*partitionBuffer),
			}

			// Pre-generate source messages once.
			source := benchutil.NewMessages(0, size*2, 1024)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				// Refill the partition buffer from pre-generated source.
				pb := &partitionBuffer{
					messages: make([]types.Message, len(source)),
				}
				copy(pb.messages, source)
				d.partitions[0] = pb

				d.assembleBatch(pb, 0)
			}
		})
	}
}

// BenchmarkDispatchRoundTrip measures the full dispatch cycle: Send →
// channel → worker → processor → onBatchDone. Uses a no-op processor
// to isolate framework overhead from application logic.
func BenchmarkDispatchRoundTrip(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			benchDispatchRoundTrip(b, workers, 50, 1024)
		})
	}
}

// BenchmarkDispatchRoundTrip_BatchSize measures dispatch round-trip time
// at varying batch sizes.
func BenchmarkDispatchRoundTrip_BatchSize(b *testing.B) {
	for _, batchSize := range []int{10, 50, 100, 500} {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			benchDispatchRoundTrip(b, 4, batchSize, 1024)
		})
	}
}

// BenchmarkDispatchRoundTrip_MessageSize measures dispatch round-trip time
// at varying message payload sizes.
func BenchmarkDispatchRoundTrip_MessageSize(b *testing.B) {
	for _, valueSize := range []int{100, 512, 1024, 10240} {
		b.Run(fmt.Sprintf("payload=%dB", valueSize), func(b *testing.B) {
			benchDispatchRoundTrip(b, 4, 50, valueSize)
		})
	}
}

func benchDispatchRoundTrip(b *testing.B, workers, batchSize, valueSize int) {
	b.Helper()

	cfg := DefaultConfig()
	cfg.WorkerCount = workers
	cfg.BatchSize = batchSize
	cfg.ChannelCap = 100
	cfg.LingerTime = 1 * time.Second // Prevent linger flushes during benchmark.

	var processed atomic.Int64
	processor := func(_ context.Context, batch []types.Message) error {
		processed.Add(int64(len(batch)))
		return nil
	}

	coord := offset.NewCoordinator(offset.WithLogger(slog.New(slog.DiscardHandler)))
	m := benchutil.DiscardDispatcherMetrics()

	d, err := NewUnorderedDispatcher(cfg, processor, coord,
		WithLogger(slog.New(slog.DiscardHandler)),
		WithMetrics(m),
	)
	if err != nil {
		b.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.Start(ctx, cancel)

	// Pre-generate exactly one batch worth of messages.
	msgs := benchutil.NewMessages(0, batchSize, valueSize)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		processed.Store(0)
		target := int64(batchSize)

		if err := d.Send(ctx, 0, msgs); err != nil {
			b.Fatal(err)
		}

		// Spin-wait for the batch to complete. In benchmarks this is
		// acceptable — we're measuring dispatch latency, not waiting cost.
		for processed.Load() < target {
			// Yield to scheduler.
		}
	}

	b.StopTimer()
	cancel()
	_ = d.Close(context.Background())
}

// BenchmarkBackoff measures the cost of backoff calculation. This is called
// on every retry attempt, so per-call overhead matters at high retry rates.
func BenchmarkBackoff(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = backoff(3)
	}
}

// BenchmarkChannelThroughput measures raw channel send/receive throughput
// at various buffer capacities. This establishes the ceiling for the
// dispatcher's batch channel.
func BenchmarkChannelThroughput(b *testing.B) {
	for _, cap := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("cap=%d", cap), func(b *testing.B) {
			ch := make(chan batch, cap)

			// Start a consumer goroutine that drains the channel.
			done := make(chan struct{})
			go func() {
				defer close(done)
				for range ch {
				}
			}()

			testBatch := batch{
				partition: 0,
				maxOffset: 1,
				messages:  make([]types.Message, 50),
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				ch <- testBatch
			}

			b.StopTimer()
			close(ch)
			<-done
		})
	}
}
