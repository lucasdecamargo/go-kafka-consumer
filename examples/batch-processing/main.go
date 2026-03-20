// Package main demonstrates how to tune batch assembly for high-throughput
// processing. The framework collects messages into batches before calling the
// processor, amortizing per-message overhead.
//
// Key configuration knobs:
//   - BatchSize:   max messages per batch (larger = higher throughput, more memory)
//   - LingerTime:  max wait before dispatching a partial batch (lower = lower latency)
//   - WorkerCount: concurrent processor goroutines (more = better for I/O-bound work)
//   - ChannelCap:  internal buffer between poll loop and workers (backpressure control)
//
// Run:
//
//	go run main.go
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lucasdecamargo/go-kafka-consumer/consumer"
)

func main() {
	cfg := consumer.DefaultConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Topics = []string{"example-events"}
	cfg.GroupID = "batch-example"

	// Tune batch assembly for high throughput.
	cfg.BatchSize = 200                    // Collect up to 200 messages per batch.
	cfg.LingerTime = 50 * time.Millisecond // Wait up to 50ms for a full batch.
	cfg.WorkerCount = 8                    // 8 concurrent workers for I/O-bound processing.
	cfg.ChannelCap = 500                   // Buffer up to 500 batches in the dispatch channel.

	// Use a structured logger so you can see framework internals.
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	processor := func(ctx context.Context, batch []consumer.Message) error {
		// In a real application, you would write the batch to a database,
		// send it to an HTTP API, or perform any bulk operation.
		logger.Info("processing batch",
			slog.Int("size", len(batch)),
			slog.Int64("first_offset", batch[0].Offset),
			slog.Int64("last_offset", batch[len(batch)-1].Offset),
		)

		// Simulate work — replace with your actual processing logic.
		for i := range batch {
			_ = fmt.Sprintf("processed key=%s", batch[i].Key)
		}

		return nil
	}

	c, err := consumer.New(cfg, processor,
		consumer.WithLogger(logger),
	)
	if err != nil {
		log.Fatalf("create consumer: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)

	logger.Info("starting batch processing consumer")
	err = c.Run(ctx)
	stop()
	if err != nil {
		log.Fatalf("consumer error: %v", err)
	}
}
