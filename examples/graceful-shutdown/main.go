// Package main demonstrates graceful shutdown behavior. When the process
// receives SIGTERM or SIGINT, the consumer:
//
//  1. Stops polling for new messages
//  2. Drains in-flight batches (workers finish current work)
//  3. Commits final offsets to Kafka
//  4. Closes the Kafka consumer (leaves the consumer group)
//
// The ShutdownTimeout configuration controls how long steps 2-4 are allowed
// to take before the process exits forcefully.
//
// Run:
//
//	go run main.go
//
// Then send SIGTERM:
//
//	kill -TERM $(pgrep -f "graceful-shutdown")
//
// Or press Ctrl+C.
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lucasdecamargo/go-kafka-consumer/consumer"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug, // Use Debug to see shutdown internals.
	}))

	cfg := consumer.DefaultConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Topics = []string{"example-events"}
	cfg.GroupID = "graceful-shutdown-example"

	// Give the consumer up to 30 seconds to drain in-flight work on shutdown.
	// In Kubernetes, set this to less than terminationGracePeriodSeconds.
	cfg.ShutdownTimeout = 30 * time.Second

	processor := func(ctx context.Context, batch []consumer.Message) error {
		logger.Info("processing batch", slog.Int("size", len(batch)))

		// Simulate slow processing to demonstrate shutdown drain behavior.
		// During shutdown, the framework waits for this to complete.
		select {
		case <-time.After(2 * time.Second):
			logger.Info("batch processed successfully")
		case <-ctx.Done():
			// The context is canceled during shutdown. You can use this
			// to abort long-running work early if needed.
			logger.Warn("processing interrupted by shutdown")
			return ctx.Err()
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
	defer stop()

	logger.Info("consumer started — send SIGTERM or Ctrl+C to see graceful shutdown")
	if err := c.Run(ctx); err != nil {
		logger.Error("consumer exited with error", slog.String("error", err.Error()))
		os.Exit(1)
	}

	logger.Info("consumer stopped cleanly")
}
