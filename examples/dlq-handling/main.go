// Package main demonstrates Dead Letter Queue (DLQ) handling. When a batch
// processor encounters a non-retryable error (e.g., invalid payload, business
// rule violation), wrapping the error with consumer.ErrNonRetryable causes the
// framework to:
//
//  1. Skip retries for the batch
//  2. Publish the failed messages to the configured DLQ topic
//  3. Include error details as Kafka message headers
//  4. Continue processing the next batch
//
// Retryable errors (plain errors) are retried up to MaxRetries times with
// exponential backoff. Only after all retries are exhausted is the batch
// routed to the DLQ.
//
// Run:
//
//	# First, create the DLQ topic:
//	docker exec kafka kafka-topics.sh --create \
//	  --topic example-events-dlq \
//	  --partitions 3 \
//	  --replication-factor 1 \
//	  --bootstrap-server localhost:9092
//
//	go run main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/lucasdecamargo/go-kafka-consumer/consumer"
)

// Event represents the expected message payload.
type Event struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Data string `json:"data"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg := consumer.DefaultConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Topics = []string{"example-events"}
	cfg.GroupID = "dlq-example"

	// Enable DLQ — failed messages are published to this topic.
	cfg.DLQTopic = "example-events-dlq"

	// Retry transient errors up to 3 times before routing to DLQ.
	cfg.MaxRetries = 3

	processor := func(ctx context.Context, batch []consumer.Message) error {
		for _, msg := range batch {
			var event Event
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				// Parsing failure — retrying won't help.
				// Wrap with ErrNonRetryable to skip retries and go straight to DLQ.
				return &consumer.ErrNonRetryable{
					Err: fmt.Errorf("invalid JSON in message partition=%d offset=%d: %w",
						msg.Partition, msg.Offset, err),
				}
			}

			if event.Type == "" {
				// Business rule violation — also non-retryable.
				return &consumer.ErrNonRetryable{
					Err: fmt.Errorf("missing event type in message partition=%d offset=%d",
						msg.Partition, msg.Offset),
				}
			}

			logger.Info("processed event",
				slog.String("id", event.ID),
				slog.String("type", event.Type),
			)
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

	logger.Info("starting consumer with DLQ enabled",
		slog.String("dlq_topic", cfg.DLQTopic),
	)
	if err := c.Run(ctx); err != nil {
		log.Fatalf("consumer error: %v", err)
	}
}
