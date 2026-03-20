// Package main demonstrates the simplest possible usage of the go-kafka-consumer
// framework. It creates a consumer with default settings and logs every message
// received.
//
// Run:
//
//	go run main.go
//
// Stop with Ctrl+C.
package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"

	"github.com/lucasdecamargo/go-kafka-consumer/consumer"
)

func main() {
	// 1. Start with production defaults and set the required fields.
	cfg := consumer.DefaultConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Topics = []string{"example-events"}
	cfg.GroupID = "basic-example"

	// 2. Define the processor — this is the only business logic you write.
	//    The framework calls this function with batches of messages.
	processor := func(ctx context.Context, batch []consumer.Message) error {
		for _, msg := range batch {
			fmt.Printf("partition=%d offset=%d key=%s value=%s\n",
				msg.Partition, msg.Offset, msg.Key, msg.Value,
			)
		}
		return nil
	}

	// 3. Create the consumer.
	c, err := consumer.New(cfg, processor)
	if err != nil {
		log.Fatalf("create consumer: %v", err)
	}

	// 4. Run until SIGTERM or SIGINT.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := c.Run(ctx); err != nil {
		log.Fatalf("consumer error: %v", err)
	}
}
