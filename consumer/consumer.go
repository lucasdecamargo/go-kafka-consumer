// Package consumer provides a production-grade Kafka consumer framework
// with automatic batching, offset management, backpressure, retries,
// and dead letter queue routing.
//
// The developer implements a single BatchProcessor function. The framework
// handles all Kafka interaction, message routing, concurrency, and fault
// tolerance.
//
// Example:
//
//	cfg := consumer.DefaultConfig()
//	cfg.Brokers = []string{"localhost:9092"}
//	cfg.Topics = []string{"events"}
//	cfg.GroupID = "my-service"
//
//	c, err := consumer.New(cfg, func(ctx context.Context, batch []consumer.Message) error {
//	    for _, msg := range batch {
//	        log.Printf("received: %s", msg.Value)
//	    }
//	    return nil
//	},
//	    consumer.WithLogger(logger),
//	    consumer.WithMetrics(reg),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	if err := c.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
package consumer

import (
	"context"
	"errors"
)

// Consumer is the top-level entry point for the Kafka consumer framework.
// It assembles all internal components (poll loop, dispatcher, offset
// coordinator) and manages the consumer lifecycle.
type Consumer struct {
	cfg       Config
	processor BatchProcessor
	opts      options
}

// New creates a new Consumer with the given configuration, batch processor,
// and optional dependencies. It validates the configuration and returns an
// error if any values are invalid.
//
// The processor is a required parameter — it is the function the developer
// implements to process batches of messages.
func New(cfg Config, processor BatchProcessor, opts ...Option) (*Consumer, error) {
	if processor == nil {
		return nil, errors.New("consumer: batch processor must not be nil")
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	return &Consumer{
		cfg:       cfg,
		processor: processor,
		opts:      o,
	}, nil
}

// Run starts the consumer and blocks until the context is canceled or a
// fatal error occurs. On context cancellation, it performs a graceful
// shutdown: drains in-flight work, commits final offsets, and closes the
// Kafka consumer.
//
// Run is the main entry point after constructing a Consumer with New().
func (c *Consumer) Run(ctx context.Context) error {
	// Implementation in Phase 4/5: assemble poll loop, dispatcher,
	// offset coordinator, and run the main loop.
	_ = ctx
	return errors.New("consumer: not implemented")
}
