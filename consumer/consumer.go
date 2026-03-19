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
	"fmt"
	"log/slog"

	"github.com/lucasdecamargo/go-kafka-consumer/internal/dispatcher"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/kafka"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/offset"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/pollloop"
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
// The caller is responsible for signal handling. The idiomatic pattern is
// to use signal.NotifyContext to cancel the context on SIGTERM/SIGINT:
//
//	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
//	defer stop()
//	if err := c.Run(ctx); err != nil { ... }
//
// Run is the main entry point after constructing a Consumer with New().
func (c *Consumer) Run(ctx context.Context) error {
	logger := c.opts.logger
	reg := c.opts.registerer

	logger.Info("consumer starting",
		slog.String("group_id", c.cfg.GroupID),
		slog.Any("topics", c.cfg.Topics),
		slog.String("dispatch_mode", c.cfg.DispatchMode.String()),
		slog.Int("workers", c.cfg.WorkerCount),
	)

	// 1. Create OffsetCoordinator.
	coord := offset.NewCoordinator(
		offset.WithLogger(logger.With(slog.String("component", "offset-coordinator"))),
	)

	// 2. Create Dispatcher based on dispatch mode.
	disp, err := c.createDispatcher(coord, logger)
	if err != nil {
		return fmt.Errorf("consumer: create dispatcher: %w", err)
	}

	// 3. Create Health state.
	health := &pollloop.Health{}

	// 4. Create rebalance forwarder to break the circular dependency
	// between the Kafka adapter (needs RebalanceHandler) and the PollLoop
	// (needs KafkaConsumer). Safe because rebalance callbacks only fire
	// during Poll(), which runs after the PollLoop is fully constructed.
	fwd := &rebalanceForwarder{}

	// 5. Create Kafka adapter with mapped config.
	kafkaCfg := c.mapKafkaConfig()
	adapter, err := kafka.NewAdapter(kafkaCfg, fwd,
		kafka.WithLogger(logger.With(slog.String("component", "kafka-adapter"))),
	)
	if err != nil {
		return fmt.Errorf("consumer: create kafka adapter: %w", err)
	}

	// 6. Create PollLoop.
	plCfg := pollloop.Config{
		PollInterval:           c.cfg.PollInterval,
		CommitInterval:         c.cfg.CommitInterval,
		CommitFailureThreshold: c.cfg.CommitFailureThreshold,
		ShutdownTimeout:        c.cfg.ShutdownTimeout,
	}

	pl, err := pollloop.New(plCfg, adapter, disp, coord, health,
		pollloop.WithLogger(logger.With(slog.String("component", "poll-loop"))),
		pollloop.WithMetrics(reg),
	)
	if err != nil {
		_ = adapter.Close()
		return fmt.Errorf("consumer: create poll loop: %w", err)
	}

	// 7. Complete the forwarder — the PollLoop is the rebalance handler.
	fwd.target = pl

	// 8. Start dispatcher workers.
	disp.Start(ctx)

	logger.Info("consumer started — all components assembled")

	// 9. Run the poll loop (blocks until ctx canceled).
	// PollLoop.Run handles graceful shutdown: drain dispatcher, final
	// commit, close Kafka consumer.
	return pl.Run(ctx)
}

// createDispatcher builds the appropriate Dispatcher implementation based
// on the configured dispatch mode, wiring in the processor, coordinator,
// and optional dependencies (logger, metrics, DLQ).
func (c *Consumer) createDispatcher(
	coord offset.Coordinator,
	logger *slog.Logger,
) (dispatcher.Dispatcher, error) {
	dispCfg := dispatcher.Config{
		WorkerCount:        c.cfg.WorkerCount,
		ChannelCap:         c.cfg.ChannelCap,
		BatchSize:          c.cfg.BatchSize,
		LingerTime:         c.cfg.LingerTime,
		MaxRetries:         c.cfg.MaxRetries,
		CBMinRequests:      c.cfg.CBMinRequests,
		CBFailureThreshold: c.cfg.CBFailureThreshold,
		CBOpenTimeout:      c.cfg.CBOpenTimeout,
		CBMaxRequests:      c.cfg.CBMaxRequests,
		CBInterval:         c.cfg.CBInterval,
	}

	dispLogger := logger.With(slog.String("component", "dispatcher"))

	switch c.cfg.DispatchMode {
	case Unordered:
		return dispatcher.NewUnorderedDispatcher(
			dispCfg,
			c.processor,
			coord,
			dispatcher.WithLogger(dispLogger),
			// DLQ producer is wired via consumer-level options (future WI-5).
		)

	case PartitionOrdered:
		return nil, fmt.Errorf("dispatch mode %q is not yet implemented", c.cfg.DispatchMode)

	case KeyOrdered:
		return nil, fmt.Errorf("dispatch mode %q is not yet implemented", c.cfg.DispatchMode)

	default:
		return nil, fmt.Errorf("unknown dispatch mode: %d", c.cfg.DispatchMode)
	}
}

// mapKafkaConfig translates the public consumer.Config and
// consumer.SecurityConfig to the internal kafka.AdapterConfig.
func (c *Consumer) mapKafkaConfig() kafka.AdapterConfig {
	cfg := kafka.AdapterConfig{
		Brokers: c.cfg.Brokers,
		GroupID: c.cfg.GroupID,
		Topics:  c.cfg.Topics,
		Security: kafka.SecurityConfig{
			Protocol: string(c.cfg.Security.Protocol),
		},
	}

	if c.cfg.Security.TLS != nil {
		cfg.Security.TLS = &kafka.TLSConfig{
			CAFile:             c.cfg.Security.TLS.CAFile,
			CertFile:           c.cfg.Security.TLS.CertFile,
			KeyFile:            c.cfg.Security.TLS.KeyFile,
			InsecureSkipVerify: c.cfg.Security.TLS.InsecureSkipVerify,
		}
	}

	if c.cfg.Security.SASL != nil {
		cfg.Security.SASL = &kafka.SASLConfig{
			Mechanism:         string(c.cfg.Security.SASL.Mechanism),
			Username:          c.cfg.Security.SASL.Username,
			Password:          c.cfg.Security.SASL.Password,
			OAuthBearerConfig: c.cfg.Security.SASL.OAuthBearerConfig,
		}
	}

	return cfg
}

// rebalanceForwarder breaks the circular dependency between the Kafka
// adapter and the PollLoop. The adapter needs a RebalanceHandler at
// construction time, but the PollLoop (which implements RebalanceHandler)
// needs the adapter as its KafkaConsumer.
//
// This is safe because rebalance callbacks only fire during Poll(), which
// is only called from PollLoop.Run() — by which time the target is set.
type rebalanceForwarder struct {
	target pollloop.RebalanceHandler
}

// OnPartitionsAssigned forwards to the PollLoop.
func (f *rebalanceForwarder) OnPartitionsAssigned(partitions []dispatcher.Partition) {
	f.target.OnPartitionsAssigned(partitions)
}

// OnPartitionsRevoked forwards to the PollLoop.
func (f *rebalanceForwarder) OnPartitionsRevoked(partitions []dispatcher.Partition) {
	f.target.OnPartitionsRevoked(partitions)
}
