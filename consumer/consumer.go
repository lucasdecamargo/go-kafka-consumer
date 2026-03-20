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
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lucasdecamargo/go-kafka-consumer/internal/dispatcher"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/kafka"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/metrics"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/offset"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/pollloop"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/server"
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

	// Register all Prometheus metrics.
	plMetrics := metrics.NewPollLoopMetrics(reg)
	dispMetrics := metrics.NewDispatcherMetrics(reg)

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

	// 2. Create DLQ producer (if configured).
	var dlq *kafka.DLQProducer
	if c.cfg.DLQTopic != "" {
		dlqCfg := kafka.DLQProducerConfig{
			Topic:    c.cfg.DLQTopic,
			Brokers:  c.cfg.Brokers,
			Security: c.mapSecurityConfig(),
		}

		var err error
		dlq, err = kafka.NewDLQProducer(dlqCfg,
			kafka.WithDLQLogger(logger.With(slog.String("component", "dlq-producer"))),
		)
		if err != nil {
			return fmt.Errorf("consumer: create DLQ producer: %w", err)
		}
		defer dlq.Close()

		logger.Info("DLQ producer created", slog.String("topic", c.cfg.DLQTopic))
	}

	// 3. Create Dispatcher based on dispatch mode.
	disp, err := c.createDispatcher(coord, dlq, dispMetrics, logger)
	if err != nil {
		return fmt.Errorf("consumer: create dispatcher: %w", err)
	}

	// 4. Create Health state.
	health := &pollloop.Health{}

	// 5. Create rebalance forwarder to break the circular dependency
	// between the Kafka adapter (needs RebalanceHandler) and the PollLoop
	// (needs KafkaConsumer). Safe because rebalance callbacks only fire
	// during Poll(), which runs after the PollLoop is fully constructed.
	fwd := &rebalanceForwarder{}

	// 6. Create Kafka adapter with mapped config.
	kafkaCfg := c.mapKafkaConfig()
	adapter, err := kafka.NewAdapter(kafkaCfg, fwd,
		kafka.WithLogger(logger.With(slog.String("component", "kafka-adapter"))),
	)
	if err != nil {
		return fmt.Errorf("consumer: create kafka adapter: %w", err)
	}

	// 7. Create PollLoop.
	plCfg := pollloop.Config{
		PollInterval:           c.cfg.PollInterval,
		CommitInterval:         c.cfg.CommitInterval,
		CommitFailureThreshold: c.cfg.CommitFailureThreshold,
		ShutdownTimeout:        c.cfg.ShutdownTimeout,
	}

	pl, err := pollloop.New(plCfg, adapter, disp, coord, health,
		pollloop.WithLogger(logger.With(slog.String("component", "poll-loop"))),
		pollloop.WithMetrics(plMetrics),
	)
	if err != nil {
		_ = adapter.Close()
		return fmt.Errorf("consumer: create poll loop: %w", err)
	}

	// 8. Complete the forwarder — the PollLoop is the rebalance handler.
	fwd.target = pl

	// 9. Start dispatcher workers.
	// Create a cancelable context for the entire run. Components that
	// detect fatal conditions (e.g., worker panics) call runCancel,
	// which propagates shutdown to the poll loop and all workers.
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	disp.Start(runCtx, runCancel)

	// 10. Start HTTP health/metrics server (if configured).
	if c.cfg.HealthAddr != "" {
		gatherer := c.resolveGatherer(reg)
		srv := server.New(
			server.Config{Addr: c.cfg.HealthAddr},
			health,
			gatherer,
			logger.With(slog.String("component", "health-server")),
		)

		go func() {
			if err := srv.ListenAndServe(); err != nil {
				logger.Error("health server error", slog.String("error", err.Error()))
			}
		}()

		// Shut down the HTTP server when the poll loop exits.
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				logger.Warn("health server shutdown error", slog.String("error", err.Error()))
			}
		}()
	}

	logger.Info("consumer started — all components assembled")

	// 11. Run the poll loop (blocks until runCtx canceled).
	// PollLoop.Run handles graceful shutdown: drain dispatcher, final
	// commit, close Kafka consumer.
	return pl.Run(runCtx)
}

// createDispatcher builds the appropriate Dispatcher implementation based
// on the configured dispatch mode, wiring in the processor, coordinator,
// and optional dependencies (logger, metrics, DLQ).
func (c *Consumer) createDispatcher(
	coord offset.Coordinator,
	dlq *kafka.DLQProducer,
	dm *metrics.DispatcherMetrics,
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

	dispOpts := []dispatcher.UnorderedOption{
		dispatcher.WithLogger(dispLogger),
		dispatcher.WithMetrics(dm),
	}
	if dlq != nil {
		dispOpts = append(dispOpts, dispatcher.WithDLQProducer(dlq))
	}

	switch c.cfg.DispatchMode {
	case Unordered:
		return dispatcher.NewUnorderedDispatcher(
			dispCfg,
			c.processor,
			coord,
			dispOpts...,
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
	return kafka.AdapterConfig{
		Brokers:  c.cfg.Brokers,
		GroupID:  c.cfg.GroupID,
		Topics:   c.cfg.Topics,
		Security: c.mapSecurityConfig(),
	}
}

// mapSecurityConfig translates consumer.SecurityConfig to kafka.SecurityConfig.
func (c *Consumer) mapSecurityConfig() kafka.SecurityConfig {
	sec := kafka.SecurityConfig{
		Protocol: string(c.cfg.Security.Protocol),
	}

	if c.cfg.Security.TLS != nil {
		sec.TLS = &kafka.TLSConfig{
			CAFile:             c.cfg.Security.TLS.CAFile,
			CertFile:           c.cfg.Security.TLS.CertFile,
			KeyFile:            c.cfg.Security.TLS.KeyFile,
			InsecureSkipVerify: c.cfg.Security.TLS.InsecureSkipVerify,
		}
	}

	if c.cfg.Security.SASL != nil {
		sec.SASL = &kafka.SASLConfig{
			Mechanism:         string(c.cfg.Security.SASL.Mechanism),
			Username:          c.cfg.Security.SASL.Username,
			Password:          c.cfg.Security.SASL.Password,
			OAuthBearerConfig: c.cfg.Security.SASL.OAuthBearerConfig,
		}
	}

	return sec
}

// resolveGatherer returns a prometheus.Gatherer from the configured
// registerer. If the registerer also implements Gatherer (as
// prometheus.Registry does), it is used directly. Otherwise, falls back
// to the default gatherer.
func (c *Consumer) resolveGatherer(reg prometheus.Registerer) prometheus.Gatherer {
	if g, ok := reg.(prometheus.Gatherer); ok {
		return g
	}
	return prometheus.DefaultGatherer
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
