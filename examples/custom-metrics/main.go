// Package main demonstrates how to use a custom Prometheus registry with the
// consumer framework. The framework automatically registers metrics for:
//
//   - Poll loop throughput and latency
//   - Dispatcher batch processing latency and errors
//   - Worker utilization
//   - Offset commit success/failure
//
// By passing a custom prometheus.Registry via consumer.WithMetrics(), you get
// an isolated metrics namespace that doesn't collide with the default registry.
// The framework's built-in health server exposes these metrics at /metrics.
//
// You can also add your own application-level metrics to the same registry.
//
// Run:
//
//	go run main.go
//
// Then visit:
//
//	curl http://localhost:9090/metrics
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lucasdecamargo/go-kafka-consumer/consumer"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Create a custom Prometheus registry — keeps metrics isolated.
	reg := prometheus.NewRegistry()

	// Register your own application-level metrics alongside the framework's.
	eventsProcessed := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "app_events_processed_total",
			Help: "Total events processed by type.",
		},
		[]string{"event_type"},
	)
	reg.MustRegister(eventsProcessed)

	cfg := consumer.DefaultConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Topics = []string{"example-events"}
	cfg.GroupID = "metrics-example"

	// Use a custom address for the health/metrics server.
	cfg.HealthAddr = ":9090"

	processor := func(ctx context.Context, batch []consumer.Message) error {
		for range batch {
			// In a real app, parse the event type from the message.
			eventsProcessed.WithLabelValues("unknown").Inc()
		}
		logger.Info("batch processed", slog.Int("size", len(batch)))
		return nil
	}

	c, err := consumer.New(cfg, processor,
		consumer.WithLogger(logger),
		consumer.WithMetrics(reg), // Pass the custom registry.
	)
	if err != nil {
		log.Fatalf("create consumer: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)

	logger.Info("starting consumer with custom metrics",
		slog.String("metrics_endpoint", "http://localhost"+cfg.HealthAddr+"/metrics"),
	)
	err = c.Run(ctx)
	stop()
	if err != nil {
		log.Fatalf("consumer error: %v", err)
	}
}
