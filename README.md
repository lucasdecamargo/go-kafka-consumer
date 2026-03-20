# go-kafka-consumer

[![CI](https://github.com/lucasdecamargo/go-kafka-consumer/actions/workflows/ci.yml/badge.svg)](https://github.com/lucasdecamargo/go-kafka-consumer/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/lucasdecamargo/go-kafka-consumer/branch/main/graph/badge.svg)](https://codecov.io/gh/lucasdecamargo/go-kafka-consumer)
[![Go Reference](https://pkg.go.dev/badge/github.com/lucasdecamargo/go-kafka-consumer.svg)](https://pkg.go.dev/github.com/lucasdecamargo/go-kafka-consumer)
[![Go Report Card](https://goreportcard.com/badge/github.com/lucasdecamargo/go-kafka-consumer)](https://goreportcard.com/report/github.com/lucasdecamargo/go-kafka-consumer)

A production-grade Go framework for consuming Apache Kafka messages with automatic batching, offset management, backpressure, retries, and dead letter queue routing.

You implement a single `BatchProcessor` function. The framework handles all Kafka interaction, message routing, concurrency, and fault tolerance.

## Features

- **Automatic batching** — Configurable batch size and linger time for high-throughput processing
- **Worker pool** — Concurrent message processing with configurable parallelism
- **Backpressure** — Pause/resume flow control with bounded in-memory buffering
- **Retries with circuit breaker** — Exponential backoff with configurable circuit breaker to protect downstream services
- **Dead Letter Queue** — Non-retryable errors route to a DLQ topic with error metadata headers
- **Offset management** — Per-partition offset tracking with periodic commits
- **Graceful shutdown** — Drains in-flight work, commits final offsets, and closes the consumer group
- **Observability** — Prometheus metrics, structured logging (`slog`), and HTTP health probes
- **Pluggable ordering** — Unordered (max throughput), partition-ordered, or key-ordered dispatch modes

## Installation

```bash
go get github.com/lucasdecamargo/go-kafka-consumer
```

> **Note:** This module depends on [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go),
> which requires CGo and `librdkafka`. On most systems this is handled automatically.

## Quick Start

```go
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
    cfg := consumer.DefaultConfig()
    cfg.Brokers = []string{"localhost:9092"}
    cfg.Topics = []string{"events"}
    cfg.GroupID = "my-service"

    c, err := consumer.New(cfg, func(ctx context.Context, batch []consumer.Message) error {
        for _, msg := range batch {
            fmt.Printf("partition=%d offset=%d key=%s\n", msg.Partition, msg.Offset, msg.Key)
        }
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
    defer stop()

    if err := c.Run(ctx); err != nil {
        log.Fatal(err)
    }
}
```

## Configuration

Start with `consumer.DefaultConfig()` and override what you need:

| Field | Default | Description |
|---|---|---|
| `Brokers` | *(required)* | Kafka broker addresses |
| `Topics` | *(required)* | Topics to subscribe to |
| `GroupID` | *(required)* | Consumer group ID |
| `DispatchMode` | `Unordered` | `Unordered`, `PartitionOrdered`, or `KeyOrdered` |
| `WorkerCount` | `4` | Concurrent worker goroutines |
| `BatchSize` | `50` | Max messages per batch |
| `LingerTime` | `100ms` | Max wait before dispatching a partial batch |
| `ChannelCap` | `100` | Dispatch channel capacity (backpressure bound) |
| `MaxRetries` | `3` | Retry attempts for failed batches |
| `CommitInterval` | `5s` | Offset commit frequency |
| `ShutdownTimeout` | `25s` | Max graceful shutdown duration |
| `DLQTopic` | `""` | Dead letter queue topic (empty = disabled) |
| `HealthAddr` | `":8080"` | Health/metrics HTTP server address (empty = disabled) |

See [`consumer.Config`](https://pkg.go.dev/github.com/lucasdecamargo/go-kafka-consumer/consumer#Config) for the full list including circuit breaker and security settings.

## Error Handling

```go
// Retryable error — the framework retries with exponential backoff.
return fmt.Errorf("database timeout: %w", err)

// Non-retryable error — skips retries, routes directly to DLQ.
return &consumer.ErrNonRetryable{Err: fmt.Errorf("invalid payload: %w", err)}
```

## Security

Supports `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, and `SASL_SSL` with PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, and OAUTHBEARER mechanisms:

```go
cfg.Security = consumer.SecurityConfig{
    Protocol: consumer.ProtocolSASLSSL,
    TLS:      &consumer.TLSConfig{CAFile: "/etc/kafka/ca.pem"},
    SASL:     &consumer.SASLConfig{
        Mechanism: consumer.SASLSCRAMSHA512,
        Username:  os.Getenv("KAFKA_USERNAME"),
        Password:  os.Getenv("KAFKA_PASSWORD"),
    },
}
```

## Observability

### Metrics

Pass a `prometheus.Registerer` to expose framework metrics:

```go
reg := prometheus.NewRegistry()
c, err := consumer.New(cfg, processor, consumer.WithMetrics(reg))
```

The built-in HTTP server at `HealthAddr` exposes `/metrics` (Prometheus) and `/healthz` (liveness).

### Logging

Pass a structured logger:

```go
c, err := consumer.New(cfg, processor, consumer.WithLogger(slog.Default()))
```

## Examples

The [`examples/`](examples/) directory contains runnable examples:

| Example | Description |
|---|---|
| [`basic`](examples/basic/) | Minimal consumer setup |
| [`batch-processing`](examples/batch-processing/) | Tuning batch size, linger time, and worker count |
| [`graceful-shutdown`](examples/graceful-shutdown/) | Signal handling and shutdown timeout |
| [`dlq-handling`](examples/dlq-handling/) | Dead letter queue with non-retryable errors |
| [`custom-metrics`](examples/custom-metrics/) | Custom Prometheus registry |
| [`security`](examples/security/) | TLS and SASL authentication |

## Architecture

```
Kafka ──► Poll Loop ──► Dispatcher ──► Worker Pool ──► BatchProcessor (your code)
               │              │              │
          offset mgmt    backpressure    circuit breaker
               │              │              │
          commit ticker   pause/resume   DLQ routing
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full design, and [`docs/adr/`](docs/adr/) for architectural decision records.

## Development

```bash
# Unit tests
go test ./...

# Integration tests (requires Docker)
go test -tags integration -timeout 300s ./tests/integration/

# Microbenchmarks
go test -bench=. -benchmem ./internal/dispatcher/ ./internal/offset/

# Lint
golangci-lint run
```

## License

See [LICENSE](LICENSE) for details.
