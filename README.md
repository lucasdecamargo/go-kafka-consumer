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
- **Kubernetes-native** — readiness probe gated on partition assignment, consumer lag metric for KEDA auto-scaling, and configurable grace period validation
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
| `PreStopDelay` | `0` | Duration of pod `preStop` hook; used for Kubernetes grace period validation |
| `LagReportInterval` | `10s` | How often librdkafka emits stats for `kafka_consumer_lag` updates |
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

Key metrics exposed:

| Metric | Type | Description |
|---|---|---|
| `kafka_consumer_lag` | Gauge | Uncommitted message lag `{group, topic, partition}`. Primary KEDA scaling signal. |
| `kafka_consumer_messages_polled_total` | Counter | Messages received from Kafka per partition. |
| `kafka_consumer_messages_processed_total` | Counter | Messages processed `{status, partition}` — `success`, `non_retryable`, `retries_exhausted`. |
| `kafka_consumer_message_delay_seconds` | Histogram | Poll-to-completion latency. Operator SLO metric. |
| `kafka_consumer_circuit_breaker_state` | Gauge | `0`=closed, `1`=half-open, `2`=open. |
| `kafka_consumer_degraded_mode` | Gauge | `1` when in degraded mode. |

See [`docs/metrics.md`](docs/metrics.md) for the full reference and PromQL dashboard queries.

### Health Probes

The HTTP server at `HealthAddr` exposes three endpoints:

| Endpoint | Probe | 200 when... |
|---|---|---|
| `/healthz` | Liveness | Poll loop goroutine is running |
| `/readyz` | Readiness | Not degraded **and** at least one partition is assigned |
| `/metrics` | — | Always (Prometheus scrape target) |

`/readyz` returns `503` during the startup window (before the first rebalance assigns partitions) and again when all partitions are revoked (scale-down scenario). This prevents Kubernetes from routing traffic to a consumer that has not yet joined the consumer group.

### Logging

Pass a structured logger:

```go
c, err := consumer.New(cfg, processor, consumer.WithLogger(slog.Default()))
```

## Kubernetes

### Graceful Shutdown

Set `terminationGracePeriodSeconds` to satisfy:

```
terminationGracePeriodSeconds ≥ ShutdownTimeout + PreStopDelay + 10s
```

The 10-second buffer accounts for SIGTERM propagation latency and kernel overhead. With defaults (`ShutdownTimeout=25s`, no preStop hook), set `terminationGracePeriodSeconds` to at least **35 seconds**.

Set `cfg.PreStopDelay` to match your pod's `lifecycle.preStop` sleep duration:

```go
cfg.ShutdownTimeout = 25 * time.Second
cfg.PreStopDelay    = 5 * time.Second
// → terminationGracePeriodSeconds must be ≥ 40
```

To enable startup validation, mirror the grace period in the container `env` block — Kubernetes does not inject this automatically:

```yaml
terminationGracePeriodSeconds: 60
spec:
  containers:
    - name: consumer
      env:
        - name: TERMINATION_GRACE_PERIOD_SECONDS
          value: "60"
```

If `TERMINATION_GRACE_PERIOD_SECONDS` is absent or too small, the consumer logs a `Warn` at startup with the recommended minimum. Detection uses `KUBERNETES_SERVICE_HOST`, which is injected automatically by the kubelet into every pod.

### KEDA Auto-Scaling

`kafka_consumer_lag` is the primary KEDA scaling signal. Use a Prometheus trigger pointing at your metrics endpoint:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: my-consumer
spec:
  scaleTargetRef:
    name: my-consumer
  minReplicaCount: 1
  maxReplicaCount: 10
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus.monitoring.svc:9090
        metricName: kafka_consumer_lag_total
        query: sum(kafka_consumer_lag{group="my-group"})
        threshold: "1000"   # scale out when lag exceeds 1000 messages per replica
```

Set `minReplicaCount: 1` (not 0) to keep at least one consumer in the group — a scale-to-zero consumer stops committing offsets and lag can grow unbounded.

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
