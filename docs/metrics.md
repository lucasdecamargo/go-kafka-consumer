# Prometheus Metrics Reference

All metrics follow the naming convention `kafka_consumer_<component>_<metric>_<unit>` and are registered via the `prometheus.Registerer` passed to `consumer.WithMetrics()`.

## Poll Loop Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `kafka_consumer_poll_loop_messages_total` | Counter | Total messages received from Kafka. Rate: `rate(kafka_consumer_poll_loop_messages_total[5m])`. |
| `kafka_consumer_poll_loop_poll_errors_total` | Counter | Total `Poll()` errors. |
| `kafka_consumer_poll_loop_commits_total` | Counter | Total successful offset commits. |
| `kafka_consumer_poll_loop_commit_failures_total` | Counter | Total failed offset commits. |
| `kafka_consumer_poll_loop_degraded_mode` | Gauge | Whether the poll loop is in degraded mode (1=degraded, 0=normal). |

## Dispatcher Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `kafka_consumer_dispatcher_batches_total` | Counter | `status` | Total batches processed. Status values: `success`, `non_retryable`, `retries_exhausted`. |
| `kafka_consumer_dispatcher_batch_latency_seconds` | Histogram | — | End-to-end batch processing latency (dispatch to completion). Uses default Prometheus buckets. |
| `kafka_consumer_dispatcher_retries_total` | Counter | — | Total retry attempts across all batches. |
| `kafka_consumer_dispatcher_circuit_breaker_state` | Gauge | — | Current circuit breaker state: 0=closed, 1=half-open, 2=open. |
| `kafka_consumer_dispatcher_workers_active` | Gauge | — | Number of workers currently processing a batch. |
| `kafka_consumer_dispatcher_dlq_messages_total` | Counter | `reason` | Total messages sent to the DLQ. Reason values: `non_retryable`, `retries_exhausted`. |

## Grafana Dashboard Queries

### Throughput (messages/sec)
```promql
rate(kafka_consumer_poll_loop_messages_total[5m])
```

### Batch success rate
```promql
rate(kafka_consumer_dispatcher_batches_total{status="success"}[5m])
/ rate(kafka_consumer_dispatcher_batches_total[5m])
```

### p99 batch latency
```promql
histogram_quantile(0.99, rate(kafka_consumer_dispatcher_batch_latency_seconds_bucket[5m]))
```

### DLQ rate
```promql
rate(kafka_consumer_dispatcher_dlq_messages_total[5m])
```

### Circuit breaker state
```promql
kafka_consumer_dispatcher_circuit_breaker_state
```

### Worker utilization
```promql
kafka_consumer_dispatcher_workers_active
```
