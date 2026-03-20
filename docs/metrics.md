# Prometheus Metrics Reference

All metrics follow the naming convention `kafka_consumer_<component>_<metric>_<unit>` and are registered via the `prometheus.Registerer` passed to `consumer.WithMetrics()`.

Metric design informed by [Confluent Parallel Consumer](https://github.com/confluentinc/parallel-consumer), [Jaeger Kafka Ingester](https://github.com/jaegertracing/jaeger), kafka_exporter, segmentio/kafka-go, and [WarpStream's time-based lag analysis](https://www.warpstream.com/blog/the-kafka-metric-youre-not-using-stop-counting-messages-start-measuring-time).

## Poll Loop Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `kafka_consumer_poll_loop_messages_polled_total` | Counter | `partition` | Total messages received from Kafka, per partition. Primary ingestion throughput signal. |
| `kafka_consumer_poll_loop_poll_errors_total` | Counter | — | Total `Poll()` errors. |
| `kafka_consumer_poll_loop_commits_total` | Counter | `partition` | Total successful offset commits, per partition. |
| `kafka_consumer_poll_loop_commit_failures_total` | Counter | — | Total failed offset commits. Bulk commits succeed or fail atomically, so partition attribution is not meaningful. Consecutive failures trigger degraded mode. |
| `kafka_consumer_poll_loop_degraded_mode` | Gauge | — | Whether the poll loop is in degraded mode (1=degraded, 0=normal). |
| `kafka_consumer_poll_loop_partitions_assigned` | Gauge | — | Number of partitions currently assigned to this consumer instance. Oscillation signals rebalance storms. |
| `kafka_consumer_poll_loop_rebalances_total` | Counter | — | Total rebalance events (incremented on each revoke). Rate > 2/hour signals consumer group instability. |
| `kafka_consumer_poll_loop_last_committed_offset` | Gauge | `partition` | Last successfully committed offset per partition. A flat value while `messages_polled_total` grows indicates a stuck partition. |
| `kafka_consumer_poll_loop_partitions_paused` | Gauge | — | Number of partitions currently paused due to backpressure or degraded mode. Direct flow-control visibility. |

## Dispatcher Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `kafka_consumer_dispatcher_messages_processed_total` | Counter | `status`, `partition` | Total messages processed. Status: `success`, `non_retryable`, `retries_exhausted`. |
| `kafka_consumer_dispatcher_message_delay_seconds` | Histogram | `partition` | Time from poll to processing completion per message (`time.Since(msg.PolledAt)`). Captures queueing, batching, channel wait, and processor execution time. Operator SLO metric. |
| `kafka_consumer_dispatcher_processing_time_seconds` | Histogram | `partition` | Processor function execution time only (`time.Since(start)` around `processor(ctx, batch)`). Developer tuning metric — isolates target service latency from pipeline overhead. |
| `kafka_consumer_dispatcher_record_age_seconds` | Histogram | `partition` | End-to-end latency from message production to processing completion (`time.Now() - msg.Timestamp`). Captures broker transit, partition wait, and framework delay. The time-domain equivalent of consumer lag — directly answers "how stale are the messages we're processing?" |
| `kafka_consumer_dispatcher_inflight_messages` | Gauge | `partition` | Messages currently buffered in worker channels or being processed, per partition. Backpressure visibility — shows which partition is filling up. |
| `kafka_consumer_dispatcher_circuit_breaker_state` | Gauge | — | Current circuit breaker state: 0=closed, 1=half-open, 2=open. |
| `kafka_consumer_dispatcher_workers_active` | Gauge | — | Number of workers currently processing a batch. |
| `kafka_consumer_dispatcher_dlq_messages_total` | Counter | `reason`, `partition` | Messages sent to the DLQ. Reason: `non_retryable`, `retries_exhausted`. |

## PromQL Dashboard Queries

### Ingestion throughput (messages/sec)
```promql
sum(rate(kafka_consumer_poll_loop_messages_polled_total[5m]))
```

### Ingestion throughput per partition
```promql
rate(kafka_consumer_poll_loop_messages_polled_total[5m])
```

### Processing throughput (messages/sec)
```promql
sum(rate(kafka_consumer_dispatcher_messages_processed_total[5m]))
```

### Ingestion vs processing gap (backlog growth rate)
```promql
sum(rate(kafka_consumer_poll_loop_messages_polled_total[5m]))
- sum(rate(kafka_consumer_dispatcher_messages_processed_total{status="success"}[5m]))
```

### Message success rate
```promql
sum(rate(kafka_consumer_dispatcher_messages_processed_total{status="success"}[5m]))
/ sum(rate(kafka_consumer_dispatcher_messages_processed_total[5m]))
```

### Per-partition success rate
```promql
rate(kafka_consumer_dispatcher_messages_processed_total{status="success"}[5m])
/ on(partition) rate(kafka_consumer_dispatcher_messages_processed_total[5m])
```

### p99 message delay (poll-to-completion)
```promql
histogram_quantile(0.99, sum by (le)(rate(kafka_consumer_dispatcher_message_delay_seconds_bucket[5m])))
```

### p99 message delay per partition (spot hot partitions)
```promql
histogram_quantile(0.99, sum by (partition, le)(rate(kafka_consumer_dispatcher_message_delay_seconds_bucket[5m])))
```

### p99 processing time (processor function only)
```promql
histogram_quantile(0.99, sum by (le)(rate(kafka_consumer_dispatcher_processing_time_seconds_bucket[5m])))
```

### p99 processing time per partition
```promql
histogram_quantile(0.99, sum by (partition, le)(rate(kafka_consumer_dispatcher_processing_time_seconds_bucket[5m])))
```

### p99 record age (end-to-end production-to-completion)
```promql
histogram_quantile(0.99, sum by (le)(rate(kafka_consumer_dispatcher_record_age_seconds_bucket[5m])))
```

### p99 record age per partition (spot partitions falling behind)
```promql
histogram_quantile(0.99, sum by (partition, le)(rate(kafka_consumer_dispatcher_record_age_seconds_bucket[5m])))
```

### Consumer lag in time (how long messages sat in Kafka before being polled)
```promql
histogram_quantile(0.99, sum by (partition, le)(rate(kafka_consumer_dispatcher_record_age_seconds_bucket[5m])))
- histogram_quantile(0.99, sum by (partition, le)(rate(kafka_consumer_dispatcher_message_delay_seconds_bucket[5m])))
```

### Queueing overhead (delay minus processing = time spent waiting)
```promql
histogram_quantile(0.99, sum by (le)(rate(kafka_consumer_dispatcher_message_delay_seconds_bucket[5m])))
- histogram_quantile(0.99, sum by (le)(rate(kafka_consumer_dispatcher_processing_time_seconds_bucket[5m])))
```

### DLQ rate
```promql
sum(rate(kafka_consumer_dispatcher_dlq_messages_total[5m]))
```

### DLQ rate per partition (find problematic partitions)
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

### Inflight messages (backpressure signal)
```promql
sum(kafka_consumer_dispatcher_inflight_messages)
```

### Inflight messages per partition (spot the bottleneck)
```promql
kafka_consumer_dispatcher_inflight_messages
```

### Commit failure rate
```promql
rate(kafka_consumer_poll_loop_commit_failures_total[5m])
```

### Partitions assigned (detect rebalance storms)
```promql
kafka_consumer_poll_loop_partitions_assigned
```

### Rebalance rate (alert if > 2/hour)
```promql
rate(kafka_consumer_poll_loop_rebalances_total[1h]) * 3600
```

### Stuck partition detection (offset not advancing)
```promql
changes(kafka_consumer_poll_loop_last_committed_offset[5m]) == 0
and on(partition)
rate(kafka_consumer_poll_loop_messages_polled_total[5m]) > 0
```

### Partitions paused (flow control active)
```promql
kafka_consumer_poll_loop_partitions_paused
```

## Latency Metrics Breakdown

The three latency histograms decompose the full message lifecycle:

```
├── record_age_seconds ──────────────────────────────────────────────┤
│                                                                    │
│  producer → broker → partition wait │ poll → queue → process       │
│  (time in Kafka, external)          │ (message_delay_seconds)      │
│                                     │                              │
│                                     │ queue wait │ processor()     │
│                                     │            │ (processing_    │
│                                     │            │  time_seconds)  │
│                                     │            │                 │
produce                             polled      worker picks up    done
```

- **`record_age_seconds`** = total end-to-end (production → completion). SLO metric. Alert: "messages older than X seconds being processed."
- **`message_delay_seconds`** = internal pipeline (poll → completion). If this grows while `processing_time` is flat, the bottleneck is queueing/backpressure — add workers or increase channel capacity.
- **`processing_time_seconds`** = processor function only. If this grows, the target service is degrading — the circuit breaker should eventually trip.
- **`record_age - message_delay`** = time-domain consumer lag (how long messages sat in Kafka). Equivalent to offset lag but computed without admin API calls.

## Metrics Not Included (and why)

| Metric | Rationale |
|--------|-----------|
| Consumer lag (offset-based) | Requires broker log-end offset (high watermark) — belongs in external tooling like [Burrow](https://github.com/linkedin/Burrow), [kafka_exporter](https://github.com/danielqsj/kafka_exporter), or Confluent Control Center, which can monitor even when the consumer is down. Our `record_age_seconds` provides the time-domain equivalent without admin API calls. |
| Bytes consumed | Low value for a framework that operates on message semantics, not raw byte throughput. |
| Fetch rate / fetch latency | librdkafka internals — confluent-kafka-go exposes these via its stats callback (`statistics.interval.ms`) if operators need them. |
