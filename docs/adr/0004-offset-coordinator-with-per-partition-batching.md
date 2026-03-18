# ADR-0004: OffsetCoordinator with Per-Partition Batching

## Status

Accepted

## Context

### The Gap Problem

Kafka's offset model stores a single committed offset per partition per consumer group, meaning "all messages up to this offset have been processed." When multiple workers process messages from the same partition concurrently, they may complete out of order. Committing the highest completed offset would tell Kafka that all prior offsets are done — but some may still be in-flight. On crash, those in-flight messages would be skipped, violating at-least-once delivery (NFR-2.1).

Example: offsets `[1, 2, 3, 4, 5]` dispatched to workers. Worker A finishes offset 5 first. Committing offset 6 skips still-in-flight offsets 2 and 4. If the service crashes, those messages are lost.

### Industry Solutions

Two established approaches exist:

**1. Contiguous offset tracking (bitmap/high-watermark)**

Track individual offset completions and only commit the highest offset for which ALL prior offsets are also complete. Used by:
- **Confluent Parallel Consumer**: Uses BitSet + RunLength encoding stored in Kafka commit metadata. Allows multiple in-flight batches per partition. Complex implementation (~4KB metadata limit per partition).
- **Jaeger ingester** (Uber): Uses a `ConcurrentList` with periodic hashset scans to find the contiguous frontier. ~100 lines of Go. Production-proven.

**2. Per-partition batching (one in-flight batch per partition)**

The Dispatcher groups messages by partition before dispatching. Each batch contains contiguous offsets from a single partition. Only one batch per partition is in-flight at a time. On completion, commit the batch's max offset + 1. No gaps possible.

This is the pattern recommended in the [Kafka JavaDoc](https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html) as "Pattern 2": pause the partition while its batch is being processed, resume after commit.

### Separation of Concerns

Offset tracking should not live in the Dispatcher (which owns routing) or in the poll loop (which owns Kafka interaction) or in workers (which own processing logic). A dedicated component — the **OffsetCoordinator** — owns the coordination between "batch dispatched" and "safe to commit."

This name follows the precedent set by KafkaFlow's `OffsetCoordinator` component, which serves the same purpose.

### Commit Strategy

At high throughput, committing after every batch completion generates excessive broker load (one network round-trip per commit). The recommended approach is **periodic commits** with event-driven commits at critical moments:

- **Periodic timer** (steady state): Commit all committable offsets every N seconds (configurable, default 5s, matching Kafka's `auto.commit.interval.ms` default). Batches multiple completions into fewer broker calls.
- **Synchronous commit on shutdown** (SIGTERM/SIGINT): Commit all completed work before exiting.
- **Synchronous commit on partition revocation**: Commit completed work for revoked partitions before releasing them to another consumer.

The tradeoff: the reprocessing window on crash equals the timer interval. With at-least-once semantics (NFR-2.1) and idempotent processing (NFR-2.2), this is acceptable.

## Decision

### 1. Per-Partition Batching as the Initial Strategy

The Dispatcher groups messages by partition and dispatches per-partition batches. Only one batch per partition is in-flight at a time. This eliminates the gap problem entirely without requiring bitmap tracking.

The poll loop groups polled messages by `TopicPartition` before calling `Dispatcher.Send()`. The Dispatcher assembles these into batches (by size and/or linger time) and dispatches them to workers. When a worker completes a batch, the Dispatcher notifies the OffsetCoordinator.

### 2. OffsetCoordinator as a Dedicated Component

The OffsetCoordinator is an internal framework component. It coordinates the timing of offset commits by tracking which batches have been dispatched and which have completed. It is not exposed to the developer — it is plumbing between the poll loop and the Dispatcher.

#### Interface

```go
// OffsetCoordinator tracks dispatched and completed batches per partition
// and determines when offsets are safe to commit to Kafka.
//
// The OffsetCoordinator does not commit offsets itself — it reports
// what is committable. The poll loop is responsible for the actual
// Kafka CommitOffsets() call.
//
// Concurrency: all methods are safe for concurrent use. The Dispatcher
// calls BatchDispatched and BatchComplete from worker goroutines,
// while the poll loop calls Committable from the poll goroutine.
type OffsetCoordinator interface {
    // BatchDispatched records that a batch with the given max offset
    // has been dispatched to a worker for the specified partition.
    // Called by the Dispatcher when it sends a batch to a worker.
    BatchDispatched(partition int32, maxOffset int64)

    // BatchComplete records that the in-flight batch for the specified
    // partition has been fully processed (success, DLQ'd, or abandoned).
    // The maxOffset must match the dispatched offset for defensive
    // validation. Idempotent: completing an already-completed batch
    // is a no-op. Panics if maxOffset does not match.
    // Called by the Dispatcher when a worker reports completion.
    BatchComplete(partition int32, maxOffset int64)

    // Committable returns the next offset to fetch (maxOffset + 1) for
    // each partition with a completed batch. Returns the same offsets on
    // repeated calls until state changes (new dispatch, reset, or
    // completion). This ensures failed commits are retried automatically.
    // Called periodically by the poll loop on a timer.
    Committable() map[int32]int64

    // Reset clears all tracking state for a partition. Must be called
    // when a partition is revoked so stale state does not leak into
    // a future assignment of the same partition.
    // Called by the poll loop during rebalance handling.
    Reset(partition int32)
}
```

#### Component Interaction Flow

```
1. Poll loop fetches messages, groups by partition
2. Poll loop calls Dispatcher.Send(partition, messages)
3. Dispatcher assembles batch (size/linger), dispatches to worker
4. Dispatcher calls OffsetCoordinator.BatchDispatched(partition, maxOffset)
5. Worker processes batch, returns success/failure to Dispatcher
6. On completion: Dispatcher calls OffsetCoordinator.BatchComplete(partition, maxOffset)
7. On timer tick: Poll loop calls OffsetCoordinator.Committable()
8. Poll loop commits returned offsets to Kafka
```

#### Who Calls What

| Caller | Method | When |
|--------|--------|------|
| Dispatcher | `BatchDispatched()` | After dispatching a batch to a worker |
| Dispatcher | `BatchComplete()` | After a worker reports success |
| Poll loop | `Committable()` | On periodic timer, on shutdown, on revocation |
| Poll loop | `Reset()` | On partition revocation |

Workers never interact with the OffsetCoordinator.

### 3. Hybrid Commit Strategy

- **Periodic**: Poll loop calls `Committable()` every N seconds (configurable) and commits the result.
- **On shutdown**: Poll loop calls `Committable()` synchronously and commits before closing.
- **On revocation**: Poll loop calls `Committable()` for revoked partitions, commits, then calls `Reset()`.

### 4. Future Evolution

The OffsetCoordinator interface supports a future upgrade to the contiguous offset tracker (bitmap/high-watermark) without changing any other component. This would allow multiple in-flight batches per partition for higher concurrency, at the cost of more complex offset tracking internally.

## Consequences

### What becomes easier

- **Offset safety is guaranteed by design.** Per-partition batching with one in-flight batch eliminates the gap problem. No bitmap, no sorted set, no scanning.
- **Clean separation of concerns.** The OffsetCoordinator is the single place that knows about offset safety. The poll loop just commits what it's told. The Dispatcher just reports events. Workers are Kafka-unaware.
- **Commit frequency is decoupled from processing speed.** The periodic timer means broker load is constant regardless of throughput.

### What becomes harder

- **Per-partition concurrency is limited.** With one in-flight batch per partition, parallelism equals the number of assigned partitions. For most workloads this is sufficient — increase partitions for more parallelism. The OffsetCoordinator interface allows upgrading to bitmap tracking later if needed.
- **Batch failures retry the entire batch.** Individual message failures within a batch cannot be separated. This is acceptable with at-least-once semantics.

### What changes in other components

- **Dispatcher (ADR-0003)**: No longer owns offset tracking via `Ack()`. Instead, it calls `OffsetCoordinator.BatchDispatched()` and `BatchComplete()`. The `Ack()` method is removed from the Dispatcher interface. Batch assembly now produces per-partition batches.
- **Poll loop**: Adds a periodic commit timer that calls `OffsetCoordinator.Committable()`. Groups polled messages by partition before sending to Dispatcher.
- **Workers**: No change. Workers remain Kafka-unaware.

### References

- [Confluent Parallel Consumer - Offset Encoding](https://github.com/confluentinc/parallel-consumer)
- [Jaeger Ingester - Offset Package](https://pkg.go.dev/github.com/jaegertracing/jaeger/cmd/ingester/app/consumer/offset)
- [Kafka JavaDoc - KafkaConsumer (Pattern 2)](https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
- [Confluent - Guide to Consumer Offsets](https://www.confluent.io/blog/guide-to-consumer-offsets/)
