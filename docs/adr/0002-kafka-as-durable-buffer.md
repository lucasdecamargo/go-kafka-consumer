# ADR-0002: Use Kafka as the Durable Buffer Instead of a Local Disk-Backed Store

## Status

Accepted

## Context

The original architecture placed a local disk-backed buffer between Kafka consumption and message processing:

```
Kafka → [Producer] → [Disk-Backed Buffer] → [Consumer] → Target Service
```

The rationale was to protect against crashes and decouple ingestion speed from processing speed. However, this design raises several concerns:

1. **Redundant durability.** Kafka is itself a durable, disk-backed, replayable log. Messages remain available in the topic for the configured retention period. If the consumer does not commit an offset, Kafka will redeliver the message on restart. Adding a second durable store replicates guarantees Kafka already provides.

2. **Dual source of truth.** With a disk buffer, the system must reconcile two independent position trackers: Kafka consumer offsets and the buffer's internal cursor. This creates complex failure modes — e.g., a crash after writing to the buffer but before committing to Kafka causes duplicates, while a crash after committing but before flushing the buffer causes message loss.

3. **Implementation burden.** A correct disk-backed queue with crash recovery, concurrent access, backpressure, and garbage collection is a non-trivial component. It is effectively re-implementing a message broker inside the consumer.

4. **Industry precedent.** Confluent, Uber (uForwarder), and LinkedIn all use Kafka's native offset management as the durability mechanism for consumers. None advocate for a consumer-side disk buffer when the upstream source is Kafka.

The alternative is to commit Kafka offsets **only after successful processing** and use the Kafka consumer `Pause()`/`Resume()` API for backpressure and flow control.

### How Pause/Resume Works

- `Pause(partitions)` tells the Kafka client to stop fetching records from specific partitions. Calls to `Poll()` continue to maintain the consumer group session (heartbeats) but return no records from paused partitions.
- `Resume(partitions)` re-enables fetching.
- This prevents `max.poll.interval.ms` violations because `Poll()` is never blocked by slow processing.
- Combined with a bounded in-memory channel (Go channel), this provides natural backpressure: when the channel is full, pause; when workers drain it, resume.

## Decision

We adopt the following architecture:

```
Kafka Topic
    |
    v
[Poll Loop] --pause/resume--> [Dispatcher] ---> [Workers (internal)] ---> Target Service
    |                          (bounded channel)         |
    +--- commit offset <--- only after successful processing via OffsetCoordinator ---+
```

> **Note:** The Dispatcher encapsulates the bounded in-memory channel and worker pool internally. See [ADR-0003](0003-single-poll-loop-with-dispatcher.md) for the full Dispatcher design.

Specifically:

1. **Kafka is the sole durable buffer.** No local disk-backed store. Messages are retained in Kafka until the configured retention period expires.
2. **Manual offset commits.** `enable.auto.commit` is disabled. Offsets are committed only after a batch has been successfully written to the target service.
3. **Pause/Resume for backpressure.** When the Dispatcher's bounded internal channel reaches capacity, the poll loop pauses the relevant partitions. When the Dispatcher signals capacity is available (via the `Ready()` channel), it resumes them.
4. **Bounded in-memory channel.** A Go channel with a configurable capacity, internal to the Dispatcher, acts as the handoff between the poll loop and worker goroutines. This bounds memory usage without requiring disk I/O.
5. **Cooperative sticky partition assignment.** Use `CooperativeStickyAssignor` to minimize rebalance disruption.
6. **DLQ via Kafka topic.** Dead-lettered messages are produced to a separate Kafka topic, not stored locally.

## Consequences

### What becomes easier

- **Simpler architecture.** One durable store (Kafka), one position tracker (offsets), one recovery mechanism (re-read from last committed offset).
- **Fewer failure modes.** No buffer corruption, no buffer/offset synchronization issues, no disk space management for the buffer.
- **Lower resource usage.** No consumer-side disk I/O for buffering. The in-memory channel is lightweight.
- **Proven pattern.** Aligns with Confluent's recommended consumer design and production patterns at Uber and LinkedIn.

### What becomes harder or must be accepted

- **Reprocessing on crash.** If the service crashes after processing but before committing, those messages will be reprocessed on restart. This is inherent to at-least-once semantics and must be handled via idempotent processing or idempotent target services.
- **Memory-bounded throughput.** The in-memory channel limits how many messages can be in-flight. This is intentional (backpressure), but means burst absorption is bounded by channel capacity rather than disk space. In practice, Kafka's own retention absorbs the burst.
- **Careful rebalance handling.** On partition revocation, in-flight messages for that partition must be drained or discarded, and offsets for completed work must be committed before the partition moves to another consumer.

### References

- [Confluent - Consumer Configuration and Offset Management](https://docs.confluent.io/platform/current/clients/consumer.html)
- [Confluent - Kafka Scaling Best Practices](https://www.confluent.io/learn/kafka-scaling-best-practices/)
- [Parallel Back-pressured Kafka Consumer Pattern](https://tuleism.github.io/blog/2021/parallel-backpressured-kafka-consumer/)
- [Uber - Introducing uForwarder](https://www.uber.com/blog/introducing-ufowarder/)
- [Apache Kafka Design - Persistence](https://kafka.apache.org/documentation/#design_persistence)
