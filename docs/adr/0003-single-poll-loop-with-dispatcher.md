# ADR-0003: Single Poll Loop with Pluggable Dispatcher for Concurrency

## Status

Accepted

## Context

We need to define the concurrency model for the service: how messages flow from Kafka to the workers that process them, how many goroutines are involved, and how ordering guarantees are managed.

### Kafka Consumer Thread Safety

Kafka consumer clients are **not thread-safe for polling**. This is a hard constraint across all Go libraries:

- **confluent-kafka-go**: Wraps librdkafka via cgo. The maintainers explicitly confirmed that `Poll()`/`ReadMessage()` must be called from a single goroutine ([confluent-kafka-go#67](https://github.com/confluentinc/confluent-kafka-go/issues/67)).
- **segmentio/kafka-go**: The `Reader` with consumer groups should have `ReadMessage()` called from a single goroutine to avoid unpredictable offset behavior.
- **IBM/sarama**: Internally spawns one goroutine per partition via `ConsumeClaim()`, but the consumer group coordination is single-threaded.

This means a single process must have exactly **one goroutine owning the Kafka poll loop**. Parallelism at the poll level is achieved by running multiple instances (pods) in the same consumer group — Kafka distributes partitions across them.

### Processing Parallelism

While polling is single-goroutine, processing can be parallelized with a worker pool. The question is how the poll loop dispatches messages to workers, which determines ordering guarantees.

Confluent identifies two main models in their [Multi-Threaded Messaging](https://www.confluent.io/blog/kafka-consumer-multi-threaded-messaging/) guide:

1. **Multiple consumer instances** — one poll loop each, Kafka assigns partitions. Simple but capped at partition count.
2. **Single consumer + worker pool** — one poll loop dispatches to N workers. Can exceed partition count but complicates offset management and ordering.

For a template project, we want to support multiple ordering modes without coupling the poll loop or workers to a specific strategy.

### Ordering Modes

Different use cases require different ordering guarantees:

| Mode | Behavior | Use Case |
|------|----------|----------|
| **Unordered** | Any worker picks any message from a shared channel | Maximum throughput; idempotent operations, metrics, logs |
| **Partition-ordered** | Messages from the same partition are processed serially | Standard Kafka ordering guarantee; state machines, sequential workflows |
| **Key-ordered** | Messages with the same key are routed to the same worker | Per-entity ordering across partitions; user event streams |

### The Dispatch Problem

The routing logic between poll loop and workers varies by mode:

- **Unordered**: single shared channel, N workers race to consume.
- **Partition-ordered**: one channel per assigned partition, one worker per channel. Channels are created/destroyed dynamically as partitions are assigned/revoked.
- **Key-ordered**: N channels, message key is hashed to select the channel, one worker per channel.

This routing logic — channel topology, worker lifecycle, and backpressure signaling — needs to live somewhere. If it lives in the poll loop, the poll loop becomes coupled to every ordering mode. If it lives in the workers, the workers need to know about partitions.

## Decision

We introduce a **Dispatcher** component that sits between the poll loop and workers. The Dispatcher is defined as a Go interface, with one implementation per ordering mode.

### Architecture

```
                        ┌──────────────────────────────────────────┐
                        │              Dispatcher (interface)       │
                        │                                          │
Poll Loop ──Send()──►   │  groups by partition, assembles batches   │ ──► Workers (internal)
                        │  manages worker lifecycle                 │        |
                        │  signals backpressure to poll loop        │   success/failure
                        │  notifies OffsetCoordinator on completion │        |
                        │                                           │ ◄──────┘
                        └──────────────────────────────────────────┘
                                         |
                                         v
                                  OffsetCoordinator
                                  (BatchDispatched / BatchComplete)
```

Workers are **internal** to the Dispatcher — the Dispatcher spawns, manages, and collects results from them. The poll loop does not interact with workers directly.

### Interface Contract

```go
// Dispatcher routes messages from the poll loop to internal workers
// and coordinates with the OffsetCoordinator for offset safety.
//
// The Dispatcher owns worker lifecycle. Workers are not exposed
// outside the Dispatcher — the developer provides a processing
// function at construction time, and the Dispatcher calls it
// from worker goroutines.
type Dispatcher interface {
    // Send accepts a batch of messages for a single partition.
    // The Dispatcher buffers them internally, assembles batches
    // (by size and/or linger time), and dispatches to workers.
    // Returns an error if at capacity (backpressure signal to poll loop).
    Send(ctx context.Context, partition int32, msgs []Message) error

    // Ready returns a channel that is signaled when the Dispatcher
    // has capacity to accept new messages after being at capacity.
    // The poll loop selects on this channel to know when to resume
    // paused partitions. The channel is signaled each time a worker
    // completes a batch and frees a slot.
    Ready() <-chan struct{}

    // CircuitStateChanged returns a channel that emits the new CircuitState
    // whenever the internal circuit breaker transitions between states
    // (Closed, Open, HalfOpen). The poll loop selects on this channel to
    // enter/exit degraded mode when the target service is unavailable.
    // See ADR-0007.
    CircuitStateChanged() <-chan CircuitState

    // OnPartitionsAssigned notifies the dispatcher of new partition assignments.
    // The dispatcher may create partition-specific internal state (channels, workers).
    OnPartitionsAssigned(partitions []Partition)

    // OnPartitionsRevoked notifies the dispatcher that partitions are being revoked.
    // The dispatcher MUST drain in-flight work for these partitions before returning.
    OnPartitionsRevoked(partitions []Partition)

    // Close shuts down the dispatcher: stops accepting new messages,
    // waits for in-flight workers to complete, and releases resources.
    // The provided context enforces a shutdown deadline — if it expires
    // before all workers finish, Close returns immediately with an error
    // and the remaining in-flight batches are abandoned (their offsets
    // will not be committed; Kafka redelivers them on restart).
    Close(ctx context.Context) error
}
```

Note: compared to the original design, `Receive()` and `Ack()` have been removed. Workers are internal to the Dispatcher, so `Receive()` is an internal detail. Offset tracking has moved to the OffsetCoordinator (see [ADR-0004](0004-offset-coordinator-with-per-partition-batching.md)), so `Ack()` is replaced by the Dispatcher calling `OffsetCoordinator.BatchComplete()`.

The `Ready()` channel enables event-driven backpressure recovery: when the poll loop pauses partitions because `Send()` returned a capacity error, it `select`s on the `Ready()` channel to know when to call `Resume()`. This avoids polling the Dispatcher's state or causing unnecessary pause/resume churn on the Kafka consumer.

### Worker Processing Function

The developer provides a processing function at Dispatcher construction time:

```go
type BatchProcessor func(ctx context.Context, batch []Message) error
```

The Dispatcher calls this function from worker goroutines. The developer implements only this function — all framework plumbing (batching, offset tracking, retries, DLQ) is handled by the Dispatcher and OffsetCoordinator.

### Implementations

1. **UnorderedDispatcher** (default): Single bounded channel, static worker pool. Per-partition batches are dispatched to any available worker. Maximum throughput, no ordering guarantees.
2. **PartitionDispatcher** (future): Per-partition channel and worker, created/destroyed on rebalance. Preserves Kafka partition ordering.
3. **KeyDispatcher** (future): Consistent-hash routing by message key to N worker channels. Preserves per-key ordering.

### Component Responsibilities

| Component | Owns | Does NOT own |
|-----------|------|-------------|
| **Poll Loop** | Kafka consumer, calling `Poll()`, grouping messages by partition, pause/resume, periodic offset commits via OffsetCoordinator | Message routing, worker lifecycle, batch assembly |
| **Dispatcher** | Channel topology, message routing, worker lifecycle, backpressure signaling, batch assembly, notifying OffsetCoordinator, circuit breaker for target service health | Kafka interaction, offset commits, offset tracking |
| **OffsetCoordinator** | Tracking dispatched/completed batches, determining committable offsets | Kafka interaction, committing offsets, message routing |
| **Workers** | Processing logic (via `BatchProcessor`), writing to target service | Channel management, Kafka awareness, offset awareness |

### Concurrency Summary

- **1 poll loop goroutine** per process (hard constraint from Kafka client libraries).
- **N worker goroutines** managed by the Dispatcher (configurable, mode-dependent).
- **Horizontal scaling** via multiple pods in the same consumer group (Kafka distributes partitions).
- **Vertical scaling** via worker count and channel capacity within each pod.

## Consequences

### What becomes easier

- **Adding new ordering modes** requires only a new Dispatcher implementation — no changes to the poll loop or worker logic.
- **Developer experience is simple.** The developer only implements `BatchProcessor` — a single function that receives a batch and returns an error. All framework plumbing is internal.
- **Testing** is cleaner: each Dispatcher implementation can be unit-tested with a mock `BatchProcessor`. The poll loop can be tested with a mock Dispatcher.
- **Configuration** is straightforward: a single `dispatch_mode` setting selects the implementation at startup.

### What becomes harder

- **Interface design must be stable.** The Dispatcher interface is a central contract. Changing it requires updating all implementations.
- **Partition-aware dispatchers** (partition-ordered, key-ordered) must handle dynamic partition assignment/revocation, which adds complexity to those implementations.

### What we defer

- **PartitionDispatcher and KeyDispatcher** are defined as future implementations. The initial implementation will be UnorderedDispatcher only.

### References

- [Confluent - Multi-Threaded Messaging with Kafka Consumer](https://www.confluent.io/blog/kafka-consumer-multi-threaded-messaging/)
- [Confluent - Parallel Consumer Library](https://github.com/confluentinc/parallel-consumer)
- [confluent-kafka-go#67 - Consumer Thread Safety](https://github.com/confluentinc/confluent-kafka-go/issues/67)
- [Parallel Back-pressured Kafka Consumer](https://tuleism.github.io/blog/2021/parallel-backpressured-kafka-consumer/)
- [ADR-0004 - OffsetCoordinator with Per-Partition Batching](0004-offset-coordinator-with-per-partition-batching.md)
- [ADR-0007 - Circuit Breaker for Target Service Unavailability](0007-circuit-breaker-for-target-unavailability.md)
