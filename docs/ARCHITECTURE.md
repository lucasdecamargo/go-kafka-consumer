# Architecture Guide

## Project Overview

**go-kafka-consumer** is a production-grade Go framework for high-throughput Kafka message consumption. It implements a pause/resume flow-control pattern with bounded in-memory buffering and a pluggable dispatcher for message routing.

**Repository**: `github.com/lucasdecamargo/go-kafka-consumer`

### What the Developer Implements

The developer implements a single function:

```go
type BatchProcessor func(ctx context.Context, batch []Message) error
```

The framework handles everything else: Kafka interaction, batching, offset management, backpressure, retries, and DLQ routing.

---

## Core Architecture

```
Kafka Topic
    |
    v
[Poll Loop] ──Send()──► [Dispatcher] ──► [Workers (internal)]──► Target Service
    |                        |                    |
    |                   assembles batches     processes batch
    |                   manages workers       returns success/failure
    |                        |                    |
    |                        ├── BatchDispatched ──► [OffsetCoordinator]
    |                        └── BatchComplete  ──►        |
    |                                                      |
    +--- Committable() ◄──────────────────────────────────-+
    |
    +--- CommitOffsets() to Kafka (periodic + on shutdown/revocation)
```

### Components

#### 1. Poll Loop

A single goroutine that owns the Kafka consumer. This is a hard constraint — Kafka consumer clients (confluent-kafka-go, segmentio/kafka-go, sarama) are **not thread-safe for polling**.

**Responsibilities:**
- Calls `Poll()` to fetch messages from Kafka
- Groups polled messages by partition
- Feeds per-partition message groups to the Dispatcher via `Send()`
- Uses `Pause()`/`Resume()` on partitions for backpressure
- Continues calling `Poll()` even when paused (maintains consumer group session, avoids `max.poll.interval.ms` violations)
- Periodically commits offsets via the OffsetCoordinator
- Handles consumer group rebalancing (assignment/revocation)
- Uses `CooperativeStickyAssignor` to minimize rebalance disruption

**Does NOT own:** message routing, worker lifecycle, batch assembly.

#### 2. Dispatcher

A Go interface with pluggable implementations for different ordering modes. Sits between the poll loop and workers.

**Responsibilities:**
- Accepts per-partition message groups from the poll loop via `Send()`
- Assembles messages into per-partition batches (by configurable size and/or linger time)
- Manages internal channel topology and worker goroutine lifecycle
- Signals backpressure to the poll loop when at capacity (`Send()` returns error, `Ready()` channel signals recovery)
- Notifies the OffsetCoordinator on batch dispatch and completion
- Handles partition assignment/revocation notifications (drains in-flight work on revocation)
- Owns the circuit breaker for target service health (wraps `BatchProcessor` attempts, signals state transitions to the poll loop)
- Classifies errors: non-retryable (`*ErrNonRetryable`) → DLQ immediately; transient (default) → retry + circuit breaker

**Does NOT own:** Kafka interaction, offset commits, offset tracking.

**Interface:**

```go
type CircuitState int
const (
    CircuitClosed  CircuitState = iota
    CircuitOpen
    CircuitHalfOpen
)

type Dispatcher interface {
    Send(ctx context.Context, partition int32, msgs []Message) error
    Ready() <-chan struct{}                  // signaled when capacity is available after backpressure
    CircuitStateChanged() <-chan CircuitState // emits new state on circuit breaker transitions
    OnPartitionsAssigned(partitions []Partition)
    OnPartitionsRevoked(partitions []Partition)
    Close(ctx context.Context) error         // context enforces shutdown deadline
}
```

**Ordering Modes (separate implementations):**

| Mode | Channel Topology | Worker Model | Ordering Guarantee |
|------|-----------------|--------------|-------------------|
| **Unordered** (default) | Single bounded channel | Static pool of N workers | None — max throughput |
| **Partition-ordered** (future) | One channel per partition | One worker per partition, dynamic | Per-partition ordering preserved |
| **Key-ordered** (future) | N channels, hash-routed | One worker per channel | Per-key ordering preserved |

#### 3. Workers

Goroutines managed internally by the Dispatcher. Execute the developer-provided `BatchProcessor` function.

**Responsibilities:**
- Process each batch atomically (entire batch succeeds or fails)
- Write results to the target service
- Return success/failure to the Dispatcher

**Does NOT own:** channel management, Kafka awareness, offset awareness. Workers have zero knowledge of Kafka.

#### 4. OffsetCoordinator

A dedicated internal component that tracks dispatched and completed batches per partition. Named after KafkaFlow's equivalent component.

**Responsibilities:**
- Tracks which batches have been dispatched and which have completed, per partition
- Reports which offsets are safe to commit
- Supports resetting state for revoked partitions
- Thread-safe: Dispatcher calls from worker goroutines, poll loop calls from poll goroutine

**Does NOT own:** Kafka interaction, committing offsets, message routing. Not exposed to the developer.

**Interface:**

```go
type OffsetCoordinator interface {
    // Called by Dispatcher when dispatching a batch to a worker.
    BatchDispatched(partition int32, maxOffset int64)

    // Called by Dispatcher when a worker reports completion.
    // maxOffset must match dispatched offset (defensive validation).
    // Idempotent: completing an already-completed batch is a no-op.
    BatchComplete(partition int32, maxOffset int64)

    // Called by poll loop on timer, shutdown, or revocation.
    // Returns maxOffset+1 for partitions with completed batches.
    // Stable: returns same offsets until state changes.
    Committable() map[int32]int64

    // Called by poll loop on partition revocation.
    Reset(partition int32)
}
```

**Who calls what:**

| Caller | Method | When |
|--------|--------|------|
| Dispatcher | `BatchDispatched()` | After dispatching a batch to a worker |
| Dispatcher | `BatchComplete()` | After a worker reports completion (success, DLQ, or abandoned) |
| Poll loop | `Committable()` | On periodic timer, on shutdown, on revocation |
| Poll loop | `Reset()` | On partition revocation |

Workers never interact with the OffsetCoordinator.

---

## Key Design Decisions

### Kafka as Sole Durable Buffer (ADR-0002)

No local disk-backed store. Kafka IS the durable buffer — messages remain available for the configured retention period. If the consumer doesn't commit an offset, Kafka redelivers on restart.

**Backpressure:** When the Dispatcher's bounded in-memory channel is full, the poll loop pauses Kafka partitions. When workers drain space, it resumes them. This replaces any need for a disk-backed buffer.

**Why not a disk buffer?** It would be redundant durability, create a dual source-of-truth problem (Kafka offsets vs buffer cursor), and add significant implementation complexity. Industry leaders (Confluent, Uber, LinkedIn) all use Kafka's native offset management without a consumer-side disk buffer.

### Single Poll Loop with Pluggable Dispatcher (ADR-0003)

One goroutine owns the Kafka consumer per process. This is not a design choice but a hard constraint from Kafka client libraries (not thread-safe for polling).

**Horizontal scaling:** Multiple pods in the same consumer group — Kafka distributes partitions.
**Vertical scaling:** Worker count and channel capacity within each pod.

The Dispatcher interface decouples message routing from both the poll loop and workers. Adding a new ordering mode requires only a new Dispatcher implementation.

### Per-Partition Batching with OffsetCoordinator (ADR-0004)

**The gap problem:** When multiple workers process messages from the same partition concurrently and complete out of order, committing the highest offset would skip in-flight messages. On crash, those messages are lost.

**Solution:** One in-flight batch per partition. The Dispatcher groups messages by partition and dispatches per-partition batches. Since only one batch per partition is ever in-flight, there are no gaps to track. No bitmap, no sorted set, no scanning needed.

**Trade-off:** Per-partition concurrency is limited to the number of assigned partitions. For higher concurrency, increase topic partitions. The interface supports future upgrade to contiguous offset tracking (bitmap/high-watermark) for multiple in-flight batches per partition.

### Hybrid Offset Commit Strategy (ADR-0004)

- **Periodic timer (steady state):** Commit all committable offsets every N seconds (default 5s). Batches multiple completions into fewer broker calls — critical for high throughput where per-batch commits would overwhelm the broker.
- **Synchronous on shutdown (SIGTERM/SIGINT):** Commit all completed work before exiting.
- **Synchronous on partition revocation:** Commit completed work for revoked partitions, then reset OffsetCoordinator state.

**Trade-off:** Reprocessing window on crash equals the commit interval. Acceptable with at-least-once semantics and idempotent processing.

---

## Message Flow (Happy Path)

```
1. Poll loop fetches messages from Kafka via Poll()
2. Poll loop groups messages by partition
3. Poll loop calls Dispatcher.Send(partition, messages)
4. Dispatcher assembles batch (by size and/or linger time)
5. Dispatcher dispatches batch to a worker
6. Dispatcher calls OffsetCoordinator.BatchDispatched(partition, maxOffset)
7. Worker executes BatchProcessor(ctx, batch)
8. Worker returns success to Dispatcher
9. Dispatcher calls OffsetCoordinator.BatchComplete(partition, maxOffset)
10. On timer tick: Poll loop calls OffsetCoordinator.Committable()
11. Poll loop commits returned offsets to Kafka via CommitOffsets()
```

---

## Delivery Guarantees

- **At-least-once delivery** end-to-end. Offsets are committed only after successful processing.
- **Idempotent processing required.** On crash recovery, messages between the last committed offset and the crash point will be reprocessed. The reprocessing window is bounded by the commit interval (default 5s).
- **`enable.auto.commit` is disabled.** All offset commits are manual.
- **Batch atomicity.** The entire batch succeeds or fails as a unit. Failed batches are retried with exponential backoff and jitter, then routed to the DLQ after max retries.

---

## Error Handling & DLQ

### Error Classification

Errors returned by `BatchProcessor` are classified before action:

| Returned Error | Classification | Action |
|---|---|---|
| `*ErrNonRetryable` | Non-retryable | DLQ immediately. No retries. Not reported to circuit breaker. |
| Any other `error` | Transient (default) | Retry with backoff. Each attempt reported to circuit breaker. |
| `nil` | Success | Reported to circuit breaker as success. |

The framework provides a `ErrNonRetryable` type. The default is transient (safe — unknown errors are retried).

### Circuit Breaker (ADR-0007)

The Dispatcher owns a circuit breaker (`sony/gobreaker`) that monitors transient error rates across all workers using a failure rate threshold over a sliding window.

| State | Behavior | Effect on Dispatch |
|---|---|---|
| **Closed** | Normal operation. All attempts reported. | Active |
| **Open** | Downstream is broken. No batches dispatched. Degraded mode. | Paused |
| **Half-Open** | Recovery probing. Limited batches dispatched. | Limited (`max_requests`) |

**Key rule:** A batch is only sent to the DLQ when retries are exhausted **AND** the circuit is Closed. If the circuit is Open, the batch is held — not DLQ'd.

### DLQ

- Non-retryable errors: DLQ'd immediately, no retries.
- Transient retries exhausted + circuit Closed: DLQ'd (per-batch problem, not system-wide).
- Transient retries exhausted + circuit Open: held for reprocessing (downstream is down, message is fine).
- DLQ messages preserve: original content, source topic, partition, offset, error reason, error classification, timestamp.
- After DLQ production, the offset is committed so the consumer advances.

### Unified Degraded Mode

Both broker unavailability (ADR-0006) and target unavailability (ADR-0007) trigger the same degraded mode in the poll loop, tracked via a bitmask:

```go
type DegradedReason uint8
const (
    BrokerUnavailable  DegradedReason = 1 << 0  // commit failures
    TargetUnavailable  DegradedReason = 1 << 1  // circuit breaker open
)
```

Degraded mode: pause all partitions, stop dispatch, continue `Poll()`, readiness = unhealthy, liveness = healthy. Exits only when **all reasons are cleared**.

---

## Cross-Cutting Concerns (ADR-0008)

### Constructor Pattern

All components use the **functional options pattern**:

```go
dispatcher, err := NewDispatcher(cfg,
    WithLogger(logger),
    WithMetrics(reg),
)
```

- **Required dependencies** (config, OffsetCoordinator) are positional parameters.
- **Optional dependencies** (logger, metrics) are functional options with sensible defaults.
- Adding new options never breaks existing call sites.

### Logging

- **Library:** Go's standard `slog` package (`*slog.Logger`).
- **Injection:** Via `WithLogger(logger)` functional option. Default: `slog.Default()`.
- **Format:** JSON structured logging with contextual fields (component, partition, offset range).

### Metrics

- **Library:** `prometheus/client_golang` with `promauto.With(reg)`.
- **Injection:** Components receive a `prometheus.Registerer` via `WithMetrics(reg)`. Metrics are created internally by the component — callers don't need to know what's measured.
- **Naming:** Constants in dedicated `metrics.go` files. Convention: `kafka_consumer_<component>_<metric>` with standard suffixes (`_total`, `_seconds`).
- **Default:** `prometheus.DefaultRegisterer` if no option is provided.

### Configuration

- **Per-component config structs** (e.g., `DispatcherConfig`, `PollLoopConfig`).
- **Required constructor parameters** — not optional.
- **Validated at startup** — fail fast on invalid values.
- **`main()` owns wiring** — creates registry, logger, config structs, and assembles components.

---

## Scaling Model

| Dimension | Mechanism |
|-----------|-----------|
| **Horizontal** | Multiple pods in the same consumer group. Kafka distributes partitions across them. |
| **Vertical** | Worker count, dispatcher channel capacity, poll interval, commit interval, batch size/linger. |
| **Partition concurrency** | One in-flight batch per partition (initial). Upgradeable to multiple via contiguous offset tracking. |

---

## Configuration

All tunable parameters are configurable via environment variables and/or config file, with sensible defaults. Validated at startup (fail-fast on invalid config).

Key parameters:
- Dispatch mode (unordered / partition-ordered / key-ordered)
- Worker count
- Channel capacity (bounds memory)
- Batch size and linger time
- Poll interval
- Commit interval (default 5s)
- Shutdown timeout (default 25s, must be < K8s `terminationGracePeriodSeconds`)
- Commit failure threshold (default 3, triggers degraded mode on broker unavailability)
- Circuit breaker: min requests (3), failure threshold (0.6), open timeout (30s), max probe requests (1), sliding window (60s)
- Max retries
- DLQ topic name
- Kafka connection settings (TLS, SASL)

---

## Observability

- **Metrics:** consumer lag, throughput (msg/sec), batch processing latency, dispatcher queue depth, worker utilization, offset commit rate, error rates (transient/non-retryable), DLQ production rate, circuit breaker state transitions, degraded mode duration.
- **Structured logging:** JSON format with correlation IDs.
- **Health checks:** liveness and readiness endpoints for orchestrator integration.

---

## Execution Scenarios

Detailed execution path documentation lives in `docs/scenarios/`. Each scenario includes trigger, preconditions, step-by-step execution sequence, state changes, outcome, and a Mermaid sequence diagram.

### Steady State
| # | Scenario | Key Requirements |
|---|----------|-----------------|
| [01](scenarios/01-happy-path.md) | Happy Path | FR-1, FR-2, FR-3, FR-5, FR-6 |
| [02](scenarios/02-backpressure.md) | Backpressure (Pause/Resume) | FR-1.5, FR-2.5, NFR-1 |
| [03](scenarios/03-periodic-offset-commit.md) | Periodic Offset Commit | FR-5.2, FR-6.1, FR-6.2 |

### Failure & Recovery
| # | Scenario | Key Requirements |
|---|----------|-----------------|
| [04](scenarios/04-batch-failure-retry.md) | Batch Processing Failure with Retry (Transient) | FR-3.4, FR-7.2, NFR-6.2 |
| [05](scenarios/05-retries-exhausted-dlq.md) | Retries Exhausted / Non-retryable Error → DLQ | FR-4.1, FR-7.1, NFR-6.4, NFR-6.5 |
| [06](scenarios/06-crash-recovery.md) | Crash Recovery | NFR-2.1, NFR-2.4 |

### Lifecycle Events
| # | Scenario | Key Requirements |
|---|----------|-----------------|
| [07](scenarios/07-graceful-shutdown.md) | Graceful Shutdown | FR-6.3, NFR-2.3, NFR-5.4 |
| [08](scenarios/08-rebalance-revocation.md) | Rebalance — Partition Revocation | FR-1.4, FR-2.7, FR-6.4 |
| [09](scenarios/09-rebalance-assignment.md) | Rebalance — Partition Assignment | FR-1.4, FR-2.7 |

### Edge Cases
| # | Scenario | Key Requirements |
|---|----------|-----------------|
| [10](scenarios/10-session-keepalive.md) | Session Keepalive Under Load | FR-1.6, NFR-1.3 |
| [11](scenarios/11-worker-panic.md) | Worker Panic | FR-3.6, NFR-2.3 |

### Infrastructure Failures
| # | Scenario | Key Requirements |
|---|----------|-----------------|
| [12](scenarios/12-broker-unavailable.md) | Kafka Broker Unavailable | FR-1.9, FR-1.10, NFR-3.3 |
| [13](scenarios/13-target-unavailable.md) | Target Service Unavailable (Circuit Breaker) | FR-1.11, FR-2.10, NFR-5.6, NFR-6.5 |

---

## Testing Strategy

- **Unit tests:** All core components (poll loop, dispatcher implementations, offset coordinator, batch processor integration).
- **Integration tests:** Real Kafka broker via testcontainers-go.
- **Benchmark tests:** Throughput validation under load.

---

## Architectural Decision Records

| ADR | Title | Status |
|-----|-------|--------|
| [0001](adr/0001-template.md) | ADR Template | Accepted |
| [0002](adr/0002-kafka-as-durable-buffer.md) | Kafka as Durable Buffer (no disk-backed store) | Accepted |
| [0003](adr/0003-single-poll-loop-with-dispatcher.md) | Single Poll Loop with Pluggable Dispatcher | Accepted |
| [0004](adr/0004-offset-coordinator-with-per-partition-batching.md) | OffsetCoordinator with Per-Partition Batching | Accepted |
| [0005](adr/0005-panic-recovery-with-graceful-shutdown.md) | Recover Worker Panics with Graceful Shutdown | Accepted |
| [0006](adr/0006-broker-unavailability-handling.md) | Broker Unavailability with Dispatch Pause and Readiness Degradation | Accepted |
| [0007](adr/0007-circuit-breaker-for-target-unavailability.md) | Circuit Breaker for Target Service Unavailability with Error Classification | Accepted |
| [0008](adr/0008-cross-cutting-concerns.md) | Cross-Cutting Concerns: Logging, Metrics, Configuration, Constructor Pattern | Accepted |

---

## Implementation Status

All requirements are in **Planned** status. Initial implementation will focus on:
1. UnorderedDispatcher (the default ordering mode)
2. Per-partition batching with one in-flight batch (simplest correct offset strategy)

Future implementations: PartitionDispatcher, KeyDispatcher, contiguous offset tracking.
