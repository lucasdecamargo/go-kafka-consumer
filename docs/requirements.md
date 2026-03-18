# Requirements

## Overview

This project is a template for a Go service that implements a high-throughput Kafka consumer using a pause/resume flow-control pattern with a pluggable dispatcher for message routing and an offset coordinator for safe commit tracking.

### Architecture Summary

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

- **Poll Loop**: A single goroutine that owns the Kafka consumer, calls `Poll()`, groups messages by partition, and feeds them to the Dispatcher. Uses `Pause()`/`Resume()` for backpressure. Periodically commits offsets via the OffsetCoordinator. One per process (Kafka client constraint).
- **Dispatcher**: Accepts per-partition messages from the poll loop, assembles batches (by size and/or linger time), and dispatches them to internal worker goroutines. Notifies the OffsetCoordinator on batch dispatch and completion. Defined as a Go interface with pluggable implementations for different ordering modes.
- **Workers**: Goroutines managed internally by the Dispatcher. Execute the developer-provided `BatchProcessor` function. Workers have no knowledge of Kafka or offsets — they process a batch and return success or failure.
- **OffsetCoordinator**: Tracks dispatched and completed batches per partition. Reports which offsets are safe to commit. Called by the Dispatcher (dispatch/complete events) and the poll loop (committable query). Internal framework component — not exposed to the developer.

> **Architectural Decisions**:
> - Kafka serves as the sole durable buffer. See [ADR-0002](adr/0002-kafka-as-durable-buffer.md).
> - Single poll loop with pluggable Dispatcher for concurrency. See [ADR-0003](adr/0003-single-poll-loop-with-dispatcher.md).
> - OffsetCoordinator with per-partition batching for safe offset commits. See [ADR-0004](adr/0004-offset-coordinator-with-per-partition-batching.md).
> - Recover worker panics with graceful shutdown. See [ADR-0005](adr/0005-panic-recovery-with-graceful-shutdown.md).
> - Broker unavailability handling with dispatch pause and readiness degradation. See [ADR-0006](adr/0006-broker-unavailability-handling.md).
> - Circuit breaker for target service unavailability with error classification. See [ADR-0007](adr/0007-circuit-breaker-for-target-unavailability.md).

---

## Functional Requirements

### FR-1: Kafka Message Ingestion (Poll Loop)

- **FR-1.1**: The poll loop MUST consume messages from one or more Kafka topic partitions using consumer groups.
- **FR-1.2**: The poll loop MUST group polled messages by partition and push each group to the Dispatcher via `Send()`.
- **FR-1.3**: Kafka offsets MUST only be committed after messages are successfully processed and delivered to the target service by workers, as reported by the OffsetCoordinator.
- **FR-1.4**: The poll loop MUST handle consumer group rebalancing (partition reassignment) without data loss. On partition revocation, it MUST: notify the Dispatcher (which drains in-flight work), commit completed offsets via the OffsetCoordinator, and reset the OffsetCoordinator state for revoked partitions.
- **FR-1.5**: The poll loop MUST use `Pause()` and `Resume()` on partitions to implement backpressure when the Dispatcher signals it is full.
- **FR-1.6**: The poll loop MUST continue calling `Poll()` at regular intervals regardless of pause state, to maintain the consumer group session and avoid `max.poll.interval.ms` violations.
- **FR-1.7**: There MUST be exactly one poll loop goroutine per process. Horizontal scaling is achieved by running multiple instances in the same consumer group.
- **FR-1.8**: The poll loop MUST use cooperative sticky partition assignment (`CooperativeStickyAssignor`) to minimize rebalance disruption.
- **FR-1.9**: The poll loop MUST track consecutive `CommitOffsets()` failures. After a configurable threshold of consecutive failures (default: 3), the poll loop MUST enter degraded mode (`BrokerUnavailable` reason): pause all partitions and stop dispatching new batches. The poll loop MUST continue calling `Poll()` to maintain the consumer group session. See [ADR-0006](adr/0006-broker-unavailability-handling.md).
- **FR-1.10**: On successful `CommitOffsets()` after being in degraded mode, the poll loop MUST clear the `BrokerUnavailable` degraded reason. The poll loop MUST only exit degraded mode (resume partitions and dispatching) when **all** degraded reasons are cleared. See [ADR-0006](adr/0006-broker-unavailability-handling.md), [ADR-0007](adr/0007-circuit-breaker-for-target-unavailability.md).
- **FR-1.11**: The poll loop MUST react to the Dispatcher's `CircuitStateChanged()` channel. When the circuit breaker opens (`CircuitOpen`), the poll loop MUST enter degraded mode (`TargetUnavailable` reason). When the circuit breaker closes (`CircuitClosed`), the poll loop MUST clear the `TargetUnavailable` reason. When the circuit breaker enters half-open (`CircuitHalfOpen`), the poll loop MUST resume limited dispatch for probing. See [ADR-0007](adr/0007-circuit-breaker-for-target-unavailability.md).

### FR-2: Dispatcher

- **FR-2.1**: The Dispatcher MUST be defined as a Go interface to allow pluggable ordering mode implementations.
- **FR-2.2**: The Dispatcher MUST accept per-partition message groups from the poll loop via `Send()`.
- **FR-2.3**: The Dispatcher MUST assemble messages into per-partition batches by configurable batch size and/or linger time.
- **FR-2.4**: The Dispatcher MUST manage internal channel topology and worker goroutine lifecycle. Workers are not exposed outside the Dispatcher.
- **FR-2.5**: The Dispatcher MUST signal backpressure to the poll loop when its internal capacity is reached (`Send()` returns an error). The Dispatcher MUST expose a `Ready()` channel that is signaled when capacity becomes available again, so the poll loop can resume paused partitions without polling.
- **FR-2.6**: The Dispatcher MUST support configurable bounded capacity to cap memory usage.
- **FR-2.7**: The Dispatcher MUST handle partition assignment and revocation notifications from the poll loop. On revocation, the Dispatcher MUST drain in-flight work for the revoked partitions before returning.
- **FR-2.8**: The Dispatcher MUST call `OffsetCoordinator.BatchDispatched()` when dispatching a batch to a worker, and `OffsetCoordinator.BatchComplete()` when a worker reports success.
- **FR-2.9**: The Dispatcher MUST invoke the developer-provided `BatchProcessor` function from worker goroutines. The developer implements only this function.
- **FR-2.10**: The Dispatcher MUST implement a circuit breaker (using `sony/gobreaker`) that wraps each `BatchProcessor` attempt for transient errors. The circuit breaker MUST use a failure rate threshold over a sliding window. The Dispatcher MUST expose a `CircuitStateChanged() <-chan CircuitState` channel that emits the new state on every transition (Closed, Open, HalfOpen). See [ADR-0007](adr/0007-circuit-breaker-for-target-unavailability.md).

#### FR-2.11: Ordering Modes

The following ordering modes MUST be supported via separate Dispatcher implementations:

- **FR-2.11.1 — Unordered (default)**: A single bounded channel feeds a static worker pool. Per-partition batches are dispatched to any available worker. Provides maximum throughput with no ordering guarantees.
- **FR-2.11.2 — Partition-ordered (future)**: One channel and one worker per assigned partition, created and destroyed dynamically on rebalance. Preserves Kafka's per-partition message ordering.
- **FR-2.11.3 — Key-ordered (future)**: Messages are routed by consistent hash of the message key to one of N worker channels. Preserves per-key ordering across partitions.

### FR-3: Batch Processing (Workers)

- **FR-3.1**: Workers MUST be managed internally by the Dispatcher. The developer does not create or manage worker goroutines directly.
- **FR-3.2**: Workers MUST execute the developer-provided `BatchProcessor` function, which receives a batch of messages and returns success or failure.
- **FR-3.3**: Workers MUST process each batch atomically — the entire batch succeeds or fails as a unit.
- **FR-3.4**: On processing failure, the Dispatcher MUST classify the error before acting. If the `BatchProcessor` returns a `*ErrNonRetryable`, the batch MUST be sent to the DLQ immediately without retries and without reporting to the circuit breaker. If the `BatchProcessor` returns any other error (transient, the default), the batch MUST be retried according to the retry policy (see NFR-6), and each attempt MUST be reported to the circuit breaker. The Kafka offset for failed batches MUST NOT be committed until the batch is handled (either successfully, DLQ'd, or held by the circuit breaker). See [ADR-0007](adr/0007-circuit-breaker-for-target-unavailability.md).
- **FR-3.5**: Workers MUST have no knowledge of Kafka, offsets, or the OffsetCoordinator. They only know about the batch they receive and the target service they write to.
- **FR-3.6**: Worker goroutines MUST recover panics using `defer`/`recover`. On panic recovery, the worker MUST log the panic value, full stack trace, and batch context (partition, offset range), then trigger a graceful shutdown by canceling the root context. The panicking batch's offset MUST NOT be committed. See [ADR-0005](adr/0005-panic-recovery-with-graceful-shutdown.md).

### FR-4: Dead Letter Queue (DLQ)

- **FR-4.1**: Messages MUST be produced to a Dead Letter Queue Kafka topic in two cases: (a) the `BatchProcessor` returns a `*ErrNonRetryable` (immediate DLQ, no retries), or (b) retries are exhausted **and** the circuit breaker is in Closed state (genuine persistent failure for that specific batch). If retries are exhausted but the circuit breaker is Open, the batch MUST be held — not DLQ'd — because the downstream is unavailable, not the message. See [ADR-0007](adr/0007-circuit-breaker-for-target-unavailability.md).
- **FR-4.2**: The DLQ message MUST preserve the original message content and metadata (source topic, partition, offset, error reason, error classification, timestamp).
- **FR-4.3**: After a message is sent to the DLQ, its offset MUST be committed so the consumer advances past it.

### FR-5: Offset Coordination (OffsetCoordinator)

- **FR-5.1**: The OffsetCoordinator MUST track which batches have been dispatched and which have completed, per partition.
- **FR-5.2**: The OffsetCoordinator MUST report committable offsets: the highest offset (+ 1) for each partition where the in-flight batch has completed since the last query.
- **FR-5.3**: The OffsetCoordinator MUST support resetting state for a partition when it is revoked, to prevent stale state from leaking into future assignments.
- **FR-5.4**: The OffsetCoordinator MUST be safe for concurrent use. The Dispatcher calls `BatchDispatched()` and `BatchComplete()` from worker goroutines, while the poll loop calls `Committable()` from the poll goroutine.
- **FR-5.5**: The initial implementation MUST use per-partition batching with one in-flight batch per partition, eliminating the need for bitmap or high-watermark tracking. The interface MUST support future upgrade to a contiguous offset tracker for multiple in-flight batches per partition.

### FR-6: Offset Commit Strategy

- **FR-6.1**: Offsets MUST be committed using a hybrid strategy: periodic timer for steady state, synchronous commit on shutdown and partition revocation.
- **FR-6.2**: The periodic commit interval MUST be configurable (default: 5 seconds).
- **FR-6.3**: On graceful shutdown (SIGTERM/SIGINT), the poll loop MUST commit all committable offsets synchronously before closing the Kafka consumer.
- **FR-6.4**: On partition revocation, the poll loop MUST commit committable offsets for the revoked partitions synchronously, then reset the OffsetCoordinator state for those partitions.
- **FR-6.5**: `enable.auto.commit` MUST be disabled. All offset commits are manual.

### FR-7: Error Classification

- **FR-7.1**: The framework MUST provide a `ErrNonRetryable` type that developers can use to wrap errors that should never be retried (e.g., validation failures, schema mismatches, authorization errors).
- **FR-7.2**: Any error returned by `BatchProcessor` that is NOT a `*ErrNonRetryable` MUST be treated as transient by default. This is a safe default: unknown errors are retried, preventing accidental data loss.
- **FR-7.3**: Error classification MUST be checked using Go's `errors.As()` to support wrapped error chains.

---

## Non-Functional Requirements

### NFR-1: Backpressure

- **NFR-1.1**: The system MUST protect against the poll loop outpacing workers. When the Dispatcher reaches capacity, the poll loop MUST pause the relevant Kafka partitions until workers drain space.
- **NFR-1.2**: Backpressure MUST NOT cause message loss.
- **NFR-1.3**: Backpressure MUST NOT cause consumer group session timeouts. The poll loop MUST remain active (calling `Poll()`) even when partitions are paused.

### NFR-2: Reliability & Delivery Guarantees

- **NFR-2.1**: The system MUST provide at-least-once delivery semantics end-to-end.
- **NFR-2.2**: The developer-provided `BatchProcessor` SHOULD handle duplicate messages idempotently, or the target service MUST be idempotent.
- **NFR-2.3**: The system MUST shut down gracefully on SIGTERM/SIGINT: stop polling new messages, close the Dispatcher with a shutdown timeout context (draining in-flight batches), commit final offsets via OffsetCoordinator, and close the Kafka consumer before exiting. If the shutdown timeout expires before all workers complete, the remaining in-flight batches are abandoned and their offsets are not committed. The shutdown timeout MUST be shorter than Kubernetes `terminationGracePeriodSeconds` to leave margin for offset commits and cleanup.
- **NFR-2.4**: On crash recovery, the service MUST resume consumption from the last committed Kafka offset, accepting that some messages may be reprocessed (at-least-once guarantee). The reprocessing window is bounded by the commit interval.

### NFR-3: Observability

- **NFR-3.1**: The service MUST expose metrics: consumer lag, throughput (messages/sec), batch processing latency, dispatcher queue depth, worker utilization, offset commit rate, error rates, and DLQ production rate.
- **NFR-3.2**: The service MUST use structured logging (JSON) with correlation IDs for tracing messages through the pipeline.
- **NFR-3.3**: The service MUST expose health check endpoints (liveness and readiness) for orchestrator integration. The liveness probe MUST succeed as long as the poll loop is running. The readiness probe MUST fail when the service is in degraded mode (any degraded reason active: `BrokerUnavailable` or `TargetUnavailable`). See [ADR-0006](adr/0006-broker-unavailability-handling.md), [ADR-0007](adr/0007-circuit-breaker-for-target-unavailability.md).

### NFR-4: Performance

- **NFR-4.1**: Concurrency MUST be configurable: number of worker goroutines, dispatcher channel capacity, poll interval, commit interval, and dispatch mode.
- **NFR-4.2**: Batch size and linger time MUST be tunable.
- **NFR-4.3**: The in-memory footprint MUST be bounded by the dispatcher channel capacity to prevent OOM conditions.

### NFR-5: Configuration

- **NFR-5.1**: All tunable parameters MUST be configurable via environment variables and/or a configuration file.
- **NFR-5.2**: The service MUST provide sensible defaults for all configuration values.
- **NFR-5.3**: Configuration MUST be validated at startup; the service MUST fail fast on invalid configuration.
- **NFR-5.4**: The shutdown timeout MUST be configurable (default: 25 seconds). It MUST be validated to be less than the Kubernetes `terminationGracePeriodSeconds` when known.
- **NFR-5.5**: The commit failure threshold MUST be configurable (default: 3 consecutive failures). This controls when the service enters degraded mode on broker unavailability.
- **NFR-5.6**: Circuit breaker parameters MUST be configurable: minimum requests for evaluation (default: 3), failure rate threshold (default: 0.6), open timeout (default: 30s), max probe requests in half-open (default: 1), and sliding window interval (default: 60s). See [ADR-0007](adr/0007-circuit-breaker-for-target-unavailability.md).

### NFR-6: Retry & Error Handling

- **NFR-6.1**: Errors returned by `BatchProcessor` MUST be classified before action. The framework provides a `ErrNonRetryable` type. Errors wrapped with `*ErrNonRetryable` are classified as non-retryable; all other errors default to transient. See [ADR-0007](adr/0007-circuit-breaker-for-target-unavailability.md).
- **NFR-6.2**: Transient failures MUST be retried with exponential backoff and jitter. Each attempt MUST be reported to the circuit breaker (success or failure).
- **NFR-6.3**: The maximum number of retries MUST be configurable.
- **NFR-6.4**: Non-retryable errors (`*ErrNonRetryable`) MUST be routed to the DLQ immediately without retries and without being reported to the circuit breaker.
- **NFR-6.5**: When transient retries are exhausted, the batch MUST be routed to the DLQ **only if** the circuit breaker is in Closed state. If the circuit breaker is Open, the batch MUST be held for reprocessing after recovery (see FR-4.1).

### NFR-7: Security

- **NFR-7.1**: Kafka connections MUST support TLS and SASL authentication.
- **NFR-7.2**: Credentials and secrets MUST NOT be hardcoded; they MUST be sourced from environment variables or a secrets manager.

### NFR-8: Testing

- **NFR-8.1**: The service MUST include unit tests for all core components (poll loop, dispatcher implementations, offset coordinator, batch processor integration).
- **NFR-8.2**: The service MUST include integration tests using a real Kafka broker (e.g., via testcontainers-go).
- **NFR-8.3**: The service SHOULD include benchmark tests to validate throughput under load.

---

## Requirement Status

| ID       | Status  | Notes |
|----------|---------|-------|
| FR-1     | Planned | FR-1.11 added for circuit breaker state transitions |
| FR-2     | Planned | Initial implementation: UnorderedDispatcher only. FR-2.10 added for circuit breaker. |
| FR-3     | Planned | FR-3.4 updated with error classification |
| FR-4     | Planned | FR-4.1 updated: DLQ gated by circuit breaker state |
| FR-5     | Planned | Initial implementation: per-partition batching (one in-flight batch) |
| FR-6     | Planned |       |
| FR-7     | Planned | Error classification (`ErrNonRetryable` type) |
| NFR-1    | Planned |       |
| NFR-2    | Planned |       |
| NFR-3    | Planned | NFR-3.3 updated: degraded mode from both broker and target unavailability |
| NFR-4    | Planned |       |
| NFR-5    | Planned | NFR-5.6 added for circuit breaker configuration |
| NFR-6    | Planned | Restructured with error classification (NFR-6.1–6.5) |
| NFR-7    | Planned |       |
| NFR-8    | Planned |       |
