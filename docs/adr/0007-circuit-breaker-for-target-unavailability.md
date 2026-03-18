# ADR-0007: Circuit Breaker for Target Service Unavailability with Error Classification

## Status

Accepted

## Context

When the target service (database, API, downstream microservice) becomes unavailable, every batch the workers send fails. Without intervention, each batch exhausts its retries and is routed to the Dead Letter Queue (FR-4). This is wrong — the messages aren't poison, the downstream is simply down. The result is a DLQ flooded with valid messages that need manual reprocessing.

This problem is fundamentally different from individual bad messages (which belong in the DLQ) and from transient errors (which retries resolve). It's a **sustained downstream failure** — a system-level condition, not a per-message condition.

### The DLQ Flooding Problem

Without a circuit breaker:

1. Batch fails → retries exhaust → DLQ.
2. Next batch fails → retries exhaust → DLQ.
3. Every message ends up in the DLQ during the outage.
4. When the target recovers, the DLQ is full of valid messages requiring manual intervention.

### Error Classification Gap

Our existing error handling (NFR-6) treats all errors uniformly: retry with backoff → DLQ on exhaustion. But errors have different causes that require different responses:

| Error Type | Cause | Correct Response |
|---|---|---|
| **Non-retryable** | Invalid data, schema mismatch, authorization failure | DLQ immediately — retries will never succeed |
| **Transient** | Timeout, connection refused, 503, rate limit | Retry with backoff — likely to resolve |

Without classification, non-retryable errors waste time on retries (they'll never succeed), and transient errors during a sustained outage flood the DLQ (the messages are fine, the downstream is broken).

### Industry Precedent

**Circuit breaker pattern** (Martin Fowler, Michael Nygard's *Release It!*): A proxy that monitors failure rates and stops requests to a failing downstream, preventing cascading failures and resource waste.

**Resilience4j + Spring Kafka**: The standard Java integration pauses Kafka partitions when the circuit breaker opens and resumes them when it closes. This is the exact pattern we need.

**Sony gobreaker**: The most popular Go circuit breaker library. Provides a `ReadyToTrip` callback with sliding window failure rate evaluation, configurable `MaxRequests` for half-open probing, and automatic state transitions.

### Alternatives Considered

**No circuit breaker — rely on retries and DLQ (current design)**: Every batch exhausts retries independently. DLQ floods with valid messages during sustained outages. Rejected.

**Consecutive failure counter (simple threshold)**: Trip after N consecutive failures. A single success resets the counter. Simple but fragile — a flaky downstream that occasionally succeeds resets the counter even though 90% of requests are failing.

**Failure rate over sliding window (gobreaker model)**: Trip when the failure rate exceeds a threshold within a request window (e.g., `≥ 3 requests AND failure rate ≥ 60%`). More robust — captures the pattern "most requests are failing" even if occasional requests succeed. This is the industry standard approach.

## Decision

We adopt a **circuit breaker pattern with error classification** for handling target service unavailability. The circuit breaker uses a **failure rate threshold over a sliding window** (not simple consecutive failures) and leverages `sony/gobreaker` internally. Only **transient errors** interact with the circuit breaker; **non-retryable errors** bypass it entirely.

### 1. Error Classification

The developer's `BatchProcessor` communicates error type by wrapping non-retryable errors with a framework-provided sentinel type:

```go
// Framework provides:
type ErrNonRetryable struct{ Err error }
func (e *ErrNonRetryable) Error() string { return e.Err.Error() }
func (e *ErrNonRetryable) Unwrap() error { return e.Err }

// Developer returns:
return &ErrNonRetryable{Err: fmt.Errorf("invalid schema: field X")}  // → non-retryable
return fmt.Errorf("connection refused: %w", err)                      // → transient (default)
```

**Classification rules:**

| Returned Error | Classification | Action |
|---|---|---|
| `*ErrNonRetryable` | Non-retryable | DLQ immediately. No retries. Not reported to circuit breaker. |
| Any other `error` | Transient (default) | Retry with backoff. Each attempt reported to circuit breaker. |
| `nil` | Success | Reported to circuit breaker as success. |

The **default is transient**. This is a safe default: if the developer doesn't classify, the error is retried (no data lost). Non-retryable errors are opt-in — the developer explicitly marks errors they know will never succeed.

### 2. Circuit Breaker State Machine

The circuit breaker follows the standard three-state model:

```
         failure rate ≥ threshold
  ┌────────────────────────────────────┐
  │                                    ▼
CLOSED ◄──── probe succeeds ──── HALF_OPEN ◄──── timeout expires ──── OPEN
  │                                    │                                 ▲
  │                                    └── probe fails ──────────────────┘
  └── normal operation                      (backoff increases)
```

**States:**

| State | Behavior | Dispatch | Partitions |
|---|---|---|---|
| **Closed** | Normal operation. All attempts reported (success/failure). | Active | Resumed |
| **Open** | Downstream is broken. No batches dispatched. Degraded mode. | Paused | Paused |
| **Half-Open** | Recovery probing. Limited batches dispatched to test downstream. | Limited (`max_requests`) | Partially resumed |

**Transition rules (leveraging gobreaker):**

- **Closed → Open**: `ReadyToTrip` returns true when `requests >= min_requests AND failureRate >= failure_threshold`.
- **Open → Half-Open**: After `open_timeout` expires (with exponential backoff on repeated trips).
- **Half-Open → Closed**: `max_requests` probe batches all succeed.
- **Half-Open → Open**: Any probe batch fails.

### 3. Circuit Breaker Ownership

The **Dispatcher** owns the circuit breaker. It's the only component that sees results from all workers and can evaluate system-wide failure patterns.

**Where the circuit breaker wraps:**

```
Worker receives batch
    │
    ▼
CircuitBreaker.Execute(func() {
    BatchProcessor(ctx, batch)  ← each individual attempt
})
    │
    ├── ErrNonRetryable → bypass CB, DLQ immediately
    ├── Transient error → CB records failure, retry with backoff
    ├── Success → CB records success
    └── ErrCircuitOpen → don't even try, batch held
```

The circuit breaker wraps **each individual attempt** (including retries), but **only for transient errors**. This gives fast detection without non-retryable errors inflating the failure rate.

### 4. State Transition Signaling

The Dispatcher exposes a channel that emits the **new state** on every circuit breaker transition. The poll loop receives the state directly — no separate check needed.

```go
type CircuitState int
const (
    CircuitClosed  CircuitState = iota
    CircuitOpen
    CircuitHalfOpen
)

type Dispatcher interface {
    // ... existing methods ...

    // CircuitStateChanged returns a channel that emits the new state
    // whenever the circuit breaker transitions between states.
    // The poll loop selects on this channel to enter/exit degraded mode.
    CircuitStateChanged() <-chan CircuitState
}
```

The poll loop reacts to state transitions:

```go
case newState := <-dispatcher.CircuitStateChanged():
    switch newState {
    case CircuitOpen:
        enterDegradedMode(TargetUnavailable)
    case CircuitHalfOpen:
        // resume limited dispatch for probing
        resumeForProbing()
    case CircuitClosed:
        exitDegradedMode(TargetUnavailable)
    }
```

### 5. Unified Degraded Mode

Both broker unavailability (ADR-0006) and target unavailability trigger the same **degraded mode** behavior. The poll loop tracks the reason(s) for degradation using a bitmask:

```go
type DegradedReason uint8
const (
    BrokerUnavailable  DegradedReason = 1 << 0  // from commit failures (ADR-0006)
    TargetUnavailable  DegradedReason = 1 << 1  // from circuit breaker open
)
```

**Degraded mode behavior** (same for both triggers):
- Pause all partitions.
- Stop dispatching new batches.
- Continue calling `Poll()` for session keepalive.
- Readiness probe reports unhealthy.
- Liveness probe remains healthy.

**Exit condition**: Degraded mode exits only when **all reasons are cleared**. If the broker recovers but the target is still down, the service stays degraded.

### 6. Interaction with Retry and DLQ

The complete error handling flow:

```
BatchProcessor returns error
    │
    ├── ErrNonRetryable?
    │   ├── Yes → DLQ immediately (no retries, no CB)
    │   └── No (transient) ──→ Report to Circuit Breaker
    │                              │
    │                              ├── Circuit OPEN?
    │                              │   └── Yes → ErrCircuitOpen, batch held
    │                              │       (will be retried when half-open)
    │                              │
    │                              └── Circuit CLOSED/HALF_OPEN
    │                                  └── Retry with backoff
    │                                      │
    │                                      ├── Retry succeeds → CB records success
    │                                      │
    │                                      └── Retries exhausted
    │                                          │
    │                                          ├── Circuit now OPEN?
    │                                          │   └── Batch held (downstream is down)
    │                                          │
    │                                          └── Circuit still CLOSED
    │                                              └── DLQ (genuine persistent failure
    │                                                  for this specific batch)
    │
    └── nil (success) → CB records success → BatchComplete
```

**Key rule**: A batch is only sent to the DLQ when retries are exhausted **AND** the circuit is still closed. If the circuit opens at any point during the retry cycle, the batch is held — not DLQ'd. This prevents DLQ flooding entirely.

### 7. Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `cb_min_requests` | 3 | Minimum requests in the evaluation window before the failure rate is calculated |
| `cb_failure_threshold` | 0.6 | Failure rate (0.0–1.0) that triggers the circuit to open |
| `cb_open_timeout` | 30s | Time to wait in Open state before transitioning to Half-Open |
| `cb_max_requests` | 1 | Number of probe requests allowed in Half-Open state |
| `cb_interval` | 60s | Sliding window interval for failure rate calculation (Closed state) |

These map directly to `sony/gobreaker` `Settings` fields.

## Consequences

### What becomes easier

- **No DLQ flooding.** When the downstream is down, the circuit opens and batches are held — not sent to the DLQ. Valid messages are preserved in Kafka.
- **Fast detection.** Failure rate over a sliding window detects sustained outages quickly without being tripped by occasional transient errors.
- **Self-healing.** Half-open probing automatically detects recovery. No manual intervention.
- **Error classification.** Non-retryable errors skip retries entirely — no wasted time. Transient errors are retried and tracked by the circuit breaker.
- **Unified degraded mode.** Both broker and target unavailability use the same pause/resume/readiness infrastructure. No duplication.

### What becomes harder

- **Developer must classify errors.** To get the benefit of immediate DLQ for non-retryable errors, the developer wraps them with `ErrNonRetryable`. If they don't, all errors default to transient (safe but suboptimal — non-retryable errors will burn through retries).
- **More configuration.** Five new circuit breaker parameters. Mitigated by sensible defaults.
- **Half-open probing complexity.** Resuming limited dispatch for probe batches requires coordination between the Dispatcher and poll loop.

### What changes in other components

- **Dispatcher interface**: Adds `CircuitStateChanged() <-chan CircuitState`. Internal circuit breaker wraps `BatchProcessor` attempts.
- **Poll loop**: Reacts to `CircuitStateChanged()` channel. Degraded mode now tracks multiple reasons (bitmask). ADR-0006 commit failure detection unchanged — it's an independent trigger for the same degraded mode.
- **Scenario 04 (batch failure retry)**: Now specifies that only transient errors follow the retry path. Non-retryable errors bypass retries.
- **Scenario 05 (retries exhausted DLQ)**: Now specifies that DLQ only happens when the circuit is closed. If circuit is open, batch is held.
- **FR-4 (DLQ)**: Updated to specify that only non-retryable errors or retries-exhausted-with-circuit-closed go to DLQ.
- **NFR-6 (Retry & Error Handling)**: Updated with error classification.

### References

- [Martin Fowler: Circuit Breaker](https://martinfowler.com/bliki/CircuitBreaker.html)
- [Michael Nygard: Release It! — Stability Patterns](https://pragprog.com/titles/mnee2/release-it-second-edition/)
- [Sony gobreaker — Circuit Breaker in Go](https://github.com/sony/gobreaker)
- [Resilience4j CircuitBreaker](https://resilience4j.readme.io/docs/circuitbreaker)
- [Circuit Breaker for Kafka using Resilience4j](https://dev.to/yashtailor/circuit-breaker-for-kafka-3ime)
- [Kafka Consumer with Circuit Breaker, Retry Patterns](https://www.linkedin.com/pulse/kafka-consumer-circuit-breaker-retry-patterns-using-raghuram)
- [Spring Kafka: Pausing and Resuming Partitions](https://docs.enterprise.spring.io/spring-kafka/reference/kafka/pause-resume-partitions.html)
- [Advanced Kafka Resilience: DLQs, Circuit Breakers, and Exactly-Once](https://www.vinaypal.com/2025/05/advanced-kafka-resilience-dead-letter.html)
- [Confluent: How to Survive a Kafka Outage](https://www.confluent.io/blog/how-to-survive-a-kafka-outage/)
- [ADR-0006: Broker Unavailability Handling](0006-broker-unavailability-handling.md)
