# ADR-0006: Broker Unavailability Handling with Dispatch Pause and Readiness Degradation

## Status

Accepted

## Context

The Kafka broker can become unavailable due to network partitions, broker crashes, maintenance rolling restarts, or full cluster outages. During these periods:

- `Poll()` returns no messages but does not crash — librdkafka handles reconnection internally with exponential backoff (`reconnect.backoff.ms` → `reconnect.backoff.max.ms`, default 100ms → 10s with jitter).
- `CommitOffsets()` calls fail — the offset coordinator (broker managing `__consumer_offsets`) is unreachable.
- The consumer group session may survive if the outage is shorter than `session.timeout.ms` (default 45s in Kafka 3.0+), since heartbeats are queued locally by librdkafka.

The poll loop continues running, workers continue processing, and the OffsetCoordinator accumulates completed-but-uncommitted offsets. This creates a problem: if the service crashes during an extended broker outage, the reprocessing window equals the entire outage duration — potentially hours of duplicate processing.

### Alternatives Considered

**Keep running with no changes (Option A)**: The simplest approach. `Poll()` continues, workers process, commits retry on each timer tick. On broker recovery, all accumulated offsets commit immediately. However, in-flight work grows unbounded during the outage — a crash at any point means reprocessing everything since the last successful commit.

**Crash after a timeout (Option C)**: Shut down after N seconds of broker unavailability. Rejected because crashing doesn't help when the broker is still down — the pod restarts, reconnects, and crashes again. This also breaks group membership and triggers unnecessary rebalances, causing cascading disruption to other consumers.

**Pause dispatch on sustained commit failure (Option B)**: After detecting sustained commit failures, stop dispatching new batches. Workers drain in-flight work. The poll loop continues calling `Poll()` (session keepalive) but pauses all partitions. On broker recovery, commits succeed, dispatch resumes, partitions are unpaused. This bounds the reprocessing window to batches that were in-flight when commit failures started, regardless of outage duration.

## Decision

We adopt **dispatch pause with readiness degradation** when the Kafka broker is unavailable. The degraded mode behavior defined here is shared with [ADR-0007](0007-circuit-breaker-for-target-unavailability.md) (target service unavailability). Both scenarios trigger the same unified degraded mode in the poll loop, tracked via a bitmask of reasons (`BrokerUnavailable`, `TargetUnavailable`). The service exits degraded mode only when all reasons are cleared.

### 1. Detection: Sustained Commit Failure

The poll loop detects broker unavailability through `CommitOffsets()` failures. A single failed commit is not sufficient — transient network blips are normal. The poll loop tracks consecutive commit failures:

- After a configurable number of **consecutive commit failures** (default: 3), the service enters **degraded mode**.
- The counter resets to zero on any successful commit.

We use commit failures as the signal rather than `Poll()` errors because:
- `Poll()` returning no messages is ambiguous (could be a low-traffic topic).
- `CommitOffsets()` failure is an unambiguous signal that the broker is unreachable for offset coordination.
- Commit failure is the dangerous condition — it's what causes the reprocessing window to grow.

### 2. Degraded Mode: Pause Dispatch

When entering degraded mode:

1. **Dispatcher stops accepting new batches** — `Send()` returns a backpressure error.
2. **Poll loop pauses all partitions** — Kafka stops delivering messages.
3. **In-flight workers drain** — workers complete their current batches. The Dispatcher calls `OffsetCoordinator.BatchComplete()` as usual, but offsets remain uncommitted.
4. **Poll loop continues calling `Poll()`** — maintains consumer group session (heartbeats, rebalance callbacks). This is critical to avoid `session.timeout.ms` and `max.poll.interval.ms` violations.
5. **Commit timer continues firing** — each tick retries `CommitOffsets()`. Failed commits are logged but the service does not crash.

### 3. Readiness Probe Degradation

The readiness probe reports unhealthy when in degraded mode:

- **Readiness probe fails**: Kubernetes stops routing HTTP traffic to the pod (relevant if the service also exposes an API). Monitoring systems detect the degraded state.
- **Liveness probe succeeds**: The poll loop is alive and calling `Poll()`. No spurious restarts — crashing would only make things worse.

### 4. Recovery: Auto-Resume on Broker Return

When the broker becomes reachable again:

1. **`CommitOffsets()` succeeds** — the commit timer's next call goes through.
2. **Accumulated offsets are committed** — the OffsetCoordinator reports all completed batches.
3. **Consecutive failure counter resets** to zero.
4. **Service exits degraded mode** — Dispatcher accepts new batches, poll loop resumes partitions.
5. **Readiness probe reports healthy**.
6. **Normal processing resumes** automatically with no manual intervention.

### 5. Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `commit_failure_threshold` | 3 | Consecutive `CommitOffsets()` failures before entering degraded mode |

This is intentionally simple. Combined with the existing `commit_interval` (default 5s), the default detection time is `3 × 5s = 15s` of sustained failure before degrading.

## Consequences

### What becomes easier

- **Bounded reprocessing window.** During a broker outage, in-flight work drains and no new work is dispatched. A crash at any point during the outage only reprocesses the batches that were in-flight when degraded mode started — not the entire outage duration.
- **Self-healing.** No manual intervention needed. The service automatically resumes when the broker returns.
- **Visible degradation.** Readiness probe failure surfaces the issue in Kubernetes dashboards and alerting. Engineers know the service is alive but degraded.
- **Session preservation.** The poll loop keeps calling `Poll()`, so the consumer stays in the group. No unnecessary rebalances during the outage.

### What becomes harder

- **Processing stops during outage.** Messages are not consumed while in degraded mode. This is intentional — consuming without being able to commit creates a growing reprocessing liability. The topic retains messages until the consumer catches up (Kafka's role as durable buffer, ADR-0002).
- **Detection delay.** With the default threshold of 3 consecutive failures at 5s intervals, it takes ~15s to detect a sustained outage. During this window, new batches may be dispatched with uncommittable offsets. This is acceptable — the window is bounded and small.

### What changes in other components

- **Poll loop**: Adds a consecutive commit failure counter. On threshold breach, sets the `BrokerUnavailable` degraded reason, pauses all partitions, and enters degraded mode. On successful commit, clears the `BrokerUnavailable` reason. Exits degraded mode only when all reasons are cleared (the circuit breaker from [ADR-0007](0007-circuit-breaker-for-target-unavailability.md) may also be holding the service in degraded mode).
- **Dispatcher**: No change — backpressure already works via `Send()` returning an error. The poll loop simply stops calling `Send()`.
- **OffsetCoordinator**: No change — it already accumulates completed offsets and reports them on `Committable()`.
- **Health checks (NFR-3.3)**: Readiness probe checks degraded mode flag (any reason active = unhealthy). Liveness probe is unaffected.

### References

- [Confluent: How to Survive a Kafka Outage](https://www.confluent.io/blog/how-to-survive-a-kafka-outage/)
- [Karafka: Broker Failures and Fault Tolerance](https://karafka.io/docs/Broker-Failures-and-Fault-Tolerance/)
- [librdkafka Configuration Reference](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
- [Kafka Consumer Configuration: session.timeout.ms, heartbeat.interval.ms](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html)
- [Kubernetes: Liveness, Readiness, and Startup Probes](https://kubernetes.io/docs/concepts/configuration/liveness-readiness-startup-probes/)
- [Uber: Introducing uForwarder](https://www.uber.com/blog/introducing-ufowarder/)
- [ADR-0007: Circuit Breaker for Target Service Unavailability](0007-circuit-breaker-for-target-unavailability.md) — shares the unified degraded mode concept
