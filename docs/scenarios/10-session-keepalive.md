# Scenario: Session Keepalive Under Load

> **Requirements:** FR-1.6, NFR-1.3

## Trigger

Workers are processing slowly (heavy batch processing, target service latency) and partitions are paused due to backpressure. The poll loop must maintain the Kafka consumer group session to avoid being kicked out.

## Preconditions

- One or more partitions are paused (backpressure active — see [scenario 02](02-backpressure.md)).
- Workers are busy with long-running batch processing.
- The Kafka consumer has `max.poll.interval.ms` and `session.timeout.ms` configured.

## Execution Sequence

1. **Poll loop**: Partitions are paused due to backpressure.
2. **Poll loop**: Continues calling `Poll()` at regular intervals (must be shorter than `max.poll.interval.ms`).
3. **Kafka**: `Poll()` returns no messages from paused partitions, but the call itself:
   - Sends heartbeats to the group coordinator (maintains session).
   - Processes any pending rebalance callbacks.
   - Resets the `max.poll.interval.ms` timer.
4. **Poll loop**: Between `Poll()` calls, `select`s on the poll timer and the `Dispatcher.Ready()` channel. When `Ready()` fires, the poll loop resumes partitions (see [scenario 02](02-backpressure.md)). Until then, the loop repeats.

### Failure Mode: What Happens If Poll() Is NOT Called

If the poll loop were blocked (e.g., synchronously waiting for a worker), Kafka would:

1. **After `max.poll.interval.ms`** (default 300s): The broker considers the consumer dead.
2. **Kafka**: Triggers a rebalance — revokes all partitions from this consumer.
3. **Result**: Partitions are reassigned to other consumers. In-flight work on this consumer is wasted. Messages are reprocessed by the new owner.

This scenario is **prevented by design**: the poll loop never blocks on worker completion. Backpressure is handled via pause/resume, keeping the poll loop free to call `Poll()`.

## State Changes

- **Partitions**: Remain paused. No messages delivered from paused partitions.
- **Consumer group session**: Maintained — heartbeats sent via `Poll()`.
- **OffsetCoordinator**: Commit timer continues to fire. If batches complete during this period, offsets are committed normally.

## Outcome

- The consumer group session is never interrupted by slow processing.
- No unnecessary rebalances triggered by processing latency.
- Backpressure is transparent to Kafka — the broker sees a healthy consumer that simply isn't fetching from some partitions.
- When workers catch up, partitions are resumed and processing continues without any group disruption.
