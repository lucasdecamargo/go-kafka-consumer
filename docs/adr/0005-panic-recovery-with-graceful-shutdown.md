# ADR-0005: Recover Worker Panics with Graceful Shutdown

## Status

Accepted

## Context

Workers execute the developer-provided `BatchProcessor` function inside goroutines managed by the Dispatcher. A panic in any of these goroutines — caused by a nil pointer dereference, index out of bounds, failed type assertion, or similar programming error — would crash the entire process if unrecovered, killing all in-flight work across all partitions without committing offsets.

We need to define how worker panics are handled. The key insight is that **panics are fundamentally different from errors**:

| | Error | Panic |
|---|---|---|
| **Cause** | Expected operational failure (network timeout, target unavailable, invalid response) | Programming bug (nil dereference, index out of bounds, assertion failure) |
| **State** | Program state is valid; the operation simply failed | Program state may be corrupted; invariants may be violated |
| **Correct response** | Retry, backoff, eventually DLQ | Stop the service; fix the code |
| **Retrying helps?** | Yes — transient failures resolve | No — same input produces same panic |
| **DLQ appropriate?** | Yes — the message is poison or the operation is permanently failed | No — the message is fine, the code is broken |

### Industry Precedent

**Go community consensus**: Panics should be recovered in goroutines to prevent uncontrolled process crashes, but should not be silently swallowed or converted to retryable errors.

- Go's official [Defer, Panic, and Recover](https://go.dev/blog/defer-panic-and-recover) blog establishes the recover pattern.
- Eli Bendersky's [On the Uses and Misuses of Panics in Go](https://eli.thegreenplace.net/2018/on-the-uses-and-misuses-of-panics-in-go/) draws a clear line: panics signal bugs, not operational failures.
- Sarama (IBM's Go Kafka library) provides a `PanicHandler` configuration specifically for recovering panics in consumer goroutines, confirming this is a known concern in the Kafka ecosystem.

**Kubernetes as supervisor**: The "let it crash" philosophy from Erlang/OTP maps directly to Kubernetes. The pod is the isolation boundary; the kubelet is the supervisor.

- Kubernetes [Pod Lifecycle](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/) documentation defines `restartPolicy: Always` (the default) with exponential backoff: 10s → 20s → 40s → ... capped at 5 minutes.
- Persistent panics from a specific message will cause repeated crashes, entering [CrashLoopBackOff](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#restart-policy), which is visible in monitoring and triggers alerts.
- This is the Kubernetes equivalent of Erlang's supervision trees: crash, restart in a known state, alert if persistent.

### Alternatives Considered

**Let it crash (unrecovered panic)**: The panic kills the process immediately. All other in-flight batches are lost, no offsets are committed, and the reprocessing window is maximized. Rejected because it causes unnecessary collateral damage.

**Recover and retry / DLQ**: Convert the panic to an error and feed it into the retry pipeline. Rejected because (a) deterministic panics will exhaust retries with identical crashes, wasting time, (b) DLQ'ing hides the bug — the message isn't poison, the code is broken, and (c) the program may be in a corrupted state after the panic, making continued execution unsafe.

**Recover and continue (skip the batch)**: Log the panic and continue processing other batches. Rejected because a panic indicates potentially corrupted state in the process — continuing may produce silent data corruption or further panics. A clean restart is the safest recovery.

## Decision

Worker goroutines MUST recover panics using `defer`/`recover`. On recovery:

1. **Capture diagnostics**: Record the panic value and full stack trace.
2. **Log with context**: Write a structured log entry (error level) containing the panic value, stack trace, partition, batch offset range, and worker identity.
3. **Trigger graceful shutdown**: Cancel the root `context.Context`, initiating the standard graceful shutdown path ([scenario 07](../scenarios/07-graceful-shutdown.md)): the Dispatcher drains other in-flight batches, the OffsetCoordinator reports committable offsets, and the poll loop commits them before closing the Kafka consumer.
4. **Let Kubernetes restart**: The process exits with a non-zero exit code. Kubernetes restarts the pod per `restartPolicy`. The service resumes from the last committed offset.

The offset for the panicking batch is **not committed**. On restart, Kafka redelivers that batch. If the panic is deterministic for that message, the service enters CrashLoopBackOff — this is the desired behavior, as it makes the bug visible for engineers to fix.

### Panic Recovery in the Worker

```go
func (d *dispatcher) runWorker(ctx context.Context, ch <-chan Batch) {
    for batch := range ch {
        d.processBatch(ctx, batch)
    }
}

func (d *dispatcher) processBatch(ctx context.Context, batch Batch) {
    defer func() {
        if r := recover(); r != nil {
            d.logger.Error("worker panic: initiating graceful shutdown",
                "panic", r,
                "stack", string(debug.Stack()),
                "partition", batch.Partition,
                "offset_range", fmt.Sprintf("[%d, %d]", batch.MinOffset, batch.MaxOffset),
            )
            // cancelRoot() is idempotent — safe to call from multiple
            // panicking workers. The first call triggers graceful shutdown;
            // subsequent calls are no-ops.
            d.cancelRoot()
        }
    }()

    // retry loop for errors (not panics)
    err := d.processor(ctx, batch.Messages)
    if err != nil {
        // ... retry / DLQ logic (see ADR-0007, scenario 04/05)
    }
    d.coordinator.BatchComplete(batch.Partition)
}
```

### Cascading Panics During Graceful Shutdown

When the first panic triggers graceful shutdown, `Dispatcher.Close()` waits for all in-flight workers to finish. Other workers may also panic during the drain phase. The design handles this correctly:

1. **`recover()` catches every panic independently.** Each worker goroutine has its own deferred `recover()`. A panic in worker 2 does not affect the recovery of worker 3.
2. **`cancelRoot()` is idempotent.** Calling `cancel()` on an already-canceled `context.Context` is a no-op in Go. Multiple panicking workers can all call it safely.
3. **Every panic is logged.** Each recovered panic produces its own structured log entry with full diagnostics. No panics are swallowed.
4. **Worker completion MUST be signaled on panic.** The Dispatcher tracks in-flight workers (e.g., via `sync.WaitGroup`). The `defer` block that calls `recover()` MUST also decrement the worker count, ensuring `Close()` does not hang waiting for a panicked worker. This is critical: if a panicked worker fails to signal completion, the shutdown blocks indefinitely.

```go
func (d *dispatcher) processBatch(ctx context.Context, batch Batch) {
    defer d.wg.Done() // MUST run even on panic — signals worker is done
    defer func() {
        if r := recover(); r != nil {
            // ... log and cancel as above
        }
    }()
    // ...
}
```

Note the `defer` ordering: `d.wg.Done()` is deferred first but executes last (LIFO), ensuring the `recover()` runs before the WaitGroup is decremented. This means the panic is fully handled before `Close()` sees the worker as done.

### What Happens to the Panicking Batch

The panicking batch is neither committed nor DLQ'd. It remains unconsumed at its Kafka offset. On restart:

- **If the panic was a transient bug** (race condition, corrupted state from a prior operation): the batch processes successfully after restart.
- **If the panic is deterministic for that input**: the batch causes another panic → restart → panic → CrashLoopBackOff → alerts fire → engineers deploy a fix.

In both cases, the correct outcome is achieved without data loss or silent error masking.

## Consequences

### What becomes easier

- **Bugs are loud.** Panics crash the service and trigger CrashLoopBackOff — engineers are alerted. No silent data loss or hidden failures.
- **Clean recovery.** Graceful shutdown commits completed work, minimizing the reprocessing window.
- **Simple mental model.** Errors are retried/DLQ'd (scenario 04/05). Panics crash the service (scenario 11). No ambiguity.

### What becomes harder

- **Deterministic panics block the partition.** If a specific message always triggers a panic, the partition is blocked until engineers deploy a fix. This is intentional — the alternative (DLQ) would hide the bug.
- **Shutdown latency.** Graceful shutdown waits for other in-flight batches to complete. If a batch is slow, the shutdown takes longer. This is bounded by the processing timeout.

### What changes in other components

- **Dispatcher**: Worker goroutines add `defer`/`recover` with structured logging and root context cancellation.
- **Poll loop**: No change — graceful shutdown is already triggered by context cancellation (scenario 07).
- **OffsetCoordinator**: No change — the panicking batch was dispatched but never completed, so its offset is not committable.

### References

- [Go Blog: Defer, Panic, and Recover](https://go.dev/blog/defer-panic-and-recover)
- [Eli Bendersky: On the Uses and Misuses of Panics in Go](https://eli.thegreenplace.net/2018/on-the-uses-and-misuses-of-panics-in-go/)
- [Kubernetes: Pod Lifecycle](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/)
- [Applying "Let It Crash" Outside Erlang](https://stratus3d.com/blog/2020/01/20/applying-the-let-it-crash-philosophy-outside-erlang/)
- [Crash-Proof Go Services: Recovering Panics in Goroutines](https://medium.com/@sogol.hedayatmanesh/crash-proof-go-services-why-you-must-recover-panics-in-goroutines-whether-you-like-it-or-not-4c2bbecfd191)
- [Sarama PanicHandler](https://github.com/IBM/sarama)
