# Benchmarking Implementation Plan

This document describes the concrete implementation plan for Layer 1 (microbenchmarks) and Layer 2 (end-to-end benchmarks) defined in `docs/benchmarking.md`.

## Design Decisions

### Microbenchmarks live in-package, not in a separate directory

The `benchmarking.md` strategy proposed `benchmarks/micro/` as a standalone directory. After reviewing the codebase, this is **not the right approach** for microbenchmarks because:

1. **Internal access**: Key functions to benchmark (`assembleBatch`, `tryDispatch`, `dispatchBatch`, `backoff`, partition buffers) are unexported. Benchmarks in a separate package cannot call them.
2. **Go convention**: `*_test.go` files in the same package have full access to unexported symbols. This is the idiomatic location for microbenchmarks.
3. **`go test -bench` discovery**: The standard toolchain already discovers `*_test.go` benchmarks. No custom test runner needed.

**Decision**: Layer 1 microbenchmarks live as `bench_test.go` files inside their respective packages. A top-level script runs them all.

### Shared benchmark utilities

Several benchmarks need the same setup: creating a `types.Message` with realistic payloads, building a `DispatcherMetrics`/`PollLoopMetrics` with a no-op registry, or constructing a mock coordinator. These go into `internal/benchutil/` — a test-only helper package.

### E2E benchmarks use the public consumer API

Layer 2 benchmarks exercise the full framework through `consumer.New()` and `consumer.Run()`. They live in `benchmarks/e2e/` and import only public packages. They require a running Kafka cluster (Docker Compose).

## File Structure

```
internal/
├── benchutil/                      # Shared benchmark helpers
│   └── helpers.go                  # Message generators, no-op coordinator, metrics factories
├── dispatcher/
│   └── bench_test.go               # Dispatcher microbenchmarks
├── offset/
│   └── bench_test.go               # Offset coordinator microbenchmarks
benchmarks/
├── e2e/
│   ├── bench_test.go               # E2E throughput/latency benchmarks
│   └── helpers_test.go             # E2E-specific setup (cluster, topic, produce)
├── docker-compose.bench.yml        # 3-broker KRaft cluster
├── scripts/
│   ├── run_micro.sh                # Run all microbenchmarks, produce benchstat input
│   ├── run_e2e.sh                  # Start cluster, run e2e, stop cluster
│   └── compare.sh                  # Compare two benchmark runs via benchstat
└── results/
    └── .gitkeep
```

## Layer 1: Microbenchmarks

### 1.1 `internal/dispatcher/bench_test.go`

| Benchmark | What it measures | Setup |
|-----------|-----------------|-------|
| `BenchmarkAssembleBatch` | Time/allocs to slice N messages from partition buffer into a batch | Pre-fill `partitionBuffer` with N messages, call `assembleBatch` in loop |
| `BenchmarkDispatchRoundTrip` | Full dispatch→worker→completion cycle with no-op processor | Create `UnorderedDispatcher` with no-op processor, call `Send()`, wait for `onBatchDone` via `Ready()` signal |
| `BenchmarkDispatchRoundTrip_Workers` | Same as above, sub-benchmarks with worker counts: 1, 2, 4, 8, 16 | Vary `Config.WorkerCount` |
| `BenchmarkBackoff` | Cost of backoff calculation | Call `backoff(attempt)` for various attempts |
| `BenchmarkChannelThroughput` | Raw batch channel send/receive throughput | Goroutine pair with `chan batch` at various capacities: 10, 100, 1000 |

**Key implementation details:**

- `BenchmarkAssembleBatch`: Creates an `UnorderedDispatcher` (needs coordinator + metrics), directly populates `d.partitions[0]` with messages, calls `assembleBatch` in a `b.Loop()`. Reports allocs.
- `BenchmarkDispatchRoundTrip`: Uses a real `UnorderedDispatcher` with a no-op `BatchProcessor`. Sends `BatchSize` messages to trigger immediate dispatch. Uses an `atomic.Int64` counter in the processor to detect completion. The benchmark loop sends messages and waits for the counter to increment.
- Worker scaling: Sub-benchmarks (`b.Run`) with different `WorkerCount` values, same total message count.

### 1.2 `internal/offset/bench_test.go`

| Benchmark | What it measures | Setup |
|-----------|-----------------|-------|
| `BenchmarkCoordinatorCycle` | Full Dispatch→Complete→Committable cycle per partition | Single partition, loop of Dispatch+Complete+Committable |
| `BenchmarkCoordinatorCycle_Partitions` | Same cycle with increasing partition counts: 1, 6, 12, 24 | Sub-benchmarks varying partition count |
| `BenchmarkCommittable` | Cost of `Committable()` with N completed partitions | Pre-fill N completed partitions, call `Committable()` in loop |

### 1.3 `internal/benchutil/helpers.go`

Shared utilities:

```go
// NewMessages creates N messages with a given payload size for a single partition.
func NewMessages(partition int32, count int, valueSize int) []types.Message

// NoopProcessor returns a BatchProcessor that does nothing.
func NoopProcessor() types.BatchProcessor

// FixedLatencyProcessor returns a BatchProcessor that sleeps for the given duration.
func FixedLatencyProcessor(d time.Duration) types.BatchProcessor

// NoopCoordinator returns a minimal Coordinator that tracks state without validation.
// Used in benchmarks where offset correctness is not under test.
func NoopCoordinator() offset.Coordinator

// DiscardMetrics returns DispatcherMetrics and PollLoopMetrics registered against
// a throwaway registry. Used to satisfy metric requirements without overhead.
func DiscardDispatcherMetrics() *metrics.DispatcherMetrics
func DiscardPollLoopMetrics() *metrics.PollLoopMetrics
```

## Layer 2: E2E Benchmarks

### 2.1 Docker Compose (`benchmarks/docker-compose.bench.yml`)

3-node KRaft cluster using `apache/kafka-native:latest`:
- Nodes: kafka-1 (controller+broker), kafka-2 (broker), kafka-3 (broker)
- Ports: 9092, 9093, 9094 mapped to host
- KRaft mode (no ZooKeeper)
- Volume mounts for data persistence across benchmark runs

### 2.2 `benchmarks/e2e/bench_test.go`

| Benchmark | What it measures | Variable swept |
|-----------|-----------------|----------------|
| `BenchmarkThroughput_MessageSize` | Records/sec at varying payload sizes | 100B, 512B, 1KB, 10KB |
| `BenchmarkThroughput_Workers` | Records/sec at varying worker counts | 1, 2, 4, 8, 16 |
| `BenchmarkThroughput_BatchSize` | Records/sec at varying batch sizes | 10, 50, 100, 500 |
| `BenchmarkThroughput_ProcessorLatency` | Records/sec with simulated target latency | 0ms, 1ms, 5ms, 10ms |

**Methodology:**
1. Pre-produce `N` messages to the test topic (N = 50,000 per sub-benchmark).
2. Start the consumer with the configuration under test.
3. Wait for all messages to be processed (tracked by an atomic counter in the processor).
4. Report `b.Elapsed() / N` as the per-message cost, and `N / b.Elapsed()` as throughput.
5. Use `b.ReportMetric()` for custom metrics (records/sec, MB/sec).

**E2E helpers (`helpers_test.go`):**
- `startCluster()` — checks if Docker Compose cluster is reachable
- `produceMessages()` — bulk-produces N messages using confluent-kafka-go admin client
- `waitForProcessed()` — blocks until atomic counter reaches target

### 2.3 Scripts

**`benchmarks/scripts/run_micro.sh`:**
```bash
#!/usr/bin/env bash
# Runs all microbenchmarks across internal packages, outputs benchstat-compatible format.
go test -bench=. -benchmem -count=5 -timeout=10m ./internal/dispatcher/ ./internal/offset/ | tee results/micro_$(date +%Y%m%d_%H%M%S).txt
```

**`benchmarks/scripts/run_e2e.sh`:**
```bash
#!/usr/bin/env bash
# Starts the 3-broker cluster, runs e2e benchmarks, optionally stops the cluster.
docker compose -f docker-compose.bench.yml up -d --wait
go test -bench=. -benchmem -count=3 -timeout=30m ./benchmarks/e2e/ | tee results/e2e_$(date +%Y%m%d_%H%M%S).txt
```

**`benchmarks/scripts/compare.sh`:**
```bash
#!/usr/bin/env bash
# Compares two benchmark result files using benchstat.
benchstat "$1" "$2"
```

## Self-Review

### Consistency check

- All microbenchmarks use `b.ReportAllocs()` and the `testing.B` harness — consistent with Go conventions.
- E2E benchmarks use the same `consumer.Config` structure as integration tests — no custom wiring.
- Helper package (`benchutil`) is clearly test-only, does not export production code.
- Docker Compose uses the same `apache/kafka-native` image as integration tests (via testcontainers).

### Clean code evaluation

- **No duplication**: Shared helpers extracted to `benchutil`. E2E helpers in `helpers_test.go`.
- **No test logic in production code**: All benchmark code is in `_test.go` or `benchutil` (which has no production callers).
- **Minimal new modules**: `benchutil` is the only new package. Benchmarks are added to existing packages or `benchmarks/e2e/` which is self-contained.
- **No over-engineering**: Benchmarks are straightforward Go benchmark functions. No frameworks, no custom harnesses.

### Module creation evaluation

| New module | Justification |
|-----------|---------------|
| `internal/benchutil/` | Avoids duplicating message generation, no-op processor, and metric factory across 3+ benchmark files. Small, focused, test-only. **Justified.** |
| `benchmarks/e2e/` | E2E benchmarks need their own test binary (separate `go test` invocation), docker infrastructure, and helper setup. Cannot live in an existing package. **Justified.** |

### What is NOT included (and why)

- **`BenchmarkFlowControl`** from the strategy doc: Pause/resume is poll loop logic tightly coupled to the Kafka adapter. Cannot be meaningfully benchmarked without mocking the entire Kafka client, which defeats the purpose. Covered by E2E benchmarks instead.
- **`BenchmarkCircuitBreakerOverhead`**: The circuit breaker is from `gobreaker` (third-party). Benchmarking it measures someone else's library, not our code. The real overhead is captured by `BenchmarkDispatchRoundTrip` which includes the CB call path.
- **Linger timer benchmarks**: Timer-based behavior is non-deterministic and unsuitable for microbenchmarks. The linger path is exercised by E2E benchmarks with small batch sizes.
