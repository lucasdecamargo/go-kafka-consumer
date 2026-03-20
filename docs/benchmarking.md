# Benchmarking Strategy

This document defines the benchmarking strategy for the Kafka consumer framework. It covers tooling, methodology, test configurations, and the layered benchmark structure.

## Goals

1. **Quantify framework overhead** — measure the gap between raw Kafka client throughput and the framework's end-to-end throughput.
2. **Identify bottlenecks** — pinpoint CPU hotspots and allocation-heavy paths in the dispatcher, worker pool, and flow-control logic.
3. **Guide tuning** — produce data that maps configuration knobs (worker count, batch size, linger time, channel capacity) to throughput and latency outcomes.
4. **Prevent regressions** — establish baseline numbers that CI can compare against on every change.

## Tooling

### Go Standard Toolchain

| Tool | Purpose |
|------|---------|
| `testing.B.Loop` (Go 1.24+) | Microbenchmark harness. Prevents dead-code elimination, auto-manages timer start/stop. Replaces the legacy `for i := 0; i < b.N; i++` pattern. |
| `-benchmem` / `b.ReportAllocs()` | Reports `B/op` (bytes per operation) and `allocs/op`. Essential for hot-path allocation tracking. |
| `benchstat` (`golang.org/x/perf`) | Statistical comparison of benchmark runs — median, confidence intervals, A/B percentage change. Install: `go install golang.org/x/perf/cmd/benchstat@latest`. |
| `runtime/pprof` + `net/http/pprof` | CPU, heap, goroutine, mutex, and block profiling. `go tool pprof -http=":8000" cpu.prof` launches a web UI with flame graphs. |
| `-cpuprofile` / `-memprofile` | Flags on `go test -bench` to capture profiles during benchmark runs. |

### Kafka Baseline Tools

| Tool | Purpose |
|------|---------|
| `kafka-consumer-perf-test.sh` | Ships with Kafka. Measures raw broker-to-consumer throughput (records/sec, MB/sec) without application logic. Establishes the cluster ceiling. |
| `kafka-producer-perf-test.sh` | Ships with Kafka. Loads topics for consumer benchmarks and measures producer throughput. |
| franz-go `bench` example | Pure-Go Kafka client benchmark (`github.com/twmb/franz-go/examples/bench`). Establishes the Go-client throughput ceiling — the gap between franz-go raw and the framework is the overhead budget. |

### Optional / Production

| Tool | Purpose |
|------|---------|
| Grafana Pyroscope | Continuous profiling platform (~2–5% overhead). Stores historical profiles, generates flame graphs, integrates with Grafana dashboards. Can feed profiles into PGO. |
| Profile-Guided Optimization (PGO) | Go 1.21+ compiler feature. Feed a production CPU profile to the compiler for automatic hot-path optimization. |

## Methodology

### General Principles

1. **Establish a baseline first.** Run `kafka-consumer-perf-test.sh` and franz-go's bench tool on the target cluster before measuring the framework. The framework's overhead is the difference.
2. **Run long enough.** Minimum 15 minutes per configuration after warm-up. Short runs (<5 min) never leave the warm-up phase (page cache, connection pooling, batch accumulation need time to stabilize).
3. **Repeat and report statistics.** Run each configuration 3–5 times. Report median and variance via `benchstat`. Never report a single run.
4. **Change one variable at a time.** Sweep one knob (e.g., worker count) while holding all others constant. This isolates the effect of each parameter.
5. **Use realistic payloads.** Test with message sizes that match production (typically 512 B–10 KB JSON). Trivial payloads (10 bytes) stress per-record overhead but don't represent real workloads.
6. **Include warm-up.** Discard the first 1–2 minutes of data. OS page cache, Go runtime, and Kafka client internals all need time to stabilize.
7. **Measure percentiles, not averages.** Report p50, p90, p95, p99, p99.9. Averages hide tail latency problems.

### What to Measure

| Metric | How | Why |
|--------|-----|-----|
| **Throughput** (records/sec) | Count messages processed over time | Primary capacity signal |
| **Throughput** (MB/sec) | Multiply records/sec by avg message size | Measures serialization/network overhead |
| **End-to-end latency** (p50/p95/p99) | `record_age_seconds` histogram | SLO metric — production to processing completion |
| **Framework latency** (p50/p95/p99) | `message_delay_seconds` histogram | Poll to processing completion — the framework's contribution |
| **Processor latency** (p50/p95/p99) | `processing_time_seconds` histogram | Isolates target service latency from pipeline overhead |
| **Consumer lag** | `record_age - message_delay` | Time messages spent in Kafka before being polled |
| **CPU utilization** | `pprof` CPU profile | Identifies hot functions |
| **Allocations** | `-benchmem`, `pprof` allocs profile | Allocation pressure drives GC pauses at p99.9 |
| **GC pause time** | `runtime.MemStats.PauseTotalNs` | Directly impacts tail latency |
| **Inflight messages** | `inflight_messages` gauge | Backpressure and buffer utilization |
| **Worker utilization** | `workers_active` gauge | Are workers saturated or idle? |

### Common Pitfalls to Avoid

1. **Measuring averages instead of percentiles** — hides tail latency; p99 is where problems live.
2. **Too-short test runs** — <5 min never leaves warm-up; data is meaningless.
3. **Unrealistic message sizes** — "hello world" payloads produce misleading throughput numbers.
4. **Changing multiple variables at once** — impossible to attribute causation.
5. **Ignoring coordinated omission** — if the benchmark tool pauses under backpressure, it undercounts latency. Measure latency from intended send time, not actual send time.
6. **Benchmarking on a single-broker container** — doesn't predict production behavior. Testcontainers is for correctness tests, not performance.
7. **Not accounting for rebalances** — spinning up consumers gradually causes rebalance storms that distort throughput numbers.

## Benchmark Layers

### Layer 1: Microbenchmarks (No Kafka)

**Purpose:** Measure internal component performance in isolation — no network, no Kafka, no I/O. These run in CI on every commit.

**Tool:** `go test -bench -benchmem` with `testing.B.Loop` + `benchstat` for comparison.

**Targets:**

| Benchmark | What It Measures |
|-----------|-----------------|
| `BenchmarkBatchAssembly` | Time and allocations to assemble a batch of N messages from a channel |
| `BenchmarkDispatchRoundTrip` | Time from `dispatchBatch()` call to `onBatchDone` callback, with a no-op processor |
| `BenchmarkWorkerPoolScaling` | Throughput vs. worker count (1, 2, 4, 8, 16) with a fixed-latency processor |
| `BenchmarkChannelSendReceive` | Raw channel throughput at various buffer capacities (100, 1K, 10K) |
| `BenchmarkMessageAllocation` | Bytes/op and allocs/op for creating and routing a `types.Message` through the pipeline |
| `BenchmarkFlowControl` | Pause/resume signaling latency under contention |
| `BenchmarkCircuitBreakerOverhead` | Per-call overhead of the circuit breaker in the closed state |

**Profiling:** Run with `-cpuprofile` and `-memprofile` to identify hotspots:

```bash
go test -bench=BenchmarkDispatchRoundTrip -benchmem -cpuprofile cpu.prof -memprofile mem.prof ./internal/dispatcher/
go tool pprof -http=":8000" cpu.prof
```

### Layer 2: End-to-End Benchmarks (Real Kafka)

**Purpose:** Measure the full framework under realistic conditions — Kafka broker, network, serialization, consumer group coordination, offset commits.

**Infrastructure:** 3-broker KRaft cluster via Docker Compose (not testcontainers — the cluster must stay up across benchmark runs).

```yaml
# docker-compose.bench.yml (simplified)
services:
  kafka-1:
    image: apache/kafka-native:latest
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      # ... KRaft configuration
  kafka-2:
    image: apache/kafka-native:latest
    # ...
  kafka-3:
    image: apache/kafka-native:latest
    # ...
```

**Procedure:**

1. Start the 3-broker cluster.
2. Create the test topic with the desired partition count and replication factor 3.
3. Pre-load the topic using `kafka-producer-perf-test.sh` or a Go producer.
4. Run the framework consumer with the test configuration.
5. Collect metrics from the Prometheus endpoint throughout the run.
6. After the run, extract percentile latencies from histogram metrics and compute throughput.

**Test Matrix:**

| Variable | Values | Hold Constant |
|----------|--------|---------------|
| **Message size** | 100 B, 512 B, 1 KB, 10 KB | 12 partitions, 4 workers, batch 50 |
| **Partition count** | 6, 12, 24 | 1 KB messages, workers = partitions, batch 50 |
| **Worker count** | 1, 2, 4, 8, 16 | 12 partitions, 1 KB messages, batch 50 |
| **Batch size** | 10, 50, 100, 500 | 12 partitions, 4 workers, 1 KB messages |
| **Linger time** | 10 ms, 50 ms, 100 ms, 500 ms | 12 partitions, 4 workers, 1 KB messages, batch 50 |
| **Channel capacity** | 100, 500, 1000, 5000 | 12 partitions, 4 workers, 1 KB messages, batch 50 |

Each cell: 15-minute run after 2-minute warm-up, repeated 3 times.

**Processor Simulation:** Use a configurable-latency processor to simulate target service behavior:

```go
// benchProcessor returns a processor that sleeps for the given duration,
// simulating target service latency.
func benchProcessor(latency time.Duration) func(context.Context, []types.Message) error {
    return func(ctx context.Context, msgs []types.Message) error {
        time.Sleep(latency)
        return nil
    }
}
```

Test with processor latencies: 0 ms (throughput ceiling), 1 ms, 5 ms, 10 ms, 50 ms.

### Layer 3: Continuous Profiling (Production)

**Purpose:** Observe real-world performance characteristics under production traffic patterns that synthetic benchmarks can't reproduce.

**Setup:**

1. Expose `net/http/pprof` on a separate port (not the public metrics port):
   ```go
   go func() {
       mux := http.NewServeMux()
       mux.HandleFunc("/debug/pprof/", pprof.Index)
       // ... register pprof handlers
       http.ListenAndServe("localhost:6060", mux)
   }()
   ```
2. Optionally integrate Grafana Pyroscope for continuous collection:
   ```go
   pyroscope.Start(pyroscope.Config{
       ApplicationName: "kafka-consumer",
       ServerAddress:   "http://pyroscope:4040",
       ProfileTypes:    []pyroscope.ProfileType{pyroscope.ProfileCPU, pyroscope.ProfileAllocObjects},
   })
   ```
3. Feed collected profiles into PGO for compiler optimization:
   ```bash
   go build -pgo=cpu.pprof -o consumer ./cmd/consumer
   ```

## Go-Specific Tuning Notes

| Area | Guidance |
|------|----------|
| **GC tuning** | Default `GOGC=100`. Under high allocation rates, try `GOGC=200` or use `GOMEMLIMIT` to cap heap and let the runtime decide GC frequency. Measure p99.9 latency impact. |
| **Channel overhead** | Channels have measurable overhead at >100K ops/sec. Benchmark with and without channels (direct function calls) to quantify the cost of the channel-based dispatcher architecture. |
| **`sync.Pool`** | Consider pooling `types.Message` slices and batch structs to reduce allocation pressure on the hot path. Measure `allocs/op` before and after. |
| **PGO** | After collecting a representative CPU profile, rebuild with `-pgo=profile.pprof`. Expect 2–7% throughput improvement on hot paths. |

## File Structure

Layer 1 microbenchmarks live in-package as `bench_test.go` files so they can access unexported symbols (idiomatic Go convention). Shared benchmark helpers are in `internal/benchutil/`. Layer 2 E2E benchmarks live in `benchmarks/e2e/` and use the public consumer API.

```
internal/
├── benchutil/                    # Shared benchmark helpers (message generators, no-op impls)
│   └── helpers.go
├── dispatcher/
│   └── bench_test.go             # Dispatcher microbenchmarks (assembly, dispatch, channel)
├── offset/
│   └── bench_test.go             # Offset coordinator microbenchmarks (cycle, committable)
benchmarks/
├── e2e/                          # Layer 2: End-to-end benchmarks (build tag: e2ebench)
│   ├── bench_test.go             # Throughput sweeps (msg size, workers, batch, latency, channel)
│   └── helpers_test.go           # Cluster setup, message production, consumer runner
├── docker-compose.bench.yml      # 3-broker KRaft cluster for e2e benchmarks
├── scripts/
│   ├── run_micro.sh              # Run all microbenchmarks, produce benchstat output
│   ├── run_e2e.sh                # Start cluster + run e2e benchmarks
│   └── compare.sh                # Compare two benchmark runs with benchstat
└── results/                      # Benchmark result files (gitignored)
    └── .gitkeep
```

## References

- [Go Blog: More Predictable Benchmarking with testing.B.Loop](https://go.dev/blog/testing-b-loop)
- [benchstat Documentation](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat)
- [Go runtime/pprof](https://pkg.go.dev/runtime/pprof)
- [Grafana Pyroscope](https://github.com/grafana/pyroscope)
- [PGO with Pyroscope](https://grafana.com/blog/how-to-use-pgo-and-grafana-pyroscope-to-optimize-go-applications/)
- [Confluent: Kafka Performance Testing](https://www.confluent.io/learn/kafka-performance-testing/)
- [LinkedIn: Benchmarking Apache Kafka](https://engineering.linkedin.com/kafka/benchmarking-apache-kafka-2-million-writes-second-three-cheap-machines)
- [OpenMessaging Benchmark](https://openmessaging.cloud/docs/benchmarks/)
- [WarpStream: Benchmarking and Tuning](https://docs.warpstream.com/warpstream/kafka/benchmarking)
- [franz-go Bench Example](https://github.com/twmb/franz-go/tree/master/examples/bench)
- [Prometheus: Histograms and Summaries](https://prometheus.io/docs/practices/histograms/)
