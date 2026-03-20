# ADR-0008: Cross-Cutting Concerns — Logging, Metrics, Configuration, and Constructor Pattern

## Status

Accepted

## Context

Before implementing core components, we need to define how cross-cutting concerns (logging, metrics, configuration) are wired into every component. These decisions affect every constructor signature, every test setup, and every component's internal structure. Getting them wrong means refactoring every component later.

### Key Questions

1. **Constructor pattern**: How do components accept required and optional dependencies?
2. **Logging**: Which library, and how is a logger injected?
3. **Metrics**: How are Prometheus metrics defined, registered, and accessed by components?
4. **Configuration**: How are configuration values structured and passed to components?

### Alternatives Considered

#### Constructor Patterns

**Struct with all fields**: `NewDispatcher(cfg, logger, metrics, coordinator)`. Simple but grows with each new dependency. Adding a parameter is a breaking change to all call sites.

**Builder / With-chaining**: `NewDispatcher(cfg).WithLogger(logger).WithMetrics(reg)`. Returns a partially configured object that the caller must remember to finalize. The object is usable in an invalid intermediate state. Error handling is awkward — each `With` call must either store errors for later or panic.

**Functional options**: `NewDispatcher(cfg, WithLogger(logger), WithMetrics(reg))`. The object is fully configured when the constructor returns. Adding new options never breaks existing call sites. The constructor can validate and return an error. This is the idiomatic Go pattern, advocated by Dave Cheney and Rob Pike, and widely adopted in production Go libraries.

#### Metrics

**Global metrics with `promauto`**: Package-level `var` declarations auto-registered on the default registry. Zero boilerplate but untestable (shared global state) and tightly coupled.

**Metrics struct injected via constructor**: A per-component struct containing Prometheus types, assembled externally and passed in. Maximum control but high boilerplate — metrics become part of the public API even though they're an implementation detail.

**Pass `prometheus.Registerer` to the component**: The component creates its own metrics internally using `promauto.With(reg)`. The caller provides the registry; the component defines what to measure. This is the pattern used by the Prometheus project itself (alertmanager, node_exporter, Thanos, Cortex).

#### Logging

**zerolog**: Fast, zero-allocation structured logging. External dependency.

**slog**: Go's standard library structured logging (since Go 1.21). No external dependency. Sufficient for production use. Growing adoption as the community standard.

## Decision

### 1. Functional Options Pattern

All components use the functional options pattern for constructor injection:

```go
type DispatcherOption func(*dispatcherOptions)

type dispatcherOptions struct {
    logger    *slog.Logger
    registerer prometheus.Registerer
}

func WithLogger(l *slog.Logger) DispatcherOption {
    return func(o *dispatcherOptions) {
        o.logger = l
    }
}

func WithMetrics(reg prometheus.Registerer) DispatcherOption {
    return func(o *dispatcherOptions) {
        o.registerer = reg
    }
}

func NewDispatcher(cfg DispatcherConfig, opts ...DispatcherOption) (*Dispatcher, error) {
    options := dispatcherOptions{
        logger:     slog.Default(),              // sensible default
        registerer: prometheus.DefaultRegisterer, // sensible default
    }
    for _, opt := range opts {
        opt(&options)
    }
    // ... build dispatcher using options
}
```

**Required dependencies** (like `DispatcherConfig` and `OffsetCoordinator`) are positional parameters — they are not optional. **Optional dependencies** (logger, metrics registerer) are functional options with sensible defaults.

### 2. Structured Logging with `slog`

All components use Go's standard `*slog.Logger` for structured logging.

- Injected via `WithLogger(logger)` functional option.
- Default: `slog.Default()` (if no logger is provided, the component still works).
- No `fmt.Println` in production code.
- Log levels follow slog conventions: `Debug`, `Info`, `Warn`, `Error`.
- All log entries include contextual fields (component name, partition, offset range, etc.).

### 3. Prometheus Metrics via Registerer Injection

Components receive a `prometheus.Registerer` via the `WithMetrics(reg)` functional option and create their own metrics internally using `promauto.With(reg)`:

```go
func newDispatcher(cfg DispatcherConfig, opts dispatcherOptions) *Dispatcher {
    reg := opts.registerer

    d := &Dispatcher{
        batchesDispatched: promauto.With(reg).NewCounter(prometheus.CounterOpts{
            Name: MetricDispatcherBatchesTotal,
            Help: "Total number of batches dispatched to workers.",
        }),
        batchLatency: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
            Name:    MetricDispatcherBatchLatencySeconds,
            Help:    "Time from batch dispatch to completion.",
            Buckets: prometheus.DefBuckets,
        }),
        // ...
    }
    return d
}
```

**Metric names are defined as constants** in the central `internal/metrics` package, not inline strings:

```go
// internal/metrics/pollloop.go, internal/metrics/dispatcher.go
const (
    MessagesPolledTotal    = "kafka_consumer_messages_polled_total"
    CommitsTotal           = "kafka_consumer_commits_total"
    CommitFailuresTotal    = "kafka_consumer_commit_failures_total"
    DegradedMode           = "kafka_consumer_degraded_mode"

    MessagesProcessedTotal = "kafka_consumer_messages_processed_total"
    MessageDelaySeconds    = "kafka_consumer_message_delay_seconds"
    CircuitBreakerState    = "kafka_consumer_circuit_breaker_state"
    // ...
)
```

**Naming conventions** follow Prometheus best practices:
- Prefix: `kafka_consumer_` (project namespace).
- No component segment — metric names are self-descriptive without encoding internal architecture.
- Suffix: `_total` for counters, `_seconds` for durations, no suffix for gauges.
- See `docs/metrics.md` for the full metrics reference.

**Default behavior**: If no `WithMetrics` is provided, the default `prometheus.DefaultRegisterer` is used. Components can also check for a nil registerer to disable metrics entirely in unit tests if needed.

### 4. Per-Component Configuration Structs

Each component defines its own configuration struct. The `main()` function (bootstrap) populates them from environment variables or a config file and passes them as required constructor parameters:

```go
type DispatcherConfig struct {
    WorkerCount    int           `env:"DISPATCHER_WORKER_COUNT" default:"4"`
    ChannelCap     int           `env:"DISPATCHER_CHANNEL_CAP" default:"100"`
    BatchSize      int           `env:"DISPATCHER_BATCH_SIZE" default:"50"`
    LingerTime     time.Duration `env:"DISPATCHER_LINGER_TIME" default:"100ms"`
    MaxRetries     int           `env:"DISPATCHER_MAX_RETRIES" default:"3"`
    // Circuit breaker
    CBMinRequests      int           `env:"CB_MIN_REQUESTS" default:"3"`
    CBFailureThreshold float64       `env:"CB_FAILURE_THRESHOLD" default:"0.6"`
    CBOpenTimeout      time.Duration `env:"CB_OPEN_TIMEOUT" default:"30s"`
    CBMaxRequests      int           `env:"CB_MAX_REQUESTS" default:"1"`
    CBInterval         time.Duration `env:"CB_INTERVAL" default:"60s"`
}

type PollLoopConfig struct {
    PollInterval           time.Duration `env:"POLL_INTERVAL" default:"100ms"`
    CommitInterval         time.Duration `env:"COMMIT_INTERVAL" default:"5s"`
    CommitFailureThreshold int           `env:"COMMIT_FAILURE_THRESHOLD" default:"3"`
    ShutdownTimeout        time.Duration `env:"SHUTDOWN_TIMEOUT" default:"25s"`
}
```

- Config structs are **required** constructor parameters (not optional).
- Validated at startup — the service fails fast on invalid configuration.
- The config loading mechanism (env vars, file, library choice) is a Phase 5 concern and does not affect component interfaces.

## Consequences

### What becomes easier

- **Consistent constructor pattern.** Every component follows the same `NewX(cfg, opts...)` signature. Developers learn one pattern.
- **Non-breaking evolution.** New optional dependencies (tracing, additional metrics) are added as new `WithX` options without changing existing call sites.
- **Testable.** Tests pass `prometheus.NewRegistry()` via `WithMetrics()` for isolated metric assertions, or omit it entirely to use defaults. `WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))` silences logs in tests.
- **Encapsulated metrics.** Components own their metric definitions. Callers don't need to know what metrics exist — they just provide a registry.
- **Discoverable metric names.** Constants in a dedicated file make it easy to grep for all metrics, generate documentation, or validate naming conventions.

### What becomes harder

- **Functional options boilerplate.** Each component needs an `options` struct, `WithX` functions, and default initialization. This is a one-time cost per component (~20 lines).
- **Metric name discipline.** All metric names must be defined as constants, not inline strings. Enforced by code review.

### What changes in other components

- **All component constructors** adopt the `NewX(cfg, opts...)` pattern.
- **All component packages** include a `metrics.go` file with metric name constants.
- **`main()`** creates the `prometheus.Registry`, `slog.Logger`, and config structs, then wires everything together.

### References

- [Dave Cheney: Functional Options for Friendly APIs](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis)
- [Rob Pike: Self-referential Functions and the Design of Options](https://commandcenter.blogspot.com/2014/01/self-referential-functions-and-design-of.html)
- [10 Years of Functional Options and Key Lessons Learned](https://www.bytesizego.com/blog/10-years-functional-options-golang)
- [Prometheus: Instrumenting a Go Application](https://prometheus.io/docs/guides/go-application/)
- [promauto Package Documentation](https://pkg.go.dev/github.com/prometheus/client_golang/prometheus/promauto)
- [Prometheus Metric Naming Best Practices](https://prometheus.io/docs/practices/naming/)
- [Go slog Package Documentation](https://pkg.go.dev/log/slog)
- [Logging in Go with Slog: A Practitioner's Guide](https://www.dash0.com/guides/logging-in-go-with-slog)
