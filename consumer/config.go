package consumer

import (
	"errors"
	"time"
)

// Config holds the configuration for a Consumer instance.
// This is the top-level configuration struct passed to New().
// All fields are validated by the constructor.
type Config struct {
	// Brokers is the list of Kafka broker addresses (host:port).
	// At least one broker must be specified.
	Brokers []string

	// Topics is the list of Kafka topics to subscribe to.
	// Supports exact names and wildcard patterns (e.g., "events.*").
	// At least one topic must be specified.
	Topics []string

	// GroupID is the Kafka consumer group ID. All consumer instances
	// with the same GroupID share partition assignments.
	GroupID string

	// DispatchMode selects the message ordering strategy.
	// Default: Unordered.
	DispatchMode DispatchMode

	// WorkerCount is the number of concurrent worker goroutines.
	// Default: 4.
	WorkerCount int

	// ChannelCap is the capacity of the internal dispatch channel.
	// Bounds the in-memory footprint. Default: 100.
	ChannelCap int

	// BatchSize is the maximum number of messages per batch.
	// Default: 50.
	BatchSize int

	// LingerTime is the maximum wait time for batch assembly.
	// Default: 100ms.
	LingerTime time.Duration

	// MaxRetries is the maximum retry attempts for a failed batch.
	// Default: 3.
	MaxRetries int

	// PollInterval is the time between Poll() calls. Default: 100ms.
	PollInterval time.Duration

	// CommitInterval is the time between offset commit attempts.
	// Default: 5s.
	CommitInterval time.Duration

	// CommitFailureThreshold is the number of consecutive commit
	// failures before entering degraded mode. Default: 3.
	CommitFailureThreshold int

	// ShutdownTimeout is the maximum time the consumer is given to drain
	// in-flight work, commit final offsets, and close the Kafka client
	// after the context is canceled.
	//
	// Kubernetes relationship — terminationGracePeriodSeconds must satisfy:
	//
	//   terminationGracePeriodSeconds >= ShutdownTimeout + PreStopDelay + 10s
	//
	// The 10s buffer accounts for SIGTERM propagation latency, cgroup
	// freezer delays, and other kernel overhead. If the pod is
	// force-killed before shutdown completes, in-flight messages may be
	// reprocessed and uncommitted offsets will be lost.
	//
	// Example: ShutdownTimeout=25s, PreStopDelay=5s → set
	// terminationGracePeriodSeconds to at least 40.
	//
	// Default: 25s.
	ShutdownTimeout time.Duration

	// PreStopDelay is the duration of the preStop hook configured in the
	// pod spec (if any). A preStop sleep gives Kubernetes time to remove
	// the pod from Service endpoints before SIGTERM is sent, preventing
	// new requests from being routed to a terminating pod.
	//
	// Set this to match the sleep duration in your pod's lifecycle.preStop
	// hook. The value is used only to compute the minimum required
	// terminationGracePeriodSeconds and to emit a startup warning when
	// running in Kubernetes. It does not affect shutdown behaviour.
	//
	// Default: 0 (no preStop hook).
	PreStopDelay time.Duration

	// Circuit breaker configuration.

	// CBMinRequests is the minimum requests before the circuit breaker
	// evaluates the failure rate. Default: 3.
	CBMinRequests int

	// CBFailureThreshold is the failure rate (0.0–1.0) that triggers
	// the circuit breaker to open. Default: 0.6.
	CBFailureThreshold float64

	// CBOpenTimeout is how long the circuit stays open before probing.
	// Default: 30s.
	CBOpenTimeout time.Duration

	// CBMaxRequests is the number of probe requests in half-open state.
	// Default: 1.
	CBMaxRequests int

	// CBInterval is the circuit breaker's sliding window duration.
	// Default: 60s.
	CBInterval time.Duration

	// DLQTopic is the Kafka topic name for the Dead Letter Queue.
	// When set, messages that fail processing (non-retryable errors or
	// retries exhausted) are published to this topic with error metadata
	// as headers. If empty, failed batches are logged and dropped.
	// See FR-4.
	DLQTopic string

	// LagReportInterval controls how often librdkafka emits internal
	// statistics used to update the kafka_consumer_lag Prometheus gauge.
	// Lower values increase metric resolution at the cost of slightly more
	// JSON parsing overhead. Default: 10s.
	LagReportInterval time.Duration

	// HealthAddr is the TCP address for the health and metrics HTTP server
	// (e.g., ":8080", "0.0.0.0:9090"). If empty, the server is not started.
	// Exposes /healthz, /readyz, and /metrics endpoints.
	// Default: ":8080".
	HealthAddr string

	// Security holds TLS and SASL authentication settings for Kafka
	// broker connections. The zero value uses plaintext without
	// authentication (suitable for development only). See NFR-7.1.
	Security SecurityConfig
}

// DefaultConfig returns a Config with sensible production defaults.
// The caller must still set Brokers, Topics, and GroupID.
func DefaultConfig() Config {
	return Config{
		DispatchMode:           Unordered,
		WorkerCount:            4,
		ChannelCap:             100,
		BatchSize:              50,
		LingerTime:             100 * time.Millisecond,
		MaxRetries:             3,
		PollInterval:           100 * time.Millisecond,
		CommitInterval:         5 * time.Second,
		CommitFailureThreshold: 3,
		ShutdownTimeout:        25 * time.Second,
		CBMinRequests:          3,
		CBFailureThreshold:     0.6,
		CBOpenTimeout:          30 * time.Second,
		CBMaxRequests:          1,
		CBInterval:             60 * time.Second,
		LagReportInterval:      10 * time.Second,
		HealthAddr:             ":8080",
	}
}

// Validate checks that all required fields are set and all values are
// within acceptable ranges. Returns an error describing the first
// invalid field found.
func (c *Config) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("config: at least one broker address is required")
	}
	if len(c.Topics) == 0 {
		return errors.New("config: at least one topic is required")
	}
	if c.GroupID == "" {
		return errors.New("config: group ID is required")
	}
	if c.WorkerCount <= 0 {
		return errors.New("config: worker count must be positive")
	}
	if c.ChannelCap <= 0 {
		return errors.New("config: channel capacity must be positive")
	}
	if c.BatchSize <= 0 {
		return errors.New("config: batch size must be positive")
	}
	if c.LingerTime <= 0 {
		return errors.New("config: linger time must be positive")
	}
	if c.MaxRetries < 0 {
		return errors.New("config: max retries must be non-negative")
	}
	if c.PollInterval <= 0 {
		return errors.New("config: poll interval must be positive")
	}
	if c.CommitInterval <= 0 {
		return errors.New("config: commit interval must be positive")
	}
	if c.CommitFailureThreshold <= 0 {
		return errors.New("config: commit failure threshold must be positive")
	}
	if c.ShutdownTimeout <= 0 {
		return errors.New("config: shutdown timeout must be positive")
	}
	if c.PreStopDelay < 0 {
		return errors.New("config: pre-stop delay must not be negative")
	}
	if c.CBFailureThreshold <= 0 || c.CBFailureThreshold > 1 {
		return errors.New("config: circuit breaker failure threshold must be between 0 and 1 (exclusive/inclusive)")
	}
	if c.CBOpenTimeout <= 0 {
		return errors.New("config: circuit breaker open timeout must be positive")
	}
	if c.CBMinRequests <= 0 {
		return errors.New("config: circuit breaker min requests must be positive")
	}
	if c.CBMaxRequests <= 0 {
		return errors.New("config: circuit breaker max requests must be positive")
	}
	if c.CBInterval <= 0 {
		return errors.New("config: circuit breaker interval must be positive")
	}
	if c.LagReportInterval <= 0 {
		return errors.New("config: lag report interval must be positive")
	}
	if err := c.Security.Validate(); err != nil {
		return err
	}
	return nil
}
