package dispatcher

import "time"

// Config holds the configuration for a Dispatcher implementation.
// All fields are validated by the constructor; invalid values cause
// the constructor to return an error.
type Config struct {
	// WorkerCount is the number of concurrent worker goroutines that
	// process batches. Default: 4.
	WorkerCount int

	// ChannelCap is the capacity of the internal buffered channel used
	// to queue batches for workers. This bounds the in-memory footprint.
	// Default: 100.
	ChannelCap int

	// BatchSize is the maximum number of messages per batch. A batch is
	// dispatched to a worker when it reaches this size or when LingerTime
	// expires, whichever comes first. Default: 50.
	BatchSize int

	// LingerTime is the maximum time to wait for a batch to fill before
	// dispatching it to a worker, even if BatchSize has not been reached.
	// Default: 100ms.
	LingerTime time.Duration

	// MaxRetries is the maximum number of retry attempts for a failed
	// batch before routing it to the DLQ (if circuit breaker is closed).
	// Default: 3.
	MaxRetries int

	// CBMinRequests is the minimum number of requests in the circuit
	// breaker's sliding window before it evaluates the failure rate.
	// Default: 3.
	CBMinRequests int

	// CBFailureThreshold is the failure rate (0.0–1.0) above which the
	// circuit breaker transitions from Closed to Open. Default: 0.6.
	CBFailureThreshold float64

	// CBOpenTimeout is how long the circuit breaker stays in the Open
	// state before transitioning to HalfOpen for probing. Default: 30s.
	CBOpenTimeout time.Duration

	// CBMaxRequests is the number of probe requests allowed through in
	// the HalfOpen state. Default: 1.
	CBMaxRequests int

	// CBInterval is the duration of the circuit breaker's sliding window
	// for failure rate calculation. Default: 60s.
	CBInterval time.Duration
}

// DefaultConfig returns a Config with sensible production defaults.
func DefaultConfig() Config {
	return Config{
		WorkerCount:        4,
		ChannelCap:         100,
		BatchSize:          50,
		LingerTime:         100 * time.Millisecond,
		MaxRetries:         3,
		CBMinRequests:      3,
		CBFailureThreshold: 0.6,
		CBOpenTimeout:      30 * time.Second,
		CBMaxRequests:      1,
		CBInterval:         60 * time.Second,
	}
}
