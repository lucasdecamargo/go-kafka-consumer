package pollloop

import "time"

// Config holds the configuration for the poll loop.
// All fields are validated at startup; invalid values cause the
// constructor to return an error.
type Config struct {
	// PollInterval is the time between successive Poll() calls to the
	// Kafka consumer. Default: 100ms.
	PollInterval time.Duration

	// CommitInterval is the time between periodic offset commit attempts.
	// The poll loop calls OffsetCoordinator.Committable() and commits the
	// result to Kafka on each tick. Default: 5s.
	CommitInterval time.Duration

	// CommitFailureThreshold is the number of consecutive CommitOffsets()
	// failures before the poll loop enters degraded mode with the
	// BrokerUnavailable reason. Default: 3.
	CommitFailureThreshold int

	// ShutdownTimeout is the maximum time allowed for graceful shutdown.
	// Must be less than the Kubernetes terminationGracePeriodSeconds to
	// leave time for final offset commits and resource cleanup. Default: 25s.
	ShutdownTimeout time.Duration
}

// DefaultConfig returns a Config with sensible production defaults.
func DefaultConfig() Config {
	return Config{
		PollInterval:           100 * time.Millisecond,
		CommitInterval:         5 * time.Second,
		CommitFailureThreshold: 3,
		ShutdownTimeout:        25 * time.Second,
	}
}
