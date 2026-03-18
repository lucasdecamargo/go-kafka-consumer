// Package circuit provides the circuit breaker state type shared across
// internal components. The Dispatcher owns the circuit breaker; the poll
// loop reacts to state transitions via the Dispatcher's CircuitStateChanged
// channel.
//
// See ADR-0007 for the full architectural rationale.
package circuit

// State represents the current state of the circuit breaker.
type State int

const (
	// Closed is the normal operating state. All requests pass through
	// to the target service. Failures are tracked by the circuit breaker.
	Closed State = iota

	// Open indicates sustained failure. The circuit breaker blocks all
	// requests to the target service. The Dispatcher signals the poll
	// loop to enter degraded mode (TargetUnavailable).
	Open

	// HalfOpen is the recovery probing state. The circuit breaker allows
	// a limited number of probe requests through. If the probe succeeds,
	// the circuit transitions to Closed. If it fails, it returns to Open
	// with an increased backoff.
	HalfOpen
)

// String returns the human-readable name of the circuit state.
func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}
