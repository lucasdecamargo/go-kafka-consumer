// Package pollloop contains the poll loop implementation and related types.
package pollloop

// DegradedReason is a bitmask that tracks why the service is in degraded mode.
// Multiple reasons can be active simultaneously. The service exits degraded
// mode only when all reasons are cleared.
//
// See ADR-0006 (broker unavailability) and ADR-0007 (target unavailability)
// for the unified degraded mode design.
type DegradedReason uint8

const (
	// BrokerUnavailable indicates the Kafka broker is unreachable.
	// Set when consecutive CommitOffsets() failures exceed the configured
	// threshold. Cleared on successful CommitOffsets().
	BrokerUnavailable DegradedReason = 1 << iota

	// TargetUnavailable indicates the target service is unreachable.
	// Set when the Dispatcher's circuit breaker transitions to Open.
	// Cleared when the circuit breaker transitions back to Closed.
	TargetUnavailable
)

// DegradedState tracks the current set of active degraded reasons.
type DegradedState struct {
	reasons DegradedReason
}

// Set adds a degraded reason to the current state.
func (d *DegradedState) Set(reason DegradedReason) {
	d.reasons |= reason
}

// Clear removes a degraded reason from the current state.
func (d *DegradedState) Clear(reason DegradedReason) {
	d.reasons &^= reason
}

// IsDegraded reports whether any degraded reason is currently active.
func (d *DegradedState) IsDegraded() bool {
	return d.reasons != 0
}

// Has reports whether a specific degraded reason is currently active.
func (d *DegradedState) Has(reason DegradedReason) bool {
	return d.reasons&reason != 0
}

// Reasons returns the current bitmask of active degraded reasons.
func (d *DegradedState) Reasons() DegradedReason {
	return d.reasons
}

// String returns a human-readable representation of all active reasons.
func (r DegradedReason) String() string {
	if r == 0 {
		return "none"
	}

	var s string
	if r&BrokerUnavailable != 0 {
		s += "broker-unavailable"
	}
	if r&TargetUnavailable != 0 {
		if s != "" {
			s += ","
		}
		s += "target-unavailable"
	}
	return s
}
