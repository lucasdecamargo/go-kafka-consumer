package pollloop

import "sync"

// Health exposes the readiness and liveness state of the poll loop.
// Thread-safe: the poll loop goroutine writes, and the HTTP health
// handler reads concurrently.
//
//   - Liveness: true while the poll loop goroutine is running. Prevents
//     Kubernetes from restarting a healthy process.
//   - Readiness: true when the service is not degraded AND at least one
//     partition is currently assigned. This ensures /readyz returns 503
//     during startup (before the first rebalance) and after all
//     partitions are revoked (scale-down scenario).
type Health struct {
	mu                 sync.RWMutex
	live               bool
	ready              bool
	partitionsAssigned bool
}

// SetLive marks the poll loop as alive.
func (h *Health) SetLive(live bool) {
	h.mu.Lock()
	h.live = live
	h.mu.Unlock()
}

// SetReady marks the service as ready (or not) for traffic.
// Ready is a necessary but not sufficient condition for /readyz to
// return 200 — the consumer must also have at least one assigned
// partition (see SetPartitionsAssigned).
func (h *Health) SetReady(ready bool) {
	h.mu.Lock()
	h.ready = ready
	h.mu.Unlock()
}

// SetPartitionsAssigned records whether at least one partition is
// currently assigned to this consumer instance. It is set to true on
// the first OnPartitionsAssigned callback and reset to false when all
// partitions are revoked (e.g., during scale-down or graceful shutdown).
func (h *Health) SetPartitionsAssigned(assigned bool) {
	h.mu.Lock()
	h.partitionsAssigned = assigned
	h.mu.Unlock()
}

// IsLive reports whether the poll loop goroutine is running.
func (h *Health) IsLive() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.live
}

// IsReady reports whether the service is ready to process messages.
// Returns true only when the poll loop is not in a degraded state AND
// at least one partition has been assigned by the broker.
func (h *Health) IsReady() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.ready && h.partitionsAssigned
}
