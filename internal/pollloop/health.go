package pollloop

import "sync"

// Health exposes the readiness and liveness state of the poll loop.
// Thread-safe: the poll loop goroutine writes, and the HTTP health
// handler reads concurrently.
//
// - Liveness: true while the poll loop goroutine is running. Prevents
//   Kubernetes from restarting a healthy process.
// - Readiness: true when the service can accept traffic. False when
//   degraded (broker unavailable, circuit breaker open).
type Health struct {
	mu    sync.RWMutex
	live  bool
	ready bool
}

// SetLive marks the poll loop as alive.
func (h *Health) SetLive(live bool) {
	h.mu.Lock()
	h.live = live
	h.mu.Unlock()
}

// SetReady marks the service as ready (or not) for traffic.
func (h *Health) SetReady(ready bool) {
	h.mu.Lock()
	h.ready = ready
	h.mu.Unlock()
}

// IsLive reports whether the poll loop goroutine is running.
func (h *Health) IsLive() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.live
}

// IsReady reports whether the service is ready to process messages.
func (h *Health) IsReady() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.ready
}
