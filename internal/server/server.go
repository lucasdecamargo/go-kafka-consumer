// Package server provides a lightweight HTTP server that exposes Kubernetes
// health probes and Prometheus metrics for the Kafka consumer framework.
//
// Endpoints:
//   - GET /healthz — liveness probe (200 if poll loop goroutine is running)
//   - GET /readyz  — readiness probe (200 if not degraded, 503 otherwise)
//   - GET /metrics — Prometheus metrics in exposition format
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HealthChecker provides the liveness and readiness state used by the
// health probe handlers. Implemented by pollloop.Health.
type HealthChecker interface {
	IsLive() bool
	IsReady() bool
}

// Config holds the configuration for the HTTP server.
type Config struct {
	// Addr is the TCP address to listen on (e.g., ":8080", "0.0.0.0:9090").
	// If empty, the server is not started.
	Addr string
}

// Server is a lightweight HTTP server that exposes health probes and
// Prometheus metrics. It is started by Consumer.Run() and shut down
// gracefully when the consumer context is canceled.
type Server struct {
	srv    *http.Server
	logger *slog.Logger
}

// New creates a new HTTP server with health and metrics endpoints.
// The server is not started until ListenAndServe() is called.
func New(cfg Config, health HealthChecker, gatherer prometheus.Gatherer, logger *slog.Logger) *Server {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", healthzHandler(health))
	mux.HandleFunc("GET /readyz", readyzHandler(health))
	mux.Handle("GET /metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))

	return &Server{
		srv: &http.Server{
			Addr:              cfg.Addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		logger: logger,
	}
}

// ListenAndServe starts the HTTP server. It blocks until the server
// is shut down or an error occurs. Returns http.ErrServerClosed on
// graceful shutdown.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("health server: listen: %w", err)
	}

	s.logger.Info("health server started",
		slog.String("addr", ln.Addr().String()),
	)

	if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("health server: serve: %w", err)
	}

	return nil
}

// Shutdown gracefully shuts down the server with the given context
// deadline. In-flight requests are given time to complete.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("health server shutting down")
	return s.srv.Shutdown(ctx)
}

// healthzHandler returns an HTTP handler for the liveness probe.
// Returns 200 OK if the poll loop goroutine is running, 503 otherwise.
func healthzHandler(health HealthChecker) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if health.IsLive() {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "ok")
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "not live")
	}
}

// readyzHandler returns an HTTP handler for the readiness probe.
// Returns 200 OK if the service is ready for traffic (not degraded),
// 503 otherwise.
func readyzHandler(health HealthChecker) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if health.IsReady() {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "ok")
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "not ready")
	}
}
