package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// --- Test Helpers ---

type mockHealth struct {
	live  bool
	ready bool
}

func (m *mockHealth) IsLive() bool  { return m.live }
func (m *mockHealth) IsReady() bool { return m.ready }

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestServer(health HealthChecker) *Server {
	reg := prometheus.NewRegistry()
	return New(Config{Addr: ":0"}, health, reg, silentLogger())
}

func serveRequest(t *testing.T, srv *Server, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	w := httptest.NewRecorder()
	srv.srv.Handler.ServeHTTP(w, req)
	return w
}

// --- /healthz Tests ---

func TestHealthz_Live(t *testing.T) {
	srv := newTestServer(&mockHealth{live: true, ready: true})
	w := serveRequest(t, srv, http.MethodGet, "/healthz")

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusOK)
	}
	if body := w.Body.String(); body != "ok" {
		t.Errorf("body: got %q, want %q", body, "ok")
	}
}

func TestHealthz_NotLive(t *testing.T) {
	srv := newTestServer(&mockHealth{live: false, ready: false})
	w := serveRequest(t, srv, http.MethodGet, "/healthz")

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
	if body := w.Body.String(); body != "not live" {
		t.Errorf("body: got %q, want %q", body, "not live")
	}
}

// --- /readyz Tests ---

func TestReadyz_Ready(t *testing.T) {
	srv := newTestServer(&mockHealth{live: true, ready: true})
	w := serveRequest(t, srv, http.MethodGet, "/readyz")

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusOK)
	}
	if body := w.Body.String(); body != "ok" {
		t.Errorf("body: got %q, want %q", body, "ok")
	}
}

func TestReadyz_NotReady(t *testing.T) {
	srv := newTestServer(&mockHealth{live: true, ready: false})
	w := serveRequest(t, srv, http.MethodGet, "/readyz")

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
	if body := w.Body.String(); body != "not ready" {
		t.Errorf("body: got %q, want %q", body, "not ready")
	}
}

// --- /metrics Tests ---

func TestMetrics_Endpoint(t *testing.T) {
	reg := prometheus.NewRegistry()

	// Register a test counter so we can verify it appears.
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_total",
		Help: "A test counter.",
	})
	reg.MustRegister(counter)
	counter.Inc()

	srv := New(Config{Addr: ":0"}, &mockHealth{live: true, ready: true}, reg, silentLogger())
	w := serveRequest(t, srv, http.MethodGet, "/metrics")

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusOK)
	}

	body := w.Body.String()
	if !strings.Contains(body, "test_total") {
		t.Errorf("metrics body should contain test_total, got:\n%s", body)
	}
}

// --- State Transitions ---

func TestHealthz_TransitionsToNotLive(t *testing.T) {
	health := &mockHealth{live: true, ready: true}
	srv := newTestServer(health)

	// Initially live.
	w := serveRequest(t, srv, http.MethodGet, "/healthz")
	if w.Code != http.StatusOK {
		t.Fatalf("initial: got %d, want 200", w.Code)
	}

	// Transition to not live.
	health.live = false
	w = serveRequest(t, srv, http.MethodGet, "/healthz")
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("after transition: got %d, want 503", w.Code)
	}
}

func TestReadyz_TransitionsDuringDegradedMode(t *testing.T) {
	health := &mockHealth{live: true, ready: true}
	srv := newTestServer(health)

	// Initially ready.
	w := serveRequest(t, srv, http.MethodGet, "/readyz")
	if w.Code != http.StatusOK {
		t.Fatalf("initial: got %d, want 200", w.Code)
	}

	// Enter degraded mode.
	health.ready = false
	w = serveRequest(t, srv, http.MethodGet, "/readyz")
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("degraded: got %d, want 503", w.Code)
	}

	// Exit degraded mode.
	health.ready = true
	w = serveRequest(t, srv, http.MethodGet, "/readyz")
	if w.Code != http.StatusOK {
		t.Errorf("recovered: got %d, want 200", w.Code)
	}
}

// --- ListenAndServe / Shutdown ---

func TestServer_ListenAndShutdown(t *testing.T) {
	srv := newTestServer(&mockHealth{live: true, ready: true})

	// Override addr to use a random available port.
	srv.srv.Addr = "127.0.0.1:0"

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	// Give server time to start.
	time.Sleep(50 * time.Millisecond)

	// Graceful shutdown.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// ListenAndServe should return without error (ErrServerClosed is swallowed).
	if err := <-errCh; err != nil {
		t.Errorf("ListenAndServe: got %v, want nil", err)
	}
}
