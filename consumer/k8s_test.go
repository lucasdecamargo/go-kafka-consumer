package consumer

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Logging helpers
// ---------------------------------------------------------------------------

// logRecord captures a single slog entry for assertion.
type logRecord struct {
	Level   slog.Level
	Message string
	Attrs   map[string]any
}

// capturingHandler is a slog.Handler that accumulates all log records.
type capturingHandler struct {
	records []logRecord
}

func (h *capturingHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	rec := logRecord{
		Level:   r.Level,
		Message: r.Message,
		Attrs:   make(map[string]any),
	}
	r.Attrs(func(a slog.Attr) bool {
		rec.Attrs[a.Key] = a.Value.Any()
		return true
	})
	h.records = append(h.records, rec)
	return nil
}

func (h *capturingHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(_ string) slog.Handler      { return h }

func (h *capturingHandler) warnCount() int {
	n := 0
	for _, r := range h.records {
		if r.Level == slog.LevelWarn {
			n++
		}
	}
	return n
}

func (h *capturingHandler) firstWarn() (logRecord, bool) {
	for _, r := range h.records {
		if r.Level == slog.LevelWarn {
			return r, true
		}
	}
	return logRecord{}, false
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// consumerWithCfg builds a bare Consumer suitable for testing
// warnKubernetesGracePeriod without making any network connections.
func consumerWithCfg(cfg Config) *Consumer {
	return &Consumer{cfg: cfg}
}

func baseConfig() Config {
	cfg := DefaultConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Topics = []string{"test"}
	cfg.GroupID = "test-group"
	return cfg
}

// setEnv sets an env var and restores the original value (or unsets it) when
// the test finishes.
func setEnv(t *testing.T, key, value string) {
	t.Helper()
	prev, had := os.LookupEnv(key)
	if err := os.Setenv(key, value); err != nil {
		t.Fatalf("setenv %s: %v", key, err)
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(key, prev)
		} else {
			_ = os.Unsetenv(key)
		}
	})
}

// unsetEnv removes an env var for the duration of the test and restores it
// (or leaves it unset) on cleanup.
func unsetEnv(t *testing.T, key string) {
	t.Helper()
	prev, had := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("unsetenv %s: %v", key, err)
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(key, prev)
		}
	})
}

func intAttr(t *testing.T, rec logRecord, key string) int64 {
	t.Helper()
	v, ok := rec.Attrs[key]
	if !ok {
		t.Fatalf("expected attr %q in log record, attrs: %v", key, rec.Attrs)
	}
	n, ok := v.(int64)
	if !ok {
		t.Fatalf("attr %q: expected int64, got %T (%v)", key, v, v)
	}
	return n
}

// ---------------------------------------------------------------------------
// warnKubernetesGracePeriod tests
// ---------------------------------------------------------------------------

// TestWarnK8s_NotInKubernetes: KUBERNETES_SERVICE_HOST absent → silent.
func TestWarnK8s_NotInKubernetes(t *testing.T) {
	unsetEnv(t, kubernetesServiceHostEnv)
	setEnv(t, kubernetesGracePeriodEnv, "30")

	h := &capturingHandler{}
	consumerWithCfg(baseConfig()).warnKubernetesGracePeriod(slog.New(h))

	if h.warnCount() != 0 {
		t.Errorf("expected no warnings outside Kubernetes, got %d", h.warnCount())
	}
}

// TestWarnK8s_NoGracePeriodEnv: in Kubernetes but env var absent → Warn with
// recommendation carrying the correct minimum.
func TestWarnK8s_NoGracePeriodEnv(t *testing.T) {
	setEnv(t, kubernetesServiceHostEnv, "10.96.0.1")
	unsetEnv(t, kubernetesGracePeriodEnv)

	cfg := baseConfig()
	cfg.ShutdownTimeout = 25 * time.Second
	cfg.PreStopDelay = 5 * time.Second
	// Minimum: 25 + 5 + 10 = 40s.

	h := &capturingHandler{}
	consumerWithCfg(cfg).warnKubernetesGracePeriod(slog.New(h))

	if h.warnCount() != 1 {
		t.Fatalf("expected 1 warning, got %d", h.warnCount())
	}

	rec, _ := h.firstWarn()
	if got := intAttr(t, rec, "recommended_termination_grace_period_seconds"); got != 40 {
		t.Errorf("recommended seconds: got %d, want 40", got)
	}
}

// TestWarnK8s_GracePeriodTooSmall: env var set but below minimum → Warn with
// configured, minimum, and shortfall attributes.
func TestWarnK8s_GracePeriodTooSmall(t *testing.T) {
	setEnv(t, kubernetesServiceHostEnv, "10.96.0.1")
	setEnv(t, kubernetesGracePeriodEnv, "30") // 30s < 25+0+10 = 35s

	cfg := baseConfig()
	cfg.ShutdownTimeout = 25 * time.Second
	cfg.PreStopDelay = 0

	h := &capturingHandler{}
	consumerWithCfg(cfg).warnKubernetesGracePeriod(slog.New(h))

	if h.warnCount() != 1 {
		t.Fatalf("expected 1 warning, got %d", h.warnCount())
	}

	rec, _ := h.firstWarn()
	if got := intAttr(t, rec, "configured_termination_grace_period_seconds"); got != 30 {
		t.Errorf("configured seconds: got %d, want 30", got)
	}
	if got := intAttr(t, rec, "minimum_termination_grace_period_seconds"); got != 35 {
		t.Errorf("minimum seconds: got %d, want 35", got)
	}
	if got := intAttr(t, rec, "shortfall_seconds"); got != 5 {
		t.Errorf("shortfall seconds: got %d, want 5", got)
	}
}

// TestWarnK8s_GracePeriodSufficient: env var at or above minimum → silent.
func TestWarnK8s_GracePeriodSufficient(t *testing.T) {
	setEnv(t, kubernetesServiceHostEnv, "10.96.0.1")
	setEnv(t, kubernetesGracePeriodEnv, "60") // 60s >= 25+0+10 = 35s

	cfg := baseConfig()
	cfg.ShutdownTimeout = 25 * time.Second
	cfg.PreStopDelay = 0

	h := &capturingHandler{}
	consumerWithCfg(cfg).warnKubernetesGracePeriod(slog.New(h))

	if h.warnCount() != 0 {
		t.Errorf("expected no warnings when grace period is sufficient, got %d", h.warnCount())
	}
}

// TestWarnK8s_ExactlyAtMinimum: value exactly equal to minimum → silent
// (boundary: >= is acceptable).
func TestWarnK8s_ExactlyAtMinimum(t *testing.T) {
	setEnv(t, kubernetesServiceHostEnv, "10.96.0.1")
	setEnv(t, kubernetesGracePeriodEnv, "35") // exactly 25+0+10

	cfg := baseConfig()
	cfg.ShutdownTimeout = 25 * time.Second
	cfg.PreStopDelay = 0

	h := &capturingHandler{}
	consumerWithCfg(cfg).warnKubernetesGracePeriod(slog.New(h))

	if h.warnCount() != 0 {
		t.Errorf("expected no warning at exactly the minimum, got %d", h.warnCount())
	}
}

// TestWarnK8s_PreStopDelayAccountedFor: a grace period that would be fine
// without PreStopDelay becomes insufficient when PreStopDelay is set.
func TestWarnK8s_PreStopDelayAccountedFor(t *testing.T) {
	setEnv(t, kubernetesServiceHostEnv, "10.96.0.1")
	// 35s is fine for PreStopDelay=0 (25+0+10=35) but not for PreStopDelay=5s
	// (25+5+10=40).
	setEnv(t, kubernetesGracePeriodEnv, "35")

	cfg := baseConfig()
	cfg.ShutdownTimeout = 25 * time.Second
	cfg.PreStopDelay = 5 * time.Second

	h := &capturingHandler{}
	consumerWithCfg(cfg).warnKubernetesGracePeriod(slog.New(h))

	if h.warnCount() != 1 {
		t.Fatalf("expected 1 warning when PreStopDelay pushes minimum above configured, got %d", h.warnCount())
	}

	rec, _ := h.firstWarn()
	if got := intAttr(t, rec, "minimum_termination_grace_period_seconds"); got != 40 {
		t.Errorf("minimum with PreStopDelay=5s: got %d, want 40", got)
	}
}

// TestWarnK8s_MalformedGracePeriodEnv: malformed env var → exactly one Warn
// about the invalid value (not a size-mismatch false alarm).
func TestWarnK8s_MalformedGracePeriodEnv(t *testing.T) {
	setEnv(t, kubernetesServiceHostEnv, "10.96.0.1")
	setEnv(t, kubernetesGracePeriodEnv, "not-a-number")

	h := &capturingHandler{}
	consumerWithCfg(baseConfig()).warnKubernetesGracePeriod(slog.New(h))

	if h.warnCount() != 1 {
		t.Fatalf("expected 1 warning for malformed value, got %d", h.warnCount())
	}
	rec, _ := h.firstWarn()
	if _, ok := rec.Attrs["value"]; !ok {
		t.Errorf("expected attr 'value' in malformed-value warning, attrs: %v", rec.Attrs)
	}
}

// TestWarnK8s_ZeroGracePeriodEnv: "0" is not a valid positive integer and
// should be treated as malformed.
func TestWarnK8s_ZeroGracePeriodEnv(t *testing.T) {
	setEnv(t, kubernetesServiceHostEnv, "10.96.0.1")
	setEnv(t, kubernetesGracePeriodEnv, "0")

	h := &capturingHandler{}
	consumerWithCfg(baseConfig()).warnKubernetesGracePeriod(slog.New(h))

	// "0" fails the secs <= 0 guard → malformed warning.
	if h.warnCount() != 1 {
		t.Fatalf("expected 1 warning for value=0, got %d", h.warnCount())
	}
}

// ---------------------------------------------------------------------------
// Config.PreStopDelay validation tests
// ---------------------------------------------------------------------------

func TestConfig_PreStopDelay_Negative(t *testing.T) {
	cfg := baseConfig()
	cfg.PreStopDelay = -1 * time.Second
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for negative PreStopDelay")
	}
}

func TestConfig_PreStopDelay_Zero(t *testing.T) {
	cfg := baseConfig()
	cfg.PreStopDelay = 0
	if err := cfg.Validate(); err != nil {
		t.Errorf("expected no error for zero PreStopDelay, got: %v", err)
	}
}

func TestConfig_PreStopDelay_Positive(t *testing.T) {
	cfg := baseConfig()
	cfg.PreStopDelay = 5 * time.Second
	if err := cfg.Validate(); err != nil {
		t.Errorf("expected no error for positive PreStopDelay, got: %v", err)
	}
}
