package consumer

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// kubernetesServiceHostEnv is injected by the kubelet into every pod and is
// the canonical way to detect whether the process is running inside a
// Kubernetes cluster.
const kubernetesServiceHostEnv = "KUBERNETES_SERVICE_HOST"

// kubernetesGracePeriodEnv is NOT injected by Kubernetes automatically.
// Users must mirror their pod's terminationGracePeriodSeconds into the
// container env block to enable startup validation:
//
//	env:
//	  - name: TERMINATION_GRACE_PERIOD_SECONDS
//	    value: "60"
const kubernetesGracePeriodEnv = "TERMINATION_GRACE_PERIOD_SECONDS"

// gracePeriodSafetyBuffer is the minimum headroom required between
// ShutdownTimeout and terminationGracePeriodSeconds. It accounts for
// SIGTERM propagation latency, cgroup freezer delays, and other kernel
// overhead between the kubelet sending the signal and the process
// receiving it.
const gracePeriodSafetyBuffer = 10 * time.Second

// warnKubernetesGracePeriod emits structured warning log lines at startup
// when the consumer is running inside Kubernetes and the configured
// terminationGracePeriodSeconds appears too small to allow a clean shutdown.
//
// The required relationship is:
//
//	terminationGracePeriodSeconds >= ShutdownTimeout + PreStopDelay + 10s
//
// Detection uses KUBERNETES_SERVICE_HOST (set by the kubelet in every pod).
// Validation uses TERMINATION_GRACE_PERIOD_SECONDS, which the user must
// set manually in the pod spec env block — Kubernetes does not inject it.
//
// Three outcomes:
//   - Not in Kubernetes → no-op (silent).
//   - In Kubernetes, env var absent → Warn with the recommended minimum.
//   - In Kubernetes, env var present but too small → Warn with exact diff.
//   - In Kubernetes, env var present and sufficient → no-op (silent).
func (c *Consumer) warnKubernetesGracePeriod(logger *slog.Logger) {
	if os.Getenv(kubernetesServiceHostEnv) == "" {
		return // Not running inside Kubernetes.
	}

	minimum := c.cfg.ShutdownTimeout + c.cfg.PreStopDelay + gracePeriodSafetyBuffer

	// Check whether the env var is present before parsing, so we can
	// distinguish "absent" (emit a recommendation) from "set but malformed"
	// (readGracePeriodEnv already logged the parse error — no extra noise).
	_, envPresent := os.LookupEnv(kubernetesGracePeriodEnv)

	configured, ok := readGracePeriodEnv(logger)
	if !ok {
		if !envPresent {
			// Running in Kubernetes but the validation env var is absent.
			// We cannot compare against the actual pod spec — emit a
			// recommendation so the operator knows what to set.
			logger.Warn("running in Kubernetes: set TERMINATION_GRACE_PERIOD_SECONDS in your pod env block to enable shutdown validation",
				slog.Int("recommended_termination_grace_period_seconds", int(minimum.Seconds())),
				slog.String("formula", "ShutdownTimeout + PreStopDelay + 10s"),
				slog.Duration("shutdown_timeout", c.cfg.ShutdownTimeout),
				slog.Duration("pre_stop_delay", c.cfg.PreStopDelay),
				slog.Duration("safety_buffer", gracePeriodSafetyBuffer),
			)
		}
		// If envPresent but !ok, the value was malformed; readGracePeriodEnv
		// already logged a warning — nothing more to do.
		return
	}

	if configured < minimum {
		logger.Warn("terminationGracePeriodSeconds is too small — pod may be force-killed before graceful shutdown completes",
			slog.Int("configured_termination_grace_period_seconds", int(configured.Seconds())),
			slog.Int("minimum_termination_grace_period_seconds", int(minimum.Seconds())),
			slog.Int("shortfall_seconds", int((minimum-configured).Seconds())),
			slog.Duration("shutdown_timeout", c.cfg.ShutdownTimeout),
			slog.Duration("pre_stop_delay", c.cfg.PreStopDelay),
			slog.Duration("safety_buffer", gracePeriodSafetyBuffer),
		)
	}
}

// readGracePeriodEnv reads TERMINATION_GRACE_PERIOD_SECONDS and parses it
// as a whole number of seconds. Returns (duration, true) on success,
// (0, false) if the variable is absent or malformed (with a warning log
// for the malformed case).
func readGracePeriodEnv(logger *slog.Logger) (time.Duration, bool) {
	raw, ok := os.LookupEnv(kubernetesGracePeriodEnv)
	if !ok {
		return 0, false
	}

	secs, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || secs <= 0 {
		logger.Warn("TERMINATION_GRACE_PERIOD_SECONDS is not a valid positive integer — skipping shutdown validation",
			slog.String("value", raw),
		)
		return 0, false
	}

	return time.Duration(secs) * time.Second, true
}
