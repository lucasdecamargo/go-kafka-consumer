package pollloop

import (
	"log/slog"

	"github.com/lucasdecamargo/go-kafka-consumer/internal/metrics"
)

// Option configures optional dependencies for the poll loop.
type Option func(*options)

type options struct {
	logger  *slog.Logger
	metrics *metrics.PollLoopMetrics
}

func defaultOptions() options {
	return options{
		logger: slog.Default(),
	}
}

// WithLogger sets the structured logger for the poll loop.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		o.logger = l
	}
}

// WithMetrics sets the poll loop metrics struct. Created via
// metrics.NewPollLoopMetrics(reg).
func WithMetrics(m *metrics.PollLoopMetrics) Option {
	return func(o *options) {
		o.metrics = m
	}
}
