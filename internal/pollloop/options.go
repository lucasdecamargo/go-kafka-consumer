package pollloop

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// Option configures optional dependencies for the poll loop.
type Option func(*options)

type options struct {
	logger     *slog.Logger
	registerer prometheus.Registerer
}

func defaultOptions() options {
	return options{
		logger:     slog.Default(),
		registerer: prometheus.DefaultRegisterer,
	}
}

// WithLogger sets the structured logger for the poll loop.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		o.logger = l
	}
}

// WithMetrics sets the Prometheus registerer for the poll loop.
func WithMetrics(reg prometheus.Registerer) Option {
	return func(o *options) {
		o.registerer = reg
	}
}
