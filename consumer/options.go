package consumer

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// options holds the optional dependencies for a Consumer instance.
// These are configured via functional options passed to New().
type options struct {
	logger     *slog.Logger
	registerer prometheus.Registerer
}

// defaultOptions returns options with sensible defaults.
func defaultOptions() options {
	return options{
		logger:     slog.Default(),
		registerer: prometheus.DefaultRegisterer,
	}
}

// Option configures optional dependencies for a Consumer.
// Use the With* functions to create Options.
type Option func(*options)

// WithLogger sets the structured logger for the consumer and all its
// internal components. Default: slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithMetrics sets the Prometheus registerer used by all internal
// components to register their metrics. Default: prometheus.DefaultRegisterer.
func WithMetrics(reg prometheus.Registerer) Option {
	return func(o *options) {
		if reg != nil {
			o.registerer = reg
		}
	}
}
