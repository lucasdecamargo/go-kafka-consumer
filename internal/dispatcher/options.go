package dispatcher

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// unorderedOptions holds optional dependencies for the UnorderedDispatcher.
type unorderedOptions struct {
	logger      *slog.Logger
	registerer  prometheus.Registerer
	dlqProducer DLQProducer
}

// defaultUnorderedOptions returns options with sensible defaults.
func defaultUnorderedOptions() unorderedOptions {
	return unorderedOptions{
		logger:     slog.Default(),
		registerer: prometheus.DefaultRegisterer,
	}
}

// UnorderedOption configures optional dependencies for the UnorderedDispatcher.
type UnorderedOption func(*unorderedOptions)

// WithLogger sets the structured logger. Default: slog.Default().
func WithLogger(l *slog.Logger) UnorderedOption {
	return func(o *unorderedOptions) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithMetrics sets the Prometheus registerer. Default: prometheus.DefaultRegisterer.
func WithMetrics(reg prometheus.Registerer) UnorderedOption {
	return func(o *unorderedOptions) {
		if reg != nil {
			o.registerer = reg
		}
	}
}

// WithDLQProducer sets the dead letter queue producer. If not provided,
// failed batches are logged and dropped after retries exhaust.
func WithDLQProducer(dlq DLQProducer) UnorderedOption {
	return func(o *unorderedOptions) {
		o.dlqProducer = dlq
	}
}
