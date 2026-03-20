package dispatcher

import (
	"log/slog"

	"github.com/lucasdecamargo/go-kafka-consumer/internal/metrics"
)

// unorderedOptions holds optional dependencies for the UnorderedDispatcher.
type unorderedOptions struct {
	logger      *slog.Logger
	metrics     *metrics.DispatcherMetrics
	dlqProducer DLQProducer
}

// defaultUnorderedOptions returns options with sensible defaults.
func defaultUnorderedOptions() unorderedOptions {
	return unorderedOptions{
		logger: slog.Default(),
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

// WithMetrics sets the dispatcher metrics struct. Created via
// metrics.NewDispatcherMetrics(reg).
func WithMetrics(m *metrics.DispatcherMetrics) UnorderedOption {
	return func(o *unorderedOptions) {
		o.metrics = m
	}
}

// WithDLQProducer sets the dead letter queue producer. If not provided,
// failed batches are logged and dropped after retries exhaust.
func WithDLQProducer(dlq DLQProducer) UnorderedOption {
	return func(o *unorderedOptions) {
		o.dlqProducer = dlq
	}
}
