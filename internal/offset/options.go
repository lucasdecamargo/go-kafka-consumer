package offset

import "log/slog"

// coordinatorOptions holds optional dependencies for the Coordinator.
type coordinatorOptions struct {
	logger *slog.Logger
}

// defaultCoordinatorOptions returns options with sensible defaults.
func defaultCoordinatorOptions() coordinatorOptions {
	return coordinatorOptions{
		logger: slog.Default(),
	}
}

// CoordinatorOption configures optional dependencies for a Coordinator.
type CoordinatorOption func(*coordinatorOptions)

// WithLogger sets the structured logger for the OffsetCoordinator.
// Default: slog.Default().
func WithLogger(l *slog.Logger) CoordinatorOption {
	return func(o *coordinatorOptions) {
		if l != nil {
			o.logger = l
		}
	}
}
