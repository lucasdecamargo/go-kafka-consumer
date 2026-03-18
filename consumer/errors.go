package consumer

import (
	"errors"
	"fmt"
)

// ErrNonRetryable wraps an error to classify it as non-retryable.
// When a BatchProcessor returns an error wrapped with ErrNonRetryable,
// the framework skips retries and routes the batch directly to the DLQ.
// This error is not reported to the circuit breaker.
//
// Use this for errors that will never succeed on retry, such as:
//   - Schema validation failures
//   - Malformed message payloads
//   - Business rule violations
//
// All other errors are treated as transient by default and will be
// retried with exponential backoff.
//
// Example:
//
//	if err := validate(msg); err != nil {
//	    return &consumer.ErrNonRetryable{Err: fmt.Errorf("validation failed: %w", err)}
//	}
type ErrNonRetryable struct {
	Err error
}

// Error returns the string representation of the wrapped error.
func (e *ErrNonRetryable) Error() string {
	return fmt.Sprintf("non-retryable: %v", e.Err)
}

// Unwrap returns the underlying error for use with errors.Is and errors.As.
func (e *ErrNonRetryable) Unwrap() error {
	return e.Err
}

// IsNonRetryable reports whether err or any error in its chain is an
// ErrNonRetryable. This is a convenience wrapper around errors.As.
func IsNonRetryable(err error) bool {
	var target *ErrNonRetryable
	return errors.As(err, &target)
}
