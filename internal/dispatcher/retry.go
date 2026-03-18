package dispatcher

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

const (
	// retryBaseDelay is the base delay for exponential backoff.
	retryBaseDelay = 100 * time.Millisecond

	// retryMaxDelay caps the backoff to prevent excessively long waits.
	retryMaxDelay = 30 * time.Second

	// jitterFactor controls the random jitter added to the backoff.
	// A value of 0.5 means up to 50% additional delay.
	jitterFactor = 0.5
)

// backoff calculates the delay for a given retry attempt using exponential
// backoff with jitter. The delay is: base * 2^attempt + random jitter,
// capped at retryMaxDelay.
func backoff(attempt int) time.Duration {
	delay := float64(retryBaseDelay) * math.Pow(2, float64(attempt))
	if delay > float64(retryMaxDelay) {
		delay = float64(retryMaxDelay)
	}

	// Add jitter: up to jitterFactor * delay.
	jitter := delay * jitterFactor * rand.Float64()
	delay += jitter

	return time.Duration(delay)
}

// sleepWithContext sleeps for the given duration, but returns early if the
// context is canceled. Returns the context error if canceled, nil otherwise.
func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
