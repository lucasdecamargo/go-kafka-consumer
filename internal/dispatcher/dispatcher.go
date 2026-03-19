// Package dispatcher defines the Dispatcher interface and related types.
// The Dispatcher is the central routing engine that receives messages from
// the poll loop, assembles them into batches, manages worker goroutines,
// and coordinates with the OffsetCoordinator.
//
// See ADR-0003 for the Dispatcher design and ADR-0007 for circuit breaker
// integration.
package dispatcher

import (
	"context"

	"github.com/lucasdecamargo/go-kafka-consumer/internal/circuit"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/types"
)

// Partition represents a Kafka partition assignment used in rebalance
// callbacks. It carries the topic and partition ID needed to manage
// per-partition state within the Dispatcher and OffsetCoordinator.
type Partition struct {
	Topic     string
	Partition int32
}

// Dispatcher defines the interface for pluggable message dispatching
// strategies. Each implementation provides a different ordering guarantee
// (unordered, partition-ordered, key-ordered) while sharing the same
// contract with the poll loop.
//
// The poll loop is the sole caller of this interface. Workers and the
// OffsetCoordinator are internal to the Dispatcher implementation.
type Dispatcher interface {
	// Start launches the Dispatcher's worker goroutines. Must be called
	// exactly once before Send(). The context controls worker lifecycle —
	// canceling it triggers graceful shutdown of all workers.
	//
	// The shutdown function is called by the Dispatcher when a fatal
	// condition is detected (e.g., worker panic). It cancels the shared
	// context that the poll loop also watches, propagating shutdown to
	// the entire consumer pipeline.
	//
	// Called by: Consumer.Run(), during startup.
	Start(ctx context.Context, shutdown context.CancelFunc)

	// Send delivers a group of messages from a single partition to the
	// Dispatcher for batch assembly and processing. Returns an error when
	// the Dispatcher is at capacity (backpressure signal), indicating the
	// poll loop should pause the partition.
	//
	// Called by: Poll loop (single goroutine).
	Send(ctx context.Context, partition int32, msgs []types.Message) error

	// Ready returns a channel that is signaled when the Dispatcher has
	// capacity to accept new messages after a backpressure event. The poll
	// loop selects on this channel to know when to resume paused partitions.
	//
	// Called by: Poll loop (single goroutine).
	Ready() <-chan struct{}

	// CircuitStateChanged returns a channel that emits the new circuit
	// breaker state on every transition (Closed→Open, Open→HalfOpen,
	// HalfOpen→Closed, HalfOpen→Open). The poll loop selects on this
	// channel to enter or exit degraded mode for TargetUnavailable.
	//
	// Called by: Poll loop (single goroutine).
	CircuitStateChanged() <-chan circuit.State

	// OnPartitionsAssigned notifies the Dispatcher that new partitions
	// have been assigned to this consumer. The Dispatcher creates any
	// per-partition state needed for the active dispatch mode.
	//
	// Called by: Poll loop (single goroutine), during rebalance callback.
	OnPartitionsAssigned(partitions []Partition)

	// OnPartitionsRevoked notifies the Dispatcher that partitions are
	// being revoked. The Dispatcher MUST drain all in-flight batches for
	// the revoked partitions before returning, so that the poll loop can
	// safely commit final offsets and reset the OffsetCoordinator.
	//
	// Called by: Poll loop (single goroutine), during rebalance callback.
	OnPartitionsRevoked(partitions []Partition)

	// Close initiates a graceful shutdown of the Dispatcher. It stops
	// accepting new messages, waits for in-flight workers to complete
	// (bounded by the context deadline), and releases all resources.
	//
	// If the context expires before workers finish, Close returns with
	// an error and remaining in-flight batches are abandoned.
	//
	// Called by: Poll loop (single goroutine), during shutdown.
	Close(ctx context.Context) error
}
