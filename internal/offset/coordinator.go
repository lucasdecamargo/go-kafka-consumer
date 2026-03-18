// Package offset defines the OffsetCoordinator interface for safe offset
// commit tracking with per-partition batching.
//
// See ADR-0004 for the full architectural rationale.
package offset

// Coordinator defines the interface for tracking which offsets are safe
// to commit to Kafka. It implements per-partition batching with at most
// one in-flight batch per partition, eliminating the offset gap problem
// without bitmap tracking.
//
// Thread safety: implementations MUST be safe for concurrent use.
// The Dispatcher calls BatchDispatched and BatchComplete from worker
// goroutines, while the poll loop calls Committable and Reset from
// the poll goroutine.
type Coordinator interface {
	// BatchDispatched records that a batch has been dispatched to a worker
	// for the given partition. The maxOffset is the highest offset in the
	// batch (inclusive). At most one batch per partition may be in-flight
	// at any time.
	//
	// Called by: Dispatcher, after sending a batch to a worker.
	BatchDispatched(partition int32, maxOffset int64)

	// BatchComplete records that the in-flight batch for the given
	// partition has been fully processed (success, DLQ'd, or abandoned).
	// The partition is now eligible for a new batch dispatch.
	//
	// Called by: Dispatcher, after worker completes a batch.
	BatchComplete(partition int32)

	// Committable returns the offsets that are safe to commit to Kafka.
	// For each partition with a completed batch, it returns maxOffset+1
	// (the next offset to consume). Partitions with in-flight batches
	// are excluded.
	//
	// Called by: Poll loop, on the periodic commit timer, during shutdown,
	// and during partition revocation.
	Committable() map[int32]int64

	// Reset clears all tracking state for the given partition. Called
	// when a partition is revoked during a consumer group rebalance,
	// after final offsets have been committed.
	//
	// Called by: Poll loop, during rebalance (after committing offsets
	// for the revoked partition).
	Reset(partition int32)
}
