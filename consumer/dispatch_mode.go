package consumer

// DispatchMode defines the message ordering strategy used by the Dispatcher
// to route messages to workers. The mode is set per consumer instance and
// determines how concurrency and ordering guarantees interact.
//
// See ADR-0003 for the full architectural rationale.
type DispatchMode int

const (
	// Unordered dispatches messages to a shared worker pool with no ordering
	// guarantees. This is the default mode, optimized for maximum throughput
	// when message processing order does not matter.
	Unordered DispatchMode = iota

	// PartitionOrdered assigns a dedicated worker per partition, ensuring
	// messages within each partition are processed in order. Use this when
	// partition-level ordering is required (e.g., per-entity event streams).
	PartitionOrdered

	// KeyOrdered routes messages with the same key to the same worker using
	// consistent hashing. Use this when ordering must be preserved per
	// message key, regardless of partition assignment.
	KeyOrdered
)

// String returns the human-readable name of the dispatch mode.
func (m DispatchMode) String() string {
	switch m {
	case Unordered:
		return "unordered"
	case PartitionOrdered:
		return "partition-ordered"
	case KeyOrdered:
		return "key-ordered"
	default:
		return "unknown"
	}
}
