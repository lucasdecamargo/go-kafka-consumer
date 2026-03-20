package pollloop

import (
	"github.com/lucasdecamargo/go-kafka-consumer/internal/dispatcher"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/types"
)

// KafkaConsumer abstracts the Kafka client for the poll loop. This
// interface is the seam that allows unit testing with mocks, isolating
// poll loop logic from the real Kafka client (confluent-kafka-go).
//
// All methods are called from the single poll loop goroutine.
// Implementations must not be assumed thread-safe.
type KafkaConsumer interface {
	// Poll retrieves messages from Kafka. Returns nil when no messages
	// are available (e.g., all partitions paused, low-traffic topic).
	// The timeout parameter controls how long the call blocks.
	Poll(timeoutMs int) ([]types.Message, error)

	// CommitOffsets commits the given partition offsets to Kafka
	// synchronously. Returns an error if the broker is unreachable.
	CommitOffsets(offsets map[int32]int64) error

	// Pause stops message delivery for the specified partitions.
	// Paused partitions still participate in consumer group heartbeats
	// via Poll(), but return no messages.
	Pause(partitions []int32) error

	// Resume restarts message delivery for previously paused partitions.
	Resume(partitions []int32) error

	// Assignment returns the set of partitions currently assigned to
	// this consumer. Used to know which partitions to pause/resume.
	Assignment() ([]dispatcher.Partition, error)

	// Close closes the Kafka consumer, leaving the consumer group.
	// Must be called during shutdown after all offset commits are done.
	Close() error
}

// RebalanceHandler is implemented by the poll loop and invoked by the
// Kafka client when partition assignments change during a consumer
// group rebalance. The Kafka adapter (Phase 5) calls these methods
// from within the rebalance callback triggered by Poll().
type RebalanceHandler interface {
	// OnPartitionsAssigned is called when new partitions are assigned.
	OnPartitionsAssigned(partitions []dispatcher.Partition)

	// OnPartitionsRevoked is called when partitions are being revoked.
	// The implementation MUST drain in-flight work, commit offsets for
	// revoked partitions, and reset the OffsetCoordinator before
	// returning.
	OnPartitionsRevoked(partitions []dispatcher.Partition)
}
