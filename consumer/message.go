package consumer

import "time"

// Header represents a single Kafka message header as a key-value pair.
// Headers carry metadata such as tracing IDs, content types, schema versions,
// and routing information alongside the message payload.
type Header struct {
	Key   string
	Value []byte
}

// Message represents a single Kafka message consumed from a topic.
// This is the primary data type the developer interacts with inside a
// BatchProcessor. It contains all the information needed to process
// the message without direct access to the Kafka client.
type Message struct {
	// Topic is the Kafka topic this message was consumed from.
	Topic string

	// Partition is the partition number within the topic.
	Partition int32

	// Offset is the message's position within the partition.
	Offset int64

	// Key is the message key (may be nil). Used for partitioning
	// and key-ordered dispatch mode.
	Key []byte

	// Value is the message payload (may be nil).
	Value []byte

	// Timestamp is the message timestamp set by the producer or broker.
	Timestamp time.Time

	// Headers contains optional key-value metadata attached to the message.
	Headers []Header
}
