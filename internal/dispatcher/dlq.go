package dispatcher

import (
	"context"

	"github.com/lucasdecamargo/go-kafka-consumer/consumer"
)

// DLQProducer produces failed message batches to a dead letter queue topic.
// The actual implementation (using the Kafka client) is wired at startup.
// If no DLQProducer is provided to the Dispatcher, failed batches are
// logged and dropped after retries exhaust.
type DLQProducer interface {
	// Produce sends the failed batch to the DLQ topic with error metadata.
	// The reason describes why the batch was routed to the DLQ (e.g., the
	// original processing error). Blocks until the DLQ produce is acknowledged.
	Produce(ctx context.Context, msgs []consumer.Message, reason error) error
}
