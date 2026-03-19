package consumer

import "github.com/lucasdecamargo/go-kafka-consumer/internal/types"

// Header represents a single Kafka message header as a key-value pair.
// Headers carry metadata such as tracing IDs, content types, schema versions,
// and routing information alongside the message payload.
type Header = types.Header

// Message represents a single Kafka message consumed from a topic.
// This is the primary data type the developer interacts with inside a
// BatchProcessor. It contains all the information needed to process
// the message without direct access to the Kafka client.
//
// Example:
//
//	processor := func(ctx context.Context, batch []consumer.Message) error {
//	    for _, msg := range batch {
//	        log.Printf("topic=%s partition=%d offset=%d key=%s",
//	            msg.Topic, msg.Partition, msg.Offset, msg.Key)
//	    }
//	    return nil
//	}
type Message = types.Message
