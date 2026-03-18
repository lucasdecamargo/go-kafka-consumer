package consumer

import "context"

// BatchProcessor is the function the developer implements to process
// a batch of Kafka messages. The framework handles all Kafka interaction,
// batching, offset management, retries, and DLQ routing. The developer
// only implements this function.
//
// The batch contains messages from a single partition, assembled by the
// Dispatcher based on batch size and/or linger time configuration.
//
// Return nil to indicate success — the batch offsets will be committed.
// Return an error to indicate failure — the framework retries with
// exponential backoff. Wrap with ErrNonRetryable to skip retries and
// route directly to the DLQ.
//
// Example:
//
//	processor := func(ctx context.Context, batch []consumer.Message) error {
//	    records := make([]db.Record, len(batch))
//	    for i, msg := range batch {
//	        r, err := parseRecord(msg.Value)
//	        if err != nil {
//	            return &consumer.ErrNonRetryable{Err: err}
//	        }
//	        records[i] = r
//	    }
//	    return db.BulkInsert(ctx, records)
//	}
type BatchProcessor func(ctx context.Context, batch []Message) error
