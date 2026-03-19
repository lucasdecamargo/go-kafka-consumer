package dispatcher

// Metric name constants for the dispatcher. All metrics follow the naming
// convention: kafka_consumer_dispatcher_<metric>_<unit>.
const (
	// MetricBatchesTotal counts the total number of batches processed,
	// labeled by outcome: success, non_retryable, retries_exhausted.
	MetricBatchesTotal = "kafka_consumer_dispatcher_batches_total"

	// MetricBatchLatencySeconds measures the end-to-end latency for
	// processing a single batch, from dispatch to completion.
	MetricBatchLatencySeconds = "kafka_consumer_dispatcher_batch_latency_seconds"

	// MetricRetriesTotal counts the total number of retry attempts across
	// all batches.
	MetricRetriesTotal = "kafka_consumer_dispatcher_retries_total"

	// MetricCircuitBreakerState is a gauge representing the current circuit
	// breaker state: 0=closed, 1=half-open, 2=open.
	MetricCircuitBreakerState = "kafka_consumer_dispatcher_circuit_breaker_state"

	// MetricWorkersActive is a gauge tracking the number of workers
	// currently processing a batch.
	MetricWorkersActive = "kafka_consumer_dispatcher_workers_active"

	// MetricDLQMessagesTotal counts the total number of messages sent to
	// the dead letter queue, labeled by reason: non_retryable, retries_exhausted.
	MetricDLQMessagesTotal = "kafka_consumer_dispatcher_dlq_messages_total"
)
