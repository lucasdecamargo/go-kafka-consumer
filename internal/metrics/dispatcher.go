package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Dispatcher metric name constants.
const (
	DispatcherMessagesProcessedTotal = "kafka_consumer_dispatcher_messages_processed_total"
	DispatcherMessageDelaySeconds    = "kafka_consumer_dispatcher_message_delay_seconds"
	DispatcherProcessingTimeSeconds  = "kafka_consumer_dispatcher_processing_time_seconds"
	DispatcherRecordAgeSeconds       = "kafka_consumer_dispatcher_record_age_seconds"
	DispatcherInflightMessages       = "kafka_consumer_dispatcher_inflight_messages"
	DispatcherCircuitBreakerState    = "kafka_consumer_dispatcher_circuit_breaker_state"
	DispatcherWorkersActive          = "kafka_consumer_dispatcher_workers_active"
	DispatcherDLQMessagesTotal       = "kafka_consumer_dispatcher_dlq_messages_total"
)

// Dispatcher metric label values.
const (
	StatusSuccess          = "success"
	StatusNonRetryable     = "non_retryable"
	StatusRetriesExhausted = "retries_exhausted"
)

// DispatcherMetrics holds all Prometheus metrics for the dispatcher component.
type DispatcherMetrics struct {
	// MessagesProcessed counts total messages processed, labeled by outcome
	// (success, non_retryable, retries_exhausted) and partition.
	MessagesProcessed *prometheus.CounterVec

	// MessageDelay measures the time from poll to processing completion per
	// message (time.Since(msg.PolledAt)). Captures queueing, batching,
	// channel wait, and processor execution time. Operator SLO metric.
	MessageDelay *prometheus.HistogramVec

	// ProcessingTime measures processor function execution time only
	// (time.Since(start) around processor(ctx, batch)). Developer tuning
	// metric — isolates target service latency from pipeline overhead.
	ProcessingTime *prometheus.HistogramVec

	// RecordAge measures end-to-end latency from message production to
	// processing completion (time.Now() - msg.Timestamp). The time-domain
	// equivalent of consumer lag.
	RecordAge *prometheus.HistogramVec

	// InflightMessages tracks messages currently buffered in worker channels
	// or being processed, per partition. Backpressure visibility.
	InflightMessages *prometheus.GaugeVec

	// CircuitBreakerState is the current circuit breaker state:
	// 0=closed, 1=half-open, 2=open.
	CircuitBreakerState prometheus.Gauge

	// WorkersActive tracks the number of workers currently processing a batch.
	WorkersActive prometheus.Gauge

	// DLQMessages counts messages sent to the dead letter queue, labeled by
	// reason (non_retryable, retries_exhausted) and partition.
	DLQMessages *prometheus.CounterVec
}

// NewDispatcherMetrics registers and returns all dispatcher metrics against
// the given registerer.
func NewDispatcherMetrics(reg prometheus.Registerer) *DispatcherMetrics {
	return &DispatcherMetrics{
		MessagesProcessed: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: DispatcherMessagesProcessedTotal,
			Help: "Total messages processed, labeled by outcome and partition.",
		}, []string{"status", "partition"}),

		MessageDelay: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    DispatcherMessageDelaySeconds,
			Help:    "Time from poll to processing completion per message.",
			Buckets: prometheus.DefBuckets,
		}, []string{"partition"}),

		ProcessingTime: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    DispatcherProcessingTimeSeconds,
			Help:    "Processor function execution time per batch.",
			Buckets: prometheus.DefBuckets,
		}, []string{"partition"}),

		RecordAge: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    DispatcherRecordAgeSeconds,
			Help:    "End-to-end latency from message production to processing completion.",
			Buckets: prometheus.DefBuckets,
		}, []string{"partition"}),

		InflightMessages: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: DispatcherInflightMessages,
			Help: "Messages currently buffered in worker channels or being processed, per partition.",
		}, []string{"partition"}),

		CircuitBreakerState: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: DispatcherCircuitBreakerState,
			Help: "Current circuit breaker state: 0=closed, 1=half-open, 2=open.",
		}),

		WorkersActive: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: DispatcherWorkersActive,
			Help: "Number of workers currently processing a batch.",
		}),

		DLQMessages: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: DispatcherDLQMessagesTotal,
			Help: "Total messages sent to the dead letter queue, labeled by reason and partition.",
		}, []string{"reason", "partition"}),
	}
}
