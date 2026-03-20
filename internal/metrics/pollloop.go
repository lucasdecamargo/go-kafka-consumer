package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Poll loop metric name constants.
const (
	MessagesPolledTotal = "kafka_consumer_messages_polled_total"
	PollErrorsTotal     = "kafka_consumer_poll_errors_total"
	CommitsTotal        = "kafka_consumer_commits_total"
	CommitFailuresTotal = "kafka_consumer_commit_failures_total"
	DegradedMode        = "kafka_consumer_degraded_mode"
	PartitionsAssigned  = "kafka_consumer_partitions_assigned"
	RebalancesTotal     = "kafka_consumer_rebalances_total"
	LastCommittedOffset = "kafka_consumer_last_committed_offset"
	PartitionsPaused    = "kafka_consumer_partitions_paused"
)

// PollLoopMetrics holds all Prometheus metrics for the poll loop component.
type PollLoopMetrics struct {
	// MessagesPolled counts total messages received from Kafka, per partition.
	MessagesPolled *prometheus.CounterVec

	// PollErrors counts total Poll() errors (global, not per-partition).
	PollErrors prometheus.Counter

	// Commits counts successful offset commits, per partition.
	Commits *prometheus.CounterVec

	// CommitFailures counts failed offset commits (global — bulk commits
	// succeed or fail atomically, so partition attribution is not meaningful).
	CommitFailures prometheus.Counter

	// DegradedMode indicates whether the poll loop is in degraded mode
	// (1=degraded, 0=normal).
	DegradedMode prometheus.Gauge

	// PartitionsAssigned tracks the number of partitions currently assigned
	// to this consumer instance.
	PartitionsAssigned prometheus.Gauge

	// Rebalances counts total rebalance events (incremented on each revoke).
	Rebalances prometheus.Counter

	// LastCommittedOffset tracks the last successfully committed offset per
	// partition. A flat value while MessagesPolled grows indicates a stuck
	// partition.
	LastCommittedOffset *prometheus.GaugeVec

	// PartitionsPaused tracks the number of partitions currently paused due
	// to backpressure or degraded mode.
	PartitionsPaused prometheus.Gauge
}

// NewPollLoopMetrics registers and returns all poll loop metrics against the
// given registerer.
func NewPollLoopMetrics(reg prometheus.Registerer) *PollLoopMetrics {
	return &PollLoopMetrics{
		MessagesPolled: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: MessagesPolledTotal,
			Help: "Total messages received from Kafka, per partition.",
		}, []string{"partition"}),

		PollErrors: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: PollErrorsTotal,
			Help: "Total Poll() errors.",
		}),

		Commits: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: CommitsTotal,
			Help: "Total successful offset commits, per partition.",
		}, []string{"partition"}),

		CommitFailures: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: CommitFailuresTotal,
			Help: "Total failed offset commits.",
		}),

		DegradedMode: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: DegradedMode,
			Help: "Whether the poll loop is in degraded mode (1=degraded, 0=normal).",
		}),

		PartitionsAssigned: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: PartitionsAssigned,
			Help: "Number of partitions currently assigned to this consumer instance.",
		}),

		Rebalances: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: RebalancesTotal,
			Help: "Total rebalance events (incremented on each revoke).",
		}),

		LastCommittedOffset: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: LastCommittedOffset,
			Help: "Last successfully committed offset per partition.",
		}, []string{"partition"}),

		PartitionsPaused: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: PartitionsPaused,
			Help: "Number of partitions currently paused due to backpressure or degraded mode.",
		}),
	}
}
