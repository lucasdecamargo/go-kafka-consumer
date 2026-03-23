package metrics

import (
	"encoding/json"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ConsumerLag is the Prometheus metric name for the consumer lag gauge.
const ConsumerLag = "kafka_consumer_lag"

// LagMetrics holds the Prometheus gauge for per-partition consumer lag.
// It is updated by parsing librdkafka statistics events, which are emitted
// every StatisticsIntervalMs milliseconds through the Poll() event loop.
type LagMetrics struct {
	lag *prometheus.GaugeVec
}

// NewLagMetrics registers and returns the consumer lag gauge against the
// given registerer.
func NewLagMetrics(reg prometheus.Registerer) *LagMetrics {
	return &LagMetrics{
		lag: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: ConsumerLag,
			Help: "Current consumer lag per topic partition " +
				"(high-water mark minus committed offset). " +
				"Sourced from librdkafka statistics. " +
				"Primary signal for KEDA horizontal autoscaling.",
		}, []string{"group", "topic", "partition"}),
	}
}

// Update parses a librdkafka stats JSON payload and updates the lag gauge
// for every topic partition. Partitions with an unknown lag value (-1) are
// skipped — this occurs before the first fetch offset is established.
func (m *LagMetrics) Update(groupID, statsJSON string) {
	var s rdkafkaStats
	if err := json.Unmarshal([]byte(statsJSON), &s); err != nil {
		slog.Default().Warn("lag metrics: failed to parse rdkafka stats",
			slog.String("error", err.Error()),
		)
		return
	}

	for topic, t := range s.Topics {
		for partID, p := range t.Partitions {
			if p.ConsumerLag < 0 {
				// -1 means the high-water mark or committed offset is not
				// yet known. Skip until librdkafka has enough information.
				continue
			}
			m.lag.WithLabelValues(groupID, topic, partID).Set(float64(p.ConsumerLag))
		}
	}
}

// rdkafkaStats is a minimal representation of the librdkafka stats JSON
// payload. Only fields required for lag calculation are decoded.
// Full schema: https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md
type rdkafkaStats struct {
	Topics map[string]rdkafkaTopic `json:"topics"`
}

type rdkafkaTopic struct {
	// Partitions maps partition ID (as a string key) to per-partition stats.
	Partitions map[string]rdkafkaPartition `json:"partitions"`
}

type rdkafkaPartition struct {
	// ConsumerLag is hi_offset minus the stored (committed) offset.
	// -1 indicates the value is not yet known.
	ConsumerLag int64 `json:"consumer_lag"`
}
