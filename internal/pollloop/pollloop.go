package pollloop

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/lucasdecamargo/go-kafka-consumer/internal/types"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/circuit"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/dispatcher"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/offset"
)

// PollLoop is the central orchestrator that ties together the Kafka
// consumer, Dispatcher, and OffsetCoordinator. It runs in a single
// goroutine and manages the message lifecycle:
//
//   - Polls Kafka for messages and dispatches them to the Dispatcher.
//   - Periodically commits offsets reported by the OffsetCoordinator.
//   - Reacts to backpressure (Dispatcher.Ready()) and circuit breaker
//     state changes (Dispatcher.CircuitStateChanged()).
//   - Enters/exits degraded mode on broker or target unavailability.
//   - Handles partition rebalance callbacks (assign/revoke).
//   - Coordinates graceful shutdown.
//
// See ADR-0006 (broker unavailability), ADR-0007 (circuit breaker),
// and the scenario documents for the full behavior specification.
type PollLoop struct {
	cfg         Config
	kafka       KafkaConsumer
	dispatcher  dispatcher.Dispatcher
	coordinator offset.Coordinator
	health      *Health
	logger      *slog.Logger

	// Degraded mode state.
	degraded DegradedState

	// Consecutive commit failure counter for broker unavailability
	// detection (ADR-0006).
	commitFailures int

	// Tracks which partitions are currently paused for backpressure.
	// Key: partition ID, Value: true if paused due to backpressure.
	pausedPartitions map[int32]bool

	// Prometheus metrics.
	messagesTotal       prometheus.Counter
	pollErrorsTotal     prometheus.Counter
	commitsTotal        prometheus.Counter
	commitFailuresTotal prometheus.Counter
	degradedModeGauge   prometheus.Gauge
}

// New creates a new PollLoop with the given required dependencies and
// optional configuration. The poll loop does not start until Run() is
// called.
func New(
	cfg Config,
	kafka KafkaConsumer,
	disp dispatcher.Dispatcher,
	coordinator offset.Coordinator,
	health *Health,
	opts ...Option,
) (*PollLoop, error) {
	if kafka == nil {
		return nil, errors.New("pollloop: kafka consumer must not be nil")
	}
	if disp == nil {
		return nil, errors.New("pollloop: dispatcher must not be nil")
	}
	if coordinator == nil {
		return nil, errors.New("pollloop: offset coordinator must not be nil")
	}
	if health == nil {
		return nil, errors.New("pollloop: health must not be nil")
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	reg := o.registerer

	pl := &PollLoop{
		cfg:              cfg,
		kafka:            kafka,
		dispatcher:       disp,
		coordinator:      coordinator,
		health:           health,
		logger:           o.logger,
		pausedPartitions: make(map[int32]bool),

		messagesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: MetricPollLoopMessagesTotal,
			Help: "Total number of messages received from Kafka.",
		}),
		pollErrorsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: MetricPollLoopPollErrorsTotal,
			Help: "Total number of Poll() errors.",
		}),
		commitsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: MetricPollLoopCommitsTotal,
			Help: "Total number of successful offset commits.",
		}),
		commitFailuresTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: MetricPollLoopCommitFailuresTotal,
			Help: "Total number of failed offset commits.",
		}),
		degradedModeGauge: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: MetricPollLoopDegradedMode,
			Help: "Whether the poll loop is in degraded mode (1=degraded, 0=normal).",
		}),
	}

	return pl, nil
}

// Run starts the poll loop and blocks until the context is canceled.
// It returns after graceful shutdown completes (or the shutdown timeout
// expires).
//
// The caller (consumer.Consumer) is responsible for signal handling and
// context cancellation. Run does not catch OS signals directly.
func (pl *PollLoop) Run(ctx context.Context) error {
	pl.health.SetLive(true)
	pl.health.SetReady(true)
	defer pl.health.SetLive(false)

	pl.logger.Info("poll loop started",
		slog.Duration("poll_interval", pl.cfg.PollInterval),
		slog.Duration("commit_interval", pl.cfg.CommitInterval),
		slog.Int("commit_failure_threshold", pl.cfg.CommitFailureThreshold),
	)

	pollTicker := time.NewTicker(pl.cfg.PollInterval)
	defer pollTicker.Stop()

	commitTicker := time.NewTicker(pl.cfg.CommitInterval)
	defer commitTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return pl.shutdown()

		case <-pollTicker.C:
			pl.poll(ctx)

		case <-commitTicker.C:
			pl.commitOffsets()

		case <-pl.dispatcher.Ready():
			pl.onDispatcherReady()

		case state := <-pl.dispatcher.CircuitStateChanged():
			pl.onCircuitStateChanged(state)
		}
	}
}

// poll calls Poll() on the Kafka consumer and dispatches messages to
// the Dispatcher. Skipped when in degraded mode.
func (pl *PollLoop) poll(ctx context.Context) {
	if pl.degraded.IsDegraded() {
		// In degraded mode, still call Poll() for session keepalive
		// (heartbeats, rebalance callbacks) but don't dispatch.
		_, err := pl.kafka.Poll(int(pl.cfg.PollInterval.Milliseconds()))
		if err != nil {
			pl.logger.Debug("poll error in degraded mode", slog.String("error", err.Error()))
			pl.pollErrorsTotal.Inc()
		}
		return
	}

	msgs, err := pl.kafka.Poll(int(pl.cfg.PollInterval.Milliseconds()))
	if err != nil {
		pl.logger.Warn("poll error", slog.String("error", err.Error()))
		pl.pollErrorsTotal.Inc()
		return
	}

	if len(msgs) == 0 {
		return
	}

	pl.messagesTotal.Add(float64(len(msgs)))

	// Group messages by partition.
	grouped := groupByPartition(msgs)

	// Dispatch each partition group to the Dispatcher.
	for partition, partMsgs := range grouped {
		if pl.pausedPartitions[partition] {
			// Partition is paused due to backpressure — skip.
			// Messages will be re-fetched after resume.
			continue
		}

		err := pl.dispatcher.Send(ctx, partition, partMsgs)
		if err != nil {
			if errors.Is(err, dispatcher.ErrBackpressure) {
				pl.logger.Debug("backpressure on partition, pausing",
					slog.Int("partition", int(partition)),
				)
				pl.pausePartition(partition)
			} else {
				pl.logger.Error("dispatcher send error",
					slog.Int("partition", int(partition)),
					slog.String("error", err.Error()),
				)
			}
		}
	}
}

// commitOffsets collects committable offsets from the OffsetCoordinator
// and commits them to Kafka. Tracks consecutive failures for broker
// unavailability detection.
func (pl *PollLoop) commitOffsets() {
	offsets := pl.coordinator.Committable()
	if len(offsets) == 0 {
		return
	}

	err := pl.kafka.CommitOffsets(offsets)
	if err != nil {
		pl.commitFailures++
		pl.commitFailuresTotal.Inc()
		pl.logger.Warn("commit offsets failed",
			slog.Int("consecutive_failures", pl.commitFailures),
			slog.Int("threshold", pl.cfg.CommitFailureThreshold),
			slog.String("error", err.Error()),
		)

		if pl.commitFailures >= pl.cfg.CommitFailureThreshold && !pl.degraded.Has(BrokerUnavailable) {
			pl.enterDegradedMode(BrokerUnavailable)
		}
		return
	}

	// Success — reset failure counter.
	if pl.commitFailures > 0 {
		pl.logger.Info("commit offsets succeeded after failures",
			slog.Int("previous_failures", pl.commitFailures),
		)
	}
	pl.commitFailures = 0
	pl.commitsTotal.Inc()

	// If we were degraded due to broker unavailability, clear it.
	if pl.degraded.Has(BrokerUnavailable) {
		pl.exitDegradedMode(BrokerUnavailable)
	}
}

// onDispatcherReady handles the Ready() signal from the Dispatcher,
// indicating that capacity is available after a backpressure event.
// Resumes all paused partitions (unless in degraded mode).
func (pl *PollLoop) onDispatcherReady() {
	if pl.degraded.IsDegraded() {
		return // Don't resume while in degraded mode.
	}

	pl.resumeAllPausedPartitions()
}

// onCircuitStateChanged handles circuit breaker state transitions from
// the Dispatcher.
func (pl *PollLoop) onCircuitStateChanged(state circuit.State) {
	pl.logger.Info("circuit breaker state change received",
		slog.String("state", state.String()),
	)

	switch state {
	case circuit.Open:
		if !pl.degraded.Has(TargetUnavailable) {
			pl.enterDegradedMode(TargetUnavailable)
		}

	case circuit.Closed:
		if pl.degraded.Has(TargetUnavailable) {
			pl.exitDegradedMode(TargetUnavailable)
		}

	case circuit.HalfOpen:
		// Half-open: the circuit breaker is probing. We stay in
		// degraded mode but log for visibility.
		pl.logger.Info("circuit breaker half-open, probing target service")
	}
}

// enterDegradedMode adds a degraded reason and applies degraded mode
// behavior: pause all partitions, mark readiness as unhealthy.
func (pl *PollLoop) enterDegradedMode(reason DegradedReason) {
	wasDegraded := pl.degraded.IsDegraded()
	pl.degraded.Set(reason)

	pl.logger.Error("entering degraded mode",
		slog.String("reason", reason.String()),
		slog.String("active_reasons", pl.degraded.Reasons().String()),
	)

	if !wasDegraded {
		// First reason — apply degraded mode behavior.
		pl.pauseAllPartitions()
		pl.health.SetReady(false)
		pl.degradedModeGauge.Set(1)
	}
}

// exitDegradedMode clears a degraded reason. If no reasons remain,
// resumes normal operation: resume partitions, mark readiness healthy.
func (pl *PollLoop) exitDegradedMode(reason DegradedReason) {
	pl.degraded.Clear(reason)

	pl.logger.Info("degraded reason cleared",
		slog.String("reason", reason.String()),
		slog.String("remaining_reasons", pl.degraded.Reasons().String()),
	)

	if !pl.degraded.IsDegraded() {
		// All reasons cleared — resume normal operation.
		pl.logger.Info("exiting degraded mode — all reasons cleared")
		pl.resumeAllPartitions()
		pl.health.SetReady(true)
		pl.degradedModeGauge.Set(0)
	}
}

// pausePartition pauses a single partition due to backpressure.
func (pl *PollLoop) pausePartition(partition int32) {
	if pl.pausedPartitions[partition] {
		return // Already paused.
	}

	if err := pl.kafka.Pause([]int32{partition}); err != nil {
		pl.logger.Error("failed to pause partition",
			slog.Int("partition", int(partition)),
			slog.String("error", err.Error()),
		)
		return
	}

	pl.pausedPartitions[partition] = true
	pl.logger.Debug("partition paused (backpressure)",
		slog.Int("partition", int(partition)),
	)
}

// pauseAllPartitions pauses all assigned partitions. Used when entering
// degraded mode.
func (pl *PollLoop) pauseAllPartitions() {
	assignment, err := pl.kafka.Assignment()
	if err != nil {
		pl.logger.Error("failed to get assignment for pause",
			slog.String("error", err.Error()),
		)
		return
	}

	partitions := make([]int32, len(assignment))
	for i, p := range assignment {
		partitions[i] = p.Partition
	}

	if len(partitions) == 0 {
		return
	}

	if err := pl.kafka.Pause(partitions); err != nil {
		pl.logger.Error("failed to pause all partitions",
			slog.String("error", err.Error()),
		)
	}
}

// resumeAllPartitions resumes all assigned partitions. Used when
// exiting degraded mode.
func (pl *PollLoop) resumeAllPartitions() {
	assignment, err := pl.kafka.Assignment()
	if err != nil {
		pl.logger.Error("failed to get assignment for resume",
			slog.String("error", err.Error()),
		)
		return
	}

	partitions := make([]int32, len(assignment))
	for i, p := range assignment {
		partitions[i] = p.Partition
	}

	if len(partitions) == 0 {
		return
	}

	if err := pl.kafka.Resume(partitions); err != nil {
		pl.logger.Error("failed to resume all partitions",
			slog.String("error", err.Error()),
		)
	}

	// Clear the backpressure pause tracking too.
	for k := range pl.pausedPartitions {
		delete(pl.pausedPartitions, k)
	}
}

// resumeAllPausedPartitions resumes only the partitions that were
// paused due to backpressure.
func (pl *PollLoop) resumeAllPausedPartitions() {
	if len(pl.pausedPartitions) == 0 {
		return
	}

	partitions := make([]int32, 0, len(pl.pausedPartitions))
	for p := range pl.pausedPartitions {
		partitions = append(partitions, p)
	}

	if err := pl.kafka.Resume(partitions); err != nil {
		pl.logger.Error("failed to resume paused partitions",
			slog.String("error", err.Error()),
		)
		return
	}

	for _, p := range partitions {
		delete(pl.pausedPartitions, p)
	}

	pl.logger.Debug("resumed backpressure-paused partitions",
		slog.Int("count", len(partitions)),
	)
}

// OnPartitionsAssigned implements RebalanceHandler. Called by the Kafka
// adapter during a rebalance callback when new partitions are assigned.
func (pl *PollLoop) OnPartitionsAssigned(partitions []dispatcher.Partition) {
	pl.logger.Info("partitions assigned",
		slog.Int("count", len(partitions)),
	)
	pl.dispatcher.OnPartitionsAssigned(partitions)
}

// OnPartitionsRevoked implements RebalanceHandler. Called by the Kafka
// adapter during a rebalance callback when partitions are revoked.
// Drains in-flight work, commits offsets, and resets state.
func (pl *PollLoop) OnPartitionsRevoked(partitions []dispatcher.Partition) {
	pl.logger.Info("partitions revoked",
		slog.Int("count", len(partitions)),
	)

	// 1. Notify Dispatcher — drains in-flight batches for revoked partitions.
	pl.dispatcher.OnPartitionsRevoked(partitions)

	// 2. Commit offsets for revoked partitions.
	allCommittable := pl.coordinator.Committable()
	revokedSet := make(map[int32]bool, len(partitions))
	for _, p := range partitions {
		revokedSet[p.Partition] = true
	}

	revokedOffsets := make(map[int32]int64)
	for part, off := range allCommittable {
		if revokedSet[part] {
			revokedOffsets[part] = off
		}
	}

	if len(revokedOffsets) > 0 {
		if err := pl.kafka.CommitOffsets(revokedOffsets); err != nil {
			pl.logger.Error("failed to commit offsets for revoked partitions",
				slog.String("error", err.Error()),
			)
		}
	}

	// 3. Reset OffsetCoordinator state for revoked partitions.
	for _, p := range partitions {
		pl.coordinator.Reset(p.Partition)
		delete(pl.pausedPartitions, p.Partition)
	}
}

// shutdown performs graceful shutdown as described in scenario 07.
func (pl *PollLoop) shutdown() error {
	pl.logger.Info("poll loop shutting down",
		slog.Duration("timeout", pl.cfg.ShutdownTimeout),
	)

	// Create a deadline-bounded context for draining.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), pl.cfg.ShutdownTimeout)
	defer cancel()

	// 1. Close the Dispatcher — drains in-flight workers.
	dispatcherErr := pl.dispatcher.Close(shutdownCtx)
	if dispatcherErr != nil {
		pl.logger.Warn("dispatcher close returned error",
			slog.String("error", dispatcherErr.Error()),
		)
	}

	// 2. Final offset commit — collect whatever completed.
	offsets := pl.coordinator.Committable()
	if len(offsets) > 0 {
		if err := pl.kafka.CommitOffsets(offsets); err != nil {
			pl.logger.Error("final offset commit failed",
				slog.String("error", err.Error()),
			)
		} else {
			pl.logger.Info("final offset commit succeeded",
				slog.Int("partitions", len(offsets)),
			)
		}
	}

	// 3. Close the Kafka consumer — leaves the consumer group.
	if err := pl.kafka.Close(); err != nil {
		pl.logger.Error("kafka consumer close error",
			slog.String("error", err.Error()),
		)
	}

	pl.health.SetReady(false)

	if dispatcherErr != nil {
		return dispatcherErr
	}

	pl.logger.Info("poll loop shutdown complete")
	return nil
}

// groupByPartition groups a slice of messages by their partition.
func groupByPartition(msgs []types.Message) map[int32][]types.Message {
	grouped := make(map[int32][]types.Message)
	for _, msg := range msgs {
		grouped[msg.Partition] = append(grouped[msg.Partition], msg)
	}
	return grouped
}
