package kafka

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/lucasdecamargo/go-kafka-consumer/internal/dispatcher"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/pollloop"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/types"
)

// Adapter wraps confluent-kafka-go's Consumer and implements
// pollloop.KafkaConsumer. All methods are called from the single poll
// loop goroutine; thread safety is not required.
type Adapter struct {
	c       *kafka.Consumer
	handler pollloop.RebalanceHandler
	topics  []string
	logger  *slog.Logger
}

// AdapterOption configures optional dependencies for the Adapter.
type AdapterOption func(*adapterOptions)

type adapterOptions struct {
	logger *slog.Logger
}

func defaultAdapterOptions() adapterOptions {
	return adapterOptions{
		logger: slog.Default(),
	}
}

// WithLogger sets the structured logger for the Kafka adapter.
func WithLogger(l *slog.Logger) AdapterOption {
	return func(o *adapterOptions) {
		if l != nil {
			o.logger = l
		}
	}
}

// NewAdapter creates a Kafka consumer adapter from the internal AdapterConfig.
// The adapter subscribes to the configured topics and delegates rebalance
// events to the provided RebalanceHandler.
//
// The caller must call Close() when done to leave the consumer group.
func NewAdapter(
	cfg AdapterConfig,
	handler pollloop.RebalanceHandler,
	opts ...AdapterOption,
) (*Adapter, error) {
	if handler == nil {
		return nil, fmt.Errorf("kafka adapter: rebalance handler must not be nil")
	}

	o := defaultAdapterOptions()
	for _, opt := range opts {
		opt(&o)
	}

	configMap, err := BuildConsumerConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka adapter: %w", err)
	}

	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, fmt.Errorf("kafka adapter: create consumer: %w", err)
	}

	a := &Adapter{
		c:       c,
		handler: handler,
		topics:  cfg.Topics,
		logger:  o.logger,
	}

	// Subscribe with rebalance callback.
	if err := c.SubscribeTopics(cfg.Topics, a.rebalanceCb); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("kafka adapter: subscribe: %w", err)
	}

	a.logger.Info("kafka adapter initialized",
		slog.Any("topics", cfg.Topics),
		slog.String("group_id", cfg.GroupID),
		slog.String("security_protocol", cfg.Security.Protocol),
	)

	return a, nil
}

// Poll retrieves messages from Kafka. On the first call, it blocks up to
// timeoutMs waiting for a message. If a message arrives, subsequent
// non-blocking Poll(0) calls drain any buffered messages for maximum
// throughput per poll cycle.
//
// Returns nil, nil when no messages are available.
// Rebalance events are handled internally via the rebalance callback.
func (a *Adapter) Poll(timeoutMs int) ([]types.Message, error) {
	// First poll: block up to timeoutMs.
	ev := a.c.Poll(timeoutMs)
	if ev == nil {
		return nil, nil
	}

	var msgs []types.Message
	if err := a.handleEvent(ev, &msgs); err != nil {
		return nil, err
	}

	// Drain buffered messages with non-blocking polls.
	for {
		ev = a.c.Poll(0)
		if ev == nil {
			break
		}
		if err := a.handleEvent(ev, &msgs); err != nil {
			// Return what we have so far plus the error.
			// The poll loop will process collected messages and
			// handle the error on the next cycle.
			if len(msgs) > 0 {
				a.logger.Warn("poll drain error, returning partial batch",
					slog.String("error", err.Error()),
					slog.Int("messages_collected", len(msgs)),
				)
				return msgs, nil
			}
			return nil, err
		}
	}

	return msgs, nil
}

// handleEvent processes a single Kafka event. Messages are appended to
// the msgs slice. Rebalance events are handled via the callback.
// Errors are returned to the caller.
func (a *Adapter) handleEvent(ev kafka.Event, msgs *[]types.Message) error {
	switch e := ev.(type) {
	case *kafka.Message:
		*msgs = append(*msgs, mapMessage(e))

	case kafka.Error:
		// Transient errors (broker disconnect, etc.) are logged but
		// returned to the poll loop for counting. Fatal errors
		// (authentication failure, etc.) are also returned.
		return fmt.Errorf("kafka error (code=%s): %w", e.Code().String(), e)

	default:
		// Other events (stats, logs, offsets) are silently ignored.
		// Rebalance events are handled via the rebalance callback,
		// not via Poll() events.
	}

	return nil
}

// CommitOffsets commits the given partition offsets synchronously.
// The offsets map values must already be next-to-fetch (i.e., the
// coordinator returns maxOffset+1). No additional +1 is applied here.
func (a *Adapter) CommitOffsets(offsets map[int32]int64) error {
	tps := make([]kafka.TopicPartition, 0, len(offsets))
	for partition, offset := range offsets {
		// Find the topic for this partition from the current assignment.
		topic := a.topicForPartition(partition)
		tps = append(tps, kafka.TopicPartition{
			Topic:     &topic,
			Partition: partition,
			Offset:    kafka.Offset(offset), // Already next-to-fetch from coordinator.
		})
	}

	_, err := a.c.CommitOffsets(tps)
	if err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}

	return nil
}

// Pause stops message delivery for the specified partitions. Paused
// partitions still participate in heartbeats via Poll().
func (a *Adapter) Pause(partitions []int32) error {
	tps := a.partitionsToTopicPartitions(partitions)
	if err := a.c.Pause(tps); err != nil {
		return fmt.Errorf("pause partitions: %w", err)
	}
	return nil
}

// Resume restarts message delivery for previously paused partitions.
func (a *Adapter) Resume(partitions []int32) error {
	tps := a.partitionsToTopicPartitions(partitions)
	if err := a.c.Resume(tps); err != nil {
		return fmt.Errorf("resume partitions: %w", err)
	}
	return nil
}

// Assignment returns the set of partitions currently assigned to this
// consumer.
func (a *Adapter) Assignment() ([]dispatcher.Partition, error) {
	tps, err := a.c.Assignment()
	if err != nil {
		return nil, fmt.Errorf("get assignment: %w", err)
	}

	result := make([]dispatcher.Partition, len(tps))
	for i, tp := range tps {
		topic := ""
		if tp.Topic != nil {
			topic = *tp.Topic
		}
		result[i] = dispatcher.Partition{
			Topic:     topic,
			Partition: tp.Partition,
		}
	}

	return result, nil
}

// Close closes the Kafka consumer, leaving the consumer group.
func (a *Adapter) Close() error {
	a.logger.Info("closing kafka consumer")
	if err := a.c.Close(); err != nil {
		return fmt.Errorf("close consumer: %w", err)
	}
	return nil
}

// rebalanceCb is the confluent-kafka-go rebalance callback. It is called
// synchronously from within Poll() when partition assignments change.
// Uses cooperative incremental rebalancing (CooperativeStickyAssignor).
func (a *Adapter) rebalanceCb(c *kafka.Consumer, ev kafka.Event) error {
	switch e := ev.(type) {
	case kafka.AssignedPartitions:
		partitions := mapTopicPartitions(e.Partitions)
		a.logger.Info("partitions assigned",
			slog.Int("count", len(partitions)),
		)

		// Notify the poll loop (RebalanceHandler).
		a.handler.OnPartitionsAssigned(partitions)

		// Cooperative: incrementally add new partitions.
		if err := c.IncrementalAssign(e.Partitions); err != nil {
			a.logger.Error("incremental assign failed",
				slog.String("error", err.Error()),
			)
			return err
		}

	case kafka.RevokedPartitions:
		partitions := mapTopicPartitions(e.Partitions)
		a.logger.Info("partitions revoked",
			slog.Int("count", len(partitions)),
		)

		// Notify the poll loop — this drains in-flight work and
		// commits offsets for revoked partitions.
		a.handler.OnPartitionsRevoked(partitions)

		// Cooperative: incrementally remove revoked partitions.
		if err := c.IncrementalUnassign(e.Partitions); err != nil {
			a.logger.Error("incremental unassign failed",
				slog.String("error", err.Error()),
			)
			return err
		}
	}

	return nil
}

// topicForPartition returns the topic name for a given partition ID
// by looking it up in the current assignment. Falls back to the first
// subscribed topic if the partition is not found (defensive).
func (a *Adapter) topicForPartition(partition int32) string {
	tps, err := a.c.Assignment()
	if err == nil {
		for _, tp := range tps {
			if tp.Partition == partition && tp.Topic != nil {
				return *tp.Topic
			}
		}
	}

	// Fallback: if only one topic is subscribed, use it.
	if len(a.topics) == 1 {
		return a.topics[0]
	}

	a.logger.Warn("could not resolve topic for partition, using first topic",
		slog.Int("partition", int(partition)),
	)
	return a.topics[0]
}

// partitionsToTopicPartitions converts partition IDs to kafka.TopicPartition
// structs by resolving each partition's topic from the current assignment.
func (a *Adapter) partitionsToTopicPartitions(partitions []int32) []kafka.TopicPartition {
	tps := make([]kafka.TopicPartition, len(partitions))
	for i, p := range partitions {
		topic := a.topicForPartition(p)
		tps[i] = kafka.TopicPartition{
			Topic:     &topic,
			Partition: p,
		}
	}
	return tps
}

// mapMessage converts a confluent-kafka-go Message to our framework's
// types.Message type.
func mapMessage(km *kafka.Message) types.Message {
	msg := types.Message{
		Partition: km.TopicPartition.Partition,
		Offset:    int64(km.TopicPartition.Offset),
		Key:       km.Key,
		Value:     km.Value,
		Timestamp: km.Timestamp,
		PolledAt:  time.Now(),
	}

	if km.TopicPartition.Topic != nil {
		msg.Topic = *km.TopicPartition.Topic
	}

	if len(km.Headers) > 0 {
		msg.Headers = make([]types.Header, len(km.Headers))
		for i, h := range km.Headers {
			msg.Headers[i] = types.Header{
				Key:   h.Key,
				Value: h.Value,
			}
		}
	}

	return msg
}

// mapTopicPartitions converts confluent-kafka-go TopicPartitions to
// our framework's dispatcher.Partition slice.
func mapTopicPartitions(tps []kafka.TopicPartition) []dispatcher.Partition {
	result := make([]dispatcher.Partition, len(tps))
	for i, tp := range tps {
		topic := ""
		if tp.Topic != nil {
			topic = *tp.Topic
		}
		result[i] = dispatcher.Partition{
			Topic:     topic,
			Partition: tp.Partition,
		}
	}
	return result
}

// Ensure Adapter implements pollloop.KafkaConsumer at compile time.
var _ pollloop.KafkaConsumer = (*Adapter)(nil)
