package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/lucasdecamargo/go-kafka-consumer/internal/types"
)

// DLQProducerConfig holds the settings needed to create a DLQ producer.
// The broker and security settings are reused from the consumer adapter
// config so the DLQ producer connects to the same Kafka cluster.
type DLQProducerConfig struct {
	// Topic is the Kafka topic where failed messages are produced.
	Topic string

	// Brokers is the list of Kafka broker addresses.
	Brokers []string

	// Security holds TLS and SASL authentication settings.
	Security SecurityConfig
}

// DLQProducer publishes failed message batches to a dead letter queue
// topic. It preserves the original message content and attaches error
// metadata as Kafka headers (FR-4.2).
//
// Headers added to each DLQ message:
//   - dlq.source.topic:     original source topic
//   - dlq.source.partition: original partition number
//   - dlq.source.offset:    original offset
//   - dlq.error.reason:     error message describing why the batch failed
//   - dlq.error.timestamp:  RFC 3339 timestamp when the error occurred
//
// The producer is safe for concurrent use.
type DLQProducer struct {
	producer *kafka.Producer
	topic    string
	logger   *slog.Logger
}

// DLQOption configures optional dependencies for the DLQ producer.
type DLQOption func(*dlqOptions)

type dlqOptions struct {
	logger *slog.Logger
}

// WithDLQLogger sets the structured logger for the DLQ producer.
func WithDLQLogger(l *slog.Logger) DLQOption {
	return func(o *dlqOptions) {
		if l != nil {
			o.logger = l
		}
	}
}

// NewDLQProducer creates a new DLQ producer that publishes to the
// configured topic. The producer is backed by a confluent-kafka-go
// producer that shares the same broker and security settings as the
// consumer.
func NewDLQProducer(cfg DLQProducerConfig, opts ...DLQOption) (*DLQProducer, error) {
	if cfg.Topic == "" {
		return nil, fmt.Errorf("dlq: topic must not be empty")
	}
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("dlq: at least one broker is required")
	}

	o := dlqOptions{logger: slog.Default()}
	for _, opt := range opts {
		opt(&o)
	}

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.Brokers, ","),
		// Acks=all for DLQ durability — failed messages must not be lost.
		"acks": "all",
		// Short linger for low-latency DLQ delivery.
		"linger.ms": 10,
	}

	if err := applySecurity(configMap, cfg.Security); err != nil {
		return nil, fmt.Errorf("dlq: %w", err)
	}

	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, fmt.Errorf("dlq: create producer: %w", err)
	}

	dlq := &DLQProducer{
		producer: producer,
		topic:    cfg.Topic,
		logger:   o.logger,
	}

	// Drain delivery reports in the background to prevent the
	// internal producer queue from blocking.
	go dlq.drainEvents()

	return dlq, nil
}

// Produce sends each message in the batch to the DLQ topic with error
// metadata attached as headers. It preserves the original key, value,
// and headers, and adds DLQ-specific metadata headers.
//
// This method blocks until all delivery reports are received, ensuring
// at-least-once delivery to the DLQ topic.
func (d *DLQProducer) Produce(ctx context.Context, msgs []types.Message, reason error) error {
	deliveryCh := make(chan kafka.Event, len(msgs))
	reasonStr := reason.Error()
	timestamp := time.Now().UTC().Format(time.RFC3339)

	for _, msg := range msgs {
		headers := buildDLQHeaders(msg, reasonStr, timestamp)

		// Preserve original message headers.
		for _, h := range msg.Headers {
			headers = append(headers, kafka.Header{
				Key:   h.Key,
				Value: h.Value,
			})
		}

		kMsg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &d.topic,
				Partition: kafka.PartitionAny,
			},
			Key:     msg.Key,
			Value:   msg.Value,
			Headers: headers,
		}

		if err := d.producer.Produce(kMsg, deliveryCh); err != nil {
			return fmt.Errorf("dlq: enqueue message (partition=%d, offset=%d): %w",
				msg.Partition, msg.Offset, err)
		}
	}

	// Wait for all delivery reports.
	var firstErr error
	for range len(msgs) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("dlq: context canceled while waiting for delivery: %w", ctx.Err())
		case e := <-deliveryCh:
			m := e.(*kafka.Message)
			if m.TopicPartition.Error != nil && firstErr == nil {
				firstErr = fmt.Errorf("dlq: delivery failed: %w", m.TopicPartition.Error)
			}
		}
	}

	if firstErr != nil {
		return firstErr
	}

	d.logger.Info("DLQ batch produced",
		slog.String("topic", d.topic),
		slog.Int("messages", len(msgs)),
		slog.String("reason", reasonStr),
	)

	return nil
}

// Close flushes pending messages and shuts down the producer.
// It blocks until all outstanding messages are delivered or the
// timeout expires.
func (d *DLQProducer) Close() {
	d.producer.Flush(10000) // 10s timeout.
	d.producer.Close()
}

// drainEvents reads from the producer's Events channel to handle
// delivery reports and errors that are not routed to per-message
// delivery channels.
func (d *DLQProducer) drainEvents() {
	for e := range d.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				d.logger.Warn("DLQ delivery report error",
					slog.String("topic", d.topic),
					slog.String("error", ev.TopicPartition.Error.Error()),
				)
			}
		case kafka.Error:
			d.logger.Warn("DLQ producer error",
				slog.String("error", ev.Error()),
			)
		}
	}
}

// buildDLQHeaders creates the DLQ metadata headers for a message.
func buildDLQHeaders(msg types.Message, reason, timestamp string) []kafka.Header {
	return []kafka.Header{
		{Key: "dlq.source.topic", Value: []byte(msg.Topic)},
		{Key: "dlq.source.partition", Value: []byte(strconv.FormatInt(int64(msg.Partition), 10))},
		{Key: "dlq.source.offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
		{Key: "dlq.error.reason", Value: []byte(reason)},
		{Key: "dlq.error.timestamp", Value: []byte(timestamp)},
	}
}
