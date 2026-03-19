package kafkatest

import (
	"context"
	"fmt"
	"testing"

	ckg "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Produce publishes messages to the given topic and waits for all
// delivery reports. It returns the number of messages successfully
// delivered.
func (c *Cluster) Produce(ctx context.Context, topic string, msgs []ProduceMessage) (int, error) {
	producer, err := ckg.NewProducer(&ckg.ConfigMap{
		"bootstrap.servers": c.brokers,
	})
	if err != nil {
		return 0, fmt.Errorf("create producer: %w", err)
	}
	defer producer.Close()

	deliveryCh := make(chan ckg.Event, len(msgs))

	for _, m := range msgs {
		msg := &ckg.Message{
			TopicPartition: ckg.TopicPartition{
				Topic:     &topic,
				Partition: ckg.PartitionAny,
			},
			Key:   m.Key,
			Value: m.Value,
		}
		for _, h := range m.Headers {
			msg.Headers = append(msg.Headers, ckg.Header{
				Key:   h.Key,
				Value: h.Value,
			})
		}
		if err := producer.Produce(msg, deliveryCh); err != nil {
			return 0, fmt.Errorf("enqueue message: %w", err)
		}
	}

	delivered := 0
	for range len(msgs) {
		select {
		case <-ctx.Done():
			return delivered, ctx.Err()
		case e := <-deliveryCh:
			m := e.(*ckg.Message)
			if m.TopicPartition.Error != nil {
				return delivered, fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
			}
			delivered++
		}
	}

	return delivered, nil
}

// ProduceT is a test-friendly wrapper that calls t.Fatal on error.
func (c *Cluster) ProduceT(t *testing.T, topic string, msgs []ProduceMessage) int {
	t.Helper()
	n, err := c.Produce(context.Background(), topic, msgs)
	if err != nil {
		t.Fatalf("kafkatest: produce to %q: %v", topic, err)
	}
	t.Logf("kafkatest: produced %d messages to %q", n, topic)
	return n
}
