package kafkatest

import (
	"context"
	"fmt"
	"time"

	ckg "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ConsumedMessage is a message read back from Kafka by the raw consumer
// helper. Useful for verifying DLQ contents or inspecting message metadata.
type ConsumedMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
}

// ConsumeAll reads up to maxMessages from the given topic using a fresh
// consumer group. It returns after maxMessages are received or the timeout
// expires, whichever comes first. Useful for verifying DLQ topic contents.
func (c *Cluster) ConsumeAll(ctx context.Context, topic, groupID string, maxMessages int, timeout time.Duration) ([]ConsumedMessage, error) {
	consumer, err := ckg.NewConsumer(&ckg.ConfigMap{
		"bootstrap.servers":  c.brokers,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}
	defer func() { _ = consumer.Close() }()

	if err := consumer.Subscribe(topic, nil); err != nil {
		return nil, fmt.Errorf("subscribe to %q: %w", topic, err)
	}

	var msgs []ConsumedMessage
	deadline := time.Now().Add(timeout)

	for len(msgs) < maxMessages && time.Now().Before(deadline) {
		ev := consumer.Poll(1000)
		if ev == nil {
			continue
		}
		switch e := ev.(type) {
		case *ckg.Message:
			cm := ConsumedMessage{
				Topic:     *e.TopicPartition.Topic,
				Partition: e.TopicPartition.Partition,
				Offset:    int64(e.TopicPartition.Offset),
				Key:       e.Key,
				Value:     e.Value,
			}
			for _, h := range e.Headers {
				cm.Headers = append(cm.Headers, Header{Key: h.Key, Value: h.Value})
			}
			msgs = append(msgs, cm)
		case ckg.Error:
			if e.Code() != ckg.ErrTimedOut {
				return msgs, fmt.Errorf("consumer error: %w", e)
			}
		}
	}

	return msgs, nil
}
