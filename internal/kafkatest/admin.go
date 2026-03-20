package kafkatest

import (
	"context"
	"fmt"
	"testing"
	"time"

	ckg "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// CreateTopic creates a topic with the given number of partitions
// (replication factor 1). Returns an error if the topic cannot be
// created.
func (c *Cluster) CreateTopic(ctx context.Context, topic string, partitions int) error {
	admin, err := ckg.NewAdminClient(&ckg.ConfigMap{
		"bootstrap.servers": c.brokers,
	})
	if err != nil {
		return fmt.Errorf("create admin client: %w", err)
	}
	defer admin.Close()

	results, err := admin.CreateTopics(
		ctx,
		[]ckg.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		}},
		ckg.SetAdminOperationTimeout(10*time.Second),
	)
	if err != nil {
		return fmt.Errorf("create topic %q: %w", topic, err)
	}
	for _, r := range results {
		if r.Error.Code() != ckg.ErrNoError {
			return fmt.Errorf("create topic %q: %w", topic, r.Error)
		}
	}

	return nil
}

// CreateTopicT is a test-friendly wrapper that calls t.Fatal on error.
func (c *Cluster) CreateTopicT(t *testing.T, topic string, partitions int) {
	t.Helper()
	if err := c.CreateTopic(context.Background(), topic, partitions); err != nil {
		t.Fatalf("kafkatest: %v", err)
	}
	t.Logf("kafkatest: topic %q created with %d partitions", topic, partitions)
}

// CommittedOffsets returns the committed offsets for the given consumer
// group and topic. Returns an empty map if no offsets are committed.
func (c *Cluster) CommittedOffsets(ctx context.Context, groupID, topic string, partitions int) (map[int32]int64, error) {
	consumer, err := ckg.NewConsumer(&ckg.ConfigMap{
		"bootstrap.servers": c.brokers,
		"group.id":          groupID,
	})
	if err != nil {
		return nil, fmt.Errorf("create consumer for offset check: %w", err)
	}
	defer func() { _ = consumer.Close() }()

	tps := make([]ckg.TopicPartition, partitions)
	for i := range partitions {
		tps[i] = ckg.TopicPartition{
			Topic:     &topic,
			Partition: int32(i),
		}
	}

	committed, err := consumer.Committed(tps, 5000)
	if err != nil {
		return nil, fmt.Errorf("get committed offsets: %w", err)
	}

	result := make(map[int32]int64)
	for _, tp := range committed {
		if tp.Offset >= 0 {
			result[tp.Partition] = int64(tp.Offset)
		}
	}

	return result, nil
}

// CommittedOffsetsT is a test-friendly wrapper that calls t.Fatal on error.
func (c *Cluster) CommittedOffsetsT(t *testing.T, groupID, topic string, partitions int) map[int32]int64 {
	t.Helper()
	offsets, err := c.CommittedOffsets(context.Background(), groupID, topic, partitions)
	if err != nil {
		t.Fatalf("kafkatest: %v", err)
	}
	return offsets
}

// TopicMessageCount returns the total number of messages across all
// partitions of a topic (sum of high watermarks). Useful for verifying
// DLQ topics received the expected number of messages without consuming
// them.
func (c *Cluster) TopicMessageCount(ctx context.Context, topic string, partitions int) (int64, error) {
	consumer, err := ckg.NewConsumer(&ckg.ConfigMap{
		"bootstrap.servers": c.brokers,
		"group.id":          "kafkatest-count-" + topic,
	})
	if err != nil {
		return 0, fmt.Errorf("create consumer for message count: %w", err)
	}
	defer func() { _ = consumer.Close() }()

	var total int64
	for i := range partitions {
		_, high, err := consumer.QueryWatermarkOffsets(topic, int32(i), 5000)
		if err != nil {
			return 0, fmt.Errorf("query watermark for %s[%d]: %w", topic, i, err)
		}
		total += high
	}

	return total, nil
}

// TopicMessageCountT is a test-friendly wrapper that calls t.Fatal on error.
func (c *Cluster) TopicMessageCountT(t *testing.T, topic string, partitions int) int64 {
	t.Helper()
	count, err := c.TopicMessageCount(context.Background(), topic, partitions)
	if err != nil {
		t.Fatalf("kafkatest: %v", err)
	}
	return count
}
