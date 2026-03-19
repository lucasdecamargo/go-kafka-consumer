//go:build integration

// Package integration contains end-to-end tests that validate the Kafka
// consumer framework against a real Apache Kafka broker using testcontainers.
//
// Run with: go test -tags integration -timeout 300s ./tests/integration/
package integration

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/lucasdecamargo/go-kafka-consumer/consumer"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/kafkatest"
)

// cluster is the shared Kafka container for all integration tests.
// Initialized once in TestMain, reused across all test functions.
var cluster *kafkatest.Cluster

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	cluster, err = kafkatest.Start(ctx)
	if err != nil {
		panic("start kafka cluster: " + err.Error())
	}
	code := m.Run()
	_ = cluster.Terminate(ctx)
	os.Exit(code)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig(brokers, topic, groupID string) consumer.Config {
	cfg := consumer.DefaultConfig()
	cfg.Brokers = []string{brokers}
	cfg.Topics = []string{topic}
	cfg.GroupID = groupID
	cfg.WorkerCount = 2
	cfg.ChannelCap = 10
	cfg.BatchSize = 5
	cfg.LingerTime = 50 * time.Millisecond
	cfg.PollInterval = 50 * time.Millisecond
	cfg.CommitInterval = 500 * time.Millisecond
	cfg.ShutdownTimeout = 10 * time.Second
	cfg.HealthAddr = "" // Disable HTTP server in tests.
	return cfg
}
