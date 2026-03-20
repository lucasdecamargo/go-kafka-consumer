package kafkatest

import (
	"context"
	"fmt"
	"testing"
	"time"

	ckg "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Cluster manages a single-node Kafka broker in a Docker container.
// It is designed to be shared across subtests via TestMain or a
// top-level test function, eliminating per-test container startup.
type Cluster struct {
	container testcontainers.Container
	brokers   string
}

// Brokers returns the host:port address for client connections.
func (c *Cluster) Brokers() string {
	return c.brokers
}

// Start spins up a single-node Apache Kafka broker in KRaft mode and
// blocks until the broker is ready to accept connections.
//
// The challenge with testcontainers is that port mappings are dynamic,
// but Kafka's KAFKA_ADVERTISED_LISTENERS must reflect the host-visible
// address. We solve this with a custom startup script that waits for a
// lifecycle hook to write the mapped port, then configures and starts
// Kafka.
//
// For test suites, call Start once in TestMain and share the Cluster
// across all tests. Each test uses its own topic for isolation.
func Start(ctx context.Context) (*Cluster, error) {
	// Custom startup script: waits for the external port to be written
	// by the PostStarts lifecycle hook, then starts Kafka with the
	// correct advertised listener.
	startScript := `
while [ ! -f /tmp/external_port ]; do sleep 0.1; done
EXTERNAL_PORT=$(cat /tmp/external_port)
export KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://localhost:29092,EXTERNAL://localhost:${EXTERNAL_PORT}"
exec /etc/kafka/docker/run
`

	req := testcontainers.ContainerRequest{
		Image:        "apache/kafka:latest",
		ExposedPorts: []string{"9092/tcp"},
		Env: map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT",
			"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9092",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"CLUSTER_ID": "MkU3OEVBNTcwNTJENDM2Qk",
		},
		Entrypoint: []string{"sh", "-c"},
		Cmd:        []string{startScript},
		LifecycleHooks: []testcontainers.ContainerLifecycleHooks{{
			PostStarts: []testcontainers.ContainerHook{
				func(ctx context.Context, c testcontainers.Container) error {
					port, err := c.MappedPort(ctx, "9092")
					if err != nil {
						return fmt.Errorf("get mapped port: %w", err)
					}
					// Write the mapped port so the startup script can
					// configure KAFKA_ADVERTISED_LISTENERS correctly.
					_, _, err = c.Exec(ctx, []string{
						"sh", "-c",
						fmt.Sprintf("echo %s > /tmp/external_port", port.Port()),
					})
					return err
				},
			},
		}},
		WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(90 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("start kafka container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("get container host: %w", err)
	}
	mappedPort, err := container.MappedPort(ctx, "9092")
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("get mapped port: %w", err)
	}

	brokers := fmt.Sprintf("%s:%s", host, mappedPort.Port())

	cluster := &Cluster{
		container: container,
		brokers:   brokers,
	}

	// Verify broker is responsive before returning.
	if err := cluster.waitReady(); err != nil {
		_ = container.Terminate(ctx)
		return nil, err
	}

	return cluster, nil
}

// StartT is a test-friendly wrapper that starts a Cluster, registers
// cleanup with t.Cleanup, and calls t.Fatal on error.
func StartT(t *testing.T) *Cluster {
	t.Helper()

	ctx := context.Background()
	cluster, err := Start(ctx)
	if err != nil {
		t.Fatalf("kafkatest: start cluster: %v", err)
	}

	t.Cleanup(func() {
		if err := cluster.Terminate(ctx); err != nil {
			t.Logf("kafkatest: terminate cluster: %v", err)
		}
	})

	t.Logf("kafkatest: broker ready at %s", cluster.brokers)
	return cluster
}

// Terminate shuts down the Kafka container. Safe to call multiple times.
func (c *Cluster) Terminate(ctx context.Context) error {
	if c.container == nil {
		return nil
	}
	return c.container.Terminate(ctx)
}

// waitReady polls until the Kafka broker responds to metadata requests.
func (c *Cluster) waitReady() error {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		admin, err := ckg.NewAdminClient(&ckg.ConfigMap{
			"bootstrap.servers": c.brokers,
		})
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		md, err := admin.GetMetadata(nil, true, 3000)
		admin.Close()
		if err == nil && len(md.Brokers) > 0 {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("kafkatest: timeout waiting for broker at %s", c.brokers)
}
