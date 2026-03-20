# Examples

This directory contains runnable usage examples for the `go-kafka-consumer` framework.
Each subdirectory is a self-contained `main` package demonstrating a specific feature
or use case.

## Prerequisites

All examples require a running Kafka broker at `localhost:9092`. The quickest way
to start one locally:

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_CFG_NODE_ID=1 \
  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  bitnami/kafka:latest
```

Create the example topic:

```bash
docker exec kafka kafka-topics.sh --create \
  --topic example-events \
  --partitions 6 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

Produce some test messages:

```bash
for i in $(seq 1 100); do
  echo "message-$i" | docker exec -i kafka kafka-console-producer.sh \
    --topic example-events \
    --bootstrap-server localhost:9092
done
```

## Examples

| Directory | Description |
|---|---|
| [`basic`](basic/) | Minimal consumer — the simplest working setup |
| [`batch-processing`](batch-processing/) | Process messages in batches with custom batch size and linger time |
| [`graceful-shutdown`](graceful-shutdown/) | Signal handling with `SIGTERM`/`SIGINT` for clean shutdown |
| [`dlq-handling`](dlq-handling/) | Dead Letter Queue routing for non-retryable errors |
| [`custom-metrics`](custom-metrics/) | Prometheus metrics with a custom registry and HTTP endpoint |
| [`security`](security/) | TLS and SASL authentication configuration |

## Running an Example

```bash
cd examples/basic
go run main.go
```

Stop the consumer with `Ctrl+C` (sends `SIGINT`) — you'll see the graceful shutdown
sequence in the logs.
