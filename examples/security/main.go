// Package main demonstrates how to configure TLS and SASL authentication
// for connecting to a secured Kafka cluster.
//
// The framework supports four security protocols:
//
//   - Plaintext:     No encryption or authentication (development only)
//   - SSL:           TLS encryption without SASL
//   - SASL_PLAINTEXT: SASL authentication without TLS (not recommended)
//   - SASL_SSL:      Both TLS and SASL (recommended for production)
//
// SASL mechanisms supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER
//
// This example shows SASL_SSL with SCRAM-SHA-512. Adjust the configuration
// to match your cluster's security settings.
//
// Run:
//
//	# Set credentials via environment variables — never hardcode secrets.
//	export KAFKA_BROKERS="kafka-1.example.com:9093,kafka-2.example.com:9093"
//	export KAFKA_USERNAME="my-service"
//	export KAFKA_PASSWORD="secret"
//	export KAFKA_CA_FILE="/etc/kafka/ca.pem"
//
//	go run main.go
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/lucasdecamargo/go-kafka-consumer/consumer"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Read configuration from environment variables.
	brokers := getEnvOrDefault("KAFKA_BROKERS", "localhost:9092")
	username := getEnvOrDefault("KAFKA_USERNAME", "")
	password := getEnvOrDefault("KAFKA_PASSWORD", "")
	caFile := getEnvOrDefault("KAFKA_CA_FILE", "")

	cfg := consumer.DefaultConfig()
	cfg.Brokers = strings.Split(brokers, ",")
	cfg.Topics = []string{"example-events"}
	cfg.GroupID = "security-example"

	// Configure security based on available credentials.
	switch {
	case username != "" && caFile != "":
		// Production: SASL_SSL with SCRAM-SHA-512
		cfg.Security = consumer.SecurityConfig{
			Protocol: consumer.ProtocolSASLSSL,
			TLS: &consumer.TLSConfig{
				CAFile: caFile,
				// For mTLS (mutual TLS), also set:
				// CertFile: "/etc/kafka/client.pem",
				// KeyFile:  "/etc/kafka/client-key.pem",
			},
			SASL: &consumer.SASLConfig{
				Mechanism: consumer.SASLSCRAMSHA512,
				Username:  username,
				Password:  password,
			},
		}
		logger.Info("security configured",
			slog.String("protocol", "SASL_SSL"),
			slog.String("mechanism", "SCRAM-SHA-512"),
		)
	case caFile != "":
		// TLS only — no SASL authentication.
		cfg.Security = consumer.SecurityConfig{
			Protocol: consumer.ProtocolSSL,
			TLS: &consumer.TLSConfig{
				CAFile: caFile,
			},
		}
		logger.Info("security configured",
			slog.String("protocol", "SSL"),
		)
	default:
		// Development: plaintext (default).
		logger.Warn("running without security — set KAFKA_USERNAME, KAFKA_PASSWORD, and KAFKA_CA_FILE for production")
	}

	processor := func(ctx context.Context, batch []consumer.Message) error {
		for i := range batch {
			fmt.Printf("partition=%d offset=%d key=%s\n",
				batch[i].Partition, batch[i].Offset, batch[i].Key,
			)
		}
		return nil
	}

	c, err := consumer.New(cfg, processor,
		consumer.WithLogger(logger),
	)
	if err != nil {
		log.Fatalf("create consumer: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)

	err = c.Run(ctx)
	stop()
	if err != nil {
		log.Fatalf("consumer error: %v", err)
	}
}

// getEnvOrDefault returns the value of the environment variable or the default.
func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
