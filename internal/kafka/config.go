// Package kafka provides the concrete Kafka client adapter that bridges
// the confluent-kafka-go library with the framework's internal interfaces.
package kafka

import (
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// AdapterConfig holds the Kafka connection and consumer group settings
// needed by the adapter. This is an internal representation — the public
// consumer.Config is mapped to this struct by Consumer.Run().
type AdapterConfig struct {
	Brokers           []string
	GroupID           string
	Topics            []string
	Security          SecurityConfig
	LagReportInterval time.Duration
}

// SecurityConfig mirrors the security settings needed by the Kafka
// adapter. Mapped from consumer.SecurityConfig by Consumer.Run().
type SecurityConfig struct {
	Protocol string
	TLS      *TLSConfig
	SASL     *SASLConfig
}

// TLSConfig holds TLS settings for the Kafka connection.
type TLSConfig struct {
	CAFile             string
	CertFile           string
	KeyFile            string
	InsecureSkipVerify bool
}

// SASLConfig holds SASL authentication settings.
type SASLConfig struct {
	Mechanism         string
	Username          string
	Password          string
	OAuthBearerConfig string
}

// BuildConsumerConfig translates AdapterConfig into a confluent-kafka-go
// ConfigMap suitable for creating a Kafka consumer.
//
// The resulting ConfigMap sets:
//   - bootstrap.servers, group.id
//   - enable.auto.commit = false (FR-6.5)
//   - partition.assignment.strategy = cooperative-sticky (FR-1.8)
//   - auto.offset.reset = earliest (at-least-once semantics, NFR-2.1)
//   - Security protocol, TLS, and SASL settings (NFR-7.1)
func BuildConsumerConfig(cfg AdapterConfig) (*kafka.ConfigMap, error) {
	m := &kafka.ConfigMap{
		"bootstrap.servers":               strings.Join(cfg.Brokers, ","),
		"group.id":                        cfg.GroupID,
		"enable.auto.commit":              false,
		"partition.assignment.strategy":   "cooperative-sticky",
		"auto.offset.reset":               "earliest",
		"go.application.rebalance.enable": true,
		// Emit a *kafka.Stats event through Poll() at the configured interval.
		// Used to derive per-partition consumer lag for Prometheus and KEDA.
		"statistics.interval.ms": cfg.LagReportInterval.Milliseconds(),
	}

	if err := applySecurity(m, cfg.Security); err != nil {
		return nil, fmt.Errorf("kafka config: %w", err)
	}

	return m, nil
}

// applySecurity maps SecurityConfig fields to librdkafka configuration
// properties on the given ConfigMap.
func applySecurity(m *kafka.ConfigMap, sec SecurityConfig) error {
	protocol := sec.Protocol
	if protocol == "" {
		protocol = "plaintext"
	}

	if err := m.SetKey("security.protocol", protocol); err != nil {
		return fmt.Errorf("set security.protocol: %w", err)
	}

	// TLS settings.
	if sec.TLS != nil {
		if sec.TLS.CAFile != "" {
			if err := m.SetKey("ssl.ca.location", sec.TLS.CAFile); err != nil {
				return fmt.Errorf("set ssl.ca.location: %w", err)
			}
		}
		if sec.TLS.CertFile != "" {
			if err := m.SetKey("ssl.certificate.location", sec.TLS.CertFile); err != nil {
				return fmt.Errorf("set ssl.certificate.location: %w", err)
			}
		}
		if sec.TLS.KeyFile != "" {
			if err := m.SetKey("ssl.key.location", sec.TLS.KeyFile); err != nil {
				return fmt.Errorf("set ssl.key.location: %w", err)
			}
		}
		if sec.TLS.InsecureSkipVerify {
			if err := m.SetKey("enable.ssl.certificate.verification", false); err != nil {
				return fmt.Errorf("set enable.ssl.certificate.verification: %w", err)
			}
		}
	}

	// SASL settings.
	if sec.SASL != nil {
		if err := m.SetKey("sasl.mechanism", sec.SASL.Mechanism); err != nil {
			return fmt.Errorf("set sasl.mechanism: %w", err)
		}
		if sec.SASL.Username != "" {
			if err := m.SetKey("sasl.username", sec.SASL.Username); err != nil {
				return fmt.Errorf("set sasl.username: %w", err)
			}
		}
		if sec.SASL.Password != "" {
			if err := m.SetKey("sasl.password", sec.SASL.Password); err != nil {
				return fmt.Errorf("set sasl.password: %w", err)
			}
		}
		if sec.SASL.OAuthBearerConfig != "" {
			if err := m.SetKey("sasl.oauthbearer.config", sec.SASL.OAuthBearerConfig); err != nil {
				return fmt.Errorf("set sasl.oauthbearer.config: %w", err)
			}
		}
	}

	return nil
}
