package kafka

import (
	"fmt"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/lucasdecamargo/go-kafka-consumer/consumer"
)

func baseConfig() consumer.Config {
	cfg := consumer.DefaultConfig()
	cfg.Brokers = []string{"broker1:9092", "broker2:9092"}
	cfg.Topics = []string{"test-topic"}
	cfg.GroupID = "test-group"
	return cfg
}

func TestBuildConsumerConfig_Defaults(t *testing.T) {
	cfg := baseConfig()

	m, err := BuildConsumerConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertConfigValue(t, m, "bootstrap.servers", "broker1:9092,broker2:9092")
	assertConfigValue(t, m, "group.id", "test-group")
	assertConfigValue(t, m, "enable.auto.commit", false)
	assertConfigValue(t, m, "partition.assignment.strategy", "cooperative-sticky")
	assertConfigValue(t, m, "auto.offset.reset", "earliest")
	assertConfigValue(t, m, "security.protocol", "plaintext")
}

func TestBuildConsumerConfig_SSL(t *testing.T) {
	cfg := baseConfig()
	cfg.Security = consumer.SecurityConfig{
		Protocol: consumer.ProtocolSSL,
		TLS: &consumer.TLSConfig{
			CAFile:   "/certs/ca.pem",
			CertFile: "/certs/client.pem",
			KeyFile:  "/certs/client.key",
		},
	}

	m, err := BuildConsumerConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertConfigValue(t, m, "security.protocol", "ssl")
	assertConfigValue(t, m, "ssl.ca.location", "/certs/ca.pem")
	assertConfigValue(t, m, "ssl.certificate.location", "/certs/client.pem")
	assertConfigValue(t, m, "ssl.key.location", "/certs/client.key")
}

func TestBuildConsumerConfig_SSL_InsecureSkipVerify(t *testing.T) {
	cfg := baseConfig()
	cfg.Security = consumer.SecurityConfig{
		Protocol: consumer.ProtocolSSL,
		TLS: &consumer.TLSConfig{
			InsecureSkipVerify: true,
		},
	}

	m, err := BuildConsumerConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertConfigValue(t, m, "security.protocol", "ssl")
	assertConfigValue(t, m, "enable.ssl.certificate.verification", false)
}

func TestBuildConsumerConfig_SASL_Plain(t *testing.T) {
	cfg := baseConfig()
	cfg.Security = consumer.SecurityConfig{
		Protocol: consumer.ProtocolSASLPlaintext,
		SASL: &consumer.SASLConfig{
			Mechanism: consumer.SASLPlain,
			Username:  "user",
			Password:  "secret",
		},
	}

	m, err := BuildConsumerConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertConfigValue(t, m, "security.protocol", "sasl_plaintext")
	assertConfigValue(t, m, "sasl.mechanism", "PLAIN")
	assertConfigValue(t, m, "sasl.username", "user")
	assertConfigValue(t, m, "sasl.password", "secret")
}

func TestBuildConsumerConfig_SASL_SCRAM256(t *testing.T) {
	cfg := baseConfig()
	cfg.Security = consumer.SecurityConfig{
		Protocol: consumer.ProtocolSASLSSL,
		TLS: &consumer.TLSConfig{
			CAFile: "/certs/ca.pem",
		},
		SASL: &consumer.SASLConfig{
			Mechanism: consumer.SASLSCRAMSHA256,
			Username:  "user",
			Password:  "secret",
		},
	}

	m, err := BuildConsumerConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertConfigValue(t, m, "security.protocol", "sasl_ssl")
	assertConfigValue(t, m, "sasl.mechanism", "SCRAM-SHA-256")
	assertConfigValue(t, m, "ssl.ca.location", "/certs/ca.pem")
}

func TestBuildConsumerConfig_SASL_SCRAM512(t *testing.T) {
	cfg := baseConfig()
	cfg.Security = consumer.SecurityConfig{
		Protocol: consumer.ProtocolSASLSSL,
		TLS: &consumer.TLSConfig{
			CAFile: "/certs/ca.pem",
		},
		SASL: &consumer.SASLConfig{
			Mechanism: consumer.SASLSCRAMSHA512,
			Username:  "user",
			Password:  "secret",
		},
	}

	m, err := BuildConsumerConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertConfigValue(t, m, "sasl.mechanism", "SCRAM-SHA-512")
}

func TestBuildConsumerConfig_OAuthBearer(t *testing.T) {
	cfg := baseConfig()
	cfg.Security = consumer.SecurityConfig{
		Protocol: consumer.ProtocolSASLSSL,
		TLS: &consumer.TLSConfig{
			CAFile: "/certs/ca.pem",
		},
		SASL: &consumer.SASLConfig{
			Mechanism:         consumer.SASLOAuthBearer,
			OAuthBearerConfig: "scope=openid",
		},
	}

	m, err := BuildConsumerConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertConfigValue(t, m, "sasl.mechanism", "OAUTHBEARER")
	assertConfigValue(t, m, "sasl.oauthbearer.config", "scope=openid")
}

func TestBuildConsumerConfig_NoSASLFields_WhenPlaintext(t *testing.T) {
	cfg := baseConfig()

	m, err := BuildConsumerConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// SASL keys should not be set — Get with a default should return the default.
	v, _ := m.Get("sasl.mechanism", "NOT_SET")
	if fmt.Sprintf("%v", v) != "NOT_SET" {
		t.Errorf("expected sasl.mechanism to not be set, got %v", v)
	}
}

// assertConfigValue checks that a ConfigMap key has the expected value.
func assertConfigValue(t *testing.T, m *kafka.ConfigMap, key string, expected interface{}) {
	t.Helper()

	val, err := m.Get(key, nil)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}

	// Compare using string representation to handle ConfigValue types.
	got := fmt.Sprintf("%v", val)
	want := fmt.Sprintf("%v", expected)
	if got != want {
		t.Errorf("%s: got %v (%T), want %v (%T)", key, val, val, expected, expected)
	}
}
