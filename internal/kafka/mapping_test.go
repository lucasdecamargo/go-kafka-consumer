package kafka

import (
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/lucasdecamargo/go-kafka-consumer/consumer"
)

func TestMapMessage_FullMessage(t *testing.T) {
	topic := "test-topic"
	now := time.Now().Truncate(time.Millisecond)

	km := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 3,
			Offset:    42,
		},
		Key:       []byte("msg-key"),
		Value:     []byte("msg-value"),
		Timestamp: now,
		Headers: []kafka.Header{
			{Key: "trace-id", Value: []byte("abc-123")},
			{Key: "content-type", Value: []byte("application/json")},
		},
	}

	msg := mapMessage(km)

	if msg.Topic != "test-topic" {
		t.Errorf("topic: got %q, want %q", msg.Topic, "test-topic")
	}
	if msg.Partition != 3 {
		t.Errorf("partition: got %d, want 3", msg.Partition)
	}
	if msg.Offset != 42 {
		t.Errorf("offset: got %d, want 42", msg.Offset)
	}
	if string(msg.Key) != "msg-key" {
		t.Errorf("key: got %q, want %q", msg.Key, "msg-key")
	}
	if string(msg.Value) != "msg-value" {
		t.Errorf("value: got %q, want %q", msg.Value, "msg-value")
	}
	if !msg.Timestamp.Equal(now) {
		t.Errorf("timestamp: got %v, want %v", msg.Timestamp, now)
	}
	if len(msg.Headers) != 2 {
		t.Fatalf("headers: got %d, want 2", len(msg.Headers))
	}
	if msg.Headers[0].Key != "trace-id" || string(msg.Headers[0].Value) != "abc-123" {
		t.Errorf("header[0]: got %+v", msg.Headers[0])
	}
	if msg.Headers[1].Key != "content-type" || string(msg.Headers[1].Value) != "application/json" {
		t.Errorf("header[1]: got %+v", msg.Headers[1])
	}
}

func TestMapMessage_NilTopic(t *testing.T) {
	km := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     nil,
			Partition: 0,
			Offset:    0,
		},
	}

	msg := mapMessage(km)

	if msg.Topic != "" {
		t.Errorf("topic: got %q, want empty", msg.Topic)
	}
}

func TestMapMessage_NoHeaders(t *testing.T) {
	topic := "t"
	km := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    0,
		},
	}

	msg := mapMessage(km)

	if msg.Headers != nil {
		t.Errorf("headers: got %v, want nil", msg.Headers)
	}
}

func TestMapMessage_NilKeyAndValue(t *testing.T) {
	topic := "t"
	km := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 1,
			Offset:    10,
		},
		Key:   nil,
		Value: nil,
	}

	msg := mapMessage(km)

	if msg.Key != nil {
		t.Errorf("key: got %v, want nil", msg.Key)
	}
	if msg.Value != nil {
		t.Errorf("value: got %v, want nil", msg.Value)
	}
}

func TestMapTopicPartitions(t *testing.T) {
	topic1 := "topic-a"
	topic2 := "topic-b"

	tps := []kafka.TopicPartition{
		{Topic: &topic1, Partition: 0},
		{Topic: &topic1, Partition: 1},
		{Topic: &topic2, Partition: 0},
		{Topic: nil, Partition: 5},
	}

	result := mapTopicPartitions(tps)

	if len(result) != 4 {
		t.Fatalf("got %d partitions, want 4", len(result))
	}

	// Verify each mapping.
	expected := []struct {
		topic     string
		partition int32
	}{
		{"topic-a", 0},
		{"topic-a", 1},
		{"topic-b", 0},
		{"", 5},
	}

	for i, exp := range expected {
		if result[i].Topic != exp.topic {
			t.Errorf("[%d] topic: got %q, want %q", i, result[i].Topic, exp.topic)
		}
		if result[i].Partition != exp.partition {
			t.Errorf("[%d] partition: got %d, want %d", i, result[i].Partition, exp.partition)
		}
	}
}

func TestMapMessage_PreservesHeaderOrder(t *testing.T) {
	topic := "t"
	km := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    0,
		},
		Headers: []kafka.Header{
			{Key: "z-header", Value: []byte("last")},
			{Key: "a-header", Value: []byte("first")},
			{Key: "m-header", Value: []byte("middle")},
		},
	}

	msg := mapMessage(km)

	expectedKeys := []string{"z-header", "a-header", "m-header"}
	for i, key := range expectedKeys {
		if msg.Headers[i].Key != key {
			t.Errorf("header[%d].Key: got %q, want %q", i, msg.Headers[i].Key, key)
		}
	}
}

func TestMapMessage_LargeOffset(t *testing.T) {
	topic := "t"
	km := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    kafka.Offset(9_223_372_036_854_775_000), // near int64 max
		},
	}

	msg := mapMessage(km)

	if msg.Offset != 9_223_372_036_854_775_000 {
		t.Errorf("offset: got %d, want 9223372036854775000", msg.Offset)
	}
}

// TestSecurityConfigValidation tests the SecurityConfig.Validate() method.
func TestSecurityConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  consumer.SecurityConfig
		wantErr bool
	}{
		{
			name:    "plaintext/default",
			config:  consumer.SecurityConfig{},
			wantErr: false,
		},
		{
			name: "ssl with CA",
			config: consumer.SecurityConfig{
				Protocol: consumer.ProtocolSSL,
				TLS:      &consumer.TLSConfig{CAFile: "/ca.pem"},
			},
			wantErr: false,
		},
		{
			name: "ssl without TLS config",
			config: consumer.SecurityConfig{
				Protocol: consumer.ProtocolSSL,
			},
			wantErr: true,
		},
		{
			name: "ssl without CA or insecure",
			config: consumer.SecurityConfig{
				Protocol: consumer.ProtocolSSL,
				TLS:      &consumer.TLSConfig{},
			},
			wantErr: true,
		},
		{
			name: "ssl with cert but no key",
			config: consumer.SecurityConfig{
				Protocol: consumer.ProtocolSSL,
				TLS: &consumer.TLSConfig{
					CAFile:   "/ca.pem",
					CertFile: "/client.pem",
				},
			},
			wantErr: true,
		},
		{
			name: "ssl with mTLS",
			config: consumer.SecurityConfig{
				Protocol: consumer.ProtocolSSL,
				TLS: &consumer.TLSConfig{
					CAFile:   "/ca.pem",
					CertFile: "/client.pem",
					KeyFile:  "/client.key",
				},
			},
			wantErr: false,
		},
		{
			name: "sasl_plaintext without SASL config",
			config: consumer.SecurityConfig{
				Protocol: consumer.ProtocolSASLPlaintext,
			},
			wantErr: true,
		},
		{
			name: "sasl_plaintext PLAIN without username",
			config: consumer.SecurityConfig{
				Protocol: consumer.ProtocolSASLPlaintext,
				SASL: &consumer.SASLConfig{
					Mechanism: consumer.SASLPlain,
					Password:  "pass",
				},
			},
			wantErr: true,
		},
		{
			name: "sasl_plaintext PLAIN without password",
			config: consumer.SecurityConfig{
				Protocol: consumer.ProtocolSASLPlaintext,
				SASL: &consumer.SASLConfig{
					Mechanism: consumer.SASLPlain,
					Username:  "user",
				},
			},
			wantErr: true,
		},
		{
			name: "sasl_plaintext PLAIN valid",
			config: consumer.SecurityConfig{
				Protocol: consumer.ProtocolSASLPlaintext,
				SASL: &consumer.SASLConfig{
					Mechanism: consumer.SASLPlain,
					Username:  "user",
					Password:  "pass",
				},
			},
			wantErr: false,
		},
		{
			name: "sasl_ssl requires both TLS and SASL",
			config: consumer.SecurityConfig{
				Protocol: consumer.ProtocolSASLSSL,
				TLS:      &consumer.TLSConfig{CAFile: "/ca.pem"},
			},
			wantErr: true,
		},
		{
			name: "sasl_ssl valid",
			config: consumer.SecurityConfig{
				Protocol: consumer.ProtocolSASLSSL,
				TLS:      &consumer.TLSConfig{CAFile: "/ca.pem"},
				SASL: &consumer.SASLConfig{
					Mechanism: consumer.SASLSCRAMSHA512,
					Username:  "user",
					Password:  "pass",
				},
			},
			wantErr: false,
		},
		{
			name: "unknown protocol",
			config: consumer.SecurityConfig{
				Protocol: "kerberos",
			},
			wantErr: true,
		},
		{
			name: "unknown SASL mechanism",
			config: consumer.SecurityConfig{
				Protocol: consumer.ProtocolSASLPlaintext,
				SASL: &consumer.SASLConfig{
					Mechanism: "GSSAPI",
				},
			},
			wantErr: true,
		},
		{
			name: "oauthbearer without credentials is valid",
			config: consumer.SecurityConfig{
				Protocol: consumer.ProtocolSASLSSL,
				TLS:      &consumer.TLSConfig{CAFile: "/ca.pem"},
				SASL: &consumer.SASLConfig{
					Mechanism: consumer.SASLOAuthBearer,
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
