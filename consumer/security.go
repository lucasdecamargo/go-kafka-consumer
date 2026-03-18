package consumer

import (
	"errors"
	"fmt"
)

// SecurityProtocol defines how the client communicates with Kafka brokers.
// It determines the combination of encryption and authentication used.
type SecurityProtocol string

const (
	// ProtocolPlaintext uses unencrypted, unauthenticated connections.
	// Suitable for development and trusted networks only.
	ProtocolPlaintext SecurityProtocol = "plaintext"

	// ProtocolSSL uses TLS encryption without SASL authentication.
	// Requires TLS configuration (at minimum, a CA certificate or
	// InsecureSkipVerify for testing).
	ProtocolSSL SecurityProtocol = "ssl"

	// ProtocolSASLPlaintext uses SASL authentication without TLS encryption.
	// Not recommended for production — credentials are sent in cleartext.
	ProtocolSASLPlaintext SecurityProtocol = "sasl_plaintext"

	// ProtocolSASLSSL uses both TLS encryption and SASL authentication.
	// Recommended for production environments.
	ProtocolSASLSSL SecurityProtocol = "sasl_ssl"
)

// SASLMechanism defines the SASL authentication mechanism.
type SASLMechanism string

const (
	// SASLPlain uses username/password authentication in plain text.
	// Must be used with TLS encryption (ProtocolSASLSSL) in production.
	SASLPlain SASLMechanism = "PLAIN"

	// SASLSCRAMSHA256 uses Salted Challenge Response Authentication
	// with SHA-256. Provides mutual authentication without sending
	// the password over the wire.
	SASLSCRAMSHA256 SASLMechanism = "SCRAM-SHA-256"

	// SASLSCRAMSHA512 uses Salted Challenge Response Authentication
	// with SHA-512. Preferred over SHA-256 for stronger hashing.
	SASLSCRAMSHA512 SASLMechanism = "SCRAM-SHA-512"

	// SASLOAuthBearer uses OAuth 2.0 bearer token authentication.
	// Requires an OAuthBearerConfig string or a custom token provider.
	SASLOAuthBearer SASLMechanism = "OAUTHBEARER"
)

// TLSConfig holds TLS/SSL settings for Kafka broker connections.
// At minimum, CAFile should be set to verify the broker's certificate.
type TLSConfig struct {
	// CAFile is the path to the CA certificate file used to verify
	// the broker's identity. Required unless InsecureSkipVerify is set.
	CAFile string

	// CertFile is the path to the client certificate file for mutual
	// TLS (mTLS) authentication. Optional — only needed when the broker
	// requires client certificate authentication.
	CertFile string

	// KeyFile is the path to the client private key file for mTLS.
	// Must be set when CertFile is set.
	KeyFile string

	// InsecureSkipVerify disables server certificate verification.
	// WARNING: This makes the connection vulnerable to MITM attacks.
	// Use only for development and testing.
	InsecureSkipVerify bool
}

// SASLConfig holds SASL authentication settings for Kafka connections.
type SASLConfig struct {
	// Mechanism is the SASL authentication mechanism to use.
	Mechanism SASLMechanism

	// Username is the SASL username. Required for PLAIN and SCRAM
	// mechanisms.
	Username string

	// Password is the SASL password. Required for PLAIN and SCRAM
	// mechanisms. Must be sourced from environment variables or a
	// secrets manager — never hardcoded (NFR-7.2).
	Password string

	// OAuthBearerConfig is the configuration string for the OAUTHBEARER
	// mechanism. Only used when Mechanism is SASLOAuthBearer.
	OAuthBearerConfig string
}

// SecurityConfig holds all security-related settings for Kafka connections.
// The zero value uses ProtocolPlaintext with no authentication.
type SecurityConfig struct {
	// Protocol determines the security protocol used for broker
	// connections. Default: ProtocolPlaintext.
	Protocol SecurityProtocol

	// TLS holds TLS settings. Required when Protocol is ProtocolSSL
	// or ProtocolSASLSSL.
	TLS *TLSConfig

	// SASL holds SASL authentication settings. Required when Protocol
	// is ProtocolSASLPlaintext or ProtocolSASLSSL.
	SASL *SASLConfig
}

// Validate checks that all security settings are consistent and complete.
// Returns an error describing the first invalid configuration found.
func (s *SecurityConfig) Validate() error {
	switch s.Protocol {
	case ProtocolPlaintext, "":
		// No additional config needed.

	case ProtocolSSL:
		if err := s.validateTLS(); err != nil {
			return fmt.Errorf("security: protocol %q requires TLS: %w", s.Protocol, err)
		}

	case ProtocolSASLPlaintext:
		if err := s.validateSASL(); err != nil {
			return fmt.Errorf("security: protocol %q requires SASL: %w", s.Protocol, err)
		}

	case ProtocolSASLSSL:
		if err := s.validateTLS(); err != nil {
			return fmt.Errorf("security: protocol %q requires TLS: %w", s.Protocol, err)
		}
		if err := s.validateSASL(); err != nil {
			return fmt.Errorf("security: protocol %q requires SASL: %w", s.Protocol, err)
		}

	default:
		return fmt.Errorf("security: unknown protocol %q", s.Protocol)
	}

	return nil
}

// validateTLS checks TLS configuration completeness.
func (s *SecurityConfig) validateTLS() error {
	if s.TLS == nil {
		return errors.New("TLS configuration must be provided")
	}

	if s.TLS.CAFile == "" && !s.TLS.InsecureSkipVerify {
		return errors.New("either CAFile or InsecureSkipVerify must be set")
	}

	// If one of cert/key is set, both must be set (mTLS).
	if (s.TLS.CertFile == "") != (s.TLS.KeyFile == "") {
		return errors.New("CertFile and KeyFile must both be set for mutual TLS")
	}

	return nil
}

// validateSASL checks SASL configuration completeness.
func (s *SecurityConfig) validateSASL() error {
	if s.SASL == nil {
		return errors.New("SASL configuration must be provided")
	}

	switch s.SASL.Mechanism {
	case SASLPlain, SASLSCRAMSHA256, SASLSCRAMSHA512:
		if s.SASL.Username == "" {
			return fmt.Errorf("SASL %s requires username", s.SASL.Mechanism)
		}
		if s.SASL.Password == "" {
			return fmt.Errorf("SASL %s requires password", s.SASL.Mechanism)
		}

	case SASLOAuthBearer:
		// OAuthBearerConfig is optional — the user may use the
		// built-in token refresh callback instead.

	default:
		return fmt.Errorf("unknown SASL mechanism %q", s.SASL.Mechanism)
	}

	return nil
}
