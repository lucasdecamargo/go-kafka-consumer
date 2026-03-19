package consumer

import "testing"

func TestSecurityConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  SecurityConfig
		wantErr bool
	}{
		{
			name:    "plaintext/default",
			config:  SecurityConfig{},
			wantErr: false,
		},
		{
			name: "ssl with CA",
			config: SecurityConfig{
				Protocol: ProtocolSSL,
				TLS:      &TLSConfig{CAFile: "/ca.pem"},
			},
			wantErr: false,
		},
		{
			name: "ssl without TLS config",
			config: SecurityConfig{
				Protocol: ProtocolSSL,
			},
			wantErr: true,
		},
		{
			name: "ssl without CA or insecure",
			config: SecurityConfig{
				Protocol: ProtocolSSL,
				TLS:      &TLSConfig{},
			},
			wantErr: true,
		},
		{
			name: "ssl with cert but no key",
			config: SecurityConfig{
				Protocol: ProtocolSSL,
				TLS: &TLSConfig{
					CAFile:   "/ca.pem",
					CertFile: "/client.pem",
				},
			},
			wantErr: true,
		},
		{
			name: "ssl with mTLS",
			config: SecurityConfig{
				Protocol: ProtocolSSL,
				TLS: &TLSConfig{
					CAFile:   "/ca.pem",
					CertFile: "/client.pem",
					KeyFile:  "/client.key",
				},
			},
			wantErr: false,
		},
		{
			name: "sasl_plaintext without SASL config",
			config: SecurityConfig{
				Protocol: ProtocolSASLPlaintext,
			},
			wantErr: true,
		},
		{
			name: "sasl_plaintext PLAIN without username",
			config: SecurityConfig{
				Protocol: ProtocolSASLPlaintext,
				SASL: &SASLConfig{
					Mechanism: SASLPlain,
					Password:  "pass",
				},
			},
			wantErr: true,
		},
		{
			name: "sasl_plaintext PLAIN without password",
			config: SecurityConfig{
				Protocol: ProtocolSASLPlaintext,
				SASL: &SASLConfig{
					Mechanism: SASLPlain,
					Username:  "user",
				},
			},
			wantErr: true,
		},
		{
			name: "sasl_plaintext PLAIN valid",
			config: SecurityConfig{
				Protocol: ProtocolSASLPlaintext,
				SASL: &SASLConfig{
					Mechanism: SASLPlain,
					Username:  "user",
					Password:  "pass",
				},
			},
			wantErr: false,
		},
		{
			name: "sasl_ssl requires both TLS and SASL",
			config: SecurityConfig{
				Protocol: ProtocolSASLSSL,
				TLS:      &TLSConfig{CAFile: "/ca.pem"},
			},
			wantErr: true,
		},
		{
			name: "sasl_ssl valid",
			config: SecurityConfig{
				Protocol: ProtocolSASLSSL,
				TLS:      &TLSConfig{CAFile: "/ca.pem"},
				SASL: &SASLConfig{
					Mechanism: SASLSCRAMSHA512,
					Username:  "user",
					Password:  "pass",
				},
			},
			wantErr: false,
		},
		{
			name: "unknown protocol",
			config: SecurityConfig{
				Protocol: "kerberos",
			},
			wantErr: true,
		},
		{
			name: "unknown SASL mechanism",
			config: SecurityConfig{
				Protocol: ProtocolSASLPlaintext,
				SASL: &SASLConfig{
					Mechanism: "GSSAPI",
				},
			},
			wantErr: true,
		},
		{
			name: "oauthbearer without credentials is valid",
			config: SecurityConfig{
				Protocol: ProtocolSASLSSL,
				TLS:      &TLSConfig{CAFile: "/ca.pem"},
				SASL: &SASLConfig{
					Mechanism: SASLOAuthBearer,
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
