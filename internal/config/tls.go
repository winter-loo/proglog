package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type TLSConfig struct {
	CAFile        string
	CertFile      string
	KeyFile       string
	ServerAddress string
	Server        bool
}

// SetupTLSConfig returns a *tls.Config for the given TLSConfig.
// It loads certificates and CA files to configure mutual TLS.
// If Server is true, it configures the server-side TLS, requiring and verifying client certificates.
// If Server is false, it configures the client-side TLS, using the CA to verify the server.
// https://blog.cloudflare.com/how-to-build-your-own-public-key-infrastructure
func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	var err error
	tlsConfig := &tls.Config{}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}
	}

	if cfg.CAFile != "" {
		b, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		ca := x509.NewCertPool()
		ok := ca.AppendCertsFromPEM([]byte(b))
		if !ok {
			return nil, fmt.Errorf("failed to parse root certificate: %q", cfg.CAFile)
		}

		if cfg.Server {
			// Ther server is configured for Mutual TLS
			//
			// This tell the server which CAs to use when verifying certificates presented
			// by incoming clients
			tlsConfig.ClientCAs = ca
			// This is the "Mutual" part. It instructs the server to strictly require a
			// certificate from the client and verify it against the ClientCAs. If the
			// client doesn't provide a valid certificate signed by your CA, the connection
			// is rejected.
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			// The client is being configured to verify the server it connects to
			//
			// This tells the client which CAs to trust when checking the server's
			// identity. This ensures the client doesn't connect to a malicious server
			// with a self-signed or unrecognized certificate.
			tlsConfig.RootCAs = ca
		}
		tlsConfig.ServerName = cfg.ServerAddress
	}
	return tlsConfig, nil
}
