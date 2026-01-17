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
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsConfig.RootCAs = ca
		}
		tlsConfig.ServerName = cfg.ServerAddress
	}
	return tlsConfig, nil
}
