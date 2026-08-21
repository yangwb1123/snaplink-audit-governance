package grpcapi

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
)

// ServerCredentials loads and validates the PEM cert/key pair and returns
// grpc transport credentials for the ingest listener. Errors are returned
// (never logged inside) so callers fail fast with the same text at
// check-config and startup.
//
// tls.LoadX509KeyPair validates that both files exist, parse as PEM, and
// that the key matches the cert; the returned credentials are then the only
// transport credential on the listener, so grpc-go rejects plaintext (h2c)
// clients at the handshake before any RPC — the authorization bearer
// metadata can never be sent on a plaintext channel (RFC 6750 §1). No
// MinVersion/cipher overrides (grpc-go defaults; server-side hardening is a
// non-goal) and no network access, keeping check-config deterministic.
func ServerCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(serverTLSConfig(certificate)), nil
}

// MutualTLSCredentials loads the server certificate and a trusted client CA.
// ClientAuth is RequireAndVerifyClientCert, so a bearer token can only reach
// an RPC after the peer has presented a certificate signed by the configured
// CA. The CA file is deliberately explicit rather than inferred from the
// server certificate bundle.
func MutualTLSCredentials(certFile, keyFile, clientCAFile string) (credentials.TransportCredentials, error) {
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	caPEM, err := os.ReadFile(clientCAFile)
	if err != nil {
		return nil, err
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("client CA file contains no certificates")
	}
	config := serverTLSConfig(certificate)
	config.ClientCAs = clientCAs
	config.ClientAuth = tls.RequireAndVerifyClientCert
	return credentials.NewTLS(config), nil
}

func serverTLSConfig(certificate tls.Certificate) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	}
}
