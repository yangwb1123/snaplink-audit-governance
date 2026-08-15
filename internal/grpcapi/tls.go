package grpcapi

import (
	"crypto/tls"

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
	return credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}}), nil
}
