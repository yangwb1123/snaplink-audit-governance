//go:build !linux

package grpcapi

import "fmt"

// No blocking or unrestricted-path fallback on unsupported platforms.
func readTLSPEMFile(string) ([]byte, error) {
	return nil, fmt.Errorf("bounded TLS PEM file loading requires Linux")
}
