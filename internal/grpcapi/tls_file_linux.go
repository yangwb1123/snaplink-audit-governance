//go:build linux

package grpcapi

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const maxTLSPEMBytes = 4 << 20

// TLS paths are OPERATOR configuration, never request input. Pin the configured
// parent as the trust root; unlike mutable data files, relative symlinks inside
// this root are allowed for projected Kubernetes secret mounts (..data/...).
// Escapes, special files and unbounded reads fail closed. No PEM is logged.
func readTLSPEMFile(path string) (raw []byte, err error) {
	if path == "" || strings.IndexByte(path, 0) >= 0 {
		return nil, fmt.Errorf("TLS PEM file path is required")
	}
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	file, err := root.OpenFile(filepath.Base(path), os.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxTLSPEMBytes {
		return nil, fmt.Errorf("TLS PEM requires a regular file of at most %d bytes", maxTLSPEMBytes)
	}
	raw, err = io.ReadAll(io.LimitReader(file, maxTLSPEMBytes+1))
	if err != nil || len(raw) > maxTLSPEMBytes {
		return nil, errors.Join(err, fmt.Errorf("TLS PEM read failed or exceeds %d bytes", maxTLSPEMBytes))
	}
	return raw, nil
}
