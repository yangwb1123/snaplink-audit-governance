// Package runtimeconfig builds the shared signing/archive configuration for
// the audit-api and audit-governance-worker binaries so both processes use
// the same signature scheme and archive destination (a worker sealing
// segments with HMAC while the API signs with Vault would break the
// evidence chain).
package runtimeconfig

import (
	"fmt"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/service"
)

// SigningArchive carries the optional external-infrastructure settings.
type SigningArchive struct {
	ArchiveDir      string
	SigningSecret   string
	VaultAddr       string
	VaultToken      string
	VaultTransitKey string
	S3Endpoint      string
	S3Bucket        string
	S3AccessKey     string
	S3SecretKey     string
}

// Signer returns the configured checkpoint signer (Vault Transit when all
// three Vault settings are present, HMAC otherwise).
func (s SigningArchive) Signer() (service.Signer, error) {
	configured := s.VaultAddr != "" || s.VaultToken != "" || s.VaultTransitKey != ""
	if configured && (s.VaultAddr == "" || s.VaultToken == "" || s.VaultTransitKey == "") {
		return nil, fmt.Errorf("vault signing requires addr, token and transit key together")
	}
	if s.VaultAddr == "" {
		return nil, nil
	}
	return security.NewVaultTransitSigner(s.VaultAddr, s.VaultToken, s.VaultTransitKey), nil
}

// Archive returns the configured compliance archive (S3 Object Lock when
// all four S3 settings are present, local read-only directory otherwise).
func (s SigningArchive) Archive() (archive.Store, error) {
	configured := s.S3Endpoint != "" || s.S3Bucket != "" || s.S3AccessKey != "" || s.S3SecretKey != ""
	if configured && (s.S3Endpoint == "" || s.S3Bucket == "" || s.S3AccessKey == "" || s.S3SecretKey == "") {
		return nil, fmt.Errorf("s3 archive requires endpoint, bucket, access key and secret key together")
	}
	if s.S3Endpoint == "" {
		return &archive.FileStore{Dir: s.ArchiveDir}, nil
	}
	return archive.NewS3Store(s.S3Endpoint, s.S3AccessKey, s.S3SecretKey, s.S3Bucket, false)
}
