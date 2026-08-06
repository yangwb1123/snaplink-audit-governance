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

// Environment variable names for the signing/encryption secrets, shared by
// both binaries. A single source for the names underpins the cross-process
// consistency guard: identical env plumbing means the API and worker resolve
// identical keys from the same deployment configuration.
const (
	EnvSigningSecret = "AUDIT_SIGNING_SECRET"
	EnvEncryptionKey = "AUDIT_ENCRYPTION_KEY"
	EnvDevSecrets    = "AUDIT_ALLOW_DEV_SECRETS"
)

// SigningArchive carries the optional external-infrastructure settings.
type SigningArchive struct {
	ArchiveDir      string
	SigningSecret   string
	EncryptionKey   string
	VaultAddr       string
	VaultToken      string
	VaultTransitKey string
	S3Endpoint      string
	S3Bucket        string
	S3AccessKey     string
	S3SecretKey     string
}

// Signer returns the configured checkpoint signer (Vault Transit when all
// three Vault settings are present, HMAC otherwise). A well-known default
// signing secret is rejected whenever any Vault setting is present, so a
// direct caller cannot bypass the service-layer guard (defense in depth;
// validation still completes before any Vault client is constructed).
func (s SigningArchive) Signer() (service.Signer, error) {
	configured := s.VaultAddr != "" || s.VaultToken != "" || s.VaultTransitKey != ""
	if configured && isKnownDefaultSecret(s.SigningSecret) {
		return nil, fmt.Errorf("%w: %s is a well-known default and cannot be combined with Vault Transit", service.ErrDefaultSecret, EnvSigningSecret)
	}
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
// A well-known default encryption key is rejected whenever any S3 setting
// is present (defense in depth, before any S3 client is constructed).
func (s SigningArchive) Archive() (archive.Store, error) {
	configured := s.S3Endpoint != "" || s.S3Bucket != "" || s.S3AccessKey != "" || s.S3SecretKey != ""
	if configured && isKnownDefaultSecret(s.EncryptionKey) {
		return nil, fmt.Errorf("%w: %s is a well-known default and cannot be combined with an S3 archive", service.ErrDefaultSecret, EnvEncryptionKey)
	}
	if configured && (s.S3Endpoint == "" || s.S3Bucket == "" || s.S3AccessKey == "" || s.S3SecretKey == "") {
		return nil, fmt.Errorf("s3 archive requires endpoint, bucket, access key and secret key together")
	}
	if s.S3Endpoint == "" {
		return &archive.FileStore{Dir: s.ArchiveDir}, nil
	}
	return archive.NewS3Store(s.S3Endpoint, s.S3AccessKey, s.S3SecretKey, s.S3Bucket, false)
}

// isKnownDefaultSecret reads the single-source deny-list from the service
// package (exact match only).
func isKnownDefaultSecret(secret string) bool {
	for _, candidate := range service.KnownDefaultSecrets() {
		if secret == candidate {
			return true
		}
	}
	return false
}
