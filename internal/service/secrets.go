package service

import "errors"

// devSigningSecret and devEncryptionKey are the well-known development
// secrets that previously filled SigningSecret/EncryptionKey silently.
// They are public strings: outside explicit dev mode they must never be
// used, because anyone holding them can forge checkpoint signatures and
// decrypt protected fields and export files. This file is the single
// source of truth for both strings (enforced by the quality gate).
const (
	devSigningSecret = "development-signing-key-change-me"
	devEncryptionKey = "development-encryption-key-change-me"
)

// Sentinel errors reported by New for invalid secret configuration. All
// wrap with %w so callers can test with errors.Is while the message stays
// actionable (it names the environment variable to fix).
var (
	// ErrMissingSecret reports an empty SigningSecret or EncryptionKey
	// outside dev mode.
	ErrMissingSecret = errors.New("service: signing/encryption secret is required")
	// ErrDefaultSecret reports a well-known development secret used outside
	// dev mode.
	ErrDefaultSecret = errors.New("service: well-known default secret is not allowed")
	// ErrSharedSecret reports SigningSecret equal to EncryptionKey outside
	// dev mode (removes the silent key-reuse fallback).
	ErrSharedSecret = errors.New("service: signing and encryption secrets must be distinct")
)

// KnownDefaultSecrets returns the exact well-known development secrets that
// must never be used outside explicit dev mode. Single source of truth:
// service validation, runtimeconfig guards and scanners all read this list.
func KnownDefaultSecrets() []string {
	return []string{devSigningSecret, devEncryptionKey}
}

// isKnownDefaultSecret reports whether secret is an exact match for a
// well-known default. Exact-match only: near misses (for example the
// distinct local-only values used by docker-compose.verify.yml) pass.
func isKnownDefaultSecret(secret string) bool {
	for _, candidate := range KnownDefaultSecrets() {
		if secret == candidate {
			return true
		}
	}
	return false
}
