package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestEnvConsistencyAcrossBinaries pins AC-3 at source level: both binaries
// must resolve the signing/encryption secrets from the runtimeconfig
// env-name constants (identical plumbing => identical resolved keys for the
// same deployment input) and must not hard-code the well-known defaults
// anywhere else (single-source deny-list).
func TestEnvConsistencyAcrossBinaries(t *testing.T) {
	binaries := []string{
		filepath.Join("..", "..", "cmd", "audit-api", "main.go"),
		filepath.Join("..", "..", "cmd", "audit-governance-worker", "main.go"),
	}
	for _, rel := range binaries {
		src, err := os.ReadFile(rel)
		if err != nil {
			t.Fatal(err)
		}
		text := string(src)
		for _, needle := range []string{"runtimeconfig.EnvSigningSecret", "runtimeconfig.EnvEncryptionKey", "runtimeconfig.EnvDevSecrets"} {
			if !strings.Contains(text, needle) {
				t.Errorf("%s must reference %s (shared env-name constants)", rel, needle)
			}
		}
		for _, forbidden := range []string{"development-signing-key-change-me", "development-encryption-key-change-me"} {
			if strings.Contains(text, forbidden) {
				t.Errorf("%s must not hard-code the well-known default %q", rel, forbidden)
			}
		}
	}
}
