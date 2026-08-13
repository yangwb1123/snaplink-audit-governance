package service

import (
	"os"
	"path/filepath"
	"regexp"
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
		for _, needle := range []string{"runtimeconfig.EnvSigningSecret", "runtimeconfig.EnvEncryptionKey", "runtimeconfig.EnvDevSecrets", "runtimeconfig.EnvS3UseSSL", "runtimeconfig.EnvAllowInsecureVaultLoopback"} {
			if !strings.Contains(text, needle) {
				t.Errorf("%s must reference %s (shared env-name constants)", rel, needle)
			}
		}
		for _, needle := range []string{"transport_s3=%s", "transport_vault=%s"} {
			if !strings.Contains(text, needle) {
				t.Errorf("%s must emit per-leg transport labels in check_config=ok (%s)", rel, needle)
			}
		}
		for _, forbidden := range []string{"development-signing-key-change-me", "development-encryption-key-change-me"} {
			if strings.Contains(text, forbidden) {
				t.Errorf("%s must not hard-code the well-known default %q", rel, forbidden)
			}
		}
	}
}

// TestCheckConfigOkLineShapeIdenticalAcrossBinaries pins C6/F11: both
// binaries must print the byte-identical check_config=ok format string
// (same fields, same order). The exact literal is extracted from each main
// and compared, so a drift in either file — field order, names, or the
// transport suffixes — fails the gate.
func TestCheckConfigOkLineShapeIdenticalAcrossBinaries(t *testing.T) {
	binaries := []string{
		filepath.Join("..", "..", "cmd", "audit-api", "main.go"),
		filepath.Join("..", "..", "cmd", "audit-governance-worker", "main.go"),
	}
	formatRe := regexp.MustCompile(`"check_config=ok[^"]*"`)
	var formats []string
	for _, rel := range binaries {
		src, err := os.ReadFile(rel)
		if err != nil {
			t.Fatal(err)
		}
		match := formatRe.FindString(string(src))
		if match == "" {
			t.Fatalf("%s must contain the check_config=ok format literal", rel)
		}
		formats = append(formats, match)
	}
	if formats[0] != formats[1] {
		t.Errorf("check_config=ok format strings differ between binaries:\n  %s\n  %s", formats[0], formats[1])
	}
}
