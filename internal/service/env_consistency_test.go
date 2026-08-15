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
// binaries must print the same check_config=ok field set and order for the
// fields they share (signing/encryption lengths, signer, archive, transport
// labels). The audit-api line is a deliberate superset: it also reports
// jwt_secret_length (the local HS256 trust-source metric) because the API
// carries an authenticator; the worker has no JWT trust source, so its line
// is unchanged. The exact literals are extracted from each main and compared
// so a drift in either file — field order, names, or the transport
// suffixes — fails the gate.
func TestCheckConfigOkLineShapeIdenticalAcrossBinaries(t *testing.T) {
	apiSrc, err := os.ReadFile(filepath.Join("..", "..", "cmd", "audit-api", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	workerSrc, err := os.ReadFile(filepath.Join("..", "..", "cmd", "audit-governance-worker", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	formatRe := regexp.MustCompile(`"check_config=ok[^"]*"`)
	apiFormat := formatRe.FindString(string(apiSrc))
	workerFormat := formatRe.FindString(string(workerSrc))
	if apiFormat == "" || workerFormat == "" {
		t.Fatal("both binaries must contain the check_config=ok format literal")
	}
	// The worker line is unchanged; the API line is exactly the worker line
	// with jwt_secret_length inserted after encryption_key_length.
	wantAPILine := strings.Replace(workerFormat, "encryption_key_length=%d ", "encryption_key_length=%d jwt_secret_length=%d ", 1)
	if apiFormat != wantAPILine {
		t.Errorf("check_config=ok format strings must agree except for the API-only jwt_secret_length field:\n  api:    %s\n  worker: %s", apiFormat, workerFormat)
	}
}
