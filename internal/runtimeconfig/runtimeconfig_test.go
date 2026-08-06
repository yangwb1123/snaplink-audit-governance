package runtimeconfig

import (
	"errors"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/service"
)

func TestSignerSelection(t *testing.T) {
	cfg := SigningArchive{SigningSecret: "secret"}
	signer, err := cfg.Signer()
	if err != nil || signer != nil {
		t.Fatalf("no vault config: signer=%v err=%v, want nil/nil", signer, err)
	}

	cfg = SigningArchive{SigningSecret: "secret", VaultAddr: "http://vault:8200", VaultToken: "t", VaultTransitKey: "audit-checkpoints"}
	signer, err = cfg.Signer()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := signer.(*security.VaultTransitSigner); !ok {
		t.Fatalf("signer type=%T, want *security.VaultTransitSigner", signer)
	}
	if signer.Algorithm() != "vault-transit:audit-checkpoints" {
		t.Fatalf("algorithm=%s", signer.Algorithm())
	}

	// 部分配置必须报错而不是静默回退到 HMAC。
	cfg = SigningArchive{SigningSecret: "secret", VaultAddr: "http://vault:8200"}
	if _, err := cfg.Signer(); err == nil || !strings.Contains(err.Error(), "together") {
		t.Fatalf("partial vault config err=%v, want validation error", err)
	}
}

func TestArchiveSelection(t *testing.T) {
	cfg := SigningArchive{ArchiveDir: "/tmp/archive"}
	store, err := cfg.Archive()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.(*archive.FileStore); !ok {
		t.Fatalf("default archive type=%T, want *archive.FileStore", store)
	}

	cfg = SigningArchive{ArchiveDir: "/tmp/archive", S3Endpoint: "localhost:9000", S3Bucket: "worm", S3AccessKey: "k", S3SecretKey: "s"}
	store, err = cfg.Archive()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.(*archive.S3Store); !ok {
		t.Fatalf("s3 archive type=%T, want *archive.S3Store", store)
	}

	cfg = SigningArchive{S3Endpoint: "localhost:9000"}
	if _, err := cfg.Archive(); err == nil {
		t.Fatal("partial s3 config must fail")
	}
}

// TestDefaultsSatisfyService ensures the service layer accepts the
// defaults this package produces.
func TestDefaultsSatisfyService(t *testing.T) {
	cfg := SigningArchive{SigningSecret: "secret", ArchiveDir: t.TempDir()}
	signer, err := cfg.Signer()
	if err != nil {
		t.Fatal(err)
	}
	store, err := cfg.Archive()
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(nil, service.Config{SigningSecret: cfg.SigningSecret, ArchiveDir: cfg.ArchiveDir, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if svc.Config.Signer == nil || svc.Config.Archive == nil {
		t.Fatal("service defaults missing")
	}
	_ = signer
	_ = store
}

// TestSignerRejectsDefaultSecretWithVault pins REQ-5/T10: a well-known
// default signing secret is rejected whenever any Vault setting is present,
// before any Vault client is constructed.
func TestSignerRejectsDefaultSecretWithVault(t *testing.T) {
	cfg := SigningArchive{SigningSecret: "development-signing-key-change-me", VaultAddr: "http://vault:8200", VaultToken: "t", VaultTransitKey: "audit-checkpoints"}
	if _, err := cfg.Signer(); !errors.Is(err, service.ErrDefaultSecret) {
		t.Fatalf("err=%v, want ErrDefaultSecret", err)
	}
}

// TestArchiveRejectsDefaultSecretWithS3 pins REQ-5/T10: a well-known
// default encryption key is rejected whenever any S3 setting is present,
// before any S3 client is constructed.
func TestArchiveRejectsDefaultSecretWithS3(t *testing.T) {
	cfg := SigningArchive{EncryptionKey: "development-encryption-key-change-me", S3Endpoint: "localhost:9000", S3Bucket: "worm", S3AccessKey: "k", S3SecretKey: "s"}
	if _, err := cfg.Archive(); !errors.Is(err, service.ErrDefaultSecret) {
		t.Fatalf("err=%v, want ErrDefaultSecret", err)
	}
}

// TestPartialExternalConfigRejectedWithDefaultSecret covers T11 for the
// default-secret corner: partial Vault/S3 configuration is rejected (never
// a silent HMAC/file fallback) even when the secret is a known default.
func TestPartialExternalConfigRejectedWithDefaultSecret(t *testing.T) {
	cfg := SigningArchive{SigningSecret: "development-signing-key-change-me", VaultAddr: "http://vault:8200"}
	if _, err := cfg.Signer(); err == nil {
		t.Fatal("partial vault config must fail")
	}
	cfg = SigningArchive{EncryptionKey: "development-encryption-key-change-me", S3Endpoint: "localhost:9000"}
	if _, err := cfg.Archive(); err == nil {
		t.Fatal("partial s3 config must fail")
	}
}

// TestEnvNameConstants pins the shared env-var names (T12 golden): both
// binaries resolve secrets from these constants, so identical deployment
// input means identical resolved keys across processes.
func TestEnvNameConstants(t *testing.T) {
	if EnvSigningSecret != "AUDIT_SIGNING_SECRET" {
		t.Fatalf("EnvSigningSecret=%q", EnvSigningSecret)
	}
	if EnvEncryptionKey != "AUDIT_ENCRYPTION_KEY" {
		t.Fatalf("EnvEncryptionKey=%q", EnvEncryptionKey)
	}
	if EnvDevSecrets != "AUDIT_ALLOW_DEV_SECRETS" {
		t.Fatalf("EnvDevSecrets=%q", EnvDevSecrets)
	}
}
