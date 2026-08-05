package runtimeconfig

import (
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
	svc := service.New(nil, service.Config{SigningSecret: cfg.SigningSecret, ArchiveDir: cfg.ArchiveDir})
	if svc.Config.Signer == nil || svc.Config.Archive == nil {
		t.Fatal("service defaults missing")
	}
	_ = signer
	_ = store
}
