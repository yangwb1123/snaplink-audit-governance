package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestRebuildFileLedgerUsesOperatorDeadline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	seed, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Update(context.Background(), func(data *store.Snapshot) error {
		data.Tenants["demo"] = domain.Tenant{ID: "demo", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AUDIT_STORE_OPERATION_TIMEOUT", "25ms")
	ledger, err := openRebuildLedger(context.Background(), path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	if ledger.OperationTimeout() != 25*time.Millisecond {
		t.Fatal("file rebuild ignored operator deadline")
	}
}

func TestInspectScopeBeforeExternalIO(t *testing.T) {
	for _, args := range [][]string{{}, {"-tenant", "demo"}, {"-tenant", "demo", "-stream", "stream", "-hash", "unbound"}, {"-tenant", "demo", "-stream", "stream", "-event", "id"}} {
		if err := runInspectCLI(context.Background(), args, &bytes.Buffer{}); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("args=%v err=%v", args, err)
		}
	}
}

func TestRebuildConfigurationRejectsInvalidEnvironment(t *testing.T) {
	for _, name := range []string{"AUDIT_ALLOW_DEV_SECRETS", "AUDIT_S3_USE_SSL", "AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK", "AUDIT_ARCHIVE_RETENTION_DAYS"} {
		_, _, err := rebuildServiceConfig(func(key string) string {
			if key == name {
				return "credential-must-not-be-logged"
			}
			return ""
		})
		if err == nil || strings.Contains(err.Error(), "credential-must-not-be-logged") {
			t.Fatalf("invalid env %s err=%v", name, err)
		}
	}
	cfg, external, err := rebuildServiceConfig(func(key string) string {
		switch key {
		case "AUDIT_SIGNING_SECRET":
			return "signing"
		case "AUDIT_ENCRYPTION_KEY":
			return "encryption"
		case "AUDIT_S3_USE_SSL":
			return "true"
		case "AUDIT_ARCHIVE_RETENTION_DAYS":
			return "365"
		default:
			return ""
		}
	})
	if err != nil || cfg.SigningSecret != "signing" || cfg.EncryptionKey != "encryption" || !external.S3UseSSL || external.ArchiveRetentionDays != 365 {
		t.Fatal("rebuild configuration diverges from API keys/transport")
	}
}

func TestRebuildBadScopeAndBoundsBeforeExternalIO(t *testing.T) {
	for _, args := range [][]string{{}, {"-tenant", "a:b"}, {"-tenant", "demo", "-page-size", "101"}, {"-tenant", "demo", "-sweeps", "0"}} {
		if err := runRebuildCLI(context.Background(), args, nil); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("args=%v err=%v", args, err)
		}
	}
	if _, err := openRebuildLedger(context.Background(), filepath.Join(t.TempDir(), "missing.json"), ""); err == nil {
		t.Fatal("nonexistent ledger must not be created and called rebuilt")
	}
}

type rebuildCLISink struct {
	writes int
	err    error
}

func (s *rebuildCLISink) Insert(context.Context, domain.Event) error { s.writes++; return s.err }

func TestRebuildSweepsDoesNotClaimCompletenessOrLogErrors(t *testing.T) {
	ledger, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	svc, err := service.New(ledger, service.Config{AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant(context.Background(), "operator", domain.Tenant{ID: "demo", Name: "demo", Active: true}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := rebuildSweeps(context.Background(), svc, &rebuildCLISink{}, "demo", 1, 2, log.New(&output, "", 0)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "completeness=not_asserted") || !strings.Contains(output.String(), "sweep=2") {
		t.Fatal("scan end masquerades as watermark")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	if err := rebuildSweeps(ctx, svc, &rebuildCLISink{}, "demo", 1, 2, nil); err == nil || strings.Contains(err.Error(), "credential") {
		t.Fatal("failed sweep falsely succeeded")
	}
}
