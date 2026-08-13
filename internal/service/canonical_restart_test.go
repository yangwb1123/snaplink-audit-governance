package service

// AC-5 restart-persistence tests: an event ingested with a payload int64 >
// 2^53 must verify via VerifyIntegrity after a store reload (file and
// PostgreSQL backends) with an unchanged SourceDigest. The reload decode
// must preserve the exact digits (json.Number) so digest re-derivation
// reproduces the ingest-time digest.

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// reloadService opens a service over st. bootstrap registers the tenant,
// source and schema and must only run on the first (empty) store; after a
// reload they are already part of the persisted snapshot.
func reloadService(t *testing.T, st *store.Store, bootstrap bool) *Service {
	t.Helper()
	svc, err := New(st, Config{SegmentSize: 1000, SigningSecret: "test-secret", Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if !bootstrap {
		return svc
	}
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true, EventsPerSecond: 1000, Burst: 1000}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); err != nil {
		t.Fatal(err)
	}
	return svc
}

func bigIntEvent() domain.Event {
	return domain.Event{EventID: "big-int", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), OperationID: "big-int-op", Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "big-int-idem", AggregateVersion: 9007199254740993, Payload: map[string]any{"resource": "invoice", "value": int64(9007199254740993)}}
}

// assertFileReloadVerifies ingests the large-int event, captures the stored
// SourceDigest, reloads the file-backed store (process restart) and checks
// that VerifyIntegrity passes with the unchanged digest.
func assertFileReloadVerifies(t *testing.T, path string) {
	t.Helper()
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	svc := reloadService(t, st, true)
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, bigIntEvent(), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	stored, err := svc.GetEvent("tenant-a", "test", "big-int")
	if err != nil {
		t.Fatal(err)
	}
	if stored.SourceDigest == "" {
		t.Fatal("ingest must set SourceDigest")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	svc2 := reloadService(t, reopened, false)
	result, err := svc2.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid || len(result.Errors) != 0 {
		t.Fatalf("verify after reload failed: %+v", result)
	}
	after, err := svc2.GetEvent("tenant-a", "test", "big-int")
	if err != nil {
		t.Fatal(err)
	}
	if after.SourceDigest != stored.SourceDigest {
		t.Fatalf("SourceDigest changed across reload: %s != %s", after.SourceDigest, stored.SourceDigest)
	}
}

func TestCanonicalDigestSurvivesFileReload(t *testing.T) {
	assertFileReloadVerifies(t, filepath.Join(t.TempDir(), "state.json"))
}

// TestCanonicalDigestSurvivesPostgresReload is the PostgreSQL leg of AC-5.
// It runs only when AUDIT_TEST_POSTGRES_DSN is set (migrations 001 and 004
// applied) and skips cleanly otherwise, keeping the quality gate green in CI.
func TestCanonicalDigestSurvivesPostgresReload(t *testing.T) {
	dsn := os.Getenv("AUDIT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set AUDIT_TEST_POSTGRES_DSN to run PostgreSQL backend tests")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM audit_state_snapshot`); err != nil {
		t.Fatalf("reset snapshot row: %v", err)
	}
	st, err := store.OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	// The postgres backend shares one snapshot row: seed it through a
	// service, close the store, and verify against a freshly opened store
	// (the same restart shape as the file leg).
	svc := reloadService(t, st, true)
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, bigIntEvent(), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	stored, err := svc.GetEvent("tenant-a", "test", "big-int")
	if err != nil {
		t.Fatal(err)
	}
	if stored.SourceDigest == "" {
		t.Fatal("ingest must set SourceDigest")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	svc2 := reloadService(t, reopened, false)
	result, err := svc2.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid || len(result.Errors) != 0 {
		t.Fatalf("verify after postgres reload failed: %+v", result)
	}
	after, err := svc2.GetEvent("tenant-a", "test", "big-int")
	if err != nil {
		t.Fatal(err)
	}
	if after.SourceDigest != stored.SourceDigest {
		t.Fatalf("SourceDigest changed across postgres reload: %s != %s", after.SourceDigest, stored.SourceDigest)
	}
}
