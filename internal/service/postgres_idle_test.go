package service

// The tests in this file exercise a Service over the real PostgreSQL
// snapshot row against AUDIT_TEST_POSTGRES_DSN (migrations 001 and 004
// applied) and skip cleanly when it is unset, mirroring the
// internal/store/postgres_test.go convention so the quality gate stays green
// in CI. They share the single audit_state_snapshot row, so they must never
// call t.Parallel; each test resets the row first.

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// postgresTestDB opens the disposable test database and resets the single
// snapshot row so every test starts from a clean state.
func postgresTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("AUDIT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set AUDIT_TEST_POSTGRES_DSN to run PostgreSQL backend tests")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM audit_state_snapshot`); err != nil {
		t.Fatalf("reset snapshot row: %v", err)
	}
	return db
}

// TestPostgresSnapshotSizeFlatAcrossIdleTicks is AC-3 (end-to-end): a fully
// idle worker pass (SealPendingSegments with no pending hashes +
// CreateAggregateCheckpoint with an unchanged root) must not touch the
// PostgreSQL row at all — version, updated_at and serialized byte size stay
// identical across M idle passes, and the aggregate count does not grow.
func TestPostgresSnapshotSizeFlatAcrossIdleTicks(t *testing.T) {
	db := postgresTestDB(t)
	st, err := store.OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	svc := newServiceOn(t, st)
	seedTestDomain(t, svc)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("pg-idle-1", "op-pg", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("pg-idle-2", "op-pg", at.Add(time.Second)), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	// One real pass seeds the sealed checkpoint + first aggregate record
	// (below the retention cap, so no trim can fire on the idle passes).
	if err := svc.SealPendingSegments("tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateAggregateCheckpoint("tenant-a"); err != nil {
		t.Fatal(err)
	}

	var version int64
	var updatedAt time.Time
	var size int
	if err := db.QueryRow(`SELECT version, updated_at, pg_column_size(snapshot) FROM audit_state_snapshot WHERE id = 1`).Scan(&version, &updatedAt, &size); err != nil {
		t.Fatal(err)
	}
	baselineCount := tenantAggregateCount(t, svc, "tenant-a")

	const passes = 10
	for i := 0; i < passes; i++ {
		if err := svc.SealPendingSegments("tenant-a"); err != nil {
			t.Fatal(err)
		}
		if err := svc.CreateAggregateCheckpoint("tenant-a"); err != nil {
			t.Fatal(err)
		}
	}
	gotVersion, gotUpdatedAt, gotSize := version, updatedAt, size
	if err := db.QueryRow(`SELECT version, updated_at, pg_column_size(snapshot) FROM audit_state_snapshot WHERE id = 1`).Scan(&gotVersion, &gotUpdatedAt, &gotSize); err != nil {
		t.Fatal(err)
	}
	if gotVersion != version || !gotUpdatedAt.Equal(updatedAt) || gotSize != size {
		t.Fatalf("idle passes changed the row: version %d->%d, updated_at %v->%v, size %d->%d", version, gotVersion, updatedAt, gotUpdatedAt, size, gotSize)
	}
	if got := tenantAggregateCount(t, svc, "tenant-a"); got != baselineCount {
		t.Fatalf("aggregate records %d->%d across idle passes, want flat", baselineCount, got)
	}
}
