package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/snaplink/audit-governance/internal/domain"
)

// TestPostgresBackend exercises the real PostgreSQL snapshot row. It is
// skipped unless AUDIT_TEST_POSTGRES_DSN points at a disposable database
// (migrations 001 and 004 applied).
func TestPostgresBackend(t *testing.T) {
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

	first, err := OpenPostgres(db)
	if err != nil {
		t.Fatalf("OpenPostgres: %v", err)
	}
	if err := first.Update(func(data *Snapshot) error {
		data.Tenants["demo"] = domain.Tenant{ID: "demo", Name: "Demo", Active: true}
		return nil
	}); err != nil {
		t.Fatalf("first Update: %v", err)
	}

	// A second store instance (fresh connection view) must observe the write.
	second, err := OpenPostgres(db)
	if err != nil {
		t.Fatalf("second OpenPostgres: %v", err)
	}
	var tenant domain.Tenant
	if err := second.Read(func(data *Snapshot) error {
		var ok bool
		tenant, ok = data.Tenants["demo"]
		if !ok {
			t.Fatal("tenant demo missing after reload")
		}
		return nil
	}); err != nil {
		t.Fatalf("second Read: %v", err)
	}
	if tenant.Name != "Demo" {
		t.Fatalf("tenant name=%q", tenant.Name)
	}

	// Optimistic lock: both backends load version N, the first saves, the
	// second must fail instead of overwriting.
	firstBackend := &postgresBackend{db: db}
	secondBackend := &postgresBackend{db: db}
	stale, err := secondBackend.Load()
	if err != nil {
		t.Fatalf("stale load: %v", err)
	}
	fresh, err := firstBackend.Load()
	if err != nil {
		t.Fatalf("fresh load: %v", err)
	}
	if err := firstBackend.Save(fresh); err != nil {
		t.Fatalf("fresh save: %v", err)
	}
	if err := secondBackend.Save(stale); !errors.Is(err, ErrSnapshotConflict) {
		t.Fatalf("stale save error=%v, want ErrSnapshotConflict", err)
	}

	// Conflict must not corrupt the row: newest state still readable.
	var count int
	if err := second.Read(func(data *Snapshot) error {
		count = len(data.Tenants)
		return nil
	}); err != nil {
		t.Fatalf("read after conflict: %v", err)
	}
	if count != 1 {
		t.Fatalf("tenants=%d after conflict, want 1", count)
	}
}
