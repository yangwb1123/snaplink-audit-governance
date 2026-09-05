package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/snaplink/audit-governance/internal/domain"
)

func newHotColdPostgresTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("AUDIT_TEST_POSTGRES_HOTCOLD_DSN")
	if dsn == "" {
		dsn = os.Getenv("AUDIT_TEST_POSTGRES_DSN")
	}
	if dsn == "" {
		t.Skip("set AUDIT_TEST_POSTGRES_HOTCOLD_DSN or AUDIT_TEST_POSTGRES_DSN to run PostgreSQL hot/cold tests")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open hot/cold postgres: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP TABLE IF EXISTS audit_state_snapshot_v1_backup`)
		_, _ = db.Exec(`DROP TABLE IF EXISTS audit_ledger`)
		_, _ = db.Exec(`DROP TABLE IF EXISTS audit_tenant`)
		_ = db.Close()
	})
	if err := db.Ping(); err != nil {
		t.Fatalf("ping hot/cold postgres: %v", err)
	}
	applyHotColdMigrations(t, db)
	resetHotColdPostgres(t, db)
	return db
}

// applyHotColdMigrations makes the DSN-gated suite self-contained. The split
// tables alone are insufficient: OpenPostgres and the reset helper also need
// migration 004's snapshot row and migration 005's trail table. Applying the
// complete ordered set mirrors the disposable database contract used by the
// deployment and prevents a raw database from failing with a missing-table
// error before the control-plane tests start.
func applyHotColdMigrations(t *testing.T, db *sql.DB) {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate repository root for PostgreSQL migrations")
	}
	migrationDir := filepath.Join(filepath.Dir(filename), "..", "..", "migrations")
	for _, name := range []string{
		"001_control_plane.sql",
		"002_source_identity_binding.sql",
		"003_outbox_relay.sql",
		"004_state_snapshot.sql",
		"005_admin_action_trail.sql",
		"006_hot_cold_split.sql",
	} {
		raw, err := os.ReadFile(filepath.Join(migrationDir, name))
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		if _, err := db.Exec(string(raw)); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}
}

func resetHotColdPostgres(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`DROP TABLE IF EXISTS audit_state_snapshot_v1_backup`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`TRUNCATE audit_ledger, audit_tenant`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM audit_state_snapshot`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`TRUNCATE admin_action_trail RESTART IDENTITY`); err != nil {
		t.Fatal(err)
	}
}

type postgresHotColdSize struct {
	control int64
	hot     int64
	ledger  int64
}

func measurePostgresHotColdSize(t *testing.T, db *sql.DB, count int) postgresHotColdSize {
	t.Helper()
	resetHotColdPostgres(t, db)
	st, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateControl(func(data *Snapshot) error {
		data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateTenant("tenant-a", HotFirst, func(view *TenantView) error {
		for i := 0; i < count; i++ {
			eventID := fmt.Sprintf("pg-capacity-%05d", i)
			view.Ledger.SetReceipt(domain.EventReceipt{EventID: eventID, TenantID: "tenant-a", IdempotencyKey: "pg-idem-" + eventID, Status: domain.StatusArchived, StreamID: "tenant-a:source:crm", Sequence: int64(i + 1), Hash: "hash-" + eventID})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var result postgresHotColdSize
	if err := db.QueryRow(`SELECT pg_column_size(snapshot) FROM audit_state_snapshot WHERE id = 1`).Scan(&result.control); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COALESCE(sum(pg_column_size(snapshot)), 0) FROM audit_tenant`).Scan(&result.hot); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COALESCE(sum(pg_column_size(record)), 0) FROM audit_ledger`).Scan(&result.ledger); err != nil {
		t.Fatal(err)
	}
	return result
}

// TestZPostgresHotColdCapacityEnvelope is the A1 PG leg. It is DSN-gated so
// the normal quality gate remains hermetic, but when a disposable database is
// supplied it measures the logical persisted bytes in the control row, hot
// tenant rows and cold ledger records rather than materializing one snapshot.
func TestZPostgresHotColdCapacityEnvelope(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	small := measurePostgresHotColdSize(t, db, 100)
	medium := measurePostgresHotColdSize(t, db, 1000)
	large := measurePostgresHotColdSize(t, db, 5000)
	firstDelta := medium.ledger - small.ledger
	secondDelta := large.ledger - medium.ledger
	if firstDelta <= 0 || secondDelta <= 0 {
		t.Fatalf("cold ledger did not grow: small=%+v medium=%+v large=%+v", small, medium, large)
	}
	if secondDelta > firstDelta*6 {
		t.Fatalf("cold ledger growth is super-linear: 100->1000=%d, 1000->5000=%d", firstDelta, secondDelta)
	}
	if large.control > small.control+1024 || small.control > large.control+1024 {
		t.Fatalf("control row grew with cold history: small=%d large=%d", small.control, large.control)
	}
	if large.hot > small.hot+1024 || small.hot > large.hot+1024 {
		t.Fatalf("tenant hot row grew with cold history: small=%d large=%d", small.hot, large.hot)
	}
}

func TestZPostgresHotColdMigrationHelper(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	legacy := NewSnapshot()
	legacy.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}
	key := EventKey("tenant-a", "migration-event")
	legacy.Events[key] = domain.Event{EventID: "migration-event", TenantID: "tenant-a", StreamID: "tenant-a:source:crm", Sequence: 1, Hash: "migration-hash"}
	legacy.Receipts[key] = domain.EventReceipt{EventID: "migration-event", TenantID: "tenant-a", Status: domain.StatusArchived, StreamID: "tenant-a:source:crm", Sequence: 1, Hash: "migration-hash"}
	encoded, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO audit_state_snapshot (id, snapshot, version) VALUES (1, $1::jsonb, 1)`, string(encoded)); err != nil {
		t.Fatal(err)
	}
	if err := MigratePostgresSnapshot(db); err != nil {
		t.Fatalf("MigratePostgresSnapshot: %v", err)
	}
	var layout string
	if err := db.QueryRow(`SELECT snapshot->>'layout_version' FROM audit_state_snapshot WHERE id = 1`).Scan(&layout); err != nil {
		t.Fatal(err)
	}
	if layout != "2" {
		t.Fatalf("layout marker=%q, want 2", layout)
	}
	var tenants, records, backups int
	if err := db.QueryRow(`SELECT count(*) FROM audit_tenant`).Scan(&tenants); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM audit_ledger`).Scan(&records); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM audit_state_snapshot_v1_backup`).Scan(&backups); err != nil {
		t.Fatal(err)
	}
	if tenants != 1 || records != 1 || backups != 1 {
		t.Fatalf("migration counts tenants=%d records=%d backups=%d, want 1/1/1", tenants, records, backups)
	}
}

func TestZPostgresMigrationRejectsAbsentTargetSchema(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	if _, err := db.Exec(`DROP TABLE audit_ledger, audit_tenant`); err != nil {
		t.Fatal(err)
	}
	if err := MigratePostgresSnapshot(db); err == nil || !strings.Contains(err.Error(), "006") {
		t.Fatalf("migration error=%v, want migration 006 schema error", err)
	}
	var snapshots int
	var backup, tenants, ledger bool
	if err := db.QueryRow(`SELECT count(*) FROM audit_state_snapshot`).Scan(&snapshots); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT to_regclass('audit_state_snapshot_v1_backup') IS NOT NULL`).Scan(&backup); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT to_regclass('audit_tenant') IS NOT NULL`).Scan(&tenants); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT to_regclass('audit_ledger') IS NOT NULL`).Scan(&ledger); err != nil {
		t.Fatal(err)
	}
	if snapshots != 0 || backup || tenants || ledger {
		t.Fatalf("failed cutover mutated database: snapshots=%d backup=%t tenant=%t ledger=%t", snapshots, backup, tenants, ledger)
	}
}

func TestZPostgresStoreRejectsIncompleteCatalog(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	if _, err := db.Exec(`DROP TABLE audit_ledger`); err != nil {
		t.Fatal(err)
	}
	st, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Snapshot(); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("split store accepted incomplete catalog: %v", err)
	}
	if err := st.Update(func(data *Snapshot) error {
		data.LayoutVersion = hotColdLayoutVersion
		return nil
	}); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("split store update accepted incomplete catalog: %v", err)
	}
	var snapshots int
	if err := db.QueryRow(`SELECT count(*) FROM audit_state_snapshot`).Scan(&snapshots); err != nil {
		t.Fatal(err)
	}
	if snapshots != 0 {
		t.Fatalf("incomplete catalog update created snapshot row: %d", snapshots)
	}
	if err := st.Ready(context.Background()); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("readiness accepted incomplete catalog: %v", err)
	}
}

func TestZPostgresMigrationRejectsIncompatibleSchemaBeforeMutation(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	if _, err := db.Exec(`DROP TABLE audit_ledger, audit_tenant`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
CREATE TABLE audit_tenant (
    tenant_id TEXT PRIMARY KEY,
    snapshot JSONB NOT NULL,
    version BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE audit_ledger (
    id BIGINT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    record_type TEXT NOT NULL,
    key TEXT NOT NULL,
    version INTEGER NOT NULL,
    record JSONB NOT NULL,
    written_at TIMESTAMPTZ NOT NULL
);
INSERT INTO audit_tenant VALUES ('tenant-a', '{}', 3, '2025-01-01T00:00:00Z');
INSERT INTO audit_ledger VALUES (1, 'tenant-a', 'receipt', 'key-a', 1, '{}', '2025-01-01T00:00:00Z');
INSERT INTO audit_state_snapshot (id, snapshot, version) VALUES (1, '{"events":{}}', 7);`); err != nil {
		t.Fatal(err)
	}
	var before string
	var sourceVersion int64
	if err := db.QueryRow(`SELECT snapshot::text, version FROM audit_state_snapshot WHERE id = 1`).Scan(&before, &sourceVersion); err != nil {
		t.Fatal(err)
	}
	if err := MigratePostgresSnapshot(db); err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("migration error=%v, want incompatible schema error", err)
	}
	var after string
	var afterVersion int64
	var tenants, records int
	var backups bool
	if err := db.QueryRow(`SELECT snapshot::text, version FROM audit_state_snapshot WHERE id = 1`).Scan(&after, &afterVersion); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM audit_tenant`).Scan(&tenants); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM audit_ledger`).Scan(&records); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT to_regclass('audit_state_snapshot_v1_backup') IS NOT NULL`).Scan(&backups); err != nil {
		t.Fatal(err)
	}
	if before != after || sourceVersion != afterVersion || tenants != 1 || records != 1 || backups {
		t.Fatalf("failed validation mutated database: source %s/%d -> %s/%d, target=%d/%d, backup=%t", before, sourceVersion, after, afterVersion, tenants, records, backups)
	}
	st, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Snapshot(); err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("split store accepted incompatible catalog: %v", err)
	}
}

func TestZPostgresMigrationFreshCutoverReady(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	if err := MigratePostgresSnapshot(db); err != nil {
		t.Fatalf("fresh MigratePostgresSnapshot: %v", err)
	}
	var layout string
	if err := db.QueryRow(`SELECT snapshot->>'layout_version' FROM audit_state_snapshot WHERE id = 1`).Scan(&layout); err != nil {
		t.Fatal(err)
	}
	if layout != "2" {
		t.Fatalf("layout marker=%q, want 2", layout)
	}
	var baselineVersion int64
	if err := db.QueryRow(`SELECT version FROM audit_state_snapshot_v1_backup WHERE id = 1`).Scan(&baselineVersion); err != nil {
		t.Fatal(err)
	}
	if baselineVersion != postgresHotColdZeroBaselineVersion {
		t.Fatalf("zero-data baseline version=%d, want %d", baselineVersion, postgresHotColdZeroBaselineVersion)
	}
	if err := MigratePostgresSnapshot(db); err != nil {
		t.Fatalf("fresh idempotent migration: %v", err)
	}
	st, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Ready(context.Background()); err != nil {
		t.Fatalf("fresh split store readiness: %v", err)
	}
	if _, err := db.Exec(`DROP TABLE audit_state_snapshot_v1_backup`); err != nil {
		t.Fatal(err)
	}
	if err := MigratePostgresSnapshot(db); err == nil || !strings.Contains(err.Error(), "hot/cold data inconsistency") {
		t.Fatalf("v2 marker without baseline migration error=%v, want inconsistency", err)
	}
	if err := st.Ready(context.Background()); err == nil || !strings.Contains(err.Error(), "hot/cold data inconsistency") {
		t.Fatalf("v2 marker without baseline readiness error=%v, want inconsistency", err)
	}
}

func TestZPostgresFirstControlWriteEstablishesBaseline(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	st, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateControl(func(data *Snapshot) error {
		data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}
		return nil
	}); err != nil {
		t.Fatalf("first v2 control write: %v", err)
	}
	var layout string
	if err := db.QueryRow(`SELECT snapshot->>'layout_version' FROM audit_state_snapshot WHERE id = 1`).Scan(&layout); err != nil {
		t.Fatal(err)
	}
	if layout != "2" {
		t.Fatalf("layout marker=%q, want 2", layout)
	}
	var baselineVersion int64
	if err := db.QueryRow(`SELECT version FROM audit_state_snapshot_v1_backup WHERE id = 1`).Scan(&baselineVersion); err != nil {
		t.Fatal(err)
	}
	if baselineVersion != postgresHotColdZeroBaselineVersion {
		t.Fatalf("first-write baseline version=%d, want %d", baselineVersion, postgresHotColdZeroBaselineVersion)
	}
	if err := st.Ready(context.Background()); err != nil {
		t.Fatalf("readiness after first v2 control write: %v", err)
	}
}

func TestZPostgresFreshMigrationRejectsPreexistingTargets(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	if _, err := db.Exec(`
INSERT INTO audit_tenant (tenant_id, snapshot, version)
VALUES ('orphan-tenant', '{"tenant_id":"orphan-tenant","version":1,"events":{},"streams":{}}'::jsonb, 1);
INSERT INTO audit_ledger (tenant_id, record_type, key, version, record)
VALUES ('orphan-tenant', 'receipt', 'orphan-key', 1,
        '{"tenant_id":"orphan-tenant","record_type":"receipt","key":"orphan-key","version":1}'::jsonb)`); err != nil {
		t.Fatal(err)
	}
	if err := MigratePostgresSnapshot(db); !errors.Is(err, ErrHotColdDataInconsistency) {
		t.Fatalf("fresh migration error=%v, want ErrHotColdDataInconsistency", err)
	}

	var snapshots, tenants, records int
	if err := db.QueryRow(`SELECT count(*) FROM audit_state_snapshot`).Scan(&snapshots); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM audit_tenant`).Scan(&tenants); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM audit_ledger`).Scan(&records); err != nil {
		t.Fatal(err)
	}
	var backup bool
	if err := db.QueryRow(`SELECT to_regclass('audit_state_snapshot_v1_backup') IS NOT NULL`).Scan(&backup); err != nil {
		t.Fatal(err)
	}
	if snapshots != 0 || tenants != 1 || records != 1 || backup {
		t.Fatalf("orphan-target rejection mutated state: snapshots=%d tenants=%d records=%d backup=%t", snapshots, tenants, records, backup)
	}
	st, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Ready(context.Background()); !errors.Is(err, ErrHotColdDataInconsistency) {
		t.Fatalf("ready on orphan targets error=%v, want ErrHotColdDataInconsistency", err)
	}
}

func TestZPostgresFreshMigrationConcurrentIsIdempotent(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	dsn := os.Getenv("AUDIT_TEST_POSTGRES_HOTCOLD_DSN")
	if dsn == "" {
		dsn = os.Getenv("AUDIT_TEST_POSTGRES_DSN")
	}
	other, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	if err := other.Ping(); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	var group sync.WaitGroup
	for _, connection := range []*sql.DB{db, other} {
		group.Add(1)
		go func(connection *sql.DB) {
			defer group.Done()
			<-start
			results <- MigratePostgresSnapshot(connection)
		}(connection)
	}
	close(start)
	group.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent fresh migration: %v", err)
		}
	}

	var snapshots, backups, tenants, records int
	if err := db.QueryRow(`SELECT count(*) FROM audit_state_snapshot`).Scan(&snapshots); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM audit_state_snapshot_v1_backup`).Scan(&backups); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM audit_tenant`).Scan(&tenants); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM audit_ledger`).Scan(&records); err != nil {
		t.Fatal(err)
	}
	if snapshots != 1 || backups != 1 || tenants != 0 || records != 0 {
		t.Fatalf("concurrent migration state snapshots=%d backups=%d tenants=%d records=%d, want 1/1/0/0", snapshots, backups, tenants, records)
	}
}

func TestZPostgresMigrationAllowsPostCutoverRows(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	if err := MigratePostgresSnapshot(db); err != nil {
		t.Fatalf("initial migration: %v", err)
	}
	st, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	key := EventKey("tenant-a", "post-cutover-event")
	if err := st.UpdateTenant("tenant-a", HotFirst, func(view *TenantView) error {
		view.Hot.Events[key] = domain.Event{
			EventID: "post-cutover-event", TenantID: "tenant-a",
			StreamID: "tenant-a:source:crm", Sequence: 1, Hash: "post-cutover-hash",
		}
		view.Ledger.SetReceipt(domain.EventReceipt{
			EventID: "post-cutover-event", TenantID: "tenant-a", IdempotencyKey: "post-cutover-idem",
			Status: domain.StatusIndexed, StreamID: "tenant-a:source:crm", Sequence: 1,
			Hash: "post-cutover-hash",
		})
		return nil
	}); err != nil {
		t.Fatalf("post-cutover write: %v", err)
	}
	if err := MigratePostgresSnapshot(db); err != nil {
		t.Fatalf("idempotent migration with post-cutover rows: %v", err)
	}
	if err := st.Ready(context.Background()); err != nil {
		t.Fatalf("readiness with post-cutover rows: %v", err)
	}
	var tenants, records int
	if err := db.QueryRow(`SELECT count(*) FROM audit_tenant`).Scan(&tenants); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM audit_ledger`).Scan(&records); err != nil {
		t.Fatal(err)
	}
	if tenants != 1 || records != 1 {
		t.Fatalf("post-cutover rows tenants=%d records=%d, want 1/1", tenants, records)
	}
}

func TestZPostgresMigrationRollsBackPostWriteFailure(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	legacy := NewSnapshot()
	legacy.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}
	key := EventKey("tenant-a", "rollback-event")
	legacy.Events[key] = domain.Event{EventID: "rollback-event", TenantID: "tenant-a", StreamID: "tenant-a:source:crm", Sequence: 1, Hash: "rollback-hash"}
	legacy.Receipts[key] = domain.EventReceipt{EventID: "rollback-event", TenantID: "tenant-a", Status: domain.StatusArchived, StreamID: "tenant-a:source:crm", Sequence: 1, Hash: "rollback-hash"}
	encoded, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO audit_state_snapshot (id, snapshot, version) VALUES (1, $1::jsonb, 1)`, string(encoded)); err != nil {
		t.Fatal(err)
	}
	const functionName = "snaplink_test_fail_hotcold_ledger"
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP TRIGGER IF EXISTS snaplink_test_fail_hotcold_ledger ON audit_ledger`)
		_, _ = db.Exec(`DROP FUNCTION IF EXISTS snaplink_test_fail_hotcold_ledger()`)
	})
	if _, err := db.Exec(`
CREATE OR REPLACE FUNCTION snaplink_test_fail_hotcold_ledger() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'snaplink_test_fail_hotcold_ledger: rollback trigger';
END;
$$;
CREATE TRIGGER snaplink_test_fail_hotcold_ledger
AFTER INSERT ON audit_ledger
FOR EACH ROW EXECUTE FUNCTION snaplink_test_fail_hotcold_ledger()`); err != nil {
		t.Fatal(err)
	}
	var before string
	var beforeVersion int64
	if err := db.QueryRow(`SELECT snapshot::text, version FROM audit_state_snapshot WHERE id = 1`).Scan(&before, &beforeVersion); err != nil {
		t.Fatal(err)
	}
	if err := MigratePostgresSnapshot(db); err == nil || !strings.Contains(err.Error(), functionName) {
		t.Fatalf("migration error=%v, want rollback trigger failure", err)
	}
	var after string
	var afterVersion int64
	if err := db.QueryRow(`SELECT snapshot::text, version FROM audit_state_snapshot WHERE id = 1`).Scan(&after, &afterVersion); err != nil {
		t.Fatal(err)
	}
	var tenants, records int
	if err := db.QueryRow(`SELECT count(*) FROM audit_tenant`).Scan(&tenants); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM audit_ledger`).Scan(&records); err != nil {
		t.Fatal(err)
	}
	var backup bool
	if err := db.QueryRow(`SELECT to_regclass('audit_state_snapshot_v1_backup') IS NOT NULL`).Scan(&backup); err != nil {
		t.Fatal(err)
	}
	if before != after || beforeVersion != afterVersion || tenants != 0 || records != 0 || backup {
		t.Fatalf("failed migration was not atomic: source %s/%d -> %s/%d, targets=%d/%d, backup=%t", before, beforeVersion, after, afterVersion, tenants, records, backup)
	}
}

func TestZPostgresMigrationRejectsEmptyTargets(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	legacy := NewSnapshot()
	legacy.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}
	key := EventKey("tenant-a", "empty-target-event")
	legacy.Events[key] = domain.Event{EventID: "empty-target-event", TenantID: "tenant-a", StreamID: "tenant-a:source:crm", Sequence: 1, Hash: "empty-target-hash"}
	legacy.Receipts[key] = domain.EventReceipt{EventID: "empty-target-event", TenantID: "tenant-a", Status: domain.StatusArchived, StreamID: "tenant-a:source:crm", Sequence: 1, Hash: "empty-target-hash"}
	encoded, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO audit_state_snapshot (id, snapshot, version) VALUES (1, $1::jsonb, 1)`, string(encoded)); err != nil {
		t.Fatal(err)
	}
	if err := MigratePostgresSnapshot(db); err != nil {
		t.Fatal(err)
	}
	var beforeSnapshot, beforeBackup string
	var beforeVersion, beforeBackupVersion int64
	if err := db.QueryRow(`SELECT snapshot::text, version FROM audit_state_snapshot WHERE id = 1`).Scan(&beforeSnapshot, &beforeVersion); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT snapshot::text, version FROM audit_state_snapshot_v1_backup WHERE id = 1`).Scan(&beforeBackup, &beforeBackupVersion); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM audit_tenant; DELETE FROM audit_ledger`); err != nil {
		t.Fatal(err)
	}
	if err := MigratePostgresSnapshot(db); err == nil || !strings.Contains(err.Error(), "hot/cold data inconsistency") {
		t.Fatalf("empty-target migration error=%v, want inconsistency", err)
	}
	st, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Ready(context.Background()); err == nil || !strings.Contains(err.Error(), "hot/cold data inconsistency") {
		t.Fatalf("empty-target readiness error=%v, want inconsistency", err)
	}
	var afterSnapshot, afterBackup string
	var afterVersion, afterBackupVersion int64
	if err := db.QueryRow(`SELECT snapshot::text, version FROM audit_state_snapshot WHERE id = 1`).Scan(&afterSnapshot, &afterVersion); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT snapshot::text, version FROM audit_state_snapshot_v1_backup WHERE id = 1`).Scan(&afterBackup, &afterBackupVersion); err != nil {
		t.Fatal(err)
	}
	if beforeSnapshot != afterSnapshot || beforeVersion != afterVersion || beforeBackup != afterBackup || beforeBackupVersion != afterBackupVersion {
		t.Fatalf("inconsistency changed durable baseline: source %s/%d -> %s/%d, backup %s/%d -> %s/%d", beforeSnapshot, beforeVersion, afterSnapshot, afterVersion, beforeBackup, beforeBackupVersion, afterBackup, afterBackupVersion)
	}
}

func TestZPostgresMigrationRejectsPartialTargets(t *testing.T) {
	cases := []struct {
		name   string
		mutate string
	}{
		{name: "missing tenant", mutate: `DELETE FROM audit_tenant WHERE tenant_id = 'tenant-b'`},
		{name: "missing ledger", mutate: `DELETE FROM audit_ledger WHERE tenant_id = 'tenant-a'`},
		{name: "mismatched ledger payload", mutate: `UPDATE audit_ledger SET record = jsonb_set(record, '{receipt,status}', '"indexed"'::jsonb) WHERE tenant_id = 'tenant-a'`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := newHotColdPostgresTestDB(t)
			legacy := NewSnapshot()
			for _, tenantID := range []string{"tenant-a", "tenant-b"} {
				legacy.Tenants[tenantID] = domain.Tenant{ID: tenantID, Name: tenantID, Active: true}
				key := EventKey(tenantID, "partial-target-event")
				legacy.Events[key] = domain.Event{EventID: "partial-target-event", TenantID: tenantID, StreamID: tenantID + ":source:crm", Sequence: 1, Hash: tenantID + "-hash"}
				legacy.Receipts[key] = domain.EventReceipt{EventID: "partial-target-event", TenantID: tenantID, Status: domain.StatusArchived, StreamID: tenantID + ":source:crm", Sequence: 1, Hash: tenantID + "-hash"}
			}
			encoded, err := json.Marshal(legacy)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO audit_state_snapshot (id, snapshot, version) VALUES (1, $1::jsonb, 1)`, string(encoded)); err != nil {
				t.Fatal(err)
			}
			if err := MigratePostgresSnapshot(db); err != nil {
				t.Fatal(err)
			}
			var before string
			var beforeVersion int64
			if err := db.QueryRow(`SELECT snapshot::text, version FROM audit_state_snapshot WHERE id = 1`).Scan(&before, &beforeVersion); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(tc.mutate); err != nil {
				t.Fatal(err)
			}
			if err := MigratePostgresSnapshot(db); err == nil || !strings.Contains(err.Error(), "hot/cold data inconsistency") {
				t.Fatalf("migration error=%v, want inconsistency", err)
			}
			st, err := OpenPostgres(db)
			if err != nil {
				t.Fatal(err)
			}
			if err := st.Ready(context.Background()); err == nil || !strings.Contains(err.Error(), "hot/cold data inconsistency") {
				t.Fatalf("readiness error=%v, want inconsistency", err)
			}
			var after string
			var afterVersion int64
			if err := db.QueryRow(`SELECT snapshot::text, version FROM audit_state_snapshot WHERE id = 1`).Scan(&after, &afterVersion); err != nil {
				t.Fatal(err)
			}
			if before != after || beforeVersion != afterVersion {
				t.Fatalf("failed validation changed marker: %s/%d -> %s/%d", before, beforeVersion, after, afterVersion)
			}
		})
	}
}

func TestZPostgresMigrationRejectsInvalidV2Marker(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	if _, err := db.Exec(`INSERT INTO audit_state_snapshot (id, snapshot, version) VALUES (1, '{"layout_version":2}', 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE audit_ledger, audit_tenant`); err != nil {
		t.Fatal(err)
	}
	var before string
	if err := db.QueryRow(`SELECT snapshot::text FROM audit_state_snapshot WHERE id = 1`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := MigratePostgresSnapshot(db); err == nil {
		t.Fatal("invalid v2 marker migration unexpectedly succeeded")
	}
	var after string
	if err := db.QueryRow(`SELECT snapshot::text FROM audit_state_snapshot WHERE id = 1`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("failed validation rewrote marker: before=%s after=%s", before, after)
	}
	st, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Ready(context.Background()); err == nil {
		t.Fatal("readiness accepted v2 marker without target schema")
	}
}

func TestZPostgresTenantCASNoCrossTenantConflicts(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	seed, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.UpdateControl(func(data *Snapshot) error {
		for _, tenantID := range []string{"tenant-a", "tenant-b", "tenant-c", "tenant-d"} {
			data.Tenants[tenantID] = domain.Tenant{ID: tenantID, Name: tenantID, Active: true}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	dsn := os.Getenv("AUDIT_TEST_POSTGRES_HOTCOLD_DSN")
	if dsn == "" {
		dsn = os.Getenv("AUDIT_TEST_POSTGRES_DSN")
	}
	const workers, rounds = 4, 50
	errs := make(chan error, workers)
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker, tenantID := worker, fmt.Sprintf("tenant-%c", 'a'+worker)
		group.Add(1)
		go func() {
			defer group.Done()
			connection, openErr := sql.Open("pgx", dsn)
			if openErr != nil {
				errs <- openErr
				return
			}
			defer connection.Close()
			st, openErr := OpenPostgres(connection)
			if openErr != nil {
				errs <- openErr
				return
			}
			for round := 0; round < rounds; round++ {
				key := EventKey(tenantID, fmt.Sprintf("cross-%d-%d", worker, round))
				if updateErr := st.UpdateTenant(tenantID, HotFirst, func(view *TenantView) error {
					view.Hot.Events[key] = domain.Event{EventID: fmt.Sprintf("cross-%d-%d", worker, round), TenantID: tenantID, StreamID: tenantID + ":source:crm", Sequence: int64(round + 1), Hash: fmt.Sprintf("hash-%d-%d", worker, round)}
					return nil
				}); updateErr != nil {
					errs <- updateErr
					return
				}
			}
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if errors.Is(err, ErrSnapshotConflict) {
			t.Fatalf("cross-tenant update conflicted: %v", err)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	var tenantRows int
	if err := db.QueryRow(`SELECT count(*) FROM audit_tenant`).Scan(&tenantRows); err != nil {
		t.Fatal(err)
	}
	if tenantRows != workers {
		t.Fatalf("tenant rows=%d, want %d", tenantRows, workers)
	}
}

type scriptedTenantCAS struct {
	mu        sync.Mutex
	versions  map[string]int
	conflicts int
}

func (h *scriptedTenantCAS) attempt(tenantID string, ready *sync.WaitGroup, release <-chan struct{}) error {
	h.mu.Lock()
	version := h.versions[tenantID]
	h.mu.Unlock()
	ready.Done()
	<-release
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.versions[tenantID] != version {
		h.conflicts++
		return ErrSnapshotConflict
	}
	h.versions[tenantID] = version + 1
	return nil
}

func runScriptedTenantCAS(t *testing.T, tenants []string, rounds int) int {
	t.Helper()
	harness := &scriptedTenantCAS{versions: map[string]int{}}
	for _, tenantID := range tenants {
		harness.versions[tenantID] = 0
	}
	for round := 0; round < rounds; round++ {
		ready := &sync.WaitGroup{}
		release := make(chan struct{})
		results := make(chan error, len(tenants))
		for _, tenantID := range tenants {
			ready.Add(1)
			go func(id string) { results <- harness.attempt(id, ready, release) }(tenantID)
		}
		ready.Wait()
		close(release)
		for range tenants {
			if err := <-results; err != nil && !errors.Is(err, ErrSnapshotConflict) {
				t.Fatal(err)
			}
		}
	}
	return harness.conflicts
}

// TestTenantCASConflictPartition is the deterministic scripted A4 contract:
// cross-tenant CAS attempts use distinct row versions, while same-tenant
// attempts contend on one row. It mirrors the SQL WHERE tenant/version guard
// and keeps the property testable without requiring PostgreSQL in CI.
func TestTenantCASConflictPartition(t *testing.T) {
	crossTenantConflicts := runScriptedTenantCAS(t, []string{"tenant-a", "tenant-b", "tenant-c", "tenant-d"}, 200)
	sameTenantConflicts := runScriptedTenantCAS(t, []string{"tenant-a", "tenant-a", "tenant-a", "tenant-a"}, 200)
	if crossTenantConflicts != 0 {
		t.Fatalf("cross-tenant conflicts=%d, want 0", crossTenantConflicts)
	}
	if sameTenantConflicts == 0 || crossTenantConflicts*2 >= sameTenantConflicts {
		t.Fatalf("conflict partition cross=%d same=%d, want cross < half of non-zero same-tenant baseline", crossTenantConflicts, sameTenantConflicts)
	}
}

// openHotColdReplica opens an independent connection to the same PostgreSQL
// database and returns a Store that shares the single audit_state_snapshot row.
// This reproduces the multi-replica deployment where cross-writer control-plane
// clobbering was observed.
func openHotColdReplica(t *testing.T, dsn string) *Store {
	t.Helper()
	conn, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open hot/cold replica: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	st, err := OpenPostgres(conn)
	if err != nil {
		t.Fatalf("OpenPostgres replica: %v", err)
	}
	return st
}

// readControlDeadLetters fetches the persisted control-plane dead_letters jsonb
// from the shared snapshot row, decoupling the assertion from any in-memory
// state.
func readControlDeadLetters(t *testing.T, db *sql.DB) map[string]domain.DeadLetter {
	t.Helper()
	var raw string
	if err := db.QueryRow(`SELECT (snapshot->'dead_letters')::text FROM audit_state_snapshot WHERE id = 1`).Scan(&raw); err != nil {
		t.Fatalf("read dead_letters: %v", err)
	}
	out := map[string]domain.DeadLetter{}
	if raw == "" || raw == "null" {
		return out
	}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("decode dead_letters %q: %v", raw, err)
	}
	return out
}

// deadLetterClosure reproduces the two control-plane writes the archive worker
// performs for a permanently un-archivable event: record a DeadLetter keyed by
// EventKey(tenantID, eventID) and reset ArchiveConflictFailures for the tenant.
func deadLetterClosure(tenantID, eventID string) func(*TenantView) error {
	return func(view *TenantView) error {
		key := EventKey(tenantID, eventID)
		view.Global.DeadLetters[key] = domain.DeadLetter{
			TenantID: tenantID, EventID: eventID, Reason: "archive-permanent", At: time.Now(),
		}
		view.Global.ArchiveConflictFailures[tenantID] = 0
		return nil
	}
}

// TestUTCtrl03MergeControlDeltaUnion is the AC-3 unit contract: mergeControlDelta
// must OVERLAY the writer's delta onto the freshly reloaded current snapshot,
// never replace whole map fields. The fixture carries a concurrent writer's
// entry in `current` (absent from both baseline and mutated); a whole-field
// replacement (the old behavior) would silently drop it.
func TestUTCtrl03MergeControlDeltaUnion(t *testing.T) {
	baseline := NewSnapshot()
	mutated := NewSnapshot()
	current := NewSnapshot()

	// A concurrent writer added this entry to the live snapshot after the
	// writer's baseline was captured.
	current.DeadLetters[EventKey("b", "y")] = domain.DeadLetter{TenantID: "b", EventID: "y"}
	current.ArchiveConflictFailures["b"] = 1
	// A distinguishing value in an untouched field that must survive verbatim.
	current.Tenants["b"] = domain.Tenant{ID: "b", Name: "keep"}

	// The writer's own additions only; every other field equals baseline.
	mutated.DeadLetters[EventKey("a", "x")] = domain.DeadLetter{TenantID: "a", EventID: "x"}
	mutated.ArchiveConflictFailures["a"] = 0

	mergeControlDelta(current, baseline, mutated)

	if _, ok := current.DeadLetters[EventKey("a", "x")]; !ok {
		t.Fatalf("writer's DeadLetter dropped: %+v", current.DeadLetters)
	}
	if _, ok := current.DeadLetters[EventKey("b", "y")]; !ok {
		t.Fatalf("concurrent writer's DeadLetter clobbered: %+v", current.DeadLetters)
	}
	if v, ok := current.ArchiveConflictFailures["a"]; !ok || v != 0 {
		t.Fatalf("writer's ArchiveConflictFailures dropped: %+v", current.ArchiveConflictFailures)
	}
	if v, ok := current.ArchiveConflictFailures["b"]; !ok || v != 1 {
		t.Fatalf("concurrent writer's ArchiveConflictFailures clobbered: %+v", current.ArchiveConflictFailures)
	}
	if tv, ok := current.Tenants["b"]; !ok || tv.Name != "keep" {
		t.Fatalf("untouched field overwritten: %+v", current.Tenants)
	}
}

// TestUTCtrl05MergeControlSliceUnion covers the slice fields listed by
// REQ-STORE-CTRL-1. A concurrent append must survive alongside the writer's
// append, and re-applying the same delta on a conflict retry must not grow the
// slices with duplicates.
func TestUTCtrl05MergeControlSliceUnion(t *testing.T) {
	baseline := NewSnapshot()
	mutated := NewSnapshot()
	current := NewSnapshot()
	concurrentAction := domain.AdminAction{ID: "concurrent-action", TenantID: "tenant-b", Action: "tenant.created", CreatedAt: time.Unix(1_700_000_001, 0).UTC()}
	writerAction := domain.AdminAction{ID: "writer-action", TenantID: "tenant-a", Action: "tenant.created", CreatedAt: time.Unix(1_700_000_002, 0).UTC()}
	concurrentCheckpoint := domain.AggregateCheckpoint{ID: "concurrent-checkpoint", TenantID: "tenant-b", Root: "root-b", CreatedAt: time.Unix(1_700_000_003, 0).UTC()}
	writerCheckpoint := domain.AggregateCheckpoint{ID: "writer-checkpoint", TenantID: "tenant-a", Root: "root-a", CreatedAt: time.Unix(1_700_000_004, 0).UTC()}

	current.AdminActions = append(current.AdminActions, concurrentAction)
	current.AggregateCheckpoints = append(current.AggregateCheckpoints, concurrentCheckpoint)
	mutated.AdminActions = append(mutated.AdminActions, writerAction)
	mutated.AggregateCheckpoints = append(mutated.AggregateCheckpoints, writerCheckpoint)

	mergeControlDelta(current, baseline, mutated)
	if len(current.AdminActions) != 2 || !containsAdminAction(current.AdminActions, concurrentAction) || !containsAdminAction(current.AdminActions, writerAction) {
		t.Fatalf("admin action union=%+v, want both concurrent and writer entries", current.AdminActions)
	}
	if len(current.AggregateCheckpoints) != 2 || !containsAggregateCheckpoint(current.AggregateCheckpoints, concurrentCheckpoint) || !containsAggregateCheckpoint(current.AggregateCheckpoints, writerCheckpoint) {
		t.Fatalf("aggregate checkpoint union=%+v, want both concurrent and writer entries", current.AggregateCheckpoints)
	}

	mergeControlDelta(current, baseline, mutated)
	if len(current.AdminActions) != 2 || len(current.AggregateCheckpoints) != 2 {
		t.Fatalf("reapplying slice delta grew union: admin=%d checkpoints=%d, want 2/2", len(current.AdminActions), len(current.AggregateCheckpoints))
	}
}

func containsAdminAction(actions []domain.AdminAction, want domain.AdminAction) bool {
	for _, action := range actions {
		if reflect.DeepEqual(action, want) {
			return true
		}
	}
	return false
}

func containsAggregateCheckpoint(checkpoints []domain.AggregateCheckpoint, want domain.AggregateCheckpoint) bool {
	for _, checkpoint := range checkpoints {
		if reflect.DeepEqual(checkpoint, want) {
			return true
		}
	}
	return false
}

// TestITCtrl01CrossTenantConcurrentArchive is the AC-1 integration contract:
// two independent replicas updating different tenants concurrently must both
// persist their DeadLetters to the shared control row. The old whole-map
// replacement dropped whichever writer committed second.
func TestITCtrl01CrossTenantConcurrentArchive(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	dsn := os.Getenv("AUDIT_TEST_POSTGRES_HOTCOLD_DSN")
	if dsn == "" {
		dsn = os.Getenv("AUDIT_TEST_POSTGRES_DSN")
	}
	seed, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.UpdateControl(func(d *Snapshot) error {
		d.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Active: true}
		d.Tenants["tenant-b"] = domain.Tenant{ID: "tenant-b", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	st1 := openHotColdReplica(t, dsn)
	st2 := openHotColdReplica(t, dsn)

	const rounds = 8
	var wg sync.WaitGroup
	for r := 0; r < rounds; r++ {
		wg.Add(2)
		go func(r int) {
			defer wg.Done()
			if err := st1.UpdateTenant("tenant-a", ColdFirst, deadLetterClosure("tenant-a", fmt.Sprintf("e-a-%d", r))); err != nil {
				t.Errorf("tenant-a update: %v", err)
			}
		}(r)
		go func(r int) {
			defer wg.Done()
			if err := st2.UpdateTenant("tenant-b", ColdFirst, deadLetterClosure("tenant-b", fmt.Sprintf("e-b-%d", r))); err != nil {
				t.Errorf("tenant-b update: %v", err)
			}
		}(r)
	}
	wg.Wait()

	dead := readControlDeadLetters(t, db)
	for r := 0; r < rounds; r++ {
		if _, ok := dead[EventKey("tenant-a", fmt.Sprintf("e-a-%d", r))]; !ok {
			t.Fatalf("tenant-a dead letter e-a-%d dropped; have %d keys: %v", r, len(dead), keysOf(dead))
		}
		if _, ok := dead[EventKey("tenant-b", fmt.Sprintf("e-b-%d", r))]; !ok {
			t.Fatalf("tenant-b dead letter e-b-%d dropped; have %d keys: %v", r, len(dead), keysOf(dead))
		}
	}
}

// TestITCtrl02SameTenantTwoEvents is the AC-2 integration contract: two
// independent replicas updating two distinct events of the SAME tenant must
// both persist their DeadLetters (distinct EventKey prefixes), not clobber
// each other.
func TestITCtrl02SameTenantTwoEvents(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	dsn := os.Getenv("AUDIT_TEST_POSTGRES_HOTCOLD_DSN")
	if dsn == "" {
		dsn = os.Getenv("AUDIT_TEST_POSTGRES_DSN")
	}
	seed, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.UpdateControl(func(d *Snapshot) error {
		d.Tenants["tenant-x"] = domain.Tenant{ID: "tenant-x", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	st1 := openHotColdReplica(t, dsn)
	st2 := openHotColdReplica(t, dsn)

	const rounds = 8
	var wg sync.WaitGroup
	for r := 0; r < rounds; r++ {
		wg.Add(2)
		go func(r int) {
			defer wg.Done()
			if err := st1.UpdateTenant("tenant-x", ColdFirst, deadLetterClosure("tenant-x", fmt.Sprintf("e1-%d", r))); err != nil {
				t.Errorf("e1 update: %v", err)
			}
		}(r)
		go func(r int) {
			defer wg.Done()
			if err := st2.UpdateTenant("tenant-x", ColdFirst, deadLetterClosure("tenant-x", fmt.Sprintf("e2-%d", r))); err != nil {
				t.Errorf("e2 update: %v", err)
			}
		}(r)
	}
	wg.Wait()

	dead := readControlDeadLetters(t, db)
	for r := 0; r < rounds; r++ {
		if _, ok := dead[EventKey("tenant-x", fmt.Sprintf("e1-%d", r))]; !ok {
			t.Fatalf("same-tenant e1-%d dropped; have %d keys: %v", r, len(dead), keysOf(dead))
		}
		if _, ok := dead[EventKey("tenant-x", fmt.Sprintf("e2-%d", r))]; !ok {
			t.Fatalf("same-tenant e2-%d dropped; have %d keys: %v", r, len(dead), keysOf(dead))
		}
	}
}

// TestITCtrl04ConflictRetryNoLoss is the AC-4 integration contract: when a
// control save loses the optimistic-lock race, the retry must reload current and
// re-apply the writer's delta as a non-destructive overlay — never clobber the
// winner's committed entries. A deterministic saveControlHook forces exactly one
// conflict on replica 1 so the retry path is exercised without flaky timing.
func TestITCtrl04ConflictRetryNoLoss(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	dsn := os.Getenv("AUDIT_TEST_POSTGRES_HOTCOLD_DSN")
	if dsn == "" {
		dsn = os.Getenv("AUDIT_TEST_POSTGRES_DSN")
	}
	seed, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.UpdateControl(func(d *Snapshot) error {
		d.Tenants["tenant-c"] = domain.Tenant{ID: "tenant-c", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	st1 := openHotColdReplica(t, dsn)
	st2 := openHotColdReplica(t, dsn)

	var conflictOnce sync.Once
	st1.pgSplit.saveControlHook = func() error {
		fired := false
		conflictOnce.Do(func() { fired = true })
		if fired {
			return ErrSnapshotConflict
		}
		return nil
	}
	defer func() { st1.pgSplit.saveControlHook = nil }()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := st1.UpdateTenant("tenant-c", ColdFirst, deadLetterClosure("tenant-c", "e1")); err != nil {
			t.Errorf("st1 update: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := st2.UpdateTenant("tenant-c", ColdFirst, deadLetterClosure("tenant-c", "e2")); err != nil {
			t.Errorf("st2 update: %v", err)
		}
	}()
	wg.Wait()

	dead := readControlDeadLetters(t, db)
	if _, ok := dead[EventKey("tenant-c", "e1")]; !ok {
		t.Fatalf("retried writer's DeadLetter e1 dropped after conflict; have %d keys: %v", len(dead), keysOf(dead))
	}
	if _, ok := dead[EventKey("tenant-c", "e2")]; !ok {
		t.Fatalf("winner's DeadLetter e2 dropped after conflict; have %d keys: %v", len(dead), keysOf(dead))
	}
}

func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
