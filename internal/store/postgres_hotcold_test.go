package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

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
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS audit_tenant (
            tenant_id TEXT PRIMARY KEY,
            snapshot JSONB NOT NULL,
            version BIGINT NOT NULL DEFAULT 1,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )`,
		`CREATE TABLE IF NOT EXISTS audit_ledger (
            id BIGSERIAL PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            record_type TEXT NOT NULL CHECK (record_type IN ('receipt', 'segment', 'checkpoint')),
            key TEXT NOT NULL,
            version INTEGER NOT NULL CHECK (version > 0),
            record JSONB NOT NULL,
            written_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (tenant_id, record_type, key, version)
        )`,
		`CREATE INDEX IF NOT EXISTS audit_ledger_tenant_id_idx ON audit_ledger (tenant_id, id)`,
		`CREATE INDEX IF NOT EXISTS audit_ledger_receipt_key_idx ON audit_ledger (tenant_id, record_type, key, version DESC) WHERE record_type = 'receipt'`,
		`CREATE INDEX IF NOT EXISTS audit_ledger_receipt_idempotency_idx ON audit_ledger (tenant_id, ((record->'receipt'->>'idempotency_key'))) WHERE record_type = 'receipt' AND (record->'receipt'->>'idempotency_key') <> ''`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("apply hot/cold schema: %v", err)
		}
	}
	resetHotColdPostgres(t, db)
	return db
}

func resetHotColdPostgres(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`TRUNCATE audit_ledger, audit_tenant`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM audit_state_snapshot`); err != nil {
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
