package store

// The tests in this file exercise the real PostgreSQL snapshot row against
// AUDIT_TEST_POSTGRES_DSN (migrations 001, 004 and 005 applied; 005 is
// applied by newPostgresTestDB so the readyz trail-table probe stays green)
// and skip cleanly when it is unset, keeping the quality gate green in CI.
// They share one row (audit_state_snapshot id=1), so they must never call
// t.Parallel; each test resets the row first via newPostgresTestDB.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/snaplink/audit-governance/internal/domain"
)

// newPostgresTestDB opens the disposable test database and resets the
// single snapshot row so every test starts from a clean state.
func newPostgresTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("AUDIT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set AUDIT_TEST_POSTGRES_DSN to run PostgreSQL backend tests")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	// Hot/cold integration tests may run against the same disposable DSN.
	// Always restore the legacy schema before a snapshot-row test so test
	// order cannot accidentally select the split backend.
	for _, statement := range []string{
		`DROP TABLE IF EXISTS audit_state_snapshot_v1_backup`,
		`DROP TABLE IF EXISTS audit_ledger`,
		`DROP TABLE IF EXISTS audit_tenant`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("reset hot/cold tables: %v", err)
		}
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM audit_state_snapshot`); err != nil {
		t.Fatalf("reset snapshot row: %v", err)
	}
	// Apply migration 005 (idempotent) so the readyz trail-table probe and
	// trail tests see the table from the start.
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS admin_action_trail (
    seq         BIGSERIAL PRIMARY KEY,
    id          TEXT NOT NULL,
    tenant_id   TEXT NOT NULL,
    actor       TEXT NOT NULL,
    action      TEXT NOT NULL,
    target_type TEXT NOT NULL DEFAULT '',
    target_id   TEXT NOT NULL DEFAULT '',
    detail      TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL
)`); err != nil {
		t.Fatalf("apply migration 005: %v", err)
	}
	if _, err := db.ExecContext(ctx, `TRUNCATE admin_action_trail RESTART IDENTITY`); err != nil {
		t.Fatalf("reset admin action trail: %v", err)
	}
	return db
}

// seedTenant writes one tenant through a store, leaving the row at version 2.
func seedTenant(t *testing.T, db *sql.DB) *Store {
	t.Helper()
	st, err := OpenPostgres(db)
	if err != nil {
		t.Fatalf("seed OpenPostgres: %v", err)
	}
	if err := st.Update(func(data *Snapshot) error {
		data.Tenants["demo"] = domain.Tenant{ID: "demo", Name: "Demo", Active: true}
		return nil
	}); err != nil {
		t.Fatalf("seed Update: %v", err)
	}
	return st
}

// T-4: optimistic lock. Both backends load version N, the first saves, the
// second must fail instead of overwriting. White-box baseline priming uses
// LoadForUpdate (the contract-correct read-modify-write entry point);
// Load is side-effect-free and can no longer prime the baseline.
func TestPostgresBackend(t *testing.T) {
	db := newPostgresTestDB(t)
	seedTenant(t, db)

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
	stale, err := secondBackend.LoadForUpdate()
	if err != nil {
		t.Fatalf("stale load: %v", err)
	}
	fresh, err := firstBackend.LoadForUpdate()
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

// T-1 (AC-1): concurrent readers and writers on one OpenPostgres store must
// never race. Pre-fix, every RLock'd Read/Snapshot wrote p.lastVersion on
// the same plain int64, so `go test -race` reports DATA RACE at
// postgres.go:48. The start barrier and N>=50 iterations make the overlap
// deterministic enough to reproduce without a DSN-less false green.
func TestPostgresBackendConcurrentReadsRace(t *testing.T) {
	db := newPostgresTestDB(t)
	st, err := OpenPostgres(db)
	if err != nil {
		t.Fatalf("OpenPostgres: %v", err)
	}
	const (
		readers    = 8
		updaters   = 2
		iterations = 100
	)
	start := make(chan struct{})
	errs := make(chan error, readers+updaters)
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				if err := st.Read(func(*Snapshot) error { return nil }); err != nil {
					errs <- err
					return
				}
				if _, err := st.Snapshot(); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	for i := 0; i < updaters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				if err := st.Update(func(data *Snapshot) error {
					data.AdminActions = append(data.AdminActions, domain.AdminAction{
						ID: fmt.Sprintf("t1-%d-%d", id, j), Actor: "test",
						Action: "tenant.created", TargetType: "tenant", TargetID: "demo",
					})
					return nil
				}); err != nil {
					errs <- err
					return
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent store call: %v", err)
	}
}

// T-2 (AC-2): Load must be side-effect-free. Seed the row through a separate
// store instance, then verify a bare, unprimed backend's Load leaves the
// optimistic-lock baseline untouched. Pre-fix, Load wrote the row version
// into lastVersion (observable 0 -> 2) and this test fails.
func TestPostgresBackendLoadIsSideEffectFree(t *testing.T) {
	db := newPostgresTestDB(t)
	seedTenant(t, db)

	b := &postgresBackend{db: db}
	if b.lastVersion != 0 {
		t.Fatalf("backend must be unprimed, lastVersion=%d", b.lastVersion)
	}
	data, err := b.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, ok := data.Tenants["demo"]; !ok {
		t.Fatal("Load returned stale data: tenant demo missing")
	}
	if b.lastVersion != 0 {
		t.Fatalf("Load mutated lastVersion: got %d, want 0", b.lastVersion)
	}
}

// T-3 (AC-2, Store level): interleaved Read/Update cycles on one store must
// never surface ErrSnapshotConflict. This pins the acceptance wording as an
// invariant; the conflict mechanism itself is exercised by T-4.
func TestPostgresBackendReadBetweenUpdatesNoConflict(t *testing.T) {
	db := newPostgresTestDB(t)
	st, err := OpenPostgres(db)
	if err != nil {
		t.Fatalf("OpenPostgres: %v", err)
	}
	for i := 0; i < 50; i++ {
		if err := st.Read(func(*Snapshot) error { return nil }); err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		if err := st.Update(func(data *Snapshot) error {
			data.Tenants["demo"] = domain.Tenant{ID: "demo", Name: "Demo", Active: true}
			return nil
		}); err != nil {
			t.Fatalf("Update %d: %v", i, err)
		}
	}
}

// T-8 (FM-1): Save on a backend whose LoadForUpdate never primed the
// baseline must fail loudly with ErrSnapshotConflict instead of silently
// overwriting the row, and the row must stay untouched.
func TestPostgresBackendSaveWithoutPrime(t *testing.T) {
	db := newPostgresTestDB(t)
	seed := seedTenant(t, db)

	b := &postgresBackend{db: db}
	if err := b.Save(NewSnapshot()); !errors.Is(err, ErrSnapshotConflict) {
		t.Fatalf("Save without prime error=%v, want ErrSnapshotConflict", err)
	}
	var tenants int
	if err := seed.Read(func(data *Snapshot) error {
		tenants = len(data.Tenants)
		return nil
	}); err != nil {
		t.Fatalf("read after conflict: %v", err)
	}
	if tenants != 1 {
		t.Fatalf("tenants=%d after conflict, want 1", tenants)
	}
}

// T-9 (FM-6): a failing Update closure after priming must not poison the
// store: the row keeps its previous state and the next Update commits.
func TestPostgresBackendFailedUpdateRecovers(t *testing.T) {
	db := newPostgresTestDB(t)
	st := seedTenant(t, db)

	wantErr := errors.New("closure failed")
	if err := st.Update(func(*Snapshot) error { return wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("failing Update error=%v, want %v", err, wantErr)
	}
	var tenants int
	if err := st.Read(func(data *Snapshot) error {
		tenants = len(data.Tenants)
		return nil
	}); err != nil {
		t.Fatalf("read after failed Update: %v", err)
	}
	if tenants != 1 {
		t.Fatalf("tenants=%d after failed Update, want 1", tenants)
	}
	if err := st.Update(func(data *Snapshot) error {
		data.Tenants["second"] = domain.Tenant{ID: "second", Name: "Second", Active: true}
		return nil
	}); err != nil {
		t.Fatalf("Update after recovery: %v", err)
	}
	var count int
	if err := st.Read(func(data *Snapshot) error {
		count = len(data.Tenants)
		return nil
	}); err != nil {
		t.Fatalf("read after recovery: %v", err)
	}
	if count != 2 {
		t.Fatalf("tenants=%d after recovery, want 2", count)
	}
}

// T-10: an externally deleted row must not silently reset a primed backend.
// LoadForUpdate keeps the previous baseline on sql.ErrNoRows, so Save still
// conflicts loudly instead of recreating an empty row.
func TestPostgresBackendDeletedRowKeepsBaseline(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	seedTenant(t, db)

	b := &postgresBackend{db: db}
	data, err := b.LoadForUpdate()
	if err != nil {
		t.Fatalf("LoadForUpdate: %v", err)
	}
	if b.lastVersion != 2 {
		t.Fatalf("lastVersion=%d after prime, want 2", b.lastVersion)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM audit_state_snapshot`); err != nil {
		t.Fatalf("delete row: %v", err)
	}
	if _, err := b.LoadForUpdate(); err != nil {
		t.Fatalf("LoadForUpdate after delete: %v", err)
	}
	if b.lastVersion != 2 {
		t.Fatalf("LoadForUpdate reset lastVersion to %d, want 2 kept", b.lastVersion)
	}
	if err := b.Save(data); !errors.Is(err, ErrSnapshotConflict) {
		t.Fatalf("Save after delete error=%v, want ErrSnapshotConflict", err)
	}
}

// T-6 PG leg: two store instances over one DSN updating concurrently must
// converge through the bounded-jitter retry — the loser re-runs its closure
// on the fresh snapshot and wins its write, instead of surfacing a bare
// error. (The old single-attempt Update would fail the second writer with
// ErrSnapshotConflict.) Uses two separate connections on the same DB.
func TestPostgresBackendConcurrentUpdateConverges(t *testing.T) {
	db := newPostgresTestDB(t)
	db2, err := sql.Open("pgx", os.Getenv("AUDIT_TEST_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db2.Close() })
	seedTenant(t, db)

	first := &postgresBackend{db: db}
	second := &postgresBackend{db: db2}
	firstStore := &Store{backend: first}
	secondStore := &Store{backend: second}

	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		errs <- firstStore.Update(func(data *Snapshot) error {
			data.Tenants["tenant-x"] = domain.Tenant{ID: "tenant-x", Name: "X", Active: true}
			return nil
		})
	}()
	go func() {
		<-start
		errs <- secondStore.Update(func(data *Snapshot) error {
			data.Tenants["tenant-y"] = domain.Tenant{ID: "tenant-y", Name: "Y", Active: true}
			return nil
		})
	}()
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent Update must converge: %v", err)
		}
	}
	if err := firstStore.Read(func(data *Snapshot) error {
		if _, ok := data.Tenants["tenant-x"]; !ok {
			t.Fatal("tenant-x missing after concurrent updates")
		}
		if _, ok := data.Tenants["tenant-y"]; !ok {
			t.Fatal("tenant-y missing after concurrent updates")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// T-6 readyz leg: a closed database makes the postgresBackend.Ready probe
// fail, which the HTTP readyz handler maps to 503 store_unavailable.
func TestPostgresBackendReadyProbeFailsWhenUnavailable(t *testing.T) {
	db := newPostgresTestDB(t)
	backend := &postgresBackend{db: db}
	if err := backend.Ready(context.Background()); err != nil {
		t.Fatalf("Ready on live db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := backend.Ready(context.Background()); err == nil {
		t.Fatal("Ready on closed db must fail")
	}
}

// rowMetrics captures the parts of the snapshot row that an idle pass must
// not touch: the optimistic-lock version, the updated_at timestamp and the
// serialized snapshot byte size.
func rowMetrics(t *testing.T, db *sql.DB) (version int64, updatedAt time.Time, bytes int) {
	t.Helper()
	if err := db.QueryRow(`SELECT version, updated_at, pg_column_size(snapshot) FROM audit_state_snapshot WHERE id = 1`).Scan(&version, &updatedAt, &bytes); err != nil {
		t.Fatalf("read row metrics: %v", err)
	}
	return version, updatedAt, bytes
}

// TestPostgresUpdateCheckedNoWriteOnCleanClosure is AC-3 store leg: the
// checked primitive must not persist a no-op closure against the real
// PostgreSQL row — version, updated_at and byte size stay identical across
// repeated clean closures, and a mutating closure still advances the version.
func TestPostgresUpdateCheckedNoWriteOnCleanClosure(t *testing.T) {
	db := newPostgresTestDB(t)
	st := seedTenant(t, db)

	version, updatedAt, size := rowMetrics(t, db)
	for i := 0; i < 10; i++ {
		if err := st.UpdateChecked(func(data *Snapshot) (bool, error) {
			data.Tenants["ghost"] = domain.Tenant{ID: "ghost", Name: "Ghost", Active: true}
			return false, nil
		}); err != nil {
			t.Fatalf("clean closure %d: %v", i, err)
		}
	}
	gotVersion, gotUpdatedAt, gotSize := rowMetrics(t, db)
	if gotVersion != version || !gotUpdatedAt.Equal(updatedAt) || gotSize != size {
		t.Fatalf("clean closures changed the row: version %d->%d, updated_at %v->%v, size %d->%d", version, gotVersion, updatedAt, gotUpdatedAt, size, gotSize)
	}

	// A mutating closure still writes exactly once.
	if err := st.UpdateChecked(func(data *Snapshot) (bool, error) {
		data.Tenants["real"] = domain.Tenant{ID: "real", Name: "Real", Active: true}
		return true, nil
	}); err != nil {
		t.Fatalf("mutating closure: %v", err)
	}
	gotVersion, _, _ = rowMetrics(t, db)
	if gotVersion != version+1 {
		t.Fatalf("version after mutating closure=%d, want %d", gotVersion, version+1)
	}
}
