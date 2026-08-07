package store

// The tests in this file exercise the real PostgreSQL snapshot row against
// AUDIT_TEST_POSTGRES_DSN (migrations 001 and 004 applied) and skip cleanly
// when it is unset, keeping the quality gate green in CI. They share one
// row (audit_state_snapshot id=1), so they must never call t.Parallel; each
// test resets the row first via newPostgresTestDB.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

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
	if _, err := db.ExecContext(ctx, `DELETE FROM audit_state_snapshot`); err != nil {
		t.Fatalf("reset snapshot row: %v", err)
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
