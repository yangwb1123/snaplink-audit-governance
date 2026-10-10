package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestPostgresQuerySnapshotReplicasShareAdmission(t *testing.T) {
	db := newHotColdPostgresTestDB(t) // disposable databases only
	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", "008_query_snapshot.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(raw)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`TRUNCATE audit_query_snapshot`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DROP TABLE IF EXISTS audit_query_snapshot`) })
	one, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	two, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now().UTC()
	if err := one.CheckQuerySnapshotStorage(ctx); err != nil {
		t.Fatal(err)
	}
	q := snapshotFixture(1, now)
	if err := one.SaveQuerySnapshot(ctx, q, now); err != nil {
		t.Fatal(err)
	}
	got, err := two.ReadQuerySnapshot(ctx, q.TenantID, q.ID, now)
	if err != nil || got.Events[0].Hash != "hash" {
		t.Fatalf("q=%+v err=%v", got, err)
	}
	if _, err := two.ReadQuerySnapshot(ctx, "tenant-b", q.ID, now); !errors.Is(err, ErrQuerySnapshotUnavailable) {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, MaxActiveQuerySnapshots+8)
	for i := 2; i <= MaxActiveQuerySnapshots+9; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			st := one
			if id%2 == 0 {
				st = two
			}
			errs <- st.SaveQuerySnapshot(ctx, snapshotFixture(id, now), now)
		}(i)
	}
	wg.Wait()
	close(errs)
	successes := 1
	for err := range errs {
		if err == nil {
			successes++
		} else if !errors.Is(err, domain.ErrQuotaExceeded) {
			t.Fatal(err)
		}
	}
	if successes != MaxActiveQuerySnapshots {
		t.Fatalf("shared admission=%d, want %d", successes, MaxActiveQuerySnapshots)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM audit_query_snapshot WHERE tenant_id='tenant-a'`).Scan(&count); err != nil || count != MaxActiveQuerySnapshots {
		t.Fatalf("count=%d err=%v", count, err)
	}
	later := now.Add(QuerySnapshotLifetime)
	if err := two.SaveQuerySnapshot(ctx, snapshotFixture(100, later), later); err != nil {
		t.Fatal(err)
	}
	if _, err := one.ReadQuerySnapshot(ctx, q.TenantID, q.ID, later); !errors.Is(err, ErrQuerySnapshotUnavailable) {
		t.Fatal(err)
	}
	// Exact indexed catalog probe fails closed, never silently drops to memory.
	if _, err := db.ExecContext(ctx, `DROP TABLE audit_query_snapshot`); err != nil {
		t.Fatal(err)
	}
	if err := one.CheckQuerySnapshotStorage(ctx); err == nil {
		t.Fatal(fmt.Errorf("missing snapshot catalog passed probe"))
	}
}
