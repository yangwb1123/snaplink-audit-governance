package service

import (
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// This suite clears its dedicated database. Never point the opt-in DSN at
// production or a database shared with another suite.
func newExportLeasePostgres(t *testing.T) (*Service, *Service) {
	t.Helper()
	dsn := os.Getenv("AUDIT_TEST_POSTGRES_EXPORT_LEASE_DSN")
	if dsn == "" {
		t.Skip("set AUDIT_TEST_POSTGRES_EXPORT_LEASE_DSN to a disposable database")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, name := range []string{"001_control_plane.sql", "002_source_identity_binding.sql", "003_outbox_relay.sql", "004_state_snapshot.sql", "005_admin_action_trail.sql", "006_hot_cold_split.sql", "007_publication_confirmation.sql", "008_query_snapshot.sql"} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(string(raw)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`DROP TABLE IF EXISTS audit_state_snapshot_v1_backup;
TRUNCATE audit_ledger, audit_tenant, admin_action_trail, audit_publication_confirmation, audit_query_snapshot;
DELETE FROM audit_state_snapshot`); err != nil {
		t.Fatal(err)
	}
	dbOther, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dbOther.Close() })
	st, err := store.OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	stOther, err := store.OpenPostgres(dbOther)
	if err != nil {
		t.Fatal(err)
	}
	return newServiceOn(t, st), newServiceOn(t, stOther)
}

func TestPostgresExportLeaseIndependentReplicasShareClaimRenewalAndBudget(t *testing.T) {
	first, second := newExportLeasePostgres(t)
	var clock atomic.Int64
	clock.Store(pinnedNow.UnixNano())
	for _, svc := range []*Service{first, second} {
		svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	}
	if err := first.Store.Update(testCtx, func(data *store.Snapshot) error {
		for _, tenant := range []string{"tenant-a", "tenant-b", "tenant-c"} {
			data.Tenants[tenant] = domain.Tenant{ID: tenant, Active: true}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if leaseSnapshot(t, first).LayoutVersion != 2 {
		t.Fatal("integration must exercise active PostgreSQL v2 control CAS")
	}
	for id, tenant := range map[string]string{"shared-job": "tenant-a", "same-tenant": "tenant-a", "second-slot": "tenant-b", "third-slot": "tenant-c"} {
		seedLeaseExport(t, first, id, tenant)
	}
	start := make(chan struct{})
	winners := make(chan exportClaim, 2)
	var wg sync.WaitGroup
	for _, svc := range []*Service{first, second} {
		wg.Add(1)
		go func(s *Service) {
			defer wg.Done()
			<-start
			claim, won, err := s.claimPendingExport(testCtx, "shared-job")
			if err != nil {
				t.Error(err)
			}
			if won {
				winners <- claim
			}
		}(svc)
	}
	close(start)
	wg.Wait()
	close(winners)
	if len(winners) != 1 {
		t.Fatal("independent SQL replicas multiplied one claim")
	}
	shared := <-winners
	if _, won, err := second.claimPendingExport(testCtx, "same-tenant"); err != nil || won {
		t.Fatal("tenant running limit was not shared")
	}
	slot, won, err := second.claimPendingExport(testCtx, "second-slot")
	if err != nil || !won {
		t.Fatal("second tenant could not use second global slot")
	}
	if _, won, err := first.claimPendingExport(testCtx, "third-slot"); err != nil || won {
		t.Fatal("global running limit was not shared")
	}
	clock.Store(pinnedNow.Add(ExportHeartbeatInterval).UnixNano())
	if err := second.renewExportLease(testCtx, shared); err != nil {
		t.Fatal(err)
	}
	if lease := leaseSnapshot(t, first).ExportLeases[shared.ID]; !lease.HeartbeatAt.Equal(first.Now()) || lease.Owner != shared.Owner {
		t.Fatal("heartbeat was not durable across independent pools")
	}
	clock.Store(pinnedNow.Add(ExportLeaseDuration).UnixNano())
	if n, err := first.recoverExpiredExportLeases(testCtx); err != nil || n != 1 {
		t.Fatal("recovery did not distinguish expired from renewed owner")
	}
	second.finishExport(exportClaimContext(testCtx, slot), slot.ID, "completed", "stale-object", "stale-digest", 1, "")
	third, won, err := second.claimPendingExport(testCtx, "third-slot")
	if err != nil || !won {
		t.Fatal("failed expired owner did not release durable budget")
	}
	first.finishExport(exportClaimContext(testCtx, shared), shared.ID, "completed", "current-object", "digest", 1, "")
	second.finishExport(exportClaimContext(testCtx, third), third.ID, "failed", "", "", 0, "failure")
	got := leaseSnapshot(t, first)
	if got.Exports[shared.ID].Status != "completed" || got.Exports[slot.ID].Status != "failed" || got.Exports[slot.ID].ObjectPath != "" || got.Exports[third.ID].Status != "failed" || len(got.ExportLeases) != 0 {
		t.Fatal("fenced terminal CAS lost lease or terminal-state isolation")
	}
	facts := 0
	for _, fact := range got.AdminActions {
		if fact.Action == domain.AdminActionExportRecovered {
			facts++
		}
	}
	if facts != 1 {
		t.Fatal("expiry self-audit did not commit once across replicas")
	}
}
