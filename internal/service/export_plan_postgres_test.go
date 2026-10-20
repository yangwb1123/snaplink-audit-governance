package service

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
)

// Explicit disposable opt-in DSN only, independent database/sql pools. The
// shared helper applies migrations/reset exclusively inside that test DB.
func postgresSelectedPlan(t *testing.T) (*Service, *Service, *checkpointArchive, *atomic.Int64, exportClaim) {
	t.Helper()
	first, second := newExportLeasePostgres(t)
	var clock atomic.Int64
	clock.Store(pinnedNow.UnixNano())
	objects := &checkpointArchive{Store: &archive.FileStore{Dir: t.TempDir()}}
	for _, svc := range []*Service{first, second} {
		svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
		svc.Config.Archive = objects
	}
	seedTestDomain(t, first)
	if _, err := first.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("plan-source", "operation", pinnedNow.Add(10*time.Second)), domain.StatusArchived); err != nil {
		t.Fatal(err)
	}
	seedLeaseExport(t, first, "checkpoint-job", "tenant-a")
	claim := leaveSelectedPlan(t, first)
	got := leaseSnapshot(t, second)
	if got.LayoutVersion != 2 || got.ExportPlans[claim.ID].Signature != claim.Plan.Signature || len(got.ExportPlans[claim.ID].Members) != 1 {
		t.Fatal("selected plan was not durable across independent SQL pools")
	}
	return first, second, objects, &clock, claim
}

func TestPostgresExportPlanCompetingTakeoversShareBudgetAndPreserveTenantDelta(t *testing.T) {
	first, second, objects, clock, old := postgresSelectedPlan(t)
	if _, won, err := second.claimExport(testCtx, old.ID, true); err != nil || won {
		t.Fatal("replica stole healthy selected-stage lease")
	}
	late, err := second.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("plan-late", "operation", pinnedNow.Add(20*time.Second)), domain.StatusArchived)
	if err != nil {
		t.Fatal(err)
	}
	if leaseSnapshot(t, first).ExportPlans[old.ID].Signature != old.Plan.Signature {
		t.Fatal("tenant commit lost concurrently retained selected stage")
	}
	seedLeaseExport(t, first, "same-tenant", "tenant-a")
	seedLeaseExport(t, first, "other-slot", "tenant-b")
	seedLeaseExport(t, first, "third-slot", "tenant-c")
	if _, won, err := second.claimPendingExport(testCtx, "same-tenant"); err != nil || won {
		t.Fatal("selected plan did not occupy tenant running budget")
	}
	if _, won, err := second.claimPendingExport(testCtx, "other-slot"); err != nil || !won {
		t.Fatal("second tenant could not use the remaining slot")
	}
	if _, won, err := first.claimPendingExport(testCtx, "third-slot"); err != nil || won {
		t.Fatal("replicas multiplied global running budget")
	}
	clock.Store(pinnedNow.Add(ExportLeaseDuration).UnixNano())
	start := make(chan struct{})
	winners := make(chan exportClaim, 2)
	var wg sync.WaitGroup
	for _, svc := range []*Service{first, second} {
		wg.Add(1)
		go func(s *Service) {
			defer wg.Done()
			<-start
			claim, won, err := s.claimExport(testCtx, old.ID, true)
			if err != nil {
				t.Error("independent selected-stage CAS failed")
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
		t.Fatal("independent SQL pools multiplied selected-stage owner")
	}
	replacement := <-winners
	if replacement.Owner == old.Owner || !replacement.StartedAt.Equal(*old.StartedAt) {
		t.Fatal("takeover reused owner or reset original start")
	}
	if err := first.saveExportPlan(exportClaimContext(testCtx, old), old, old.Plan); err == nil {
		t.Fatal("late old owner rewrote selected stage")
	}
	clock.Store(pinnedNow.Add(2 * ExportLeaseDuration).UnixNano())
	probe := &planSourceArchive{Store: objects, forbidden: eventArchiveKey("tenant-a", late.StreamID, late.Sequence, late.EventID)}
	second.Config.Archive = probe
	puts := objects.puts.Load()
	second.runExport(testCtx, old.ID)
	got := leaseSnapshot(t, first)
	if got.Exports[old.ID].Status != "completed" || got.Exports[old.ID].EventCount != 1 || len(got.ExportPlans) != 0 || len(got.ExportCheckpoints) != 0 || objects.puts.Load() != puts+1 || probe.sourceGets.Load() != 1 {
		t.Fatal("cross-pool selected continuation changed original members or phase cleanup")
	}
	if checkpointFactCount(t, first, domain.AdminActionExportPlanned) != 1 || checkpointFactCount(t, first, domain.AdminActionExportCompleted) != 1 {
		t.Fatal("selected/terminal facts were not atomic and unique across pools")
	}
}

func TestPostgresExportPlanCrossReplicaCancelFencesSourceGet(t *testing.T) {
	first, second, objects, clock, old := postgresSelectedPlan(t)
	pauseExportDispatch(first)
	clock.Store(pinnedNow.Add(ExportLeaseDuration).UnixNano())
	probe := &planSourceArchive{Store: objects, entered: make(chan context.Context, 1), release: make(chan struct{})}
	second.Config.Archive = probe
	var once sync.Once
	release := func() { once.Do(func() { close(probe.release) }) }
	done := make(chan struct{})
	t.Cleanup(func() { release(); <-done })
	puts := objects.puts.Load()
	go func() { second.runExport(testCtx, old.ID); close(done) }()
	requireCheckpointContext(t, probe.entered)
	data := leaseSnapshot(t, first)
	current := exportClaim{ExportJob: data.Exports[old.ID], Owner: data.ExportLeases[old.ID].Owner, Plan: data.ExportPlans[old.ID]}
	clock.Store(pinnedNow.Add(ExportLeaseDuration + ExportHeartbeatInterval).UnixNano())
	if err := first.renewExportLease(testCtx, current); err != nil {
		t.Fatal("independent pool renewal blocked or failed during source Get")
	}
	if _, err := first.CancelExport(testCtx, old.TenantID, old.RequestedBy, old.ID); err != nil {
		t.Fatal("independent owner cancellation failed during source Get")
	}
	release()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("revoked selected executor did not finish")
	}
	got := leaseSnapshot(t, first)
	if got.Exports[old.ID].Status != domain.ExportStatusCancelled || len(got.ExportPlans) != 0 || len(got.ExportLeases) != 0 || objects.puts.Load() != puts || got.Exports[old.ID].ObjectPath != "" {
		t.Fatal("late selected source Get overwrote cross-pool terminal cancellation")
	}
}
