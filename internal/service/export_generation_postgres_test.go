package service

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
)

// Opt-in disposable database only; the helper never falls back to a runtime
// DSN. Two independent database/sql pools exercise actual PostgreSQL v2 CAS.
func postgresGeneratedPage(t *testing.T) (*Service, *Service, *checkpointArchive, *atomic.Int64, exportClaim) {
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
	seedGenerationEvents(t, first, 0, 101)
	seedLeaseExport(t, first, "checkpoint-job", "tenant-a")
	old := leaveGeneratedPage(t, first)
	if got := leaseSnapshot(t, second); got.LayoutVersion != 2 || !reflect.DeepEqual(got.ExportPlans[old.ID], old.Plan) {
		t.Fatal("generated prefix was not durable across independent SQL pools")
	}
	return first, second, objects, &clock, old
}

func TestPostgresExportGenerationSharedCASBudgetAndProgressSurviveTenantCommit(t *testing.T) {
	first, second, objects, clock, old := postgresGeneratedPage(t)
	late, err := second.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("generation-pg-late", "late-operation", pinnedNow.Add(30*time.Second)), domain.StatusArchived)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(leaseSnapshot(t, first).ExportPlans[old.ID], old.Plan) {
		t.Fatal("tenant hot/cold commit erased newer progress")
	}
	seedLeaseExport(t, first, "same-tenant", "tenant-a")
	seedLeaseExport(t, first, "other-slot", "tenant-b")
	seedLeaseExport(t, first, "third-slot", "tenant-c")
	if _, won, err := second.claimPendingExport(testCtx, "same-tenant"); err != nil || won {
		t.Fatal("generated prefix did not occupy tenant running budget")
	}
	if _, won, err := second.claimPendingExport(testCtx, "other-slot"); err != nil || !won {
		t.Fatal("remaining global slot was not available")
	}
	if _, won, err := first.claimPendingExport(testCtx, "third-slot"); err != nil || won {
		t.Fatal("replicas multiplied shared running budget")
	}
	clock.Store(pinnedNow.Add(ExportLeaseDuration).UnixNano())
	winners := make(chan exportClaim, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, svc := range []*Service{first, second} {
		wg.Add(1)
		go func(s *Service) {
			defer wg.Done()
			<-start
			claim, won, err := s.claimExport(testCtx, old.ID, true)
			if err != nil {
				t.Error("independent progress takeover failed")
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
		t.Fatal("SQL CAS multiplied generation owner")
	}
	current := <-winners
	if current.Owner == old.Owner || !current.StartedAt.Equal(*old.StartedAt) || !reflect.DeepEqual(current.Plan, old.Plan) {
		t.Fatal("takeover reset execution identity or progress")
	}
	if _, _, err := first.generateExportPage(exportClaimContext(testCtx, old), old); err == nil {
		t.Fatal("old owner advanced generation after replacement")
	}
	clock.Store(pinnedNow.Add(2 * ExportLeaseDuration).UnixNano())
	probe := &generationArchive{Store: objects, forbiddenSources: generatedSourceKeys(old.Plan)}
	probe.forbiddenSources[eventArchiveKey("tenant-a", late.StreamID, late.Sequence, late.EventID)] = true
	second.Config.Archive = probe
	second.runExport(testCtx, old.ID)
	data := leaseSnapshot(t, first)
	job := data.Exports[old.ID]
	if job.Status != "completed" || job.EventCount != len(old.Plan.Members) || !job.StartedAt.Equal(*old.StartedAt) || len(data.ExportPlans) != 0 || len(data.ExportCheckpoints) != 0 || data.Exports["other-slot"].Status != "running" || len(data.ExportLeases) != 1 || probe.sourceGets.Load() != 1 || probe.chunkGets.Load() != 1 || probe.chunkPuts.Load() != 1 || probe.finalPuts.Load() != 1 {
		t.Fatal("cross-pool continuation regenerated committed bytes or changed shared state")
	}
	if checkpointFactCount(t, first, domain.AdminActionExportProgressed) != 2 || checkpointFactCount(t, first, domain.AdminActionExportCompleted) != 1 {
		t.Fatal("cross-pool page and terminal facts were not atomic/unique")
	}
}

func TestPostgresExportGenerationCancellationFencesRealLatePagePut(t *testing.T) {
	first, second, objects, clock, old := postgresGeneratedPage(t)
	pauseExportDispatch(first)
	clock.Store(pinnedNow.Add(ExportLeaseDuration).UnixNano())
	probe := &generationArchive{Store: objects, putEntered: make(chan leasePutObservation, 1), release: make(chan struct{}), ignoreCancel: true}
	second.Config.Archive = probe
	var once sync.Once
	release := func() { once.Do(func() { close(probe.release) }) }
	done := make(chan struct{})
	t.Cleanup(func() { release(); <-done })
	go func() { second.runExport(testCtx, old.ID); close(done) }()
	var put leasePutObservation
	select {
	case put = <-probe.putEntered:
	case <-time.After(30 * time.Second):
		t.Fatal("cross-pool executor did not enter actual page Put")
	}
	data := leaseSnapshot(t, first)
	current := exportClaim{ExportJob: data.Exports[old.ID], Owner: data.ExportLeases[old.ID].Owner, Plan: data.ExportPlans[old.ID]}
	clock.Store(pinnedNow.Add(ExportLeaseDuration + ExportHeartbeatInterval).UnixNano())
	if err := first.renewExportLease(testCtx, current); err != nil {
		t.Fatal("independent pool could not renew during Put")
	}
	if _, err := first.CancelExport(testCtx, old.TenantID, old.RequestedBy, old.ID); err != nil {
		t.Fatal("cross-pool cancellation failed at real Put boundary")
	}
	release()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("revoked cross-pool executor did not return")
	}
	data = leaseSnapshot(t, first)
	if data.Exports[old.ID].Status != domain.ExportStatusCancelled || data.Exports[old.ID].ObjectPath != "" || len(data.ExportPlans) != 0 || len(data.ExportCheckpoints) != 0 || len(data.ExportLeases) != 0 || probe.finalPuts.Load() != 0 || checkpointFactCount(t, first, domain.AdminActionExportProgressed) != 1 {
		t.Fatal("late external page result overwrote terminal cancellation")
	}
	if raw, err := objects.Get(context.Background(), put.key); err != nil || len(raw) == 0 {
		t.Fatal("late WORM result was deleted or declared rolled back")
	}
}
