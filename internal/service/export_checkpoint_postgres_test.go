package service

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
)

// Uses only the explicit disposable lease-suite DSN. The existing helper owns
// schema reset and creates genuinely independent SQL pools; no shared/production
// DSN fallback exists. Stop the test executor at the real checkpoint Save.
func postgresSealedCheckpoint(t *testing.T) (*Service, *Service, *checkpointArchive, *atomic.Int64, exportClaim) {
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
	if _, err := first.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("checkpoint-source", "operation", pinnedNow.Add(10*time.Second)), domain.StatusArchived); err != nil {
		t.Fatal(err)
	}
	seedLeaseExport(t, first, "checkpoint-job", "tenant-a")
	claim, won, err := first.claimPendingExport(testCtx, "checkpoint-job")
	if err != nil || !won {
		t.Fatal("initial PostgreSQL executor did not claim")
	}
	events, hold, err := first.selectExportEvidence(exportClaimContext(testCtx, claim), claim.ExportJob)
	if err != nil || hold.ID != "" || len(events) != 1 {
		t.Fatal("initial PostgreSQL evidence selection failed")
	}
	var plain []byte
	for _, event := range events {
		event.Payload, err = security.StripSearchDigests(event.Payload)
		if err != nil {
			t.Fatal(err)
		}
		line, err := domain.CanonicalJSON(event)
		if err != nil {
			t.Fatal(err)
		}
		plain = append(plain, line...)
		plain = append(plain, '\n')
	}
	sealed, err := security.EncryptBytesBound(plain, first.Config.EncryptionKey, security.ExportBinding(claim.TenantID, claim.ID))
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Put(testCtx, exportObjectKey(claim), sealed); err != nil {
		t.Fatal(err)
	}
	cp, err := first.saveExportCheckpoint(exportClaimContext(testCtx, claim), claim, sealed, plain, len(events))
	if err != nil {
		t.Fatal(err)
	}
	claim.Checkpoint = cp
	if got := leaseSnapshot(t, second); got.LayoutVersion != 2 || got.ExportCheckpoints[claim.ID] != cp {
		t.Fatal("checkpoint was not durable across independent PostgreSQL pools")
	}
	return first, second, objects, &clock, claim
}

func TestPostgresExportCheckpointCompetingTakeoversShareBudgetAndFenceOldOwner(t *testing.T) {
	first, second, objects, clock, old := postgresSealedCheckpoint(t)
	if _, won, err := second.claimExport(testCtx, old.ID, true); err != nil || won {
		t.Fatal("replica stole healthy checkpoint lease")
	}
	// A real tenant commit cannot lose the concurrently retained private map.
	if _, err := second.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("checkpoint-late", "operation", pinnedNow.Add(20*time.Second)), domain.StatusArchived); err != nil {
		t.Fatal(err)
	}
	if leaseSnapshot(t, first).ExportCheckpoints[old.ID] != old.Checkpoint {
		t.Fatal("tenant commit discarded checkpoint")
	}
	seedLeaseExport(t, first, "same-tenant", "tenant-a")
	seedLeaseExport(t, first, "other-slot", "tenant-b")
	seedLeaseExport(t, first, "third-slot", "tenant-c")
	if _, won, err := second.claimPendingExport(testCtx, "same-tenant"); err != nil || won {
		t.Fatal("retained sealed stage did not occupy tenant running budget")
	}
	other, won, err := second.claimPendingExport(testCtx, "other-slot")
	if err != nil || !won {
		t.Fatal("second tenant could not use remaining slot")
	}
	if _, won, err := first.claimPendingExport(testCtx, "third-slot"); err != nil || won {
		t.Fatal("replicas multiplied global running budget")
	}
	clock.Store(pinnedNow.Add(ExportLeaseDuration).UnixNano())
	type winner struct {
		svc   *Service
		claim exportClaim
	}
	winners := make(chan winner, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, svc := range []*Service{first, second} {
		wg.Add(1)
		go func(s *Service) {
			defer wg.Done()
			<-start
			claim, won, err := s.claimExport(testCtx, old.ID, true)
			if err != nil {
				t.Error(err)
			}
			if won {
				winners <- winner{s, claim}
			}
		}(svc)
	}
	close(start)
	wg.Wait()
	close(winners)
	if len(winners) != 1 {
		t.Fatal("independent CAS owners multiplied one sealed-stage takeover")
	}
	current := <-winners
	if current.claim.Owner == old.Owner || !current.claim.StartedAt.Equal(*old.StartedAt) {
		t.Fatal("takeover reused owner or reset execution start")
	}
	first.finishExport(exportClaimContext(testCtx, old), old.ID, "completed", old.Checkpoint.ObjectPath, old.Checkpoint.Digest, old.Checkpoint.EventCount, "")
	if leaseSnapshot(t, second).Exports[old.ID].Status != "running" {
		t.Fatal("expired original owner published late completion")
	}
	objects.armed.Store(true)
	puts, gets := objects.puts.Load(), objects.gets.Load()
	current.svc.finalizeExportCheckpoint(exportClaimContext(testCtx, current.claim), current.claim, nil)
	got := leaseSnapshot(t, first)
	if got.Exports[old.ID].Status != "completed" || got.Exports[old.ID].EventCount != 1 || got.Exports[old.ID].Digest != old.Checkpoint.Digest || len(got.ExportCheckpoints) != 0 || got.Exports[other.ID].Status != "running" || len(got.ExportLeases) != 1 || objects.puts.Load() != puts || objects.gets.Load() != gets+1 {
		t.Fatal("cross-pool continuation changed evidence, budget or another owner's state")
	}
	if checkpointFactCount(t, first, domain.AdminActionExportResumed) != 1 || checkpointFactCount(t, first, domain.AdminActionExportCompleted) != 1 {
		t.Fatal("cross-pool continuation facts were not atomic and unique")
	}
}

func TestPostgresExportCheckpointCrossReplicaCancelFencesPausedGet(t *testing.T) {
	first, second, objects, clock, old := postgresSealedCheckpoint(t)
	pauseExportDispatch(first)
	clock.Store(pinnedNow.Add(ExportLeaseDuration).UnixNano())
	objects.entered, objects.release = make(chan context.Context, 1), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(objects.release) }) }
	done := make(chan struct{})
	t.Cleanup(func() { release(); <-done })
	go func() { second.runExport(testCtx, old.ID); close(done) }()
	select {
	case <-objects.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("replica did not reach resumed object Get")
	}
	if summary, err := first.CancelExport(testCtx, "tenant-a", "operator", old.ID); err != nil || summary.Status != domain.ExportStatusCancelled {
		t.Fatal("independent owner cancellation failed while Get was blocked")
	}
	release()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("revoked PostgreSQL finalizer did not finish")
	}
	got := leaseSnapshot(t, first)
	if got.Exports[old.ID].Status != domain.ExportStatusCancelled || got.Exports[old.ID].ObjectPath != "" || len(got.ExportCheckpoints) != 0 || len(got.ExportLeases) != 0 || checkpointFactCount(t, first, domain.AdminActionExportCompleted) != 0 {
		t.Fatal("late Get published a canceled checkpoint")
	}
}
