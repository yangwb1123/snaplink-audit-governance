package service

import (
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestPostgresExportCheckpointHoldSourceSnapshotChangeFencesCompletion(t *testing.T) {
	first, second, objects, clock, old := postgresSealedCheckpoint(t)
	if _, err := first.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", CreatedBy: "case-worker", Name: "nonmatching", Reason: "case", Filter: domain.Query{From: pinnedNow, To: pinnedNow.Add(time.Minute), ActorID: "nonmatching-actor"}}); err != nil {
		t.Fatal("could not seed current Hold across PostgreSQL pools")
	}
	boundary := newCheckpointHoldBoundaryArchive(objects)
	second.Config.Archive = boundary
	clock.Store(pinnedNow.Add(ExportLeaseDuration).UnixNano())
	var once sync.Once
	release := func() { once.Do(func() { close(boundary.release) }) }
	done := make(chan struct{})
	t.Cleanup(func() { release(); <-done })
	puts := objects.puts.Load()
	go func() { second.runExport(testCtx, old.ID); close(done) }()
	sealedCtx := requireCheckpointContext(t, boundary.sealed)
	sourceCtx := requireCheckpointContext(t, boundary.entered)
	sealedDeadline, _ := sealedCtx.Deadline()
	sourceDeadline, _ := sourceCtx.Deadline()
	if sealedDeadline.IsZero() || !sealedDeadline.Equal(sourceDeadline) {
		t.Fatal("cross-pool current-Hold source verification restarted its operation deadline")
	}
	// A real concurrent control CAS on an independent SQL pool must remain
	// possible while the finalizer holds an owned, now-stale Hold snapshot.
	data := leaseSnapshot(t, first)
	current := exportClaim{ExportJob: data.Exports[old.ID], Owner: data.ExportLeases[old.ID].Owner, Checkpoint: old.Checkpoint}
	clock.Store(pinnedNow.Add(ExportLeaseDuration + ExportHeartbeatInterval).UnixNano())
	if err := first.renewExportLease(testCtx, current); err != nil {
		t.Fatal("independent pool could not renew during current-Hold Get")
	}
	if _, err := first.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", CreatedBy: "case-worker", Name: "matching", Reason: "case"}); err != nil {
		t.Fatal("independent pool could not change current Hold")
	}
	release()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("cross-pool current-Hold finalizer did not stop")
	}
	got := leaseSnapshot(t, first)
	if got.Exports[old.ID].Status != "running" || got.Exports[old.ID].ObjectPath != "" || got.ExportCheckpoints[old.ID] != old.Checkpoint || len(got.ExportLeases) != 1 || checkpointFactCount(t, first, domain.AdminActionExportCompleted) != 0 || objects.puts.Load() != puts {
		t.Fatal("cross-pool Hold change did not fence the stale completion CAS")
	}
	// A subsequent real expired-lease claim must recheck the new Hold and
	// converge once without another Put or changing the original start time.
	second.Config.Archive = objects
	clock.Store(second.Now().Add(ExportLeaseDuration).UnixNano())
	second.runExport(testCtx, old.ID)
	got = leaseSnapshot(t, first)
	job := got.Exports[old.ID]
	if job.Status != "failed" || job.FailureCode != domain.ExportFailureLegalHold || job.ObjectPath != "" || !job.StartedAt.Equal(*old.StartedAt) || len(got.ExportCheckpoints) != 0 || len(got.ExportLeases) != 0 || objects.puts.Load() != puts {
		t.Fatal("cross-pool revalidation bypassed the current Hold or regenerated evidence")
	}
	blocked := countAdminActions(t, first, func(fact domain.AdminAction) bool { return fact.Action == domain.AdminActionExportBlocked })
	if blocked != 1 {
		t.Fatal("cross-pool Hold block was not committed exactly once")
	}
}
