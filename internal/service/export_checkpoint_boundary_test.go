package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportCheckpointGetReleasesLockForRenewalAndCancellation(t *testing.T) {
	svc, backend, objects := newCheckpointService(t)
	cp := leaveSealedCheckpoint(t, svc, backend)
	var clock atomic.Int64
	clock.Store(pinnedNow.Add(ExportLeaseDuration).UnixNano())
	svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	pauseExportDispatch(svc)
	objects.entered, objects.release = make(chan context.Context, 1), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(objects.release) }) }
	done := make(chan struct{})
	t.Cleanup(func() { release(); <-done })
	go func() { svc.runExport(testCtx, cp.JobID); close(done) }()
	select {
	case <-objects.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("resume did not enter actual sealed-object Get")
	}
	controlDone := make(chan error, 1)
	go func() {
		data := leaseSnapshot(t, svc)
		claim := exportClaim{ExportJob: data.Exports[cp.JobID], Owner: data.ExportLeases[cp.JobID].Owner, Checkpoint: cp}
		clock.Store(pinnedNow.Add(ExportLeaseDuration + ExportHeartbeatInterval).UnixNano())
		if err := svc.renewExportLease(testCtx, claim); err != nil {
			controlDone <- err
			return
		}
		_, err := svc.CancelExport(testCtx, "tenant-a", "operator", cp.JobID)
		controlDone <- err
	}()
	select {
	case err := <-controlDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Get held Store.mu across renewal/cancel")
	}
	release()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("revoked finalizer did not return")
	}
	data := leaseSnapshot(t, svc)
	if data.Exports[cp.JobID].Status != domain.ExportStatusCancelled || data.Exports[cp.JobID].ObjectPath != "" || len(data.ExportCheckpoints) != 0 || len(data.ExportLeases) != 0 || checkpointFactCount(t, svc, domain.AdminActionExportCompleted) != 0 || checkpointFactCount(t, svc, domain.AdminActionExportCancelled) != 1 {
		t.Fatal("late Get completion overwrote cancellation")
	}
	if raw, err := objects.Store.Get(testCtx, cp.ObjectPath); err != nil || len(raw) == 0 {
		t.Fatal("cancel deleted the immutable sealed object")
	}
}

func TestExportCheckpointHoldChangeAfterReadAuditCaptureFencesCompletion(t *testing.T) {
	svc, backend, objects := newCheckpointService(t)
	cp := leaveSealedCheckpoint(t, svc, backend)
	expireCheckpoint(svc)
	backend.trailEntered, backend.trailRelease = make(chan context.Context, 1), make(chan struct{})
	objects.entered, objects.release = make(chan context.Context, 1), make(chan struct{})
	var getOnce, trailOnce sync.Once
	releaseGet := func() { getOnce.Do(func() { close(objects.release) }) }
	releaseTrail := func() { trailOnce.Do(func() { close(backend.trailRelease) }) }
	done := make(chan struct{})
	t.Cleanup(func() { releaseGet(); releaseTrail(); <-done })
	go func() { svc.runExport(testCtx, cp.JobID); close(done) }()
	var getCtx, trailCtx context.Context
	select {
	case getCtx = <-objects.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("Get boundary not reached")
	}
	releaseGet()
	select {
	case trailCtx = <-backend.trailEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("read audit boundary not reached")
	}
	getDeadline, _ := getCtx.Deadline()
	trailDeadline, _ := trailCtx.Deadline()
	if getDeadline.IsZero() || !getDeadline.Equal(trailDeadline) {
		t.Fatal("finalization restarted its shared operation deadline")
	}
	// The current-Hold snapshot has already been checked. This real control
	// write must still fence the later terminal CAS, not merely the UI read.
	if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", CreatedBy: "case-worker", Name: "case", Reason: "case"}); err != nil {
		t.Fatal(err)
	}
	releaseTrail()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("finalizer did not leave changed-Hold checkpoint available")
	}
	data := leaseSnapshot(t, svc)
	if data.Exports[cp.JobID].Status != "running" || data.ExportCheckpoints[cp.JobID] != cp || data.Exports[cp.JobID].ObjectPath != "" || checkpointFactCount(t, svc, domain.AdminActionExportCompleted) != 0 {
		t.Fatal("Hold introduced during lock-free verification did not fence completion")
	}
	// After expiry it rechecks the actual Hold, does not revive a terminal,
	// and records one atomic failure instead of publishing the sealed object.
	backend.trailEntered = nil
	objects.entered = nil
	expireCheckpoint(svc)
	puts := objects.puts.Load()
	svc.runExport(testCtx, cp.JobID)
	data = leaseSnapshot(t, svc)
	if data.Exports[cp.JobID].Status != "failed" || data.Exports[cp.JobID].FailureCode != domain.ExportFailureLegalHold || data.Exports[cp.JobID].ObjectPath != "" || len(data.ExportCheckpoints) != 0 || len(data.ExportLeases) != 0 || objects.puts.Load() != puts {
		t.Fatal("current Hold was bypassed on resumed finalization")
	}
	if n := countAdminActions(t, svc, func(fact domain.AdminAction) bool { return fact.Action == domain.AdminActionExportBlocked }); n != 1 {
		t.Fatal("current Hold did not commit exactly one block fact")
	}
}

func TestExportCheckpointCanceledOrExpiredGetCannotPublish(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[deadline], func(t *testing.T) {
			svc, backend, objects := newCheckpointService(t)
			cp := leaveSealedCheckpoint(t, svc, backend)
			expireCheckpoint(svc)
			objects.entered, objects.release = make(chan context.Context, 1), make(chan struct{})
			ctx, cancel := context.WithCancel(testCtx)
			done := make(chan struct{})
			t.Cleanup(func() { cancel(); close(objects.release); <-done })
			go func() { svc.runExport(ctx, cp.JobID); close(done) }()
			select {
			case <-objects.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("Get not reached before withdrawal")
			}
			if !deadline {
				cancel()
			}
			// Deadline case waits for the unmodified real Store operation
			// timeout. The outer guard starts only after the Get boundary.
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Fatal("withdrawn verification did not stop")
			}
			data := leaseSnapshot(t, svc)
			if data.Exports[cp.JobID].Status != "running" || data.ExportCheckpoints[cp.JobID] != cp || data.Exports[cp.JobID].ObjectPath != "" || checkpointFactCount(t, svc, domain.AdminActionExportCompleted) != 0 {
				t.Fatal("withdrawn verification published or destroyed checkpoint")
			}
		})
	}
}

func TestExportCheckpointSharedBudgetAndTerminalNeverRequeued(t *testing.T) {
	svc, backend, _ := newCheckpointService(t)
	cp := leaveSealedCheckpoint(t, svc, backend)
	expireCheckpoint(svc)
	pauseExportDispatch(svc)
	if n, err := svc.ResumePendingExports(testCtx, "tenant-a"); err != nil || n != 1 {
		t.Fatal("worker resume did not observe expired checkpoint backlog")
	}
	for _, overTenant := range []bool{false, true} {
		if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
			data.Exports["extra-1"] = domain.ExportJob{ID: "extra-1", TenantID: "tenant-b", Status: "running"}
			data.Exports["extra-2"] = domain.ExportJob{ID: "extra-2", TenantID: "tenant-c", Status: "running"}
			if overTenant {
				job := data.Exports["extra-1"]
				job.TenantID = "tenant-a"
				data.Exports[job.ID] = job
				delete(data.Exports, "extra-2")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		before := leaseSnapshot(t, svc)
		if _, won, err := svc.claimExport(testCtx, cp.JobID, true); err != nil || won {
			t.Fatal("checkpoint takeover expanded shared running budget")
		}
		if got := nextRunnableExport(before, nil, "", svc.Now()); got.ID != "" {
			t.Fatal("dispatcher selected over-budget sealed work")
		}
		if leaseSnapshot(t, svc).ExportLeases[cp.JobID] != before.ExportLeases[cp.JobID] {
			t.Fatal("budget rejection renewed or replaced a lease")
		}
	}
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		delete(data.Exports, "extra-1")
		delete(data.Exports, "extra-2")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CancelExport(testCtx, "tenant-a", "operator", cp.JobID); err != nil {
		t.Fatal(err)
	}
	if _, won, err := svc.claimExport(testCtx, cp.JobID, true); err != nil || won {
		t.Fatal("cancelled checkpoint job was requeued")
	}
	if _, err := svc.RetryExport(testCtx, "tenant-a", "other", cp.JobID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatal("checkpoint changed owner authorization")
	}
	if leaseSnapshot(t, svc).Exports[cp.JobID].Status != domain.ExportStatusCancelled {
		t.Fatal("terminal state changed")
	}
}
