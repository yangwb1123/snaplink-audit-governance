package service

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestExportPlanSourceGetIsLockFreeAndLateOwnerCannotAdvance(t *testing.T) {
	for _, mode := range []string{"owner-cancel", "lease-takeover", "new-Hold"} {
		t.Run(mode, func(t *testing.T) {
			svc, _, objects := newPlanService(t)
			old := leaveSelectedPlan(t, svc)
			pauseExportDispatch(svc)
			var clock atomic.Int64
			clock.Store(pinnedNow.Add(ExportLeaseDuration).UnixNano())
			svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
			probe := &planSourceArchive{Store: objects, entered: make(chan context.Context, 4), release: make(chan struct{})}
			svc.Config.Archive = probe
			var once sync.Once
			release := func() { once.Do(func() { close(probe.release) }) }
			done := make(chan struct{})
			t.Cleanup(func() { release(); <-done })
			puts := objects.puts.Load()
			go func() { svc.runExport(testCtx, old.ID); close(done) }()
			requireCheckpointContext(t, probe.entered)
			var current exportClaim
			control := make(chan error, 1)
			go func() {
				data := leaseSnapshot(t, svc)
				current = exportClaim{ExportJob: data.Exports[old.ID], Owner: data.ExportLeases[old.ID].Owner, Plan: data.ExportPlans[old.ID]}
				clock.Store(pinnedNow.Add(ExportLeaseDuration + ExportHeartbeatInterval).UnixNano())
				if err := svc.renewExportLease(testCtx, current); err != nil {
					control <- err
					return
				}
				switch mode {
				case "owner-cancel":
					_, err := svc.CancelExport(testCtx, old.TenantID, old.RequestedBy, old.ID)
					control <- err
				case "lease-takeover":
					clock.Store(svc.Now().Add(ExportLeaseDuration).UnixNano())
					_, won, err := svc.claimExport(testCtx, old.ID, true)
					if err == nil && !won {
						err = errExportLeaseLost
					}
					control <- err
				case "new-Hold":
					_, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: old.TenantID, CreatedBy: "case-worker", Name: "current", Reason: "case"})
					control <- err
				}
			}()
			select {
			case err := <-control:
				if err != nil {
					t.Fatal("control operation failed while selected source Get was blocked")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("selected source Get retained Store.mu")
			}
			release()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("revoked selected executor did not return")
			}
			got := leaseSnapshot(t, svc)
			job := got.Exports[old.ID]
			if objects.puts.Load() != puts || job.ObjectPath != "" || len(got.ExportCheckpoints) != 0 {
				t.Fatal("late source read published or advanced a revoked selected stage")
			}
			switch mode {
			case "owner-cancel":
				if job.Status != domain.ExportStatusCancelled || len(got.ExportPlans) != 0 || len(got.ExportLeases) != 0 {
					t.Fatal("late selected read overwrote terminal cancellation")
				}
			case "lease-takeover":
				if job.Status != "running" || got.ExportLeases[old.ID].Owner == current.Owner || got.ExportPlans[old.ID].Signature != old.Plan.Signature {
					t.Fatal("late selected read overwrote replacement ownership or plan")
				}
			case "new-Hold":
				if job.Status != "failed" || job.FailureCode != domain.ExportFailureLegalHold || len(got.ExportPlans) != 0 || len(got.ExportLeases) != 0 {
					t.Fatal("selected continuation bypassed the new current Hold")
				}
			}
		})
	}
}

func TestExportPlanSourceAndReadAuditShareOneOperationDeadline(t *testing.T) {
	svc, backend, objects := newPlanService(t)
	old := leaveSelectedPlan(t, svc)
	expireCheckpoint(svc)
	probe := &planSourceArchive{Store: objects, entered: make(chan context.Context, 1), release: make(chan struct{})}
	svc.Config.Archive = probe
	backend.readEntered, backend.readRelease = make(chan context.Context, 1), make(chan struct{})
	var sourceOnce, readOnce sync.Once
	releaseSource := func() { sourceOnce.Do(func() { close(probe.release) }) }
	releaseRead := func() { readOnce.Do(func() { close(backend.readRelease) }) }
	done := make(chan struct{})
	t.Cleanup(func() { releaseSource(); releaseRead(); <-done })
	go func() { svc.runExport(testCtx, old.ID); close(done) }()
	sourceCtx := requireCheckpointContext(t, probe.entered)
	releaseSource()
	readCtx := requireCheckpointContext(t, backend.readEntered)
	sourceDeadline, _ := sourceCtx.Deadline()
	readDeadline, _ := readCtx.Deadline()
	if sourceDeadline.IsZero() || !sourceDeadline.Equal(readDeadline) {
		t.Fatal("selected-stage source/audit restarted operation deadline")
	}
	backend.failRead.Store(true) // append already reached its actual boundary; revoke instead
	pauseExportDispatch(svc)
	if _, err := svc.CancelExport(testCtx, old.TenantID, old.RequestedBy, old.ID); err != nil {
		t.Fatal(err)
	}
	releaseRead()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("late read-audit executor did not finish")
	}
	if got := leaseSnapshot(t, svc); got.Exports[old.ID].Status != domain.ExportStatusCancelled || len(got.ExportPlans) != 0 {
		t.Fatal("late audit append resurrected selected work")
	}
}

func TestExportPlanDefaultSourceDeadlineRetainsKnownPlan(t *testing.T) {
	svc, _, objects := newPlanService(t)
	old := leaveSelectedPlan(t, svc)
	expireCheckpoint(svc)
	probe := &planSourceArchive{Store: objects, entered: make(chan context.Context, 1), release: make(chan struct{})}
	svc.Config.Archive = probe
	done := make(chan struct{})
	t.Cleanup(func() { close(probe.release); <-done })
	puts := objects.puts.Load()
	go func() { svc.runExport(testCtx, old.ID); close(done) }()
	requireCheckpointContext(t, probe.entered)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("selected-stage Get exceeded the unchanged production operation deadline")
	}
	got := leaseSnapshot(t, svc)
	if got.Exports[old.ID].Status != "running" || got.ExportPlans[old.ID].Signature != old.Plan.Signature || objects.puts.Load() != puts || got.Exports[old.ID].ObjectPath != "" {
		t.Fatal("timed-out selected stage lost its plan or published output")
	}
}
