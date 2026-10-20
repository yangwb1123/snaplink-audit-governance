package service

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestExportGenerationRemoteBoundariesReleaseLockAndFenceLateOwner(t *testing.T) {
	for _, boundary := range []string{"page-Get", "page-Put"} {
		for _, mode := range []string{"cancel", "takeover", "Hold-change"} {
			t.Run(boundary+"/"+mode, func(t *testing.T) {
				svc, _, objects := newGenerationService(t)
				old := leaveGeneratedPage(t, svc)
				pauseExportDispatch(svc)
				var clock atomic.Int64
				clock.Store(pinnedNow.Add(ExportLeaseDuration).UnixNano())
				svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
				probe := &generationArchive{Store: objects, release: make(chan struct{}), ignoreCancel: true}
				if boundary == "page-Get" {
					probe.getEntered = make(chan context.Context, 1)
				} else {
					probe.putEntered = make(chan leasePutObservation, 1)
				}
				svc.Config.Archive = probe
				done := make(chan struct{})
				var once sync.Once
				release := func() { once.Do(func() { close(probe.release) }) }
				t.Cleanup(func() { release(); <-done })
				go func() { svc.runExport(testCtx, old.ID); close(done) }()
				var put leasePutObservation
				if boundary == "page-Get" {
					requireCheckpointContext(t, probe.getEntered)
				} else {
					select {
					case put = <-probe.putEntered:
					case <-time.After(30 * time.Second):
						t.Fatal("actual page Put not reached")
					}
				}
				control := make(chan error, 1)
				go func() {
					data := leaseSnapshot(t, svc)
					current := exportClaim{ExportJob: data.Exports[old.ID], Owner: data.ExportLeases[old.ID].Owner, Plan: data.ExportPlans[old.ID]}
					clock.Store(pinnedNow.Add(ExportLeaseDuration + ExportHeartbeatInterval).UnixNano())
					if err := svc.renewExportLease(testCtx, current); err != nil {
						control <- err
						return
					}
					switch mode {
					case "cancel":
						_, err := svc.CancelExport(testCtx, old.TenantID, old.RequestedBy, old.ID)
						control <- err
					case "takeover":
						clock.Store(svc.Now().Add(ExportLeaseDuration).UnixNano())
						_, won, err := svc.claimExport(testCtx, old.ID, true)
						if err == nil && !won {
							err = errExportLeaseLost
						}
						control <- err
					case "Hold-change":
						_, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: old.TenantID, CreatedBy: "case-worker", Name: "current", Reason: "case"})
						control <- err
					}
				}()
				select {
				case err := <-control:
					if err != nil {
						t.Fatal("control operation failed at actual remote page boundary")
					}
				case <-time.After(10 * time.Second):
					t.Fatal("page I/O retained Store.mu")
				}
				release()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("late page executor did not stop")
				}
				data := leaseSnapshot(t, svc)
				job := data.Exports[old.ID]
				if job.ObjectPath != "" || len(data.ExportCheckpoints) != 0 || probe.finalPuts.Load() != 0 || checkpointFactCount(t, svc, domain.AdminActionExportProgressed) != 1 {
					t.Fatal("late I/O advanced generation or published an output")
				}
				if mode == "cancel" {
					if job.Status != domain.ExportStatusCancelled || len(data.ExportPlans) != 0 || len(data.ExportLeases) != 0 {
						t.Fatal("late page overwrote terminal cancellation")
					}
				} else if job.Status != "running" || !reflect.DeepEqual(data.ExportPlans[old.ID], old.Plan) {
					t.Fatal("late page overwrote known prefix or replacement state")
				}
				if boundary == "page-Put" {
					if probe.chunkPuts.Load() != 1 {
						t.Fatal("page Put did not really execute")
					}
					if raw, err := objects.Get(testCtx, put.key); err != nil || len(raw) == 0 {
						t.Fatal("late external commit was deleted or treated as rolled back")
					}
				}
				if _, err := objects.Get(testCtx, old.Plan.Chunks[0].ObjectPath); err != nil {
					t.Fatal("control transition deleted original immutable page")
				}
			})
		}
	}
}

func TestExportGenerationPageSourceAndAuditShareOriginalDeadline(t *testing.T) {
	svc, backend, objects := newGenerationService(t)
	old := selectedGenerationPlan(t, svc)
	expireCheckpoint(svc)
	probe := &planSourceArchive{Store: objects, entered: make(chan context.Context, 128), release: make(chan struct{})}
	svc.Config.Archive = probe
	backend.generationReadEntered, backend.generationReadRelease = make(chan context.Context, 1), make(chan struct{})
	ctx, cancel := context.WithCancel(testCtx)
	done := make(chan struct{})
	var sourceOnce, auditOnce sync.Once
	releaseSource := func() { sourceOnce.Do(func() { close(probe.release) }) }
	releaseAudit := func() { auditOnce.Do(func() { close(backend.generationReadRelease) }) }
	t.Cleanup(func() { cancel(); releaseSource(); releaseAudit(); <-done })
	go func() { svc.runExport(ctx, old.ID); close(done) }()
	sourceCtx := requireCheckpointContext(t, probe.entered)
	releaseSource()
	auditCtx := requireCheckpointContext(t, backend.generationReadEntered)
	sourceDeadline, _ := sourceCtx.Deadline()
	auditDeadline, _ := auditCtx.Deadline()
	if sourceDeadline.IsZero() || !sourceDeadline.Equal(auditDeadline) {
		t.Fatal("page source/audit restarted operation deadline")
	}
	cancel() // withdraw only after both actual operation boundaries
	releaseAudit()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled page operation did not stop")
	}
	if got := leaseSnapshot(t, svc); got.Exports[old.ID].Status != "running" || !reflect.DeepEqual(got.ExportPlans[old.ID], old.Plan) || len(got.ExportCheckpoints) != 0 {
		t.Fatal("cancelled audit invented byte progress")
	}
	if !errors.Is(auditCtx.Err(), context.Canceled) {
		t.Fatal("audit did not inherit page cancellation")
	}
}

func TestExportGenerationCommittedPageGetUsesUnchangedProductionDeadline(t *testing.T) {
	svc, _, objects := newGenerationService(t)
	old := leaveGeneratedPage(t, svc)
	expireCheckpoint(svc)
	probe := &generationArchive{Store: objects, getEntered: make(chan context.Context, 1), release: make(chan struct{})}
	svc.Config.Archive = probe
	done := make(chan struct{})
	t.Cleanup(func() { close(probe.release); <-done })
	go func() { svc.runExport(testCtx, old.ID); close(done) }()
	getCtx := requireCheckpointContext(t, probe.getEntered)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("committed page Get outlived original production operation deadline")
	}
	if !errors.Is(getCtx.Err(), context.DeadlineExceeded) {
		t.Fatal("remote page did not reach actual production deadline")
	}
	if got := leaseSnapshot(t, svc); got.Exports[old.ID].Status != "running" || !reflect.DeepEqual(got.ExportPlans[old.ID], old.Plan) || probe.chunkPuts.Load() != 0 || probe.finalPuts.Load() != 0 {
		t.Fatal("page timeout discarded or regenerated durable prefix")
	}
}
