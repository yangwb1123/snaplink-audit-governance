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
	"github.com/snaplink/audit-governance/internal/store"
)

func accessNonmatchingHold(t *testing.T, svc *Service) {
	t.Helper()
	if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", Name: "nonmatching", Reason: "case", Filter: domain.Query{From: pinnedNow, To: pinnedNow.Add(time.Minute), ActorID: "nonmatching-actor"}}); err != nil {
		t.Fatal("could not seed nonmatching current Hold")
	}
}

func awaitExportAccess(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		t.Fatal("export access did not end within its unchanged operation bound")
		return nil
	}
}

func TestExportAccessSourceGetReleasesStoreLockAndFencesCancellation(t *testing.T) {
	for _, surface := range []string{"detail", "download-audit"} {
		t.Run(surface, func(t *testing.T) {
			svc, _, objects, _, cp := checkpointHoldFixture(t, "")
			pauseExportDispatch(svc)
			accessNonmatchingHold(t, svc)
			data := leaseSnapshot(t, svc)
			claim := exportClaim{ExportJob: data.Exports[cp.JobID], Owner: data.ExportLeases[cp.JobID].Owner, Checkpoint: cp}
			var clock atomic.Int64
			clock.Store(pinnedNow.UnixNano())
			svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
			boundary := newCheckpointHoldBoundaryArchive(objects)
			svc.Config.Archive = boundary
			var once sync.Once
			release := func() { once.Do(func() { close(boundary.release) }) }
			done, exited := make(chan error, 1), make(chan struct{})
			var observed domain.ExportJob
			t.Cleanup(func() { release(); <-exited })
			go func() {
				defer close(exited)
				if surface == "detail" {
					var err error
					observed, err = svc.GetExport(testCtx, "tenant-a", cp.JobID)
					done <- err
					return
				}
				done <- callExportAccess(t, testCtx, svc, surface, "tenant-a", cp.JobID)
			}()
			requireCheckpointContext(t, boundary.entered)
			control := make(chan error, 1)
			go func() {
				clock.Store(pinnedNow.Add(ExportHeartbeatInterval).UnixNano())
				if err := svc.renewExportLease(testCtx, claim); err != nil {
					control <- err
					return
				}
				_, err := svc.CancelExport(testCtx, "tenant-a", "operator", cp.JobID)
				control <- err
			}()
			select {
			case err := <-control:
				if err != nil {
					t.Fatal("lease renewal or cancellation failed while access Get was paused")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("access Hold Get retained Store.mu")
			}
			release()
			err := awaitExportAccess(t, done)
			if surface == "detail" {
				if err != nil || observed.Status != domain.ExportStatusCancelled || observed.ObjectPath != "" {
					t.Fatal("detail polling did not observe the newer cancelled state")
				}
			} else if !errors.Is(err, domain.ErrConflict) {
				t.Fatal("late download authorization ignored owner cancellation")
			}
			data = leaseSnapshot(t, svc)
			if data.Exports[cp.JobID].Status != domain.ExportStatusCancelled || len(data.ExportCheckpoints) != 0 || len(data.ExportLeases) != 0 || checkpointFactCount(t, svc, domain.AdminActionEventExport) != 0 {
				t.Fatal("access overwrote cancellation or recorded download success")
			}
		})
	}
}

func TestExportAccessHoldChangeDuringSourceGetFailsClosed(t *testing.T) {
	for _, surface := range []string{"detail", "download-audit"} {
		t.Run(surface, func(t *testing.T) {
			svc, _, objects, _, job := completedCheckpointAccessFixture(t, "", 10)
			accessNonmatchingHold(t, svc)
			boundary := newCheckpointHoldBoundaryArchive(objects)
			svc.Config.Archive = boundary
			var once sync.Once
			release := func() { once.Do(func() { close(boundary.release) }) }
			done, exited := make(chan error, 1), make(chan struct{})
			t.Cleanup(func() { release(); <-exited })
			go func() { defer close(exited); done <- callExportAccess(t, testCtx, svc, surface, "tenant-a", job.ID) }()
			requireCheckpointContext(t, boundary.entered)
			if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", Name: "matching", Reason: "case"}); err != nil {
				t.Fatal("could not change current Hold during access Get")
			}
			release()
			if err := awaitExportAccess(t, done); !errors.Is(err, domain.ErrConflict) {
				t.Fatal("stale current Hold observation allowed access")
			}
			if !reflect.DeepEqual(leaseSnapshot(t, svc).Exports[job.ID], job) || checkpointFactCount(t, svc, domain.AdminActionEventExport) != 0 {
				t.Fatal("stale access changed terminal state or recorded success")
			}
			// A fresh attempt evaluates the newly committed Hold, not a hidden
			// loop/retry using the obsolete observation.
			if err := callExportAccess(t, testCtx, svc, surface, "tenant-a", job.ID); !errors.Is(err, domain.ErrForbidden) {
				t.Fatal("fresh access did not evaluate the current matching Hold")
			}
		})
	}
}

type exportAccessTrailBackend struct {
	*checkpointBackend
	entered chan context.Context
	release chan struct{}
	fail    bool // installed before the request; never mutated concurrently
}

func (b *exportAccessTrailBackend) AppendAdminFact(ctx context.Context, fact domain.AdminAction, cap int) error {
	if fact.Action == domain.AdminActionEventExport {
		b.entered <- ctx
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
		if b.fail {
			return errors.New("injected export access self-audit failure")
		}
	}
	return b.checkpointBackend.AppendAdminFact(ctx, fact, cap)
}

func TestExportAccessDownloadAuditSharesDeadlineAndRechecksControl(t *testing.T) {
	for _, interruption := range []string{"Hold-change", "job-change", "self-audit-failure", "caller-withdrawal", "foreign-Hold"} {
		t.Run(interruption, func(t *testing.T) {
			svc, backend, objects, source, job := completedCheckpointAccessFixture(t, "", 10)
			late := testEvent("access-late", "checkpoint-operation", pinnedNow.Add(20*time.Second))
			late.SchemaID = source.SchemaID
			if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, late, domain.StatusArchived); err != nil {
				t.Fatal("could not commit late current-Hold evidence")
			}
			accessNonmatchingHold(t, svc)
			trail := &exportAccessTrailBackend{checkpointBackend: backend, entered: make(chan context.Context, 1), release: make(chan struct{}), fail: interruption == "self-audit-failure"}
			svc.Store = store.NewWithBackend(trail)
			boundary := newCheckpointHoldBoundaryArchive(objects)
			svc.Config.Archive = boundary
			ctx, cancel := context.WithCancel(testCtx)
			var sourceOnce, trailOnce sync.Once
			releaseSource := func() { sourceOnce.Do(func() { close(boundary.release) }) }
			releaseTrail := func() { trailOnce.Do(func() { close(trail.release) }) }
			done, exited := make(chan error, 1), make(chan struct{})
			t.Cleanup(func() { cancel(); releaseSource(); releaseTrail(); <-exited })
			go func() { defer close(exited); done <- svc.RecordExportDownload(ctx, "tenant-a", "operator", job.ID) }()
			firstCtx := requireCheckpointContext(t, boundary.entered)
			releaseSource()
			secondCtx := requireCheckpointContext(t, boundary.entered)
			trailCtx := requireCheckpointContext(t, trail.entered)
			deadline, _ := firstCtx.Deadline()
			for _, stageCtx := range []context.Context{secondCtx, trailCtx} {
				stageDeadline, _ := stageCtx.Deadline()
				if deadline.IsZero() || !deadline.Equal(stageDeadline) {
					t.Fatal("source reads or self-audit restarted the access deadline")
				}
			}
			switch interruption {
			case "Hold-change", "foreign-Hold":
				tenant := "tenant-a"
				if interruption == "foreign-Hold" {
					tenant = "tenant-b"
					if err := svc.CreateTenant(testCtx, "test", domain.Tenant{ID: tenant, Name: "Isolated foreign tenant", Active: true}); err != nil {
						t.Fatal("could not create isolated foreign tenant")
					}
				}
				if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: tenant, Name: "matching", Reason: "case"}); err != nil {
					t.Fatal("could not change Hold while the independent trail append was paused")
				}
			case "job-change":
				if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
					changed := data.Exports[job.ID]
					changed.Query.ActorID = "obsolete-writer-drift"
					data.Exports[job.ID] = changed
					return nil
				}); err != nil {
					t.Fatal("could not install owned job drift at the actual append boundary")
				}
			case "caller-withdrawal":
				cancel()
			}
			releaseTrail()
			err := awaitExportAccess(t, done)
			switch interruption {
			case "Hold-change", "job-change":
				if !errors.Is(err, domain.ErrConflict) {
					t.Fatal("control change during self-audit allowed download")
				}
			case "caller-withdrawal":
				if !errors.Is(err, context.Canceled) {
					t.Fatal("withdrawn self-audit allowed download")
				}
			case "self-audit-failure":
				if err == nil {
					t.Fatal("unrecorded download audit allowed access")
				}
			case "foreign-Hold":
				if err != nil {
					t.Fatal("unrelated tenant control change invalidated access")
				}
			}
			wantFacts := 1 // a recorded attempt never proves delivery of bytes
			if interruption == "self-audit-failure" || interruption == "caller-withdrawal" {
				wantFacts = 0
			}
			if checkpointFactCount(t, svc, domain.AdminActionEventExport) != wantFacts {
				t.Fatal("download attempt audit did not match the real append outcome")
			}
		})
	}
}

func TestExportAccessSourceDeadlineAndWithdrawalFailClosed(t *testing.T) {
	for _, surface := range []string{"detail", "download-audit"} {
		for _, withdrawal := range []bool{false, true} {
			name := "store-deadline"
			if withdrawal {
				name = "caller-withdrawal"
			}
			t.Run(surface+"/"+name, func(t *testing.T) {
				svc, _, objects, _, job := completedCheckpointAccessFixture(t, "", 10)
				accessNonmatchingHold(t, svc)
				boundary := newCheckpointHoldBoundaryArchive(objects)
				svc.Config.Archive = boundary
				ctx, cancel := context.WithCancel(testCtx)
				done, exited := make(chan error, 1), make(chan struct{})
				t.Cleanup(func() { cancel(); close(boundary.release); <-exited })
				go func() { defer close(exited); done <- callExportAccess(t, ctx, svc, surface, "tenant-a", job.ID) }()
				requireCheckpointContext(t, boundary.entered)
				if withdrawal {
					cancel() // only after the real source Get has started
				}
				err := awaitExportAccess(t, done)
				if withdrawal {
					if !errors.Is(err, context.Canceled) || svc.Store.OperationDeadlineExhaustions() != 0 {
						t.Fatal("caller cancellation was misclassified as a store timeout")
					}
				} else if !errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || svc.Store.OperationDeadlineExhaustions() != 1 {
					t.Fatal("access did not retain the original single Store timeout classification")
				}
				if !reflect.DeepEqual(leaseSnapshot(t, svc).Exports[job.ID], job) || checkpointFactCount(t, svc, domain.AdminActionEventExport) != 0 {
					t.Fatal("interrupted access changed terminal state or recorded success")
				}
			})
		}
	}
}
