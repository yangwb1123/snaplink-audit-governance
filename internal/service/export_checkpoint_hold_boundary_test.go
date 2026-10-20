package service

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
)

type checkpointHoldBoundaryArchive struct {
	archive.Store
	sealed  chan context.Context
	entered chan context.Context
	release chan struct{}
}

func (a *checkpointHoldBoundaryArchive) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasPrefix(key, "exports/") {
		a.sealed <- ctx
	} else {
		a.entered <- ctx
		select {
		case <-a.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return a.Store.Get(ctx, key)
}

func newCheckpointHoldBoundaryArchive(objects archive.Store) *checkpointHoldBoundaryArchive {
	return &checkpointHoldBoundaryArchive{Store: objects, sealed: make(chan context.Context, 2), entered: make(chan context.Context, 2), release: make(chan struct{})}
}

func requireCheckpointContext(t *testing.T, entered <-chan context.Context) context.Context {
	t.Helper()
	select {
	case ctx := <-entered:
		return ctx
	case <-time.After(10 * time.Second):
		t.Fatal("current-Hold verification did not reach its actual I/O boundary")
		return nil
	}
}

func TestExportCheckpointHoldSourceGetIsLockFreeAndSharesFinalizationDeadline(t *testing.T) {
	for _, cancelJob := range []bool{true, false} {
		t.Run(map[bool]string{true: "owner-cancel", false: "Hold-changed-after-source-read"}[cancelJob], func(t *testing.T) {
			svc, backend, objects, _, cp := checkpointHoldFixture(t, "")
			if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", CreatedBy: "case-worker", Name: "nonmatching", Reason: "case", Filter: domain.Query{From: pinnedNow, To: pinnedNow.Add(time.Minute), ActorID: "nonmatching-actor"}}); err != nil {
				t.Fatal("could not seed current Hold")
			}
			pauseExportDispatch(svc)
			var clock atomic.Int64
			clock.Store(pinnedNow.Add(ExportLeaseDuration).UnixNano())
			svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
			boundary := newCheckpointHoldBoundaryArchive(objects)
			svc.Config.Archive = boundary
			backend.trailEntered, backend.trailRelease = make(chan context.Context, 1), make(chan struct{})
			var sourceOnce, trailOnce sync.Once
			releaseSource := func() { sourceOnce.Do(func() { close(boundary.release) }) }
			releaseTrail := func() { trailOnce.Do(func() { close(backend.trailRelease) }) }
			done := make(chan struct{})
			t.Cleanup(func() { releaseSource(); releaseTrail(); <-done })
			go func() { svc.runExport(testCtx, cp.JobID); close(done) }()
			sealedCtx := requireCheckpointContext(t, boundary.sealed)
			sourceCtx := requireCheckpointContext(t, boundary.entered)
			sealedDeadline, _ := sealedCtx.Deadline()
			sourceDeadline, _ := sourceCtx.Deadline()
			if sealedDeadline.IsZero() || !sealedDeadline.Equal(sourceDeadline) {
				t.Fatal("current-Hold source Get restarted the sealed-stage deadline")
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
				if cancelJob {
					_, err := svc.CancelExport(testCtx, "tenant-a", "operator", cp.JobID)
					controlDone <- err
					return
				}
				controlDone <- nil
			}()
			select {
			case err := <-controlDone:
				if err != nil {
					t.Fatal("renewal/cancellation failed during current-Hold Get")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("current-Hold source Get retained Store.mu")
			}
			releaseSource()
			trailCtx := requireCheckpointContext(t, backend.trailEntered)
			trailDeadline, _ := trailCtx.Deadline()
			if !sealedDeadline.Equal(trailDeadline) {
				t.Fatal("current-Hold verification restarted the read self-audit deadline")
			}
			if !cancelJob {
				if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", CreatedBy: "case-worker", Name: "matching", Reason: "case"}); err != nil {
					t.Fatal("could not change current Hold after source authentication")
				}
			}
			releaseTrail()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("current-Hold finalizer did not finish")
			}
			data := leaseSnapshot(t, svc)
			job := data.Exports[cp.JobID]
			if job.ObjectPath != "" || checkpointFactCount(t, svc, domain.AdminActionExportCompleted) != 0 {
				t.Fatal("late current-Hold verification published an object")
			}
			if cancelJob {
				if job.Status != domain.ExportStatusCancelled || len(data.ExportCheckpoints) != 0 || len(data.ExportLeases) != 0 {
					t.Fatal("source verification overwrote owner cancellation")
				}
			} else if job.Status != "running" || data.ExportCheckpoints[cp.JobID] != cp || len(data.ExportLeases) != 1 {
				t.Fatal("changed Hold set did not retain the original resumable checkpoint")
			}
		})
	}
}

func TestExportCheckpointHoldSourceDeadlineRetainsCheckpoint(t *testing.T) {
	svc, _, objects, _, cp := checkpointHoldFixture(t, "")
	if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", CreatedBy: "case-worker", Name: "nonmatching", Reason: "case", Filter: domain.Query{From: pinnedNow, To: pinnedNow.Add(time.Minute), ActorID: "nonmatching-actor"}}); err != nil {
		t.Fatal("could not seed current Hold")
	}
	expireCheckpoint(svc)
	boundary := newCheckpointHoldBoundaryArchive(objects)
	svc.Config.Archive = boundary
	done := make(chan struct{})
	t.Cleanup(func() { close(boundary.release); <-done })
	go func() { svc.runExport(testCtx, cp.JobID); close(done) }()
	requireCheckpointContext(t, boundary.entered)
	// Wait for the actual, unchanged Store operation deadline only after the
	// source Get starts. A separate wait guard is not a preparation SLA.
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("current-Hold source Get outlived the Store deadline")
	}
	data := leaseSnapshot(t, svc)
	if data.Exports[cp.JobID].Status != "running" || data.Exports[cp.JobID].ObjectPath != "" || data.ExportCheckpoints[cp.JobID] != cp || len(data.ExportLeases) != 1 || checkpointFactCount(t, svc, domain.AdminActionExportCompleted) != 0 {
		t.Fatal("timed-out current-Hold evidence destroyed or completed the checkpoint")
	}
}
