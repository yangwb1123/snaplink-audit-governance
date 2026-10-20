package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestCancelExportAtRealPutBoundaryFencesLateExternalCommit(t *testing.T) {
	svc := testService(t, true)
	pauseExportDispatch(svc)
	ticks := make(chan time.Time, 1)
	svc.exportHeartbeat = func() (<-chan time.Time, func()) { return ticks, func() {} }
	paused := &leasePausedArchive{Store: svc.Config.Archive, entered: make(chan leasePutObservation, 1), release: make(chan struct{}), ignoreCancel: true}
	svc.Config.Archive = paused
	seedActionExport(t, svc, "cancel-put", "pending")
	ctx, cancel := context.WithCancel(testCtx)
	var once sync.Once
	release := func() { once.Do(func() { close(paused.release) }) }
	done := make(chan struct{})
	t.Cleanup(func() { cancel(); release(); <-done })
	go func() { svc.runExport(ctx, "cancel-put"); close(done) }()
	var put leasePutObservation
	select {
	case put = <-paused.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("executor did not reach Put")
	}
	if summary, err := svc.CancelExport(testCtx, "tenant-a", "operator", "cancel-put"); err != nil || summary.Status != domain.ExportStatusCancelled {
		t.Fatal(err)
	}
	ticks <- svc.Now() // real renewal reads revoked control and cancels Put ctx
	select {
	case <-put.ctx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("heartbeat did not observe cancellation")
	}
	release()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("late Put did not return")
	}
	data := leaseSnapshot(t, svc)
	job := data.Exports["cancel-put"]
	if job.Status != domain.ExportStatusCancelled || job.ObjectPath != "" || job.Digest != "" || job.EventCount != 0 || len(data.ExportLeases) != 0 {
		t.Fatal("late executor overwrote canceled control")
	}
	if sealed, err := paused.Store.Get(testCtx, put.key); err != nil || len(sealed) == 0 {
		t.Fatal("cancellation deleted WORM or pretended Put rollback")
	}
}
