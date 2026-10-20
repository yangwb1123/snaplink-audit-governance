package service

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestExportQueueReapsExpiredLeaseBeforeDispatchingPending(t *testing.T) {
	svc := testService(t, true)
	var clock atomic.Int64
	clock.Store(pinnedNow.UnixNano())
	svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	seedLeaseExport(t, svc, "crashed-job", "tenant-a")
	if _, won, err := svc.claimPendingExport(testCtx, "crashed-job"); err != nil || !won {
		t.Fatal(err)
	}
	seedLeaseExport(t, svc, "export-after-expiry", "tenant-a")
	clock.Store(pinnedNow.Add(ExportLeaseDuration).UnixNano())
	ctx, cancel := context.WithCancel(testCtx)
	stopped := make(chan struct{})
	go func() { svc.RunExportQueue(ctx); close(stopped) }()
	defer func() { cancel(); <-stopped }()
	job := waitExportCompleted(t, svc, "export-after-expiry")
	if job.Status != "completed" || leaseSnapshot(t, svc).Exports["crashed-job"].Status != "failed" {
		t.Fatal("queue did not retire expired budget before next claim")
	}
}
