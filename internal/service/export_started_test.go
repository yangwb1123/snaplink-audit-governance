package service

import (
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestLongQueuedExportRecoveryUsesClaimTime(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	svc.Config.StuckExportAge = 24 * time.Hour
	seedExportJob(backend, domain.ExportJob{ID: "long-queued", TenantID: "tenant-a", Status: "pending", CreatedAt: pinnedNow.Add(-72 * time.Hour)})
	job, claimed, err := svc.claimPendingExport(testCtx, "long-queued")
	if err != nil || !claimed || job.StartedAt == nil || !job.StartedAt.Equal(pinnedNow) {
		t.Fatalf("claim=%+v claimed=%v err=%v", job, claimed, err)
	}
	saves := backend.saves
	count, err := svc.RecoverStuckExports(testCtx, "tenant-a")
	if err != nil || count != 0 || backend.saves != saves {
		t.Fatalf("freshly claimed old queue job recovered: count=%d saves=%d err=%v", count, backend.saves, err)
	}
	page, err := svc.ListExports(testCtx, "tenant-a", "actor", domain.JobQuery{})
	if err != nil || len(page.Items) != 1 || page.Items[0].StartedAt == nil || !page.Items[0].StartedAt.Equal(pinnedNow) {
		t.Fatalf("work list lost the real start time: %+v err=%v", page, err)
	}
}

func TestRecoveryStartBoundaryAndLegacyFallback(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	svc.Config.StuckExportAge = 24 * time.Hour
	started := pinnedNow.Add(-24 * time.Hour)
	current := stuckJob("started", "tenant-a", pinnedNow.Add(-72*time.Hour))
	current.StartedAt = &started
	seedExportJob(backend, current)
	seedExportJob(backend, stuckJob("legacy", "tenant-a", pinnedNow.Add(-25*time.Hour)))
	count, err := svc.RecoverStuckExports(testCtx, "tenant-a")
	if err != nil || count != 1 || backend.data.Exports["started"].Status != "running" || backend.data.Exports["legacy"].Status != "failed" {
		t.Fatalf("start boundary/legacy fallback failed: count=%d err=%v", count, err)
	}
	svc.Config.Now = func() time.Time { return pinnedNow.Add(time.Nanosecond) }
	count, err = svc.RecoverStuckExports(testCtx, "tenant-a")
	if err != nil || count != 1 || !strings.Contains(backend.data.Exports["started"].Error, started.Format(time.RFC3339)) {
		t.Fatalf("recovery did not use real started_at: count=%d err=%v", count, err)
	}
}
