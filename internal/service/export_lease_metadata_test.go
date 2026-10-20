package service

import (
	"errors"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportLeaseVersionAndTimeMetadataValidation(t *testing.T) {
	started := pinnedNow
	job := domain.ExportJob{ID: "job", TenantID: "tenant-a", StartedAt: &started}
	lease := store.ExportLease{Version: 1, JobID: job.ID, TenantID: job.TenantID, Owner: "owner",
		HeartbeatAt: started, ExpiresAt: started.Add(ExportLeaseDuration)}
	if !validExportLease(job, lease) {
		t.Fatal("generated lease is invalid")
	}
	for _, mutate := range []func(*store.ExportLease){
		func(l *store.ExportLease) { l.Version = 0 },
		func(l *store.ExportLease) { l.Version = 2 },
		func(l *store.ExportLease) { l.JobID = "other" },
		func(l *store.ExportLease) { l.TenantID = "other" },
		func(l *store.ExportLease) { l.Owner = "" },
		func(l *store.ExportLease) { l.HeartbeatAt = time.Time{} },
		func(l *store.ExportLease) {
			l.HeartbeatAt = started.Add(-time.Second)
			l.ExpiresAt = l.HeartbeatAt.Add(ExportLeaseDuration)
		},
		func(l *store.ExportLease) { l.ExpiresAt = started },
		func(l *store.ExportLease) { l.ExpiresAt = l.ExpiresAt.Add(time.Nanosecond) },
	} {
		invalid := lease
		mutate(&invalid)
		if validExportLease(job, invalid) {
			t.Fatal("malformed/version-mismatched lease was accepted")
		}
	}
	for _, invalid := range []domain.ExportJob{{ID: "", TenantID: job.TenantID, StartedAt: &started}, {ID: job.ID, TenantID: "", StartedAt: &started}, {ID: job.ID, TenantID: job.TenantID}} {
		if validExportLease(invalid, lease) {
			t.Fatal("private lease inferred for a job without identity/start")
		}
	}
}

func TestExportLeaseRecoveryRejectsCoPoisonedJobAndLeaseMapIdentity(t *testing.T) {
	svc := testService(t, true)
	seedLeaseExport(t, svc, "map-key", "tenant-a")
	if _, won, err := svc.claimPendingExport(testCtx, "map-key"); err != nil || !won {
		t.Fatal(err)
	}
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		job := data.Exports["map-key"]
		job.ID = "different-id"
		data.Exports["map-key"] = job
		lease := data.ExportLeases["map-key"]
		lease.JobID = job.ID
		data.ExportLeases["map-key"] = lease
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	svc.Config.Now = func() time.Time { return pinnedNow.Add(ExportLeaseDuration) }
	if n, err := svc.recoverExpiredExportLeases(testCtx); !errors.Is(err, domain.ErrConflict) || n != 0 {
		t.Fatal("matching poisoned bodies bypassed authoritative map identity")
	}
	if leaseSnapshot(t, svc).Exports["map-key"].Status != "running" {
		t.Fatal("invalid map binding produced partial recovery")
	}
}
