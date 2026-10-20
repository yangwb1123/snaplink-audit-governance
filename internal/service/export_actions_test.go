package service

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Unit action tests suppress only asynchronous dispatch, not claim/CAS/Hold
// checks. Real executor and cross-replica tests exercise the production wake.
func pauseExportDispatch(svc *Service) {
	svc.exportMu.Lock()
	svc.exportDispatching = true
	svc.exportMu.Unlock()
}

func seedActionExport(t *testing.T, svc *Service, id, status string) domain.ExportJob {
	t.Helper()
	at := svc.Now()
	job := domain.ExportJob{ID: id, TenantID: "tenant-a", RequestedBy: "operator", Status: status,
		CreatedAt: at, Query: domain.Query{From: at, To: at.Add(time.Minute)}}
	if status == "failed" || status == "completed" || status == domain.ExportStatusCancelled {
		job.FinishedAt = &at
	}
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error { data.Exports[id] = job; return nil }); err != nil {
		t.Fatal(err)
	}
	return job
}

func TestCancelExportOwnerIsolationMetadataOnlyAndIdempotency(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	pauseExportDispatch(svc)
	original := seedActionExport(t, svc, "cancel-pending", "pending")
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		job := data.Exports[original.ID]
		job.Query.ActorID, job.Query.PayloadDigest = "private-selector", "private-digest"
		job.Error, job.ObjectPath, job.Digest, job.EventCount = "private-error", "private-path", "private-hash", 99
		data.Exports[job.ID] = job
		data.LegalHolds["hold"] = domain.LegalHold{ID: "hold", TenantID: job.TenantID, Reason: "private-reason"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		tenant, actor string
		want          error
	}{
		{"tenant-b", "operator", domain.ErrNotFound}, {"tenant-a", "other", domain.ErrForbidden},
		{"tenant-a", "", domain.ErrInvalid}, {"", "operator", domain.ErrInvalid},
	} {
		saves := backend.saves
		if summary, err := svc.CancelExport(testCtx, tc.tenant, tc.actor, original.ID); !errors.Is(err, tc.want) || summary.ID != "" || backend.saves != saves {
			t.Fatalf("isolation failed: %v %+v", err, summary)
		}
	}
	result, err := svc.CancelExport(testCtx, original.TenantID, original.RequestedBy, original.ID)
	if err != nil || result.Status != domain.ExportStatusCancelled || result.FailureCode != domain.ExportFailureCancelled {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(result)
	for _, forbidden := range []string{"private", "query", "digest", "event_count", "error", "owner", "lease"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatal("cancel summary exposed evidence/runtime detail", forbidden)
		}
	}
	saves := backend.saves
	if again, err := svc.CancelExport(testCtx, original.TenantID, original.RequestedBy, original.ID); err != nil || !reflect.DeepEqual(again, result) || backend.saves != saves {
		t.Fatal("same-owner repeated cancel wrote or changed result")
	}
	data := leaseSnapshot(t, svc)
	job := data.Exports[original.ID]
	if job.ObjectPath != "" || job.Digest != "" || job.EventCount != 0 || job.Error != "" || job.FinishedAt == nil {
		t.Fatal("cancel did not retire execution output")
	}
	facts := 0
	for _, fact := range data.AdminActions {
		if fact.Action == domain.AdminActionExportCancelled {
			facts++
			if fact.Actor != original.RequestedBy || fact.Detail != "status=cancelled" || fact.TargetID != original.ID {
				t.Fatal("cancel fact is not bound to the authorized command")
			}
		}
	}
	if facts != 1 {
		t.Fatal("cancel did not commit exactly one fact")
	}
	page, err := svc.ListExports(testCtx, "tenant-a", "operator", domain.JobQuery{Status: domain.ExportStatusCancelled})
	if err != nil || page.Count != 1 || page.Items[0].ID != original.ID {
		t.Fatal("cancelled work list not available", err)
	}
	status, err := svc.GovernanceStatus(testCtx, "tenant-a", "operator", GovernanceStatusAccess{Exports: true})
	if err != nil || status.Exports.Cancelled != 1 || status.Exports.Unknown != 0 {
		t.Fatal("cancellation was counted as unknown", err)
	}
}

func TestCancelExportLeasedFenceSaveFailureAndCallerCancellation(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	pauseExportDispatch(svc)
	seedActionExport(t, svc, "cancel-running", "pending")
	claim, won, err := svc.claimPendingExport(testCtx, "cancel-running")
	if err != nil || !won {
		t.Fatal(err)
	}
	before := leaseSnapshot(t, svc)
	primary := errors.New("injected control Save failure")
	backend.saveErr = primary
	if summary, err := svc.CancelExport(testCtx, "tenant-a", "operator", claim.ID); !errors.Is(err, primary) || summary.ID != "" {
		t.Fatal("failed Save returned cancellation success")
	}
	backend.saveErr = nil
	ctx, cancel := context.WithCancel(testCtx)
	cancel()
	if _, err := svc.CancelExport(ctx, "tenant-a", "operator", claim.ID); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if got := leaseSnapshot(t, svc); !reflect.DeepEqual(got.Exports[claim.ID], before.Exports[claim.ID]) || got.ExportLeases[claim.ID] != before.ExportLeases[claim.ID] {
		t.Fatal("failed/canceled command retired another executor's lease")
	}
	backend.conflicts = 1
	if _, err := svc.CancelExport(testCtx, "tenant-a", "operator", claim.ID); err != nil {
		t.Fatal(err)
	}
	if err := svc.renewExportLease(testCtx, claim); !errors.Is(err, errExportLeaseLost) {
		t.Fatal("canceled owner renewed")
	}
	svc.finishExport(exportClaimContext(testCtx, claim), claim.ID, "completed", "late-object", "late-digest", 1, "")
	if got := leaseSnapshot(t, svc); got.Exports[claim.ID].Status != domain.ExportStatusCancelled || got.Exports[claim.ID].ObjectPath != "" || len(got.ExportLeases) != 0 {
		t.Fatal("stale executor published through cancellation fence")
	}
}

func TestCancelExportUnknownRequesterLegacyRunningAndTerminalFailClosed(t *testing.T) {
	svc := testService(t, true)
	pauseExportDispatch(svc)
	for _, status := range []string{"running", "completed", "failed", "future-state"} {
		job := seedActionExport(t, svc, "not-cancellable-"+status, status)
		if _, err := svc.CancelExport(testCtx, "tenant-a", "operator", job.ID); !errors.Is(err, domain.ErrConflict) {
			t.Fatal(status, err)
		}
	}
	job := seedActionExport(t, svc, "unknown-requester", "pending")
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		value := data.Exports[job.ID]
		value.RequestedBy = ""
		data.Exports[job.ID] = value
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CancelExport(testCtx, "tenant-a", "platform-admin", job.ID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatal("unknown requester was promoted to owned work")
	}
}
