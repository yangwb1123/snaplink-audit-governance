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

func seedGovernanceStatus(t *testing.T, svc *Service) time.Time {
	t.Helper()
	at := seedWorkLists(t, svc)
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		data.Exports["pending"] = domain.ExportJob{ID: "pending", TenantID: "tenant-a", Status: "pending", CreatedAt: at.Add(-time.Hour)}
		data.Exports["pending-zero"] = domain.ExportJob{ID: "pending-zero", TenantID: "tenant-a", Status: "pending"}
		data.Exports["running"] = domain.ExportJob{ID: "running", TenantID: "tenant-a", Status: "running", CreatedAt: at}
		data.Exports["failed"] = domain.ExportJob{ID: "failed", TenantID: "tenant-a", Status: "failed"}
		data.Exports["unknown"] = domain.ExportJob{ID: "unknown", TenantID: "tenant-a", Status: "future-status"}
		data.RestoreRuns["rejected"] = domain.RestoreRun{ID: "rejected", TenantID: "tenant-a", Status: domain.RestoreStatusRejected, Reason: "private reason"}
		data.RestoreRuns["unknown"] = domain.RestoreRun{ID: "unknown", TenantID: "tenant-a", Status: "future-status"}
		data.LegalHolds["active"] = domain.LegalHold{ID: "active", TenantID: "tenant-a", Name: "private name", Reason: "private reason", Filter: domain.Query{PayloadDigest: "private-digest"}}
		data.LegalHolds["released"] = domain.LegalHold{ID: "released", TenantID: "tenant-a", ReleasedAt: &at}
		data.LegalHolds["foreign"] = domain.LegalHold{ID: "foreign", TenantID: "tenant-b"}
		data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a", RetentionClass: "standard", HotDays: 7, WarmDays: 30, ArchiveDays: 365}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return at
}

func TestGovernanceStatusCountsScopeAndNoEvidence(t *testing.T) {
	svc := testService(t, true)
	at := seedGovernanceStatus(t, svc)
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("summary-evidence", "private-operation", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	archive := &queryRecordingArchive{} // A Get would panic: summaries must not fetch evidence.
	svc.Config.Archive = archive
	status, err := svc.GovernanceStatus(testCtx, "tenant-a", "alice", GovernanceStatusAccess{Exports: true, Restores: true, RestoreDecisions: true, LegalHolds: true, Retention: true})
	if err != nil {
		t.Fatal(err)
	}
	if status.TenantID != "tenant-a" || status.Source != "control" || status.Completeness != "not_asserted" || !status.ObservedAt.Equal(at) || archive.reads != 0 {
		t.Fatalf("status=%+v archive reads=%d", status, archive.reads)
	}
	wantExports := &domain.GovernanceExportStatus{Total: 8, Pending: 2, Running: 1, Completed: 3, Failed: 1, Unknown: 1, TenantQueueLimit: MaxPendingExportsPerTenant, TenantRunningLimit: ExportTenantConcurrency}
	oldest := at.Add(-time.Hour)
	wantExports.OldestPendingAt = &oldest
	if !reflect.DeepEqual(status.Exports, wantExports) {
		t.Fatalf("exports=%+v want=%+v", status.Exports, wantExports)
	}
	actionable := 1
	wantRestores := &domain.GovernanceRestoreStatus{Total: 7, PendingApproval: 4, MinePendingApproval: 2, Approved: 1, Rejected: 1, Unknown: 1, Actionable: &actionable, Execution: "not_tracked"}
	if !reflect.DeepEqual(status.Restores, wantRestores) || !reflect.DeepEqual(status.LegalHolds, &domain.GovernanceHoldStatus{Total: 2, Active: 1, Released: 1}) {
		t.Fatalf("restores=%+v holds=%+v", status.Restores, status.LegalHolds)
	}
	if status.Retention == nil || !status.Retention.Configured || status.Retention.Policy.ArchiveDays != 365 {
		t.Fatalf("retention=%+v", status.Retention)
	}
	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"private", "payload", "query", "digest", "object_path", "event_count", "reason", "requested_by", "created_by", "foreign", "future-status"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("summary leaked %s: %s", forbidden, encoded)
		}
	}
	status.Retention.Policy.ArchiveDays = 1
	*status.Exports.OldestPendingAt = at
	next, err := svc.GovernanceStatus(testCtx, "tenant-a", "alice", GovernanceStatusAccess{Exports: true, Retention: true})
	if err != nil || next.Retention.Policy.ArchiveDays != 365 || !next.Exports.OldestPendingAt.Equal(oldest) {
		t.Fatalf("result aliased control metadata: %+v err=%v", next, err)
	}
	actions, err := svc.ListAdminActions(testCtx, "tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	facts := 0
	for _, action := range actions {
		if action.Action == domain.AdminActionGovernanceStatusRead {
			facts++
			if action.Actor != "alice" || action.TargetType != "governance" || action.TargetID != "" || strings.Contains(action.Detail, "private") {
				t.Fatalf("unsafe read fact: %+v", action)
			}
		}
	}
	if facts != 2 {
		t.Fatalf("summary facts=%d want 2", facts)
	}
}

func TestGovernanceStatusSectionFilteringAndEmptyValues(t *testing.T) {
	svc := testService(t, false)
	seedGovernanceStatus(t, svc)
	status, err := svc.GovernanceStatus(testCtx, "tenant-a", "alice", GovernanceStatusAccess{Restores: true})
	if err != nil || status.Exports != nil || status.LegalHolds != nil || status.Retention != nil || status.Restores == nil || status.Restores.Actionable != nil {
		t.Fatalf("unauthorized sections: %+v err=%v", status, err)
	}
	if err := svc.CreateTenant(testCtx, "test", domain.Tenant{ID: "tenant-c", Name: "Empty", Active: true}); err != nil {
		t.Fatal(err)
	}
	status, err = svc.GovernanceStatus(testCtx, "tenant-c", "alice", GovernanceStatusAccess{Exports: true, Restores: true, RestoreDecisions: true, LegalHolds: true, Retention: true})
	if err != nil || status.Exports.Total != 0 || status.Exports.OldestPendingAt != nil || status.Restores.Total != 0 || *status.Restores.Actionable != 0 || status.LegalHolds.Total != 0 || status.Retention.Configured || status.Retention.Policy != nil {
		t.Fatalf("empty tenant summary=%+v err=%v", status, err)
	}
	for _, tc := range []struct {
		tenant, actor string
		access        GovernanceStatusAccess
		want          error
	}{
		{"", "alice", GovernanceStatusAccess{Exports: true}, domain.ErrInvalid},
		{"tenant-a", "", GovernanceStatusAccess{Exports: true}, domain.ErrInvalid},
		{"tenant-a", "alice", GovernanceStatusAccess{}, domain.ErrForbidden},
		{"tenant-a", "alice", GovernanceStatusAccess{RestoreDecisions: true}, domain.ErrForbidden},
		{"missing", "alice", GovernanceStatusAccess{Exports: true}, domain.ErrNotFound},
	} {
		got, err := svc.GovernanceStatus(testCtx, tc.tenant, tc.actor, tc.access)
		if !errors.Is(err, tc.want) || !reflect.DeepEqual(got, domain.GovernanceStatus{}) {
			t.Fatalf("case=%+v got=%+v err=%v", tc, got, err)
		}
	}
}

func TestGovernanceStatusSelfAuditFailureServesNoResult(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	seedGovernanceStatus(t, svc)
	appendErr := errors.New("private append backend failed")
	backend.saveErr = appendErr
	status, err := svc.GovernanceStatus(testCtx, "tenant-a", "alice", GovernanceStatusAccess{Exports: true})
	if !errors.Is(err, appendErr) || !reflect.DeepEqual(status, domain.GovernanceStatus{}) {
		t.Fatalf("status leaked despite self-audit error: %+v err=%v", status, err)
	}
	backend.saveErr = nil
	backend.alwaysConflict = true
	status, err = svc.GovernanceStatus(testCtx, "tenant-a", "alice", GovernanceStatusAccess{Exports: true})
	if !errors.Is(err, store.ErrSnapshotConflict) || !reflect.DeepEqual(status, domain.GovernanceStatus{}) {
		t.Fatalf("status=%+v err=%v", status, err)
	}
}

func TestGovernanceStatusRejectsMisboundPolicyWithoutLeakingSections(t *testing.T) {
	svc := testService(t, false)
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-b", RetentionClass: "private-foreign-class"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err := svc.GovernanceStatus(testCtx, "tenant-a", "alice", GovernanceStatusAccess{Retention: true})
	if !errors.Is(err, domain.ErrConflict) || !reflect.DeepEqual(got, domain.GovernanceStatus{}) {
		t.Fatalf("foreign policy served: %+v err=%v", got, err)
	}
	// An unauthorized section must neither be served nor affect another section.
	got, err = svc.GovernanceStatus(testCtx, "tenant-a", "alice", GovernanceStatusAccess{Exports: true})
	if err != nil || got.Retention != nil || got.Exports == nil || got.Exports.Total != 0 {
		t.Fatalf("unauthorized policy affected export status: %+v err=%v", got, err)
	}
}

type governanceDeadlineBackend struct {
	*scriptedConflictBackend
	readDeadline, auditDeadline time.Time
	cancelAtAppend              context.CancelFunc
}

func (b *governanceDeadlineBackend) Load(ctx context.Context) (*store.Snapshot, error) {
	b.readDeadline, _ = ctx.Deadline()
	return b.scriptedConflictBackend.Load(ctx)
}

func (b *governanceDeadlineBackend) AppendAdminFact(ctx context.Context, _ domain.AdminAction, _ int) error {
	b.auditDeadline, _ = ctx.Deadline()
	if b.cancelAtAppend != nil {
		b.cancelAtAppend()
	}
	return ctx.Err()
}

func (*governanceDeadlineBackend) ReadAdminTrail(context.Context) ([]domain.AdminAction, error) {
	return nil, nil
}

func TestGovernanceStatusSharedDeadlineAndBoundaryCancellation(t *testing.T) {
	backend := &governanceDeadlineBackend{scriptedConflictBackend: &scriptedConflictBackend{data: store.NewSnapshot()}}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	seedTestDomain(t, svc)
	if _, err := svc.GovernanceStatus(testCtx, "tenant-a", "alice", GovernanceStatusAccess{Exports: true}); err != nil {
		t.Fatal(err)
	}
	if backend.readDeadline.IsZero() || !backend.readDeadline.Equal(backend.auditDeadline) {
		t.Fatalf("operation deadline restarted: read=%s audit=%s", backend.readDeadline, backend.auditDeadline)
	}
	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()
	backend.cancelAtAppend = cancel // Cancel only after the actual control read, at the append boundary.
	got, err := svc.GovernanceStatus(ctx, "tenant-a", "alice", GovernanceStatusAccess{Exports: true})
	if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, domain.GovernanceStatus{}) || !backend.readDeadline.Equal(backend.auditDeadline) {
		t.Fatalf("cancelled read served status=%+v err=%v", got, err)
	}
}
