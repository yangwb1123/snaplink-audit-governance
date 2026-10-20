package service

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestPostgresRetentionReadControlAvoidsEvidenceLocksAndObservesCurrentPolicy(t *testing.T) {
	first, second, objects, clock, claim := postgresSelectedPlan(t)
	pauseExportDispatch(first)
	pauseExportDispatch(second)
	policy := retentionControlPolicy()
	if err := first.SetRetentionPolicy(testCtx, "policy-manager", policy); err != nil {
		t.Fatal("could not prepare original policy configuration")
	}
	original := restoreCreationSnapshot(t, first)
	gets, puts, reads := objects.gets.Load(), objects.puts.Load(), len(readFacts(t, first))
	db, err := sql.Open("pgx", os.Getenv("AUDIT_TEST_POSTGRES_EXPORT_LEASE_DSN"))
	if err != nil {
		t.Fatal("could not open owned disposable lock controller")
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(testCtx, 5*time.Second)
	defer cancel()
	var version int64
	if db.QueryRowContext(ctx, `SELECT version FROM audit_state_snapshot WHERE id = 1`).Scan(&version) != nil {
		t.Fatal("could not capture actual control version")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal("could not start owned evidence lock transaction")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `LOCK TABLE audit_tenant, audit_ledger IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal("could not establish actual evidence-only locks")
	}
	var locked int
	if tx.QueryRowContext(ctx, `SELECT count(*) FROM pg_locks WHERE pid = pg_backend_pid() AND granted AND mode = 'AccessExclusiveLock' AND relation IN ('audit_tenant'::regclass, 'audit_ledger'::regclass)`).Scan(&locked) != nil || locked != 2 {
		t.Fatal("policy oracle did not lock both real evidence relations")
	}
	// Confirm real global materialization waits, instead of assuming that a
	// transaction or sleep alone establishes the query dependency under test.
	blockedCtx, blockedCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	_, blockedErr := first.Store.Snapshot(blockedCtx)
	blockedCancel()
	if !errors.Is(blockedErr, store.ErrOperationDeadline) {
		t.Fatal("evidence-lock oracle did not interrupt actual global materialization")
	}
	read := func(svc *Service, want domain.RetentionPolicy) {
		t.Helper()
		readCtx, readCancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer readCancel()
		if got, err := svc.GetRetentionPolicy(readCtx, "tenant-a"); err != nil || got != want {
			t.Error("policy observation waited on unrelated hot/cold history or returned stale configuration")
		}
	}
	read(second, policy)
	clock.Store(first.Now().Add(ExportHeartbeatInterval).UnixNano())
	if err := first.renewExportLease(ctx, claim); err != nil {
		t.Fatal("policy observation blocked actual independent lease renewal")
	}
	if current := leaseSnapshot(t, second); !ownsExportLease(current, current.Exports[claim.ID], claim, first.Now()) {
		t.Fatal("current shared clock did not observe renewed execution authority")
	}
	updated := policy
	updated.HotDays = 14
	if err := first.SetRetentionPolicy(ctx, "policy-manager", updated); err != nil {
		t.Fatal("independent original policy update could not commit")
	}
	read(second, updated)
	cancelled, err := first.CancelExport(ctx, "tenant-a", "operator", claim.ID)
	if err != nil || cancelled.Status != domain.ExportStatusCancelled {
		t.Fatal("policy observation blocked original owner cancellation")
	}
	read(second, updated)
	read(first, updated)
	if err := tx.Rollback(); err != nil {
		t.Fatal("could not release owned evidence locks")
	}
	after := restoreCreationSnapshot(t, first)
	if len(after.AdminActions) != len(original.AdminActions)+2 {
		t.Fatal("policy reads appended facts or lost original update/cancellation facts")
	}
	facts := after.AdminActions[len(original.AdminActions):]
	if facts[0].Action != domain.AdminActionRetentionPolicySet || facts[0].Actor != "policy-manager" || facts[0].TenantID != "tenant-a" || facts[0].TargetID != policy.RetentionClass || facts[0].Detail != "hot=14 warm=30 archive=365" ||
		facts[1].Action != domain.AdminActionExportCancelled || facts[1].Actor != "operator" || facts[1].TenantID != "tenant-a" || facts[1].TargetID != claim.ID {
		t.Error("independent configuration/cancellation lost its original atomic facts")
	}
	expected, err := cloneTestSnapshot(original)
	if err != nil {
		t.Fatal("could not inspect expected independent control delta")
	}
	expected.Policies["tenant-a"] = updated
	job := expected.Exports[claim.ID]
	at := first.Now()
	job.Status, job.FailureCode, job.FinishedAt = domain.ExportStatusCancelled, domain.ExportFailureCancelled, &at
	expected.Exports[claim.ID] = job
	delete(expected.ExportLeases, claim.ID)
	delete(expected.ExportPlans, claim.ID)
	expected.AdminActions = append(expected.AdminActions, facts...)
	var currentVersion int64
	if !reflect.DeepEqual(after, expected) || db.QueryRowContext(ctx, `SELECT version FROM audit_state_snapshot WHERE id = 1`).Scan(&currentVersion) != nil || currentVersion != version+3 {
		t.Error("policy observation changed durable versions or overwrote current control/evidence")
	}
	second.finishExport(exportClaimContext(testCtx, claim), claim.ID, "completed", "late-object", "late-digest", 1, "")
	if !reflect.DeepEqual(restoreCreationSnapshot(t, first), after) || objects.gets.Load() != gets || objects.puts.Load() != puts || len(readFacts(t, first)) != reads {
		t.Error("policy observation restored revoked authority, performed WORM I/O or changed silent-read facts")
	}
	t.Logf("retention_control_read_boundary independent_pools=true evidence_relations_locked=%d global_materialization_interrupted=true current_policy_observed=%t", locked, !t.Failed())
}

func TestPostgresRetentionReadControlUnrelatedColdFaultAndActualControlLoadFailure(t *testing.T) {
	first, second, objects, _, _ := postgresSelectedPlan(t)
	policy := retentionControlPolicy()
	if err := first.SetRetentionPolicy(testCtx, "policy-manager", policy); err != nil {
		t.Fatal("could not prepare original policy configuration")
	}
	gets, puts := objects.gets.Load(), objects.puts.Load()
	reads, err := first.Store.ReadAdminTrail(testCtx)
	if err != nil {
		t.Fatal("could not capture original independent read trail")
	}
	db, err := sql.Open("pgx", os.Getenv("AUDIT_TEST_POSTGRES_EXPORT_LEASE_DSN"))
	if err != nil {
		t.Fatal("could not open owned disposable fault controller")
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(testCtx, 5*time.Second)
	defer cancel()
	result, err := db.ExecContext(ctx, `UPDATE audit_ledger SET record = '{}'::jsonb WHERE tenant_id = $1 AND record_type = 'receipt' AND key = $2`, "tenant-a", store.EventKey("tenant-a", "plan-source"))
	if err != nil {
		t.Fatal("could not persist owned unrelated cold fault")
	}
	if n, err := result.RowsAffected(); err != nil || n < 1 {
		t.Fatal("cold fault oracle did not change actual evidence records")
	}
	if _, err := first.Store.Snapshot(ctx); err == nil {
		t.Fatal("cold fault oracle did not establish a materialization failure")
	}
	before := leaseSnapshot(t, first)
	for _, svc := range []*Service{first, second} {
		if got, err := svc.GetRetentionPolicy(ctx, "tenant-a"); err != nil || got != policy {
			t.Error("policy configuration observation depended on unrelated corrupt cold history")
		}
		if got, err := svc.GetReceipt(ctx, "tenant-a", "", "plan-source"); err == nil || got != (domain.EventReceipt{}) {
			t.Error("metadata-only observation weakened independent exact receipt failure")
		}
	}
	var faultPresent bool
	if db.QueryRowContext(ctx, `SELECT bool_and(record = '{}'::jsonb) FROM audit_ledger WHERE tenant_id = $1 AND record_type = 'receipt' AND key = $2`, "tenant-a", store.EventKey("tenant-a", "plan-source")).Scan(&faultPresent) != nil || !faultPresent || !reflect.DeepEqual(leaseSnapshot(t, second), before) {
		t.Error("policy observations repaired cold evidence or rewrote control/facts")
	}
	// Corrupt only this disposable control row. A current policy read must
	// now fail, not return cached metadata or infer an unknown write result.
	if _, err := db.ExecContext(ctx, `UPDATE audit_state_snapshot SET snapshot = '[]'::jsonb WHERE id = 1`); err != nil {
		t.Fatal("could not establish actual control Load failure")
	}
	for _, svc := range []*Service{first, second} {
		if got, err := svc.GetRetentionPolicy(ctx, "tenant-a"); err == nil || got != (domain.RetentionPolicy{}) {
			t.Error("actual control Load failure exposed cached or partial policy metadata")
		}
	}
	trail, trailErr := first.Store.ReadAdminTrail(ctx)
	if objects.gets.Load() != gets || objects.puts.Load() != puts || trailErr != nil || !reflect.DeepEqual(trail, reads) {
		t.Error("control/fault observation performed WORM I/O or changed original read facts")
	}
}
