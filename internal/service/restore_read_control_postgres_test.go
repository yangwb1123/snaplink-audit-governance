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

func restoreReadControlPostgresFixture(t *testing.T) (*Service, *Service, *checkpointArchive, domain.RestoreRun, *sql.DB) {
	t.Helper()
	first, second, objects, _ := postgresInitialQueryFixture(t)
	run, err := first.CreateRestore(testCtx, "tenant-a", domain.RestoreRequest{OperationID: "initial-operation", Reason: "case"}, "requester")
	if err != nil {
		t.Fatal("could not create original request from authenticated real-file evidence")
	}
	observed := &checkpointArchive{Store: objects}
	first.Config.Archive, second.Config.Archive = observed, observed
	// Finish catalog detection before the test locks evidence relations. This
	// is the same Ready/normal operation prerequisite, not a timing sleep.
	for _, svc := range []*Service{first, second} {
		if err := svc.Store.ReadControl(testCtx, func(*store.Snapshot) error { return nil }); err != nil {
			t.Fatal("could not prepare actual v2 control reader")
		}
	}
	db, err := sql.Open("pgx", os.Getenv("AUDIT_TEST_POSTGRES_EXPORT_LEASE_DSN"))
	if err != nil {
		t.Fatal("could not open owned disposable lock/fault controller")
	}
	t.Cleanup(func() { _ = db.Close() })
	return first, second, observed, run, db
}

func TestPostgresRestoreReadControlAvoidsEvidenceLocksAndObservesCurrentDecision(t *testing.T) {
	first, second, objects, run, db := restoreReadControlPostgresFixture(t)
	original := restoreCreationSnapshot(t, first)
	reads := len(readFacts(t, first))
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
		t.Fatal("metadata oracle did not lock both real evidence relations")
	}
	// A real global snapshot must wait here. A restore metadata observation
	// must not perform those queries or silently fall back to old evidence.
	blockedCtx, blockedCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	_, blockedErr := first.Store.Snapshot(blockedCtx)
	blockedCancel()
	if !errors.Is(blockedErr, store.ErrOperationDeadline) {
		t.Fatal("evidence-lock oracle did not interrupt actual global materialization")
	}
	read := func(svc *Service, want domain.RestoreRun) {
		t.Helper()
		readCtx, readCancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer readCancel()
		got, err := svc.GetRestore(readCtx, "tenant-a", run.ID)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Error("restore metadata read waited on unrelated hot/cold history or returned a stale decision")
		}
	}
	read(second, run)
	approved, err := first.ApproveRestore(ctx, "tenant-a", run.ID, "independent-decider")
	if err != nil {
		t.Fatal("independent original control decision could not commit")
	}
	read(second, approved)
	read(first, approved)
	if err := tx.Rollback(); err != nil {
		t.Fatal("could not release owned evidence locks")
	}
	after := restoreCreationSnapshot(t, first)
	if len(after.AdminActions) != len(original.AdminActions)+1 {
		t.Fatal("metadata reads appended facts or lost the independent decision fact")
	}
	fact := after.AdminActions[len(after.AdminActions)-1]
	if fact.Action != domain.AdminActionRestoreApproved || fact.Actor != "independent-decider" || fact.TargetID != run.ID || fact.TenantID != "tenant-a" {
		t.Error("independent decision lost its original atomic fact")
	}
	expected, err := cloneTestSnapshot(original)
	if err != nil {
		t.Fatal("could not inspect owned expected control delta")
	}
	expected.RestoreRuns[run.ID] = approved
	expected.AdminActions = append(expected.AdminActions, fact)
	var currentVersion int64
	if !reflect.DeepEqual(after, expected) || db.QueryRowContext(ctx, `SELECT version FROM audit_state_snapshot WHERE id = 1`).Scan(&currentVersion) != nil || currentVersion != version+1 {
		t.Error("restore detail saved captured state over current control/evidence or changed durable versions")
	}
	if objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, first)) != reads {
		t.Error("restore detail performed WORM I/O or changed its existing silent-read policy")
	}
	t.Logf("restore_control_read_boundary independent_pools=true evidence_relations_locked=%d global_materialization_interrupted=true decision_observed=%t", locked, !t.Failed())
}

func TestPostgresRestoreReadControlUnrelatedColdFaultAndActualControlLoadFailure(t *testing.T) {
	first, second, objects, run, db := restoreReadControlPostgresFixture(t)
	ctx, cancel := context.WithTimeout(testCtx, 5*time.Second)
	defer cancel()
	result, err := db.ExecContext(ctx, `UPDATE audit_ledger SET record = '{}'::jsonb WHERE tenant_id = $1 AND record_type = 'receipt' AND key = $2`, "tenant-a", store.EventKey("tenant-a", "initial-b"))
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
		if got, err := svc.GetRestore(ctx, "tenant-a", run.ID); err != nil || !reflect.DeepEqual(got, run) {
			t.Error("restore metadata depended on unrelated corrupt cold history")
		}
		if got, err := svc.GetReceipt(ctx, "tenant-a", "", "initial-b"); err == nil || got != (domain.EventReceipt{}) {
			t.Error("metadata-only path weakened the independent exact receipt failure")
		}
	}
	var faultPresent bool
	if db.QueryRowContext(ctx, `SELECT bool_and(record = '{}'::jsonb) FROM audit_ledger WHERE tenant_id = $1 AND record_type = 'receipt' AND key = $2`, "tenant-a", store.EventKey("tenant-a", "initial-b")).Scan(&faultPresent) != nil || !faultPresent || !reflect.DeepEqual(leaseSnapshot(t, second), before) {
		t.Error("metadata reads repaired cold evidence or rewrote control/facts")
	}
	// Corrupt only this disposable control row. Metadata must now fail, not
	// use a cached decision, return partial values or guess rollback/success.
	if _, err := db.ExecContext(ctx, `UPDATE audit_state_snapshot SET snapshot = '[]'::jsonb WHERE id = 1`); err != nil {
		t.Fatal("could not establish actual control Load failure")
	}
	for _, svc := range []*Service{first, second} {
		if got, err := svc.GetRestore(ctx, "tenant-a", run.ID); err == nil || got != (domain.RestoreRun{}) {
			t.Error("actual control Load failure exposed cached or partial restore metadata")
		}
	}
	trail, trailErr := first.Store.ReadAdminTrail(ctx)
	if objects.gets.Load() != 0 || objects.puts.Load() != 0 || trailErr != nil || len(trail) != 0 {
		t.Error("control/fault observation performed WORM I/O or invented read facts")
	}
}
