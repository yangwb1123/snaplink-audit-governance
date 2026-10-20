package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestPostgresSourceCreationControlRealSaveConflictPreservesCurrentAuthority(t *testing.T) {
	for _, stage := range []string{"selected", "generating", "sealed"} {
		for _, mode := range []string{"cancelled", "renewed"} {
			t.Run(stage+"/"+mode, func(t *testing.T) { sourceCreationControlConflictCase(t, stage, mode) })
		}
	}
}

func TestPostgresSourceCreationControlRealSaveConflictRechecksTenantAndUniqueness(t *testing.T) {
	for _, mode := range []string{"same-ID", "tenant-removed", "tenant-removed-and-duplicate"} {
		t.Run(mode, func(t *testing.T) { sourceCreationControlConflictCase(t, "selected", mode) })
	}
}

// Independent pools, actual row-lock/pg_blocking_pids synchronization and real
// authenticated selected/page/sealed stages; no pre-Load fake conflict or sleep.
func sourceCreationControlConflictCase(t *testing.T, stage, mode string) {
	t.Helper()
	var first, second *Service
	var objects *checkpointArchive
	var clock *atomic.Int64
	var claim exportClaim
	operation := "operation"
	switch stage {
	case "selected":
		first, second, objects, clock, claim = postgresSelectedPlan(t)
	case "generating":
		first, second, objects, clock, claim = postgresGeneratedPage(t)
		operation = "generation-operation"
	case "sealed":
		first, second, objects, clock, claim = postgresSealedCheckpoint(t)
	}
	pauseExportDispatch(first)
	pauseExportDispatch(second)
	prior, err := first.CreateRestore(testCtx, "tenant-a", domain.RestoreRequest{OperationID: operation, Reason: "case"}, "requester")
	if err != nil {
		t.Fatal("could not prepare independent original pending request")
	}
	original := restoreCreationSnapshot(t, first)
	gets, puts, reads := objects.gets.Load(), objects.puts.Load(), len(readFacts(t, first))
	db, err := sql.Open("pgx", os.Getenv("AUDIT_TEST_POSTGRES_EXPORT_LEASE_DSN"))
	if err != nil {
		t.Fatal("could not open owned lock controller")
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(testCtx, 5*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal("could not start actual concurrent control writer")
	}
	defer tx.Rollback()
	var raw []byte
	var version int64
	var holder int
	if tx.QueryRowContext(ctx, `SELECT snapshot, version FROM audit_state_snapshot WHERE id = 1 FOR UPDATE`).Scan(&raw, &version) != nil ||
		tx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&holder) != nil {
		t.Fatal("could not establish actual control row lock")
	}
	concurrent := store.NewSnapshot()
	if json.Unmarshal(raw, concurrent) != nil {
		t.Fatal("could not decode owned current control")
	}
	request := sourceCreationControlCandidate()
	want := expectedSourceCreation(request, first.Now()) // original default timestamp, before the concurrent clock advance
	done, exited := make(chan error, 1), make(chan struct{})
	t.Cleanup(func() {
		cancel()
		_ = tx.Rollback()
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			t.Error("owned source creation writer did not exit")
		}
	})
	go func() {
		defer close(exited)
		done <- second.AddSource(ctx, "source-manager", request)
	}()
	awaitAdmissionControlSave(t, ctx, db, holder)
	at := first.Now().Add(ExportHeartbeatInterval)
	clock.Store(at.UnixNano())
	if mode == "renewed" {
		lease := concurrent.ExportLeases[claim.ID]
		lease.HeartbeatAt, lease.ExpiresAt = at, at.Add(ExportLeaseDuration)
		concurrent.ExportLeases[claim.ID] = lease
		if !ownsExportLease(concurrent, concurrent.Exports[claim.ID], claim, first.Now()) {
			t.Fatal("renewal did not retain actual current execution authority")
		}
	} else {
		job := concurrent.Exports[claim.ID]
		job.Status, job.FailureCode, job.FinishedAt = domain.ExportStatusCancelled, domain.ExportFailureCancelled, &at
		concurrent.Exports[claim.ID] = job
		delete(concurrent.ExportLeases, claim.ID)
		delete(concurrent.ExportPlans, claim.ID)
		delete(concurrent.ExportCheckpoints, claim.ID)
		concurrent.AdminActions = append(concurrent.AdminActions, first.adminAction("tenant-a", "operator", domain.AdminActionExportCancelled, "export", claim.ID, "status=cancelled"))
	}
	decision := concurrent.RestoreRuns[prior.ID]
	decision.Status, decision.ApprovedBy, decision.ApprovedAt = domain.RestoreStatusApproved, "independent-decider", &at
	concurrent.RestoreRuns[prior.ID] = decision
	concurrent.AdminActions = append(concurrent.AdminActions, first.adminAction("tenant-a", "independent-decider", domain.AdminActionRestoreApproved, "restore", prior.ID, operation))
	concurrent.LegalHolds["registration-current-hold"] = domain.LegalHold{ID: "registration-current-hold", TenantID: "tenant-a", Name: "case", Reason: "case", CreatedAt: at}
	concurrent.Policies["tenant-b"] = domain.RetentionPolicy{TenantID: "tenant-b", ArchiveDays: 730, RetentionClass: "neighbor"}
	neighbor := domain.SourceSystem{TenantID: "tenant-b", ID: "neighbor-current", Name: "Neighbor", Active: false, CreatedAt: at}
	concurrent.Sources[store.SourceKey(neighbor.TenantID, neighbor.ID)] = neighbor
	key := store.SourceKey(request.TenantID, request.ID)
	var wantErr error
	if mode == "same-ID" || mode == "tenant-removed-and-duplicate" {
		winner := domain.SourceSystem{TenantID: request.TenantID, ID: request.ID, Name: "First registration",
			AllowedClientIDs: []string{"winner-client"}, Active: false, CreatedAt: at}
		concurrent.Sources[key] = winner
		concurrent.AdminActions = append(concurrent.AdminActions, first.adminAction("tenant-a", "first-creator", domain.AdminActionSourceCreated, "source", winner.ID, fmt.Sprintf("allowed_client_ids=%v", winner.AllowedClientIDs)))
		wantErr = domain.ErrConflict
	}
	if mode == "tenant-removed" || mode == "tenant-removed-and-duplicate" {
		delete(concurrent.Tenants, request.TenantID) // owned fault, never a tenant-deletion API
		wantErr = domain.ErrNotFound                 // preserve missing-tenant before duplicate precedence
	}
	encoded, err := json.Marshal(concurrent)
	if err != nil {
		t.Fatal("could not encode actual concurrent authority")
	}
	result, err := tx.ExecContext(ctx, `UPDATE audit_state_snapshot SET snapshot = $1::jsonb, version = version + 1 WHERE id = 1 AND version = $2`, string(encoded), version)
	if err != nil {
		t.Fatal("could not persist owned concurrent authority")
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 || tx.Commit() != nil {
		t.Fatal("concurrent authority did not really commit")
	}
	var gotErr error
	select {
	case gotErr = <-done:
	case <-ctx.Done():
		t.Fatal("registration did not converge after actual Save conflict")
	}
	wantVersion := version + 2
	if wantErr != nil {
		wantVersion--
		if !errors.Is(gotErr, wantErr) {
			t.Error("source creation retry failed to recheck current tenant or source uniqueness")
		}
	} else if gotErr != nil {
		t.Error("valid registration did not commit after actual Save conflict")
	}
	var currentVersion int64
	if db.QueryRowContext(ctx, `SELECT version FROM audit_state_snapshot WHERE id = 1`).Scan(&currentVersion) != nil || currentVersion != wantVersion {
		t.Error("registration lost atomic commit or saved a rejected retry")
	}
	// Materialize evidence only in the expected private copy, never in SQL.
	concurrent.Events, concurrent.Receipts, concurrent.Streams = original.Events, original.Receipts, original.Streams
	concurrent.Segments, concurrent.Checkpoints = original.Segments, original.Checkpoints
	current := restoreCreationSnapshot(t, first)
	for field, preserved := range map[string]bool{
		"export":          reflect.DeepEqual(current.Exports, concurrent.Exports),
		"lease":           reflect.DeepEqual(current.ExportLeases, concurrent.ExportLeases),
		"plan_pages":      reflect.DeepEqual(current.ExportPlans, concurrent.ExportPlans),
		"checkpoint":      reflect.DeepEqual(current.ExportCheckpoints, concurrent.ExportCheckpoints),
		"decision":        reflect.DeepEqual(current.RestoreRuns, concurrent.RestoreRuns),
		"Hold":            reflect.DeepEqual(current.LegalHolds, concurrent.LegalHolds),
		"tenant":          reflect.DeepEqual(current.Tenants, concurrent.Tenants),
		"neighbor_policy": reflect.DeepEqual(current.Policies, concurrent.Policies),
	} {
		if !preserved {
			t.Errorf("actual registration Save conflict overwrote current field=%s", field)
		}
	}
	if mode == "renewed" {
		if !ownsExportLease(current, current.Exports[claim.ID], claim, first.Now()) {
			t.Error("registration lost healthy renewed authority at the actual shared clock")
		}
	} else {
		second.finishExport(exportClaimContext(testCtx, claim), claim.ID, "completed", "late-object", "late-digest", len(original.Receipts), "")
		if !reflect.DeepEqual(restoreCreationSnapshot(t, first), current) || current.Exports[claim.ID].Status != domain.ExportStatusCancelled || current.Exports[claim.ID].ObjectPath != "" {
			t.Error("registration restored revoked authority or permitted late publication")
		}
	}
	if objects.gets.Load() != gets || objects.puts.Load() != puts || len(readFacts(t, first)) != reads {
		t.Error("registration performed WORM I/O or changed independent read facts")
	}
	if wantErr != nil {
		if !reflect.DeepEqual(current, concurrent) {
			t.Error("refused registration replaced the first source, current control, facts or original evidence")
		}
		if mode == "same-ID" {
			if resolved, err := first.resolveIngestTenant(testCtx, "tenant-a", "relay-a", request.ID); !errors.Is(err, domain.ErrForbidden) || resolved != "" {
				t.Error("losing registration replaced or activated the first source authorization")
			}
		}
	} else {
		assertSourceCreationControlDelta(t, concurrent, current, want)
	}
	t.Logf("source_creation_control_boundary actual_Save_conflict=true stage=%s mode=%s assertions_passed=%t", stage, mode, !t.Failed())
}
