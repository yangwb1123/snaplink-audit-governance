package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Real control-row locks and pg_blocking_pids synchronize the actual Save on
// an independent pool. Original stages come from authenticated commits, not
// synthetic references, and every owned goroutine is joined even on failure.
func TestPostgresRetentionControlRealSaveConflictPreservesCurrentAuthority(t *testing.T) {
	for _, stage := range []string{"selected", "generating", "sealed"} {
		modes := []string{"cancelled", "renewed"}
		if stage == "selected" {
			modes = append(modes, "tenant-removed")
		}
		for _, mode := range modes {
			t.Run(stage+"/"+mode, func(t *testing.T) {
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
					t.Fatal("could not prepare original independent pending request")
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
				done, exited := make(chan error, 1), make(chan struct{})
				t.Cleanup(func() {
					cancel()
					_ = tx.Rollback()
					select {
					case <-exited:
					case <-time.After(5 * time.Second):
						t.Error("owned policy writer did not exit")
					}
				})
				policy := retentionControlPolicy()
				go func() {
					defer close(exited)
					done <- second.SetRetentionPolicy(ctx, "policy-manager", policy)
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
				winner := concurrent.RestoreRuns[prior.ID]
				winner.Status, winner.ApprovedBy, winner.ApprovedAt = domain.RestoreStatusApproved, "independent-decider", &at
				concurrent.RestoreRuns[prior.ID] = winner
				concurrent.AdminActions = append(concurrent.AdminActions, first.adminAction("tenant-a", "independent-decider", domain.AdminActionRestoreApproved, "restore", prior.ID, operation))
				concurrent.LegalHolds["retention-current-hold"] = domain.LegalHold{ID: "retention-current-hold", TenantID: "tenant-a", Name: "case", Reason: "case", CreatedAt: at}
				concurrent.Policies["tenant-b"] = domain.RetentionPolicy{TenantID: "tenant-b", ArchiveDays: 730, RetentionClass: "neighbor"}
				if mode == "tenant-removed" {
					delete(concurrent.Tenants, "tenant-a") // owned fault: no new removal API
				}
				encoded, err := json.Marshal(concurrent)
				if err != nil {
					t.Fatal("could not encode actual concurrent control")
				}
				result, err := tx.ExecContext(ctx, `UPDATE audit_state_snapshot SET snapshot = $1::jsonb, version = version + 1 WHERE id = 1 AND version = $2`, string(encoded), version)
				if err != nil {
					t.Fatal("could not commit owned concurrent authority")
				}
				if n, err := result.RowsAffected(); err != nil || n != 1 || tx.Commit() != nil {
					t.Fatal("concurrent authority did not really commit")
				}
				var gotErr error
				select {
				case gotErr = <-done:
				case <-ctx.Done():
					t.Fatal("policy did not converge after actual Save conflict")
				}
				wantVersion := version + 2
				if mode == "tenant-removed" {
					wantVersion--
					if !errors.Is(gotErr, domain.ErrNotFound) {
						t.Error("policy retry failed to recheck the current tenant")
					}
				} else if gotErr != nil {
					t.Error("valid policy did not commit after actual Save conflict")
				}
				var currentVersion int64
				if db.QueryRowContext(ctx, `SELECT version FROM audit_state_snapshot WHERE id = 1`).Scan(&currentVersion) != nil || currentVersion != wantVersion {
					t.Error("policy lost its atomic commit or saved a rejected retry")
				}
				// Materialize original evidence only in the expected private copy;
				// SQL/control is never rewritten to make an oracle pass.
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
					"neighbor_policy": current.Policies["tenant-b"] == concurrent.Policies["tenant-b"],
				} {
					if !preserved {
						t.Errorf("actual policy Save conflict overwrote current field=%s", field)
					}
				}
				if mode == "renewed" && !ownsExportLease(current, current.Exports[claim.ID], claim, first.Now()) {
					t.Error("policy lost actual renewed running authority at current shared clock")
				}
				if mode != "renewed" {
					second.finishExport(exportClaimContext(testCtx, claim), claim.ID, "completed", "late-object", "late-digest", len(original.Receipts), "")
					if !reflect.DeepEqual(restoreCreationSnapshot(t, first), current) || current.Exports[claim.ID].Status != domain.ExportStatusCancelled || current.Exports[claim.ID].ObjectPath != "" {
						t.Error("policy restored revoked authority or permitted late publication")
					}
				}
				if objects.gets.Load() != gets || objects.puts.Load() != puts || len(readFacts(t, first)) != reads {
					t.Error("policy configuration performed source I/O or changed original read facts")
				}
				t.Logf("retention_control_boundary actual_Save_conflict=true stage=%s cancelled=%t assertions_passed=%t", stage, mode != "renewed", !t.Failed())
				if mode == "tenant-removed" {
					if !reflect.DeepEqual(current, concurrent) {
						t.Error("refused policy retry mutated original control or evidence")
					}
				} else {
					assertRetentionControlDelta(t, concurrent, current, policy)
				}
			})
		}
	}
}
