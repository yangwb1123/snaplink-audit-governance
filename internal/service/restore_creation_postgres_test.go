package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// An actual row-lock wait proves the request reached Save on an independent
// pool. Retained stages come from real authenticated source/plan/page/sealed
// commits. No pre-Load seam or timing guess stands in for the control CAS.
func TestPostgresRestoreCreationRealSaveConflictPreservesCurrentAuthority(t *testing.T) {
	for _, stage := range []string{"selected", "generating", "sealed"} {
		for _, mode := range []string{"cancelled", "renewed"} {
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
				request := domain.RestoreRequest{OperationID: operation, Reason: "case"}
				prior, err := first.CreateRestore(testCtx, "tenant-a", request, "prior-requester")
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
				type outcome struct {
					run domain.RestoreRun
					err error
				}
				done, exited := make(chan outcome, 1), make(chan struct{})
				t.Cleanup(func() {
					cancel()
					_ = tx.Rollback()
					select {
					case <-exited:
					case <-time.After(5 * time.Second):
						t.Error("owned restore creation writer did not exit")
					}
				})
				go func() {
					defer close(exited)
					run, err := second.CreateRestore(ctx, "tenant-a", request, "new-requester")
					done <- outcome{run: run, err: err}
				}()
				awaitAdmissionControlSave(t, ctx, db, holder)
				at := first.Now().Add(ExportHeartbeatInterval)
				clock.Store(at.UnixNano()) // real shared observation time, not a future heartbeat at an old clock
				if mode == "cancelled" {
					job := concurrent.Exports[claim.ID]
					job.Status, job.FailureCode, job.FinishedAt = domain.ExportStatusCancelled, domain.ExportFailureCancelled, &at
					concurrent.Exports[claim.ID] = job
					delete(concurrent.ExportLeases, claim.ID)
					delete(concurrent.ExportPlans, claim.ID)
					delete(concurrent.ExportCheckpoints, claim.ID)
					concurrent.AdminActions = append(concurrent.AdminActions, first.adminAction("tenant-a", "operator", domain.AdminActionExportCancelled, "export", claim.ID, "status=cancelled"))
				} else {
					lease := concurrent.ExportLeases[claim.ID]
					lease.HeartbeatAt, lease.ExpiresAt = at, at.Add(ExportLeaseDuration)
					concurrent.ExportLeases[claim.ID] = lease
					if !ownsExportLease(concurrent, concurrent.Exports[claim.ID], claim, first.Now()) {
						t.Fatal("concurrent renewal did not retain actual current execution authority")
					}
				}
				winner := concurrent.RestoreRuns[prior.ID]
				winner.Status, winner.ApprovedBy, winner.ApprovedAt = domain.RestoreStatusApproved, "independent-decider", &at
				concurrent.RestoreRuns[prior.ID] = winner
				concurrent.AdminActions = append(concurrent.AdminActions, first.adminAction("tenant-a", "independent-decider", domain.AdminActionRestoreApproved, "restore", prior.ID, operation))
				concurrent.LegalHolds["creation-current-hold"] = domain.LegalHold{ID: "creation-current-hold", TenantID: "tenant-a", Name: "case", Reason: "case", CreatedAt: at}
				concurrent.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a", ArchiveDays: 365, RetentionClass: "standard"}
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
				var got outcome
				select {
				case got = <-done:
				case <-ctx.Done():
					t.Fatal("creation did not converge after actual Save conflict")
				}
				if got.err != nil || got.run.ID == "" || !got.run.CreatedAt.Equal(prior.CreatedAt) {
					t.Fatal("valid creation failed or regenerated its original timestamp after actual Save conflict")
				}
				var currentVersion int64
				if db.QueryRowContext(ctx, `SELECT version FROM audit_state_snapshot WHERE id = 1`).Scan(&currentVersion) != nil || currentVersion != version+2 {
					t.Error("creation lost its one atomic commit or performed an extra Save")
				}
				// Restore only the expected materialized evidence in the oracle,
				// never in SQL/control. Optional empty maps match the real loader.
				concurrent.Events, concurrent.Receipts, concurrent.Streams = original.Events, original.Receipts, original.Streams
				concurrent.Segments, concurrent.Checkpoints = original.Segments, original.Checkpoints
				current := restoreCreationSnapshot(t, first)
				if mode == "renewed" && !ownsExportLease(current, current.Exports[claim.ID], claim, first.Now()) {
					t.Error("creation lost actual renewed running authority at the current shared clock")
				}
				for field, preserved := range map[string]bool{
					"export":         reflect.DeepEqual(current.Exports, concurrent.Exports),
					"lease":          reflect.DeepEqual(current.ExportLeases, concurrent.ExportLeases),
					"plan_pages":     reflect.DeepEqual(current.ExportPlans, concurrent.ExportPlans),
					"checkpoint":     reflect.DeepEqual(current.ExportCheckpoints, concurrent.ExportCheckpoints),
					"prior_decision": reflect.DeepEqual(current.RestoreRuns[prior.ID], winner),
					"Hold":           reflect.DeepEqual(current.LegalHolds, concurrent.LegalHolds),
					"policy":         reflect.DeepEqual(current.Policies, concurrent.Policies),
				} {
					if !preserved {
						t.Errorf("actual creation Save conflict overwrote current control field=%s", field)
					}
				}
				gotGets, gotPuts, gotReads := objects.gets.Load()-gets, objects.puts.Load()-puts, len(readFacts(t, first))
				t.Logf("restore_creation_io source_gets=%d expected_source_gets=%d puts=%d read_facts_before=%d read_facts_after=%d", gotGets, len(original.Receipts), gotPuts, reads, gotReads)
				if gotGets != int64(len(original.Receipts)) || gotPuts != 0 || gotReads != reads {
					t.Error("control conflict repeated authenticated replay, wrote source or changed original silent-read policy")
				}
				if mode == "cancelled" {
					second.finishExport(exportClaimContext(testCtx, claim), claim.ID, "completed", "late-object", "late-digest", len(original.Receipts), "")
					late := restoreCreationSnapshot(t, first)
					if !reflect.DeepEqual(late, current) || late.Exports[claim.ID].Status != domain.ExportStatusCancelled || late.Exports[claim.ID].ObjectPath != "" {
						t.Error("creation restored revoked execution authority or allowed late publication")
					}
				}
				t.Logf("restore_creation_boundary actual_Save_conflict=true stage=%s cancelled=%t assertions_passed=%t", stage, mode == "cancelled", !t.Failed())
				assertRestoreCreationDelta(t, concurrent, current, got.run)
			})
		}
	}
}
