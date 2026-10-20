package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Independent pools plus a real control-row lock ensure the losing writer has
// reached its actual Save, not merely a hook before Load. Both retained phases
// are real authenticated artifacts; concurrent cancellation must retire them,
// not be undone by an unrelated decision retry.
func TestPostgresRestoreDecisionRealSaveConflictRechecksCurrentDecision(t *testing.T) {
	for _, target := range []string{domain.RestoreStatusApproved, domain.RestoreStatusRejected} {
		for _, mode := range []string{"preserve", "same-decision", "opposite-decision", "missing", "tenant-change", "requester-change"} {
			t.Run(target+"/"+mode, func(t *testing.T) {
				var first, second *Service
				var objects *checkpointArchive
				var claim exportClaim
				if target == domain.RestoreStatusApproved {
					first, second, objects, _, claim = postgresSelectedPlan(t)
				} else {
					first, second, objects, _, claim = postgresSealedCheckpoint(t)
				}
				pauseExportDispatch(first)
				pauseExportDispatch(second)
				run, err := first.CreateRestore(testCtx, "tenant-a", domain.RestoreRequest{OperationID: "operation", Reason: "case"}, "requester")
				if err != nil {
					t.Fatal("could not prepare original request from real committed evidence")
				}
				original := leaseSnapshot(t, first)
				gets, puts := objects.gets.Load(), objects.puts.Load()
				db, err := sql.Open("pgx", os.Getenv("AUDIT_TEST_POSTGRES_EXPORT_LEASE_DSN"))
				if err != nil {
					t.Fatal("could not open owned lock controller")
				}
				defer db.Close()
				ctx, cancel := context.WithTimeout(testCtx, 5*time.Second)
				defer cancel()
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal("could not begin owned concurrent control writer")
				}
				defer tx.Rollback()
				var raw []byte
				var version int64
				var holder int
				if err := tx.QueryRowContext(ctx, `SELECT snapshot, version FROM audit_state_snapshot WHERE id = 1 FOR UPDATE`).Scan(&raw, &version); err != nil {
					t.Fatal("could not lock actual control row")
				}
				if err := tx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&holder); err != nil {
					t.Fatal("could not identify actual row-lock holder")
				}
				// Match the real control loader's normalized empty maps. Omitted
				// optional stage maps are empty, not evidence of a lost artifact.
				concurrent := store.NewSnapshot()
				if json.Unmarshal(raw, concurrent) != nil {
					t.Fatal("could not capture actual concurrent control baseline")
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
						t.Error("owned restore decision writer did not exit")
					}
				})
				go func() {
					defer close(exited)
					value, err := callRestoreDecision(ctx, second, "tenant-a", run.ID, "later-decider", target)
					done <- outcome{run: value, err: err}
				}()
				awaitAdmissionControlSave(t, ctx, db, holder)
				at := first.Now()
				job := concurrent.Exports[claim.ID]
				job.Status, job.FailureCode, job.FinishedAt = domain.ExportStatusCancelled, domain.ExportFailureCancelled, &at
				concurrent.Exports[job.ID] = job
				delete(concurrent.ExportLeases, job.ID)
				delete(concurrent.ExportPlans, job.ID)
				delete(concurrent.ExportCheckpoints, job.ID)
				concurrent.AdminActions = append(concurrent.AdminActions, first.adminAction("tenant-a", "operator", domain.AdminActionExportCancelled, "export", job.ID, "status=cancelled"))
				concurrent.LegalHolds["decision-concurrent-hold"] = domain.LegalHold{ID: "decision-concurrent-hold", TenantID: "tenant-a", Name: "case", Reason: "case", CreatedAt: at}
				concurrent.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a", ArchiveDays: 365, RetentionClass: "standard"}
				winner := concurrent.RestoreRuns[run.ID]
				decided := mode == "same-decision" || mode == "opposite-decision"
				if decided {
					winner.Status = target
					if mode == "opposite-decision" {
						winner.Status = domain.RestoreStatusApproved
						if target == domain.RestoreStatusApproved {
							winner.Status = domain.RestoreStatusRejected
						}
					}
					action := domain.AdminActionRestoreApproved
					if winner.Status == domain.RestoreStatusApproved {
						winner.ApprovedBy, winner.ApprovedAt = "first-decider", &at
					} else {
						winner.RejectedBy, winner.RejectedAt = "first-decider", &at
						action = domain.AdminActionRestoreRejected
					}
					concurrent.RestoreRuns[run.ID] = winner
					concurrent.AdminActions = append(concurrent.AdminActions, first.adminAction("tenant-a", "first-decider", action, "restore", run.ID, run.OperationID))
				} else {
					switch mode {
					case "missing":
						delete(concurrent.RestoreRuns, run.ID) // owned fault: never recreate a vanished target
					case "tenant-change":
						winner.TenantID = "tenant-b" // owned corruption, not a supported transfer API
						concurrent.RestoreRuns[run.ID] = winner
					case "requester-change":
						winner.CreatedBy = "later-decider" // force duty recheck on the actual retry
						concurrent.RestoreRuns[run.ID] = winner
					}
				}
				encoded, err := json.Marshal(concurrent)
				if err != nil {
					t.Fatal("could not encode owned concurrent writer")
				}
				result, err := tx.ExecContext(ctx, `UPDATE audit_state_snapshot SET snapshot = $1::jsonb, version = version + 1 WHERE id = 1 AND version = $2`, string(encoded), version)
				if err != nil {
					t.Fatal("could not write actual concurrent control decision")
				}
				if n, err := result.RowsAffected(); err != nil || n != 1 || tx.Commit() != nil {
					t.Fatal("concurrent control decision did not really commit")
				}
				var got outcome
				select {
				case got = <-done:
				case <-ctx.Done():
					t.Fatal("restore writer did not converge after actual Save conflict")
				}
				wantSaves := int64(1)
				var wantErr error
				switch mode {
				case "preserve":
					wantSaves++
					if got.err != nil || got.run.ID != run.ID || got.run.Status != target {
						t.Error("valid decision did not rerun against fresh control")
					}
					concurrent.RestoreRuns[run.ID] = got.run
				case "same-decision", "opposite-decision":
					wantErr = domain.ErrConflict
				case "missing", "tenant-change":
					wantErr = domain.ErrNotFound
				case "requester-change":
					wantErr = domain.ErrForbidden
				}
				if wantErr != nil && (!errors.Is(got.err, wantErr) || got.run != (domain.RestoreRun{})) {
					t.Error("Save conflict failed to recheck current scope/status/duties or exposed a stale result")
				}
				var currentVersion int64
				if db.QueryRowContext(ctx, `SELECT version FROM audit_state_snapshot WHERE id = 1`).Scan(&currentVersion) != nil || currentVersion != version+wantSaves {
					t.Error("rejected decision performed an extra Save or lost its valid commit")
				}
				current := leaseSnapshot(t, first)
				for field, unchanged := range map[string]bool{
					"requests": reflect.DeepEqual(current.RestoreRuns, concurrent.RestoreRuns), "exports": reflect.DeepEqual(current.Exports, concurrent.Exports),
					"leases": reflect.DeepEqual(current.ExportLeases, concurrent.ExportLeases), "plans": reflect.DeepEqual(current.ExportPlans, concurrent.ExportPlans),
					"checkpoints": reflect.DeepEqual(current.ExportCheckpoints, concurrent.ExportCheckpoints), "Holds": reflect.DeepEqual(current.LegalHolds, concurrent.LegalHolds),
					"policy": reflect.DeepEqual(current.Policies, concurrent.Policies), "receipts": reflect.DeepEqual(current.Receipts, original.Receipts), "Schemas": reflect.DeepEqual(current.Schemas, original.Schemas),
				} {
					if !unchanged {
						t.Errorf("decision changed current control/evidence field=%s", field)
					}
				}
				wantFacts := len(concurrent.AdminActions)
				if mode == "preserve" {
					wantFacts++
				}
				if len(current.AdminActions) != wantFacts || !reflect.DeepEqual(current.AdminActions[:min(len(current.AdminActions), len(concurrent.AdminActions))], concurrent.AdminActions) {
					t.Error("decision lost concurrent facts or added a rejected success fact")
				}
				facts := restoreDecisionFacts(t, current, run.ID)
				wantDecisions := 0
				if decided || mode == "preserve" {
					wantDecisions = 1
				}
				if facts[domain.AdminActionRestoreCreated] != 1 || facts[domain.AdminActionRestoreApproved]+facts[domain.AdminActionRestoreRejected] != wantDecisions {
					t.Error("request/decision facts did not remain atomic and unique")
				}
				if objects.gets.Load() != gets || objects.puts.Load() != puts {
					t.Error("decision retried evidence I/O or executed external work")
				}
				second.finishExport(exportClaimContext(testCtx, claim), claim.ID, "completed", "late-object", "late-digest", 1, "")
				late := leaseSnapshot(t, first)
				if late.Exports[claim.ID].Status != domain.ExportStatusCancelled || late.Exports[claim.ID].ObjectPath != "" || len(late.ExportLeases) != 0 || len(late.ExportPlans) != 0 || len(late.ExportCheckpoints) != 0 {
					t.Error("decision conflict resurrected execution authority or allowed late publication")
				}
				t.Logf("restore_decision_boundary actual_Save_conflict=true assertions_passed=%t", !t.Failed())
			})
		}
	}
}
