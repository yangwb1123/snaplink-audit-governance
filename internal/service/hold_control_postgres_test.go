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

// These are real version conflicts at the control Save, not an artificial
// error before Load. The inherited helper checks pg_blocking_pids against the
// exact row-lock holder in this explicitly disposable independent-pool DB.
func TestPostgresHoldControlRealSaveConflictKeepsCurrentDecisions(t *testing.T) {
	for _, mode := range []string{"create-preserve", "create-same-ID", "create-tenant-gone", "release-preserve", "release-first-winner", "release-tenant-change"} {
		t.Run(mode, func(t *testing.T) {
			first, second, _, _, _ := postgresPreflightFixture(t)
			seedActionExport(t, first, "hold-control-neighbor", "pending")
			claim, won, err := first.claimPendingExport(testCtx, "hold-control-neighbor")
			if err != nil || !won {
				t.Fatal("could not prepare original fenced neighboring executor")
			}
			hold := domain.LegalHold{ID: "hold-control-race", TenantID: "tenant-a", Name: "original", Reason: "case", CreatedBy: "case-worker", CreatedAt: first.Now()}
			release := mode == "release-preserve" || mode == "release-first-winner" || mode == "release-tenant-change"
			if release {
				if _, err := first.CreateLegalHold(testCtx, hold); err != nil {
					t.Fatal("could not prepare original active Hold")
				}
			}
			original := leaseSnapshot(t, first)
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
			var concurrent store.Snapshot
			if json.Unmarshal(raw, &concurrent) != nil {
				t.Fatal("could not capture actual concurrent control baseline")
			}
			type outcome struct {
				hold domain.LegalHold
				err  error
			}
			done, exited := make(chan outcome, 1), make(chan struct{})
			t.Cleanup(func() {
				cancel()
				_ = tx.Rollback()
				select {
				case <-exited:
				case <-time.After(5 * time.Second):
					t.Error("owned Hold writer did not exit")
				}
			})
			go func() {
				defer close(exited)
				var value domain.LegalHold
				var err error
				if release {
					value, err = second.ReleaseLegalHold(ctx, "tenant-a", hold.ID, "later-releaser")
				} else {
					value, err = second.CreateLegalHold(ctx, hold)
				}
				done <- outcome{hold: value, err: err}
			}()
			awaitAdmissionControlSave(t, ctx, db, holder)
			// A legal concurrent terminal decision revokes the original claim;
			// the stale Hold writer must not resurrect it or erase its fact.
			job := concurrent.Exports[claim.ID]
			at := first.Now()
			job.Status, job.FailureCode, job.FinishedAt = domain.ExportStatusCancelled, domain.ExportFailureCancelled, &at
			concurrent.Exports[job.ID] = job
			delete(concurrent.ExportLeases, job.ID)
			concurrent.AdminActions = append(concurrent.AdminActions, first.adminAction("tenant-a", "operator", domain.AdminActionExportCancelled, "export", job.ID, "status=cancelled"))
			concurrent.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a", ArchiveDays: 365, RetentionClass: "standard"}
			concurrent.LegalHolds["hold-control-concurrent"] = domain.LegalHold{ID: "hold-control-concurrent", TenantID: "tenant-a", Name: "concurrent", Reason: "case", CreatedAt: at}
			switch mode {
			case "create-same-ID":
				winner := hold
				winner.Name, winner.CreatedBy = "first-winner", "first-creator"
				concurrent.LegalHolds[hold.ID] = winner
				concurrent.AdminActions = append(concurrent.AdminActions, first.adminAction("tenant-a", winner.CreatedBy, domain.AdminActionLegalHoldCreated, "legal_hold", hold.ID, winner.Reason))
			case "create-tenant-gone":
				delete(concurrent.Tenants, "tenant-a")
			case "release-first-winner":
				winner := concurrent.LegalHolds[hold.ID]
				winner.ReleasedAt, winner.ReleasedBy = &at, "first-releaser"
				concurrent.LegalHolds[hold.ID] = winner
				concurrent.AdminActions = append(concurrent.AdminActions, first.adminAction("tenant-a", winner.ReleasedBy, domain.AdminActionLegalHoldReleased, "legal_hold", hold.ID, ""))
			case "release-tenant-change":
				moved := concurrent.LegalHolds[hold.ID]
				moved.TenantID = "tenant-b" // owned corruption: never release across the requested scope
				concurrent.LegalHolds[hold.ID] = moved
			}
			encoded, err := json.Marshal(&concurrent)
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
				t.Fatal("Hold writer did not converge after actual Save conflict")
			}
			current := leaseSnapshot(t, first)
			wantSaves := int64(1) // the concurrent commit; a rejected/no-op retry adds none
			created, released := 0, 0
			if release {
				created = 1
			}
			switch mode {
			case "create-preserve", "release-preserve":
				wantSaves++
				if got.err != nil || got.hold.ID != hold.ID || (release && got.hold.ReleasedBy != "later-releaser") {
					t.Error("valid Hold mutation did not rerun against current control")
				}
				created = 1
				if release {
					released = 1
				}
				concurrent.LegalHolds[hold.ID] = got.hold
			case "create-same-ID":
				created = 1
				if !errors.Is(got.err, domain.ErrConflict) || got.hold.ID != "" {
					t.Error("real conflict overwrote a concurrently created same-ID Hold")
				}
			case "create-tenant-gone", "release-tenant-change":
				if !errors.Is(got.err, domain.ErrNotFound) || got.hold.ID != "" {
					t.Error("real conflict failed to recheck current tenant scope")
				}
			case "release-first-winner":
				released = 1
				if got.err != nil || !reflect.DeepEqual(got.hold, concurrent.LegalHolds[hold.ID]) {
					t.Error("real conflict replaced the original release actor/time")
				}
			}
			var currentVersion int64
			if db.QueryRowContext(ctx, `SELECT version FROM audit_state_snapshot WHERE id = 1`).Scan(&currentVersion) != nil || currentVersion != version+wantSaves {
				t.Error("Hold conflict committed an extra rejected/no-op Save or lost its valid commit")
			}
			if !reflect.DeepEqual(current.Exports, concurrent.Exports) || !reflect.DeepEqual(current.ExportLeases, concurrent.ExportLeases) ||
				!reflect.DeepEqual(current.LegalHolds, concurrent.LegalHolds) || !reflect.DeepEqual(current.Tenants, concurrent.Tenants) ||
				!reflect.DeepEqual(current.Policies, concurrent.Policies) || !reflect.DeepEqual(current.ExportPlans, original.ExportPlans) ||
				!reflect.DeepEqual(current.ExportCheckpoints, original.ExportCheckpoints) || !reflect.DeepEqual(current.Receipts, original.Receipts) ||
				!reflect.DeepEqual(current.Schemas, original.Schemas) {
				t.Error("stale Hold Save overwrote current terminal/lease/Hold/policy or evidence")
			}
			if mode != "release-tenant-change" {
				assertHoldControlFacts(t, current.AdminActions, hold.ID, created, released)
			}
			second.finishExport(exportClaimContext(testCtx, claim), claim.ID, "completed", "late-object", "late-digest", 1, "")
			late := leaseSnapshot(t, first)
			if late.Exports[claim.ID].Status != domain.ExportStatusCancelled || late.Exports[claim.ID].ObjectPath != "" || len(late.ExportLeases) != 0 {
				t.Error("Hold conflict resurrected running authority or allowed late publication")
			}
			t.Logf("hold_control_boundary actual_Save_conflict=true assertions_passed=%t", !t.Failed())
		})
	}
}
