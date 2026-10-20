package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// A real row lock parks the actual optimistic Save AFTER its read/preflight.
// pg_blocking_pids, not a sleep, proves the writer reached that boundary. The
// dedicated disposable DB is the only target; no shared/production DSN fallback.
func awaitAdmissionControlSave(t *testing.T, ctx context.Context, db *sql.DB, holder int) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		err := db.QueryRowContext(ctx, `SELECT EXISTS (
SELECT 1 FROM pg_stat_activity WHERE datname = current_database()
AND $1 = ANY(pg_blocking_pids(pid))
AND (query LIKE 'UPDATE audit_state_snapshot%' OR query LIKE 'INSERT INTO audit_state_snapshot%'))`, holder).Scan(&blocked)
		if err != nil {
			t.Fatal("could not observe the actual isolated control Save lock")
		}
		if blocked {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("admission never reached its actual control Save")
		case <-ticker.C:
		}
	}
}

func TestPostgresExportAdmissionRealSaveConflictRechecksCurrentControl(t *testing.T) {
	for _, mode := range []string{"Hold", "queue-budget", "cancel-policy"} {
		t.Run(mode, func(t *testing.T) {
			first, second, _, _, query := postgresPreflightFixture(t)
			seedActionExport(t, first, "admission-neighbor", "pending")
			claim, won, err := first.claimPendingExport(testCtx, "admission-neighbor")
			if err != nil || !won {
				t.Fatal("could not prepare original neighboring execution claim")
			}
			original := leaseSnapshot(t, first)
			db, err := sql.Open("pgx", os.Getenv("AUDIT_TEST_POSTGRES_EXPORT_LEASE_DSN"))
			if err != nil {
				t.Fatal("could not open disposable lock controller")
			}
			defer db.Close()
			ctx, cancel := context.WithTimeout(testCtx, 5*time.Second)
			defer cancel()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal("could not start isolated control writer")
			}
			defer tx.Rollback()
			var raw []byte
			var version int64
			var holder int
			if err := tx.QueryRowContext(ctx, `SELECT snapshot, version FROM audit_state_snapshot WHERE id = 1 FOR UPDATE`).Scan(&raw, &version); err != nil {
				t.Fatal("could not lock the actual control row")
			}
			if err := tx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&holder); err != nil {
				t.Fatal("could not identify the owned row-lock holder")
			}
			var concurrent store.Snapshot
			if json.Unmarshal(raw, &concurrent) != nil {
				t.Fatal("could not capture the disposable control baseline")
			}
			type outcome struct {
				job domain.ExportJob
				err error
			}
			done, exited := make(chan outcome, 1), make(chan struct{})
			t.Cleanup(func() {
				cancel()
				_ = tx.Rollback()
				select {
				case <-exited:
				case <-time.After(5 * time.Second):
					t.Error("isolated admission goroutine did not exit")
				}
			})
			go func() {
				defer close(exited)
				job, err := second.CreateExport(ctx, "tenant-a", "operator", query)
				done <- outcome{job: job, err: err}
			}()
			awaitAdmissionControlSave(t, ctx, db, holder)
			switch mode {
			case "Hold":
				concurrent.LegalHolds["admission-current"] = domain.LegalHold{ID: "admission-current", TenantID: "tenant-a", Name: "case", Reason: "case", CreatedAt: first.Now()}
			case "queue-budget":
				for i := len(concurrent.Exports); i < MaxPendingExportsPerTenant; i++ {
					id := fmt.Sprintf("admission-concurrent-%d", i)
					concurrent.Exports[id] = domain.ExportJob{ID: id, TenantID: "tenant-a", RequestedBy: "operator", Status: "pending", CreatedAt: first.Now()}
				}
			case "cancel-policy":
				job := concurrent.Exports[claim.ID]
				at := first.Now()
				job.Status, job.FailureCode, job.FinishedAt = domain.ExportStatusCancelled, domain.ExportFailureCancelled, &at
				concurrent.Exports[claim.ID] = job
				delete(concurrent.ExportLeases, claim.ID)
				concurrent.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a", ArchiveDays: 365, RetentionClass: "standard"}
			}
			encoded, err := json.Marshal(&concurrent)
			if err != nil {
				t.Fatal("could not encode owned concurrent control mutation")
			}
			result, err := tx.ExecContext(ctx, `UPDATE audit_state_snapshot SET snapshot = $1::jsonb, version = version + 1 WHERE id = 1 AND version = $2`, string(encoded), version)
			if err != nil {
				t.Fatal("could not apply the isolated concurrent control mutation")
			}
			if n, err := result.RowsAffected(); err != nil || n != 1 || tx.Commit() != nil {
				t.Fatal("concurrent control mutation did not really commit")
			}
			var got outcome
			select {
			case got = <-done:
			case <-ctx.Done():
				t.Fatal("admission failed to finish after the real Save conflict")
			}
			current := leaseSnapshot(t, first)
			wantExports, wantCreated := len(concurrent.Exports), 0
			switch mode {
			case "Hold":
				if !errors.Is(got.err, domain.ErrConflict) || got.job.ID != "" {
					t.Error("real CAS conflict reused stale Hold admission instead of rejecting")
				}
			case "queue-budget":
				if !errors.Is(got.err, domain.ErrQuotaExceeded) || got.job.ID != "" {
					t.Error("real CAS conflict reused stale queue capacity instead of rejecting")
				}
			case "cancel-policy":
				wantExports++
				wantCreated = 1
				if got.err != nil || got.job.Status != "pending" || !reflect.DeepEqual(current.Exports[got.job.ID], got.job) {
					t.Error("unrelated current control prevented valid admission")
				}
			}
			if len(current.Exports) != wantExports || !reflect.DeepEqual(current.Exports[claim.ID], concurrent.Exports[claim.ID]) || !reflect.DeepEqual(current.ExportLeases, concurrent.ExportLeases) || !reflect.DeepEqual(current.LegalHolds, concurrent.LegalHolds) || !reflect.DeepEqual(current.Policies, concurrent.Policies) {
				t.Error("stale admission overwrote current terminal/lease/Hold/budget/policy control")
			}
			if countAdminActions(t, first, func(f domain.AdminAction) bool { return f.Action == domain.AdminActionExportCreated }) != wantCreated || len(readFacts(t, first)) != 1 || !reflect.DeepEqual(current.Receipts, original.Receipts) || !reflect.DeepEqual(current.Schemas, original.Schemas) {
				t.Error("admission conflict changed evidence or invented/skipped successful facts")
			}
			t.Logf("admission_boundary actual_Save_conflict=true assertions_passed=%t", !t.Failed())
		})
	}
}
