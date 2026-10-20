package service

import (
	"database/sql"
	"os"
	"reflect"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestPostgresReceiptReadExactIndexAndFailureAcrossIndependentPools(t *testing.T) {
	first, second, _, _ := postgresInitialQueryFixture(t)
	original, err := first.Store.Snapshot(testCtx)
	if err != nil || original.LayoutVersion != 2 {
		t.Fatal("receipt index integration requires actual v2 independent pools")
	}
	// This extra connection reaches only the explicitly disposable test DB.
	// Corrupt a different owned fixture receipt: a global cold materialization
	// must fail, but the exact requested committed identity remains readable.
	db, err := sql.Open("pgx", os.Getenv("AUDIT_TEST_POSTGRES_EXPORT_LEASE_DSN"))
	if err != nil {
		t.Fatal("could not open disposable receipt fault connection")
	}
	defer db.Close()
	result, err := db.ExecContext(testCtx, `UPDATE audit_ledger SET record = '{}'::jsonb WHERE tenant_id = $1 AND record_type = 'receipt' AND key = $2`, "tenant-a", store.EventKey("tenant-a", "initial-b"))
	if err != nil {
		t.Fatal("could not persist owned unrelated receipt fault")
	}
	rows, err := result.RowsAffected()
	if err != nil || rows < 1 {
		t.Fatal("receipt cold-scan oracle did not affect actual ledger records")
	}
	if _, err := first.Store.Snapshot(testCtx); err == nil {
		t.Fatal("receipt integration did not establish the global cold materialization failure")
	}
	for _, svc := range []*Service{first, second} {
		svc.Config.Archive = receiptForbiddenArchive{Store: svc.Config.Archive}
		got, err := svc.GetReceipt(testCtx, "tenant-a", "reader", "initial-a")
		if err != nil || !reflect.DeepEqual(got, original.Receipts[store.EventKey("tenant-a", "initial-a")]) {
			t.Fatal("independent receipt reader scanned unrelated cold history, read source or changed metadata")
		}
		got, err = svc.GetReceipt(testCtx, "tenant-a", "reader", "initial-b")
		if err == nil || !reflect.DeepEqual(got, domain.EventReceipt{}) {
			t.Fatal("exact receipt storage error was masked as success or partial metadata")
		}
	}
	trail, err := second.Store.ReadAdminTrail(testCtx)
	if err != nil || len(trail) != 2 {
		t.Fatal("independent receipt failure appended success or permitted lookup lost its fact")
	}
	for _, fact := range trail {
		if fact.Action != domain.AdminActionEventRead || fact.TenantID != "tenant-a" || fact.TargetID != "initial-a" || fact.Detail != "receipt" {
			t.Fatal("independent receipt read changed its safe fact encoding")
		}
	}
	var faultStillPresent bool
	if err := db.QueryRowContext(testCtx, `SELECT bool_and(record = '{}'::jsonb) FROM audit_ledger WHERE tenant_id = $1 AND record_type = 'receipt' AND key = $2`, "tenant-a", store.EventKey("tenant-a", "initial-b")).Scan(&faultStillPresent); err != nil || !faultStillPresent {
		t.Fatal("receipt read rewrote the owned cold fault or silently repaired history")
	}
}
