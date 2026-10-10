package store

import (
	"errors"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestPostgresHotFirstColdFailureRollsBackHotCAS(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	st, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateControl(testCtx, func(data *Snapshot) error { data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a"}; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateTenant(testCtx, "tenant-a", HotFirst, func(view *TenantView) error {
		view.Hot.Events[EventKey("tenant-a", "before")] = domain.Event{TenantID: "tenant-a", EventID: "before"}
		return view.Ledger.SetReceipt(domain.EventReceipt{TenantID: "tenant-a", EventID: "before", StreamID: "stream", Sequence: 1, Hash: "first"})
	}); err != nil {
		t.Fatal(err)
	}
	before := postgresHotColdTargetState(t, db)
	_, err = db.Exec(`CREATE OR REPLACE FUNCTION audit_test_reject_hot_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected cold failure'; END $$;
CREATE TRIGGER audit_test_reject_hot_commit BEFORE INSERT ON audit_ledger FOR EACH ROW EXECUTE FUNCTION audit_test_reject_hot_commit();`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = db.Exec(`DROP TRIGGER IF EXISTS audit_test_reject_hot_commit ON audit_ledger; DROP FUNCTION IF EXISTS audit_test_reject_hot_commit()`)
	}()
	mutate := func(view *TenantView) error {
		view.Hot.Events[EventKey("tenant-a", "next")] = domain.Event{TenantID: "tenant-a", EventID: "next"}
		return view.Ledger.SetReceipt(domain.EventReceipt{TenantID: "tenant-a", EventID: "next", StreamID: "stream", Sequence: 2, Hash: "second"})
	}
	if err := st.UpdateTenant(testCtx, "tenant-a", HotFirst, mutate); err == nil {
		t.Fatal("cold fault did not fire")
	}
	if after := postgresHotColdTargetState(t, db); after != before {
		t.Fatal("hot CAS escaped its failed cold transaction")
	}
	replica, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := replica.ReadTenant(testCtx, "tenant-a", func(view *TenantView) error {
		if _, found := view.Hot.Events[EventKey("tenant-a", "next")]; found {
			t.Fatal("replica observed uncommitted hot body")
		}
		if _, found := view.Ledger.Receipt(EventKey("tenant-a", "next")); found {
			t.Fatal("replica observed uncommitted receipt")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TRIGGER audit_test_reject_hot_commit ON audit_ledger`); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateTenant(testCtx, "tenant-a", HotFirst, mutate); err != nil {
		t.Fatal(err)
	}
	if err := replica.ReadTenant(testCtx, "tenant-a", func(view *TenantView) error {
		if _, found := view.Hot.Events[EventKey("tenant-a", "next")]; !found {
			return errors.New("missing hot body")
		}
		if _, found := view.Ledger.Receipt(EventKey("tenant-a", "next")); !found {
			return errors.New("missing cold receipt")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if candidate, err := replica.ReadPendingTenantCommit(testCtx, "tenant-a"); err != nil || len(candidate.Records) != 0 {
		t.Fatalf("transaction invented recovery marker: %v", err)
	}
}
