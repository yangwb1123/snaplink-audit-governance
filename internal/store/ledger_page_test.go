package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

func seedLedgerPageRecords(t *testing.T, st *Store) {
	t.Helper()
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		if err := st.UpdateTenant(context.Background(), tenantID, HotFirst, func(view *TenantView) error {
			for _, id := range []string{"first", "second", "third"} {
				receipt := publicationTestReceipt(publicationTestEvent(tenantID, id))
				receipt.Status = domain.StatusLedgered
				if err := view.Ledger.SetReceipt(receipt); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func assertLedgerPages(t *testing.T, st *Store) {
	t.Helper()
	after := LedgerPageCursor{}
	seen := map[string]bool{}
	for round := 0; round < 10; round++ {
		page, err := st.ReadLedgerPage(context.Background(), "tenant-a", after, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Records) > 2 {
			t.Fatalf("page exceeded bound: %d", len(page.Records))
		}
		for _, record := range page.Records {
			if record.TenantID != "tenant-a" || seen[record.Key] {
				t.Fatalf("scope/duplicate failure: %+v", record)
			}
			seen[record.Key] = true
			record.Receipt.Hash = "mutated-page-copy"
		}
		if page.Done {
			if len(seen) != 3 {
				t.Fatalf("seen %d records, want 3", len(seen))
			}
			first, err := st.ReadLedgerPage(context.Background(), "tenant-a", LedgerPageCursor{}, 1)
			if err != nil || len(first.Records) != 1 || first.Records[0].Receipt.Hash != "ledger-hash" {
				t.Fatalf("page aliases committed records: %+v err=%v", first, err)
			}
			return
		}
		if page.Next.After <= after.After {
			t.Fatalf("page made no progress: after=%+v next=%+v", after, page.Next)
		}
		after = page.Next
	}
	t.Fatal("bounded scan did not terminate")
}

func TestLedgerPageBoundedAndTenantIsolated(t *testing.T) {
	st, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	seedLedgerPageRecords(t, st)
	// A tenant/cold page must not materialize unrelated hot payloads or the
	// shared control document (neither of these graphs is cloneable).
	st.split.tenant["tenant-b"].hot.Events[EventKey("tenant-b", "poison")] = domain.Event{Payload: map[string]any{"value": make(chan int)}}
	st.split.control.LedgeredOutbox[EventKey("tenant-b", "poison")] = domain.Event{Payload: map[string]any{"value": make(chan int)}}
	assertLedgerPages(t, st)
	delete(st.split.tenant["tenant-b"].hot.Events, EventKey("tenant-b", "poison"))
	delete(st.split.control.LedgeredOutbox, EventKey("tenant-b", "poison"))
	assertFiniteLedgerSweep(t, st)
	page, err := st.ReadLedgerPage(context.Background(), "tenant-a", LedgerPageCursor{After: math.MaxInt64}, 1)
	if err != nil || !page.Done || len(page.Records) != 0 {
		t.Fatalf("out-of-range position page=%+v err=%v", page, err)
	}
	for _, args := range []struct {
		tenantID string
		after    int64
		limit    int
	}{{"", 0, 1}, {"tenant-a", -1, 1}, {"tenant-a", 0, 0}, {"tenant-a", 0, MaxLedgerPageSize + 1}} {
		if _, err := st.ReadLedgerPage(context.Background(), args.tenantID, LedgerPageCursor{After: args.after}, args.limit); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("bad page bounds err=%v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := st.ReadLedgerPage(ctx, "tenant-a", LedgerPageCursor{}, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled page err=%v", err)
	}
}

func TestPostgresLedgerPages(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	st, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	// Establish the verified zero-data baseline before any tenant writes.
	if err := st.UpdateControl(context.Background(), func(*Snapshot) error { return nil }); err != nil {
		t.Fatal(err)
	}
	seedLedgerPageRecords(t, st)
	assertLedgerPages(t, st)
	assertFiniteLedgerSweep(t, st)
	assertLateCommitDiscoveredOnRescan(t, st, db)
}

func assertFiniteLedgerSweep(t *testing.T, st *Store) {
	t.Helper()
	page, err := st.ReadLedgerPage(context.Background(), "tenant-a", LedgerPageCursor{}, 1)
	if err != nil || len(page.Records) != 1 {
		t.Fatalf("first live page=%+v err=%v", page, err)
	}
	through := page.Next.Through
	for round := 0; round < 10 && !page.Done; round++ {
		// Tail keeps growing; this must not extend the captured sweep horizon.
		if err := st.UpdateTenant(context.Background(), "tenant-a", HotFirst, func(view *TenantView) error {
			return view.Ledger.SetReceipt(publicationTestReceipt(publicationTestEvent("tenant-a", fmt.Sprintf("arriving-%d", round))))
		}); err != nil {
			t.Fatal(err)
		}
		page, err = st.ReadLedgerPage(context.Background(), "tenant-a", page.Next, 1)
		if err != nil || page.Next.Through != through || page.Next.After > through {
			t.Fatalf("sweep chased moving tail: %+v err=%v", page, err)
		}
	}
	if !page.Done {
		t.Fatal("continuous ingest prevented sweep from finishing")
	}
	fresh, err := st.ReadLedgerPage(context.Background(), "tenant-a", LedgerPageCursor{}, MaxLedgerPageSize)
	if err != nil || fresh.Next.Through <= through {
		t.Fatalf("next sweep did not capture new tail: %+v err=%v", fresh, err)
	}
}

func assertLateCommitDiscoveredOnRescan(t *testing.T, st *Store, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	insert := `INSERT INTO audit_ledger (tenant_id, record_type, key, version, record) VALUES ($1, 'receipt', $2, 1, $3::jsonb)`
	encoded := func(id string) (string, string) {
		receipt := publicationTestReceipt(publicationTestEvent("tenant-a", id))
		key := EventKey("tenant-a", id)
		raw, err := json.Marshal(LedgerRecord{TenantID: "tenant-a", RecordType: LedgerReceipt, Key: key, Version: 1, Receipt: &receipt})
		if err != nil {
			t.Fatal(err)
		}
		return key, string(raw)
	}
	lateKey, lateRaw := encoded("late-commit")
	if _, err := tx.ExecContext(ctx, insert, "tenant-a", lateKey, lateRaw); err != nil {
		t.Fatal(err)
	}
	// A larger id commits first; MAX(id) is therefore NOT a commit watermark.
	highKey, highRaw := encoded("larger-id-first")
	if _, err := db.ExecContext(ctx, insert, "tenant-a", highKey, highRaw); err != nil {
		t.Fatal(err)
	}
	page, err := st.ReadLedgerPage(ctx, "tenant-a", LedgerPageCursor{}, MaxLedgerPageSize)
	if err != nil || !page.Done {
		t.Fatalf("initial sweep=%+v err=%v", page, err)
	}
	for _, record := range page.Records {
		if record.Key == lateKey {
			t.Fatal("uncommitted record entered the scan")
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tail, err := st.ReadLedgerPage(ctx, "tenant-a", page.Next, MaxLedgerPageSize)
	if err != nil || !tail.Done || len(tail.Records) != 0 {
		t.Fatalf("finished cursor unexpectedly changed: %+v err=%v", tail, err)
	}
	fresh, err := st.ReadLedgerPage(ctx, "tenant-a", LedgerPageCursor{}, MaxLedgerPageSize)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range fresh.Records {
		if record.Key == lateKey {
			return
		}
	}
	t.Fatal("reset sweep lost a lower-id late transaction commit")
}
