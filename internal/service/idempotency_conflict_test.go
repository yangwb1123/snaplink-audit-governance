package service

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// TestIdempotencyConflictKeepsOwnerReceiptClean covers AC1-AC3 for both the
// legacy (file-backed) and hot/cold tenant-scoped ingest paths. On an
// idempotency-key collision the colliding caller must receive a fresh conflict
// receipt while the pre-existing owner event's stored receipt stays clean.
func TestIdempotencyConflictKeepsOwnerReceiptClean(t *testing.T) {
	for _, hot := range []bool{false, true} {
		hot := hot
		t.Run(fmt.Sprintf("hotcold=%v", hot), func(t *testing.T) {
			svc := testService(t, hot)
			at := time.Unix(1_700_000_010, 0).UTC()

			owner := testEvent("idem-owner-1", "idem-op", at)
			if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, owner, domain.StatusLedgered); err != nil {
				t.Fatalf("owner ingest failed: %v", err)
			}

			collider := testEvent("idem-collider-1", "idem-op", at.Add(time.Second))
			collider.IdempotencyKey = owner.IdempotencyKey

			receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, collider, domain.StatusLedgered)

			// AC1 — returned receipt for the collider is a conflict.
			if !errors.Is(err, domain.ErrConflict) {
				t.Fatalf("AC1: expected ErrConflict, got %v", err)
			}
			if !receipt.Conflict || receipt.ErrorCode != "idempotency_key_conflict" {
				t.Fatalf("AC1: unexpected collider receipt %+v", receipt)
			}

			// AC2 — owner receipt is untouched when re-read.
			storedOwner, gerr := svc.GetReceipt("tenant-a", "", owner.EventID)
			if gerr != nil {
				t.Fatalf("AC2: GetReceipt(owner) failed: %v", gerr)
			}
			if storedOwner.Conflict || storedOwner.ErrorCode != "" || storedOwner.ErrorMessage != "" {
				t.Fatalf("AC2: owner receipt corrupted: %+v", storedOwner)
			}

			// AC3 — collider receipt is fresh and the owner mutation was never
			// persisted.
			if receipt.EventID != collider.EventID {
				t.Fatalf("AC3: collider receipt must describe the colliding event, got %q", receipt.EventID)
			}
			if receipt.EventID == storedOwner.EventID {
				t.Fatalf("AC3: collider receipt must not be the owner receipt")
			}
			if receipt.Conflict == storedOwner.Conflict &&
				receipt.ErrorCode == storedOwner.ErrorCode &&
				receipt.ErrorMessage == storedOwner.ErrorMessage {
				t.Fatalf("AC3: collider receipt equals owner receipt: %+v", receipt)
			}

			assertOwnerReceiptClean(t, svc, hot, owner.EventID)
		})
	}
}

// TestIdempotencyConflictKeepsArchivedOwnerReceiptClean is the archived-owner
// bonus case: the owner event is already archived when the collider arrives,
// exercising the cold-ledger idempotency index. Re-reading the owner after the
// conflict must still surface a clean receipt.
func TestIdempotencyConflictKeepsArchivedOwnerReceiptClean(t *testing.T) {
	svc := testService(t, true)
	at := time.Unix(1_700_000_010, 0).UTC()

	owner := testEvent("idem-archived-owner-1", "idem-archived", at)
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, owner, domain.StatusArchived); err != nil {
		t.Fatalf("owner ingest failed: %v", err)
	}

	collider := testEvent("idem-archived-collider-1", "idem-archived", at.Add(time.Second))
	collider.IdempotencyKey = owner.IdempotencyKey

	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, collider, domain.StatusLedgered)
	if !errors.Is(err, domain.ErrConflict) || !receipt.Conflict || receipt.ErrorCode != "idempotency_key_conflict" {
		t.Fatalf("expected archived idempotency conflict: receipt=%+v err=%v", receipt, err)
	}

	storedOwner, gerr := svc.GetReceipt("tenant-a", "", owner.EventID)
	if gerr != nil {
		t.Fatalf("GetReceipt(archived owner) failed: %v", gerr)
	}
	if storedOwner.Conflict || storedOwner.ErrorCode != "" || storedOwner.ErrorMessage != "" {
		t.Fatalf("archived owner receipt corrupted: %+v", storedOwner)
	}

	assertOwnerReceiptClean(t, svc, true, owner.EventID)
}

// assertOwnerReceiptClean reads the persisted snapshot (legacy) or tenant
// ledger (hot/cold) for the owner event and fails if any conflict marker was
// written to it — i.e. the Store closure must not have persisted a mutation to
// the owner's receipt.
func assertOwnerReceiptClean(t *testing.T, svc *Service, hot bool, ownerEventID string) {
	t.Helper()
	key := store.EventKey("tenant-a", ownerEventID)
	if hot {
		err := svc.Store.ReadTenant("tenant-a", func(view *store.TenantView) error {
			r, ok := view.Ledger.Receipt(key)
			if !ok {
				return fmt.Errorf("owner receipt missing from tenant ledger")
			}
			if r.Conflict || r.ErrorCode != "" || r.ErrorMessage != "" {
				return fmt.Errorf("owner receipt persisted dirty: %+v", r)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("AC3 (hot/cold): %v", err)
		}
		return
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	r, ok := snap.Receipts[key]
	if !ok {
		t.Fatal("AC3 (legacy): owner receipt missing from snapshot")
	}
	if r.Conflict || r.ErrorCode != "" || r.ErrorMessage != "" {
		t.Fatalf("AC3 (legacy): owner receipt persisted dirty: %+v", r)
	}
}
