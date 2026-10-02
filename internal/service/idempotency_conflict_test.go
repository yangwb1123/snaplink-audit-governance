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
			storedOwner, gerr := svc.GetReceipt(testCtx, "tenant-a", "", owner.EventID)
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

	storedOwner, gerr := svc.GetReceipt(testCtx, "tenant-a", "", owner.EventID)
	if gerr != nil {
		t.Fatalf("GetReceipt(archived owner) failed: %v", gerr)
	}
	if storedOwner.Conflict || storedOwner.ErrorCode != "" || storedOwner.ErrorMessage != "" {
		t.Fatalf("archived owner receipt corrupted: %+v", storedOwner)
	}

	assertOwnerReceiptClean(t, svc, true, owner.EventID)
}

// TestArchivedDuplicateIngestAndConflictUnchanged pins AC-7 on the hot-evicted
// path the bounded fallback serves: an identical retry resolves through the
// WORM read and still answers Duplicate with the ARCHIVED receipt, a
// content-differing retry still answers domain.ErrConflict, and neither adds a
// second ledger entry. The bound added by archiveFallbackContext adds no error
// path between checkExistingIngest's digest comparison and its return, so
// at-least-once and idempotency semantics are unchanged.
func TestArchivedDuplicateIngestAndConflictUnchanged(t *testing.T) {
	svc := testService(t, true)
	at := time.Unix(1_700_000_010, 0).UTC()

	first, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("arch-idem-1", "arch-idem-op", at), domain.StatusArchived)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	// The fixture must really be hot-evicted, or the duplicate would never
	// touch the WORM fallback this direction bounds.
	if first.Status != domain.StatusArchived || first.ArchivedAt.IsZero() {
		t.Fatalf("fixture must be hot-evicted, got %+v", first)
	}

	duplicate, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("arch-idem-1", "arch-idem-op", at), domain.StatusArchived)
	if err != nil || !duplicate.Duplicate {
		t.Fatalf("identical retry of an archived event: receipt=%+v err=%v, want Duplicate", duplicate, err)
	}
	if duplicate.Status != domain.StatusArchived {
		t.Fatalf("duplicate receipt=%+v, want the archived receipt preserved", duplicate)
	}

	conflicting := testEvent("arch-idem-1", "arch-idem-op", at)
	conflicting.Payload["value"] = 11
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, conflicting, domain.StatusArchived); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("content-differing retry: err=%v, want ErrConflict", err)
	}

	// No second ledger entry: the integrity scan still sees exactly one event.
	result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil {
		t.Fatalf("VerifyIntegrity: %v", err)
	}
	if !result.Valid || result.EventCount != 1 {
		t.Fatalf("integrity=%+v, want one valid event and no duplicate ledger entry", result)
	}

	// The fallback still resolves after the conflict was recorded.
	retry, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("arch-idem-1", "arch-idem-op", at), domain.StatusArchived)
	if err != nil || !retry.Duplicate {
		t.Fatalf("retry after conflict: receipt=%+v err=%v, want Duplicate", retry, err)
	}
}

// assertOwnerReceiptClean reads the persisted snapshot (legacy) or tenant
// ledger (hot/cold) for the owner event and fails if any conflict marker was
// written to it — i.e. the Store closure must not have persisted a mutation to
// the owner's receipt.
func assertOwnerReceiptClean(t *testing.T, svc *Service, hot bool, ownerEventID string) {
	t.Helper()
	key := store.EventKey("tenant-a", ownerEventID)
	if hot {
		err := svc.Store.ReadTenant(testCtx, "tenant-a", func(view *store.TenantView) error {
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
	snap, err := svc.Store.Snapshot(testCtx)
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
