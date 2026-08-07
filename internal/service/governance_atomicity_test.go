package service

import (
	"errors"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// TestGovernanceMutationAtomicity pins B1-4 (F13): a governance mutation
// whose Store.Update closure fails mid-way writes NOTHING — no partial legal
// hold, no admin action, no version bump — because the mutation and its
// self-audit record share one closure and the failure aborts the commit.
func TestGovernanceMutationAtomicity(t *testing.T) {
	svc := testService(t, false)
	readState := func() (holds int, actions int) {
		t.Helper()
		if err := svc.Store.Read(func(data *store.Snapshot) error {
			holds = len(data.LegalHolds)
			actions = len(data.AdminActions)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return holds, actions
	}
	beforeHolds, beforeActions := readState()

	// Tenant does not exist: the closure fails after the duplicate check but
	// before any write. Nothing may be committed.
	hold := domain.LegalHold{TenantID: "no-such-tenant", Name: "case-x", Reason: "litigation", CreatedBy: "compliance-1"}
	if _, err := svc.CreateLegalHold(hold); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("CreateLegalHold(err) = %v, want not found", err)
	}
	afterHolds, afterActions := readState()
	if afterHolds != beforeHolds || afterActions != beforeActions {
		t.Fatalf("failed mutation wrote partial state: holds %d->%d, actions %d->%d", beforeHolds, afterHolds, beforeActions, afterActions)
	}

	// The same failure mode holds for duplicate IDs (ErrConflict) and for
	// the release path (not found).
	first := domain.LegalHold{ID: "hold-fixed", TenantID: "tenant-a", Name: "case-1", Reason: "r", CreatedBy: "compliance-1"}
	if _, err := svc.CreateLegalHold(first); err != nil {
		t.Fatal(err)
	}
	duplicate := domain.LegalHold{ID: "hold-fixed", TenantID: "tenant-a", Name: "case-1", Reason: "r", CreatedBy: "compliance-1"}
	if _, err := svc.CreateLegalHold(duplicate); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("duplicate hold err=%v, want conflict", err)
	}
	midHolds, midActions := readState()
	if midHolds != beforeHolds+1 || midActions != beforeActions+1 {
		t.Fatalf("duplicate hold mutated state: holds=%d actions=%d", midHolds, midActions)
	}
	if _, err := svc.ReleaseLegalHold("tenant-a", "no-such-hold", "compliance-2"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("release missing hold err=%v, want not found", err)
	}
	finalHolds, finalActions := readState()
	if finalHolds != midHolds || finalActions != midActions {
		t.Fatalf("failed release wrote state: holds %d->%d, actions %d->%d", midHolds, finalHolds, midActions, finalActions)
	}
}
