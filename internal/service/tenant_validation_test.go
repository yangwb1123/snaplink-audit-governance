package service

import (
	"errors"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// TestCreateTenantRejectsKeyFramingIDs is AC-1 at the service boundary: the
// same reject/accept table as store.ValidTenantID, exercised through the
// public CreateTenant entry point (the only tenant-creation path).
func TestCreateTenantRejectsKeyFramingIDs(t *testing.T) {
	svc := testService(t, false)
	rejected := []string{
		"a\x1fb", "a b", " a", "a\tb", "a\nb", "\x00", "a/b", `a\b`, "a\u00a0b", "a:b",
		// FM-1 archive-component bound: 86 bytes of punctuation (3× expansion
		// = 258 > NAME_MAX) and 29 three-byte runes (87 bytes) are rejected at
		// the boundary; 85 bytes / 28 runes are accepted below.
		strings.Repeat("?", domain.MaxArchiveComponentBytes+1),
		strings.Repeat("界", 29),
	}
	for _, id := range rejected {
		err := svc.CreateTenant("test", domain.Tenant{ID: id, Name: "X", Active: true})
		if !errors.Is(err, domain.ErrInvalid) {
			t.Errorf("CreateTenant(id=%q) = %v, want ErrInvalid", id, err)
		}
	}
	accepted := []string{"tenant-c", "a-b_c.d", "tëstant", "123", strings.Repeat("?", domain.MaxArchiveComponentBytes), strings.Repeat("界", 28)}
	for _, id := range accepted {
		if err := svc.CreateTenant("test", domain.Tenant{ID: id, Name: "X", Active: true}); err != nil {
			t.Errorf("CreateTenant(id=%q) = %v, want nil", id, err)
		}
	}
}

// TestCreateTenantRejectionSideEffectFree is AC-1's side-effect guarantee:
// a rejected ID must not create a tenant, must not append a tenant.created
// admin action, and must not wedge the store for subsequent valid creates.
func TestCreateTenantRejectionSideEffectFree(t *testing.T) {
	svc := testService(t, false)
	readCounts := func() (tenants, adminActions int) {
		t.Helper()
		if err := svc.Store.Read(func(data *store.Snapshot) error {
			tenants = len(data.Tenants)
			adminActions = len(data.AdminActions)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return tenants, adminActions
	}
	beforeTenants, beforeActions := readCounts()

	for _, id := range []string{"a\x1fb", "a b", "a/b", "a:b"} {
		if err := svc.CreateTenant("test", domain.Tenant{ID: id, Name: "X", Active: true}); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("CreateTenant(id=%q) = %v, want ErrInvalid", id, err)
		}
	}
	afterTenants, afterActions := readCounts()
	if afterTenants != beforeTenants || afterActions != beforeActions {
		t.Fatalf("rejected creates mutated the snapshot: tenants %d->%d, admin actions %d->%d",
			beforeTenants, afterTenants, beforeActions, afterActions)
	}

	// A valid create after the rejections still succeeds (no wedged state).
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-c", Name: "Tenant C", Active: true}); err != nil {
		t.Fatalf("valid create after rejections failed: %v", err)
	}
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		if _, exists := data.Tenants["tenant-c"]; !exists {
			t.Fatal("tenant-c missing after valid create")
		}
		if _, exists := data.Tenants["a\x1fb"]; exists {
			t.Fatal("rejected tenant id must never be stored")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
