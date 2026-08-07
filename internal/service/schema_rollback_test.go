package service

import (
	"errors"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

// T1 / AC-1: re-registering an existing lower version after a newer one
// exists is rejected with ErrInvalid (not ErrConflict) and leaves both the
// schema snapshot and the admin trail untouched.
func TestRegisterSchemaRejectsRollback(t *testing.T) {
	svc := testService(t, false) // tenant-a already has audit.event v1
	register := func(version int) error {
		return svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: version, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}})
	}
	if err := register(2); err != nil {
		t.Fatalf("forward registration v2 failed: %v", err)
	}
	actionsBefore, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	err = register(1)
	if !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("rollback re-register of existing v1 must return ErrInvalid, got %v", err)
	}
	if errors.Is(err, domain.ErrConflict) {
		t.Fatal("rollback re-register must not be reported as a duplicate conflict")
	}
	schemas, err := svc.ListSchemas("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(schemas) != 2 {
		t.Fatalf("rejected rollback must not mutate the snapshot, got %d schemas", len(schemas))
	}
	actionsAfter, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(actionsAfter) != len(actionsBefore) {
		t.Fatalf("rejected rollback must not append an admin action: before=%d after=%d", len(actionsBefore), len(actionsAfter))
	}
}

// T2 / AC-2: strict monotonicity with forward gaps allowed (v1 -> v3);
// backfill of a never-registered version below the highest registered
// version is rejected.
func TestRegisterSchemaAllowsForwardGap(t *testing.T) {
	svc := testService(t, false) // audit.event v1
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 3, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); err != nil {
		t.Fatalf("forward gap v1->v3 must be allowed, got %v", err)
	}
	err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}})
	if !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("backfill of never-registered v2 below max v3 must be rejected, got %v", err)
	}
}

// T3 / AC-3: after the guard, the rollback state (a version registered after
// a newer version exists) is unconstructible through the API. Ingest keeps
// exact-version semantics: a legitimately registered lower version still
// accepts events (FR-2 preserved) and an unregistered version fails with
// ErrSchemaNotFound.
func TestIngestRollbackShapeFails(t *testing.T) {
	svc := testService(t, false) // audit.event v1
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); err != nil {
		t.Fatal(err)
	}
	// The re-registration that would re-admit a shape the newer version
	// deliberately restricted must be impossible to construct.
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("rollback state must be unconstructible, got %v", err)
	}
	// Exact-version lookup: the legitimate v1 still accepts events even
	// though v2 exists (FR-2 preserved; no newest-version-only ingest rule).
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("rollback-shape-1", "op-rs", at), domain.StatusLedgered); err != nil {
		t.Fatalf("ingest under legitimate lower version must succeed: %v", err)
	}
	// A version that was never registered fails the version-exact lookup.
	missing := testEvent("rollback-shape-2", "op-rs", at)
	missing.SchemaVersion = 5
	if _, err := svc.Ingest("tenant-a", crmPrincipal, missing, domain.StatusLedgered); !errors.Is(err, domain.ErrSchemaNotFound) {
		t.Fatalf("ingest under unregistered version must fail with ErrSchemaNotFound, got %v", err)
	}
}

// T5: re-registering the exact highest version stays a duplicate conflict
// (409 replay-safe) and is not reclassified as a rollback.
func TestRegisterSchemaDuplicateMaxVersionStillConflicts(t *testing.T) {
	svc := testService(t, false) // audit.event v1
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); err != nil {
		t.Fatal(err)
	}
	err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}})
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("duplicate of highest version must remain ErrConflict, got %v", err)
	}
}

// T0: the guard is scoped per (tenant_id, schema_id) — a first registration
// of the same version in another tenant or under another schema_id is
// unaffected.
func TestRegisterSchemaRollbackGuardIsScoped(t *testing.T) {
	svc := testService(t, false) // tenant-a/audit.event v1
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "other.event", Version: 1, EventType: "other.event", Active: true}); err != nil {
		t.Fatalf("first registration of another schema_id must be unaffected: %v", err)
	}
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-b", Name: "Tenant B", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-b", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		t.Fatalf("first registration in another tenant must be unaffected: %v", err)
	}
}
