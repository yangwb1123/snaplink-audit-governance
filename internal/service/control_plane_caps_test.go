package service

// Control-plane resource caps at the service write boundary. normalizeSource
// and RegisterSchema are the only writers of Snapshot.Sources / Snapshot.Schemas
// reachable from the API, so the caps are enforced there and not in the HTTP
// handlers: every control-plane caller (REST, in-process bootstrap, tests)
// passes through these two functions, and the rejection happens before
// Store.Update — no Sources/Schemas row, no mutation and no admin action is
// written for an over-cap request.

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// testEmptyService is testService without the pre-registered source and schema:
// the state cmd/audit-api bootstrap actually starts from on an empty volume.
func testEmptyService(t *testing.T) *Service {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := New(st, Config{SegmentSize: 2, SigningSecret: "test-secret", Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant(testCtx, "test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		t.Fatal(err)
	}
	return svc
}

// TestNormalizeSourceRejectsOverCapClientIDs pins the boundary at exactly the
// cap and the rejection one entry above it, plus the at-cap sort behaviour that
// the persisted row order depends on.
func TestNormalizeSourceRejectsOverCapClientIDs(t *testing.T) {
	atCap, err := normalizeSource(domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM",
		AllowedClientIDs: reverseIDs(domain.MaxSourceClientIDs)})
	if err != nil {
		t.Fatalf("exactly %d allowed_client_ids must be accepted, got %v", domain.MaxSourceClientIDs, err)
	}
	if !sortAscending(atCap.AllowedClientIDs) {
		t.Fatal("accepted allow-list must still be sorted")
	}

	for _, tc := range []struct {
		name    string
		entries []string
		want    string
	}{
		{"arity", sequentialIDs(domain.MaxSourceClientIDs + 1), fmt.Sprintf("allowed_client_ids count %d, max %d", domain.MaxSourceClientIDs+1, domain.MaxSourceClientIDs)},
		{"entry bytes", []string{strings.Repeat("c", domain.MaxEnvelopeFieldBytes+1)}, fmt.Sprintf("allowed_client_ids[] is %d bytes, max %d", domain.MaxEnvelopeFieldBytes+1, domain.MaxEnvelopeFieldBytes)},
	} {
		_, err := normalizeSource(domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", AllowedClientIDs: tc.entries})
		if !errors.Is(err, domain.ErrInvalid) || !errors.Is(err, domain.ErrEnvelopeTooLarge) {
			t.Fatalf("%s: error=%v, want ErrInvalid and ErrEnvelopeTooLarge", tc.name, err)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: message=%q does not contain %q", tc.name, err.Error(), tc.want)
		}
	}
}

// TestAddSourceOverCapPersistsNothing proves the service boundary — not the
// HTTP layer — is what rejects, and that the store closure is never entered.
func TestAddSourceOverCapPersistsNothing(t *testing.T) {
	svc := testService(t, false)
	before := snapshotFacts(t, svc, "tenant-a")

	err := svc.AddSource(testCtx, "test", domain.SourceSystem{TenantID: "tenant-a", ID: "erp", Name: "ERP",
		AllowedClientIDs: sequentialIDs(domain.MaxSourceClientIDs + 1)})
	if !errors.Is(err, domain.ErrEnvelopeTooLarge) {
		t.Fatalf("AddSource over-cap error=%v, want ErrEnvelopeTooLarge", err)
	}
	assertNoSourceRow(t, svc, "tenant-a", "erp")
	assertFactsUnchanged(t, svc, "tenant-a", before)
}

func TestUpdateSourceOverCapPersistsNothing(t *testing.T) {
	svc := testService(t, false)
	stored := mustListSources(t, svc, "tenant-a")["crm"]
	before := snapshotFacts(t, svc, "tenant-a")

	_, err := svc.UpdateSource(testCtx, "test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM renamed", Active: true,
		AllowedClientIDs: []string{strings.Repeat("c", domain.MaxEnvelopeFieldBytes+1)}})
	if !errors.Is(err, domain.ErrEnvelopeTooLarge) {
		t.Fatalf("UpdateSource over-cap error=%v, want ErrEnvelopeTooLarge", err)
	}
	unchanged := mustListSources(t, svc, "tenant-a")["crm"]
	if unchanged.Name != stored.Name || len(unchanged.AllowedClientIDs) != len(stored.AllowedClientIDs) {
		t.Fatalf("rejected update mutated the stored row: before=%+v after=%+v", stored, unchanged)
	}
	assertFactsUnchanged(t, svc, "tenant-a", before)
}

// TestRegisterSchemaOverCapPersistsNothing covers each of the four field lists,
// the exact-cap acceptance, and the verbatim (caller-order) storage that
// ValidateSchemaCaps must not disturb.
func TestRegisterSchemaOverCapPersistsNothing(t *testing.T) {
	svc := testService(t, false)
	before := snapshotFacts(t, svc, "tenant-a")

	for _, list := range []struct {
		name  string
		apply func(*domain.EventSchema, []string)
	}{
		{"required_fields", func(s *domain.EventSchema, v []string) { s.RequiredFields = v }},
		{"allowed_fields", func(s *domain.EventSchema, v []string) { s.AllowedFields = v }},
		{"encrypted_fields", func(s *domain.EventSchema, v []string) { s.EncryptedFields = v }},
		{"searchable_fields", func(s *domain.EventSchema, v []string) { s.SearchableFields = v }},
	} {
		schema := domain.EventSchema{TenantID: "tenant-a", SchemaID: "capped." + list.name, Version: 1, EventType: "audit.event", Active: true}
		list.apply(&schema, sequentialIDs(domain.MaxSchemaFields+1))
		err := svc.RegisterSchema(testCtx, "test", schema)
		if !errors.Is(err, domain.ErrEnvelopeTooLarge) {
			t.Fatalf("%s: RegisterSchema over-cap error=%v, want ErrEnvelopeTooLarge", list.name, err)
		}
		if !strings.Contains(err.Error(), fmt.Sprintf("%s count %d, max %d", list.name, domain.MaxSchemaFields+1, domain.MaxSchemaFields)) {
			t.Fatalf("%s: message=%q does not name the violated list", list.name, err.Error())
		}
		if _, exists := mustSchemaRow(t, svc, "tenant-a", "capped."+list.name, 1); exists {
			t.Fatalf("%s: over-cap registration wrote a schema key", list.name)
		}
		assertFactsUnchanged(t, svc, "tenant-a", before)
	}

	atCap := domain.EventSchema{TenantID: "tenant-a", SchemaID: "capped.at-cap", Version: 1, EventType: "audit.event", Active: true,
		RequiredFields: reverseIDs(domain.MaxSchemaFields), AllowedFields: sequentialIDs(domain.MaxSchemaFields)}
	if err := svc.RegisterSchema(testCtx, "test", atCap); err != nil {
		t.Fatalf("schema at exactly %d entries must be accepted, got %v", domain.MaxSchemaFields, err)
	}
	stored, exists := mustSchemaRow(t, svc, "tenant-a", "capped.at-cap", 1)
	if !exists {
		t.Fatal("at-cap schema was not persisted")
	}
	if !sortDescending(stored.RequiredFields) {
		t.Fatalf("stored required_fields must keep the caller's order, got %v", stored.RequiredFields[:3])
	}
	if !sortAscending(stored.AllowedFields) {
		t.Fatal("stored allowed_fields must keep the caller's order")
	}
}

// TestBootstrapInProcessCallerStillAccepted keeps the shipped in-process
// bootstrap shape (cmd/audit-api main.go bootstrap: no allow-list, no field
// lists) accepted on the empty state it actually runs against, so the new gate
// cannot break server start-up.
func TestBootstrapInProcessCallerStillAccepted(t *testing.T) {
	svc := testEmptyService(t)
	schema := domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Classification: "internal", Active: true}
	if err := svc.RegisterSchema(testCtx, "bootstrap", schema); err != nil {
		t.Fatalf("bootstrap schema shape must stay accepted, got %v", err)
	}
	source := domain.SourceSystem{TenantID: "tenant-a", ID: "demo", Name: "Local development source", Active: true}
	if err := svc.AddSource(testCtx, "bootstrap", source); err != nil {
		t.Fatalf("bootstrap source shape must stay accepted, got %v", err)
	}
	// The bootstrap source authorizes exactly one client, which keeps working.
	if _, err := svc.Ingest(testCtx, "tenant-a", domain.IngestPrincipal{ClientID: "demo"}, domain.Event{
		EventID: "bootstrap-1", SourceSystem: "demo", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1,
		OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "write", Outcome: "success",
		DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "bootstrap-idem-1",
		Payload: map[string]any{"value": 1},
	}, ""); err != nil {
		t.Fatalf("ingest through the bootstrap source must still work, got %v", err)
	}
}

// --- A7: non-cap regressions stay pinned ------------------------------------

// TestEmptyAllowedClientIDsMeansClientIDEqualsID pins the meaning an empty
// allow-list keeps under the cap change.
func TestEmptyAllowedClientIDsMeansClientIDEqualsID(t *testing.T) {
	source := domain.SourceSystem{ID: "crm"}
	if !source.AllowsClient("crm") {
		t.Fatal("empty allow-list must authorize client_id == id")
	}
	if source.AllowsClient("other") || source.AllowsClient("") {
		t.Fatal("empty allow-list must authorize nothing else")
	}
}

// TestSchemaVersionSupersetAndRollbackUnchanged pins that the cap check did not
// become a second, weaker version rule — the version guards keep their messages
// and their precedence, and encrypted_fields/searchable_fields stay
// unconstrained across versions (a shrinking encrypted_fields is still legal,
// so no reader can infer a new superset rule from this change).
func TestSchemaVersionSupersetAndRollbackUnchanged(t *testing.T) {
	svc := testService(t, false)
	v1 := domain.EventSchema{TenantID: "tenant-a", SchemaID: "versioned", Version: 1, EventType: "audit.event", Active: true,
		RequiredFields: []string{"resource", "value"}, AllowedFields: []string{"resource", "value", "extra"},
		EncryptedFields: []string{"secret_a", "secret_b"}, SearchableFields: []string{"resource"}}
	if err := svc.RegisterSchema(testCtx, "test", v1); err != nil {
		t.Fatal(err)
	}

	dropRequired := v1
	dropRequired.Version = 2
	dropRequired.RequiredFields = []string{"resource"}
	err := svc.RegisterSchema(testCtx, "test", dropRequired)
	if !errors.Is(err, domain.ErrInvalid) || !strings.Contains(err.Error(), "schema version removes required field value") {
		t.Fatalf("dropping a required field error=%v, want the superset rejection", err)
	}

	dropAllowed := v1
	dropAllowed.Version = 2
	dropAllowed.AllowedFields = []string{"resource", "value"}
	if err := svc.RegisterSchema(testCtx, "test", dropAllowed); !errors.Is(err, domain.ErrInvalid) ||
		!strings.Contains(err.Error(), "schema version removes allowed field extra") {
		t.Fatalf("dropping an allowed field error=%v, want the superset rejection", err)
	}

	rollback := v1
	rollback.Version = 0
	rollback.Version = 2
	lowVersion := v1
	lowVersion.Version = 1
	if err := svc.RegisterSchema(testCtx, "test", lowVersion); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("re-registering the highest version error=%v, want ErrConflict", err)
	}

	// encrypted_fields may shrink across versions; only required/allowed are
	// superset-guarded.
	shrinkEncrypted := v1
	shrinkEncrypted.Version = 2
	shrinkEncrypted.EncryptedFields = []string{"secret_a"}
	shrinkEncrypted.SearchableFields = nil
	if err := svc.RegisterSchema(testCtx, "test", shrinkEncrypted); err != nil {
		t.Fatalf("shrinking encrypted_fields must stay accepted, got %v", err)
	}

	below := shrinkEncrypted
	below.Version = 1
	if err := svc.RegisterSchema(testCtx, "test", below); !errors.Is(err, domain.ErrInvalid) ||
		!strings.Contains(err.Error(), "version rollback is not allowed") {
		t.Fatalf("below-highest version error=%v, want the rollback rejection", err)
	}
}

// TestSchemaListsAtCapStillEnforceSupersetRules proves the cap check did not
// short-circuit the in-closure version guards: a v2 at the arity cap that drops
// a v1 required field is still rejected for the superset reason.
func TestSchemaListsAtCapStillEnforceSupersetRules(t *testing.T) {
	svc := testService(t, false)
	v1 := domain.EventSchema{TenantID: "tenant-a", SchemaID: "capped-superset", Version: 1, EventType: "audit.event", Active: true,
		RequiredFields: []string{"must_stay"}}
	if err := svc.RegisterSchema(testCtx, "test", v1); err != nil {
		t.Fatal(err)
	}
	v2 := domain.EventSchema{TenantID: "tenant-a", SchemaID: "capped-superset", Version: 2, EventType: "audit.event", Active: true,
		RequiredFields: sequentialIDs(domain.MaxSchemaFields)}
	err := svc.RegisterSchema(testCtx, "test", v2)
	if !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("at-cap v2 error=%v, want the superset rejection", err)
	}
	if !strings.Contains(err.Error(), "schema version removes required field") {
		t.Fatalf("at-cap v2 message=%q must name the superset reason", err.Error())
	}
	if _, exists := mustSchemaRow(t, svc, "tenant-a", "capped-superset", 2); exists {
		t.Fatal("rejected at-cap v2 wrote a schema key")
	}
}

// --- A9: scope guard — read paths and legacy data are untouched --------------

// TestLegacyOverCapSourceRowStillAuthorizes pins D7: the caps are a
// write-boundary rule, not a migration. A row that predates the cap (injected
// straight into the snapshot here) keeps authorizing and keeps being served.
func TestLegacyOverCapSourceRowStillAuthorizes(t *testing.T) {
	svc := testService(t, false)
	legacy := domain.SourceSystem{TenantID: "tenant-a", ID: "legacy", Name: "Legacy", Active: true,
		AllowedClientIDs: sequentialIDs(domain.MaxSourceClientIDs + 64)}
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
		data.Sources[store.SourceKey("tenant-a", "legacy")] = legacy
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := svc.Store.ReadControl(testCtx, func(data *store.Snapshot) error {
		if !sourceAccessAllowed(data, "tenant-a", "legacy", legacy.AllowedClientIDs[len(legacy.AllowedClientIDs)-1]) {
			t.Fatal("legacy over-cap row must still authorize its last-listed client")
		}
		tenant, err := resolveIngestTenantFromData(data, "", legacy.AllowedClientIDs[0], "legacy")
		if err != nil || tenant != "tenant-a" {
			t.Fatalf("platform sweep over a legacy over-cap row: tenant=%q err=%v", tenant, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	served, ok := mustListSources(t, svc, "tenant-a")["legacy"]
	if !ok || len(served.AllowedClientIDs) != len(legacy.AllowedClientIDs) {
		t.Fatalf("ListSources must return the legacy row verbatim, got %d entries", len(served.AllowedClientIDs))
	}
}

// TestIngestValidationUnchangedByCaps pins that the per-event ingest path is
// not gated by the new constants: a stored schema whose lists exceed the cap
// still validates, encrypts, digests and reconstructs an event.
func TestIngestValidationUnchangedByCaps(t *testing.T) {
	svc := testService(t, false)
	overCap := domain.EventSchema{TenantID: "tenant-a", SchemaID: "legacy.schema", Version: 1, EventType: "audit.event",
		Active: true, Classification: "internal",
		RequiredFields:   []string{"resource"},
		AllowedFields:    append(append(sequentialIDs(domain.MaxSchemaFields+8), "resource", "value", "national_id"), "resource"),
		EncryptedFields:  append(sequentialIDs(domain.MaxSchemaFields+8), "national_id"),
		SearchableFields: append(sequentialIDs(domain.MaxSchemaFields+8), "resource"),
	}
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
		data.Schemas[store.SchemaKey("tenant-a", "legacy.schema", 1)] = overCap
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	event := domain.Event{
		EventID: "caps-legacy-1", SourceSystem: "crm", EventType: "audit.event", SchemaID: "legacy.schema", SchemaVersion: 1,
		OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success",
		DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "caps-legacy-idem-1",
		Payload: map[string]any{"resource": "invoice", "value": 10, "national_id": "123456789"},
	}
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, "")
	if err != nil {
		t.Fatalf("ingest against a stored over-cap schema must still succeed, got %v", err)
	}
	if receipt.Status == "" {
		t.Fatal("ingest receipt is empty")
	}
	// reconstructEventForDigest and digestMatches walk the same over-cap
	// lists during verification, so integrity must stay intact too.
	result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "tester", "")
	if err != nil {
		t.Fatalf("VerifyIntegrity against a stored over-cap schema failed: %v", err)
	}
	if !result.Valid {
		t.Fatalf("integrity verification failed for an over-cap schema: %+v", result.Errors)
	}
}

// --- helpers ----------------------------------------------------------------

type snapshotState struct {
	sources      int
	schemas      int
	adminActions int
}

func snapshotFacts(t *testing.T, svc *Service, tenantID string) snapshotState {
	t.Helper()
	state := snapshotState{}
	if err := svc.Store.Read(testCtx, func(data *store.Snapshot) error {
		for _, source := range data.Sources {
			if source.TenantID == tenantID {
				state.sources++
			}
		}
		for _, schema := range data.Schemas {
			if schema.TenantID == tenantID {
				state.schemas++
			}
		}
		state.adminActions = len(data.AdminActions)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return state
}

// assertFactsUnchanged pins that a rejected control-plane write entered no
// store closure: no row was added and no admin action was appended.
func assertFactsUnchanged(t *testing.T, svc *Service, tenantID string, before snapshotState) {
	t.Helper()
	if after := snapshotFacts(t, svc, tenantID); after != before {
		t.Fatalf("rejected write touched the snapshot: before=%+v after=%+v", before, after)
	}
}

func assertNoSourceRow(t *testing.T, svc *Service, tenantID, sourceID string) {
	t.Helper()
	if err := svc.Store.Read(testCtx, func(data *store.Snapshot) error {
		if _, exists := data.Sources[store.SourceKey(tenantID, sourceID)]; exists {
			t.Fatalf("rejected registration persisted source %q", sourceID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func mustListSources(t *testing.T, svc *Service, tenantID string) map[string]domain.SourceSystem {
	t.Helper()
	sources, err := svc.ListSources(testCtx, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	indexed := make(map[string]domain.SourceSystem, len(sources))
	for _, source := range sources {
		indexed[source.ID] = source
	}
	return indexed
}

func mustSchemaRow(t *testing.T, svc *Service, tenantID, schemaID string, version int) (domain.EventSchema, bool) {
	t.Helper()
	var schema domain.EventSchema
	found := false
	if err := svc.Store.Read(testCtx, func(data *store.Snapshot) error {
		schema, found = data.Schemas[store.SchemaKey(tenantID, schemaID, version)]
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return schema, found
}

func sequentialIDs(n int) []string {
	entries := make([]string, n)
	for i := range entries {
		entries[i] = fmt.Sprintf("client-%06d", i)
	}
	return entries
}

func reverseIDs(n int) []string {
	entries := sequentialIDs(n)
	for left, right := 0, len(entries)-1; left < right; left, right = left+1, right-1 {
		entries[left], entries[right] = entries[right], entries[left]
	}
	return entries
}

func sortAscending(entries []string) bool {
	for i := 1; i < len(entries); i++ {
		if entries[i-1] > entries[i] {
			return false
		}
	}
	return true
}

func sortDescending(entries []string) bool {
	for i := 1; i < len(entries); i++ {
		if entries[i-1] < entries[i] {
			return false
		}
	}
	return true
}
