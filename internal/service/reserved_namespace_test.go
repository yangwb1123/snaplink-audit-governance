package service

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// registerReservedSchema registers a schema whose searchable field ("email")
// makes field+"__search_digest" a reserved on-disk namespace. AllowedFields
// stays empty unless explicitly passed: with an empty allow-list the only
// rule that can reject a suffixed payload key is the reserved-namespace
// check (REQ-1), not the pre-existing allow-list rule. A fresh SchemaID
// keeps the strictly-monotonic schema-version guard (per tenant+schema_id)
// out of the picture.
func registerReservedSchema(t *testing.T, svc *Service, schemaID string, allowed []string) {
	t.Helper()
	err := svc.RegisterSchema("test", domain.EventSchema{
		TenantID: "tenant-a", SchemaID: schemaID, Version: 1, EventType: "audit.event",
		Active: true, RequiredFields: []string{"resource"},
		AllowedFields:    allowed,
		SearchableFields: []string{"email"},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func reservedEvent(id string, at time.Time, schemaID string, payload map[string]any) domain.Event {
	event := testEvent(id, "op-reserved", at)
	event.SchemaID = schemaID
	event.SchemaVersion = 1
	event.Payload = payload
	return event
}

func snapshotAdminActionCount(t *testing.T, svc *Service) int {
	t.Helper()
	count := 0
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		count = len(data.AdminActions)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

func assertNothingPersisted(t *testing.T, svc *Service, eventID string, baselineAdmin int) {
	t.Helper()
	err := svc.Store.Read(func(data *store.Snapshot) error {
		key := store.EventKey("tenant-a", eventID)
		if _, ok := data.Events[key]; ok {
			t.Fatalf("rejected event %s must not be stored", eventID)
		}
		if _, ok := data.Receipts[key]; ok {
			t.Fatalf("rejected event %s must not produce a receipt", eventID)
		}
		if got := len(data.AdminActions); got != baselineAdmin {
			t.Fatalf("rejected ingest must not append admin actions: baseline=%d got=%d", baselineAdmin, got)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestIngestRejectsTopLevelSearchDigestKey is AC-1: a colliding top-level
// key on a searchable field is rejected with domain.ErrInvalid and nothing
// persists (no event, no receipt, no admin action). AllowedFields is empty
// so only the reserved-namespace check can reject the payload.
func TestIngestRejectsTopLevelSearchDigestKey(t *testing.T) {
	svc := testService(t, false)
	registerReservedSchema(t, svc, "audit.event.reserved", nil)
	at := time.Unix(1_700_000_010, 0).UTC()

	// Positive control: the same event without the colliding key ingests.
	ok := reservedEvent("reserved-ok-1", at, "audit.event.reserved", map[string]any{"resource": "invoice", "email": "alice@example.test"})
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, ok, domain.StatusLedgered); err != nil {
		t.Fatalf("positive control must ingest: %v", err)
	}

	// Colliding key: producer-planted top-level "email__search_digest".
	colliding := reservedEvent("reserved-collide-1", at, "audit.event.reserved", map[string]any{"resource": "invoice", "email": "alice@example.test", "email__search_digest": "producer-value"})
	_, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, colliding, domain.StatusLedgered)
	if !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("expected domain.ErrInvalid, got %v", err)
	}
	assertNothingPersisted(t, svc, "reserved-collide-1", snapshotAdminActionCount(t, svc))
}

// TestIngestRejectsReservedKeyEvenWhenAllowed is AC-2: the rejection fires
// even when the schema's AllowedFields explicitly lists the colliding key —
// the reserved namespace is not user-allocatable. The pre-existing
// allow-list rule would accept the key, so only the reserved-namespace
// check can reject.
func TestIngestRejectsReservedKeyEvenWhenAllowed(t *testing.T) {
	svc := testService(t, false)
	registerReservedSchema(t, svc, "audit.event.reserved.allow", []string{"resource", "email", "email__search_digest"})
	at := time.Unix(1_700_000_010, 0).UTC()

	colliding := reservedEvent("reserved-collide-allow-1", at, "audit.event.reserved.allow", map[string]any{"resource": "invoice", "email": "alice@example.test", "email__search_digest": "producer-value"})
	_, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, colliding, domain.StatusLedgered)
	if !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("expected domain.ErrInvalid even when the key is allowed, got %v", err)
	}
	assertNothingPersisted(t, svc, "reserved-collide-allow-1", snapshotAdminActionCount(t, svc))
}

// TestVerifyIntegrityUnchangedAfterRejectedCollision is AC-3.1: a rejected
// event never enters the ledger, so VerifyIntegrity on the tenant reports
// Valid with zero errors and an EventCount at the pre-attempt baseline.
func TestVerifyIntegrityUnchangedAfterRejectedCollision(t *testing.T) {
	svc := testService(t, false)
	registerReservedSchema(t, svc, "audit.event.reserved", nil)
	at := time.Unix(1_700_000_010, 0).UTC()

	colliding := reservedEvent("reserved-collide-int-1", at, "audit.event.reserved", map[string]any{"resource": "invoice", "email": "alice@example.test", "email__search_digest": "producer-value"})
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, colliding, domain.StatusLedgered); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("expected domain.ErrInvalid, got %v", err)
	}

	result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid || len(result.Errors) != 0 || result.EventCount != 0 {
		t.Fatalf("rejected event must leave integrity untouched: %+v", result)
	}
}

// TestVerifyIntegrityReportsLegacyCollisionDistinctly is AC-3.2: a legacy
// event at rest with a producer-planted top-level digest key and a
// SourceDigest computed over the payload including it is reported invalid
// with an explicit reserved-namespace error — never the generic
// "content digest mismatch" string. The event is seeded directly into the
// store (bypassing ingest, mirroring seedLegacyEvent): that is the exact
// shape a pre-fix deployment persisted.
func TestVerifyIntegrityReportsLegacyCollisionDistinctly(t *testing.T) {
	svc := testService(t, false)
	registerReservedSchema(t, svc, "audit.event.reserved", nil)
	at := time.Unix(1_700_000_010, 0).UTC()

	event := testEvent("reserved-legacy-collide-1", "op-legacy", at)
	event.TenantID = "tenant-a"
	event.SchemaID = "audit.event.reserved"
	event.SchemaVersion = 1
	event.Payload = map[string]any{"resource": "invoice", "email": "alice@example.test", "email__search_digest": "producer-value"}
	event.StreamID = "tenant-a:aggregate:invoice:inv-legacy-collide"
	event.Sequence = 1
	event.PrevHash = ""
	// SourceDigest authenticates the pre-protection payload exactly as the
	// producer sent it — including the colliding key (E3): the digest a
	// pre-fix ingest would have stored.
	contentDigest, err := domain.EventContentDigest(event)
	if err != nil {
		t.Fatal(err)
	}
	event.SourceDigest = contentDigest
	hash, err := svc.eventHash(event)
	if err != nil {
		t.Fatal(err)
	}
	event.Hash = hash
	key := store.EventKey("tenant-a", event.EventID)
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Events[key] = event
		streamKey := store.StreamKey("tenant-a", event.StreamID)
		data.Streams[streamKey] = store.StreamState{TenantID: "tenant-a", StreamID: event.StreamID, NextSequence: 2, HeadHash: hash}
		data.Receipts[key] = domain.EventReceipt{EventID: event.EventID, TenantID: "tenant-a", Status: domain.StatusIndexed, AcceptedAt: event.OccurredAt, LedgeredAt: event.OccurredAt, StreamID: event.StreamID, Sequence: 1, Hash: hash}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid {
		t.Fatal("legacy collision event must be reported invalid")
	}
	if result.EventCount != 1 {
		t.Fatalf("expected exactly the seeded event, got %d", result.EventCount)
	}
	foundDistinct := false
	for _, entry := range result.Errors {
		if strings.Contains(entry, "search_digest") || strings.Contains(entry, "reserved") {
			foundDistinct = true
		}
		if strings.Contains(entry, "content digest mismatch") {
			t.Fatalf("legacy collision must not surface the generic mismatch string: %s", entry)
		}
	}
	if !foundDistinct {
		t.Fatalf("expected a distinct reserved-namespace error, got: %v", result.Errors)
	}
}

// TestVerifyIntegrityTamperWithStoredDigestKeyReportsNeutralWording is the
// F-1 regression pin: genuine tampering of a healthy searchable-field event
// (stored plaintext modified, the genuine server-written email__search_digest
// left in place) must NOT be misreported as a legacy collision that "cannot
// be reconstructed" — reconstruction succeeded (that is precisely how the
// mismatch was detected). The diagnostic stays reserved-namespace-attributed
// and neutral, and never contains the generic "content digest mismatch"
// substring either (AC-3.2 pin).
func TestVerifyIntegrityTamperWithStoredDigestKeyReportsNeutralWording(t *testing.T) {
	svc := testService(t, false)
	registerReservedSchema(t, svc, "audit.event.reserved", nil)
	at := time.Unix(1_700_000_010, 0).UTC()

	// Healthy ingest: the store now holds the genuine server-written
	// email__search_digest over the original plaintext.
	healthy := reservedEvent("reserved-tamper-1", at, "audit.event.reserved", map[string]any{"resource": "invoice", "email": "alice@example.test"})
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, healthy, domain.StatusLedgered); err != nil {
		t.Fatalf("healthy event must ingest: %v", err)
	}

	// U4 tamper: modify the stored plaintext of the searchable field, leaving
	// the genuine digest key in place (reconstructAndDerive deletes the key
	// and re-derives over the tampered payload → mismatch).
	key := store.EventKey("tenant-a", healthy.EventID)
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		ev := data.Events[key]
		ev.Payload["email"] = "mallory@example.test"
		data.Events[key] = ev
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid {
		t.Fatal("tampered event must be reported invalid")
	}
	found := false
	for _, entry := range result.Errors {
		if strings.Contains(entry, "cannot be reconstructed") {
			t.Fatalf("tamper must not be misattributed as a legacy collision: %s", entry)
		}
		if strings.Contains(entry, "content digest mismatch") {
			t.Fatalf("tamper with stored digest key must not surface the generic mismatch string: %s", entry)
		}
		if strings.Contains(entry, "reserved") || strings.Contains(entry, "search_digest") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a reserved-namespace-attributed error, got: %v", result.Errors)
	}
}

// TestVerifyIntegrityLegacyCollisionDeterministicKey pins F-4: with multiple
// top-level colliding keys the reported key is the lexicographically first
// one — deterministic across runs (raw Go map iteration order is random).
func TestVerifyIntegrityLegacyCollisionDeterministicKey(t *testing.T) {
	svc := testService(t, false)
	registerReservedSchema(t, svc, "audit.event.reserved", nil)
	at := time.Unix(1_700_000_010, 0).UTC()

	event := testEvent("reserved-legacy-multi-1", "op-legacy", at)
	event.TenantID = "tenant-a"
	event.SchemaID = "audit.event.reserved"
	event.SchemaVersion = 1
	event.Payload = map[string]any{"resource": "invoice", "email": "alice@example.test", "zzz__search_digest": "producer-1", "aaa__search_digest": "producer-2", "email__search_digest": "producer-3"}
	event.StreamID = "tenant-a:aggregate:invoice:inv-legacy-multi"
	event.Sequence = 1
	event.PrevHash = ""
	contentDigest, err := domain.EventContentDigest(event)
	if err != nil {
		t.Fatal(err)
	}
	event.SourceDigest = contentDigest
	hash, err := svc.eventHash(event)
	if err != nil {
		t.Fatal(err)
	}
	event.Hash = hash
	key := store.EventKey("tenant-a", event.EventID)
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Events[key] = event
		streamKey := store.StreamKey("tenant-a", event.StreamID)
		data.Streams[streamKey] = store.StreamState{TenantID: "tenant-a", StreamID: event.StreamID, NextSequence: 2, HeadHash: hash}
		data.Receipts[key] = domain.EventReceipt{EventID: event.EventID, TenantID: "tenant-a", Status: domain.StatusIndexed, AcceptedAt: event.OccurredAt, LedgeredAt: event.OccurredAt, StreamID: event.StreamID, Sequence: 1, Hash: hash}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid {
		t.Fatal("multi-collision event must be reported invalid")
	}
	reported := 0
	for _, entry := range result.Errors {
		if strings.Contains(entry, "aaa__search_digest") {
			reported++
		}
		if strings.Contains(entry, "zzz__search_digest") {
			t.Fatalf("reported key must be the lexicographically first collision, got zzz__search_digest: %s", entry)
		}
	}
	if reported == 0 {
		t.Fatalf("expected the deterministic first collision key in the error, got: %v", result.Errors)
	}
}

// TestIngestAllowsNestedSearchDigestKeys pins REQ-1's top-level-only scope:
// nested *__search_digest keys remain legal at ingest (stripped only at
// read/export boundaries), so TestExportJSONLStripsSearchDigests stays
// green.
func TestIngestAllowsNestedSearchDigestKeys(t *testing.T) {
	svc := testService(t, false)
	registerReservedSchema(t, svc, "audit.event.reserved", nil)
	at := time.Unix(1_700_000_010, 0).UTC()

	event := reservedEvent("reserved-nested-1", at, "audit.event.reserved", map[string]any{"resource": "invoice", "email": "alice@example.test", "nested": map[string]any{"note__search_digest": "producer-value"}})
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err != nil {
		t.Fatalf("nested digest-like keys must remain legal at ingest: %v", err)
	}
	if receipt.EventID != "reserved-nested-1" {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}
}
