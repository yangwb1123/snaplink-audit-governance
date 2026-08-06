package service

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// largeSegmentService is testService with a SegmentSize large enough that a
// handful of events never seal a segment, isolating the per-event checks.
func largeSegmentService(t *testing.T) *Service {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := New(st, Config{SegmentSize: 1000, SigningSecret: "test-secret", Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true, EventsPerSecond: 1000, Burst: 1000}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); err != nil {
		t.Fatal(err)
	}
	return svc
}

// sensitiveTestService registers the v2 schema with EncryptedFields and
// SearchableFields used by the FR-3/F1/H-1 fixtures.
func sensitiveTestService(t *testing.T) *Service {
	t.Helper()
	svc := testService(t, false)
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value", "email", "amount"}, EncryptedFields: []string{"email", "amount"}, SearchableFields: []string{"email"}}); err != nil {
		t.Fatal(err)
	}
	return svc
}

func joinErrors(result IntegrityResult) string { return strings.Join(result.Errors, "\n") }

func ingestThree(t *testing.T, svc *Service) {
	t.Helper()
	base := time.Unix(1_700_000_010, 0).UTC()
	for _, event := range []domain.Event{
		testEvent("evt-1", "op-1", base),
		testEvent("evt-2", "op-1", base.Add(time.Second)),
		testEvent("evt-3", "op-1", base.Add(2*time.Second)),
	} {
		if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatal(err)
		}
	}
}

// TestVerifyIntegrityDetectsContentTampering is the FR-1 acceptance test: an
// attacker who mutates a payload, rewrites SourceDigest and recomputes the
// chain hashes with the short-circuit formula must be detected. At HEAD this
// exact attack returned Valid=true with no errors.
func TestVerifyIntegrityDetectsContentTampering(t *testing.T) {
	t.Run("attacker-hash tamper", func(t *testing.T) {
		svc := largeSegmentService(t)
		ingestThree(t, svc)
		if err := svc.Store.Update(func(data *store.Snapshot) error {
			key := store.EventKey("tenant-a", "evt-2")
			event := data.Events[key]
			event.Payload = map[string]any{"resource": "invoice", "value": 999}
			event.SourceDigest = "attacker-chosen-value"
			event.Hash, _ = svc.eventHash(event)
			third := data.Events[store.EventKey("tenant-a", "evt-3")]
			third.PrevHash = event.Hash
			third.Hash, _ = svc.eventHash(third)
			data.Events[key] = event
			data.Events[store.EventKey("tenant-a", "evt-3")] = third
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		result, err := svc.VerifyIntegrity("tenant-a", "")
		if err != nil {
			t.Fatal(err)
		}
		if result.Valid {
			t.Fatalf("tampered ledger must be invalid: %+v", result)
		}
		if !strings.Contains(joinErrors(result), "content digest mismatch") {
			t.Fatalf("expected content digest mismatch, got %+v", result.Errors)
		}
	})
	t.Run("stale digest tamper", func(t *testing.T) {
		svc := largeSegmentService(t)
		ingestThree(t, svc)
		// Payload-only tamper: SourceDigest and Hash are left stale.
		if err := svc.Store.Update(func(data *store.Snapshot) error {
			key := store.EventKey("tenant-a", "evt-2")
			event := data.Events[key]
			event.Payload = map[string]any{"resource": "invoice", "value": 999}
			data.Events[key] = event
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		result, err := svc.VerifyIntegrity("tenant-a", "")
		if err != nil {
			t.Fatal(err)
		}
		if result.Valid {
			t.Fatalf("tampered ledger must be invalid: %+v", result)
		}
		if !strings.Contains(joinErrors(result), "content digest mismatch") {
			t.Fatalf("expected content digest mismatch, got %+v", result.Errors)
		}
	})
}

// TestEventContentDigestMatchesIngestDigest is the FR-2 property: for honest
// events (plain and sensitive), reconstructing the pre-protection payload and
// deriving the content digest reproduces the stored SourceDigest, so old
// stores and sealed segments keep verifying and no migration is required.
func TestEventContentDigestMatchesIngestDigest(t *testing.T) {
	svc := sensitiveTestService(t)
	base := time.Unix(1_700_000_010, 0).UTC()
	plain := testEvent("evt-plain", "op-plain", base)
	plain.SchemaVersion = 1
	sensitive := testEvent("evt-sensitive", "op-sensitive", base.Add(time.Second))
	sensitive.SchemaVersion = 2
	sensitive.Payload["email"] = "alice@example.test"
	for _, event := range []domain.Event{plain, sensitive} {
		if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatal(err)
		}
	}
	schemas := map[string]domain.EventSchema{}
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		for key, schema := range data.Schemas {
			schemas[key] = schema
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, eventID := range []string{"evt-plain", "evt-sensitive"} {
		stored, err := svc.GetEvent("tenant-a", "test", eventID)
		if err != nil {
			t.Fatal(err)
		}
		derived, err := svc.reconstructAndDerive(stored, schemas)
		if err != nil {
			t.Fatal(err)
		}
		if derived != stored.SourceDigest {
			t.Fatalf("%s: derived digest %s != stored %s", eventID, derived, stored.SourceDigest)
		}
	}
}

// TestVerifyIntegrityAcceptsSensitiveEvents is the FR-3 acceptance test: an
// honest event with encrypted/searchable fields verifies, and a naive
// re-derivation on the stored post-protection payload must NOT match the
// stored digest (reconstruction is mandatory).
func TestVerifyIntegrityAcceptsSensitiveEvents(t *testing.T) {
	svc := sensitiveTestService(t)
	event := testEvent("evt-sensitive", "op-sensitive", time.Unix(1_700_000_010, 0).UTC())
	event.SchemaVersion = 2
	event.Payload["email"] = "alice@example.test"
	if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	stored, err := svc.GetEvent("tenant-a", "test", "evt-sensitive")
	if err != nil {
		t.Fatal(err)
	}
	naive, err := domain.EventContentDigest(stored)
	if err != nil {
		t.Fatal(err)
	}
	if naive == stored.SourceDigest {
		t.Fatalf("naive re-derivation must not match the stored digest (reconstruction is mandatory): %s", naive)
	}
	result, err := svc.VerifyIntegrity("tenant-a", "")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid || len(result.Errors) != 0 {
		t.Fatalf("honest sensitive events must verify: %+v", result)
	}
}

// TestDedupeConfirmsOnTamperedStoredContent is the FR-4 acceptance test:
// re-ingesting the original content of a tampered event must return
// ErrConflict instead of silently blessing the tampered state with
// Duplicate=true (the behavior at HEAD).
func TestDedupeConflictsOnTamperedStoredContent(t *testing.T) {
	svc := testService(t, false)
	base := time.Unix(1_700_000_010, 0).UTC()
	original := testEvent("evt-1", "op-1", base)
	if _, err := svc.Ingest("tenant-a", crmPrincipal, original, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	// Attacker tampers the stored payload but leaves SourceDigest stale.
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		key := store.EventKey("tenant-a", "evt-1")
		event := data.Events[key]
		event.Payload = map[string]any{"resource": "invoice", "value": 999}
		data.Events[key] = event
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest("tenant-a", crmPrincipal, original, domain.StatusLedgered); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("re-ingest of original content over tampered state must conflict, got %v", err)
	}
	// Honest duplicate detection is unchanged.
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		key := store.EventKey("tenant-a", "evt-1")
		event := data.Events[key]
		event.Payload = map[string]any{"resource": "invoice", "value": 10}
		data.Events[key] = event
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	receipt, err := svc.Ingest("tenant-a", crmPrincipal, original, domain.StatusLedgered)
	if err != nil || !receipt.Duplicate {
		t.Fatalf("honest duplicate must still be Duplicate=true: %+v %v", receipt, err)
	}
}

// TestVerifyIntegrityReportsKeyMismatch is failure mode F1: after key
// rotation, verification of sensitive events fails with a distinct
// "cannot decrypt field" error instead of silently passing.
func TestVerifyIntegrityReportsKeyMismatch(t *testing.T) {
	svc := sensitiveTestService(t)
	event := testEvent("evt-key", "op-key", time.Unix(1_700_000_010, 0).UTC())
	event.SchemaVersion = 2
	event.Payload["email"] = "alice@example.test"
	if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	svc.Config.EncryptionKey = "rotated-key"
	result, err := svc.VerifyIntegrity("tenant-a", "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid {
		t.Fatalf("key mismatch must fail verification: %+v", result)
	}
	if !strings.Contains(joinErrors(result), "cannot decrypt field") {
		t.Fatalf("expected cannot decrypt field error, got %+v", result.Errors)
	}
}

// TestVerifyIntegrityMissingSchemaAndDigest is failure modes F2 and F4: a
// deregistered schema version and an empty SourceDigest both fail closed with
// distinct errors.
func TestVerifyIntegrityMissingSchemaAndDigest(t *testing.T) {
	svc := testService(t, false)
	ingestThree(t, svc)
	// F4: an event without SourceDigest is unauthenticated and must fail closed.
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		key := store.EventKey("tenant-a", "evt-1")
		event := data.Events[key]
		event.SourceDigest = ""
		data.Events[key] = event
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := svc.VerifyIntegrity("tenant-a", "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid || !strings.Contains(joinErrors(result), "missing source_digest") {
		t.Fatalf("expected missing source_digest, got %+v", result)
	}
	// F2: the event's exact schema version is gone.
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		key := store.EventKey("tenant-a", "evt-1")
		event := data.Events[key]
		event.SourceDigest = "restored"
		event.SchemaVersion = 99
		data.Events[key] = event
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err = svc.VerifyIntegrity("tenant-a", "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid || !strings.Contains(joinErrors(result), "schema audit.event v99 not found") {
		t.Fatalf("expected schema not found, got %+v", result)
	}
}

// TestVerifyIntegrityDetectsChainTamper anchors the chain-check failure
// branches (hash mismatch / prev_hash mismatch), which had no behavioral
// coverage before this campaign.
func TestVerifyIntegrityDetectsChainTamper(t *testing.T) {
	t.Run("hash tamper", func(t *testing.T) {
		svc := testService(t, false)
		ingestThree(t, svc)
		if err := svc.Store.Update(func(data *store.Snapshot) error {
			key := store.EventKey("tenant-a", "evt-2")
			event := data.Events[key]
			event.Hash = strings.Repeat("0", len(event.Hash))
			data.Events[key] = event
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		result, err := svc.VerifyIntegrity("tenant-a", "")
		if err != nil {
			t.Fatal(err)
		}
		if result.Valid || !strings.Contains(joinErrors(result), "hash mismatch") {
			t.Fatalf("expected hash mismatch, got %+v", result)
		}
	})
	t.Run("prev_hash tamper", func(t *testing.T) {
		svc := testService(t, false)
		ingestThree(t, svc)
		if err := svc.Store.Update(func(data *store.Snapshot) error {
			key := store.EventKey("tenant-a", "evt-3")
			event := data.Events[key]
			event.PrevHash = strings.Repeat("0", len(event.PrevHash))
			data.Events[key] = event
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		result, err := svc.VerifyIntegrity("tenant-a", "")
		if err != nil {
			t.Fatal(err)
		}
		if result.Valid || !strings.Contains(joinErrors(result), "prev_hash mismatch") {
			t.Fatalf("expected prev_hash mismatch, got %+v", result)
		}
	})
	t.Run("sequence tamper", func(t *testing.T) {
		svc := testService(t, false)
		ingestThree(t, svc)
		// Chain binds sequence: a rewritten Sequence without a matching Hash
		// recomputation must surface as a hash mismatch.
		if err := svc.Store.Update(func(data *store.Snapshot) error {
			key := store.EventKey("tenant-a", "evt-2")
			event := data.Events[key]
			event.Sequence = 42
			data.Events[key] = event
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		result, err := svc.VerifyIntegrity("tenant-a", "")
		if err != nil {
			t.Fatal(err)
		}
		if result.Valid || !strings.Contains(joinErrors(result), "hash mismatch") {
			t.Fatalf("expected hash mismatch on sequence tamper, got %+v", result)
		}
	})
}

// TestVerifyIntegrityStreamFilter pins the scoped-verify behavior: a
// non-empty streamID checks only that stream, and cross-stream tampering is
// invisible to a scoped verify while the full verify still surfaces it.
func TestVerifyIntegrityStreamFilter(t *testing.T) {
	svc := testService(t, false)
	base := time.Unix(1_700_000_000, 0).UTC()
	first := testEvent("evt-a-1", "", base.Add(10*time.Second))
	first.OperationID = ""
	first.AggregateID = ""
	first.AggregateType = ""
	first.StreamID = "stream-a"
	second := testEvent("evt-b-1", "", base)
	second.OperationID = ""
	second.AggregateID = ""
	second.AggregateType = ""
	second.StreamID = "stream-b"
	for _, event := range []domain.Event{first, second} {
		if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatal(err)
		}
	}
	scoped, err := svc.VerifyIntegrity("tenant-a", "stream-a")
	if err != nil {
		t.Fatal(err)
	}
	if !scoped.Valid || scoped.EventCount != 1 || scoped.StreamID != "stream-a" {
		t.Fatalf("scoped verify failed: %+v", scoped)
	}
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		key := store.EventKey("tenant-a", "evt-b-1")
		event := data.Events[key]
		event.Payload = map[string]any{"resource": "invoice", "value": 424242}
		event.SourceDigest = "tampered"
		event.Hash = strings.Repeat("0", len(event.Hash))
		data.Events[key] = event
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	scoped, err = svc.VerifyIntegrity("tenant-a", "stream-a")
	if err != nil {
		t.Fatal(err)
	}
	if !scoped.Valid || scoped.EventCount != 1 {
		t.Fatalf("cross-stream tamper must not affect scoped verify: %+v", scoped)
	}
	full, err := svc.VerifyIntegrity("tenant-a", "")
	if err != nil {
		t.Fatal(err)
	}
	if full.Valid || len(full.Errors) == 0 {
		t.Fatalf("full verify must surface the tampered stream: %+v", full)
	}
}

// TestVerifyIntegrityConcurrentIngest stresses concurrent ingest + verify
// under -race. VerifyIntegrity reads one snapshot per call, so every observed
// ledger prefix is internally consistent and no silent false-invalid is
// allowed.
func TestVerifyIntegrityConcurrentIngest(t *testing.T) {
	svc := testService(t, false)
	base := time.Unix(1_700_000_010, 0).UTC()
	var mu sync.Mutex
	var ingestErr, verifyErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			event := testEvent(fmt.Sprintf("concurrent-%d", i), "op-concurrent", base.Add(time.Duration(i)*time.Second))
			if _, err := svc.Ingest("tenant-a", crmPrincipal, event, ""); err != nil {
				mu.Lock()
				ingestErr = err
				mu.Unlock()
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			result, err := svc.VerifyIntegrity("tenant-a", "")
			if err != nil {
				mu.Lock()
				verifyErr = err
				mu.Unlock()
				return
			}
			if !result.Valid {
				mu.Lock()
				verifyErr = fmt.Errorf("unexpected invalid result during concurrent ingest: %+v", result)
				mu.Unlock()
				return
			}
		}
	}()
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if ingestErr != nil {
		t.Fatalf("ingest failed: %v", ingestErr)
	}
	if verifyErr != nil {
		t.Fatalf("verify failed: %v", verifyErr)
	}
}

// TestVerifyIntegrityLargeIntSensitiveField is the H-1 coupling canary
// between this campaign and the canonical number-encoding campaign. Both the
// ingest-time digest path (clonePayload) and the verification-time
// reconstruction (DecryptJSON) currently round-trip numbers through float64,
// so a >2^53 int64 in an encrypted field verifies consistently. If the
// sibling campaign changes one side without the other, this test fails.
func TestVerifyIntegrityLargeIntSensitiveField(t *testing.T) {
	svc := sensitiveTestService(t)
	event := testEvent("evt-large", "op-large", time.Unix(1_700_000_010, 0).UTC())
	event.SchemaVersion = 2
	event.Payload["amount"] = int64(9007199254740993)
	if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	result, err := svc.VerifyIntegrity("tenant-a", "")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid || len(result.Errors) != 0 {
		t.Fatalf("large int64 sensitive field must verify consistently: %+v", result)
	}
}

// BenchmarkVerifyIntegrity measures full-ledger verification over a mixed
// plain/sensitive ledger with sealed segments (design failure mode F7).
func BenchmarkVerifyIntegrity(b *testing.B) {
	svc := benchService(b)
	if err := svc.RegisterSchema("bench", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value", "email"}, EncryptedFields: []string{"email"}, SearchableFields: []string{"email"}}); err != nil {
		b.Fatal(err)
	}
	base := time.Unix(1_700_000_010, 0).UTC()
	for i := 0; i < 1000; i++ {
		event := domain.Event{EventID: fmt.Sprintf("bench-verify-%d", i), SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: base.Add(time.Duration(i) * time.Second), Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: fmt.Sprintf("bench-verify-idem-%d", i), Payload: map[string]any{"resource": "invoice", "value": i}}
		if i%2 == 0 {
			event.SchemaVersion = 2
			event.Payload["email"] = fmt.Sprintf("user%d@example.test", i)
		}
		if _, err := svc.Ingest("tenant-a", crmPrincipal, event, ""); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := svc.VerifyIntegrity("tenant-a", "")
		if err != nil || !result.Valid {
			b.Fatalf("verify failed: %+v %v", result, err)
		}
	}
}
