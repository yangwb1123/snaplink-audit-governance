package service

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

// registerDigestSchemas registers a schema with a searchable plaintext field
// (email), a searchable+encrypted field (pii_email) and a searchable
// numeric field (amount) so every dual-format matching path is exercised.
func registerDigestSchemas(t *testing.T, svc *Service) {
	t.Helper()
	err := svc.RegisterSchema("test", domain.EventSchema{
		TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event",
		Active: true, RequiredFields: []string{"resource"},
		// v2 must keep v1's allowed fields ("value") — version-rollback guard.
		AllowedFields:    []string{"resource", "value", "email", "pii_email", "amount", "nested"},
		SearchableFields: []string{"email", "pii_email", "amount"},
		EncryptedFields:  []string{"pii_email"},
	})
	if err != nil {
		t.Fatal(err)
	}
}

// openServiceAt opens a service backed by a state file at path (used to
// simulate a process restart: numbers reload through the store's
// UseNumber snapshot decoding instead of collapsing into float64).
func openServiceAt(t *testing.T, path string) *Service {
	t.Helper()
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := New(st, Config{SegmentSize: 2, SigningSecret: "test-secret", Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func bootstrapDigestService(t *testing.T, svc *Service) {
	t.Helper()
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true, EventsPerSecond: 1000, Burst: 1000}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	registerDigestSchemas(t, svc)
}

// seedLegacyEvent writes an event with a format-v1 (unbound) search digest
// directly into the store, bypassing ingest — exactly the shape of events
// persisted by pre-change deployments. The event forms a self-consistent
// one-event stream (prev_hash "", sequence 1, SourceDigest over the
// pre-protection payload, chain hash) so VerifyIntegrity can authenticate it.
func seedLegacyEvent(t *testing.T, svc *Service, base domain.Event, payload map[string]any) {
	t.Helper()
	digest, err := security.SearchDigest(payload["email"], svc.Config.EncryptionKey)
	if err != nil {
		t.Fatal(err)
	}
	event := base
	event.TenantID = "tenant-a"
	// The legacy digest is a search digest for the digest schema (v2): the
	// v1 schema has no searchable fields, so reconstructAndDerive would not
	// remove the digest key and content verification would fail.
	event.SchemaVersion = 2
	event.Payload = map[string]any{}
	for key, value := range payload {
		event.Payload[key] = value
	}
	event.Payload["email__search_digest"] = digest
	event.StreamID = "tenant-a:aggregate:invoice:inv-legacy-" + event.EventID
	event.Sequence = 1
	// SourceDigest authenticates the pre-protection content (digest keys
	// removed), exactly like reconstructAndDerive computes it.
	pre := map[string]any{}
	for key, value := range event.Payload {
		if !strings.HasSuffix(key, "__search_digest") {
			pre[key] = value
		}
	}
	contentEvent := event
	contentEvent.Payload = pre
	contentDigest, err := domain.EventContentDigest(contentEvent)
	if err != nil {
		t.Fatal(err)
	}
	event.SourceDigest = contentDigest
	hash, err := svc.eventHash(event)
	if err != nil {
		t.Fatal(err)
	}
	event.Hash = hash
	event.PrevHash = ""
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
}

func hasDigestKey(value any) bool {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if strings.HasSuffix(key, "__search_digest") || hasDigestKey(child) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if hasDigestKey(child) {
				return true
			}
		}
	}
	return false
}

func digestQuery(base domain.Query, field, digest string) domain.Query {
	query := base
	query.PayloadField = field
	query.PayloadDigest = digest
	return query
}

// TestDualFormatSearchDigestMatching is AC-3: search-by-digest end-to-end
// still matches events ingested under the old unbound digest format, and
// the bound format matches across the full 4-way (stored, query) format
// matrix. The same plaintext in another tenant never matches.
func TestDualFormatSearchDigestMatching(t *testing.T) {
	svc := testService(t, false)
	registerDigestSchemas(t, svc)
	at := time.Unix(1_700_000_010, 0).UTC()
	base := domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_100, 0).UTC(), PageSize: 100}

	// Legacy event: format-v1 unbound digest seeded directly into the store.
	seedLegacyEvent(t, svc, testEvent("legacy-1", "op-legacy", at), map[string]any{"resource": "invoice", "email": "alice@example.test"})

	// New event: format-v2 bound digest produced by the current ingest path.
	event := testEvent("new-1", "op-new", at)
	event.SchemaVersion = 2
	event.Payload = map[string]any{"resource": "invoice", "email": "alice@example.test"}
	if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}

	unbound, err := security.SearchDigest("alice@example.test", svc.Config.EncryptionKey)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := security.SearchDigestBound("alice@example.test", svc.Config.EncryptionKey, "tenant-a", "email")
	if err != nil {
		t.Fatal(err)
	}
	wrongUnbound, err := security.SearchDigest("mallory@example.test", svc.Config.EncryptionKey)
	if err != nil {
		t.Fatal(err)
	}
	wrongBound, err := security.SearchDigestBound("mallory@example.test", svc.Config.EncryptionKey, "tenant-a", "email")
	if err != nil {
		t.Fatal(err)
	}
	// A bound digest for another tenant must never match (boundary D), even
	// though the plaintext is identical.
	foreignBound, err := security.SearchDigestBound("alice@example.test", svc.Config.EncryptionKey, "tenant-b", "email")
	if err != nil {
		t.Fatal(err)
	}

	matrix := []struct {
		name        string
		queryDigest string
	}{
		{"v1 query vs v1 stored", unbound},
		{"v2 query vs v1 stored", bound},
		{"v1 query vs v2 stored", unbound},
		{"v2 query vs v2 stored", bound},
	}
	for _, tc := range matrix {
		result, err := svc.QueryEvents("tenant-a", "test", digestQuery(base, "email", tc.queryDigest))
		if err != nil {
			t.Fatal(err)
		}
		if result.Count != 2 {
			t.Fatalf("%s: expected both legacy and new events to match, got %d", tc.name, result.Count)
		}
		// A wrong digest in the same format must not match either event.
		wrong := wrongUnbound
		if security.IsBoundSearchDigest(tc.queryDigest) {
			wrong = wrongBound
		}
		negative, err := svc.QueryEvents("tenant-a", "test", digestQuery(base, "email", wrong))
		if err != nil {
			t.Fatal(err)
		}
		if negative.Count != 0 {
			t.Fatalf("%s: wrong digest must not match, got %d events", tc.name, negative.Count)
		}
	}

	// Cross-tenant bound digest never matches the legacy event either
	// (cross-format re-derivation binds tenant-a, so tenant-b's digest
	// cannot collide).
	foreign, err := svc.QueryEvents("tenant-a", "test", digestQuery(base, "email", foreignBound))
	if err != nil {
		t.Fatal(err)
	}
	if foreign.Count != 0 {
		t.Fatalf("foreign-tenant bound digest must never match: got %d events", foreign.Count)
	}

	// Re-ingesting the legacy event's original content is still detected as
	// a duplicate: reconstructAndDerive deletes digest keys by schema name,
	// so the v1 stored digest does not perturb idempotency.
	replay := testEvent("legacy-1", "op-legacy", at)
	replay.SchemaVersion = 2
	replay.Payload = map[string]any{"resource": "invoice", "email": "alice@example.test"}
	receipt, err := svc.Ingest("tenant-a", crmPrincipal, replay, domain.StatusLedgered)
	if err != nil || !receipt.Duplicate {
		t.Fatalf("legacy event re-ingest must dedupe: %+v %v", receipt, err)
	}
}

// TestEncryptedSearchableFieldLimitsCrossFormatMatching pins the accepted
// limitation: a searchable field that is also encrypted has no stored
// plaintext, so cross-format matching fails closed (same-format only).
func TestEncryptedSearchableFieldLimitsCrossFormatMatching(t *testing.T) {
	svc := testService(t, false)
	registerDigestSchemas(t, svc)
	at := time.Unix(1_700_000_010, 0).UTC()
	base := domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_100, 0).UTC(), PageSize: 100}

	event := testEvent("enc-1", "op-enc", at)
	event.SchemaVersion = 2
	event.Payload = map[string]any{"resource": "invoice", "pii_email": "alice@example.test"}
	if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}

	unbound, err := security.SearchDigest("alice@example.test", svc.Config.EncryptionKey)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := security.SearchDigestBound("alice@example.test", svc.Config.EncryptionKey, "tenant-a", "pii_email")
	if err != nil {
		t.Fatal(err)
	}
	sameFormat, err := svc.QueryEvents("tenant-a", "test", digestQuery(base, "pii_email", bound))
	if err != nil || sameFormat.Count != 1 {
		t.Fatalf("same-format bound query must match the encrypted field: count=%d err=%v", sameFormat.Count, err)
	}
	crossFormat, err := svc.QueryEvents("tenant-a", "test", digestQuery(base, "pii_email", unbound))
	if err != nil || crossFormat.Count != 0 {
		t.Fatalf("cross-format query on an encrypted field must fail closed: count=%d err=%v", crossFormat.Count, err)
	}
}

// TestCrossFormatDigestLargeIntParityAfterRestart pins the number-encoding
// invariant: the cross-format fallback re-derives from the stored plaintext,
// which after a restart is a json.Number. Deriving through float64 would
// round int64 values > 2^53 and break the match.
func TestCrossFormatDigestLargeIntParityAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	svc := openServiceAt(t, path)
	bootstrapDigestService(t, svc)
	at := time.Unix(1_700_000_010, 0).UTC()

	big := int64(9_007_199_254_740_993) // 2^53 + 1; exact digits must survive
	event := testEvent("big-1", "op-big", at)
	event.SchemaVersion = 2
	event.Payload = map[string]any{"resource": "invoice", "amount": big}
	if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}

	// Digests computed from the original int64 (exact digits).
	unbound, err := security.SearchDigest(big, svc.Config.EncryptionKey)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := security.SearchDigestBound(big, svc.Config.EncryptionKey, "tenant-a", "amount")
	if err != nil {
		t.Fatal(err)
	}
	wrong, err := security.SearchDigest(int64(9_007_199_254_740_992), svc.Config.EncryptionKey)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate a restart: the store reloads the snapshot, so the stored
	// plaintext is a json.Number, not the original int64.
	reloaded := openServiceAt(t, path)
	base := domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_100, 0).UTC(), PageSize: 100}
	for _, tc := range []struct {
		name        string
		queryDigest string
	}{
		{"v1 query after restart", unbound},
		{"v2 query after restart", bound},
	} {
		result, err := reloaded.QueryEvents("tenant-a", "test", digestQuery(base, "amount", tc.queryDigest))
		if err != nil {
			t.Fatal(err)
		}
		if result.Count != 1 {
			t.Fatalf("%s: exact-digit parity lost after restart: count=%d", tc.name, result.Count)
		}
	}
	negative, err := reloaded.QueryEvents("tenant-a", "test", digestQuery(base, "amount", wrong))
	if err != nil || negative.Count != 0 {
		t.Fatalf("wrong digest must not match after restart: count=%d err=%v", negative.Count, err)
	}
}

// TestVerifyIntegrityMixedDigestFormats is AC-4: integrity verification
// still passes for events ingested with the new bound digest format, and
// for a legacy v1-format event stored side by side.
func TestVerifyIntegrityMixedDigestFormats(t *testing.T) {
	svc := testService(t, false)
	registerDigestSchemas(t, svc)
	at := time.Unix(1_700_000_010, 0).UTC()

	// New-format events through the ingest path: searchable-only and
	// encrypted+searchable.
	first := testEvent("v2-plain-1", "op-mixed", at)
	first.SchemaVersion = 2
	first.Payload = map[string]any{"resource": "invoice", "email": "alice@example.test"}
	if _, err := svc.Ingest("tenant-a", crmPrincipal, first, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	second := testEvent("v2-enc-1", "op-mixed", at.Add(time.Second))
	second.SchemaVersion = 2
	second.Payload = map[string]any{"resource": "invoice", "pii_email": "bob@example.test"}
	if _, err := svc.Ingest("tenant-a", crmPrincipal, second, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}

	// Legacy-format event seeded directly on its own stream.
	seedLegacyEvent(t, svc, testEvent("v1-legacy-1", "op-mixed", at.Add(2*time.Second)), map[string]any{"resource": "invoice", "email": "carol@example.test"})

	result, err := svc.VerifyIntegrity("tenant-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid || result.EventCount != 3 {
		t.Fatalf("mixed-format integrity failed: %+v", result)
	}
}

// TestExportJSONLStripsSearchDigests is AC-5: the decrypted export JSONL
// contains no *__search_digest keys anywhere (recursive), while the store
// keeps the digest for search purposes.
func TestExportJSONLStripsSearchDigests(t *testing.T) {
	svc := testService(t, true)
	registerDigestSchemas(t, svc)
	at := time.Unix(1_700_000_010, 0).UTC()

	event := testEvent("exp-strip-1", "op-exp", at)
	event.SchemaVersion = 2
	event.Payload = map[string]any{"resource": "invoice", "email": "alice@example.test", "nested": map[string]any{"note__search_digest": "sd2:should-be-stripped", "keep": 1}}
	if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}

	query := domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_100, 0).UTC()}
	job, err := svc.CreateExport("tenant-a", "compliance-1", query)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, getErr := svc.GetExport("tenant-a", job.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if current.Status == "completed" || current.Status == "failed" {
			job = current
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("export stuck in %s", current.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if job.Status != "completed" {
		t.Fatalf("export failed: %+v", job)
	}
	sealed, err := svc.Config.Archive.Get(context.Background(), job.ObjectPath)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := security.DecryptBytes(sealed, svc.Config.EncryptionKey)
	if err != nil {
		t.Fatal(err)
	}

	// Positive control: the store must keep the digest — stripping is
	// response/export-only and never mutates the stored payload.
	stored, err := svc.GetEvent("tenant-a", "test", "exp-strip-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stored.Payload["email__search_digest"]; !ok {
		t.Fatal("store must retain the search digest")
	}
	if _, ok := stored.Payload["nested"].(map[string]any)["note__search_digest"]; !ok {
		t.Fatal("store must retain the nested digest-like key")
	}

	// Every JSONL line is free of digest keys at any nesting depth, and the
	// exported content itself survives.
	for _, line := range strings.Split(string(plain), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		if hasDigestKey(record) {
			t.Fatalf("export line contains a digest key: %s", line)
		}
	}
	if !strings.Contains(string(plain), "alice@example.test") {
		t.Fatal("export must keep the plaintext field")
	}
	if !strings.Contains(string(plain), `"keep":1`) && !strings.Contains(string(plain), `"keep": 1`) {
		t.Fatal("export must keep non-digest nested content")
	}
}
