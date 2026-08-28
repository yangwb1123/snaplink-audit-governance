package projection

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
)

// TestClickHouseProjectionIntegration exercises the real ClickHouse table.
// Skipped unless AUDIT_TEST_CLICKHOUSE_DSN points at a disposable server
// with an existing database.
func TestClickHouseProjectionIntegration(t *testing.T) {
	dsn := os.Getenv("AUDIT_TEST_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("set AUDIT_TEST_CLICKHOUSE_DSN to run ClickHouse projection tests")
	}
	ctx := context.Background()
	store, err := Open(dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("schema: %v", err)
	}

	event := domain.Event{
		EventID: "projection-it-1", TenantID: "demo", SourceSystem: "demo",
		EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1,
		OccurredAt: time.Now().UTC(), StreamID: "demo:source:demo", Sequence: 1,
		Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success",
		DataClassification: "internal", RetentionClass: "standard",
		IdempotencyKey: "projection-it-idem", Hash: "abc123",
		Payload: map[string]any{"note": "integration", "amount": 7},
	}
	if err := store.Insert(ctx, event); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// 重复插入（at-least-once 重投）不报错；ReplacingMergeTree 最终收敛。
	if err := store.Insert(ctx, event); err != nil {
		t.Fatalf("duplicate insert: %v", err)
	}
	count, err := store.CountTenant(ctx, "demo")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count == 0 {
		t.Fatal("projection count is zero after insert")
	}
}

// ledgeredFixture returns a post-ledger-shaped event (the shape a
// ledgered-topic message carries: StreamID non-empty, Sequence > 0,
// Hash non-empty) with a caller-supplied event id.
func ledgeredFixture(eventID string) domain.Event {
	return domain.Event{
		EventID: eventID, TenantID: "demo", SourceSystem: "demo",
		EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1,
		OccurredAt: time.Now().UTC(), StreamID: "demo:source:demo", Sequence: 2,
		Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success",
		DataClassification: "internal", RetentionClass: "standard",
		IdempotencyKey: "projection-guard-idem", Hash: "hash-abc",
		Payload: map[string]any{"note": "guard", "amount": 3},
	}
}

// TestProjectionPayloadStripsSearchDigests pins the projection boundary: the
// ledger payload may contain derived lookup digests, but the ClickHouse copy
// must not retain them. The original event is left untouched and encrypted
// values remain opaque strings.
func TestProjectionPayloadStripsSearchDigests(t *testing.T) {
	payload := map[string]any{
		"account_number":       "enc:v1:opaque",
		"email":                "alice@example.test",
		"email__search_digest": "sd2:bound",
		"nested": map[string]any{
			"legacy__search_digest": "legacy",
			"keep":                  json.Number("9007199254740993"),
		},
		"items": []any{
			map[string]any{"item__search_digest": "sd2:item", "value": 7},
		},
	}
	event := ledgeredFixture("projection-payload-1")
	event.Payload = payload
	got, err := projectionPayload(event)
	if err != nil {
		t.Fatal(err)
	}
	stripped, err := security.StripSearchDigests(payload)
	if err != nil {
		t.Fatal(err)
	}
	want, err := domain.CanonicalJSON(stripped)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("projection payload=%s, want=%s", got, want)
	}
	if _, ok := payload["email__search_digest"]; !ok {
		t.Fatal("projection preparation mutated the source payload")
	}
	if bytes.Contains(got, []byte("__search_digest")) {
		t.Fatalf("projection payload retains a search-digest key: %s", got)
	}
	if !bytes.Contains(got, []byte("enc:v1:opaque")) || !bytes.Contains(got, []byte("9007199254740993")) {
		t.Fatalf("projection payload lost protected or exact numeric content: %s", got)
	}
}

// TestInsertRejectsUnledgered is the AC-1.1 regression probe for guard
// placement: a zero-value *Store (nil db) must return ErrNotLedgered without
// touching the database, proving no row can be written for an unledgered
// event. If the guard were moved after the DB access, the nil-db probe would
// panic instead of returning the sentinel.
func TestInsertRejectsUnledgered(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*domain.Event)
	}{
		{"empty stream id", func(e *domain.Event) { e.StreamID = "" }},
		{"zero sequence", func(e *domain.Event) { e.Sequence = 0 }},
		{"empty hash", func(e *domain.Event) { e.Hash = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event := ledgeredFixture("guard-probe-1")
			tc.mutate(&event)
			var store *Store // nil db: any DB touch would panic
			if err := store.Insert(context.Background(), event); !errors.Is(err, ErrNotLedgered) {
				t.Fatalf("Insert() error = %v, want ErrNotLedgered", err)
			}
		})
	}
}

// TestClickHouseRoundTripsChainColumns (AC-2.1): a post-ledger-shaped event
// round-trips stream_id/sequence/event_hash into the projection columns.
func TestClickHouseRoundTripsChainColumns(t *testing.T) {
	dsn := os.Getenv("AUDIT_TEST_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("set AUDIT_TEST_CLICKHOUSE_DSN to run ClickHouse projection tests")
	}
	ctx := context.Background()
	store, err := Open(dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("schema: %v", err)
	}

	event := ledgeredFixture("projection-ledgered-1")
	if err := store.Insert(ctx, event); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var streamID string
	var sequence uint64
	var eventHash string
	if err := store.db.QueryRowContext(ctx, `SELECT stream_id, sequence, event_hash FROM audit_events WHERE event_id = ?`, event.EventID).Scan(&streamID, &sequence, &eventHash); err != nil {
		t.Fatalf("select chain columns: %v", err)
	}
	if streamID != event.StreamID || sequence != uint64(event.Sequence) || eventHash != event.Hash {
		t.Fatalf("chain columns mismatch: stream_id=%q sequence=%d event_hash=%q, want %q/%d/%q", streamID, sequence, eventHash, event.StreamID, event.Sequence, event.Hash)
	}
}

// TestClickHouseRejectsPreLedgerRow (AC-1.3/AC-2.2): a pre-ledger-shaped
// event (chain fields empty) is rejected by the guard and writes zero rows.
func TestClickHouseRejectsPreLedgerRow(t *testing.T) {
	dsn := os.Getenv("AUDIT_TEST_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("set AUDIT_TEST_CLICKHOUSE_DSN to run ClickHouse projection tests")
	}
	ctx := context.Background()
	store, err := Open(dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("schema: %v", err)
	}

	event := ledgeredFixture("projection-pre-ledger-1")
	event.StreamID = ""
	event.Sequence = 0
	event.Hash = ""
	if err := store.Insert(ctx, event); !errors.Is(err, ErrNotLedgered) {
		t.Fatalf("Insert() error = %v, want ErrNotLedgered", err)
	}
	var count uint64
	if err := store.db.QueryRowContext(ctx, `SELECT count() FROM audit_events WHERE event_id = ?`, event.EventID).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("projection contains %d row(s) for pre-ledger event, want 0", count)
	}
}

// TestInsertRejectsOutOfRangeOccurredAt is AC-3: the ledger-horizon guard
// precedes payload encoding and DB access. An out-of-range occurred_at on a
// ledgered-shaped event fails with domain.ErrOccurredAtOutOfRange even on a
// zero-value *Store (nil db) — the guard provably runs before any database
// round-trip — so the consumer can classify the class as permanent. Presence
// precedence is preserved: missing chain state still returns ErrNotLedgered
// first, regardless of occurred_at.
//
// Note: the in-range success path (ledgered shape, time.Now().UTC()) is
// covered by TestClickHouseProjectionIntegration; a nil-*Store probe cannot
// exercise it because an in-range event proceeds to the DB exec step, which
// is exactly what the guard is designed to skip only for the rejected class.
func TestInsertRejectsOutOfRangeOccurredAt(t *testing.T) {
	for _, tc := range []struct {
		name string
		at   time.Time
	}{
		{"year 1800 (below floor)", time.Date(1800, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"year 2300 (above ceiling)", time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := ledgeredFixture("occ-guard-probe")
			event.OccurredAt = tc.at
			var store *Store // nil db: any DB touch would panic
			if err := store.Insert(context.Background(), event); !errors.Is(err, domain.ErrOccurredAtOutOfRange) {
				t.Fatalf("Insert() error = %v, want ErrOccurredAtOutOfRange", err)
			}
		})
	}
	t.Run("presence precedence", func(t *testing.T) {
		event := ledgeredFixture("occ-guard-precedence")
		event.OccurredAt = time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)
		event.StreamID = ""
		var store *Store
		if err := store.Insert(context.Background(), event); !errors.Is(err, ErrNotLedgered) {
			t.Fatalf("Insert() error = %v, want ErrNotLedgered (presence guard first)", err)
		}
	})
}

// TestInsertRejectsUnscopedTenant is AC-1/AC-2: the tenant-scoping guard
// precedes payload encoding and DB access. Empty, ':'-containing, and
// over-85-byte tenant ids all fail with ErrTenantUnscoped on a zero-value
// *Store (nil db) — proving no row can ever be written for an unscoped event
// and the guard runs before any database round-trip (mirrors
// TestInsertRejectsUnledgered / TestInsertRejectsOutOfRangeOccurredAt).
func TestInsertRejectsUnscopedTenant(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*domain.Event)
	}{
		{"empty tenant id", func(e *domain.Event) { e.TenantID = "" }},
		{"colon in tenant id", func(e *domain.Event) { e.TenantID = "acme:evil" }},
		{"over-long tenant id", func(e *domain.Event) { e.TenantID = strings.Repeat("a", 86) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event := ledgeredFixture("tenant-guard-probe")
			tc.mutate(&event)
			var store *Store // nil db: any DB touch would panic
			if err := store.Insert(context.Background(), event); !errors.Is(err, ErrTenantUnscoped) {
				t.Fatalf("Insert() error = %v, want ErrTenantUnscoped (guard before DB access)", err)
			}
		})
	}
}

// TestInsertRejectsUnscopedTenantClickHouse is AC-3: against the real
// schema, a properly-scoped tenant is unaffected and a bad-tenant event is
// rejected at the boundary and writes no row — the core invariant is zero
// tenant_id=” rows in the shared table.
func TestInsertRejectsUnscopedTenantClickHouse(t *testing.T) {
	dsn := os.Getenv("AUDIT_TEST_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("set AUDIT_TEST_CLICKHOUSE_DSN to run ClickHouse projection tests")
	}
	ctx := context.Background()
	store, err := Open(dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("schema: %v", err)
	}

	good := ledgeredFixture("tenant-guard-good")
	good.TenantID = "demo"
	if err := store.Insert(ctx, good); err != nil {
		t.Fatalf("insert good: %v", err)
	}

	bad := ledgeredFixture("tenant-guard-bad")
	bad.TenantID = ""
	if err := store.Insert(ctx, bad); !errors.Is(err, ErrTenantUnscoped) {
		t.Fatalf("insert empty tenant: error = %v, want ErrTenantUnscoped", err)
	}
	badColon := ledgeredFixture("tenant-guard-bad-colon")
	badColon.TenantID = "acme:evil"
	if err := store.Insert(ctx, badColon); !errors.Is(err, ErrTenantUnscoped) {
		t.Fatalf("insert colon tenant: error = %v, want ErrTenantUnscoped", err)
	}

	countGood, err := store.CountTenant(ctx, "demo")
	if err != nil {
		t.Fatalf("count demo: %v", err)
	}
	if countGood != 1 {
		t.Fatalf("CountTenant(demo) = %d, want 1", countGood)
	}
	countPhantom, err := store.CountTenant(ctx, "")
	if err != nil {
		t.Fatalf("count empty: %v", err)
	}
	if countPhantom != 0 {
		t.Fatalf("CountTenant(\"\") = %d, want 0 (no phantom tenant_id='' rows)", countPhantom)
	}
}
