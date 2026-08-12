package projection

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
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
