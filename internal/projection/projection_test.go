package projection

import (
	"context"
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
