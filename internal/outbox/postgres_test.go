package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/snaplink/audit-governance/internal/domain"
)

// TestPostgresStoreIntegration exercises the real audit_outbox table
// (migration 001 with 003 columns applied). Skipped unless
// AUDIT_TEST_POSTGRES_DSN points at a disposable database.
func TestPostgresStoreIntegration(t *testing.T) {
	dsn := os.Getenv("AUDIT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set AUDIT_TEST_POSTGRES_DSN to run PostgreSQL integration tests")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM audit_outbox`); err != nil {
		t.Fatalf("reset outbox: %v", err)
	}

	payload, err := json.Marshal(domain.Event{EventID: "pg-relay-1", TenantID: "demo", SourceSystem: "demo", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Actor: domain.Actor{ID: "u1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "pg-relay-idem"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO audit_outbox (event_id, tenant_id, idempotency_key, payload, occurred_at, status, attempts, next_attempt_at)
VALUES ('pg-relay-1', 'demo', 'pg-relay-idem', $1, now(), 'pending', 0, now() - interval '1 minute')`, string(payload)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	store := NewPostgresStore(db)
	records, err := store.ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(records) != 1 || records[0].Event.EventID != "pg-relay-1" {
		t.Fatalf("ListPending records=%+v", records)
	}

	// 成功标记：条件更新返回 true，之后不再出现在 pending。
	applied, err := store.Update(ctx, records[0].ID, Update{Status: StatusDelivered, Attempts: 1, NextAttemptAt: time.Now().UTC(), DeliveredAt: time.Now().UTC(), DeliveredEventID: "pg-relay-1", APIStatus: domain.StatusAccepted})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !applied {
		t.Fatal("first Update must apply")
	}
	again, err := store.ListPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("delivered record still pending: %+v", again)
	}

	// 重复完成（模拟并发实例）：status 已非 pending，必须返回 false。
	stale, err := store.Update(ctx, records[0].ID, Update{Status: StatusDelivered, Attempts: 2})
	if err != nil {
		t.Fatalf("stale Update: %v", err)
	}
	if stale {
		t.Fatal("stale Update must not apply")
	}

	// 失败重试路径：重新插入一条，失败后 attempts 递增且仍 pending。
	if _, err := db.ExecContext(ctx, `INSERT INTO audit_outbox (event_id, tenant_id, idempotency_key, payload, occurred_at, status, attempts, next_attempt_at)
VALUES ('pg-relay-2', 'demo', 'pg-relay-idem-2', $1, now(), 'pending', 0, now() - interval '1 minute')`, string(payload)); err != nil {
		t.Fatalf("insert second: %v", err)
	}
	records, err = store.ListPending(ctx, 10)
	if err != nil || len(records) != 1 {
		t.Fatalf("second ListPending=%d err=%v", len(records), err)
	}
	applied, err = store.Update(ctx, records[0].ID, Update{Status: StatusFailed, Attempts: 1, NextAttemptAt: time.Now().UTC(), LastError: "audit api returned 400"})
	if err != nil || !applied {
		t.Fatalf("failure Update applied=%v err=%v", applied, err)
	}
	var rowStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM audit_outbox WHERE event_id='pg-relay-2'`).Scan(&rowStatus); err != nil {
		t.Fatal(err)
	}
	if rowStatus != StatusFailed {
		t.Fatalf("row status=%s, want failed", rowStatus)
	}
}
