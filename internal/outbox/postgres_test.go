package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
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
	records, corrupt, err := store.ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(corrupt) != 0 {
		t.Fatalf("ListPending corrupt=%+v, want none", corrupt)
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
	again, corrupt, err := store.ListPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(corrupt) != 0 {
		t.Fatalf("ListPending corrupt=%+v, want none", corrupt)
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
	records, corrupt, err = store.ListPending(ctx, 10)
	if err != nil || len(corrupt) != 0 || len(records) != 1 {
		t.Fatalf("second ListPending=%d corrupt=%d err=%v", len(records), len(corrupt), err)
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

	// ---- Insert conflict reporting: each scenario starts from a clean
	// table and rolls back between scenarios (the delete is a test-harness
	// reset, not a product path). ----
	reset := func() {
		if _, err := db.ExecContext(ctx, `DELETE FROM audit_outbox`); err != nil {
			t.Fatalf("reset outbox: %v", err)
		}
	}
	countOutbox := func() int {
		var n int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM audit_outbox`).Scan(&n); err != nil {
			t.Fatalf("count outbox: %v", err)
		}
		return n
	}

	// S1: identical re-insert in the same caller tx -> nil, one row.
	reset()
	base := domain.Event{EventID: "sdk-1", TenantID: "demo", SourceSystem: "demo", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Actor: domain.Actor{ID: "u1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "sdk-idem-1", Payload: map[string]any{"n": 1}}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Insert(ctx, tx, base); err != nil {
		tx.Rollback()
		t.Fatalf("first Insert: %v", err)
	}
	if err := Insert(ctx, tx, base); err != nil {
		tx.Rollback()
		t.Fatalf("duplicate Insert must be idempotent: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if n := countOutbox(); n != 1 {
		t.Fatalf("S1 row count=%d, want 1", n)
	}

	// S2: same event_id, different canonical content -> ErrConflict.
	reset()
	first := base
	first.EventID, first.IdempotencyKey = "sdk-2", "sdk-idem-2"
	if err := Insert(ctx, db, first); err != nil {
		t.Fatalf("S2 first Insert: %v", err)
	}
	other := first
	other.Action = "delete"
	err = Insert(ctx, db, other)
	if !errors.Is(err, domain.ErrConflict) || !strings.Contains(err.Error(), "event_id already exists with different canonical content") {
		t.Fatalf("S2 content conflict: %v", err)
	}

	// S3: idempotency_key reuse for a different event -> ErrConflict.
	reset()
	first = base
	first.EventID, first.IdempotencyKey = "sdk-3", "sdk-idem-3"
	if err := Insert(ctx, db, first); err != nil {
		t.Fatalf("S3 first Insert: %v", err)
	}
	sameKey := first
	sameKey.EventID = "sdk-3b"
	err = Insert(ctx, db, sameKey)
	if !errors.Is(err, domain.ErrConflict) || !strings.Contains(err.Error(), "idempotency_key is already associated with another event") {
		t.Fatalf("S3 idem-key conflict: %v", err)
	}

	// S4: the same idempotency_key under different tenants is fine.
	reset()
	tenantA := base
	tenantA.EventID, tenantA.IdempotencyKey = "sdk-4a", "cross-key"
	tenantB := base
	tenantB.TenantID, tenantB.EventID, tenantB.IdempotencyKey = "tenant-b", "sdk-4b", "cross-key"
	if err := Insert(ctx, db, tenantA); err != nil {
		t.Fatalf("S4 tenant A: %v", err)
	}
	if err := Insert(ctx, db, tenantB); err != nil {
		t.Fatalf("S4 tenant B must not conflict with tenant A's key: %v", err)
	}
	if n := countOutbox(); n != 2 {
		t.Fatalf("S4 row count=%d, want 2", n)
	}

	// S5: an identical re-insert of a dead-lettered row -> ErrConflict,
	// never nil (the row will never be delivered).
	reset()
	dead := base
	dead.EventID, dead.IdempotencyKey = "sdk-5", "sdk-idem-5"
	if err := Insert(ctx, db, dead); err != nil {
		t.Fatalf("S5 first Insert: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE audit_outbox SET status = 'failed' WHERE event_id = 'sdk-5'`); err != nil {
		t.Fatal(err)
	}
	err = Insert(ctx, db, dead)
	if !errors.Is(err, domain.ErrConflict) || !strings.Contains(err.Error(), "event already dead-lettered") {
		t.Fatalf("S5 dead-lettered duplicate: %v", err)
	}

	// S6: same logical event, same instant, different zone encoding -> nil
	// via the EventContentDigest cross-check, row count stays 1.
	reset()
	zone := base
	zone.EventID, zone.IdempotencyKey = "sdk-6", "sdk-idem-6"
	if err := Insert(ctx, db, zone); err != nil {
		t.Fatalf("S6 first Insert: %v", err)
	}
	if err := Insert(ctx, db, zoneVariant(zone)); err != nil {
		t.Fatalf("S6 zone-variant re-insert must be idempotent: %v", err)
	}
	if n := countOutbox(); n != 1 {
		t.Fatalf("S6 row count=%d, want 1", n)
	}

	// S7: REPEATABLE READ with a snapshot taken before the winner commits
	// degrades to a fail-closed error (never nil) — documented degradation.
	reset()
	rr := base
	rr.EventID, rr.IdempotencyKey = "sdk-7", "sdk-idem-7"
	rrTx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rrTx.ExecContext(ctx, `SELECT count(*) FROM audit_outbox`); err != nil {
		t.Fatal(err)
	}
	if err := Insert(ctx, db, rr); err != nil {
		t.Fatalf("S7 winner Insert: %v", err)
	}
	err = Insert(ctx, rrTx, rr)
	rrTx.Rollback()
	if err == nil {
		t.Fatal("S7 REPEATABLE READ duplicate insert must fail closed (never nil)")
	}

	// S8: two concurrent transactions inserting the same event: exactly one
	// row survives and both callers observe nil (the blocked insert waits
	// out the winner and classifies it as an identical duplicate).
	reset()
	conc := base
	conc.EventID, conc.IdempotencyKey = "sdk-8", "sdk-idem-8"
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				results <- err
				return
			}
			err = Insert(ctx, tx, conc)
			if err != nil {
				tx.Rollback()
				results <- err
				return
			}
			results <- tx.Commit()
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("S8 concurrent duplicate Insert: %v", err)
		}
	}
	if n := countOutbox(); n != 1 {
		t.Fatalf("S8 concurrent inserts must collapse to one row, got %d", n)
	}
}

// TestPostgresStoreCorruptPayloadWedgeBreak (T4+T6, AC-1/AC-2) seeds one
// row with a payload that is valid JSON but fails to unmarshal into
// domain.Event (REQ-1/F1: '[]'::jsonb) plus valid rows, and verifies the
// relay dead-letters the corrupt row and keeps delivering. Skipped unless
// AUDIT_TEST_POSTGRES_DSN points at a disposable database.
func TestPostgresStoreCorruptPayloadWedgeBreak(t *testing.T) {
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

	// One corrupt row ('[]'::jsonb decodes without error into a slice, then
	// fails Event unmarshal) + two valid rows. Never seed null/{ } payloads:
	// they decode into a zero Event without error (REQ-7 limitation).
	if _, err := db.ExecContext(ctx, `INSERT INTO audit_outbox (event_id, tenant_id, idempotency_key, payload, occurred_at, status, attempts, next_attempt_at)
VALUES ('corrupt-1', 'demo', 'corrupt-idem-1', '[]'::jsonb, now(), 'pending', 0, now() - interval '1 minute')`); err != nil {
		t.Fatalf("insert corrupt: %v", err)
	}
	validPayload := func(id int) []byte {
		payload, err := json.Marshal(domain.Event{EventID: fmt.Sprintf("corrupt-valid-%d", id), TenantID: "demo", SourceSystem: "demo", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Actor: domain.Actor{ID: "u1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: fmt.Sprintf("corrupt-valid-idem-%d", id)})
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	for id := 1; id <= 2; id++ {
		if _, err := db.ExecContext(ctx, `INSERT INTO audit_outbox (event_id, tenant_id, idempotency_key, payload, occurred_at, status, attempts, next_attempt_at)
VALUES ($1, 'demo', $2, $3, now(), 'pending', 0, now() - interval '1 minute')`, fmt.Sprintf("corrupt-valid-%d", id), fmt.Sprintf("corrupt-valid-idem-%d", id), string(validPayload(id))); err != nil {
			t.Fatalf("insert valid %d: %v", id, err)
		}
	}

	store := NewPostgresStore(db)
	records, corrupt, err := store.ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(records) != 2 || len(corrupt) != 1 {
		t.Fatalf("ListPending records=%d corrupt=%d, want 2/1", len(records), len(corrupt))
	}
	if corrupt[0].Attempts != 0 {
		t.Fatalf("corrupt attempts=%d, want 0 as scanned", corrupt[0].Attempts)
	}
	if !strings.Contains(corrupt[0].Err.Error(), "decode outbox payload id=") {
		t.Fatalf("corrupt err=%v, want decode outbox payload error", corrupt[0].Err)
	}

	// RunOnce must deliver the 2 valid rows and dead-letter the corrupt one.
	delivered := 0
	relay := &Relay{
		Store:     store,
		BatchSize: 10,
		Deliver: func(_ context.Context, event domain.Event) error {
			delivered++
			return nil
		},
	}
	handled, err := relay.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if handled != 3 || delivered != 2 {
		t.Fatalf("RunOnce handled=%d delivered=%d, want 3/2", handled, delivered)
	}
	var rowStatus, lastError string
	if err := db.QueryRowContext(ctx, `SELECT status, last_error FROM audit_outbox WHERE event_id='corrupt-1'`).Scan(&rowStatus, &lastError); err != nil {
		t.Fatal(err)
	}
	if rowStatus != StatusFailed {
		t.Fatalf("corrupt row status=%s, want failed", rowStatus)
	}
	if !strings.Contains(lastError, "decode outbox payload id=") {
		t.Fatalf("corrupt row last_error=%q, want decode error", lastError)
	}

	// T6: the quarantined row never reappears; only the remaining due valid
	// rows are returned, and the relay keeps making progress.
	again, corruptAgain, err := store.ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("second ListPending: %v", err)
	}
	if len(corruptAgain) != 0 {
		t.Fatalf("second ListPending corrupt=%+v, want none after quarantine", corruptAgain)
	}
	if len(again) != 0 {
		t.Fatalf("second ListPending records=%d, want 0 (valid rows already delivered)", len(again))
	}
	handled, err = relay.RunOnce(ctx)
	if err != nil {
		t.Fatalf("second RunOnce: %v", err)
	}
	if handled != 0 {
		t.Fatalf("second RunOnce handled=%d, want 0", handled)
	}
}
