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

	occurredAt := time.Date(2026, 8, 4, 12, 0, 0, 123000000, time.UTC)
	firstEvent := domain.Event{EventID: "pg-relay-1", TenantID: "demo", SourceSystem: "demo", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: occurredAt, Actor: domain.Actor{ID: "u1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "pg-relay-idem"}
	payload, err := json.Marshal(firstEvent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO audit_outbox (event_id, tenant_id, idempotency_key, payload, occurred_at, status, attempts, next_attempt_at)
VALUES ('pg-relay-1', 'demo', 'pg-relay-idem', $1, $2, 'pending', 0, now() - interval '1 minute')`, string(payload), occurredAt); err != nil {
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
	secondEvent := firstEvent
	secondEvent.EventID = "pg-relay-2"
	secondEvent.IdempotencyKey = "pg-relay-idem-2"
	secondPayload, err := json.Marshal(secondEvent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO audit_outbox (event_id, tenant_id, idempotency_key, payload, occurred_at, status, attempts, next_attempt_at)
VALUES ('pg-relay-2', 'demo', 'pg-relay-idem-2', $1, $2, 'pending', 0, now() - interval '1 minute')`, string(secondPayload), occurredAt); err != nil {
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

	// S2: same event_id and caller-supplied SourceDigest, different payload
	// -> ErrConflict; the original row remains unchanged.
	reset()
	first := base
	first.EventID, first.IdempotencyKey, first.SourceDigest = "sdk-2", "sdk-idem-2", "shared-source-digest"
	if err := Insert(ctx, db, first); err != nil {
		t.Fatalf("S2 first Insert: %v", err)
	}
	other := first
	other.Payload = map[string]any{"n": 2}
	err = Insert(ctx, db, other)
	if !errors.Is(err, domain.ErrConflict) || !strings.Contains(err.Error(), "event_id already exists with different canonical content") {
		t.Fatalf("S2 content conflict: %v", err)
	}
	if n := countOutbox(); n != 1 {
		t.Fatalf("S2 row count=%d, want 1", n)
	}
	var storedPayload []byte
	if err := db.QueryRowContext(ctx, `SELECT payload FROM audit_outbox WHERE event_id = 'sdk-2'`).Scan(&storedPayload); err != nil {
		t.Fatalf("S2 read original payload: %v", err)
	}
	var storedEvent domain.Event
	if err := json.Unmarshal(storedPayload, &storedEvent); err != nil {
		t.Fatalf("S2 decode original payload: %v", err)
	}
	if storedEvent.SourceDigest != first.SourceDigest || storedEvent.Payload["n"] != float64(1) {
		t.Fatalf("S2 original row changed: source_digest=%q payload=%#v", storedEvent.SourceDigest, storedEvent.Payload)
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
	validOccurredAt := time.Date(2026, 8, 4, 12, 0, 0, 456000000, time.UTC)
	validPayload := func(id int) []byte {
		payload, err := json.Marshal(domain.Event{EventID: fmt.Sprintf("corrupt-valid-%d", id), TenantID: "demo", SourceSystem: "demo", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: validOccurredAt, Actor: domain.Actor{ID: "u1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: fmt.Sprintf("corrupt-valid-idem-%d", id)})
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	for id := 1; id <= 2; id++ {
		if _, err := db.ExecContext(ctx, `INSERT INTO audit_outbox (event_id, tenant_id, idempotency_key, payload, occurred_at, status, attempts, next_attempt_at)
VALUES ($1, 'demo', $2, $3, $4, 'pending', 0, now() - interval '1 minute')`, fmt.Sprintf("corrupt-valid-%d", id), fmt.Sprintf("corrupt-valid-idem-%d", id), string(validPayload(id)), validOccurredAt); err != nil {
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
		Deliver: func(_ context.Context, _ domain.Event) (*domain.EventReceipt, error) {
			delivered++
			return nil, nil
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

// postgresIdentityEvent is a complete event fixture for direct SQL and SDK
// integration checks. Its timestamp is supplied by the caller so the value
// survives PostgreSQL timestamptz microsecond precision exactly.
func postgresIdentityEvent(eventID, tenantID, idempotencyKey string, occurredAt time.Time) domain.Event {
	return domain.Event{
		EventID:            eventID,
		TenantID:           tenantID,
		SourceSystem:       "demo",
		EventType:          "audit.event",
		SchemaID:           "audit.event",
		SchemaVersion:      1,
		OccurredAt:         occurredAt,
		Actor:              domain.Actor{ID: "u1"},
		Action:             "update",
		Outcome:            "success",
		DataClassification: "internal",
		RetentionClass:     "standard",
		IdempotencyKey:     idempotencyKey,
		Payload:            map[string]any{"value": 1},
	}
}

// TestPostgresOutboxIdentityBinding covers each duplicated identity field with
// direct SQL corruption, a matching row in the same batch, and a valid SDK
// insert in a caller-owned transaction. Skipped unless
// AUDIT_TEST_POSTGRES_DSN points at a disposable database.
func TestPostgresOutboxIdentityBinding(t *testing.T) {
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
	reset := func() {
		if _, err := db.ExecContext(ctx, `DELETE FROM audit_outbox`); err != nil {
			t.Fatalf("reset outbox: %v", err)
		}
	}
	insertRow := func(row, payload domain.Event) int64 {
		t.Helper()
		encoded, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal %s: %v", payload.EventID, err)
		}
		var id int64
		err = db.QueryRowContext(ctx, `INSERT INTO audit_outbox
(event_id, tenant_id, idempotency_key, payload, occurred_at, status, attempts, next_attempt_at)
VALUES ($1, $2, $3, $4, $5, 'pending', 0, now() - interval '1 minute')
RETURNING id`, row.EventID, row.TenantID, row.IdempotencyKey, string(encoded), row.OccurredAt).Scan(&id)
		if err != nil {
			t.Fatalf("insert %s: %v", row.EventID, err)
		}
		return id
	}

	reset()
	occurredAt := time.Date(2026, 8, 4, 12, 0, 0, 123000000, time.UTC)
	matching := postgresIdentityEvent("identity-match", "tenant-match", "idem-match", occurredAt)
	matchingID := insertRow(matching, matching)
	cases := []struct {
		field string
	}{
		{field: "event_id"},
		{field: "tenant_id"},
		{field: "idempotency_key"},
		{field: "occurred_at"},
	}
	wantDiagnostics := map[int64]string{}
	for _, tc := range cases {
		payload := postgresIdentityEvent("payload-"+tc.field, "tenant-"+tc.field, "idem-"+tc.field, occurredAt)
		row := payload
		rowValue := "row-" + tc.field
		switch tc.field {
		case "event_id":
			row.EventID = rowValue
		case "tenant_id":
			row.TenantID = rowValue
		case "idempotency_key":
			row.IdempotencyKey = rowValue
		case "occurred_at":
			row.OccurredAt = occurredAt.Add(time.Second)
		}
		id := insertRow(row, payload)
		rowText, payloadText := stringValue(rowValue, true), stringValue(payload.EventID, true)
		if tc.field == "tenant_id" {
			rowText, payloadText = stringValue(row.TenantID, true), stringValue(payload.TenantID, true)
		}
		if tc.field == "idempotency_key" {
			rowText, payloadText = stringValue(row.IdempotencyKey, true), stringValue(payload.IdempotencyKey, true)
		}
		if tc.field == "occurred_at" {
			rowText, payloadText = timeValue(row.OccurredAt, true), timeValue(payload.OccurredAt, true)
		}
		wantDiagnostics[id] = fmt.Sprintf("%s row=%s payload=%s", tc.field, rowText, payloadText)
	}

	store := NewPostgresStore(db)
	records, corrupt, err := store.ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(records) != 1 || records[0].ID != matchingID {
		t.Fatalf("ListPending records=%+v, want matching row %d only", records, matchingID)
	}
	if len(corrupt) != len(cases) {
		t.Fatalf("ListPending corrupt=%d, want %d", len(corrupt), len(cases))
	}
	for _, report := range corrupt {
		want, ok := wantDiagnostics[report.ID]
		if !ok {
			t.Fatalf("unexpected corruption report: %+v", report)
		}
		if report.Err == nil || !strings.Contains(report.Err.Error(), "outbox identity mismatch id=") || !strings.Contains(report.Err.Error(), want) {
			t.Fatalf("report id=%d err=%v, want diagnostic containing %q", report.ID, report.Err, want)
		}
	}

	var delivered []domain.Event
	relay := fixedRelay(store, func(_ context.Context, event domain.Event) (*domain.EventReceipt, error) {
		delivered = append(delivered, event)
		return nil, nil
	})
	handled, err := relay.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if handled != len(cases)+1 || len(delivered) != 1 || delivered[0].EventID != matching.EventID {
		t.Fatalf("RunOnce handled=%d delivered=%+v, want %d/one matching event", handled, delivered, len(cases)+1)
	}
	for id, diagnostic := range wantDiagnostics {
		var status, lastError string
		var attempts int
		var nextAttemptAt time.Time
		if err := db.QueryRowContext(ctx, `SELECT status, attempts, next_attempt_at, last_error FROM audit_outbox WHERE id=$1`, id).Scan(&status, &attempts, &nextAttemptAt, &lastError); err != nil {
			t.Fatalf("read quarantined id=%d: %v", id, err)
		}
		if status != StatusFailed || attempts != 1 || !nextAttemptAt.Equal(relay.Now()) {
			t.Fatalf("id=%d status=%s attempts=%d next_attempt_at=%v, want failed/1/%v", id, status, attempts, nextAttemptAt, relay.Now())
		}
		if lastError != diagnostic && !strings.Contains(lastError, diagnostic) {
			t.Fatalf("id=%d last_error=%q, want %q", id, lastError, diagnostic)
		}
	}
	var matchingStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM audit_outbox WHERE id=$1`, matchingID).Scan(&matchingStatus); err != nil {
		t.Fatal(err)
	}
	if matchingStatus != StatusDelivered {
		t.Fatalf("matching row status=%s, want delivered", matchingStatus)
	}
	again, corruptAgain, err := store.ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("second ListPending: %v", err)
	}
	if len(again) != 0 || len(corruptAgain) != 0 {
		t.Fatalf("after quarantine ListPending records=%d corrupt=%d, want 0/0", len(again), len(corruptAgain))
	}

	reset()
	// The SDK writes row metadata and JSON in the same caller transaction.
	// A non-UTC representation proves comparison is by instant, not location.
	zone := time.FixedZone("UTC+02", 2*60*60)
	valid := postgresIdentityEvent("sdk-identity-valid", "tenant-sdk", "idem-sdk", time.Date(2026, 8, 4, 14, 0, 0, 123000000, zone))
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin SDK transaction: %v", err)
	}
	if err := Insert(ctx, tx, valid); err != nil {
		tx.Rollback()
		t.Fatalf("SDK Insert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit SDK transaction: %v", err)
	}
	delivered = nil
	handled, err = relay.RunOnce(ctx)
	if err != nil {
		t.Fatalf("SDK RunOnce: %v", err)
	}
	if handled != 1 || len(delivered) != 1 {
		t.Fatalf("SDK RunOnce handled=%d delivered=%d, want 1/1", handled, len(delivered))
	}
	got := delivered[0]
	if got.EventID != valid.EventID || got.TenantID != valid.TenantID || got.IdempotencyKey != valid.IdempotencyKey || !got.OccurredAt.Equal(valid.OccurredAt) {
		t.Fatalf("SDK delivered identity=%q/%q/%q/%v, want %q/%q/%q/%v", got.EventID, got.TenantID, got.IdempotencyKey, got.OccurredAt, valid.EventID, valid.TenantID, valid.IdempotencyKey, valid.OccurredAt)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM audit_outbox WHERE event_id=$1`, valid.EventID).Scan(&matchingStatus); err != nil {
		t.Fatal(err)
	}
	if matchingStatus != StatusDelivered {
		t.Fatalf("SDK row status=%s, want delivered", matchingStatus)
	}
}

// TestPostgresInfiniteOccurredAtIsQuarantined ensures PostgreSQL's special
// infinity timestamp values are treated as corrupt identity metadata rather
// than as a row-scan error that wedges the whole poll. Skipped unless
// AUDIT_TEST_POSTGRES_DSN points at a disposable database.
func TestPostgresInfiniteOccurredAtIsQuarantined(t *testing.T) {
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
	occurredAt := time.Date(2026, 8, 4, 12, 0, 0, 123000000, time.UTC)
	event := postgresIdentityEvent("infinite-occurred", "tenant-infinite", "idem-infinite", occurredAt)
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO audit_outbox
(event_id, tenant_id, idempotency_key, payload, occurred_at, status, attempts, next_attempt_at)
VALUES ($1, $2, $3, $4, 'infinity', 'pending', 0, now() - interval '1 minute')`, event.EventID, event.TenantID, event.IdempotencyKey, string(payload)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	store := NewPostgresStore(db)
	records, corrupt, err := store.ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(records) != 0 || len(corrupt) != 1 {
		t.Fatalf("ListPending records=%d corrupt=%d, want 0/1", len(records), len(corrupt))
	}
	if got := corrupt[0].Err.Error(); !strings.Contains(got, `occurred_at row="infinity"`) {
		t.Fatalf("diagnostic=%q, want infinity timestamp", got)
	}

	deliveries := 0
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) (*domain.EventReceipt, error) {
		deliveries++
		return nil, nil
	})
	if handled, err := relay.RunOnce(ctx); err != nil || handled != 1 {
		t.Fatalf("RunOnce handled=%d err=%v, want 1/nil", handled, err)
	}
	if deliveries != 0 {
		t.Fatalf("deliveries=%d, want 0 for infinite timestamp", deliveries)
	}
	var status, lastError string
	var attempts int
	if err := db.QueryRowContext(ctx, `SELECT status, attempts, last_error FROM audit_outbox WHERE event_id=$1`, event.EventID).Scan(&status, &attempts, &lastError); err != nil {
		t.Fatal(err)
	}
	if status != StatusFailed || attempts != 1 || !strings.Contains(lastError, `occurred_at row="infinity"`) {
		t.Fatalf("row status=%s attempts=%d last_error=%q, want failed/1/infinity diagnostic", status, attempts, lastError)
	}
}

// TestPostgresRelayVerifiesReceiptAgainstRealAPI is the AC-3 C Postgres
// variant: a real audit_outbox row is delivered by a real PostgresStore
// relay against the in-process audit API, and the DB row must end up with
// the API's verified receipt values in api_status/delivered_event_id.
// Skipped unless AUDIT_TEST_POSTGRES_DSN points at a disposable database.
func TestPostgresRelayVerifiesReceiptAgainstRealAPI(t *testing.T) {
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

	apiURL := newRealAuditAPI(t)
	event := domain.Event{EventID: "pg-relay-receipt-1", TenantID: "tenant-a", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "u1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "pg-relay-receipt-idem", Payload: map[string]any{"value": 1}}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO audit_outbox (event_id, tenant_id, idempotency_key, payload, occurred_at, status, attempts, next_attempt_at)
VALUES ('pg-relay-receipt-1', 'tenant-a', 'pg-relay-receipt-idem', $1, $2, 'pending', 0, now() - interval '1 minute')`, string(payload), event.OccurredAt); err != nil {
		t.Fatalf("insert: %v", err)
	}

	relay := &Relay{
		Store:     NewPostgresStore(db),
		Deliver:   HTTPDeliverer(apiURL, "dev:tenant-a:service:crm", nil),
		BatchSize: 10,
	}
	handled, err := relay.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if handled != 1 {
		t.Fatalf("handled=%d, want 1", handled)
	}
	var apiStatus, deliveredEventID string
	var deliveredAt *time.Time
	if err := db.QueryRowContext(ctx, `SELECT api_status, delivered_event_id, delivered_at FROM audit_outbox WHERE event_id='pg-relay-receipt-1'`).Scan(&apiStatus, &deliveredEventID, &deliveredAt); err != nil {
		t.Fatal(err)
	}
	if apiStatus != domain.StatusIndexed && apiStatus != domain.StatusArchived {
		t.Fatalf("api_status=%q, want indexed/archived (real API post-ledger status)", apiStatus)
	}
	if deliveredEventID != "pg-relay-receipt-1" {
		t.Fatalf("delivered_event_id=%q, want pg-relay-receipt-1", deliveredEventID)
	}
	if deliveredAt == nil || deliveredAt.IsZero() {
		t.Fatalf("delivered_at=%v, want set", deliveredAt)
	}
}

// TestPostgresInsertRejectsOversized is AC-3: no oversized jsonb row can be
// written through the SDK. Skipped unless AUDIT_TEST_POSTGRES_DSN points at a
// disposable database; reset pattern matches TestPostgresStoreIntegration.
func TestPostgresInsertRejectsOversized(t *testing.T) {
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

	// Oversized insert fails with ErrInvalid before any SQL; the caller
	// rolls back its transaction (caller-owned-tx contract) and nothing is
	// persisted — the row count for that event_id stays 0.
	oversized := sizedEvent(t, domain.MaxEventBytes+1)
	oversized.EventID, oversized.IdempotencyKey = "pg-oversized-1", "pg-oversized-idem-1"
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = Insert(ctx, tx, oversized)
	if !errors.Is(err, domain.ErrInvalid) {
		tx.Rollback()
		t.Fatalf("oversized Insert = %v, want ErrInvalid", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM audit_outbox WHERE event_id = $1`, oversized.EventID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("oversized event must not persist, rows=%d", n)
	}

	// In-bounds fixtures then land normally, and the whole table never holds
	// a payload over the bound. The in-bounds fixture is comfortably inside
	// the cap: jsonb text re-serialization can shift octet_length near the
	// boundary (FM-11), so the exact-boundary witness is AC-2 in Go, where
	// byte precision is controllable.
	good := outboxEvent("pg-inbounds-1", "pg-inbounds-idem-1")
	if err := Insert(ctx, db, good); err != nil {
		t.Fatalf("in-bounds Insert: %v", err)
	}
	var maxPayload int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(max(octet_length(payload::text)), 0) FROM audit_outbox`).Scan(&maxPayload); err != nil {
		t.Fatal(err)
	}
	if maxPayload > domain.MaxEventBytes {
		t.Fatalf("max payload octet_length=%d, want <= %d", maxPayload, domain.MaxEventBytes)
	}
}
