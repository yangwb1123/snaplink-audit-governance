package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

type fakeExecer struct {
	query string
	args  []any
}

func (f *fakeExecer) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	f.query, f.args = query, args
	return fakeResult{}, nil
}

type fakeResult struct{}

func (fakeResult) LastInsertId() (int64, error) { return 1, nil }
func (fakeResult) RowsAffected() (int64, error) { return 1, nil }

func TestInsertUsesCallerTransaction(t *testing.T) {
	fake := &fakeExecer{}
	event := domain.Event{EventID: "outbox-1", TenantID: "tenant-a", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "outbox-idem-1", Payload: map[string]any{"value": 1}}
	if err := Insert(context.Background(), fake, event); err != nil {
		t.Fatal(err)
	}
	if fake.query == "" || len(fake.args) != 5 || fake.args[0] != "outbox-1" || fake.args[1] != "tenant-a" {
		t.Fatalf("unexpected SQL call: %q %#v", fake.query, fake.args)
	}
}

// TestInsertRejectsKeyFramingFields is F-2: the outbox SDK validates via
// Event.ValidateBasic before any SQL (sdk.go:81), so events whose key
// components embed KeySeparator (0x1F) or the other rejected classes are
// never written to the outbox table — the Kafka/DLQ chain never sees them
// and the API's 400 stays the only disposal surface.
func TestInsertRejectsKeyFramingFields(t *testing.T) {
	base := domain.Event{EventID: "outbox-1", TenantID: "tenant-a", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "outbox-idem-1", Payload: map[string]any{"value": 1}}
	for _, tc := range []struct {
		name   string
		mutate func(*domain.Event)
	}{
		{"event_id", func(e *domain.Event) { e.EventID = "a\x1fb" }},
		{"source_system", func(e *domain.Event) { e.SourceSystem = "x\x1fy" }},
		{"source_system space", func(e *domain.Event) { e.SourceSystem = "x y" }},
	} {
		event := base
		tc.mutate(&event)
		fake := &fakeExecer{}
		err := Insert(context.Background(), fake, event)
		if !errors.Is(err, domain.ErrInvalid) {
			t.Errorf("%s: Insert = %v, want ErrInvalid", tc.name, err)
		}
		if fake.query != "" || len(fake.args) != 0 {
			t.Errorf("%s: Insert touched the DB for an invalid event: %q %#v", tc.name, fake.query, fake.args)
		}
	}
}

// --- conflict-path fakes ---

// fakeZeroResult reports a conflict-absorbed insert (0 rows).
type fakeZeroResult struct{}

func (fakeZeroResult) LastInsertId() (int64, error) { return 0, nil }
func (fakeZeroResult) RowsAffected() (int64, error) { return 0, nil }

// fakeFailedResult reports a RowsAffected failure.
type fakeFailedResult struct{}

func (fakeFailedResult) LastInsertId() (int64, error) { return 0, nil }
func (fakeFailedResult) RowsAffected() (int64, error) { return 0, errRowsAffected }

// fakeRow is a scripted rowScanner: Scan applies the script to dest.
type fakeRow struct {
	scan func(dest ...any) error
}

func (r fakeRow) Scan(dest ...any) error { return r.scan(dest...) }

func rowNoRows() fakeRow {
	return fakeRow{scan: func(dest ...any) error { return sql.ErrNoRows }}
}

func rowError(err error) fakeRow {
	return fakeRow{scan: func(dest ...any) error { return err }}
}

func rowStatusIdentical(status string, identical bool) fakeRow {
	return fakeRow{scan: func(dest ...any) error {
		if len(dest) != 2 {
			return fmt.Errorf("scan arity: got %d, want 2", len(dest))
		}
		*dest[0].(*string) = status
		*dest[1].(*bool) = identical
		return nil
	}}
}

func rowPayload(payload []byte) fakeRow {
	return fakeRow{scan: func(dest ...any) error {
		if len(dest) != 1 {
			return fmt.Errorf("scan arity: got %d, want 1", len(dest))
		}
		*dest[0].(*[]byte) = payload
		return nil
	}}
}

func rowIdemKeyFound() fakeRow {
	return fakeRow{scan: func(dest ...any) error {
		if len(dest) != 1 {
			return fmt.Errorf("scan arity: got %d, want 1", len(dest))
		}
		*dest[0].(*int) = 1
		return nil
	}}
}

// fakeTx is a caller-owned transaction double: it records every statement
// (proving classification runs on the caller's tx) and serves scripted rows
// to QueryRowContext in order.
type fakeTx struct {
	execResult sql.Result
	execErr    error
	rows       []fakeRow
	queries    []string
	args       [][]any
}

func (f *fakeTx) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	f.queries = append(f.queries, query)
	f.args = append(f.args, args)
	return f.execResult, f.execErr
}

func (f *fakeTx) QueryRowContext(_ context.Context, query string, _ ...any) rowScanner {
	f.queries = append(f.queries, query)
	if len(f.rows) == 0 {
		return rowNoRows()
	}
	row := f.rows[0]
	f.rows = f.rows[1:]
	return row
}

// fakeExecerOnly implements Execer without QueryRowContext: classification
// is impossible, so the fail-closed path must trigger.
type fakeExecerOnly struct{}

func (fakeExecerOnly) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return fakeZeroResult{}, nil
}

func outboxEvent(eventID, idemKey string) domain.Event {
	return domain.Event{EventID: eventID, TenantID: "tenant-a", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: idemKey, Payload: map[string]any{"value": 1}}
}

// zoneVariant returns the same logical event with the same instant encoded
// in a different zone: byte-different JSON, canonically identical.
func zoneVariant(event domain.Event) domain.Event {
	event.OccurredAt = event.OccurredAt.In(time.FixedZone("offset", 2*60*60))
	return event
}

var (
	errRowsAffected   = errors.New("rows affected unavailable")
	errClassification = errors.New("classification probe failed")
)

func TestInsertHappyPathSingleRoundTrip(t *testing.T) {
	fake := &fakeTx{execResult: fakeResult{}}
	if err := Insert(context.Background(), fake, outboxEvent("outbox-ok", "idem-ok")); err != nil {
		t.Fatal(err)
	}
	if len(fake.queries) != 1 {
		t.Fatalf("hot path must be a single ExecContext round trip, got %d statements: %v", len(fake.queries), fake.queries)
	}
	if !strings.Contains(fake.queries[0], "ON CONFLICT DO NOTHING") {
		t.Fatalf("insert must keep the targetless ON CONFLICT DO NOTHING clause: %q", fake.queries[0])
	}
	// The payload must be encoded as text (not []byte/bytea, which has no
	// cast to jsonb) so the statement works against a real PostgreSQL.
	if payload, ok := fake.args[0][3].(string); !ok || payload == "" {
		t.Fatalf("payload must be text for the jsonb column, got %T", fake.args[0][3])
	}
}

func TestInsertIdenticalEventReturnsNil(t *testing.T) {
	fake := &fakeTx{execResult: fakeZeroResult{}, rows: []fakeRow{rowStatusIdentical(StatusPending, true)}}
	if err := Insert(context.Background(), fake, outboxEvent("outbox-1", "idem-1")); err != nil {
		t.Fatalf("identical re-insert must be idempotent: %v", err)
	}
	if len(fake.queries) != 2 {
		t.Fatalf("identical duplicate must stop after the event_id probe, got %d statements: %v", len(fake.queries), fake.queries)
	}
}

func TestInsertZoneVariantReturnsNil(t *testing.T) {
	// Same logical event, same instant, different zone encoding: jsonb =
	// says different, EventContentDigest says identical -> nil.
	event := outboxEvent("outbox-zone", "idem-zone")
	stored, err := json.Marshal(zoneVariant(event))
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeTx{execResult: fakeZeroResult{}, rows: []fakeRow{
		rowStatusIdentical(StatusPending, false),
		rowPayload(stored),
	}}
	if err := Insert(context.Background(), fake, event); err != nil {
		t.Fatalf("zone-variant re-insert must be idempotent: %v", err)
	}
}

func TestInsertDeadLetteredDuplicateFailsClosed(t *testing.T) {
	// An identical re-insert of a dead-lettered row must not return nil:
	// the row is parked forever and nil would silently lose the event.
	fake := &fakeTx{execResult: fakeZeroResult{}, rows: []fakeRow{rowStatusIdentical(StatusFailed, true)}}
	err := Insert(context.Background(), fake, outboxEvent("outbox-1", "idem-1"))
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("dead-lettered duplicate: got %v, want ErrConflict", err)
	}
	if !strings.Contains(err.Error(), "event already dead-lettered") {
		t.Fatalf("message: %v", err)
	}
}

func TestInsertDeadLetteredDigestDuplicateFailsClosed(t *testing.T) {
	// Same rule through the digest arm: identical content in a different
	// zone encoding on a dead-lettered row still conflicts.
	event := outboxEvent("outbox-1", "idem-1")
	stored, err := json.Marshal(zoneVariant(event))
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeTx{execResult: fakeZeroResult{}, rows: []fakeRow{
		rowStatusIdentical(StatusFailed, false),
		rowPayload(stored),
	}}
	err = Insert(context.Background(), fake, event)
	if !errors.Is(err, domain.ErrConflict) || !strings.Contains(err.Error(), "event already dead-lettered") {
		t.Fatalf("dead-lettered digest duplicate: %v", err)
	}
}

func TestInsertConflictEventIDContent(t *testing.T) {
	event := outboxEvent("outbox-1", "idem-1")
	other := outboxEvent("outbox-1", "idem-1")
	other.Action = "delete"
	stored, err := json.Marshal(other)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeTx{execResult: fakeZeroResult{}, rows: []fakeRow{
		rowStatusIdentical(StatusPending, false),
		rowPayload(stored),
	}}
	err = Insert(context.Background(), fake, event)
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("content conflict: got %v, want ErrConflict", err)
	}
	if !strings.Contains(err.Error(), "event_id already exists with different canonical content") {
		t.Fatalf("message must match the ingest vocabulary: %v", err)
	}
}

func TestInsertConflictIdempotencyKey(t *testing.T) {
	fake := &fakeTx{execResult: fakeZeroResult{}, rows: []fakeRow{rowNoRows(), rowIdemKeyFound()}}
	err := Insert(context.Background(), fake, outboxEvent("outbox-2", "idem-shared"))
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("idem-key conflict: got %v, want ErrConflict", err)
	}
	if !strings.Contains(err.Error(), "idempotency_key is already associated with another event") {
		t.Fatalf("message must match the ingest vocabulary: %v", err)
	}
	// The idem-key probe must be tenant-scoped to match the unique index
	// (tenant_id, idempotency_key).
	probe := fake.queries[2]
	if !strings.Contains(probe, "tenant_id = $1") || !strings.Contains(probe, "idempotency_key = $2") {
		t.Fatalf("idempotency probe must be tenant-scoped: %q", probe)
	}
}

func TestInsertUnclassifiableZeroRows(t *testing.T) {
	fake := &fakeTx{execResult: fakeZeroResult{}, rows: []fakeRow{rowNoRows(), rowNoRows()}}
	err := Insert(context.Background(), fake, outboxEvent("outbox-3", "idem-3"))
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("unclassifiable zero-row: got %v, want ErrConflict", err)
	}
	if !strings.Contains(err.Error(), "unable to classify outbox insert outcome") {
		t.Fatalf("message: %v", err)
	}
}

func TestInsertExecerOnlyFailsClosed(t *testing.T) {
	// An Execer-only tx cannot run classification reads; the outcome is
	// unknown, so Insert fails closed with ErrConflict.
	err := Insert(context.Background(), fakeExecerOnly{}, outboxEvent("outbox-4", "idem-4"))
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("Execer-only tx: got %v, want ErrConflict", err)
	}
	if !strings.Contains(err.Error(), "unable to classify outbox insert outcome") {
		t.Fatalf("message: %v", err)
	}
}

func TestInsertRowsAffectedErrorFailsClosed(t *testing.T) {
	fake := &fakeTx{execResult: fakeFailedResult{}}
	err := Insert(context.Background(), fake, outboxEvent("outbox-5", "idem-5"))
	if err == nil {
		t.Fatal("RowsAffected error must not yield nil")
	}
	if errors.Is(err, domain.ErrConflict) {
		t.Fatalf("RowsAffected failure is an unknown outcome, not a conflict: %v", err)
	}
	if !errors.Is(err, errRowsAffected) {
		t.Fatalf("underlying RowsAffected error must be reachable via errors.Is: %v", err)
	}
	if !strings.Contains(err.Error(), "insert audit outbox: rows affected:") {
		t.Fatalf("message must pin the failure site: %v", err)
	}
}

func TestInsertClassificationQueryErrorFailsClosed(t *testing.T) {
	fake := &fakeTx{execResult: fakeZeroResult{}, rows: []fakeRow{rowError(errClassification)}}
	err := Insert(context.Background(), fake, outboxEvent("outbox-6", "idem-6"))
	if err == nil {
		t.Fatal("classification failure must not yield nil")
	}
	if errors.Is(err, domain.ErrConflict) {
		t.Fatalf("classification failure is an unknown outcome, not a conflict: %v", err)
	}
	if !errors.Is(err, errClassification) {
		t.Fatalf("underlying error must be reachable via errors.Is: %v", err)
	}
}

func TestInsertClassificationUsesCallerTransaction(t *testing.T) {
	// Every classification read must run on the same caller-owned tx object
	// (the fake) and no BEGIN/COMMIT may be issued.
	fake := &fakeTx{execResult: fakeZeroResult{}, rows: []fakeRow{
		rowStatusIdentical(StatusPending, false),
		rowPayload([]byte(`{"event_id":"outbox-7"}`)),
	}}
	_ = Insert(context.Background(), fake, outboxEvent("outbox-7", "idem-7"))
	for _, query := range fake.queries {
		head := strings.ToUpper(strings.TrimSpace(query))
		if strings.HasPrefix(head, "BEGIN") || strings.HasPrefix(head, "COMMIT") {
			t.Fatalf("Insert must not manage transactions: %q", query)
		}
	}
	if len(fake.queries) != 3 {
		t.Fatalf("expected insert + event_id probe + payload probe, got %d statements: %v", len(fake.queries), fake.queries)
	}
}
