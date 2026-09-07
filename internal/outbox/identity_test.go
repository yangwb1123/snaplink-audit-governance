package outbox

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestScanRowOccurredAtPreservesFiniteAndRejectsInfinity(t *testing.T) {
	finite := time.Date(2026, 8, 4, 12, 0, 0, 123456000, time.FixedZone("offset", 2*60*60))
	got, valid, raw, rawValid := scanRowOccurredAt(finite)
	if !valid || rawValid || raw != "" || !got.Equal(finite) {
		t.Fatalf("finite scan=(%v,%v,%q,%v), want finite time without raw value", got, valid, raw, rawValid)
	}
	got, valid, raw, rawValid = scanRowOccurredAt("infinity")
	if valid || !rawValid || raw != "infinity" || !got.IsZero() {
		t.Fatalf("infinity scan=(%v,%v,%q,%v), want invalid raw infinity", got, valid, raw, rawValid)
	}
	got, valid, raw, rawValid = scanRowOccurredAt(nil)
	if valid || rawValid || raw != "" || !got.IsZero() {
		t.Fatalf("NULL scan=(%v,%v,%q,%v), want invalid empty value", got, valid, raw, rawValid)
	}
}

func TestIdentityMismatchReportsEveryChangedField(t *testing.T) {
	occurredAt := time.Date(2026, 8, 4, 12, 0, 0, 123000000, time.UTC)
	payload := domain.Event{
		EventID:        "payload-event",
		TenantID:       "payload-tenant",
		IdempotencyKey: "payload-idem",
		OccurredAt:     occurredAt,
	}
	row := rowIdentity{
		EventID:        "row-event",
		EventIDValid:   true,
		TenantID:       "row-tenant",
		TenantIDValid:  true,
		IdempotencyKey: "row-idem",
		IdemKeyValid:   true,
		OccurredAt:     occurredAt.Add(time.Second),
		OccurredValid:  true,
	}
	err := identityMismatch(91, row, payload)
	if err == nil {
		t.Fatal("identityMismatch() = nil, want mismatch")
	}
	message := err.Error()
	for _, want := range []string{
		"outbox identity mismatch id=91",
		`event_id row="row-event" payload="payload-event"`,
		`tenant_id row="row-tenant" payload="payload-tenant"`,
		`idempotency_key row="row-idem" payload="payload-idem"`,
		`occurred_at row="2026-08-04T12:00:01.123Z" payload="2026-08-04T12:00:00.123Z"`,
	} {
		if !strings.Contains(message, want) {
			t.Errorf("diagnostic=%q, want %q", message, want)
		}
	}
}

func TestIdentityMismatchUsesInstantEquality(t *testing.T) {
	zone := time.FixedZone("UTC+02", 2*60*60)
	payloadTime := time.Date(2026, 8, 4, 14, 0, 0, 123000000, zone)
	row := rowIdentity{
		EventID:        "event",
		EventIDValid:   true,
		TenantID:       "tenant",
		TenantIDValid:  true,
		IdempotencyKey: "idem",
		IdemKeyValid:   true,
		OccurredAt:     payloadTime.UTC(),
		OccurredValid:  true,
	}
	payload := domain.Event{EventID: "event", TenantID: "tenant", IdempotencyKey: "idem", OccurredAt: payloadTime}
	if err := identityMismatch(92, row, payload); err != nil {
		t.Fatalf("same instant reported as mismatch: %v", err)
	}
}

func TestIdentityMismatchRejectsEmptyAndMissingValues(t *testing.T) {
	payload := domain.Event{}
	row := rowIdentity{EventIDValid: true, TenantIDValid: true, IdemKeyValid: true, OccurredValid: true}
	err := identityMismatch(93, row, payload)
	if err == nil {
		t.Fatal("empty identity values accepted")
	}
	message := err.Error()
	for _, field := range []string{"event_id", "tenant_id", "idempotency_key", "occurred_at"} {
		if !strings.Contains(message, field) {
			t.Errorf("diagnostic=%q, missing field %q", message, field)
		}
	}
}

func TestRelayQuarantineMissingDiagnosticStillFailsClosed(t *testing.T) {
	store := newFakeStore(testRecord(1))
	store.mu.Lock()
	store.corrupt[1] = CorruptRecord{ID: 1, Attempts: 0}
	store.mu.Unlock()
	deliveries := 0
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) (*domain.EventReceipt, error) {
		deliveries++
		return nil, nil
	})
	if handled, err := relay.RunOnce(context.Background()); err != nil || handled != 1 {
		t.Fatalf("RunOnce handled=%d err=%v, want 1/nil", handled, err)
	}
	if deliveries != 0 {
		t.Fatalf("deliveries=%d, want 0 for a report without a diagnostic", deliveries)
	}
	record := store.record(1)
	if record.Status != StatusFailed || record.Attempts != 1 || !strings.Contains(record.LastError, "has no diagnostic") {
		t.Fatalf("record=%+v, want failed/1 with fallback diagnostic", record)
	}
}

func TestRelayQuarantinesReportedIdentityMismatch(t *testing.T) {
	store := newFakeStore(testRecord(1), testRecord(2))
	store.withCorrupt(2, fmt.Errorf("outbox identity mismatch id=2: event_id row=%q payload=%q", "row-event", "payload-event"))
	delivered := 0
	relay := fixedRelay(store, func(_ context.Context, event domain.Event) (*domain.EventReceipt, error) {
		delivered++
		if event.EventID != "event-1" {
			t.Errorf("delivered event_id=%q, want event-1", event.EventID)
		}
		return nil, nil
	})
	handled, err := relay.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if handled != 2 || delivered != 1 {
		t.Fatalf("handled=%d delivered=%d, want 2/1", handled, delivered)
	}
	if record := store.record(2); record.Status != StatusFailed || record.Attempts != 1 || !strings.Contains(record.LastError, "event_id") {
		t.Fatalf("mismatched record=%+v, want failed/1 with diagnostic", record)
	}
}
