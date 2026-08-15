package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// TestEventStreamDerivation pins the derivation precedence contract that the
// ingest strip relies on: StreamID (legacy/read-path client value) wins when
// set, otherwise aggregate → operation → source. With the strip in place,
// Ingest only ever reaches the server-controlled branches; this test keeps
// the precedence from rotting.
func TestEventStreamDerivation(t *testing.T) {
	base := Event{TenantID: "tenant-a", SourceSystem: "crm", OccurredAt: time.Unix(1_700_000_000, 0).UTC()}
	cases := []struct {
		name  string
		event Event
		want  string
	}{
		{name: "client-value-wins", event: with(base, func(e *Event) { e.StreamID = "client-stream" }), want: "client-stream"},
		{name: "aggregate", event: with(base, func(e *Event) { e.AggregateType = "invoice"; e.AggregateID = "inv-1" }), want: "tenant-a:aggregate:invoice:inv-1"},
		{name: "operation", event: with(base, func(e *Event) { e.OperationID = "op-9" }), want: "tenant-a:operation:op-9"},
		{name: "source", event: base, want: "tenant-a:source:crm"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.event.Stream(); got != tc.want {
				t.Fatalf("Stream()=%q want %q", got, tc.want)
			}
		})
	}
}

// TestEventValidateBasicRequiredFields pins the validation gate Ingest runs
// before the strip: an event missing occurred_at, actor.id or payload is
// rejected regardless of any other fields.
func TestEventValidateBasicRequiredFields(t *testing.T) {
	valid := Event{
		EventID: "evt-1", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event",
		SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_000, 0).UTC(),
		Actor: Actor{ID: "user-1"}, Action: "update", Outcome: "success",
		DataClassification: "internal", RetentionClass: "standard",
		IdempotencyKey: "idem-1", Payload: map[string]any{"value": 1},
	}
	if err := valid.ValidateBasic(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
	for _, mutate := range []func(*Event){
		func(e *Event) { e.OccurredAt = time.Time{} },
		func(e *Event) { e.Actor = Actor{} },
		func(e *Event) { e.Payload = nil; e.PayloadRef = "" },
	} {
		event := valid
		mutate(&event)
		if err := event.ValidateBasic(); !errors.Is(err, ErrInvalid) {
			t.Fatalf("expected ErrInvalid, got %v", err)
		}
	}
}

func with(base Event, mutate func(*Event)) Event {
	mutate(&base)
	return base
}

// TestEventValidateBasicRejectsOutOfRangeOccurredAt is AC-1: ValidateBasic
// bounds OccurredAt to the ledger horizon [MinOccurredAt, MaxOccurredAt]
// (ClickHouse DateTime64 platform contract). Every rejected case must satisfy
// both errors.Is(err, ErrInvalid) (unwrap-chain compatibility for existing
// callers) and errors.Is(err, ErrOccurredAtOutOfRange) (distinguishable class
// for HTTP 422 and the projector's permanent classification). Boundaries are
// inclusive and judged on the UTC instant, matching Store.Insert's
// event.OccurredAt.UTC() binding.
func TestEventValidateBasicRejectsOutOfRangeOccurredAt(t *testing.T) {
	base := Event{
		EventID: "evt-1", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event",
		SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_000, 0).UTC(),
		Actor: Actor{ID: "user-1"}, Action: "update", Outcome: "success",
		DataClassification: "internal", RetentionClass: "standard",
		IdempotencyKey: "idem-1", Payload: map[string]any{"value": 1},
	}
	reject := func(name string, at time.Time) {
		t.Helper()
		event := base
		event.OccurredAt = at
		err := event.ValidateBasic()
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err=%v, want errors.Is(err, ErrInvalid)", name, err)
		}
		if !errors.Is(err, ErrOccurredAtOutOfRange) {
			t.Errorf("%s: err=%v, want errors.Is(err, ErrOccurredAtOutOfRange)", name, err)
		}
	}
	accept := func(name string, at time.Time) {
		t.Helper()
		event := base
		event.OccurredAt = at
		if err := event.ValidateBasic(); err != nil {
			t.Errorf("%s: err=%v, want nil", name, err)
		}
	}

	reject("year 1800", time.Date(1800, 1, 1, 0, 0, 0, 0, time.UTC))
	reject("floor minus 1ms", time.Date(1899, 12, 31, 23, 59, 59, 999_000_000, time.UTC))
	reject("ceiling plus 1ms", time.Date(2300, 1, 1, 0, 0, 0, 1_000_000, time.UTC))
	reject("year 2300", time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC))
	// 非 UTC 偏移按 UTC 时刻裁决（与投影层 event.OccurredAt.UTC() 绑定一致）。
	reject("ceiling plus 1ms via +08:00", time.Date(2300, 1, 1, 8, 0, 0, 0, time.FixedZone("UTC+8", 8*3600)))

	accept("floor inclusive", MinOccurredAt)
	accept("ceiling inclusive", MaxOccurredAt)
	// +08:00 07:59:59.999 的 UTC 时刻恰为 ceiling（2299-12-31T23:59:59.999Z），接受。
	accept("ceiling via +08:00", time.Date(2300, 1, 1, 7, 59, 59, 999_000_000, time.FixedZone("UTC+8", 8*3600)))

	// 零值仍优先报告 "occurred_at is required"（零值时间在 floor 之下，
	// 顺序冻结：zero-check 必须先于范围检查）。
	event := base
	event.OccurredAt = time.Time{}
	err := event.ValidateBasic()
	if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "occurred_at is required") {
		t.Fatalf("zero occurred_at err=%v, want 'occurred_at is required'", err)
	}
}
