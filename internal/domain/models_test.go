package domain

import (
	"errors"
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
