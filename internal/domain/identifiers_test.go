package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// TestValidKeyComponent is REQ-1 at the canonical rule: control characters
// (incl. the 0x1F key separator), whitespace and path separators are
// rejected with domain.ErrInvalid; everything else passes. Rejection-based
// (not allowlist) semantics: non-ASCII letters and punctuation stay valid.
func TestValidKeyComponent(t *testing.T) {
	rejected := []string{
		"a\x1fb",   // KeySeparator injection: the cross-tenant leak vector
		"\x00",     // NUL
		"a\tb",     // TAB
		"a\nb",     // LF
		"a\rb",     // CR
		"a\vb",     // VT
		"a\fb",     // FF
		"a b",      // space
		" a",       // leading space
		"a\u00a0b", // NBSP (unicode.IsSpace)
		"a/b",      // path separator
		`a\b`,      // path separator (windows)
	}
	for _, id := range rejected {
		if err := ValidKeyComponent("tenant id", id); !errors.Is(err, ErrInvalid) {
			t.Errorf("ValidKeyComponent(%q) = %v, want ErrInvalid", id, err)
		}
	}
	accepted := []string{
		"tenant-a",
		"a-b_c.d",
		"tëstant", // non-ASCII letters stay valid (rejection-based rule)
		"a:b",
		"123",
		"audit.event", // dotted ids used by schema fixtures
	}
	for _, id := range accepted {
		if err := ValidKeyComponent("tenant id", id); err != nil {
			t.Errorf("ValidKeyComponent(%q) = %v, want nil", id, err)
		}
	}
	// The error message names the component so operators can find the
	// offending field without decoding the error envelope.
	if err := ValidKeyComponent("event id", "a\x1fb"); err == nil ||
		!strings.Contains(err.Error(), "event id must not contain control characters") {
		t.Fatalf("message must name the component, got %v", err)
	}
	if err := ValidKeyComponent("source id", "a b"); err == nil ||
		!strings.Contains(err.Error(), "source id must not contain whitespace") {
		t.Fatalf("message must name the component, got %v", err)
	}
	if err := ValidKeyComponent("schema id", "a/b"); err == nil ||
		!strings.Contains(err.Error(), "schema id must not contain path separators") {
		t.Fatalf("message must name the component, got %v", err)
	}
}

func keyFramingValidEvent() Event {
	return Event{
		EventID: "evt-1", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event",
		SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_000, 0).UTC(),
		Actor: Actor{ID: "user-1"}, Action: "update", Outcome: "success",
		DataClassification: "internal", RetentionClass: "standard",
		IdempotencyKey: "idem-1", Payload: map[string]any{"value": 1},
	}
}

// TestValidateBasicKeyFramingFields is REQ-2 at the model boundary: all five
// fields that become composite-key components (event_id, source_system via
// EventKey; aggregate_type/aggregate_id/operation_id via Event.Stream) are
// rejected when they carry 0x1F, other control characters, whitespace or
// path separators. The three optional stream components are validated only
// when non-empty (F-7): an empty optional component never derives a key
// segment, but any non-empty value is checked regardless of which branch
// Event.Stream would take.
func TestValidateBasicKeyFramingFields(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Event)
	}{
		{"event_id 0x1F", func(e *Event) { e.EventID = "a\x1fb" }},
		{"event_id space", func(e *Event) { e.EventID = "a b" }},
		{"event_id slash", func(e *Event) { e.EventID = "a/b" }},
		{"event_id backslash", func(e *Event) { e.EventID = `a\b` }},
		{"event_id NUL", func(e *Event) { e.EventID = "\x00-nul" }},
		{"source_system 0x1F", func(e *Event) { e.SourceSystem = "x\x1fy" }},
		{"source_system space", func(e *Event) { e.SourceSystem = "x y" }},
		{"source_system slash", func(e *Event) { e.SourceSystem = "x/y" }},
		{"aggregate_type 0x1F alone", func(e *Event) { e.AggregateType = "a\x1fb"; e.AggregateID = "" }},
		{"aggregate_id 0x1F", func(e *Event) { e.AggregateID = "a\x1fb" }},
		{"operation_id 0x1F", func(e *Event) { e.OperationID = "a\x1fb" }},
		{"operation_id space", func(e *Event) { e.OperationID = "op ok" }},
		{"operation_id slash", func(e *Event) { e.OperationID = "op/ok" }},
	} {
		event := keyFramingValidEvent()
		tc.mutate(&event)
		if err := event.ValidateBasic(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: ValidateBasic = %v, want ErrInvalid", tc.name, err)
		}
	}

	// Empty optional stream components stay legal (they never become key
	// components): the event must pass with no aggregate and no operation.
	event := keyFramingValidEvent()
	if err := event.ValidateBasic(); err != nil {
		t.Fatalf("event with empty optional stream fields rejected: %v", err)
	}

	// Determinism (F-3): with both event_id and source_system invalid, the
	// ordered loop reports the first field every time — never a random map
	// order. Repeated invocations must produce byte-identical errors.
	both := keyFramingValidEvent()
	both.EventID = "a\x1fb"
	both.SourceSystem = "x\x1fy"
	first, second := both.ValidateBasic(), both.ValidateBasic()
	if first == nil || second == nil || first.Error() != second.Error() {
		t.Fatalf("nondeterministic precedence: %v vs %v", first, second)
	}
	if !strings.Contains(first.Error(), "event id must not contain control characters") {
		t.Fatalf("ordered precedence must report event id first, got %v", first)
	}
}
