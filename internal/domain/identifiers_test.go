package domain

import (
	"errors"
	"math/rand"
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
		"123",
		"audit.event", // dotted ids used by schema fixtures
	}
	for _, id := range accepted {
		if err := ValidKeyComponent("tenant id", id); err != nil {
			t.Errorf("ValidKeyComponent(%q) = %v, want nil", id, err)
		}
	}
	// The generic rule itself stays colon-permissive: ':' is legal for every
	// non-tenant, non-stream caller (event_id, source_system, operation_id,
	// schema_id, source id, VerifyIntegrity's stream_id). The ':' decision
	// lives at the stream and tenant layers (ValidStreamComponent /
	// ValidTenantIDComponent), not here — 'a:b' was removed from the
	// accepted corpus above because the tenant-id *name* is now enforced by
	// the stricter tenant layer.
	if err := ValidKeyComponent("event id", "a:b"); err != nil {
		t.Errorf("generic rule must keep ':' legal for non-tenant/non-stream callers: %v", err)
	}
	// Stream layer: ValidStreamComponent adds exactly the ':' rejection to
	// the generic rule — every pre-existing rejection class still fires
	// through it, and ':' is rejected with a component-naming message.
	for _, tc := range []struct {
		name, value string
		want        string
	}{
		{"aggregate type", "a:b", "aggregate type must not contain ':'"},
		{"aggregate id", "b:c", "aggregate id must not contain ':'"},
		{"aggregate type", "a\x1fb", "aggregate type must not contain control characters"},
		{"aggregate id", "a b", "aggregate id must not contain whitespace"},
		{"aggregate type", "a/b", "aggregate type must not contain path separators"},
		{"aggregate id", `a\\b`, "aggregate id must not contain path separators"},
	} {
		if err := ValidStreamComponent(tc.name, tc.value); !errors.Is(err, ErrInvalid) {
			t.Errorf("ValidStreamComponent(%q,%q) = %v, want ErrInvalid", tc.name, tc.value, err)
		} else if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("ValidStreamComponent(%q,%q) = %v, must name the component (%q)", tc.name, tc.value, err, tc.want)
		}
	}
	// Tenant layer: ValidTenantIDComponent rejects ':' on top of the generic
	// rule (dev-token/stream-frame ambiguity); the generic rejections stay.
	for _, tc := range []struct {
		name, value string
		want        string
	}{
		{"tenant id", "a:b", "tenant id must not contain ':'"},
		{"tenant id", "a\x1fb", "tenant id must not contain control characters"},
		{"tenant id", "a b", "tenant id must not contain whitespace"},
	} {
		if err := ValidTenantIDComponent(tc.name, tc.value); !errors.Is(err, ErrInvalid) {
			t.Errorf("ValidTenantIDComponent(%q,%q) = %v, want ErrInvalid", tc.name, tc.value, err)
		} else if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("ValidTenantIDComponent(%q,%q) = %v, must name the component (%q)", tc.name, tc.value, err, tc.want)
		}
	}
	// Colon-free values keep passing both layered rules.
	for _, id := range []string{"tenant-a", "a-b_c.d", "tëstant", "123"} {
		if err := ValidStreamComponent("aggregate type", id); err != nil {
			t.Errorf("ValidStreamComponent(%q) = %v, want nil", id, err)
		}
		if err := ValidTenantIDComponent("tenant id", id); err != nil {
			t.Errorf("ValidTenantIDComponent(%q) = %v, want nil", id, err)
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
		{"aggregate_type colon (collision pair a)", func(e *Event) { e.AggregateType = "a:b"; e.AggregateID = "c" }},
		{"aggregate_id 0x1F", func(e *Event) { e.AggregateID = "a\x1fb" }},
		{"aggregate_id colon (collision pair b)", func(e *Event) { e.AggregateType = "a"; e.AggregateID = "b:c" }},
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

	// operation id stays on the generic (colon-permissive) rule: its frame
	// tenant:operation:<id> is single-component, injective by construction.
	opColon := keyFramingValidEvent()
	opColon.OperationID = "a:b"
	opColon.AggregateType = ""
	opColon.AggregateID = ""
	if err := opColon.ValidateBasic(); err != nil {
		t.Errorf("operation id with ':' must stay legal (single-component frame): %v", err)
	}
	if got := opColon.Stream(); got != ":operation:a:b" {
		t.Errorf("Stream() = %q, want %q (operation frame is colon-permissive)", got, ":operation:a:b")
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

	// Determinism across layers (FM-1): with event_id (generic layer) and
	// aggregate_type (stream layer) both invalid, the event_id loop still
	// reports first — never a map order — byte-identical across invocations.
	cross := keyFramingValidEvent()
	cross.EventID = "a\x1fb"
	cross.AggregateType = "a:b"
	cross.AggregateID = "c"
	crossFirst, crossSecond := cross.ValidateBasic(), cross.ValidateBasic()
	if crossFirst == nil || crossSecond == nil || crossFirst.Error() != crossSecond.Error() {
		t.Fatalf("nondeterministic cross-layer precedence: %v vs %v", crossFirst, crossSecond)
	}
	if !strings.Contains(crossFirst.Error(), "event id must not contain control characters") {
		t.Fatalf("event_id loop must win over the aggregate loop, got %v", crossFirst)
	}

	// Determinism within the aggregate loop: aggregate type precedes
	// aggregate id.
	bothAgg := keyFramingValidEvent()
	bothAgg.AggregateType = "a:b"
	bothAgg.AggregateID = "b:c"
	aggFirst, aggSecond := bothAgg.ValidateBasic(), bothAgg.ValidateBasic()
	if aggFirst == nil || aggSecond == nil || aggFirst.Error() != aggSecond.Error() {
		t.Fatalf("nondeterministic aggregate precedence: %v vs %v", aggFirst, aggSecond)
	}
	if !strings.Contains(aggFirst.Error(), "aggregate type must not contain ':'") {
		t.Fatalf("aggregate type must be reported before aggregate id, got %v", aggFirst)
	}
}

// aggregateFrame derives the stream key for a fixed tenant the same way
// Event.Stream does (models.go) — kept as a local literal so this test pins
// the framing contract independently of the model implementation.
func aggregateFrame(aggregateType, aggregateID string) string {
	return "tenant-a:aggregate:" + aggregateType + ":" + aggregateID
}

// TestStreamFrameInjectiveOverAcceptedPairs is REQ-3's structural pin: with
// the colon rule in place, for a fixed tenant the aggregate frame contains
// exactly one ':' in the remainder iff both components are colon-free, and
// the pair is recovered uniquely by splitting the remainder at the first
// ':'. Round-trip + no-two-distinct-pairs-share-a-frame, and the collision
// frames (("a","b:c") / ("a:b","c")) are derivable from zero accepted pairs
// (at-most-one property).
func TestStreamFrameInjectiveOverAcceptedPairs(t *testing.T) {
	pairs := [][2]string{
		{"invoice", "inv-1"},
		{"order", "PO-1004"},
		{"a", "b"},
		{"a-b_c.d", "x"},
		{"invoice", "inv-2"},
		{"tëstant", "123"},
	}
	for _, pair := range pairs {
		if err := ValidStreamComponent("aggregate type", pair[0]); err != nil {
			t.Fatalf("fixture %v: aggregate type invalid: %v", pair, err)
		}
		if err := ValidStreamComponent("aggregate id", pair[1]); err != nil {
			t.Fatalf("fixture %v: aggregate id invalid: %v", pair, err)
		}
	}
	seen := map[string][2]string{}
	for _, pair := range pairs {
		frame := aggregateFrame(pair[0], pair[1])
		if other, exists := seen[frame]; exists {
			t.Fatalf("frame collision: %v and %v both derive %q", pair, other, frame)
		}
		seen[frame] = pair
		// Round-trip: the remainder after the fixed prefix splits at the
		// first ':' into exactly (type, id).
		rest, ok := strings.CutPrefix(frame, "tenant-a:aggregate:")
		if !ok {
			t.Fatalf("frame %q missing the fixed aggregate prefix", frame)
		}
		typ, id, cut := strings.Cut(rest, ":")
		if !cut || typ != pair[0] || id != pair[1] {
			t.Fatalf("round-trip failed for %v: %q → (%q,%q,%v)", pair, rest, typ, id, cut)
		}
	}
	// At-most-one: the documented collision frames must be derivable from
	// zero accepted pairs.
	for _, collision := range [][2]string{{"a", "b:c"}, {"a:b", "c"}} {
		if other, exists := seen[aggregateFrame(collision[0], collision[1])]; exists {
			t.Fatalf("collision frame for %v is derivable from accepted pair %v", collision, other)
		}
	}
}

// TestStreamFrameFuzzColonFreeUnique is REQ-3's fuzz pin: a seeded PRNG over
// an alphabet including ':' — deterministic across CI runs. Every generated
// pair either contains ':' (then ValidateBasic rejects with ErrInvalid) or is
// colon-free (then its frame is unique across the whole corpus).
func TestStreamFrameFuzzColonFreeUnique(t *testing.T) {
	rng := rand.New(rand.NewSource(20240815))
	alphabet := []rune("ab:0123-_.")
	randString := func() string {
		n := 1 + rng.Intn(6)
		runes := make([]rune, n)
		for i := range runes {
			runes[i] = alphabet[rng.Intn(len(alphabet))]
		}
		return string(runes)
	}
	seen := map[string][2]string{}
	iterations := 10000
	for i := 0; i < iterations; i++ {
		typ, id := randString(), randString()
		typeErr := ValidStreamComponent("aggregate type", typ)
		idErr := ValidStreamComponent("aggregate id", id)
		if strings.ContainsRune(typ, ':') || strings.ContainsRune(id, ':') {
			// The offending component must be rejected, and ValidateBasic
			// must reject the event before any framing.
			if typeErr == nil && idErr == nil {
				t.Fatalf("iteration %d: colon-bearing pair (%q,%q) accepted", i, typ, id)
			}
			event := keyFramingValidEvent()
			event.AggregateType = typ
			event.AggregateID = id
			if err := event.ValidateBasic(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("iteration %d: ValidateBasic(%q,%q) = %v, want ErrInvalid", i, typ, id, err)
			}
			continue
		}
		if typeErr != nil || idErr != nil {
			t.Fatalf("iteration %d: colon-free pair (%q,%q) rejected: %v %v", i, typ, id, typeErr, idErr)
		}
		frame := aggregateFrame(typ, id)
		if other, exists := seen[frame]; exists {
			if other != [2]string{typ, id} {
				t.Fatalf("iteration %d: fuzz collision: (%q,%q) and %v share frame %q", i, typ, id, other, frame)
			}
			// Same pair repeated: deterministic function, not a collision.
			continue
		}
		seen[frame] = [2]string{typ, id}
	}
}

// TestStreamFramesCrossBranchDisjoint is REQ-3's cross-branch probe: the
// fixed literals ':aggregate:' / ':operation:' / ':source:' keep every
// accepted aggregate frame disjoint from every operation/source frame, even
// for adversarial component values like AggregateType="operation".
func TestStreamFramesCrossBranchDisjoint(t *testing.T) {
	probe := []string{"operation", "source", "aggregate", "a", "x", "inv-1"}
	frames := map[string]string{} // frame → branch that produced it
	for _, typ := range probe {
		for _, id := range probe {
			frame := aggregateFrame(typ, id)
			if prev, exists := frames[frame]; exists {
				t.Fatalf("aggregate frame %q already produced by %s", frame, prev)
			}
			frames[frame] = "aggregate"
		}
	}
	for _, op := range probe {
		frame := "tenant-a:operation:" + op
		if prev, exists := frames[frame]; exists {
			t.Fatalf("operation frame %q collides with %s frame", frame, prev)
		}
		frames[frame] = "operation"
	}
	for _, src := range probe {
		frame := "tenant-a:source:" + src
		if prev, exists := frames[frame]; exists {
			t.Fatalf("source frame %q collides with %s frame", frame, prev)
		}
		frames[frame] = "source"
	}
}
