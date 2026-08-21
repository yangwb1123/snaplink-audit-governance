package domain

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestCanonicalJSONSortsNestedMaps(t *testing.T) {
	first, err := CanonicalJSON(map[string]any{"b": map[string]any{"z": 1, "a": 2}, "a": []any{"x", 1}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := CanonicalJSON(map[string]any{"a": []any{"x", 1}, "b": map[string]any{"a": 2, "z": 1}})
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatalf("canonical values differ: %s != %s", first, second)
	}
	if !strings.HasPrefix(string(first), `{"a"`) {
		t.Fatalf("keys were not sorted: %s", first)
	}
}

func TestEventDigestExcludesServerProcessingFields(t *testing.T) {
	event := Event{EventID: "evt-1", TenantID: "tenant-a", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(10, 0).UTC(), ReceivedAt: time.Unix(20, 0).UTC(), Actor: Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "idem-1", Payload: map[string]any{"b": 2, "a": 1}}
	first, err := EventDigest(event)
	if err != nil {
		t.Fatal(err)
	}
	event.ReceivedAt = time.Unix(30, 0).UTC()
	event.ServerVersion = "different"
	second, err := EventDigest(event)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("server fields changed digest: %s != %s", first, second)
	}
}

func TestEventContentDigestEqualsEventDigestWhenSourceDigestUnset(t *testing.T) {
	event := Event{EventID: "evt-1", TenantID: "tenant-a", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(10, 0).UTC(), ReceivedAt: time.Unix(20, 0).UTC(), Actor: Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "idem-1", Payload: map[string]any{"b": 2, "a": 1}}
	digest, err := EventDigest(event)
	if err != nil {
		t.Fatal(err)
	}
	contentDigest, err := EventContentDigest(event)
	if err != nil {
		t.Fatal(err)
	}
	if digest != contentDigest {
		t.Fatalf("EventDigest fallback must delegate to EventContentDigest: %s != %s", digest, contentDigest)
	}
	// A stored SourceDigest must never leak into the content derivation.
	signed := event
	signed.SourceDigest = "attacker-chosen-value"
	contentDigest2, err := EventContentDigest(signed)
	if err != nil {
		t.Fatal(err)
	}
	if contentDigest2 != contentDigest {
		t.Fatalf("EventContentDigest must ignore the stored SourceDigest field: %s != %s", contentDigest2, contentDigest)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	// The chronological 3-tuple round-trips exactly: RFC3339Nano time,
	// sequence and event_id survive the wire format.
	at := time.Date(2024, 1, 1, 0, 0, 0, 123456789, time.UTC)
	cursor := EncodeCursor(Cursor{OccurredAt: at, Sequence: 42, EventID: "evt/1"})
	decoded, err := DecodeCursor(cursor)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Legacy {
		t.Fatal("3-tuple cursor must decode as chronological, not legacy")
	}
	if !decoded.OccurredAt.Equal(at) || decoded.Sequence != 42 || decoded.EventID != "evt/1" {
		t.Fatalf("unexpected cursor: %+v", decoded)
	}
	// A zoned instant normalizes through CanonicalJSON (RFC3339Nano UTC) and
	// still round-trips to the same instant.
	zone := time.FixedZone("CEST", 2*3600)
	zoned := time.Date(2024, 1, 1, 0, 0, 0, 123456789, zone)
	zonedCursor := EncodeCursor(Cursor{OccurredAt: zoned, Sequence: 7, EventID: "evt-2"})
	decodedZoned, err := DecodeCursor(zonedCursor)
	if err != nil {
		t.Fatal(err)
	}
	if !decodedZoned.OccurredAt.Equal(zoned) || decodedZoned.Sequence != 7 || decodedZoned.EventID != "evt-2" || decodedZoned.Legacy {
		t.Fatalf("zoned cursor did not round-trip: %+v", decodedZoned)
	}
	// The legacy 2-tuple (the pre-chronological wire format) still parses,
	// flagged Legacy, with the original coordinates intact.
	legacy := EncodeCursor(Cursor{Sequence: 42, EventID: "evt/1", Legacy: true})
	decodedLegacy, err := DecodeCursor(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if !decodedLegacy.Legacy || decodedLegacy.Sequence != 42 || decodedLegacy.EventID != "evt/1" {
		t.Fatalf("legacy cursor did not decode: %+v", decodedLegacy)
	}
	// Fail-closed: garbage base64, non-array payload, wrong arity, and a
	// 3-tuple whose first element is not an RFC3339 time all yield ErrInvalid.
	bad := []string{
		"not-a-cursor",
		base64.RawURLEncoding.EncodeToString([]byte(`{}`)),
		base64.RawURLEncoding.EncodeToString([]byte(`["only-one"]`)),
		base64.RawURLEncoding.EncodeToString([]byte(`["a","b","c","d"]`)),
		base64.RawURLEncoding.EncodeToString([]byte(`["not-a-time",1,"evt-1"]`)),
		base64.RawURLEncoding.EncodeToString([]byte(`[1,"evt-1","extra"]`)),
	}
	for _, value := range bad {
		if _, err := DecodeCursor(value); !errors.Is(err, ErrInvalid) {
			t.Fatalf("cursor %q must fail closed with ErrInvalid, got %v", value, err)
		}
	}
}

func TestTimelineCursorRoundTripAndScopeValidation(t *testing.T) {
	when := time.Date(2024, 1, 1, 0, 0, 0, 123456789, time.UTC)
	operation := EncodeTimelineCursor(TimelineCursor{Kind: "operation", Scope: "op-1", OccurredAt: when, Sequence: 42, EventID: "evt-1"})
	decoded, err := DecodeTimelineCursor(operation)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Kind != "operation" || decoded.Scope != "op-1" || !decoded.OccurredAt.Equal(when) || decoded.Sequence != 42 || decoded.EventID != "evt-1" {
		t.Fatalf("unexpected operation timeline cursor: %+v", decoded)
	}
	aggregate := EncodeTimelineCursor(TimelineCursor{Kind: "aggregate", Scope: "invoice\x1finv-1", AggregateVersion: 7, EventID: "evt-7"})
	decoded, err = DecodeTimelineCursor(aggregate)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Kind != "aggregate" || decoded.Scope != "invoice\x1finv-1" || decoded.AggregateVersion != 7 || decoded.EventID != "evt-7" {
		t.Fatalf("unexpected aggregate timeline cursor: %+v", decoded)
	}
	for _, value := range []string{
		"not-a-cursor",
		base64.RawURLEncoding.EncodeToString([]byte(`{"v":2,"kind":"operation","scope":"op-1","event_id":"evt-1"}`)),
		base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"kind":"operation","scope":"op-1","event_id":"evt-1"}`)),
		base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"kind":"other","scope":"op-1","event_id":"evt-1"}`)),
	} {
		if _, err := DecodeTimelineCursor(value); !errors.Is(err, ErrInvalid) {
			t.Fatalf("timeline cursor %q must fail closed, got %v", value, err)
		}
	}
}

// TestCanonicalJSONExactInt64StructField is AC-1: a struct int64 field of
// 9007199254740993 (2^53+1) must be emitted as exact decimal digits — no
// "9.007199254740992e+15", no digit loss — and EventDigest of events
// differing by 1 at 2^53 must differ in both directions.
func TestCanonicalJSONExactInt64StructField(t *testing.T) {
	data, err := CanonicalJSON(struct {
		N int64 `json:"n"`
	}{N: 9007199254740993})
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"n":9007199254740993}` {
		t.Fatalf("int64 2^53+1 lost precision or changed form: %s", data)
	}

	at := time.Unix(10, 0).UTC()
	base := func(version int64) Event {
		return Event{EventID: "evt-1", TenantID: "tenant-a", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: at, Actor: Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "idem-1", AggregateVersion: version, Payload: map[string]any{"resource": "invoice"}}
	}
	first, err := EventDigest(base(9007199254740992))
	if err != nil {
		t.Fatal(err)
	}
	second, err := EventDigest(base(9007199254740993))
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("digests collide across a 1-unit difference at 2^53: %s", first)
	}
	if second == first {
		t.Fatalf("digests collide across a 1-unit difference at 2^53: %s", second)
	}
}

// TestCanonicalJSONNumberRepresentationsEqual is AC-2: json.Number("42"),
// int64(42) and float64(42) in a map must canonicalize to the same unquoted
// bytes {"n":42}.
func TestCanonicalJSONNumberRepresentationsEqual(t *testing.T) {
	fromNumber, err := CanonicalJSON(map[string]any{"n": json.Number("42")})
	if err != nil {
		t.Fatal(err)
	}
	fromInt, err := CanonicalJSON(map[string]any{"n": int64(42)})
	if err != nil {
		t.Fatal(err)
	}
	fromFloat, err := CanonicalJSON(map[string]any{"n": float64(42)})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"n":42}`
	for name, got := range map[string]string{"json.Number": string(fromNumber), "int64": string(fromInt), "float64": string(fromFloat)} {
		if got != want {
			t.Fatalf("%s representation canonicalized to %q, want %q", name, got, want)
		}
	}
}

// TestCanonicalJSONNormalizesNestedTimeUTC is AC-3: an Event whose
// OccurredAt carries a +02:00 offset must canonicalize byte-identically to
// the same instant expressed in UTC, both emitting RFC3339Nano UTC.
func TestCanonicalJSONNormalizesNestedTimeUTC(t *testing.T) {
	zone := time.FixedZone("CEST", 2*3600)
	target := time.Date(2024, 1, 1, 0, 0, 0, 0, zone)           // 2024-01-01T00:00:00+02:00
	utc := time.Date(2023, 12, 31, 22, 0, 0, 0, time.UTC)       // same instant
	nano := time.Date(2024, 1, 1, 0, 0, 0, 123456789, time.UTC) // sub-second pin
	withZone := Event{EventID: "evt-1", TenantID: "tenant-a", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: target, Actor: Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "idem-1", Payload: map[string]any{"resource": "invoice"}}
	withUTC := withZone
	withUTC.OccurredAt = utc
	a, err := CanonicalJSON(withZone)
	if err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalJSON(withUTC)
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatalf("zone and UTC forms of the same instant differ:\n%s\n%s", a, b)
	}
	if !strings.Contains(string(a), `"occurred_at":"2023-12-31T22:00:00Z"`) {
		t.Fatalf("nested time was not emitted in RFC3339Nano UTC: %s", a)
	}
	// any-typed nested times must normalize too (changed_fields / payload
	// values decoded into interface{}).
	fromAny, err := CanonicalJSON(struct {
		When any `json:"when"`
	}{When: time.Date(2024, 1, 1, 0, 0, 0, nano.Nanosecond(), zone)})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(fromAny), `"when":"2023-12-31T22:00:00.123456789Z"`) {
		t.Fatalf("any-typed time was not UTC-normalized: %s", fromAny)
	}
}

// TestCanonicalJSONRejectsNonFiniteAndOverflowNumbers is failure mode F-1:
// NaN/Infinity literals and float64-overflowing exponents must be rejected
// instead of emitting bytes that break digest injectivity.
func TestCanonicalJSONRejectsNonFiniteAndOverflowNumbers(t *testing.T) {
	cases := []any{
		json.Number("NaN"),
		json.Number("Infinity"),
		json.Number("1e9999"),
		json.Number("-1e9999"),
		json.Number("not-a-number"),
		math.NaN(),
		math.Inf(1),
		math.Inf(-1),
	}
	for _, value := range cases {
		if _, err := CanonicalJSON(map[string]any{"n": value}); err == nil {
			t.Fatalf("value %v must be rejected, got nil error", value)
		}
	}
}

// TestCanonicalJSONNumberRepresentationEquality is the FR-2 property beyond
// AC-2: numerically equal values arriving in different representations must
// canonicalize to identical bytes, and -0 must equal 0.
func TestCanonicalJSONNumberRepresentationEquality(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"exponent form", mustCanonical(t, map[string]any{"n": json.Number("1.5e3")}), `{"n":1500}`},
		{"trailing zero fraction", mustCanonical(t, map[string]any{"n": json.Number("1500.0")}), `{"n":1500}`},
		{"float equal to int", mustCanonical(t, map[string]any{"n": float64(1500)}), `{"n":1500}`},
		{"negative zero literal", mustCanonical(t, map[string]any{"n": json.Number("-0")}), `{"n":0}`},
		{"negative zero float", mustCanonical(t, map[string]any{"n": math.Copysign(0, -1)}), `{"n":0}`},
		{"positive zero float", mustCanonical(t, map[string]any{"n": float64(0)}), `{"n":0}`},
		{"fraction", mustCanonical(t, map[string]any{"n": json.Number("0.5")}), `{"n":0.5}`},
		{"fraction float", mustCanonical(t, map[string]any{"n": float64(0.5)}), `{"n":0.5}`},
		{"big float", mustCanonical(t, map[string]any{"n": json.Number("1e15")}), `{"n":1000000000000000}`},
		{"big float literal", mustCanonical(t, map[string]any{"n": json.Number("1000000000000000")}), `{"n":1000000000000000}`},
		{"big float64", mustCanonical(t, map[string]any{"n": float64(1e15)}), `{"n":1000000000000000}`},
		{"huge exact integer", mustCanonical(t, map[string]any{"n": json.Number("123456789012345678901234567890")}), `{"n":123456789012345678901234567890}`},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Fatalf("%s: canonicalized to %q, want %q", tc.name, tc.got, tc.want)
		}
	}
}

func mustCanonical(t *testing.T, value any) string {
	t.Helper()
	data, err := CanonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// TestCanonicalJSONGoldenBytesStable pins the byte-level and digest-level
// stability of unaffected values: an event whose numbers are small and whose
// times are already UTC canonicalizes to exactly the bytes (and digest) the
// pre-fix round-trip implementation produced. This is the compatibility
// contract: the fix must not churn digests of unaffected events.
func TestCanonicalJSONGoldenBytesStable(t *testing.T) {
	event := Event{EventID: "evt-1", TenantID: "tenant-a", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Date(2024, 1, 1, 0, 0, 0, 123456789, time.UTC), ReceivedAt: time.Unix(20, 0).UTC(), OperationID: "op-1", Actor: Actor{ID: "user-1", Type: "user", Roles: []string{"operator"}}, Targets: []Target{{Type: "invoice", ID: "inv-1"}}, AggregateType: "invoice", AggregateID: "inv-1", AggregateVersion: 7, Action: "update", Outcome: "success", Reason: "routine", ChangedFields: map[string]FieldChange{"status": {Before: "open", After: "paid"}}, Payload: map[string]any{"resource": "invoice", "value": 10, "nested": map[string]any{"z": 1, "a": 2}}, DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "idem-1"}
	wantBytes := `{"action":"update","actor":{"id":"user-1","roles":["operator"],"type":"user"},"aggregate_id":"inv-1","aggregate_type":"invoice","aggregate_version":7,"changed_fields":{"status":{"after":"paid","before":"open"}},"data_classification":"internal","event_id":"evt-1","event_type":"audit.event","idempotency_key":"idem-1","occurred_at":"2024-01-01T00:00:00.123456789Z","operation_id":"op-1","outcome":"success","payload":{"nested":{"a":2,"z":1},"resource":"invoice","value":10},"reason":"routine","received_at":"1970-01-01T00:00:20Z","retention_class":"standard","schema_id":"audit.event","schema_version":1,"source_system":"crm","targets":[{"id":"inv-1","type":"invoice"}],"tenant_id":"tenant-a"}`
	data, err := CanonicalJSON(event)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != wantBytes {
		t.Fatalf("canonical bytes changed for an unaffected event:\n%s", data)
	}
	digest, err := EventContentDigest(event)
	if err != nil {
		t.Fatal(err)
	}
	if digest != "d068ccc2e2e2507b7bd39137bb27b6867c5b5e3c694fe5ee58db6d4a34886559" {
		t.Fatalf("digest changed for an unaffected event: %s", digest)
	}
}

// TestCanonicalJSONStructOutputKeySorted pins the struct-branch ordering:
// keys must be emitted sorted byte-wise, matching the old round-trip shape.
func TestCanonicalJSONStructOutputKeySorted(t *testing.T) {
	data, err := CanonicalJSON(Event{EventID: "evt-1", TenantID: "tenant-a", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(10, 0).UTC(), Actor: Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "idem-1", Payload: map[string]any{"resource": "invoice"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(data), `{"action"`) {
		t.Fatalf("struct keys were not sorted byte-wise: %s", data)
	}
	// Actor is a nested struct: its keys must be sorted too (id, name, roles, type).
	if !strings.Contains(string(data), `"actor":{"id":"user-1"}`) {
		t.Fatalf("nested struct keys were not sorted: %s", data)
	}
}
