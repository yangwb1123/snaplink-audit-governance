package domain

import (
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

func TestCursorRoundTrip(t *testing.T) {
	cursor := EncodeCursor(42, "evt/1")
	sequence, eventID, err := DecodeCursor(cursor)
	if err != nil {
		t.Fatal(err)
	}
	if sequence != 42 || eventID != "evt/1" {
		t.Fatalf("unexpected cursor: %d %s", sequence, eventID)
	}
}
