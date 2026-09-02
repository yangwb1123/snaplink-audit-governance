package domain

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

type clonePayloadStruct struct {
	Label  string
	Nested map[string]any
	Values []json.Number
}

type unsafeCloneStruct struct {
	Visible string
	hidden  string
}

type cloneNode struct {
	Next *cloneNode
}

func TestCloneEventDeepCopyPreservesNilEmptyAndNumbers(t *testing.T) {
	event := Event{
		EventID:            "evt-clone",
		TenantID:           "tenant-a",
		SourceSystem:       "crm",
		EventType:          "audit.event",
		SchemaID:           "audit.event",
		SchemaVersion:      2,
		OccurredAt:         time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC),
		ReceivedAt:         time.Date(2024, 1, 2, 3, 5, 5, 6, time.UTC),
		OperationID:        "op-1",
		CausationID:        "cause-1",
		CorrelationID:      "corr-1",
		TraceID:            "trace-1",
		SpanID:             "span-1",
		Actor:              Actor{ID: "user-1", Type: "user", Roles: []string{"operator"}},
		Targets:            []Target{{Type: "invoice", ID: "inv-1", Name: "January"}},
		AggregateType:      "invoice",
		AggregateID:        "inv-1",
		AggregateVersion:   7,
		Action:             "update",
		Outcome:            "success",
		ChangedFields:      map[string]FieldChange{"status": {Before: map[string]any{"code": json.Number("1")}, After: []any{map[string]any{"state": "paid"}}}},
		Payload:            map[string]any{"nil_slice": []any(nil), "empty_slice": []any{}, "nil_map": map[string]any(nil), "empty_map": map[string]any{}, "number": json.Number("1234567890123456789012345678901234567890"), "array": [2]any{json.Number("7"), map[string]any{"deep": "value"}}, "pointer": &clonePayloadStruct{Label: "ptr", Nested: map[string]any{"role": "operator"}, Values: []json.Number{json.Number("8")}}, "struct": clonePayloadStruct{Label: "struct", Nested: map[string]any{"field": "value"}, Values: []json.Number{json.Number("9")}}},
		PayloadRef:         "payload-ref",
		SourceDigest:       "source-digest",
		DataClassification: "internal",
		RetentionClass:     "standard",
		IdempotencyKey:     "idem-1",
		StreamID:           "tenant-a:aggregate:invoice:inv-1",
		Sequence:           11,
		PrevHash:           "prev-hash",
		Hash:               "hash",
		ServerVersion:      "server-v1",
	}

	cloned, err := CloneEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cloned, event) {
		t.Fatalf("CloneEvent changed content: got=%+v want=%+v", cloned, event)
	}
	originalDigest, err := EventDigest(event)
	if err != nil {
		t.Fatal(err)
	}
	clonedDigest, err := EventDigest(cloned)
	if err != nil {
		t.Fatal(err)
	}
	if originalDigest != clonedDigest {
		t.Fatalf("digest mismatch original=%s cloned=%s", originalDigest, clonedDigest)
	}

	assertNilSlice := func(name string, value any) {
		t.Helper()
		slice, ok := value.([]any)
		if !ok || slice != nil {
			t.Fatalf("%s = %#v, want typed nil []any", name, value)
		}
	}
	assertEmptySlice := func(name string, value any) {
		t.Helper()
		slice, ok := value.([]any)
		if !ok || slice == nil || len(slice) != 0 {
			t.Fatalf("%s = %#v, want non-nil empty []any", name, value)
		}
	}
	assertNilMap := func(name string, value any) {
		t.Helper()
		m, ok := value.(map[string]any)
		if !ok || m != nil {
			t.Fatalf("%s = %#v, want typed nil map[string]any", name, value)
		}
	}
	assertEmptyMap := func(name string, value any) {
		t.Helper()
		m, ok := value.(map[string]any)
		if !ok || m == nil || len(m) != 0 {
			t.Fatalf("%s = %#v, want non-nil empty map[string]any", name, value)
		}
	}
	assertNilSlice("payload.nil_slice", cloned.Payload["nil_slice"])
	assertEmptySlice("payload.empty_slice", cloned.Payload["empty_slice"])
	assertNilMap("payload.nil_map", cloned.Payload["nil_map"])
	assertEmptyMap("payload.empty_map", cloned.Payload["empty_map"])
	if got, ok := cloned.Payload["number"].(json.Number); !ok || got != json.Number("1234567890123456789012345678901234567890") {
		t.Fatalf("payload.number = %#v, want preserved json.Number", cloned.Payload["number"])
	}

	cloned.Actor.Roles[0] = "admin"
	cloned.Targets[0].ID = "inv-9"
	status := cloned.ChangedFields["status"]
	status.Before.(map[string]any)["code"] = json.Number("2")
	status.After.([]any)[0].(map[string]any)["state"] = "closed"
	cloned.ChangedFields["status"] = status
	cloned.Payload["empty_map"].(map[string]any)["extra"] = true
	array := cloned.Payload["array"].([2]any)
	array[1].(map[string]any)["deep"] = "mutated"
	cloned.Payload["array"] = array
	pointer := cloned.Payload["pointer"].(*clonePayloadStruct)
	pointer.Nested["role"] = "auditor"
	pointer.Values[0] = json.Number("10")
	copyStruct := cloned.Payload["struct"].(clonePayloadStruct)
	copyStruct.Nested["field"] = "mutated"
	copyStruct.Values[0] = json.Number("11")
	cloned.Payload["struct"] = copyStruct

	if event.Actor.Roles[0] != "operator" {
		t.Fatalf("original actor roles mutated: %v", event.Actor.Roles)
	}
	if event.Targets[0].ID != "inv-1" {
		t.Fatalf("original targets mutated: %+v", event.Targets)
	}
	if got := event.ChangedFields["status"].Before.(map[string]any)["code"]; got != json.Number("1") {
		t.Fatalf("original changed_fields.before mutated: %#v", got)
	}
	if got := event.ChangedFields["status"].After.([]any)[0].(map[string]any)["state"]; got != "paid" {
		t.Fatalf("original changed_fields.after mutated: %#v", got)
	}
	if len(event.Payload["empty_map"].(map[string]any)) != 0 {
		t.Fatalf("original payload.empty_map mutated: %#v", event.Payload["empty_map"])
	}
	if got := event.Payload["array"].([2]any)[1].(map[string]any)["deep"]; got != "value" {
		t.Fatalf("original payload.array mutated: %#v", got)
	}
	if got := event.Payload["pointer"].(*clonePayloadStruct).Nested["role"]; got != "operator" {
		t.Fatalf("original payload.pointer nested mutated: %#v", got)
	}
	if got := event.Payload["pointer"].(*clonePayloadStruct).Values[0]; got != json.Number("8") {
		t.Fatalf("original payload.pointer values mutated: %#v", got)
	}
	if got := event.Payload["struct"].(clonePayloadStruct).Nested["field"]; got != "value" {
		t.Fatalf("original payload.struct nested mutated: %#v", got)
	}
	if got := event.Payload["struct"].(clonePayloadStruct).Values[0]; got != json.Number("9") {
		t.Fatalf("original payload.struct values mutated: %#v", got)
	}
}

func TestCloneEventPreservesRootNilAndEmptyCollections(t *testing.T) {
	event := Event{
		EventID:            "evt-root-collections",
		TenantID:           "tenant-a",
		SourceSystem:       "crm",
		EventType:          "audit.event",
		SchemaID:           "audit.event",
		SchemaVersion:      1,
		OccurredAt:         time.Unix(1, 0).UTC(),
		Actor:              Actor{ID: "user-1", Roles: nil},
		Targets:            []Target{},
		ChangedFields:      nil,
		Payload:            map[string]any{},
		Action:             "update",
		Outcome:            "success",
		DataClassification: "internal",
		RetentionClass:     "standard",
		IdempotencyKey:     "idem-root",
	}
	cloned, err := CloneEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	if cloned.Actor.Roles != nil {
		t.Fatalf("roles = %#v, want nil", cloned.Actor.Roles)
	}
	if cloned.Targets == nil || len(cloned.Targets) != 0 {
		t.Fatalf("targets = %#v, want non-nil empty", cloned.Targets)
	}
	if cloned.ChangedFields != nil {
		t.Fatalf("changed_fields = %#v, want nil", cloned.ChangedFields)
	}
	if cloned.Payload == nil || len(cloned.Payload) != 0 {
		t.Fatalf("payload = %#v, want non-nil empty", cloned.Payload)
	}
}

func TestCloneEventRejectsCyclesAndUnsupportedValues(t *testing.T) {
	cycle := map[string]any{}
	cycle["self"] = cycle
	node := &cloneNode{}
	node.Next = node
	cases := []struct {
		name  string
		value any
	}{
		{name: "cycle map", value: cycle},
		{name: "cycle pointer", value: node},
		{name: "func", value: func() {}},
		{name: "channel", value: make(chan int)},
		{name: "non-string-key map", value: map[int]string{1: "x"}},
		{name: "unsafe struct", value: unsafeCloneStruct{Visible: "ok", hidden: "secret"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event := Event{
				EventID:            "evt-bad",
				TenantID:           "tenant-a",
				SourceSystem:       "crm",
				EventType:          "audit.event",
				SchemaID:           "audit.event",
				SchemaVersion:      1,
				OccurredAt:         time.Unix(1, 0).UTC(),
				Actor:              Actor{ID: "user-1"},
				Action:             "update",
				Outcome:            "success",
				DataClassification: "internal",
				RetentionClass:     "standard",
				IdempotencyKey:     "idem-bad",
				Payload:            map[string]any{"bad": tc.value},
			}
			cloned, err := CloneEvent(event)
			if !errors.Is(err, ErrEventClone) {
				t.Fatalf("error = %v, want ErrEventClone", err)
			}
			if !reflect.DeepEqual(cloned, Event{}) {
				t.Fatalf("clone = %+v, want zero event on failure", cloned)
			}
		})
	}
}
