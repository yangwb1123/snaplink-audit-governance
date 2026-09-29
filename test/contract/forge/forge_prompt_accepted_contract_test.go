package forge

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"time"
)

// forgePromptAcceptedFixture mirrors Catalyst's canonical committed-Prompt
// evidence.  This is receiver compatibility only: it neither authorizes a
// publisher nor creates an outbox, task, device, or execution effect.
//
//go:embed testdata/forge-prompt-accepted-audit-v1.json
var forgePromptAcceptedFixture []byte

type forgePromptAccepted struct {
	EventID            string                     `json:"event_id"`
	TenantID           string                     `json:"tenant_id"`
	SourceSystem       string                     `json:"source_system"`
	EventType          string                     `json:"event_type"`
	SchemaID           string                     `json:"schema_id"`
	SchemaVersion      int                        `json:"schema_version"`
	OccurredAt         string                     `json:"occurred_at"`
	OperationID        string                     `json:"operation_id"`
	Actor              forgePromptAcceptedActor   `json:"actor"`
	AggregateType      string                     `json:"aggregate_type"`
	AggregateID        string                     `json:"aggregate_id"`
	AggregateVersion   int64                      `json:"aggregate_version"`
	Action             string                     `json:"action"`
	Outcome            string                     `json:"outcome"`
	Payload            forgePromptAcceptedPayload `json:"payload"`
	DataClassification string                     `json:"data_classification"`
	RetentionClass     string                     `json:"retention_class"`
	IdempotencyKey     string                     `json:"idempotency_key"`
}

type forgePromptAcceptedActor struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

type forgePromptAcceptedPayload struct {
	ContentIncluded bool `json:"content_included"`
}

func decodeForgePromptAccepted(raw []byte) (forgePromptAccepted, map[string]json.RawMessage, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgePromptAccepted{}, nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var event forgePromptAccepted
	if err := decoder.Decode(&event); err != nil {
		return forgePromptAccepted{}, nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgePromptAccepted{}, nil, io.ErrUnexpectedEOF
		}
		return forgePromptAccepted{}, nil, err
	}
	var shape map[string]json.RawMessage
	if err := json.Unmarshal(raw, &shape); err != nil {
		return forgePromptAccepted{}, nil, err
	}
	return event, shape, nil
}

func TestForgePromptAcceptedFixtureIsStrictAndContentFree(t *testing.T) {
	event, shape, err := decodeForgePromptAccepted(forgePromptAcceptedFixture)
	if err != nil {
		t.Fatalf("decode Forge accepted-Prompt fixture: %v", err)
	}
	assertJSONKeys(t, shape, []string{
		"event_id", "tenant_id", "source_system", "event_type", "schema_id",
		"schema_version", "occurred_at", "operation_id", "actor", "aggregate_type",
		"aggregate_id", "aggregate_version", "action", "outcome", "payload",
		"data_classification", "retention_class", "idempotency_key",
	}, "Forge accepted-Prompt event")
	if event.SourceSystem != "forge-runtime" || event.EventType != "forge.prompt.accepted.v1" ||
		event.SchemaID != event.EventType || event.SchemaVersion != 1 ||
		event.AggregateType != "forge_conversation" || event.Action != "append_prompt" ||
		event.Outcome != "accepted" || event.Payload.ContentIncluded {
		t.Fatalf("unexpected Forge accepted-Prompt contract: %#v", event)
	}
	if !forgePromptAcceptedSHA256(event.EventID) || !forgePromptAcceptedSHA256(event.Actor.ID) ||
		event.IdempotencyKey != event.EventID || event.Actor.Type != "user" {
		t.Fatalf("invalid Forge accepted-Prompt identity binding: %#v", event)
	}
	for name, value := range map[string]string{
		"tenant_id": event.TenantID, "operation_id": event.OperationID,
		"aggregate_id": event.AggregateID,
	} {
		if strings.TrimSpace(value) == "" || strings.ContainsAny(value, " :/\\") {
			t.Fatalf("unsafe %s=%q", name, value)
		}
	}
	if event.AggregateVersion <= 0 {
		t.Fatalf("aggregate_version=%d, want positive", event.AggregateVersion)
	}
	for _, forbidden := range []string{"prompt", "content", "output", "result", "token", "credential", "artifact"} {
		if _, present := shape[forbidden]; present {
			t.Fatalf("Forge accepted-Prompt event contains prohibited field %q", forbidden)
		}
	}
}

func TestForgePromptAcceptedRejectsUnknownAndDuplicateFields(t *testing.T) {
	var shape map[string]json.RawMessage
	if err := json.Unmarshal(forgePromptAcceptedFixture, &shape); err != nil {
		t.Fatal(err)
	}
	shape["prompt"] = json.RawMessage(`"raw prompt must not cross the audit boundary"`)
	unknown, err := json.Marshal(shape)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := decodeForgePromptAccepted(unknown); err == nil {
		t.Fatal("accepted-Prompt decoder accepted raw prompt content")
	}
	duplicate := strings.TrimSpace(string(forgePromptAcceptedFixture))
	duplicate = strings.TrimSuffix(duplicate, "}") + `,"event_id":"different"}`
	if _, _, err := decodeForgePromptAccepted([]byte(duplicate)); err == nil {
		t.Fatal("accepted-Prompt decoder accepted a duplicate root field")
	}
}

func TestForgePromptAcceptedCanEnterGovernanceEventBoundary(t *testing.T) {
	fixture, _, err := decodeForgePromptAccepted(forgePromptAcceptedFixture)
	if err != nil {
		t.Fatal(err)
	}
	occurredAt, err := time.Parse(time.RFC3339, fixture.OccurredAt)
	if err != nil {
		t.Fatal(err)
	}
	event := domain.Event{
		EventID: fixture.EventID, TenantID: fixture.TenantID, SourceSystem: fixture.SourceSystem,
		EventType: fixture.EventType, SchemaID: fixture.SchemaID, SchemaVersion: fixture.SchemaVersion,
		OccurredAt: occurredAt, OperationID: fixture.OperationID,
		Actor:         domain.Actor{ID: fixture.Actor.ID, Type: fixture.Actor.Type},
		AggregateType: fixture.AggregateType, AggregateID: fixture.AggregateID,
		AggregateVersion: fixture.AggregateVersion, Action: fixture.Action, Outcome: fixture.Outcome,
		Payload:            map[string]any{"content_included": fixture.Payload.ContentIncluded},
		DataClassification: fixture.DataClassification, RetentionClass: fixture.RetentionClass,
		IdempotencyKey: fixture.IdempotencyKey,
	}
	if err := event.ValidateBasic(); err != nil {
		t.Fatalf("Forge accepted-Prompt event rejected: %v", err)
	}
	if event.Payload["content_included"] != false || len(event.Payload) != 1 {
		t.Fatalf("accepted-Prompt payload is not minimized: %#v", event.Payload)
	}
}

func forgePromptAcceptedSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && strings.ToLower(value) == value
}
