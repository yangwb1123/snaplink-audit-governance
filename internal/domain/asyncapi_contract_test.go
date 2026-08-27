package domain

// AsyncAPI EventEnvelope contract pins (REQ-4/5/6). The ingest contract in
// api/asyncapi/asyncapi.yaml must document exactly what ValidateBasic and
// Ingest enforce; these tests bind the spec text to the domain constants and
// rules so a drift on either side is red before merge:
//
//   - REQ-4 occurred_at horizon annotation == the rendered domain constants
//     (time.RFC3339Nano: plain RFC3339 drops the .999 millisecond ceiling and
//     would degrade the annotation to a fraction-less string, diverging from
//     openapi.yaml and the ClickHouse DateTime64 .999 projection ceiling).
//   - REQ-5 schema_version minimum: 1 mirrors the positive check.
//   - REQ-6 stream_id "Server-assigned / stripped" mirrors the Ingest strip
//     (checks/stream_consistency.py pins the runtime half).
//
// The both-payload-set runtime guard (REQ-3 second half) locks the fact that
// EventEnvelope's disjunction is anyOf, never oneOf: ValidateBasic rejects
// only the both-absent case today.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func readAsyncAPISpec(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "api", "asyncapi", "asyncapi.yaml"))
	if err != nil {
		t.Fatalf("read asyncapi.yaml: %v", err)
	}
	return string(raw)
}

// TestAsyncAPIEventEnvelopeDocumentsOccurredAtHorizon is REQ-4: the
// occurred_at property description must carry exactly the rendered ledger
// horizon window, derived from the domain constants (not a hard-coded
// literal) so a constant change trips the test instead of silently
// diverging the contract.
func TestAsyncAPIActiveChannelCatalogAndRefs(t *testing.T) {
	spec := readAsyncAPISpec(t)
	channelSection := strings.SplitN(spec, "operations:", 2)[0]
	var channels []string
	for _, line := range strings.Split(channelSection, "\n") {
		if strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "    ") && strings.HasSuffix(line, ":") {
			channels = append(channels, strings.TrimSpace(strings.TrimSuffix(line, ":")))
		}
	}
	if strings.Join(channels, ",") != "accepted,ledgered,dlq" {
		t.Fatalf("active channels=%v, want [accepted ledgered dlq]", channels)
	}
	for _, name := range []string{"  accepted:", "  ledgered:", "  dlq:"} {
		if !strings.Contains(channelSection, name) {
			t.Fatalf("active channel %q missing", strings.TrimSpace(name))
		}
	}
	for _, name := range []string{"  projection:", "  archive:"} {
		if strings.Contains(channelSection, name) {
			t.Fatalf("inactive channel %q must not be declared", strings.TrimSpace(name))
		}
	}
	for _, ref := range []string{
		"$ref: '#/components/messages/AcceptedEvent'",
		"$ref: '#/components/messages/LedgeredEvent'",
		"$ref: '#/components/messages/Failure'",
	} {
		if !strings.Contains(channelSection, ref) {
			t.Fatalf("active channel reference %q missing", ref)
		}
	}
	for _, binding := range []string{
		"description: MUST equal the AcceptedEvent payload's event_id.",
		"description: MUST equal the LedgeredEvent payload's event_id.",
		"description: MUST equal the Failure payload's event_id.",
	} {
		if !strings.Contains(channelSection, binding) {
			t.Fatalf("Kafka key binding %q missing", binding)
		}
	}
	if strings.Contains(spec, "publishProjection") || strings.Contains(spec, "publishArchive") {
		t.Fatal("projection/archive operations must not be declared")
	}
	if !strings.Contains(spec, "AuditEvent:\n      deprecated: true") {
		t.Fatal("AuditEvent compatibility alias must remain deprecated")
	}
}

func TestAsyncAPIEventEnvelopeDocumentsOccurredAtHorizon(t *testing.T) {
	spec := readAsyncAPISpec(t)
	for _, line := range strings.Split(spec, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "occurred_at:") || !strings.Contains(line, "{") {
			continue
		}
		open := strings.Index(line, "[")
		closeAt := strings.LastIndex(line, "]")
		if open < 0 || closeAt <= open {
			t.Fatalf("occurred_at line %q carries no [...] horizon window", line)
		}
		window := line[open : closeAt+1]
		want := fmt.Sprintf("[%s, %s]",
			MinOccurredAt.Format(time.RFC3339Nano), MaxOccurredAt.Format(time.RFC3339Nano))
		if window != want {
			t.Fatalf("occurred_at window=%s, want %s (derived from Min/MaxOccurredAt via RFC3339Nano)", window, want)
		}
		return
	}
	t.Fatal("asyncapi.yaml EventEnvelope occurred_at property line not found")
}

// TestAsyncAPIEventEnvelopeDocumentsSchemaVersionFloor is REQ-5: the spec
// declares minimum: 1 exactly as ValidateBasic enforces SchemaVersion > 0.
func TestAsyncAPIEventEnvelopeDocumentsSchemaVersionFloor(t *testing.T) {
	spec := readAsyncAPISpec(t)
	if !strings.Contains(spec, "schema_version: { type: integer, minimum: 1 }") {
		t.Fatal("asyncapi.yaml missing schema_version: { type: integer, minimum: 1 }")
	}
}

// TestAsyncAPILedgeredPrevHashConditionality checks that tooling-visible
// conditional rules match the runtime validation boundaries.
func TestAsyncAPILedgeredPrevHashConditionality(t *testing.T) {
	spec := readAsyncAPISpec(t)
	ledgered := strings.SplitN(spec, "    LedgeredEvent:\n", 2)
	if len(ledgered) != 2 {
		t.Fatal("asyncapi.yaml LedgeredEvent schema is missing")
	}
	section := strings.SplitN(ledgered[1], "    IngestBatch:\n", 2)[0]
	for _, fragment := range []string{
		"const: 1",
		"enum: ['']",
		"minimum: 2",
		"required: [prev_hash]",
		"pattern: '\\S'",
	} {
		if !strings.Contains(section, fragment) {
			t.Fatalf("LedgeredEvent schema missing conditional fragment %q", fragment)
		}
	}
}

// TestSchemaVersionZeroRejectedByValidateBasic is REQ-5 runtime half: the
// positive check must stay aligned with the documented minimum.
func TestSchemaVersionZeroRejectedByValidateBasic(t *testing.T) {
	base := Event{
		EventID: "evt-1", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event",
		SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_000, 0).UTC(),
		Actor: Actor{ID: "user-1"}, Action: "update", Outcome: "success",
		DataClassification: "internal", RetentionClass: "standard",
		IdempotencyKey: "idem-1", Payload: map[string]any{"value": 1},
	}
	event := base
	event.SchemaVersion = 0
	err := event.ValidateBasic()
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("SchemaVersion 0 err=%v, want errors.Is(err, ErrInvalid)", err)
	}
	event = base
	if err := event.ValidateBasic(); err != nil {
		t.Fatalf("SchemaVersion 1 err=%v, want nil", err)
	}
}

// TestAsyncAPIEventEnvelopeStreamIDDocumentsStrip is REQ-6: the spec
// documents the server strip; internal/service/service.go Ingest performs
// event.StreamID = "" (pinned by checks/stream_consistency.py).
func TestAsyncAPIEventEnvelopeStreamIDDocumentsStrip(t *testing.T) {
	spec := readAsyncAPISpec(t)
	for _, line := range strings.Split(spec, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "stream_id:") {
			continue
		}
		if !strings.Contains(line, "Server-assigned") {
			t.Fatalf("stream_id line %q missing 'Server-assigned'", line)
		}
		if !strings.Contains(line, "stripped") {
			t.Fatalf("stream_id line %q missing 'stripped'", line)
		}
		return
	}
	t.Fatal("asyncapi.yaml EventEnvelope stream_id property line not found")
}

// TestEventEnvelopePayloadDisjunctionIsAnyOf is REQ-3 runtime half: both
// payload AND payload_ref together must pass ValidateBasic (only the
// both-absent case is rejected), so the documented disjunction is anyOf —
// oneOf would reject runtime-conformant producers.
func TestEventEnvelopePayloadDisjunctionIsAnyOf(t *testing.T) {
	event := Event{
		EventID: "evt-1", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event",
		SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_000, 0).UTC(),
		Actor: Actor{ID: "user-1"}, Action: "update", Outcome: "success",
		DataClassification: "internal", RetentionClass: "standard",
		IdempotencyKey: "idem-1",
		Payload:        map[string]any{"value": 1},
		PayloadRef:     "s3://bucket/event/evt-1.json",
	}
	if err := event.ValidateBasic(); err != nil {
		t.Fatalf("both payload and payload_ref set err=%v, want nil (anyOf semantics)", err)
	}
}

func TestAsyncAPIFailureDescription(t *testing.T) {
	spec := readAsyncAPISpec(t)
	start := strings.Index(spec, "    Failure:\n")
	if start < 0 {
		t.Fatal("Failure message description block not found")
	}
	end := strings.Index(spec[start:], "      payload:")
	if end < 0 {
		t.Fatal("Failure message payload boundary not found")
	}
	description := spec[start : start+end]
	for _, phrase := range []string{
		"one complete JSON object",
		"one JSON value",
		"Trailing whitespace is allowed",
		"non-whitespace",
		"multiple JSON values",
	} {
		if !strings.Contains(description, phrase) {
			t.Fatalf("Failure description missing %q", phrase)
		}
	}
}
