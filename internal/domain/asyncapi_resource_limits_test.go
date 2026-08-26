package domain

import (
	"fmt"
	"strings"
	"testing"
)

// TestIngestResourceCapsAreFixed prevents a future change from silently
// turning the documented resource contract into a different policy. The
// AsyncAPI assertions below derive their expected values from these same
// authoritative constants, so changing one side without the other is red.
func TestIngestResourceCapsAreFixed(t *testing.T) {
	if MaxEnvelopeFieldBytes != 8192 || MaxActorRoles != 64 || MaxTargetsPerEvent != 64 || MaxChangedFields != 256 || MaxBatchEvents != 500 {
		t.Fatalf("fixed caps changed: field=%d roles=%d targets=%d changed=%d batch=%d", MaxEnvelopeFieldBytes, MaxActorRoles, MaxTargetsPerEvent, MaxChangedFields, MaxBatchEvents)
	}
}

func TestAsyncAPIEventEnvelopeDocumentsResourceCaps(t *testing.T) {
	spec := readAsyncAPISpec(t)
	envelope := asyncAPISchemaToEnd(spec, "    EventEnvelope:")
	if envelope == "" {
		t.Fatal("EventEnvelope schema not found")
	}
	stringFields := []string{
		"event_id", "source_system", "event_type", "schema_id", "operation_id",
		"causation_id", "correlation_id", "trace_id", "span_id", "aggregate_type",
		"aggregate_id", "workflow_instance_id", "execution_run_id", "action", "outcome",
		"reason", "payload_ref", "data_classification", "retention_class", "idempotency_key",
	}
	for _, field := range stringFields {
		line := schemaLine(envelope, "        "+field+":")
		want := fmt.Sprintf("maxLength: %d", MaxEnvelopeFieldBytes)
		if !strings.Contains(line, want) {
			t.Errorf("EventEnvelope.%s missing %s", field, want)
		}
	}

	actorLine := schemaLine(envelope, "        actor:")
	for _, field := range []string{"id", "type", "name", "department"} {
		if !strings.Contains(actorLine, fmt.Sprintf("%s: { type: string, maxLength: %d", field, MaxEnvelopeFieldBytes)) {
			t.Errorf("actor.%s missing string cap", field)
		}
	}
	if !strings.Contains(actorLine, fmt.Sprintf("maxItems: %d", MaxActorRoles)) ||
		!strings.Contains(actorLine, fmt.Sprintf("items: { type: string, maxLength: %d", MaxEnvelopeFieldBytes)) {
		t.Errorf("actor roles resource caps missing: %s", actorLine)
	}
	targets := asyncAPISchemaSection(envelope, "        targets:", "        aggregate_type:")
	if !strings.Contains(targets, fmt.Sprintf("maxItems: %d", MaxTargetsPerEvent)) ||
		strings.Count(targets, fmt.Sprintf("maxLength: %d", MaxEnvelopeFieldBytes)) < 3 {
		t.Errorf("target resource caps missing: %s", targets)
	}
	changed := asyncAPISchemaSection(envelope, "        changed_fields:", "        payload:")
	if !strings.Contains(changed, fmt.Sprintf("maxProperties: %d", MaxChangedFields)) ||
		!strings.Contains(changed, fmt.Sprintf("maxLength: %d", MaxEnvelopeFieldBytes)) ||
		strings.Count(changed, fmt.Sprintf("x-maxCanonicalJsonBytes: %d", MaxEnvelopeFieldBytes)) != 2 {
		t.Errorf("changed-field resource caps missing: %s", changed)
	}
	if !strings.Contains(strings.ToLower(envelope), "utf-8") || !strings.Contains(strings.ToLower(envelope), "canonical json serialization") {
		t.Fatal("AsyncAPI resource descriptions must name UTF-8 bytes and canonical JSON serialization")
	}

	batch := asyncAPISchemaSection(spec, "    IngestBatch:", "    EventEnvelope:")
	if !strings.Contains(batch, fmt.Sprintf("maxItems: %d", MaxBatchEvents)) ||
		!strings.Contains(batch, "#/components/schemas/EventEnvelope") {
		t.Fatalf("IngestBatch must machine-document maxItems=%d and reference EventEnvelope: %s", MaxBatchEvents, batch)
	}
}

func schemaLine(block, marker string) string {
	start := strings.Index(block, marker)
	if start < 0 {
		return ""
	}
	end := strings.IndexByte(block[start:], '\n')
	if end < 0 {
		return block[start:]
	}
	return block[start : start+end]
}

func asyncAPISchemaToEnd(text, startMarker string) string {
	start := strings.Index(text, startMarker)
	if start < 0 {
		return ""
	}
	return text[start+len(startMarker):]
}

func asyncAPISchemaSection(text, startMarker, endMarker string) string {
	start := strings.Index(text, startMarker)
	if start < 0 {
		return ""
	}
	start += len(startMarker)
	end := strings.Index(text[start:], endMarker)
	if end < 0 {
		return text[start:]
	}
	return text[start : start+end]
}
