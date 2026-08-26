package httpapi

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestOpenAPIDocumentsOccurredAtRange is AC-5: the accepted OccurredAt range
// is exposed on the contract surface so producers fail before sending. The
// Event.occurred_at property description must carry the exact ledger-horizon
// window [1900-01-01T00:00:00Z, 2299-12-31T23:59:59.999Z] and the rejection
// class token occurred_at_out_of_range, and the 422 response descriptions of
// the single and batch ingest endpoints must mention the class. Mirrors the
// regex-based approach of checks/contract_fields.py (no YAML dependency).
// TestOpenAPIDocumentsIngestResourceCaps keeps the REST contract aligned with
// the runtime admission caps. maxLength documents the fixed value while the
// descriptions make clear that runtime validation counts UTF-8 bytes.
func TestOpenAPIDocumentsIngestResourceCaps(t *testing.T) {
	specPath := filepath.Join("..", "..", "api", "openapi", "openapi.yaml")
	raw, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatalf("read %s: %v", specPath, err)
	}
	spec := string(raw)
	eventLine := regexp.MustCompile(`(?m)^    Event: \{.*$`).FindString(spec)
	if eventLine == "" {
		t.Fatal("openapi.yaml Event schema line not found")
	}
	for _, field := range []string{
		"event_id", "source_system", "event_type", "schema_id", "operation_id",
		"causation_id", "correlation_id", "trace_id", "span_id", "aggregate_type",
		"aggregate_id", "workflow_instance_id", "execution_run_id", "action", "outcome",
		"reason", "payload_ref", "data_classification", "retention_class", "idempotency_key",
	} {
		marker := field + ": {"
		start := strings.Index(eventLine, marker)
		if start < 0 {
			t.Errorf("Event.%s missing", field)
			continue
		}
		end := strings.IndexByte(eventLine[start:], '}')
		if end < 0 || !strings.Contains(eventLine[start:start+end], "maxLength: 8192") {
			t.Errorf("Event.%s missing maxLength: 8192", field)
		}
	}
	actorLine := regexp.MustCompile(`(?m)^    Actor: \{.*$`).FindString(spec)
	if !strings.Contains(actorLine, "roles: { type: array, maxItems: 64") ||
		!strings.Contains(actorLine, "items: { type: string, maxLength: 8192") {
		t.Fatalf("Actor roles cap is missing: %s", actorLine)
	}
	if !strings.Contains(eventLine, "targets: { type: array, maxItems: 64") ||
		!strings.Contains(eventLine, "changed_fields: { type: object, maxProperties: 256") {
		t.Fatal("Event repeated-field caps are missing")
	}
	if strings.Count(eventLine, "x-maxCanonicalJsonBytes: 8192") != 2 ||
		!strings.Contains(eventLine, "canonical JSON serialization") {
		t.Fatal("Event changed-field canonical JSON caps are missing")
	}
	if !strings.Contains(spec, "events: { type: array, maxItems: 500") ||
		!strings.Contains(spec, "rejected before any member is ingested") {
		t.Fatal("batch count cap or preflight behavior is missing from OpenAPI")
	}
	if !strings.Contains(eventLine, "UTF-8 bytes") || !strings.Contains(eventLine, "Runtime validation is authoritative") {
		t.Fatal("OpenAPI must document byte semantics and authoritative runtime validation")
	}
}

func TestOpenAPIDocumentsOccurredAtRange(t *testing.T) {
	specPath := filepath.Join("..", "..", "api", "openapi", "openapi.yaml")
	raw, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatalf("read %s: %v", specPath, err)
	}
	spec := string(raw)

	const window = "[1900-01-01T00:00:00Z, 2299-12-31T23:59:59.999Z]"
	if !strings.Contains(spec, window) {
		t.Fatalf("openapi.yaml missing the ledger horizon window %s", window)
	}
	if !strings.Contains(spec, "occurred_at_out_of_range") {
		t.Fatal("openapi.yaml missing the rejection class token occurred_at_out_of_range")
	}

	// Event schema line: occurred_at property must carry the description
	// (scope to the single-line schema, not just any mention in the file).
	eventSchema := regexp.MustCompile(`(?m)^    Event: \{.*$`).FindString(spec)
	if eventSchema == "" {
		t.Fatal("openapi.yaml Event schema line not found")
	}
	occurredProp := regexp.MustCompile(`occurred_at: \{ type: string, format: date-time, description: 'Business occurrence time, UTC\. Must be within \[1900-01-01T00:00:00Z, 2299-12-31T23:59:59\.999Z\]`).FindString(eventSchema)
	if occurredProp == "" {
		t.Fatal("Event.occurred_at description missing the ledger horizon window")
	}

	// Both ingest endpoints' 422 response descriptions mention the class.
	if strings.Count(spec, "422 occurred_at_out_of_range") < 3 {
		t.Fatalf("expected the 422 rejection semantics on the Event property, single and batch POST responses; found %d mentions of '422 occurred_at_out_of_range'", strings.Count(spec, "422 occurred_at_out_of_range"))
	}
}
