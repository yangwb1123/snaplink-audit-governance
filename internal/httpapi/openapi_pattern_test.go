package httpapi

import (
	"os"
	"strings"
	"testing"
)

// TestOpenAPIDocumentsKeyFramingPatterns is F-5/G6 (made mandatory by QA):
// the OpenAPI contract must keep documenting the key-framing charset rule at
// the same eight body fields and three tenant_id query params the runtime
// enforces. The body pattern (plus-quantified) applies to required fields;
// the query-param form allows the empty string, which is legal at runtime
// (platform all-tenants reads). The assertion pins exact occurrence counts
// so a drift in either direction (a field losing its pattern or a pattern
// spreading to a non-key field) fails here.
func TestOpenAPIDocumentsKeyFramingPatterns(t *testing.T) {
	spec, err := os.ReadFile("../../api/openapi/openapi.yaml")
	if err != nil {
		t.Fatalf("read openapi.yaml (go test runs with CWD=package dir): %v", err)
	}
	text := string(spec)
	bodyPattern := `pattern: '^[^\x00-\x1F\x7F\s/\\]+$'`
	queryPattern := `pattern: '^$|^[^\x00-\x1F\x7F\s/\\]+$'`

	// Body pattern: Event.event_id, Event.source_system, Event.operation_id,
	// Event.aggregate_type, Event.aggregate_id, Tenant.id, Source.id,
	// EventSchema.schema_id — exactly these eight.
	if count := strings.Count(text, bodyPattern); count != 8 {
		t.Errorf("body key-framing pattern appears %d times, want exactly 8 (5 Event fields + Tenant.id + Source.id + EventSchema.schema_id)", count)
	}
	// Query pattern: the three tenant_id query params (admin/actions,
	// sources, schemas) — exactly three.
	if count := strings.Count(text, queryPattern); count != 3 {
		t.Errorf("query key-framing pattern appears %d times, want exactly 3 (tenant_id query params)", count)
	}
	// The five Event key components must each carry the pattern inline;
	// non-key components (event_type, schema_id, idempotency_key) must not.
	for _, field := range []string{"event_id", "source_system", "operation_id", "aggregate_type", "aggregate_id"} {
		if !strings.Contains(text, field+": { type: string, "+bodyPattern) {
			t.Errorf("Event.%s must carry the key-framing pattern", field)
		}
	}
}
