package httpapi

import (
	"os"
	"strings"
	"testing"
)

// TestOpenAPIDocumentsKeyFramingPatterns is F-5/G6 (made mandatory by QA):
// the OpenAPI contract must keep documenting the key-framing charset rule at
// the same body fields and three tenant_id query params the runtime
// enforces. The generic body pattern (plus-quantified) applies to the
// single-component-frame fields and non-tenant ids; the stream/tenant
// variant additionally excludes ':' (aggregate_type, aggregate_id, Tenant.id
// — the ':'-delimited framing components). The query-param form allows the
// empty string, which is legal at runtime (platform all-tenants reads). The
// assertion pins exact occurrence counts so a drift in either direction (a
// field losing its pattern or a pattern spreading to a non-key field) fails
// here.
func TestOpenAPIDocumentsKeyFramingPatterns(t *testing.T) {
	spec, err := os.ReadFile("../../api/openapi/openapi.yaml")
	if err != nil {
		t.Fatalf("read openapi.yaml (go test runs with CWD=package dir): %v", err)
	}
	text := string(spec)
	bodyPattern := `pattern: '^[^\x00-\x1F\x7F\s/\\]+$'`
	// Stream/tenant variant: ValidStreamComponent / ValidTenantIDComponent
	// reject ':' on top of the generic rule because ':' is the stream-frame
	// delimiter and the dev-token delimiter.
	streamTenantPattern := `pattern: '^[^:\x00-\x1F\x7F\s/\\]+$'`
	queryPattern := `pattern: '^$|^[^:\x00-\x1F\x7F\s/\\]+$'`

	// Generic body pattern: Event.event_id, Event.source_system,
	// Event.operation_id, Source.id, EventSchema.schema_id — exactly five.
	if count := strings.Count(text, bodyPattern); count != 5 {
		t.Errorf("generic body key-framing pattern appears %d times, want exactly 5 (event_id, source_system, operation_id, Source.id, EventSchema.schema_id)", count)
	}
	// Stream/tenant body pattern: Event.aggregate_type, Event.aggregate_id,
	// Tenant.id — exactly three (the ':'-delimited framing components).
	if count := strings.Count(text, streamTenantPattern); count != 3 {
		t.Errorf("stream/tenant key-framing pattern appears %d times, want exactly 3 (aggregate_type, aggregate_id, Tenant.id)", count)
	}
	// The reusable query selector and the body tenant selector share the
	// same validation language. Query declarations are references rather than
	// duplicated inline schemas, so their 31 operations cannot drift.
	if count := strings.Count(text, queryPattern); count != 2 {
		t.Errorf("tenant selector key-framing pattern appears %d times, want exactly 2 (query parameter and body selector)", count)
	}
	if count := strings.Count(text, "#/components/parameters/tenantId"); count != 31 {
		t.Errorf("tenantId reusable parameter is referenced %d times, want 31", count)
	}
	// The five Event key components must each carry their pattern inline;
	// non-key components (event_type, schema_id, idempotency_key) must not.
	// The single-component frame fields stay colon-permissive; the aggregate
	// pair carries the colon-excluding stream variant.
	for _, field := range []string{"event_id", "source_system", "operation_id"} {
		if !strings.Contains(text, field+": { type: string, "+bodyPattern) {
			t.Errorf("Event.%s must carry the generic key-framing pattern", field)
		}
	}
	for _, field := range []string{"aggregate_type", "aggregate_id"} {
		if !strings.Contains(text, field+": { type: string, "+streamTenantPattern) {
			t.Errorf("Event.%s must carry the stream key-framing pattern (':' rejected)", field)
		}
	}
}
