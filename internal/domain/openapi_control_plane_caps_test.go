package domain

// OpenAPI control-plane contract pins. api/openapi/openapi.yaml must describe
// exactly what ValidateSourceCaps / ValidateSchemaCaps enforce, so the
// expectations are derived from the same domain constants instead of literals:
// editing one side without the other is red. This is the REST twin of
// TestAsyncAPIEventEnvelopeDocumentsResourceCaps.
//
// Only the three INPUT schemas carry maxItems / item maxLength: the caps are
// enforced at the control-plane write boundary and reads are deliberately not
// gated, so a row registered before the cap existed is still served verbatim.
// Declaring the bound on the OUTPUT schemas would document a maximum the
// server is allowed to exceed and would let a generated client reject a
// legitimate GET response, so their absence is asserted here too.

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func readOpenAPISpec(t *testing.T) string {
	t.Helper()
	specPath := filepath.Join("..", "..", "api", "openapi", "openapi.yaml")
	raw, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatalf("read %s: %v", specPath, err)
	}
	return string(raw)
}

// openAPISchemaLine returns the single flow-mapping line that declares the
// named component schema (the contract file keeps components on one line).
func openAPISchemaLine(t *testing.T, spec, name string) string {
	t.Helper()
	marker := "    " + name + ": {"
	start := strings.Index(spec, "\n"+marker)
	if start < 0 {
		if !strings.HasPrefix(spec, marker) {
			t.Fatalf("openapi.yaml schema %s not found", name)
		}
		start = 0
	} else {
		start++
	}
	end := strings.IndexByte(spec[start:], '\n')
	if end < 0 {
		return spec[start:]
	}
	return spec[start : start+end]
}

// openAPIProperty returns the flow-mapping fragment of one property, from its
// key to the matching closing brace of its mapping.
func openAPIProperty(t *testing.T, line, name string) string {
	t.Helper()
	marker := name + ": {"
	start := strings.Index(line, marker)
	if start < 0 {
		t.Fatalf("property %s missing", name)
	}
	depth, end := 0, -1
	for i := start + len(name) + 1; i < len(line); i++ {
		switch line[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				end = i + 1
			}
		}
		if end > 0 {
			break
		}
	}
	if end < 0 {
		t.Fatalf("property %s mapping is not terminated", name)
	}
	return line[start:end]
}

// TestOpenAPIControlPlaneResourceCaps binds the write-cap declarations of
// SourceInput, SourceUpdateInput and EventSchemaInput to the domain constants.
func TestOpenAPIControlPlaneResourceCaps(t *testing.T) {
	spec := readOpenAPISpec(t)
	byteNote := "Runtime limit is 8192 UTF-8 bytes, not characters."

	for _, schema := range []string{"SourceInput", "SourceUpdateInput"} {
		property := openAPIProperty(t, openAPISchemaLine(t, spec, schema), "allowed_client_ids")
		for _, want := range []string{
			"allowed_client_ids: { type: array",
			"maxItems: " + strconv.Itoa(MaxSourceClientIDs),
			"maxLength: " + strconv.Itoa(MaxEnvelopeFieldBytes),
			byteNote,
		} {
			if !strings.Contains(property, want) {
				t.Errorf("%s.allowed_client_ids missing %q: %s", schema, want, property)
			}
		}
	}

	schemaLine := openAPISchemaLine(t, spec, "EventSchemaInput")
	for _, name := range []string{"required_fields", "allowed_fields", "encrypted_fields", "searchable_fields"} {
		property := openAPIProperty(t, schemaLine, name)
		for _, want := range []string{
			name + ": { type: array",
			"maxItems: " + strconv.Itoa(MaxSchemaFields),
			"maxLength: " + strconv.Itoa(MaxEnvelopeFieldBytes),
			byteNote,
		} {
			if !strings.Contains(property, want) {
				t.Errorf("EventSchemaInput.%s missing %q: %s", name, want, property)
			}
		}
	}
}

// TestOpenAPIControlPlaneOutputSchemasStayUnbounded pins D7 in the contract:
// the response schemas describe the write cap but declare no maximum, because
// the server serves pre-existing over-cap rows verbatim.
func TestOpenAPIControlPlaneOutputSchemasStayUnbounded(t *testing.T) {
	spec := readOpenAPISpec(t)
	legacy := "may exceed it"

	source := openAPIProperty(t, openAPISchemaLine(t, spec, "Source"), "allowed_client_ids")
	if strings.Contains(source, "maxItems") || strings.Contains(source, "maxLength") {
		t.Errorf("Source.allowed_client_ids must not declare a maximum the server may exceed: %s", source)
	}
	if !strings.Contains(source, legacy) || !strings.Contains(source, strconv.Itoa(MaxSourceClientIDs)) {
		t.Errorf("Source.allowed_client_ids must describe the write cap and the legacy-row exception: %s", source)
	}

	schemaLine := openAPISchemaLine(t, spec, "EventSchema")
	for _, name := range []string{"required_fields", "allowed_fields", "encrypted_fields", "searchable_fields"} {
		property := openAPIProperty(t, schemaLine, name)
		if strings.Contains(property, "maxItems") || strings.Contains(property, "maxLength") {
			t.Errorf("EventSchema.%s must not declare a maximum the server may exceed: %s", name, property)
		}
		if !strings.Contains(property, legacy) || !strings.Contains(property, strconv.Itoa(MaxSchemaFields)) {
			t.Errorf("EventSchema.%s must describe the write cap and the legacy-row exception: %s", name, property)
		}
	}
}

// TestOpenAPIEventEnvelopeResourceCapsUnchanged guards the ingest declarations
// against collateral edits from this change.
func TestOpenAPIEventEnvelopeResourceCapsUnchanged(t *testing.T) {
	spec := readOpenAPISpec(t)
	actor := openAPISchemaLine(t, spec, "Actor")
	for _, want := range []string{
		"roles: { type: array, maxItems: " + strconv.Itoa(MaxActorRoles),
		"items: { type: string, maxLength: " + strconv.Itoa(MaxEnvelopeFieldBytes),
	} {
		if !strings.Contains(actor, want) {
			t.Errorf("Actor roles cap changed, missing %q: %s", want, actor)
		}
	}
	event := openAPISchemaLine(t, spec, "Event")
	for _, want := range []string{
		"targets: { type: array, maxItems: " + strconv.Itoa(MaxTargetsPerEvent),
		"changed_fields: { type: object, maxProperties: " + strconv.Itoa(MaxChangedFields),
	} {
		if !strings.Contains(event, want) {
			t.Errorf("Event repeated-field caps changed, missing %q", want)
		}
	}
	if !strings.Contains(spec, "events: { type: array, maxItems: "+strconv.Itoa(MaxBatchEvents)) {
		t.Errorf("batch request body cap changed, missing maxItems: %d", MaxBatchEvents)
	}
}
