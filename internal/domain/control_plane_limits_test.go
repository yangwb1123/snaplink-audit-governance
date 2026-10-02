package domain

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// TestControlPlaneCapsAreFixed pins the control-plane list caps, and re-checks
// the five ingest caps in the same assertion, so the two tables cannot drift
// apart unnoticed and a silent change to either is red. Mirrors
// TestIngestResourceCapsAreFixed; the OpenAPI assertions in
// openapi_control_plane_caps_test.go derive their expectations from these same
// constants, so changing one side without the other fails too.
func TestControlPlaneCapsAreFixed(t *testing.T) {
	if MaxSourceClientIDs != 256 || MaxSchemaFields != 256 ||
		MaxEnvelopeFieldBytes != 8192 || MaxActorRoles != 64 || MaxTargetsPerEvent != 64 || MaxChangedFields != 256 || MaxBatchEvents != 500 {
		t.Fatalf("fixed caps changed: control-plane client_ids=%d schema_fields=%d; ingest field=%d roles=%d targets=%d changed=%d batch=%d",
			MaxSourceClientIDs, MaxSchemaFields,
			MaxEnvelopeFieldBytes, MaxActorRoles, MaxTargetsPerEvent, MaxChangedFields, MaxBatchEvents)
	}
}

// TestValidateSourceCapsBoundaryAndClass pins the exact boundary of both
// dimensions of the source allow-list and the error class every transport sees.
func TestValidateSourceCapsBoundaryAndClass(t *testing.T) {
	atCap := SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", AllowedClientIDs: distinctClientIDs(MaxSourceClientIDs)}
	if err := ValidateSourceCaps(atCap); err != nil {
		t.Fatalf("exactly %d allowed_client_ids must pass, got %v", MaxSourceClientIDs, err)
	}

	overCap := SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", AllowedClientIDs: distinctClientIDs(MaxSourceClientIDs + 1)}
	err := ValidateSourceCaps(overCap)
	if !errors.Is(err, ErrInvalid) || !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatalf("over-cap allowed_client_ids error=%v, want ErrInvalid and ErrEnvelopeTooLarge", err)
	}
	want := fmt.Sprintf("allowed_client_ids count %d, max %d", MaxSourceClientIDs+1, MaxSourceClientIDs)
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("over-cap message=%q does not contain %q", err.Error(), want)
	}

	// Per-entry bytes: exactly MaxEnvelopeFieldBytes passes, one more byte does
	// not, and the message names the list with the [] index marker that only
	// checkFieldListCaps produces.
	atByteCap := SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM",
		AllowedClientIDs: []string{strings.Repeat("c", MaxEnvelopeFieldBytes)}}
	if err := ValidateSourceCaps(atByteCap); err != nil {
		t.Fatalf("entry of exactly %d bytes must pass, got %v", MaxEnvelopeFieldBytes, err)
	}
	err = ValidateSourceCaps(SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM",
		AllowedClientIDs: []string{strings.Repeat("c", MaxEnvelopeFieldBytes+1)}})
	if !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatalf("over-long entry error=%v, want ErrEnvelopeTooLarge", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("allowed_client_ids[] is %d bytes, max %d", MaxEnvelopeFieldBytes+1, MaxEnvelopeFieldBytes)) {
		t.Fatalf("over-long entry message=%q does not name allowed_client_ids[] and max %d", err.Error(), MaxEnvelopeFieldBytes)
	}

	// The byte bound counts UTF-8 bytes, not characters: 2731 three-byte runes
	// are 8193 bytes and must be rejected even though len([]rune) is under the
	// cap. (2731*3 = 8193 > 8192.)
	runes := strings.Repeat("㐀", MaxEnvelopeFieldBytes/3+1)
	if len([]rune(runes)) > MaxEnvelopeFieldBytes {
		t.Fatalf("fixture must be under the cap in characters, got %d runes", len([]rune(runes)))
	}
	err = ValidateSourceCaps(SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", AllowedClientIDs: []string{runes}})
	if !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatalf("multi-byte entry error=%v (runes=%d bytes=%d), want a byte-counted rejection", err, len([]rune(runes)), len(runes))
	}

	// An empty allow-list stays legal: AllowsClient then requires client_id == id.
	if err := ValidateSourceCaps(SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM"}); err != nil {
		t.Fatalf("empty allowed_client_ids must stay legal, got %v", err)
	}
}

// TestValidateSourceCapsCountsDuplicates pins that the arity cap is applied to
// the raw decoded entries: duplicate padding cannot buy extra entries.
func TestValidateSourceCapsCountsDuplicates(t *testing.T) {
	entries := make([]string, MaxSourceClientIDs+1)
	for i := range entries {
		entries[i] = "duplicate-client"
	}
	err := ValidateSourceCaps(SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", AllowedClientIDs: entries})
	if !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatalf("duplicate-padded allow-list error=%v, want ErrEnvelopeTooLarge", err)
	}
}

// TestValidateSchemaCapsArity pins the arity cap and the per-entry byte cap for
// each of the four EventSchema field lists, in the fixed check order, plus the
// order-independence of the reported violation.
func TestValidateSchemaCapsArity(t *testing.T) {
	for _, list := range schemaFieldLists() {
		atCap := schemaWithList(list.name, distinctClientIDs(MaxSchemaFields))
		if err := ValidateSchemaCaps(atCap); err != nil {
			t.Fatalf("%s: exactly %d entries must pass, got %v", list.name, MaxSchemaFields, err)
		}

		err := ValidateSchemaCaps(schemaWithList(list.name, distinctClientIDs(MaxSchemaFields+1)))
		if !errors.Is(err, ErrInvalid) || !errors.Is(err, ErrEnvelopeTooLarge) {
			t.Fatalf("%s: over-cap error=%v, want ErrInvalid and ErrEnvelopeTooLarge", list.name, err)
		}
		want := fmt.Sprintf("%s count %d, max %d", list.name, MaxSchemaFields+1, MaxSchemaFields)
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: over-cap message=%q does not contain %q", list.name, err.Error(), want)
		}

		overLong := distinctClientIDs(4)
		overLong[2] = strings.Repeat("f", MaxEnvelopeFieldBytes+1)
		err = ValidateSchemaCaps(schemaWithList(list.name, overLong))
		if !strings.Contains(err.Error(), fmt.Sprintf("%s[] is %d bytes, max %d", list.name, MaxEnvelopeFieldBytes+1, MaxEnvelopeFieldBytes)) {
			t.Fatalf("%s: over-long entry message=%q does not name %s[]", list.name, err.Error(), list.name)
		}
	}

	// The byte walk is over a sorted copy, so the reported violation does not
	// depend on the order the entries arrived in.
	forward := distinctClientIDs(6)
	forward[3] = strings.Repeat("f", MaxEnvelopeFieldBytes+1)
	reversed := make([]string, len(forward))
	for i := range forward {
		reversed[len(forward)-1-i] = forward[i]
	}
	first := ValidateSchemaCaps(schemaWithList("required_fields", forward))
	second := ValidateSchemaCaps(schemaWithList("required_fields", reversed))
	if first == nil || first.Error() != second.Error() {
		t.Fatalf("reported violation depends on request order: %v vs %v", first, second)
	}

	// Fixed check order across lists: required_fields is reported before
	// allowed_fields when both are over cap.
	both := EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event",
		RequiredFields: distinctClientIDs(MaxSchemaFields + 1), AllowedFields: distinctClientIDs(MaxSchemaFields + 2)}
	err := ValidateSchemaCaps(both)
	if !strings.Contains(err.Error(), "required_fields count") {
		t.Fatalf("multi-list violation message=%q must name required_fields first", err.Error())
	}

	// An all-empty schema stays legal (the shape cmd/audit-api bootstrap uses).
	if err := ValidateSchemaCaps(EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event"}); err != nil {
		t.Fatalf("empty schema field lists must stay legal, got %v", err)
	}
}

// TestValidateSchemaCapsLeavesInputUntouched pins the non-mutation guarantee:
// RegisterSchema stores the caller's slices verbatim, so validation must not
// reorder, dedup or otherwise rewrite them.
func TestValidateSchemaCapsLeavesInputUntouched(t *testing.T) {
	for _, list := range schemaFieldLists() {
		entries := distinctClientIDs(6)
		entries[0] = strings.Repeat("z", MaxEnvelopeFieldBytes+1)
		schema := schemaWithList(list.name, entries)
		before := strings.Join(entries, "\x00")
		_ = ValidateSchemaCaps(schema)
		if after := strings.Join(schemaFieldList(schema, list.name), "\x00"); after != before {
			t.Fatalf("%s: validation mutated the caller's slice\nbefore=%q\nafter =%q", list.name, before, after)
		}
	}
}

type schemaFieldListRef struct {
	name string
	get  func(EventSchema) []string
	set  func(*EventSchema, []string)
}

func schemaFieldLists() []schemaFieldListRef {
	return []schemaFieldListRef{
		{"required_fields", func(s EventSchema) []string { return s.RequiredFields }, func(s *EventSchema, v []string) { s.RequiredFields = v }},
		{"allowed_fields", func(s EventSchema) []string { return s.AllowedFields }, func(s *EventSchema, v []string) { s.AllowedFields = v }},
		{"encrypted_fields", func(s EventSchema) []string { return s.EncryptedFields }, func(s *EventSchema, v []string) { s.EncryptedFields = v }},
		{"searchable_fields", func(s EventSchema) []string { return s.SearchableFields }, func(s *EventSchema, v []string) { s.SearchableFields = v }},
	}
}

func schemaWithList(name string, entries []string) EventSchema {
	schema := EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event"}
	for _, list := range schemaFieldLists() {
		if list.name == name {
			list.set(&schema, entries)
		}
	}
	return schema
}

func schemaFieldList(schema EventSchema, name string) []string {
	for _, list := range schemaFieldLists() {
		if list.name == name {
			return list.get(schema)
		}
	}
	return nil
}

// distinctClientIDs builds n unique short entries; the number of possible
// fixed-width values comfortably exceeds every cap this test exercises.
func distinctClientIDs(n int) []string {
	entries := make([]string, n)
	for i := range entries {
		entries[i] = fmt.Sprintf("client-%09d", i)
	}
	return entries
}
