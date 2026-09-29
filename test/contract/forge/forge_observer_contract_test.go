package forge

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"time"
)

//go:embed testdata/forge-run-observed-v1.json
var forgeRunObservedFixture []byte

type forgeRunObservedAuthority struct {
	IdentityVerified          bool `json:"identity_verified"`
	OwnerAuthorized           bool `json:"owner_authorized"`
	RunAuthoritative          bool `json:"run_authoritative"`
	PersistenceAttested       bool `json:"persistence_attested"`
	ContentProvenanceVerified bool `json:"content_provenance_verified"`
	ReservationCreated        bool `json:"reservation_created"`
	ExecutionAuthorized       bool `json:"execution_authorized"`
	DispatchPerformed         bool `json:"dispatch_performed"`
}

type forgeRunObserved struct {
	APIVersion       string                    `json:"api_version"`
	OwnerRef         string                    `json:"owner_ref"`
	ConversationID   string                    `json:"conversation_id"`
	RunID            string                    `json:"run_id"`
	PromptID         string                    `json:"prompt_id"`
	CreatedAtMS      int64                     `json:"created_at_ms"`
	LatestSequence   int64                     `json:"latest_sequence"`
	Status           string                    `json:"status"`
	MetadataObserved bool                      `json:"metadata_observed"`
	ContentIncluded  bool                      `json:"content_included"`
	Authority        forgeRunObservedAuthority `json:"authority"`
}

func loadForgeRunObservedFixture(t *testing.T) (forgeRunObserved, map[string]json.RawMessage) {
	t.Helper()
	var fixture forgeRunObserved
	if err := rejectDuplicateJSONKeys(forgeRunObservedFixture); err != nil {
		t.Fatalf("Forge observer fixture has duplicate JSON fields: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(forgeRunObservedFixture))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fixture); err != nil {
		t.Fatalf("decode Forge observer fixture: %v", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		t.Fatalf("Forge observer fixture has trailing JSON: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(forgeRunObservedFixture, &raw); err != nil {
		t.Fatalf("decode Forge observer fixture object: %v", err)
	}
	assertJSONKeys(t, raw, []string{
		"api_version", "owner_ref", "conversation_id", "run_id", "prompt_id",
		"created_at_ms", "latest_sequence", "status", "metadata_observed",
		"content_included", "authority",
	}, "Forge observer fixture")
	var authority map[string]json.RawMessage
	if err := json.Unmarshal(raw["authority"], &authority); err != nil {
		t.Fatalf("decode Forge observer authority: %v", err)
	}
	assertJSONKeys(t, authority, []string{
		"identity_verified", "owner_authorized", "run_authoritative",
		"persistence_attested", "content_provenance_verified", "reservation_created",
		"execution_authorized", "dispatch_performed",
	}, "Forge observer authority")
	return fixture, raw
}

// rejectDuplicateJSONKeys keeps the fixture receiver aligned with clients
// that reject repeated JSON object names before map materialization. The
// standard decoder rejects unknown fields but otherwise keeps the last value
// for a duplicate key, which would make evidence consumers disagree.
func rejectDuplicateJSONKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanJSONValue(decoder); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			keyText, ok := key.(string)
			if !ok {
				return fmt.Errorf("JSON object key is not a string")
			}
			if _, duplicate := seen[keyText]; duplicate {
				return fmt.Errorf("duplicate JSON object key %q", keyText)
			}
			seen[keyText] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim('}') {
			return fmt.Errorf("JSON object did not close")
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim(']') {
			return fmt.Errorf("JSON array did not close")
		}
	case '}', ']':
		return fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
	return nil
}

func assertJSONKeys(t *testing.T, got map[string]json.RawMessage, want []string, label string) {
	t.Helper()
	wanted := make(map[string]struct{}, len(want))
	for _, key := range want {
		wanted[key] = struct{}{}
	}
	for key := range got {
		if _, ok := wanted[key]; !ok {
			t.Fatalf("%s contains unexpected field %q", label, key)
		}
	}
	for _, key := range want {
		if _, ok := got[key]; !ok {
			t.Fatalf("%s is missing required field %q", label, key)
		}
	}
}

// TestForgeRunObserverFixtureIsStrictAndAuthorityFree pins the receiver-side
// shape shared with Catalyst Forge. It is validation only: it does not
// register a live schema or authorize a Runner, lease, placement, or dispatch.
func TestForgeRunObserverFixtureIsStrictAndAuthorityFree(t *testing.T) {
	fixture, raw := loadForgeRunObservedFixture(t)
	if fixture.APIVersion != "forge.run.observed.v1" {
		t.Fatalf("api_version=%q", fixture.APIVersion)
	}
	if len(fixture.OwnerRef) != 64 {
		t.Fatalf("owner_ref length=%d, want SHA-256 hex", len(fixture.OwnerRef))
	}
	if _, err := hex.DecodeString(fixture.OwnerRef); err != nil {
		t.Fatalf("owner_ref is not SHA-256 hex: %v", err)
	}
	for name, value := range map[string]string{
		"conversation_id": fixture.ConversationID, "run_id": fixture.RunID,
		"prompt_id": fixture.PromptID, "status": fixture.Status,
	} {
		if value == "" {
			t.Fatalf("%s is empty", name)
		}
	}
	if fixture.CreatedAtMS < 0 || fixture.LatestSequence <= 0 || !fixture.MetadataObserved {
		t.Fatalf("invalid bounded observer metadata: %+v", fixture)
	}
	if fixture.ContentIncluded || fixture.Authority != (forgeRunObservedAuthority{}) {
		t.Fatalf("Forge observer fixture must be content-free and authority-free: %+v", fixture)
	}
	for _, key := range []string{
		"prompt", "prompt_body", "result", "result_body", "output", "message", "tool",
		"error", "title", "path", "provider", "token", "artifact", "event_body", "raw_owner",
	} {
		if _, present := raw[key]; present {
			t.Fatalf("Forge observer fixture contains prohibited field %q", key)
		}
	}
}

func TestForgeRunObserverRejectsDuplicateFields(t *testing.T) {
	rootDuplicate := strings.TrimSpace(string(forgeRunObservedFixture))
	rootDuplicate = strings.TrimSuffix(rootDuplicate, "}") +
		`,"api_version":"forge.run.observed.v1"}`
	if err := rejectDuplicateJSONKeys([]byte(rootDuplicate)); err == nil {
		t.Fatal("observer receiver accepted a duplicate root field")
	}
	nestedDuplicate := strings.Replace(
		string(forgeRunObservedFixture),
		"    \"dispatch_performed\": false\n  }",
		"    \"dispatch_performed\": false,\n    \"dispatch_performed\": false\n  }",
		1,
	)
	if nestedDuplicate == string(forgeRunObservedFixture) {
		t.Fatal("nested duplicate mutation did not match fixture")
	}
	if err := rejectDuplicateJSONKeys([]byte(nestedDuplicate)); err == nil {
		t.Fatal("observer receiver accepted a duplicate nested field")
	}
}

// TestForgeRunObserverContractIsContentFree pins the audit receiver mapping.
// The fixture metadata can be carried as an opaque event payload, while the
// audit event itself still proves no prompt/result content or authority.
func TestForgeRunObserverContractIsContentFree(t *testing.T) {
	fixture, _ := loadForgeRunObservedFixture(t)
	var payload map[string]any
	if err := json.Unmarshal(forgeRunObservedFixture, &payload); err != nil {
		t.Fatalf("decode Forge observer payload: %v", err)
	}
	event := domain.Event{
		EventID: "forge-observer-event-001", TenantID: "tenant-a",
		SourceSystem: "forge-runtime", EventType: fixture.APIVersion,
		SchemaID: fixture.APIVersion, SchemaVersion: 1,
		OccurredAt:  time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC),
		OperationID: "operation-001", Actor: domain.Actor{ID: fixture.OwnerRef, Type: "opaque_owner_ref"},
		AggregateType: "forge_run", AggregateID: fixture.RunID, AggregateVersion: fixture.LatestSequence,
		ExecutionRunID: fixture.RunID, WorkflowInstanceID: fixture.ConversationID,
		Action: "observe_run", Outcome: "observed", Payload: payload,
		DataClassification: "internal", RetentionClass: "standard",
		IdempotencyKey: "forge-observer-event-001",
	}
	if err := event.ValidateBasic(); err != nil {
		t.Fatalf("Forge observer event rejected: %v", err)
	}
	if event.ExecutionRunID != event.AggregateID || event.Payload["content_included"] != false {
		t.Fatalf("observer event lost run identity or content exclusion: %+v", event)
	}
	for _, key := range []string{"prompt", "result", "output", "lease_token", "fencing_token"} {
		if _, present := event.Payload[key]; present {
			t.Fatalf("observer payload contains prohibited field %q", key)
		}
	}
}
