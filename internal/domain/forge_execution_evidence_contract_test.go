package domain

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"
)

//go:embed testdata/forge-run-execution-evidence-v1.json
var forgeRunExecutionEvidenceFixture []byte

type forgeRunExecutionEvidence struct {
	APIVersion             string                             `json:"api_version"`
	EvaluationMode         string                             `json:"evaluation_mode"`
	OwnerRef               string                             `json:"owner_ref"`
	ConversationID         string                             `json:"conversation_id"`
	RunID                  string                             `json:"run_id"`
	PromptID               string                             `json:"prompt_id"`
	RunStatus              string                             `json:"run_status"`
	AttemptID              string                             `json:"attempt_id"`
	TargetID               string                             `json:"target_id"`
	CommandID              string                             `json:"command_id"`
	CommandSHA256          string                             `json:"command_sha256"`
	DispositionKind        string                             `json:"disposition_kind"`
	ReceiptObservedAtMS    int64                              `json:"receipt_observed_at_ms"`
	Uncertain              bool                               `json:"uncertain"`
	ReconciliationRequired bool                               `json:"reconciliation_required"`
	MetadataObserved       bool                               `json:"metadata_observed"`
	ContentIncluded        bool                               `json:"content_included"`
	Authority              forgeRunExecutionEvidenceAuthority `json:"authority"`
}

type forgeRunExecutionEvidenceAuthority struct {
	IdentityVerified    bool `json:"identity_verified"`
	OwnerAuthorized     bool `json:"owner_authorized"`
	RunAuthoritative    bool `json:"run_authoritative"`
	ReceiptPersisted    bool `json:"receipt_persisted"`
	ReservationCreated  bool `json:"reservation_created"`
	ExecutionAuthorized bool `json:"execution_authorized"`
	DispatchPerformed   bool `json:"dispatch_performed"`
	AuditPublished      bool `json:"audit_published"`
}

func decodeForgeRunExecutionEvidence(raw []byte) (forgeRunExecutionEvidence, map[string]json.RawMessage, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeRunExecutionEvidence{}, nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var fixture forgeRunExecutionEvidence
	if err := decoder.Decode(&fixture); err != nil {
		return forgeRunExecutionEvidence{}, nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeRunExecutionEvidence{}, nil, io.ErrUnexpectedEOF
		}
		return forgeRunExecutionEvidence{}, nil, err
	}
	var rawObject map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rawObject); err != nil {
		return forgeRunExecutionEvidence{}, nil, err
	}
	return fixture, rawObject, nil
}

func TestForgeRunExecutionEvidenceRejectsDuplicateFields(t *testing.T) {
	mutated := strings.TrimSpace(string(forgeRunExecutionEvidenceFixture))
	mutated = strings.TrimSuffix(mutated, "}") +
		`,"run_id":"run-001"}`
	if _, _, err := decodeForgeRunExecutionEvidence([]byte(mutated)); err == nil {
		t.Fatal("execution evidence receiver accepted a duplicate root field")
	}
}

func TestForgeRunExecutionEvidenceFixtureIsStrictAndAuthorityFree(t *testing.T) {
	fixture, raw, err := decodeForgeRunExecutionEvidence(forgeRunExecutionEvidenceFixture)
	if err != nil {
		t.Fatalf("decode Forge execution evidence fixture: %v", err)
	}
	assertForgeExecutionEvidenceJSONKeys(t, raw, []string{
		"api_version", "evaluation_mode", "owner_ref", "conversation_id", "run_id",
		"prompt_id", "run_status", "attempt_id", "target_id", "command_id",
		"command_sha256", "disposition_kind", "receipt_observed_at_ms", "uncertain",
		"reconciliation_required", "metadata_observed", "content_included", "authority",
	}, "Forge execution evidence")
	if fixture.APIVersion != "forge.run.execution-evidence.v1" ||
		fixture.EvaluationMode != "pure_run_execution_evidence_binding" {
		t.Fatalf("unexpected Forge execution evidence identity: %#v", fixture)
	}
	for name, value := range map[string]string{
		"owner_ref": fixture.OwnerRef, "conversation_id": fixture.ConversationID,
		"run_id": fixture.RunID, "prompt_id": fixture.PromptID,
		"run_status": fixture.RunStatus, "attempt_id": fixture.AttemptID,
		"target_id": fixture.TargetID, "command_id": fixture.CommandID,
	} {
		if strings.TrimSpace(value) == "" || strings.ContainsAny(value, " :/\\") {
			t.Fatalf("unsafe %s=%q", name, value)
		}
	}
	if !forgeExecutionEvidenceSHA256(fixture.OwnerRef) || !forgeExecutionEvidenceSHA256(fixture.CommandSHA256) ||
		fixture.ReceiptObservedAtMS < 0 || fixture.DispositionKind != "completed" ||
		fixture.Uncertain || fixture.ReconciliationRequired || !fixture.MetadataObserved || fixture.ContentIncluded {
		t.Fatalf("invalid bounded Forge execution evidence: %#v", fixture)
	}
	if fixture.Authority != (forgeRunExecutionEvidenceAuthority{}) {
		t.Fatalf("Forge execution evidence authority must remain disabled: %#v", fixture.Authority)
	}
	for _, forbidden := range []string{"prompt", "result", "output", "tool", "provider", "token", "credential", "artifact", "issuer", "subject", "tenant_id"} {
		if _, present := raw[forbidden]; present {
			t.Fatalf("Forge execution evidence contains prohibited field %q", forbidden)
		}
	}
}

func TestForgeRunExecutionEvidenceRejectsUnknownContent(t *testing.T) {
	var shape map[string]json.RawMessage
	if err := json.Unmarshal(forgeRunExecutionEvidenceFixture, &shape); err != nil {
		t.Fatal(err)
	}
	shape["output"] = json.RawMessage(`"raw output must not cross the observer boundary"`)
	mutated, err := json.Marshal(shape)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := decodeForgeRunExecutionEvidence(mutated); err == nil {
		t.Fatal("execution evidence decoder accepted an unknown content field")
	}
}

func TestForgeRunExecutionEvidenceContractIsContentFree(t *testing.T) {
	fixture, _, err := decodeForgeRunExecutionEvidence(forgeRunExecutionEvidenceFixture)
	if err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{
		"api_version": fixture.APIVersion, "evaluation_mode": fixture.EvaluationMode,
		"owner_ref": fixture.OwnerRef, "conversation_id": fixture.ConversationID,
		"run_id": fixture.RunID, "prompt_id": fixture.PromptID,
		"run_status": fixture.RunStatus, "attempt_id": fixture.AttemptID,
		"target_id": fixture.TargetID, "command_id": fixture.CommandID,
		"command_sha256": fixture.CommandSHA256, "disposition_kind": fixture.DispositionKind,
		"receipt_observed_at_ms": fixture.ReceiptObservedAtMS, "uncertain": fixture.Uncertain,
		"reconciliation_required": fixture.ReconciliationRequired,
		"metadata_observed":       fixture.MetadataObserved, "content_included": fixture.ContentIncluded,
	}
	event := Event{
		EventID: "forge-execution-evidence-event-001", TenantID: "tenant-a",
		SourceSystem: "forge-runtime", EventType: fixture.APIVersion, SchemaID: fixture.APIVersion,
		SchemaVersion: 1, OccurredAt: time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC),
		OperationID:   "forge-execution-evidence-operation-001",
		Actor:         Actor{ID: fixture.OwnerRef, Type: "opaque_owner_ref"},
		AggregateType: "forge_run", AggregateID: fixture.RunID,
		AggregateVersion: 1, Action: "observe_execution_evidence", Outcome: "observed",
		Payload: payload, DataClassification: "internal", RetentionClass: "standard",
		IdempotencyKey: "forge-execution-evidence-event-001",
	}
	if err := event.ValidateBasic(); err != nil {
		t.Fatalf("Forge execution evidence event rejected: %v", err)
	}
	for _, forbidden := range []string{"prompt", "result", "output", "tool", "provider", "token", "credential", "artifact"} {
		if _, present := event.Payload[forbidden]; present {
			t.Fatalf("execution evidence payload contains prohibited field %q", forbidden)
		}
	}
}

func assertForgeExecutionEvidenceJSONKeys(t *testing.T, got map[string]json.RawMessage, want []string, label string) {
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

func forgeExecutionEvidenceSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && strings.ToLower(value) == value
}
