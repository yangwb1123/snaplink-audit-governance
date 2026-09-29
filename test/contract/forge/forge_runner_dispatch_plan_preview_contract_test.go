package forge

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

// This receiver-side value contract carries only a bounded comparison of
// caller-supplied placement, Attempt, intent, and lease observations. It does
// not authenticate a Runner, persist a Run or Attempt, select a target,
// reserve capacity, dispatch work, publish Audit, or grant execution
// authority.
//
//go:embed testdata/forge-runner-dispatch-plan-preview-v1.json
var forgeRunnerDispatchPlanPreviewFixture []byte

const forgeRunnerDispatchPlanPreviewMaxSafeInteger int64 = 9_007_199_254_740_991

type forgeRunnerDispatchPlanPreviewOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeRunnerDispatchPlanPreviewCandidate struct {
	TargetID               string   `json:"target_id"`
	AttributesUnverified   bool     `json:"attributes_unverified"`
	MatchesRequirements    bool     `json:"matches_requirements"`
	LeaseTargetMatch       bool     `json:"lease_target_match"`
	LeaseActive            bool     `json:"lease_active"`
	AttemptStateAdmissible bool     `json:"attempt_state_admissible"`
	DeclarativeReady       bool     `json:"declarative_ready"`
	Reasons                []string `json:"reasons"`
}

type forgeRunnerDispatchPlanPreviewAuthority struct {
	DeviceIdentityVerified bool `json:"device_identity_verified"`
	AttemptPersisted       bool `json:"attempt_persisted"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
	AuditPublished         bool `json:"audit_published"`
}

type forgeRunnerDispatchPlanPreview struct {
	SchemaVersion          string                                    `json:"schema_version"`
	EvaluationMode         string                                    `json:"evaluation_mode"`
	Owner                  forgeRunnerDispatchPlanPreviewOwner       `json:"owner_declaration"`
	ConversationID         string                                    `json:"conversation_id"`
	RunID                  string                                    `json:"run_id"`
	AttemptID              string                                    `json:"attempt_id"`
	AttemptState           string                                    `json:"attempt_state"`
	AttemptStateAdmissible bool                                      `json:"attempt_state_admissible"`
	CommandID              string                                    `json:"command_id"`
	CommandSHA256          string                                    `json:"command_sha256"`
	TargetID               string                                    `json:"intent_target_id"`
	LeaseEpoch             uint64                                    `json:"lease_epoch"`
	LeaseActive            bool                                      `json:"lease_active"`
	EvaluatedAtMS          int64                                     `json:"evaluated_at_ms"`
	CandidateCount         int                                       `json:"candidate_count"`
	DeclarativeReadyCount  int                                       `json:"declarative_ready_count"`
	Candidates             []forgeRunnerDispatchPlanPreviewCandidate `json:"candidates"`
	SelectedTargetID       *string                                   `json:"selected_target_id"`
	PreviewOnly            bool                                      `json:"preview_only"`
	ReservationCreated     bool                                      `json:"reservation_created"`
	ExecutionAuthorized    bool                                      `json:"execution_authorized"`
	DispatchPerformed      bool                                      `json:"dispatch_performed"`
	Authority              forgeRunnerDispatchPlanPreviewAuthority   `json:"authority"`
}

func decodeForgeRunnerDispatchPlanPreview(raw []byte) (forgeRunnerDispatchPlanPreview, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeRunnerDispatchPlanPreview{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value forgeRunnerDispatchPlanPreview
	if err := decoder.Decode(&value); err != nil {
		return forgeRunnerDispatchPlanPreview{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeRunnerDispatchPlanPreview{}, fmt.Errorf("trailing JSON")
		}
		return forgeRunnerDispatchPlanPreview{}, err
	}
	if err := value.validate(); err != nil {
		return forgeRunnerDispatchPlanPreview{}, err
	}
	return value, nil
}

func (value forgeRunnerDispatchPlanPreview) validate() error {
	if value.SchemaVersion != "forge.runner-dispatch-plan-preview/v1" ||
		value.EvaluationMode != "pure_dispatch_plan_preview_only" ||
		!validForgeRunnerDispatchPlanPreviewOwner(value.Owner) ||
		!validForgeRunnerDispatchPlanPreviewIdentifier(value.ConversationID) ||
		!validForgeRunnerDispatchPlanPreviewIdentifier(value.RunID) ||
		!validForgeRunnerDispatchPlanPreviewIdentifier(value.AttemptID) ||
		!validForgeRunnerDispatchPlanPreviewIdentifier(value.CommandID) ||
		!validForgeRunnerDispatchPlanPreviewDigest(value.CommandSHA256) ||
		!validForgeRunnerDispatchPlanPreviewIdentifier(value.TargetID) ||
		!validForgeRunnerDispatchPlanPreviewAttemptState(value.AttemptState) ||
		value.AttemptStateAdmissible != validForgeRunnerDispatchPlanPreviewDispatchable(value.AttemptState) ||
		value.LeaseEpoch == 0 || value.EvaluatedAtMS <= 0 ||
		value.EvaluatedAtMS > forgeRunnerDispatchPlanPreviewMaxSafeInteger ||
		value.SelectedTargetID != nil || !value.PreviewOnly ||
		value.ReservationCreated || value.ExecutionAuthorized || value.DispatchPerformed ||
		value.Authority != (forgeRunnerDispatchPlanPreviewAuthority{}) ||
		value.CandidateCount != len(value.Candidates) || value.CandidateCount > 128 {
		return fmt.Errorf("invalid bounded Runner dispatch-plan preview")
	}
	readyCount := 0
	seen := make(map[string]struct{}, len(value.Candidates))
	for index, candidate := range value.Candidates {
		if !validForgeRunnerDispatchPlanPreviewIdentifier(candidate.TargetID) ||
			!candidate.AttributesUnverified ||
			candidate.LeaseTargetMatch != (candidate.TargetID == value.TargetID) ||
			candidate.LeaseActive != value.LeaseActive ||
			candidate.AttemptStateAdmissible != value.AttemptStateAdmissible ||
			candidate.DeclarativeReady != (candidate.MatchesRequirements && candidate.LeaseTargetMatch && candidate.LeaseActive && candidate.AttemptStateAdmissible) ||
			!validForgeRunnerDispatchPlanPreviewReasons(candidate.Reasons) ||
			(candidate.DeclarativeReady && len(candidate.Reasons) != 0) {
			return fmt.Errorf("invalid Runner dispatch-plan candidate")
		}
		if index > 0 && value.Candidates[index-1].TargetID >= candidate.TargetID {
			return fmt.Errorf("Runner dispatch-plan candidates are not sorted uniquely")
		}
		if _, exists := seen[candidate.TargetID]; exists {
			return fmt.Errorf("duplicate Runner dispatch-plan candidate")
		}
		seen[candidate.TargetID] = struct{}{}
		if candidate.DeclarativeReady {
			readyCount++
		}
	}
	if readyCount != value.DeclarativeReadyCount {
		return fmt.Errorf("Runner dispatch-plan ready count mismatch")
	}
	return nil
}

func validForgeRunnerDispatchPlanPreviewOwner(owner forgeRunnerDispatchPlanPreviewOwner) bool {
	return validForgeRunnerDispatchPlanPreviewOwnerPart(owner.Issuer) &&
		validForgeRunnerDispatchPlanPreviewOwnerPart(owner.Subject) &&
		validForgeRunnerDispatchPlanPreviewOwnerPart(owner.TenantID)
}

func validForgeRunnerDispatchPlanPreviewOwnerPart(value string) bool {
	return value != "" && strings.TrimSpace(value) == value &&
		!strings.ContainsAny(value, "\x00\r\n")
}

func validForgeRunnerDispatchPlanPreviewIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for index, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') {
			continue
		}
		if index == 0 || !strings.ContainsRune("._:+/-", char) {
			return false
		}
	}
	return true
}

func validForgeRunnerDispatchPlanPreviewDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return false
		}
	}
	return true
}

func validForgeRunnerDispatchPlanPreviewAttemptState(value string) bool {
	switch value {
	case "requested", "accepted", "starting", "running", "interrupted", "completed", "failed", "uncertain":
		return true
	default:
		return false
	}
}

func validForgeRunnerDispatchPlanPreviewDispatchable(value string) bool {
	return value == "accepted" || value == "starting" || value == "running"
}

func validForgeRunnerDispatchPlanPreviewReasons(values []string) bool {
	for index, value := range values {
		if value == "" || strings.ContainsAny(value, "\x00\r\n") || (index > 0 && values[index-1] >= value) {
			return false
		}
	}
	return true
}

func TestForgeRunnerDispatchPlanPreviewCanonicalObservation(t *testing.T) {
	value, err := decodeForgeRunnerDispatchPlanPreview(forgeRunnerDispatchPlanPreviewFixture)
	if err != nil {
		t.Fatalf("decode Runner dispatch-plan preview fixture: %v", err)
	}
	if value.SchemaVersion != "forge.runner-dispatch-plan-preview/v1" ||
		value.EvaluationMode != "pure_dispatch_plan_preview_only" ||
		value.Owner.Issuer != "https://id.example" || value.Owner.Subject != "user-1" || value.Owner.TenantID != "tenant-1" ||
		value.ConversationID != "conversation-001" || value.RunID != "run-001" || value.AttemptID != "attempt-001" ||
		value.AttemptState != "accepted" || !value.AttemptStateAdmissible || value.CommandID != "command-001" ||
		value.TargetID != "runner-1" || value.LeaseEpoch != 1 || !value.LeaseActive || value.EvaluatedAtMS != 200500 ||
		value.CandidateCount != 2 || value.DeclarativeReadyCount != 1 || len(value.Candidates) != 2 ||
		value.Candidates[0].TargetID != "runner-1" || !value.Candidates[0].DeclarativeReady ||
		value.Candidates[1].TargetID != "runner-2" || value.Candidates[1].DeclarativeReady ||
		value.SelectedTargetID != nil || !value.PreviewOnly || value.Authority != (forgeRunnerDispatchPlanPreviewAuthority{}) {
		t.Fatalf("Runner dispatch-plan preview is not bounded display-only metadata: %#v", value)
	}
}

func TestForgeRunnerDispatchPlanPreviewRejectsUnknownDuplicateOwnerCandidateAndAuthorityMutations(t *testing.T) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(forgeRunnerDispatchPlanPreviewFixture, &root); err != nil {
		t.Fatal(err)
	}
	root["unexpected"] = json.RawMessage(`true`)
	unknown, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeForgeRunnerDispatchPlanPreview(unknown); err == nil {
		t.Fatal("unknown dispatch-plan field accepted")
	}

	duplicate := strings.TrimSpace(string(forgeRunnerDispatchPlanPreviewFixture))
	duplicate = strings.TrimSuffix(duplicate, "}") + `,"schema_version":"forge.runner-dispatch-plan-preview/v1"}`
	if _, err := decodeForgeRunnerDispatchPlanPreview([]byte(duplicate)); err == nil {
		t.Fatal("duplicate dispatch-plan field accepted")
	}
	nestedDuplicate := strings.Replace(
		string(forgeRunnerDispatchPlanPreviewFixture),
		"    \"audit_published\": false\n  }",
		"    \"audit_published\": false,\n    \"audit_published\": false\n  }",
		1,
	)
	if nestedDuplicate == string(forgeRunnerDispatchPlanPreviewFixture) {
		t.Fatal("nested duplicate mutation did not apply")
	}
	if _, err := decodeForgeRunnerDispatchPlanPreview([]byte(nestedDuplicate)); err == nil {
		t.Fatal("nested duplicate dispatch-plan field accepted")
	}

	mutations := map[string]string{
		"owner mismatch":     strings.Replace(string(forgeRunnerDispatchPlanPreviewFixture), `"subject": "user-1"`, `"subject": ""`, 1),
		"candidate order":    strings.Replace(string(forgeRunnerDispatchPlanPreviewFixture), `"target_id": "runner-1"`, `"target_id": "runner-3"`, 1),
		"readiness equation": strings.Replace(string(forgeRunnerDispatchPlanPreviewFixture), `"declarative_ready": true`, `"declarative_ready": false`, 1),
		"selected target":    strings.Replace(string(forgeRunnerDispatchPlanPreviewFixture), `"selected_target_id": null`, `"selected_target_id": "runner-1"`, 1),
		"authority":          strings.Replace(string(forgeRunnerDispatchPlanPreviewFixture), `"audit_published": false`, `"audit_published": true`, 1),
	}
	for name, raw := range mutations {
		t.Run(name, func(t *testing.T) {
			if raw == string(forgeRunnerDispatchPlanPreviewFixture) {
				t.Fatal("mutation did not apply")
			}
			if _, err := decodeForgeRunnerDispatchPlanPreview([]byte(raw)); err == nil {
				t.Fatalf("invalid %s mutation accepted", name)
			}
		})
	}
}
