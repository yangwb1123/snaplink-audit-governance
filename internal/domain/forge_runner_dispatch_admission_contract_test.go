package domain

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

//go:embed testdata/forge-runner-dispatch-admission-v1.json
var forgeRunnerDispatchAdmissionFixture []byte

type forgeRunnerDispatchAdmissionOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}
type forgeRunnerDispatchAdmissionAuthority struct {
	DeviceIdentityVerified bool `json:"device_identity_verified"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
	AuditPublished         bool `json:"audit_published"`
}
type forgeRunnerDispatchAdmissionObservation struct {
	SchemaVersion          string                                `json:"schema_version"`
	EvaluationMode         string                                `json:"evaluation_mode"`
	Owner                  forgeRunnerDispatchAdmissionOwner     `json:"owner"`
	ConversationID         string                                `json:"conversation_id"`
	RunID                  string                                `json:"run_id"`
	AttemptID              string                                `json:"attempt_id"`
	AttemptState           string                                `json:"attempt_state"`
	AttemptStateAdmissible bool                                  `json:"attempt_state_admissible"`
	CommandID              string                                `json:"command_id"`
	CommandSHA256          string                                `json:"command_sha256"`
	TargetID               string                                `json:"target_id"`
	LeaseEpoch             uint64                                `json:"lease_epoch"`
	LeaseIssuedAtMS        uint64                                `json:"lease_issued_at_ms"`
	LeaseExpiresAtMS       uint64                                `json:"lease_expires_at_ms"`
	EvaluatedAtMS          uint64                                `json:"evaluated_at_ms"`
	LeaseProofCurrent      bool                                  `json:"lease_proof_current"`
	LeaseActive            bool                                  `json:"lease_active"`
	CommandBindingValid    bool                                  `json:"command_binding_valid"`
	AdmissionReady         bool                                  `json:"admission_ready"`
	RejectionReasons       []string                              `json:"rejection_reasons"`
	PreviewOnly            bool                                  `json:"preview_only"`
	Authority              forgeRunnerDispatchAdmissionAuthority `json:"authority"`
}

func decodeForgeRunnerDispatchAdmission(raw []byte) (forgeRunnerDispatchAdmissionObservation, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeRunnerDispatchAdmissionObservation{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value forgeRunnerDispatchAdmissionObservation
	if err := decoder.Decode(&value); err != nil {
		return value, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return value, err
	}
	if err := value.validate(); err != nil {
		return value, err
	}
	return value, nil
}
func (value forgeRunnerDispatchAdmissionObservation) validate() error {
	if value.SchemaVersion != "forge.runner-dispatch-admission/v1" || value.EvaluationMode != "durable_lease_bound_dispatch_admission_preview" || value.Owner.Issuer == "" || value.Owner.Subject == "" || value.Owner.TenantID == "" || value.ConversationID != "conversation-1" || value.RunID != "run-1" || value.AttemptID != "attempt-1" || value.AttemptState != "accepted" || !value.AttemptStateAdmissible || value.CommandID != "command-1" || len(value.CommandSHA256) != 64 || value.TargetID != "runner-a" || value.LeaseEpoch != 1 || value.LeaseIssuedAtMS == 0 || value.LeaseExpiresAtMS <= value.LeaseIssuedAtMS || value.EvaluatedAtMS == 0 || !value.LeaseProofCurrent || !value.LeaseActive || !value.CommandBindingValid || !value.AdmissionReady || len(value.RejectionReasons) != 0 || !value.PreviewOnly || value.Authority != (forgeRunnerDispatchAdmissionAuthority{}) {
		return errors.New("invalid Runner dispatch admission")
	}
	return nil
}
func TestForgeRunnerDispatchAdmissionCanonicalObservation(t *testing.T) {
	value, err := decodeForgeRunnerDispatchAdmission(forgeRunnerDispatchAdmissionFixture)
	if err != nil {
		t.Fatal(err)
	}
	if value.TargetID != "runner-a" || !value.AdmissionReady {
		t.Fatalf("unexpected admission observation: %#v", value)
	}
}
func TestForgeRunnerDispatchAdmissionRejectsUnknownDuplicateAndAuthority(t *testing.T) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(forgeRunnerDispatchAdmissionFixture, &root); err != nil {
		t.Fatal(err)
	}
	root["unexpected"] = json.RawMessage(`true`)
	unknown, _ := json.Marshal(root)
	if _, err := decodeForgeRunnerDispatchAdmission(unknown); err == nil {
		t.Fatal("unknown field accepted")
	}
	duplicate := strings.TrimSuffix(strings.TrimSpace(string(forgeRunnerDispatchAdmissionFixture)), "}") + `,"schema_version":"forge.runner-dispatch-admission/v1"}`
	if _, err := decodeForgeRunnerDispatchAdmission([]byte(duplicate)); err == nil {
		t.Fatal("duplicate field accepted")
	}
	authority := strings.Replace(string(forgeRunnerDispatchAdmissionFixture), `"execution_authorized": false`, `"execution_authorized": true`, 1)
	if _, err := decodeForgeRunnerDispatchAdmission([]byte(authority)); err == nil {
		t.Fatal("authority mutation accepted")
	}
}
