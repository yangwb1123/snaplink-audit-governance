package forge

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

// Audit Governance consumes this execution-boundary observation as a bounded,
// display-only interoperability projection. It never authenticates a device,
// issues a lease, dispatches a Runner, executes work, or publishes Audit from
// this value.
//
//go:embed testdata/forge-runner-execution-boundary-v1.json
var forgeRunnerExecutionBoundaryFixture []byte

type forgeRunnerExecutionBoundaryOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeRunnerExecutionBoundaryAuthority struct {
	DeviceIdentityVerified bool `json:"device_identity_verified"`
	CommandPersisted       bool `json:"command_persisted"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
	AuditPublished         bool `json:"audit_published"`
}

type forgeRunnerExecutionBoundaryObservation struct {
	SchemaVersion           string                                `json:"schema_version"`
	EvaluationMode          string                                `json:"evaluation_mode"`
	Mode                    string                                `json:"mode"`
	Owner                   forgeRunnerExecutionBoundaryOwner     `json:"owner"`
	ConversationID          string                                `json:"conversation_id"`
	RunID                   string                                `json:"run_id"`
	AttemptID               string                                `json:"attempt_id"`
	AttemptState            string                                `json:"attempt_state"`
	CommandID               string                                `json:"command_id"`
	CommandSHA256           string                                `json:"command_sha256"`
	TargetID                string                                `json:"target_id"`
	LeaseEpoch              uint64                                `json:"lease_epoch"`
	ActivationAllowed       bool                                  `json:"activation_allowed"`
	RunnerAuthorityAccepted bool                                  `json:"runner_authority_accepted"`
	DispatchAdmissionReady  bool                                  `json:"dispatch_admission_ready"`
	TransportAdmissionReady bool                                  `json:"transport_admission_ready"`
	EffectState             string                                `json:"effect_state"`
	EffectStateStartable    bool                                  `json:"effect_state_startable"`
	CancellationClear       bool                                  `json:"cancellation_clear"`
	ExecutionBoundaryReady  bool                                  `json:"execution_boundary_ready"`
	RejectionReasons        []string                              `json:"rejection_reasons"`
	PreviewOnly             bool                                  `json:"preview_only"`
	Authority               forgeRunnerExecutionBoundaryAuthority `json:"authority"`
}

func decodeForgeRunnerExecutionBoundary(raw []byte) (forgeRunnerExecutionBoundaryObservation, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeRunnerExecutionBoundaryObservation{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value forgeRunnerExecutionBoundaryObservation
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

func (value forgeRunnerExecutionBoundaryObservation) validate() error {
	if value.SchemaVersion != "forge.runner-execution-boundary/v1" ||
		value.EvaluationMode != "p4_runner_authority_execution_boundary_preview" ||
		value.Mode != "execute" || !validForgeRunnerExecutionBoundaryOwner(value.Owner) ||
		value.ConversationID != "conversation-1" || value.RunID != "run-1" || value.AttemptID != "attempt-1" ||
		value.AttemptState != "accepted" || value.CommandID != "command-1" ||
		!validForgeRunnerExecutionBoundaryDigest(value.CommandSHA256) || value.TargetID != "runner-a" ||
		value.LeaseEpoch != 3 || !value.ActivationAllowed || !value.RunnerAuthorityAccepted ||
		!value.DispatchAdmissionReady || !value.TransportAdmissionReady || value.EffectState != "not_started" ||
		!value.EffectStateStartable || !value.CancellationClear || !value.ExecutionBoundaryReady ||
		len(value.RejectionReasons) != 0 || !value.PreviewOnly ||
		value.Authority != (forgeRunnerExecutionBoundaryAuthority{}) {
		return errors.New("invalid Runner execution boundary")
	}
	return nil
}

func validForgeRunnerExecutionBoundaryOwner(value forgeRunnerExecutionBoundaryOwner) bool {
	return value.Issuer != "" && value.Subject != "" && value.TenantID != "" &&
		strings.TrimSpace(value.Issuer) == value.Issuer &&
		strings.TrimSpace(value.Subject) == value.Subject &&
		strings.TrimSpace(value.TenantID) == value.TenantID &&
		!strings.ContainsAny(value.Issuer+value.Subject+value.TenantID, "\x00\r\n")
}

func validForgeRunnerExecutionBoundaryDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if !strings.ContainsRune("0123456789abcdef", char) {
			return false
		}
	}
	return true
}

func TestForgeRunnerExecutionBoundaryCanonicalObservation(t *testing.T) {
	value, err := decodeForgeRunnerExecutionBoundary(forgeRunnerExecutionBoundaryFixture)
	if err != nil {
		t.Fatal(err)
	}
	if value.Owner.Subject != "user-1" || value.TargetID != "runner-a" ||
		!value.ExecutionBoundaryReady || value.Authority != (forgeRunnerExecutionBoundaryAuthority{}) {
		t.Fatalf("unexpected execution boundary observation: %#v", value)
	}
}

func TestForgeRunnerExecutionBoundaryRejectsUnknownDuplicateAuthorityAndReadiness(t *testing.T) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(forgeRunnerExecutionBoundaryFixture, &root); err != nil {
		t.Fatal(err)
	}
	root["unexpected"] = json.RawMessage(`true`)
	unknown, _ := json.Marshal(root)
	if _, err := decodeForgeRunnerExecutionBoundary(unknown); err == nil {
		t.Fatal("unknown field accepted")
	}
	duplicate := strings.TrimSuffix(strings.TrimSpace(string(forgeRunnerExecutionBoundaryFixture)), "}") + `,"mode":"execute"}`
	if _, err := decodeForgeRunnerExecutionBoundary([]byte(duplicate)); err == nil {
		t.Fatal("duplicate field accepted")
	}
	authority := strings.Replace(string(forgeRunnerExecutionBoundaryFixture), `"execution_authorized": false`, `"execution_authorized": true`, 1)
	if _, err := decodeForgeRunnerExecutionBoundary([]byte(authority)); err == nil {
		t.Fatal("authority mutation accepted")
	}
	readiness := strings.Replace(string(forgeRunnerExecutionBoundaryFixture), `"execution_boundary_ready": true`, `"execution_boundary_ready": false`, 1)
	if _, err := decodeForgeRunnerExecutionBoundary([]byte(readiness)); err == nil {
		t.Fatal("readiness drift accepted")
	}
}
