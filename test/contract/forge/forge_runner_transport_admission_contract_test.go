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

// Aero-ID consumes the transport observation as a bounded interoperability
// projection. It never authenticates a device, issues a lease, dispatches a
// Runner, executes work, or publishes Audit from this value.
//
//go:embed testdata/forge-runner-transport-admission-v1.json
var forgeRunnerTransportAdmissionFixture []byte

type forgeRunnerTransportAdmissionOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeRunnerTransportAdmissionAuthority struct {
	DeviceIdentityVerified bool `json:"device_identity_verified"`
	TransportAuthenticated bool `json:"transport_authenticated"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
	AuditPublished         bool `json:"audit_published"`
}

type forgeRunnerTransportAdmissionObservation struct {
	SchemaVersion          string                                 `json:"schema_version"`
	EvaluationMode         string                                 `json:"evaluation_mode"`
	Owner                  forgeRunnerTransportAdmissionOwner     `json:"owner"`
	ConversationID         string                                 `json:"conversation_id"`
	RunID                  string                                 `json:"run_id"`
	AttemptID              string                                 `json:"attempt_id"`
	AttemptState           string                                 `json:"attempt_state"`
	AttemptStateAdmissible bool                                   `json:"attempt_state_admissible"`
	CommandID              string                                 `json:"command_id"`
	CommandSHA256          string                                 `json:"command_sha256"`
	TargetID               string                                 `json:"target_id"`
	LeaseEpoch             uint64                                 `json:"lease_epoch"`
	LeaseIssuedAtMS        uint64                                 `json:"lease_issued_at_ms"`
	LeaseExpiresAtMS       uint64                                 `json:"lease_expires_at_ms"`
	EvaluatedAtMS          uint64                                 `json:"evaluated_at_ms"`
	TransportMethod        string                                 `json:"transport_method"`
	TransportPath          string                                 `json:"transport_path"`
	TransportTimestamp     int64                                  `json:"transport_timestamp"`
	TransportNonce         string                                 `json:"transport_nonce"`
	TransportPayloadSHA256 string                                 `json:"transport_payload_sha256"`
	TransportPayloadBytes  int                                    `json:"transport_payload_bytes"`
	TransportReplayChecked bool                                   `json:"transport_replay_checked"`
	LeaseProofCurrent      bool                                   `json:"lease_proof_current"`
	LeaseActive            bool                                   `json:"lease_active"`
	CommandBindingValid    bool                                   `json:"command_binding_valid"`
	TransportBindingValid  bool                                   `json:"transport_binding_valid"`
	AdmissionReady         bool                                   `json:"admission_ready"`
	RejectionReasons       []string                               `json:"rejection_reasons"`
	PreviewOnly            bool                                   `json:"preview_only"`
	Authority              forgeRunnerTransportAdmissionAuthority `json:"authority"`
}

func decodeForgeRunnerTransportAdmission(raw []byte) (forgeRunnerTransportAdmissionObservation, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeRunnerTransportAdmissionObservation{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value forgeRunnerTransportAdmissionObservation
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

func (value forgeRunnerTransportAdmissionObservation) validate() error {
	if value.SchemaVersion != "forge.runner-transport-admission/v1" ||
		value.EvaluationMode != "fenced_runner_transport_admission_preview" ||
		!validForgeRunnerTransportAdmissionOwner(value.Owner) ||
		value.ConversationID != "conversation-1" || value.RunID != "run-1" || value.AttemptID != "attempt-1" ||
		value.AttemptState != "accepted" || !value.AttemptStateAdmissible ||
		value.CommandID != "command-1" || !validForgeRunnerTransportAdmissionDigest(value.CommandSHA256) ||
		value.TargetID != "runner-1" || value.LeaseEpoch == 0 || value.LeaseIssuedAtMS == 0 ||
		value.LeaseExpiresAtMS <= value.LeaseIssuedAtMS || value.LeaseExpiresAtMS > 9007199254740991 ||
		value.EvaluatedAtMS == 0 || value.EvaluatedAtMS > 9007199254740991 ||
		value.TransportMethod != "POST" || value.TransportPath != "/api/v1/runners/runner-1/dispatch" ||
		value.TransportTimestamp <= 0 || value.TransportNonce == "" ||
		!validForgeRunnerTransportAdmissionDigest(value.TransportPayloadSHA256) ||
		value.TransportPayloadBytes <= 0 || value.TransportPayloadBytes > 1<<20 ||
		!value.TransportReplayChecked || !value.LeaseProofCurrent || !value.LeaseActive ||
		!value.CommandBindingValid || !value.TransportBindingValid || !value.AdmissionReady ||
		len(value.RejectionReasons) != 0 || !value.PreviewOnly ||
		value.Authority != (forgeRunnerTransportAdmissionAuthority{}) {
		return errors.New("invalid Runner transport admission")
	}
	return nil
}

func validForgeRunnerTransportAdmissionOwner(value forgeRunnerTransportAdmissionOwner) bool {
	return value.Issuer != "" && value.Subject != "" && value.TenantID != "" &&
		strings.TrimSpace(value.Issuer) == value.Issuer &&
		strings.TrimSpace(value.Subject) == value.Subject &&
		strings.TrimSpace(value.TenantID) == value.TenantID &&
		!strings.ContainsAny(value.Issuer+value.Subject+value.TenantID, "\x00\r\n")
}

func validForgeRunnerTransportAdmissionDigest(value string) bool {
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

func TestForgeRunnerTransportAdmissionCanonicalObservation(t *testing.T) {
	value, err := decodeForgeRunnerTransportAdmission(forgeRunnerTransportAdmissionFixture)
	if err != nil {
		t.Fatal(err)
	}
	if value.Owner.Subject != "user-1" || value.TransportPath != "/api/v1/runners/runner-1/dispatch" ||
		!value.AdmissionReady || value.Authority != (forgeRunnerTransportAdmissionAuthority{}) {
		t.Fatalf("unexpected transport admission observation: %#v", value)
	}
}

func TestForgeRunnerTransportAdmissionRejectsUnknownDuplicateAuthorityAndBinding(t *testing.T) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(forgeRunnerTransportAdmissionFixture, &root); err != nil {
		t.Fatal(err)
	}
	root["unexpected"] = json.RawMessage(`true`)
	unknown, _ := json.Marshal(root)
	if _, err := decodeForgeRunnerTransportAdmission(unknown); err == nil {
		t.Fatal("unknown field accepted")
	}
	duplicate := strings.TrimSuffix(strings.TrimSpace(string(forgeRunnerTransportAdmissionFixture)), "}") + `,"schema_version":"forge.runner-transport-admission/v1"}`
	if _, err := decodeForgeRunnerTransportAdmission([]byte(duplicate)); err == nil {
		t.Fatal("duplicate field accepted")
	}
	authority := strings.Replace(string(forgeRunnerTransportAdmissionFixture), `"transport_authenticated": false`, `"transport_authenticated": true`, 1)
	if _, err := decodeForgeRunnerTransportAdmission([]byte(authority)); err == nil {
		t.Fatal("transport authority mutation accepted")
	}
	path := strings.Replace(string(forgeRunnerTransportAdmissionFixture), `"transport_path": "/api/v1/runners/runner-1/dispatch"`, `"transport_path": "/api/v1/runners/runner-other/dispatch"`, 1)
	if _, err := decodeForgeRunnerTransportAdmission([]byte(path)); err == nil {
		t.Fatal("transport path confusion accepted")
	}
	payload := strings.Replace(string(forgeRunnerTransportAdmissionFixture), `"transport_payload_sha256": "6ad570170fca79ef62dad30b16f37314ea65f13895d4e0ea873b6512d1c99333"`, `"transport_payload_sha256": "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg"`, 1)
	if _, err := decodeForgeRunnerTransportAdmission([]byte(payload)); err == nil {
		t.Fatal("malformed transport payload digest accepted")
	}
}
