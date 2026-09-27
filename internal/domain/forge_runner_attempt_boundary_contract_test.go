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

// This receiver consumes only a lifecycle observation. It never persists an
// Attempt, mutates a lease, contacts a Runner, or grants execution authority.
//
//go:embed testdata/forge-runner-attempt-boundary-v1.json
var forgeRunnerAttemptBoundaryFixture []byte

const forgeRunnerAttemptBoundaryMaxSafeInteger = uint64(9007199254740991)

type forgeRunnerAttemptBoundaryOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeRunnerAttemptBoundaryAuthority struct {
	AttemptPersisted    bool `json:"attempt_persisted"`
	ReservationCreated  bool `json:"reservation_created"`
	ExecutionAuthorized bool `json:"execution_authorized"`
	DispatchPerformed   bool `json:"dispatch_performed"`
	AuditPublished      bool `json:"audit_published"`
}

type forgeRunnerAttemptBoundaryObservation struct {
	SchemaVersion             string                              `json:"schema_version"`
	EvaluationMode            string                              `json:"evaluation_mode"`
	Owner                     forgeRunnerAttemptBoundaryOwner     `json:"owner"`
	ConversationID            string                              `json:"conversation_id"`
	RunID                     string                              `json:"run_id"`
	AttemptID                 string                              `json:"attempt_id"`
	CommandID                 string                              `json:"command_id"`
	TargetID                  string                              `json:"target_id"`
	LeaseEpoch                uint64                              `json:"lease_epoch"`
	CurrentAttemptState       string                              `json:"current_attempt_state"`
	NextAttemptState          string                              `json:"next_attempt_state"`
	Transition                string                              `json:"transition"`
	ExecutionBoundaryReady    bool                                `json:"execution_boundary_ready"`
	AttemptTransitionValid    bool                                `json:"attempt_transition_valid"`
	AttemptTransitionDispatch bool                                `json:"attempt_transition_dispatchable"`
	AttemptBoundaryReady      bool                                `json:"attempt_boundary_ready"`
	RejectionReasons          []string                            `json:"rejection_reasons"`
	PreviewOnly               bool                                `json:"preview_only"`
	Authority                 forgeRunnerAttemptBoundaryAuthority `json:"authority"`
}

func TestForgeRunnerAttemptBoundaryReceiver(t *testing.T) {
	value, err := decodeForgeRunnerAttemptBoundary(forgeRunnerAttemptBoundaryFixture)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateForgeRunnerAttemptBoundary(value); err != nil {
		t.Fatal(err)
	}
	if value.Owner.Issuer != "https://id.example" || value.Owner.Subject != "user-1" ||
		value.Owner.TenantID != "tenant-1" || value.ConversationID != "conversation-1" ||
		value.RunID != "run-1" || value.AttemptID != "attempt-1" || value.CommandID != "command-1" ||
		value.TargetID != "runner-1" || value.LeaseEpoch != 1 || value.NextAttemptState != "starting" ||
		!value.AttemptBoundaryReady {
		t.Fatalf("canonical Attempt boundary drifted: %#v", value)
	}

	unknown := append(bytes.TrimSuffix(bytes.TrimSpace(forgeRunnerAttemptBoundaryFixture), []byte("}")), []byte(`,"unexpected":true}`)...)
	if _, err := decodeForgeRunnerAttemptBoundary(unknown); err == nil {
		t.Fatal("unknown Attempt boundary field accepted")
	}
	duplicate := append(bytes.TrimSuffix(bytes.TrimSpace(forgeRunnerAttemptBoundaryFixture), []byte("}")), []byte(`,"schema_version":"forge.runner-attempt-boundary/v1"}`)...)
	if _, err := decodeForgeRunnerAttemptBoundary(duplicate); err == nil {
		t.Fatal("duplicate Attempt boundary field accepted")
	}
	if _, err := decodeForgeRunnerAttemptBoundary(append(bytes.TrimSpace(forgeRunnerAttemptBoundaryFixture), []byte(" {}")...)); err == nil {
		t.Fatal("trailing Attempt boundary value accepted")
	}

	mutated := bytes.Replace(forgeRunnerAttemptBoundaryFixture, []byte(`"attempt_boundary_ready": true`), []byte(`"attempt_boundary_ready": false`), 1)
	value, err = decodeForgeRunnerAttemptBoundary(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if validateForgeRunnerAttemptBoundary(value) == nil {
		t.Fatal("readiness drift accepted")
	}
	mutated = bytes.Replace(forgeRunnerAttemptBoundaryFixture, []byte(`"dispatch_performed": false`), []byte(`"dispatch_performed": true`), 1)
	value, err = decodeForgeRunnerAttemptBoundary(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if validateForgeRunnerAttemptBoundary(value) == nil {
		t.Fatal("authority drift accepted")
	}

	nonReady := bytes.Replace(forgeRunnerAttemptBoundaryFixture, []byte(`"execution_boundary_ready": true`), []byte(`"execution_boundary_ready": false`), 1)
	nonReady = bytes.Replace(nonReady, []byte(`"attempt_boundary_ready": true`), []byte(`"attempt_boundary_ready": false`), 1)
	nonReady = bytes.Replace(nonReady, []byte(`"rejection_reasons": []`), []byte(`"rejection_reasons": ["execution_boundary_not_ready"]`), 1)
	value, err = decodeForgeRunnerAttemptBoundary(nonReady)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateForgeRunnerAttemptBoundary(value); err != nil {
		t.Fatalf("canonical non-ready rejection was rejected: %v", err)
	}
	value.RejectionReasons = []string{"unexpected_reason"}
	if validateForgeRunnerAttemptBoundary(value) == nil {
		t.Fatal("non-ready rejection reason drift accepted")
	}
}

func decodeForgeRunnerAttemptBoundary(raw []byte) (forgeRunnerAttemptBoundaryObservation, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeRunnerAttemptBoundaryObservation{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value forgeRunnerAttemptBoundaryObservation
	if err := decoder.Decode(&value); err != nil {
		return forgeRunnerAttemptBoundaryObservation{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || len(fields) != 19 {
		return forgeRunnerAttemptBoundaryObservation{}, errors.New("Attempt boundary fields are not exact")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeRunnerAttemptBoundaryObservation{}, errors.New("trailing JSON value")
		}
		return forgeRunnerAttemptBoundaryObservation{}, err
	}
	return value, nil
}

func validateForgeRunnerAttemptBoundary(value forgeRunnerAttemptBoundaryObservation) error {
	if value.SchemaVersion != "forge.runner-attempt-boundary/v1" ||
		value.EvaluationMode != "attempt_lifecycle_dispatch_boundary_preview" ||
		!validForgeRunnerAttemptBoundaryText(value.Owner.Issuer, 512) ||
		!validForgeRunnerAttemptBoundaryText(value.Owner.Subject, 512) ||
		!validForgeRunnerAttemptBoundaryText(value.Owner.TenantID, 512) ||
		!validForgeRunnerAttemptBoundaryIdentifier(value.ConversationID) ||
		!validForgeRunnerAttemptBoundaryIdentifier(value.RunID) ||
		!validForgeRunnerAttemptBoundaryIdentifier(value.AttemptID) ||
		!validForgeRunnerAttemptBoundaryIdentifier(value.CommandID) ||
		!validForgeRunnerAttemptBoundaryIdentifier(value.TargetID) ||
		value.LeaseEpoch == 0 || value.LeaseEpoch > forgeRunnerAttemptBoundaryMaxSafeInteger ||
		!validForgeRunnerAttemptBoundaryState(value.CurrentAttemptState) ||
		!validForgeRunnerAttemptBoundaryState(value.NextAttemptState) ||
		!validForgeRunnerAttemptBoundaryTransition(value.Transition) || !value.PreviewOnly ||
		value.Authority != (forgeRunnerAttemptBoundaryAuthority{}) {
		return errors.New("invalid Attempt boundary envelope")
	}
	valid := value.CurrentAttemptState == "accepted" && value.Transition == "begin_starting" ||
		value.CurrentAttemptState == "starting" && value.Transition == "observe_running"
	if value.AttemptTransitionValid != valid || value.NextAttemptState != forgeRunnerAttemptBoundaryTarget(value.Transition) {
		return errors.New("invalid Attempt transition")
	}
	dispatchable := valid && ((value.CurrentAttemptState == "accepted" && value.NextAttemptState == "starting") ||
		(value.CurrentAttemptState == "starting" && value.NextAttemptState == "running"))
	if value.AttemptTransitionDispatch != dispatchable || value.AttemptBoundaryReady != (value.ExecutionBoundaryReady && dispatchable) {
		return errors.New("invalid Attempt dispatch boundary")
	}
	if !forgeRunnerAttemptBoundarySameStrings(value.RejectionReasons, forgeRunnerAttemptBoundaryRejectionReasons(
		value.ExecutionBoundaryReady, valid, dispatchable,
	)) {
		return errors.New("Attempt boundary rejection reasons drifted")
	}
	if value.ExecutionBoundaryReady && len(value.RejectionReasons) != 0 {
		return errors.New("ready Attempt boundary has rejection reasons")
	}
	return nil
}

func forgeRunnerAttemptBoundaryRejectionReasons(boundaryReady, transitionValid, dispatchable bool) []string {
	reasons := make([]string, 0, 3)
	if !boundaryReady {
		reasons = append(reasons, "execution_boundary_not_ready")
	}
	if !transitionValid {
		reasons = append(reasons, "attempt_transition_invalid")
	} else if !dispatchable {
		reasons = append(reasons, "attempt_transition_not_dispatchable")
	}
	return reasons
}

func forgeRunnerAttemptBoundarySameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func validForgeRunnerAttemptBoundaryText(value string, max int) bool {
	return value != "" && len(value) <= max && value == strings.TrimSpace(value) &&
		!strings.ContainsAny(value, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x7f")
}

func validForgeRunnerAttemptBoundaryIdentifier(value string) bool {
	if !validForgeRunnerAttemptBoundaryText(value, 128) {
		return false
	}
	for index, character := range value {
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') && !(index > 0 && strings.ContainsRune(".:_+/-", character)) {
			return false
		}
	}
	return true
}

func validForgeRunnerAttemptBoundaryState(value string) bool {
	switch value {
	case "requested", "accepted", "starting", "running", "interrupted", "completed", "failed", "uncertain":
		return true
	default:
		return false
	}
}

func validForgeRunnerAttemptBoundaryTransition(value string) bool {
	switch value {
	case "accept", "begin_starting", "observe_running", "observe_interrupted", "observe_completed", "observe_failed", "observe_effect_outcome_uncertain":
		return true
	default:
		return false
	}
}

func forgeRunnerAttemptBoundaryTarget(value string) string {
	switch value {
	case "accept":
		return "accepted"
	case "begin_starting":
		return "starting"
	case "observe_running":
		return "running"
	case "observe_interrupted":
		return "interrupted"
	case "observe_completed":
		return "completed"
	case "observe_failed":
		return "failed"
	case "observe_effect_outcome_uncertain":
		return "uncertain"
	default:
		return ""
	}
}
