package domain

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

const forgeSessionRunnerReceiptVectorsMaxSafeInteger uint64 = 9_007_199_254_740_991

//go:embed testdata/forge-session-runner-receipt-vectors-v1.json
var forgeSessionRunnerReceiptVectorsFixture []byte

type forgeSessionRunnerReceiptVectorsAuthority struct {
	IdentityVerified    bool `json:"identity_verified"`
	ReceiptPersisted    bool `json:"receipt_persisted"`
	ExecutionAuthorized bool `json:"execution_authorized"`
	DispatchPerformed   bool `json:"dispatch_performed"`
	AuditPublished      bool `json:"audit_published"`
}

type forgeSessionRunnerReceiptVectorsOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeSessionRunnerReceiptVectorsTerminalAuthority struct {
	DeviceIdentityVerified bool `json:"device_identity_verified"`
	CommandPersisted       bool `json:"command_persisted"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
	AuditPublished         bool `json:"audit_published"`
}

type forgeSessionRunnerReceiptVectorsTerminal struct {
	SchemaVersion          string                                            `json:"schema_version"`
	EvaluationMode         string                                            `json:"evaluation_mode"`
	CommandID              string                                            `json:"command_id"`
	CommandSHA256          string                                            `json:"command_sha256"`
	AttemptID              string                                            `json:"attempt_id"`
	TargetID               string                                            `json:"target_id"`
	DispositionKind        string                                            `json:"disposition_kind"`
	ObservedAtMS           uint64                                            `json:"observed_at_ms"`
	ReceiptValid           bool                                              `json:"receipt_valid"`
	PreviewOnly            bool                                              `json:"preview_only"`
	Uncertain              bool                                              `json:"uncertain"`
	ReconciliationRequired bool                                              `json:"reconciliation_required"`
	ManualReviewRequired   bool                                              `json:"manual_review_required"`
	AutomaticRetry         bool                                              `json:"automatic_retry"`
	FollowUp               string                                            `json:"follow_up"`
	Authority              forgeSessionRunnerReceiptVectorsTerminalAuthority `json:"authority"`
}

type forgeSessionRunnerReceiptVectorsObservation struct {
	SchemaVersion         string                                    `json:"schema_version"`
	EvaluationMode        string                                    `json:"evaluation_mode"`
	Owner                 forgeSessionRunnerReceiptVectorsOwner     `json:"owner"`
	ConversationID        string                                    `json:"conversation_id"`
	PromptID              string                                    `json:"prompt_id"`
	RunID                 string                                    `json:"run_id"`
	ReceiptObservation    forgeSessionRunnerReceiptVectorsTerminal  `json:"receipt_observation"`
	PromptRunBindingValid bool                                      `json:"prompt_run_binding_valid"`
	ReceiptBindingValid   bool                                      `json:"receipt_binding_valid"`
	PreviewOnly           bool                                      `json:"preview_only"`
	SelectedTargetID      *string                                   `json:"selected_target_id"`
	Authority             forgeSessionRunnerReceiptVectorsAuthority `json:"authority"`
}

type forgeSessionRunnerReceiptVectorsExpected struct {
	CommandID              string `json:"command_id"`
	CommandSHA256          string `json:"command_sha256"`
	AttemptID              string `json:"attempt_id"`
	TargetID               string `json:"target_id"`
	DispositionKind        string `json:"disposition_kind"`
	ObservedAtMS           uint64 `json:"observed_at_ms"`
	Uncertain              bool   `json:"uncertain"`
	ReconciliationRequired bool   `json:"reconciliation_required"`
	ManualReviewRequired   bool   `json:"manual_review_required"`
	AutomaticRetry         bool   `json:"automatic_retry"`
	FollowUp               string `json:"follow_up"`
}

type forgeSessionRunnerReceiptVectorsVector struct {
	Name        string                                      `json:"name"`
	Observation forgeSessionRunnerReceiptVectorsObservation `json:"observation"`
	Expected    forgeSessionRunnerReceiptVectorsExpected    `json:"expected"`
}

type forgeSessionRunnerReceiptVectorsFixtureEnvelope struct {
	SchemaVersion  string                                    `json:"schema_version"`
	EvaluationMode string                                    `json:"evaluation_mode"`
	Authority      forgeSessionRunnerReceiptVectorsAuthority `json:"authority"`
	Vectors        []forgeSessionRunnerReceiptVectorsVector  `json:"vectors"`
}

func TestForgeSessionRunnerReceiptVectors(t *testing.T) {
	fixture, err := decodeForgeSessionRunnerReceiptVectors(forgeSessionRunnerReceiptVectorsFixture)
	if err != nil {
		t.Fatalf("decode session Runner receipt vectors: %v", err)
	}
	if fixture.SchemaVersion != "forge.session-runner-receipt-vectors/v1" ||
		fixture.EvaluationMode != "pure_session_runner_receipt_vectors_only" ||
		fixture.Authority != (forgeSessionRunnerReceiptVectorsAuthority{}) ||
		len(fixture.Vectors) != 3 {
		t.Fatalf("invalid session Runner receipt vectors envelope: %#v", fixture)
	}
	seen := map[string]bool{}
	for _, vector := range fixture.Vectors {
		if vector.Name == "" || seen[vector.Name] {
			t.Fatalf("duplicate or empty vector name: %q", vector.Name)
		}
		seen[vector.Name] = true
		if err := validateForgeSessionRunnerReceiptVector(vector); err != nil {
			t.Fatalf("vector %q: %v", vector.Name, err)
		}
	}
}

func TestForgeSessionRunnerReceiptVectorsRejectWireAndOutcomeDrift(t *testing.T) {
	base := bytes.TrimSpace(forgeSessionRunnerReceiptVectorsFixture)
	mutations := map[string][]byte{
		"unknown":   append(bytes.TrimSuffix(base, []byte("}")), []byte(`,"unexpected":true}`)...),
		"duplicate": append(bytes.TrimSuffix(base, []byte("}")), []byte(`,"schema_version":"forge.session-runner-receipt-vectors/v1"}`)...),
		"trailing":  append(base, []byte(" {}")...),
	}
	for name, mutation := range mutations {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeForgeSessionRunnerReceiptVectors(mutation); err == nil {
				t.Fatal("wire drift was accepted")
			}
		})
	}
	fixture, err := decodeForgeSessionRunnerReceiptVectors(base)
	if err != nil {
		t.Fatal(err)
	}
	foreign := fixture.Vectors[0]
	foreign.Observation.ReceiptObservation.CommandID = "command-foreign"
	if err := validateForgeSessionRunnerReceiptVector(foreign); err == nil {
		t.Fatal("command binding drift was accepted")
	}
	retry := fixture.Vectors[2]
	retry.Observation.ReceiptObservation.AutomaticRetry = true
	if err := validateForgeSessionRunnerReceiptVector(retry); err == nil {
		t.Fatal("uncertain automatic retry was accepted")
	}
	selected := fixture.Vectors[2]
	selected.Observation.SelectedTargetID = forgeSessionRunnerReceiptVectorsStringPtr("runner-1")
	if err := validateForgeSessionRunnerReceiptVector(selected); err == nil {
		t.Fatal("selected target was accepted")
	}
}

func decodeForgeSessionRunnerReceiptVectors(raw []byte) (forgeSessionRunnerReceiptVectorsFixtureEnvelope, error) {
	if err := rejectDuplicateForgeSessionRunnerReceiptVectorsKeys(raw); err != nil {
		return forgeSessionRunnerReceiptVectorsFixtureEnvelope{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var fixture forgeSessionRunnerReceiptVectorsFixtureEnvelope
	if err := decoder.Decode(&fixture); err != nil {
		return forgeSessionRunnerReceiptVectorsFixtureEnvelope{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeSessionRunnerReceiptVectorsFixtureEnvelope{}, fmt.Errorf("trailing JSON")
		}
		return forgeSessionRunnerReceiptVectorsFixtureEnvelope{}, err
	}
	return fixture, nil
}

func validateForgeSessionRunnerReceiptVector(vector forgeSessionRunnerReceiptVectorsVector) error {
	observation, receipt, expected := vector.Observation, vector.Observation.ReceiptObservation, vector.Expected
	if observation.SchemaVersion != "forge.session-runner-receipt-observation/v1" ||
		observation.EvaluationMode != "pure_session_runner_receipt_binding_only" ||
		!validForgeSessionRunnerReceiptVectorsOwner(observation.Owner) ||
		!validForgeSessionRunnerReceiptVectorsIdentifier(observation.ConversationID) ||
		!validForgeSessionRunnerReceiptVectorsIdentifier(observation.PromptID) ||
		!validForgeSessionRunnerReceiptVectorsIdentifier(observation.RunID) ||
		!observation.PromptRunBindingValid || !observation.ReceiptBindingValid ||
		!observation.PreviewOnly || observation.SelectedTargetID != nil ||
		observation.Authority != (forgeSessionRunnerReceiptVectorsAuthority{}) ||
		receipt.SchemaVersion != "forge.runner-command-terminal-receipt/v1" ||
		receipt.EvaluationMode != "pure_runner_command_receipt_only" ||
		!validForgeSessionRunnerReceiptVectorsIdentifier(receipt.CommandID) ||
		!validForgeSessionRunnerReceiptVectorsIdentifier(receipt.AttemptID) ||
		!validForgeSessionRunnerReceiptVectorsIdentifier(receipt.TargetID) ||
		!validForgeSessionRunnerReceiptVectorsDigest(receipt.CommandSHA256) ||
		receipt.ObservedAtMS > forgeSessionRunnerReceiptVectorsMaxSafeInteger ||
		!receipt.ReceiptValid || !receipt.PreviewOnly ||
		receipt.Authority != (forgeSessionRunnerReceiptVectorsTerminalAuthority{}) ||
		receipt.CommandID != expected.CommandID ||
		receipt.CommandSHA256 != expected.CommandSHA256 ||
		receipt.AttemptID != expected.AttemptID ||
		receipt.TargetID != expected.TargetID ||
		receipt.DispositionKind != expected.DispositionKind ||
		receipt.ObservedAtMS != expected.ObservedAtMS ||
		receipt.Uncertain != expected.Uncertain ||
		receipt.ReconciliationRequired != expected.ReconciliationRequired ||
		receipt.ManualReviewRequired != expected.ManualReviewRequired ||
		receipt.AutomaticRetry != expected.AutomaticRetry ||
		receipt.FollowUp != expected.FollowUp {
		return fmt.Errorf("invalid session Runner receipt vector")
	}
	uncertain := receipt.DispositionKind == "uncertain"
	if receipt.DispositionKind != "completed" && receipt.DispositionKind != "failed" && !uncertain {
		return fmt.Errorf("unknown disposition")
	}
	if receipt.Uncertain != uncertain || receipt.ReconciliationRequired != uncertain ||
		receipt.ManualReviewRequired != uncertain || receipt.AutomaticRetry ||
		(uncertain && receipt.FollowUp != "reconciliation_manual") ||
		(!uncertain && receipt.FollowUp != "none") {
		return fmt.Errorf("inconsistent outcome flags")
	}
	return nil
}

func validForgeSessionRunnerReceiptVectorsOwner(owner forgeSessionRunnerReceiptVectorsOwner) bool {
	return validForgeSessionRunnerReceiptVectorsOwnerPart(owner.Issuer) &&
		validForgeSessionRunnerReceiptVectorsOwnerPart(owner.Subject) &&
		validForgeSessionRunnerReceiptVectorsOwnerPart(owner.TenantID)
}

func validForgeSessionRunnerReceiptVectorsOwnerPart(value string) bool {
	return value != "" && len(value) <= 512 && strings.TrimSpace(value) == value &&
		!strings.ContainsAny(value, "\x00\r\n")
}

func validForgeSessionRunnerReceiptVectorsIdentifier(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for index, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') {
			continue
		}
		if index == 0 || !strings.ContainsRune("._:+/-", character) {
			return false
		}
	}
	return true
}

func validForgeSessionRunnerReceiptVectorsDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}

func forgeSessionRunnerReceiptVectorsStringPtr(value string) *string { return &value }

func rejectDuplicateForgeSessionRunnerReceiptVectorsKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanForgeSessionRunnerReceiptVectorsJSON(decoder); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON")
		}
		return err
	}
	return nil
}

func scanForgeSessionRunnerReceiptVectorsJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return fmt.Errorf("duplicate object key")
			}
			seen[name] = true
			if err := scanForgeSessionRunnerReceiptVectorsJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := scanForgeSessionRunnerReceiptVectorsJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return fmt.Errorf("invalid JSON delimiter")
	}
}
