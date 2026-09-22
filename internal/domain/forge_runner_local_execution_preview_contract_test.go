package domain

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

// Forge local Runner execution-readiness is a metadata-only comparison value.
// It does not authenticate a Runner, persist a command or receipt, select a
// target, reserve capacity, dispatch work, publish Audit, or grant authority.
//
//go:embed testdata/forge-runner-local-execution-preview-v1.json
var forgeRunnerLocalExecutionPreviewFixture []byte

const forgeRunnerLocalExecutionPreviewMaxSafeInteger uint64 = 9_007_199_254_740_991

type forgeRunnerLocalExecutionPreviewOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeRunnerLocalExecutionPreviewAuthority struct {
	DeviceIdentityVerified bool `json:"device_identity_verified"`
	CommandPersisted       bool `json:"command_persisted"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
	AuditPublished         bool `json:"audit_published"`
}

type forgeRunnerLocalExecutionPreviewIntent struct {
	SchemaVersion             string                                    `json:"schema_version"`
	EvaluationMode            string                                    `json:"evaluation_mode"`
	Owner                     forgeRunnerLocalExecutionPreviewOwner     `json:"owner"`
	ConversationID            string                                    `json:"conversation_id"`
	PromptID                  string                                    `json:"prompt_id"`
	RunID                     string                                    `json:"run_id"`
	AttemptID                 string                                    `json:"attempt_id"`
	CommandID                 string                                    `json:"command_id"`
	TargetID                  string                                    `json:"target_id"`
	CommandSHA256             string                                    `json:"command_sha256"`
	IdempotencyKey            string                                    `json:"idempotency_key"`
	PromptRunBindingValid     bool                                      `json:"prompt_run_binding_valid"`
	RunnerCommandBindingValid bool                                      `json:"runner_command_binding_valid"`
	PreviewOnly               bool                                      `json:"preview_only"`
	SelectedTargetID          *string                                   `json:"selected_target_id"`
	Authority                 forgeRunnerLocalExecutionPreviewAuthority `json:"authority"`
}

type forgeRunnerLocalExecutionPreviewTerminalAuthority struct {
	DeviceIdentityVerified bool `json:"device_identity_verified"`
	CommandPersisted       bool `json:"command_persisted"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
	AuditPublished         bool `json:"audit_published"`
}

type forgeRunnerLocalExecutionPreviewTerminal struct {
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
	Authority              forgeRunnerLocalExecutionPreviewTerminalAuthority `json:"authority"`
}

type forgeRunnerLocalExecutionPreviewSessionAuthority struct {
	IdentityVerified    bool `json:"identity_verified"`
	ReceiptPersisted    bool `json:"receipt_persisted"`
	ExecutionAuthorized bool `json:"execution_authorized"`
	DispatchPerformed   bool `json:"dispatch_performed"`
	AuditPublished      bool `json:"audit_published"`
}

type forgeRunnerLocalExecutionPreviewSession struct {
	SchemaVersion         string                                           `json:"schema_version"`
	EvaluationMode        string                                           `json:"evaluation_mode"`
	Owner                 forgeRunnerLocalExecutionPreviewOwner            `json:"owner"`
	ConversationID        string                                           `json:"conversation_id"`
	PromptID              string                                           `json:"prompt_id"`
	RunID                 string                                           `json:"run_id"`
	ReceiptObservation    forgeRunnerLocalExecutionPreviewTerminal         `json:"receipt_observation"`
	PromptRunBindingValid bool                                             `json:"prompt_run_binding_valid"`
	ReceiptBindingValid   bool                                             `json:"receipt_binding_valid"`
	PreviewOnly           bool                                             `json:"preview_only"`
	SelectedTargetID      *string                                          `json:"selected_target_id"`
	Authority             forgeRunnerLocalExecutionPreviewSessionAuthority `json:"authority"`
}

type forgeRunnerLocalExecutionPreviewObservation struct {
	SchemaVersion   string                                    `json:"schema_version"`
	EvaluationMode  string                                    `json:"evaluation_mode"`
	Intent          forgeRunnerLocalExecutionPreviewIntent    `json:"runner_execution_intent"`
	SessionReceipt  forgeRunnerLocalExecutionPreviewSession   `json:"session_runner_receipt"`
	CommandID       string                                    `json:"command_id"`
	AttemptID       string                                    `json:"attempt_id"`
	TargetID        string                                    `json:"target_id"`
	CommandSHA256   string                                    `json:"command_sha256"`
	DispositionKind string                                    `json:"disposition_kind"`
	ObservedAtMS    uint64                                    `json:"observed_at_ms"`
	OutputBytes     uint64                                    `json:"output_bytes"`
	ExitCode        int64                                     `json:"exit_code"`
	ExecutorInvoked bool                                      `json:"executor_invoked"`
	PreviewOnly     bool                                      `json:"preview_only"`
	Authority       forgeRunnerLocalExecutionPreviewAuthority `json:"authority"`
}

func decodeForgeRunnerLocalExecutionPreview(raw []byte) (forgeRunnerLocalExecutionPreviewObservation, error) {
	if err := rejectDuplicateForgeRunnerLocalExecutionPreviewJSONKeys(raw); err != nil {
		return forgeRunnerLocalExecutionPreviewObservation{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value forgeRunnerLocalExecutionPreviewObservation
	if err := decoder.Decode(&value); err != nil {
		return forgeRunnerLocalExecutionPreviewObservation{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeRunnerLocalExecutionPreviewObservation{}, fmt.Errorf("trailing JSON")
		}
		return forgeRunnerLocalExecutionPreviewObservation{}, err
	}
	if err := value.validate(); err != nil {
		return forgeRunnerLocalExecutionPreviewObservation{}, err
	}
	return value, nil
}

func (value forgeRunnerLocalExecutionPreviewObservation) validate() error {
	if value.SchemaVersion != "forge.runner-local-execution-preview/v1" ||
		value.EvaluationMode != "injected_local_runner_preview_only" ||
		!validForgeRunnerLocalExecutionPreviewOwner(value.Intent.Owner) ||
		!validForgeRunnerLocalExecutionPreviewIdentifier(value.Intent.ConversationID) ||
		!validForgeRunnerLocalExecutionPreviewIdentifier(value.Intent.PromptID) ||
		!validForgeRunnerLocalExecutionPreviewIdentifier(value.Intent.RunID) ||
		!validForgeRunnerLocalExecutionPreviewIdentifier(value.Intent.AttemptID) ||
		!validForgeRunnerLocalExecutionPreviewIdentifier(value.Intent.CommandID) ||
		!validForgeRunnerLocalExecutionPreviewIdentifier(value.Intent.TargetID) ||
		!validForgeRunnerLocalExecutionPreviewDigest(value.Intent.CommandSHA256) ||
		!validForgeRunnerLocalExecutionPreviewIdentifier(value.CommandID) ||
		!validForgeRunnerLocalExecutionPreviewIdentifier(value.AttemptID) ||
		!validForgeRunnerLocalExecutionPreviewIdentifier(value.TargetID) ||
		!validForgeRunnerLocalExecutionPreviewDigest(value.CommandSHA256) ||
		!validForgeRunnerLocalExecutionPreviewText(value.DispositionKind) ||
		value.ObservedAtMS == 0 || value.ObservedAtMS > forgeRunnerLocalExecutionPreviewMaxSafeInteger ||
		value.OutputBytes > forgeRunnerLocalExecutionPreviewMaxSafeInteger ||
		value.ExitCode > int64(forgeRunnerLocalExecutionPreviewMaxSafeInteger) ||
		value.ExitCode < -int64(forgeRunnerLocalExecutionPreviewMaxSafeInteger) ||
		!value.ExecutorInvoked || !value.PreviewOnly ||
		value.Authority != (forgeRunnerLocalExecutionPreviewAuthority{}) ||
		value.Intent.SelectedTargetID != nil || value.SessionReceipt.SelectedTargetID != nil {
		return fmt.Errorf("invalid bounded local Runner execution preview")
	}
	if value.Intent.SchemaVersion != "forge.runner-execution-intent/v1" ||
		value.Intent.EvaluationMode != "pure_runner_binding_only" ||
		value.Intent.PromptID == "" || value.Intent.RunID == "" ||
		!value.Intent.PromptRunBindingValid || !value.Intent.RunnerCommandBindingValid ||
		!value.Intent.PreviewOnly ||
		value.Intent.Authority != (forgeRunnerLocalExecutionPreviewAuthority{}) ||
		value.Intent.IdempotencyKey != value.Intent.RunID+":"+value.Intent.AttemptID+":"+value.Intent.CommandID {
		return fmt.Errorf("invalid local Runner execution intent binding")
	}
	if value.CommandID != value.Intent.CommandID || value.AttemptID != value.Intent.AttemptID ||
		value.TargetID != value.Intent.TargetID || value.CommandSHA256 != value.Intent.CommandSHA256 ||
		value.SessionReceipt.Owner != value.Intent.Owner ||
		value.SessionReceipt.ConversationID != value.Intent.ConversationID ||
		value.SessionReceipt.PromptID != value.Intent.PromptID ||
		value.SessionReceipt.RunID != value.Intent.RunID {
		return fmt.Errorf("local Runner execution preview identity drift")
	}
	if value.SessionReceipt.SchemaVersion != "forge.session-runner-receipt-observation/v1" ||
		value.SessionReceipt.EvaluationMode != "pure_session_runner_receipt_binding_only" ||
		!value.SessionReceipt.PromptRunBindingValid || !value.SessionReceipt.ReceiptBindingValid ||
		!value.SessionReceipt.PreviewOnly ||
		value.SessionReceipt.Authority != (forgeRunnerLocalExecutionPreviewSessionAuthority{}) {
		return fmt.Errorf("invalid local Runner session receipt binding")
	}
	receipt := value.SessionReceipt.ReceiptObservation
	if receipt.SchemaVersion != "forge.runner-command-terminal-receipt/v1" ||
		receipt.EvaluationMode != "pure_runner_command_receipt_only" ||
		receipt.CommandID != value.CommandID || receipt.AttemptID != value.AttemptID ||
		receipt.TargetID != value.TargetID || receipt.CommandSHA256 != value.CommandSHA256 ||
		receipt.DispositionKind != value.DispositionKind || receipt.ObservedAtMS != value.ObservedAtMS ||
		!receipt.ReceiptValid || !receipt.PreviewOnly ||
		receipt.Authority != (forgeRunnerLocalExecutionPreviewTerminalAuthority{}) {
		return fmt.Errorf("invalid local Runner terminal receipt binding")
	}
	switch receipt.DispositionKind {
	case "completed", "failed":
		if receipt.Uncertain || receipt.ReconciliationRequired || receipt.ManualReviewRequired || receipt.AutomaticRetry || receipt.FollowUp != "none" {
			return fmt.Errorf("terminal disposition flags are inconsistent")
		}
	case "uncertain":
		if !receipt.Uncertain || !receipt.ReconciliationRequired || !receipt.ManualReviewRequired || receipt.AutomaticRetry || receipt.FollowUp != "reconciliation_manual" {
			return fmt.Errorf("uncertain disposition flags are inconsistent")
		}
	default:
		return fmt.Errorf("unknown terminal disposition")
	}
	return nil
}

func validForgeRunnerLocalExecutionPreviewOwner(owner forgeRunnerLocalExecutionPreviewOwner) bool {
	return validForgeRunnerLocalExecutionPreviewOwnerPart(owner.Issuer) &&
		validForgeRunnerLocalExecutionPreviewOwnerPart(owner.Subject) &&
		validForgeRunnerLocalExecutionPreviewOwnerPart(owner.TenantID)
}

func validForgeRunnerLocalExecutionPreviewOwnerPart(value string) bool {
	return value != "" && len(value) <= 512 && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

func validForgeRunnerLocalExecutionPreviewIdentifier(value string) bool {
	if value == "" || len(value) > 128 {
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

func validForgeRunnerLocalExecutionPreviewDigest(value string) bool {
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

func validForgeRunnerLocalExecutionPreviewText(value string) bool {
	return value != "" && len(value) <= 256 && !strings.ContainsAny(value, "\x00\r\n")
}

func TestForgeRunnerLocalExecutionPreviewCanonicalObservation(t *testing.T) {
	value, err := decodeForgeRunnerLocalExecutionPreview(forgeRunnerLocalExecutionPreviewFixture)
	if err != nil {
		t.Fatalf("decode local Runner execution preview fixture: %v", err)
	}
	if value.Intent.Owner.Issuer != "https://id.example" || value.Intent.Owner.Subject != "user-1" ||
		value.Intent.Owner.TenantID != "tenant-1" || value.Intent.ConversationID != "conversation-001" ||
		value.Intent.RunID != "run-001" || value.Intent.TargetID != "runner-1" ||
		value.DispositionKind != "completed" || value.OutputBytes != 8 || value.ExitCode != 0 {
		t.Fatalf("local Runner execution preview is not canonical metadata: %#v", value)
	}
}

func TestForgeRunnerLocalExecutionPreviewRejectsUnknownDuplicateBindingAndAuthorityMutations(t *testing.T) {
	base := string(forgeRunnerLocalExecutionPreviewFixture)
	mutations := map[string]string{
		"unknown":   strings.TrimSuffix(strings.TrimSpace(base), "}") + `,"unexpected":true}`,
		"duplicate": strings.TrimSuffix(strings.TrimSpace(base), "}") + `,"schema_version":"forge.runner-local-execution-preview/v1"}`,
		"owner":     strings.Replace(base, `"subject": "user-1"`, `"subject": ""`, 1),
		"identity":  strings.Replace(base, `"command_id": "command-001"`, `"command_id": "command-foreign"`, 1),
		"selection": strings.Replace(base, `"selected_target_id": null`, `"selected_target_id": "runner-1"`, 1),
		"authority": strings.Replace(base, `"execution_authorized": false`, `"execution_authorized": true`, 1),
	}
	for name, raw := range mutations {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeForgeRunnerLocalExecutionPreview([]byte(raw)); err == nil {
				t.Fatalf("invalid %s mutation accepted", name)
			}
		})
	}
}

func rejectDuplicateForgeRunnerLocalExecutionPreviewJSONKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanForgeRunnerLocalExecutionPreviewJSONValue(decoder); err != nil {
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

func scanForgeRunnerLocalExecutionPreviewJSONValue(decoder *json.Decoder) error {
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
		seen := map[string]struct{}{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			keyText, ok := key.(string)
			if !ok {
				return fmt.Errorf("JSON object key is not a string")
			}
			if _, exists := seen[keyText]; exists {
				return fmt.Errorf("duplicate JSON object key %q", keyText)
			}
			seen[keyText] = struct{}{}
			if err := scanForgeRunnerLocalExecutionPreviewJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return fmt.Errorf("JSON object did not close")
		}
	case '[':
		for decoder.More() {
			if err := scanForgeRunnerLocalExecutionPreviewJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return fmt.Errorf("JSON array did not close")
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter")
	}
	return nil
}
