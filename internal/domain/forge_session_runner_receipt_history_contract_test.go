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

const forgeSessionRunnerReceiptHistoryMaxSafeInteger uint64 = 9_007_199_254_740_991
const forgeSessionRunnerReceiptHistoryMaxReceipts = 16

//go:embed testdata/forge-session-runner-receipt-history-v1.json
var forgeSessionRunnerReceiptHistoryFixture []byte

type forgeSessionRunnerReceiptHistoryAuthority struct {
	IdentityVerified    bool `json:"identity_verified"`
	ReceiptPersisted    bool `json:"receipt_persisted"`
	ExecutionAuthorized bool `json:"execution_authorized"`
	DispatchPerformed   bool `json:"dispatch_performed"`
	AuditPublished      bool `json:"audit_published"`
}

type forgeSessionRunnerReceiptHistoryOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeSessionRunnerReceiptHistoryTerminalAuthority struct {
	DeviceIdentityVerified bool `json:"device_identity_verified"`
	CommandPersisted       bool `json:"command_persisted"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
	AuditPublished         bool `json:"audit_published"`
}

type forgeSessionRunnerReceiptHistoryTerminalReceipt struct {
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
	Authority              forgeSessionRunnerReceiptHistoryTerminalAuthority `json:"authority"`
}

type forgeSessionRunnerReceiptHistoryObservation struct {
	SchemaVersion         string                                          `json:"schema_version"`
	EvaluationMode        string                                          `json:"evaluation_mode"`
	Owner                 forgeSessionRunnerReceiptHistoryOwner           `json:"owner"`
	ConversationID        string                                          `json:"conversation_id"`
	PromptID              string                                          `json:"prompt_id"`
	RunID                 string                                          `json:"run_id"`
	ReceiptObservation    forgeSessionRunnerReceiptHistoryTerminalReceipt `json:"receipt_observation"`
	PromptRunBindingValid bool                                            `json:"prompt_run_binding_valid"`
	ReceiptBindingValid   bool                                            `json:"receipt_binding_valid"`
	PreviewOnly           bool                                            `json:"preview_only"`
	SelectedTargetID      *string                                         `json:"selected_target_id"`
	Authority             forgeSessionRunnerReceiptHistoryAuthority       `json:"authority"`
}

type forgeSessionRunnerReceiptHistory struct {
	SchemaVersion          string                                        `json:"schema_version"`
	EvaluationMode         string                                        `json:"evaluation_mode"`
	Owner                  forgeSessionRunnerReceiptHistoryOwner         `json:"owner"`
	ConversationID         string                                        `json:"conversation_id"`
	PromptID               string                                        `json:"prompt_id"`
	RunID                  string                                        `json:"run_id"`
	Receipts               []forgeSessionRunnerReceiptHistoryObservation `json:"receipts"`
	AttemptCount           uint32                                        `json:"attempt_count"`
	LatestAttemptID        string                                        `json:"latest_attempt_id"`
	LatestCommandID        string                                        `json:"latest_command_id"`
	LatestTargetID         string                                        `json:"latest_target_id"`
	LatestDispositionKind  string                                        `json:"latest_disposition_kind"`
	LatestObservedAtMS     uint64                                        `json:"latest_observed_at_ms"`
	ReconciliationRequired bool                                          `json:"reconciliation_required"`
	ManualReviewRequired   bool                                          `json:"manual_review_required"`
	AutomaticRetry         bool                                          `json:"automatic_retry"`
	FollowUp               string                                        `json:"follow_up"`
	SelectedTargetID       *string                                       `json:"selected_target_id"`
	PreviewOnly            bool                                          `json:"preview_only"`
	Authority              forgeSessionRunnerReceiptHistoryAuthority     `json:"authority"`
}

func TestForgeSessionRunnerReceiptHistory(t *testing.T) {
	history, err := decodeForgeSessionRunnerReceiptHistory(forgeSessionRunnerReceiptHistoryFixture)
	if err != nil {
		t.Fatalf("decode session Runner receipt history: %v", err)
	}
	if err := validateForgeSessionRunnerReceiptHistory(history); err != nil {
		t.Fatalf("invalid session Runner receipt history: %v", err)
	}
	if len(history.Receipts) != 2 ||
		history.LatestAttemptID != "attempt-002" ||
		history.LatestDispositionKind != "uncertain" ||
		!history.ReconciliationRequired ||
		!history.ManualReviewRequired ||
		history.AutomaticRetry ||
		history.FollowUp != "reconciliation_manual" {
		t.Fatalf("unexpected history reduction: %#v", history)
	}
}

func TestForgeSessionRunnerReceiptHistoryRejectsWireAndLifecycleDrift(t *testing.T) {
	base := bytes.TrimSpace(forgeSessionRunnerReceiptHistoryFixture)
	mutations := map[string][]byte{
		"unknown":   append(bytes.TrimSuffix(base, []byte("}")), []byte(`,"unexpected":true}`)...),
		"duplicate": append(bytes.TrimSuffix(base, []byte("}")), []byte(`,"schema_version":"forge.session-runner-receipt-history/v1"}`)...),
		"trailing":  append(base, []byte(" {}")...),
	}
	for name, mutation := range mutations {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeForgeSessionRunnerReceiptHistory(mutation); err == nil {
				t.Fatal("wire drift was accepted")
			}
		})
	}

	baseHistory, err := decodeForgeSessionRunnerReceiptHistory(base)
	if err != nil {
		t.Fatal(err)
	}
	lifecycleMutations := map[string]func(*forgeSessionRunnerReceiptHistory){
		"owner_drift": func(history *forgeSessionRunnerReceiptHistory) {
			history.Receipts[1].Owner.Subject = "user-foreign"
		},
		"session_drift": func(history *forgeSessionRunnerReceiptHistory) {
			history.Receipts[1].ConversationID = "conversation-foreign"
		},
		"prompt_drift": func(history *forgeSessionRunnerReceiptHistory) {
			history.Receipts[1].PromptID = "prompt-foreign"
		},
		"run_drift": func(history *forgeSessionRunnerReceiptHistory) {
			history.Receipts[1].RunID = "run-foreign"
		},
		"out_of_order": func(history *forgeSessionRunnerReceiptHistory) {
			history.Receipts[1].ReceiptObservation.ObservedAtMS = 50
		},
		"duplicate_attempt": func(history *forgeSessionRunnerReceiptHistory) {
			history.Receipts[1].ReceiptObservation.AttemptID =
				history.Receipts[0].ReceiptObservation.AttemptID
		},
		"completed_then_receipt": func(history *forgeSessionRunnerReceiptHistory) {
			setForgeSessionRunnerReceiptHistoryDisposition(&history.Receipts[0], "completed")
		},
		"uncertain_then_receipt": func(history *forgeSessionRunnerReceiptHistory) {
			setForgeSessionRunnerReceiptHistoryDisposition(&history.Receipts[0], "uncertain")
		},
		"summary_drift": func(history *forgeSessionRunnerReceiptHistory) {
			history.AttemptCount++
		},
		"selected_target": func(history *forgeSessionRunnerReceiptHistory) {
			history.SelectedTargetID = forgeSessionRunnerReceiptHistoryStringPtr("runner-2")
		},
		"authority": func(history *forgeSessionRunnerReceiptHistory) {
			history.Authority.ReceiptPersisted = true
		},
		"nested_authority": func(history *forgeSessionRunnerReceiptHistory) {
			history.Receipts[0].ReceiptObservation.Authority.CommandPersisted = true
		},
	}
	for name, mutate := range lifecycleMutations {
		t.Run(name, func(t *testing.T) {
			mutated := baseHistory
			mutate(&mutated)
			if err := validateForgeSessionRunnerReceiptHistory(mutated); err == nil {
				t.Fatal("lifecycle drift was accepted")
			}
		})
	}
}

func decodeForgeSessionRunnerReceiptHistory(raw []byte) (forgeSessionRunnerReceiptHistory, error) {
	if err := rejectDuplicateForgeSessionRunnerReceiptHistoryKeys(raw); err != nil {
		return forgeSessionRunnerReceiptHistory{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var history forgeSessionRunnerReceiptHistory
	if err := decoder.Decode(&history); err != nil {
		return forgeSessionRunnerReceiptHistory{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeSessionRunnerReceiptHistory{}, fmt.Errorf("trailing JSON")
		}
		return forgeSessionRunnerReceiptHistory{}, err
	}
	return history, nil
}

func validateForgeSessionRunnerReceiptHistory(history forgeSessionRunnerReceiptHistory) error {
	if history.SchemaVersion != "forge.session-runner-receipt-history/v1" ||
		history.EvaluationMode != "pure_session_runner_receipt_history_only" ||
		!validForgeSessionRunnerReceiptHistoryOwner(history.Owner) ||
		!validForgeSessionRunnerReceiptHistoryIdentifier(history.ConversationID) ||
		!validForgeSessionRunnerReceiptHistoryIdentifier(history.PromptID) ||
		!validForgeSessionRunnerReceiptHistoryIdentifier(history.RunID) ||
		len(history.Receipts) == 0 ||
		len(history.Receipts) > forgeSessionRunnerReceiptHistoryMaxReceipts ||
		history.Authority != (forgeSessionRunnerReceiptHistoryAuthority{}) ||
		history.SelectedTargetID != nil ||
		!history.PreviewOnly ||
		history.AutomaticRetry {
		return fmt.Errorf("invalid history envelope")
	}
	attempts := make(map[string]bool, len(history.Receipts))
	for index := range history.Receipts {
		receipt := &history.Receipts[index]
		if err := validateForgeSessionRunnerReceiptHistoryObservation(*receipt); err != nil ||
			receipt.Owner != history.Owner ||
			receipt.ConversationID != history.ConversationID ||
			receipt.PromptID != history.PromptID ||
			receipt.RunID != history.RunID ||
			attempts[receipt.ReceiptObservation.AttemptID] {
			return fmt.Errorf("invalid receipt binding at index %d", index)
		}
		attempts[receipt.ReceiptObservation.AttemptID] = true
		if index > 0 {
			previous := &history.Receipts[index-1]
			if forgeSessionRunnerReceiptHistoryAfter(*previous, *receipt) ||
				previous.ReceiptObservation.DispositionKind == "completed" ||
				previous.ReceiptObservation.DispositionKind == "uncertain" {
				return fmt.Errorf("invalid receipt lifecycle at index %d", index)
			}
		}
	}
	latest := history.Receipts[len(history.Receipts)-1].ReceiptObservation
	uncertain := latest.DispositionKind == "uncertain"
	followUp := "none"
	if uncertain {
		followUp = "reconciliation_manual"
	}
	if history.AttemptCount != uint32(len(history.Receipts)) ||
		history.LatestAttemptID != latest.AttemptID ||
		history.LatestCommandID != latest.CommandID ||
		history.LatestTargetID != latest.TargetID ||
		history.LatestDispositionKind != latest.DispositionKind ||
		history.LatestObservedAtMS != latest.ObservedAtMS ||
		history.ReconciliationRequired != uncertain ||
		history.ManualReviewRequired != uncertain ||
		history.FollowUp != followUp {
		return fmt.Errorf("history summary drift")
	}
	return nil
}

func validateForgeSessionRunnerReceiptHistoryObservation(observation forgeSessionRunnerReceiptHistoryObservation) error {
	receipt := observation.ReceiptObservation
	if observation.SchemaVersion != "forge.session-runner-receipt-observation/v1" ||
		observation.EvaluationMode != "pure_session_runner_receipt_binding_only" ||
		!validForgeSessionRunnerReceiptHistoryOwner(observation.Owner) ||
		!validForgeSessionRunnerReceiptHistoryIdentifier(observation.ConversationID) ||
		!validForgeSessionRunnerReceiptHistoryIdentifier(observation.PromptID) ||
		!validForgeSessionRunnerReceiptHistoryIdentifier(observation.RunID) ||
		!observation.PromptRunBindingValid ||
		!observation.ReceiptBindingValid ||
		!observation.PreviewOnly ||
		observation.SelectedTargetID != nil ||
		observation.Authority != (forgeSessionRunnerReceiptHistoryAuthority{}) ||
		receipt.SchemaVersion != "forge.runner-command-terminal-receipt/v1" ||
		receipt.EvaluationMode != "pure_runner_command_receipt_only" ||
		!validForgeSessionRunnerReceiptHistoryIdentifier(receipt.CommandID) ||
		!validForgeSessionRunnerReceiptHistoryIdentifier(receipt.AttemptID) ||
		!validForgeSessionRunnerReceiptHistoryIdentifier(receipt.TargetID) ||
		!validForgeSessionRunnerReceiptHistoryDigest(receipt.CommandSHA256) ||
		receipt.ObservedAtMS > forgeSessionRunnerReceiptHistoryMaxSafeInteger ||
		!receipt.ReceiptValid ||
		!receipt.PreviewOnly ||
		receipt.Authority != (forgeSessionRunnerReceiptHistoryTerminalAuthority{}) {
		return fmt.Errorf("invalid receipt observation")
	}
	uncertain := receipt.DispositionKind == "uncertain"
	if receipt.DispositionKind != "completed" &&
		receipt.DispositionKind != "failed" &&
		!uncertain {
		return fmt.Errorf("unknown disposition")
	}
	if receipt.Uncertain != uncertain ||
		receipt.ReconciliationRequired != uncertain ||
		receipt.ManualReviewRequired != uncertain ||
		receipt.AutomaticRetry ||
		(uncertain && receipt.FollowUp != "reconciliation_manual") ||
		(!uncertain && receipt.FollowUp != "none") {
		return fmt.Errorf("inconsistent outcome flags")
	}
	return nil
}

func forgeSessionRunnerReceiptHistoryAfter(left, right forgeSessionRunnerReceiptHistoryObservation) bool {
	leftTime := left.ReceiptObservation.ObservedAtMS
	rightTime := right.ReceiptObservation.ObservedAtMS
	return leftTime > rightTime ||
		(leftTime == rightTime &&
			left.ReceiptObservation.AttemptID >= right.ReceiptObservation.AttemptID)
}

func setForgeSessionRunnerReceiptHistoryDisposition(observation *forgeSessionRunnerReceiptHistoryObservation, disposition string) {
	uncertain := disposition == "uncertain"
	receipt := &observation.ReceiptObservation
	receipt.DispositionKind = disposition
	receipt.Uncertain = uncertain
	receipt.ReconciliationRequired = uncertain
	receipt.ManualReviewRequired = uncertain
	receipt.AutomaticRetry = false
	if uncertain {
		receipt.FollowUp = "reconciliation_manual"
	} else {
		receipt.FollowUp = "none"
	}
}

func validForgeSessionRunnerReceiptHistoryOwner(owner forgeSessionRunnerReceiptHistoryOwner) bool {
	return validForgeSessionRunnerReceiptHistoryOwnerPart(owner.Issuer) &&
		validForgeSessionRunnerReceiptHistoryOwnerPart(owner.Subject) &&
		validForgeSessionRunnerReceiptHistoryOwnerPart(owner.TenantID)
}

func validForgeSessionRunnerReceiptHistoryOwnerPart(value string) bool {
	return value != "" && len(value) <= 512 && strings.TrimSpace(value) == value &&
		!strings.ContainsAny(value, "\x00\r\n")
}

func validForgeSessionRunnerReceiptHistoryIdentifier(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for index, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') {
			continue
		}
		if index == 0 || !strings.ContainsRune("._:+/-", character) {
			return false
		}
	}
	return true
}

func validForgeSessionRunnerReceiptHistoryDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}

func forgeSessionRunnerReceiptHistoryStringPtr(value string) *string { return &value }

func rejectDuplicateForgeSessionRunnerReceiptHistoryKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanForgeSessionRunnerReceiptHistoryJSON(decoder); err != nil {
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

func scanForgeSessionRunnerReceiptHistoryJSON(decoder *json.Decoder) error {
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
			if err := scanForgeSessionRunnerReceiptHistoryJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := scanForgeSessionRunnerReceiptHistoryJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return fmt.Errorf("invalid JSON delimiter")
	}
}
