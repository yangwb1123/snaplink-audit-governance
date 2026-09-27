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

const forgeSessionRunnerReconciliationProjectionMaxSafeInteger uint64 = 9_007_199_254_740_991
const forgeSessionRunnerReconciliationProjectionMaxAttempts uint32 = 16

//go:embed testdata/forge-session-runner-reconciliation-projection-v1.json
var forgeSessionRunnerReconciliationProjectionFixture []byte

type forgeSessionRunnerReconciliationProjectionAuthority struct {
	IdentityVerified    bool `json:"identity_verified"`
	ReceiptPersisted    bool `json:"receipt_persisted"`
	ExecutionAuthorized bool `json:"execution_authorized"`
	DispatchPerformed   bool `json:"dispatch_performed"`
	AuditPublished      bool `json:"audit_published"`
}

type forgeSessionRunnerReconciliationProjectionOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeSessionRunnerReconciliationProjectionSource struct {
	SchemaVersion         string                                          `json:"schema_version"`
	Owner                 forgeSessionRunnerReconciliationProjectionOwner `json:"owner"`
	ConversationID        string                                          `json:"conversation_id"`
	PromptID              string                                          `json:"prompt_id"`
	RunID                 string                                          `json:"run_id"`
	AttemptCount          uint32                                          `json:"attempt_count"`
	LatestAttemptID       string                                          `json:"latest_attempt_id"`
	LatestCommandID       string                                          `json:"latest_command_id"`
	LatestTargetID        string                                          `json:"latest_target_id"`
	LatestDispositionKind string                                          `json:"latest_disposition_kind"`
	LatestObservedAtMS    uint64                                          `json:"latest_observed_at_ms"`
}

type forgeSessionRunnerReconciliationProjection struct {
	SchemaVersion          string                                              `json:"schema_version"`
	EvaluationMode         string                                              `json:"evaluation_mode"`
	Owner                  forgeSessionRunnerReconciliationProjectionOwner     `json:"owner"`
	ConversationID         string                                              `json:"conversation_id"`
	PromptID               string                                              `json:"prompt_id"`
	RunID                  string                                              `json:"run_id"`
	Source                 forgeSessionRunnerReconciliationProjectionSource    `json:"source"`
	LatestAttemptID        string                                              `json:"latest_attempt_id"`
	LatestCommandID        string                                              `json:"latest_command_id"`
	LatestTargetID         string                                              `json:"latest_target_id"`
	LatestDispositionKind  string                                              `json:"latest_disposition_kind"`
	LatestObservedAtMS     uint64                                              `json:"latest_observed_at_ms"`
	ReconciliationKind     string                                              `json:"reconciliation_kind"`
	ReconciliationReason   string                                              `json:"reconciliation_reason"`
	ReconciliationRequired bool                                                `json:"reconciliation_required"`
	ManualReviewRequired   bool                                                `json:"manual_review_required"`
	AutomaticRetry         bool                                                `json:"automatic_retry"`
	FollowUp               string                                              `json:"follow_up"`
	SelectedTargetID       *string                                             `json:"selected_target_id"`
	PreviewOnly            bool                                                `json:"preview_only"`
	Authority              forgeSessionRunnerReconciliationProjectionAuthority `json:"authority"`
}

func TestForgeSessionRunnerReconciliationProjection(t *testing.T) {
	projection, err := decodeForgeSessionRunnerReconciliationProjection(forgeSessionRunnerReconciliationProjectionFixture)
	if err != nil {
		t.Fatalf("decode session Runner reconciliation projection: %v", err)
	}
	if err := validateForgeSessionRunnerReconciliationProjection(projection); err != nil {
		t.Fatalf("invalid session Runner reconciliation projection: %v", err)
	}
}

func TestForgeSessionRunnerReconciliationProjectionRejectsWireAndAuthorityDrift(t *testing.T) {
	base := bytes.TrimSpace(forgeSessionRunnerReconciliationProjectionFixture)
	mutations := map[string][]byte{
		"unknown":   append(bytes.TrimSuffix(base, []byte("}")), []byte(`,"unexpected":true}`)...),
		"duplicate": append(bytes.TrimSuffix(base, []byte("}")), []byte(`,"schema_version":"forge.session-runner-reconciliation-projection/v1"}`)...),
		"trailing":  append(base, []byte(" {}")...),
	}
	for name, mutation := range mutations {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeForgeSessionRunnerReconciliationProjection(mutation); err == nil {
				t.Fatal("wire drift was accepted")
			}
		})
	}

	baseProjection, err := decodeForgeSessionRunnerReconciliationProjection(base)
	if err != nil {
		t.Fatal(err)
	}
	lifecycleMutations := map[string]func(*forgeSessionRunnerReconciliationProjection){
		"owner_drift": func(value *forgeSessionRunnerReconciliationProjection) {
			value.Owner.Subject = "user-foreign"
		},
		"conversation_drift": func(value *forgeSessionRunnerReconciliationProjection) {
			value.ConversationID = "conversation-foreign"
		},
		"prompt_drift": func(value *forgeSessionRunnerReconciliationProjection) {
			value.PromptID = "prompt-foreign"
		},
		"run_drift": func(value *forgeSessionRunnerReconciliationProjection) {
			value.RunID = "run-foreign"
		},
		"source_schema_drift": func(value *forgeSessionRunnerReconciliationProjection) {
			value.Source.SchemaVersion = "forge.other-history/v1"
		},
		"source_attempt_count_drift": func(value *forgeSessionRunnerReconciliationProjection) {
			value.Source.AttemptCount = 0
		},
		"latest_attempt_drift": func(value *forgeSessionRunnerReconciliationProjection) {
			value.LatestAttemptID = "attempt-foreign"
		},
		"latest_command_drift": func(value *forgeSessionRunnerReconciliationProjection) {
			value.LatestCommandID = "command-foreign"
		},
		"latest_target_drift": func(value *forgeSessionRunnerReconciliationProjection) {
			value.LatestTargetID = "runner-foreign"
		},
		"latest_disposition_drift": func(value *forgeSessionRunnerReconciliationProjection) {
			value.LatestDispositionKind = "failed"
		},
		"latest_time_drift": func(value *forgeSessionRunnerReconciliationProjection) {
			value.LatestObservedAtMS = 100
		},
		"kind_drift": func(value *forgeSessionRunnerReconciliationProjection) {
			value.ReconciliationKind = "automatic"
		},
		"reason_drift": func(value *forgeSessionRunnerReconciliationProjection) {
			value.ReconciliationReason = "retry"
		},
		"reconciliation_flag_drift": func(value *forgeSessionRunnerReconciliationProjection) {
			value.ReconciliationRequired = false
		},
		"manual_review_flag_drift": func(value *forgeSessionRunnerReconciliationProjection) {
			value.ManualReviewRequired = false
		},
		"automatic_retry": func(value *forgeSessionRunnerReconciliationProjection) {
			value.AutomaticRetry = true
		},
		"follow_up_drift": func(value *forgeSessionRunnerReconciliationProjection) {
			value.FollowUp = "none"
		},
		"selected_target": func(value *forgeSessionRunnerReconciliationProjection) {
			value.SelectedTargetID = forgeSessionRunnerReconciliationProjectionStringPtr("runner-2")
		},
		"authority": func(value *forgeSessionRunnerReconciliationProjection) {
			value.Authority.IdentityVerified = true
		},
	}
	for name, mutate := range lifecycleMutations {
		t.Run(name, func(t *testing.T) {
			mutated := baseProjection
			mutate(&mutated)
			if err := validateForgeSessionRunnerReconciliationProjection(mutated); err == nil {
				t.Fatal("projection drift was accepted")
			}
		})
	}
}

func decodeForgeSessionRunnerReconciliationProjection(raw []byte) (forgeSessionRunnerReconciliationProjection, error) {
	if err := rejectDuplicateForgeSessionRunnerReconciliationProjectionKeys(raw); err != nil {
		return forgeSessionRunnerReconciliationProjection{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var projection forgeSessionRunnerReconciliationProjection
	if err := decoder.Decode(&projection); err != nil {
		return forgeSessionRunnerReconciliationProjection{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeSessionRunnerReconciliationProjection{}, fmt.Errorf("trailing JSON")
		}
		return forgeSessionRunnerReconciliationProjection{}, err
	}
	return projection, nil
}

func validateForgeSessionRunnerReconciliationProjection(projection forgeSessionRunnerReconciliationProjection) error {
	if projection.SchemaVersion != "forge.session-runner-reconciliation-projection/v1" ||
		projection.EvaluationMode != "pure_session_runner_reconciliation_projection_only" ||
		!validForgeSessionRunnerReconciliationProjectionOwner(projection.Owner) ||
		!validForgeSessionRunnerReconciliationProjectionIdentifier(projection.ConversationID) ||
		!validForgeSessionRunnerReconciliationProjectionIdentifier(projection.PromptID) ||
		!validForgeSessionRunnerReconciliationProjectionIdentifier(projection.RunID) ||
		projection.Source.SchemaVersion != "forge.session-runner-receipt-history/v1" ||
		!validForgeSessionRunnerReconciliationProjectionOwner(projection.Source.Owner) ||
		projection.Source.Owner != projection.Owner ||
		projection.Source.ConversationID != projection.ConversationID ||
		projection.Source.PromptID != projection.PromptID ||
		projection.Source.RunID != projection.RunID ||
		projection.Source.AttemptCount == 0 ||
		projection.Source.AttemptCount > forgeSessionRunnerReconciliationProjectionMaxAttempts ||
		!validForgeSessionRunnerReconciliationProjectionIdentifier(projection.LatestAttemptID) ||
		!validForgeSessionRunnerReconciliationProjectionIdentifier(projection.LatestCommandID) ||
		!validForgeSessionRunnerReconciliationProjectionIdentifier(projection.LatestTargetID) ||
		projection.LatestDispositionKind != "uncertain" ||
		projection.LatestObservedAtMS > forgeSessionRunnerReconciliationProjectionMaxSafeInteger ||
		projection.Source.LatestAttemptID != projection.LatestAttemptID ||
		projection.Source.LatestCommandID != projection.LatestCommandID ||
		projection.Source.LatestTargetID != projection.LatestTargetID ||
		projection.Source.LatestDispositionKind != projection.LatestDispositionKind ||
		projection.Source.LatestObservedAtMS != projection.LatestObservedAtMS ||
		projection.ReconciliationKind != "manual" ||
		projection.ReconciliationReason != "uncertain_terminal_receipt" ||
		!projection.ReconciliationRequired ||
		!projection.ManualReviewRequired ||
		projection.AutomaticRetry ||
		projection.FollowUp != "reconciliation_manual" ||
		projection.SelectedTargetID != nil ||
		!projection.PreviewOnly ||
		projection.Authority != (forgeSessionRunnerReconciliationProjectionAuthority{}) {
		return fmt.Errorf("invalid manual reconciliation projection")
	}
	return nil
}

func validForgeSessionRunnerReconciliationProjectionOwner(owner forgeSessionRunnerReconciliationProjectionOwner) bool {
	return validForgeSessionRunnerReconciliationProjectionOwnerPart(owner.Issuer) &&
		validForgeSessionRunnerReconciliationProjectionOwnerPart(owner.Subject) &&
		validForgeSessionRunnerReconciliationProjectionOwnerPart(owner.TenantID)
}

func validForgeSessionRunnerReconciliationProjectionOwnerPart(value string) bool {
	return value != "" && len(value) <= 512 && strings.TrimSpace(value) == value &&
		!strings.ContainsAny(value, "\x00\r\n")
}

func validForgeSessionRunnerReconciliationProjectionIdentifier(value string) bool {
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

func forgeSessionRunnerReconciliationProjectionStringPtr(value string) *string { return &value }

func rejectDuplicateForgeSessionRunnerReconciliationProjectionKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanForgeSessionRunnerReconciliationProjectionJSON(decoder); err != nil {
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

func scanForgeSessionRunnerReconciliationProjectionJSON(decoder *json.Decoder) error {
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
			if err := scanForgeSessionRunnerReconciliationProjectionJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := scanForgeSessionRunnerReconciliationProjectionJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return fmt.Errorf("invalid JSON delimiter")
	}
}
