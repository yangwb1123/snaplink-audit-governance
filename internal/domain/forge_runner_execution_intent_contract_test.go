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

// Aero-ID receives the canonical Runner execution intent as a bounded,
// display-only binding. It does not authenticate a device, issue a lease,
// dispatch a Runner, execute argv, or grant execution authority.
//
//go:embed testdata/forge-runner-execution-intent-v1.json
var forgeRunnerExecutionIntentFixture []byte

const forgeRunnerExecutionIntentCommandSHA256 = "42ed02a535113450e6f2cc757fb9b4e2cce6143724274191bbae159e9ea8de7a"

type forgeRunnerExecutionIntentOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeRunnerExecutionIntentAuthority struct {
	DeviceIdentityVerified bool `json:"device_identity_verified"`
	CommandPersisted       bool `json:"command_persisted"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
	AuditPublished         bool `json:"audit_published"`
}

func (value *forgeRunnerExecutionIntentAuthority) UnmarshalJSON(raw []byte) error {
	type alias forgeRunnerExecutionIntentAuthority
	var decoded alias
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for _, key := range []string{
		"device_identity_verified", "command_persisted", "reservation_created",
		"execution_authorized", "dispatch_performed", "audit_published",
	} {
		if _, ok := fields[key]; !ok {
			return fmt.Errorf("missing authority field %q", key)
		}
	}
	if len(fields) != 6 {
		return fmt.Errorf("unknown authority field")
	}
	*value = forgeRunnerExecutionIntentAuthority(decoded)
	return nil
}

type forgeRunnerExecutionIntentPromptReceipt struct {
	PromptID             string `json:"prompt_id"`
	ConversationID       string `json:"conversation_id"`
	Role                 string `json:"role"`
	AcceptedAtMS         uint64 `json:"accepted_at_ms"`
	IntentID             string `json:"intent_id"`
	InitialEventID       string `json:"initial_event_id"`
	InitialEventSequence uint64 `json:"initial_event_sequence"`
	InitialEventType     string `json:"initial_event_type"`
	Replayed             bool   `json:"replayed"`
}

type forgeRunnerExecutionIntentRunReference struct {
	RunID          string `json:"run_id"`
	ConversationID string `json:"conversation_id"`
	PromptID       string `json:"prompt_id"`
	CreatedAtMS    uint64 `json:"created_at_ms"`
	LatestSequence uint64 `json:"latest_sequence"`
	Status         string `json:"status"`
}

type forgeRunnerExecutionIntentBinding struct {
	ConversationID   string  `json:"conversation_id"`
	PromptID         string  `json:"prompt_id"`
	RunID            string  `json:"run_id"`
	AttemptID        string  `json:"attempt_id"`
	CommandID        string  `json:"command_id"`
	TargetID         string  `json:"target_id"`
	CommandSHA256    string  `json:"command_sha256"`
	IdempotencyKey   string  `json:"idempotency_key"`
	SelectedTargetID *string `json:"selected_target_id"`
}

type forgeRunnerExecutionIntentLeaseProof struct {
	AttemptID    string `json:"attempt_id"`
	TargetID     string `json:"target_id"`
	Epoch        uint64 `json:"epoch"`
	FencingToken string `json:"fencing_token"`
}

type forgeRunnerExecutionIntentCommand struct {
	V              uint64                               `json:"v"`
	CommandID      string                               `json:"command_id"`
	LeaseProof     forgeRunnerExecutionIntentLeaseProof `json:"lease_proof"`
	IdempotencyKey string                               `json:"idempotency_key"`
	WorkspaceRef   string                               `json:"workspace_ref"`
	Argv           []string                             `json:"argv"`
	TimeoutMS      uint64                               `json:"timeout_ms"`
	MaxOutputBytes uint64                               `json:"max_output_bytes"`
}

type forgeRunnerExecutionIntentExpected struct {
	PromptRunBindingValid     bool                                `json:"prompt_run_binding_valid"`
	RunnerCommandBindingValid bool                                `json:"runner_command_binding_valid"`
	PreviewOnly               bool                                `json:"preview_only"`
	CommandSHA256             string                              `json:"command_sha256"`
	SelectedTargetID          *string                             `json:"selected_target_id"`
	Authority                 forgeRunnerExecutionIntentAuthority `json:"authority"`
}

type forgeRunnerExecutionIntentObservation struct {
	SchemaVersion   string                                  `json:"schema_version"`
	EvaluationMode  string                                  `json:"evaluation_mode"`
	Authority       forgeRunnerExecutionIntentAuthority     `json:"authority"`
	Owner           forgeRunnerExecutionIntentOwner         `json:"owner"`
	ConversationID  string                                  `json:"conversation_id"`
	PromptReceipt   forgeRunnerExecutionIntentPromptReceipt `json:"prompt_receipt"`
	RunReference    forgeRunnerExecutionIntentRunReference  `json:"run_reference"`
	ExecutionIntent forgeRunnerExecutionIntentBinding       `json:"execution_intent"`
	Command         forgeRunnerExecutionIntentCommand       `json:"command"`
	Expected        forgeRunnerExecutionIntentExpected      `json:"expected"`
}

func decodeForgeRunnerExecutionIntent(raw []byte) (forgeRunnerExecutionIntentObservation, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeRunnerExecutionIntentObservation{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value forgeRunnerExecutionIntentObservation
	if err := decoder.Decode(&value); err != nil {
		return value, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return value, fmt.Errorf("trailing JSON")
		}
		return value, err
	}
	if err := value.validate(); err != nil {
		return value, err
	}
	return value, nil
}

func (value forgeRunnerExecutionIntentObservation) validate() error {
	if value.SchemaVersion != "forge.runner-execution-intent/v1" ||
		value.EvaluationMode != "pure_runner_binding_only" ||
		value.Authority != (forgeRunnerExecutionIntentAuthority{}) ||
		!validForgeRunnerExecutionIntentOwner(value.Owner) ||
		value.ConversationID != "conversation-001" {
		return fmt.Errorf("invalid Runner execution intent envelope")
	}
	prompt := value.PromptReceipt
	if prompt.PromptID != "prompt-001" || prompt.ConversationID != value.ConversationID ||
		prompt.Role != "user" || prompt.AcceptedAtMS != 200 || prompt.IntentID != "intent-001" ||
		prompt.InitialEventID != "event-001" || prompt.InitialEventSequence != 1 ||
		prompt.InitialEventType != "submitted" || prompt.Replayed {
		return fmt.Errorf("invalid prompt receipt binding")
	}
	run := value.RunReference
	if run.RunID != "run-001" || run.ConversationID != value.ConversationID ||
		run.PromptID != prompt.PromptID || run.CreatedAtMS != 200 || run.LatestSequence != 5 ||
		run.Status != "nonterminal" {
		return fmt.Errorf("invalid run reference binding")
	}
	intent := value.ExecutionIntent
	if intent.ConversationID != value.ConversationID || intent.PromptID != prompt.PromptID ||
		intent.RunID != run.RunID || intent.AttemptID != "attempt-001" ||
		intent.CommandID != "command-001" || intent.TargetID != "runner-1" ||
		intent.CommandSHA256 != forgeRunnerExecutionIntentCommandSHA256 ||
		!validForgeRunnerExecutionIntentDigest(intent.CommandSHA256) ||
		intent.IdempotencyKey != "run-001:attempt-001:command-001" || intent.SelectedTargetID != nil {
		return fmt.Errorf("invalid execution intent binding")
	}
	command := value.Command
	if command.V != 1 || command.CommandID != intent.CommandID ||
		command.IdempotencyKey != intent.IdempotencyKey || command.WorkspaceRef != "workspace-001" ||
		len(command.Argv) != 3 || command.Argv[0] != "forge-task" ||
		command.Argv[1] != "--prompt-ref" || command.Argv[2] != intent.PromptID ||
		command.TimeoutMS != 5000 || command.MaxOutputBytes != 65536 ||
		command.LeaseProof.AttemptID != intent.AttemptID || command.LeaseProof.TargetID != intent.TargetID ||
		command.LeaseProof.Epoch != 1 || command.LeaseProof.FencingToken != "fence-001" {
		return fmt.Errorf("invalid command binding")
	}
	if expected := value.Expected; !expected.PromptRunBindingValid || !expected.RunnerCommandBindingValid ||
		!expected.PreviewOnly || expected.CommandSHA256 != intent.CommandSHA256 ||
		expected.SelectedTargetID != nil || expected.Authority != (forgeRunnerExecutionIntentAuthority{}) {
		return fmt.Errorf("invalid expected metadata")
	}
	return nil
}

func validForgeRunnerExecutionIntentOwner(owner forgeRunnerExecutionIntentOwner) bool {
	for _, value := range []string{owner.Issuer, owner.Subject, owner.TenantID} {
		if value == "" || len(value) > 512 || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n") {
			return false
		}
	}
	return true
}

func validForgeRunnerExecutionIntentDigest(value string) bool {
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

func TestForgeRunnerExecutionIntentCanonicalObservation(t *testing.T) {
	value, err := decodeForgeRunnerExecutionIntent(forgeRunnerExecutionIntentFixture)
	if err != nil {
		t.Fatal(err)
	}
	if value.Owner.Subject != "user-1" || value.ExecutionIntent.RunID != "run-001" ||
		value.Command.LeaseProof.TargetID != "runner-1" {
		t.Fatalf("unexpected execution intent observation: %#v", value)
	}
	if value.Command.LeaseProof.TargetID != value.ExecutionIntent.TargetID || !value.Expected.PreviewOnly {
		t.Fatalf("execution intent is not canonical metadata: %#v", value)
	}
}

func TestForgeRunnerExecutionIntentRejectsUnknownDuplicateBindingSelectionAndAuthorityMutations(t *testing.T) {
	base := string(forgeRunnerExecutionIntentFixture)
	mutations := map[string]string{
		"unknown":   strings.TrimSuffix(strings.TrimSpace(base), "}") + `,"unexpected":true}`,
		"duplicate": strings.TrimSuffix(strings.TrimSpace(base), "}") + `,"schema_version":"forge.runner-execution-intent/v1"}`,
		"owner":     strings.Replace(base, `"subject": "user-1"`, `"subject": ""`, 1),
		"run_reference_id": strings.Replace(base,
			"\"run_reference\": {\n    \"run_id\": \"run-001\",\n    \"conversation_id\": \"conversation-001\",\n    \"prompt_id\": \"prompt-001\"",
			"\"run_reference\": {\n    \"run_id\": \"run-foreign\",\n    \"conversation_id\": \"conversation-001\",\n    \"prompt_id\": \"prompt-001\"", 1),
		"run_reference_prompt": strings.Replace(base,
			"\"run_reference\": {\n    \"run_id\": \"run-001\",\n    \"conversation_id\": \"conversation-001\",\n    \"prompt_id\": \"prompt-001\"",
			"\"run_reference\": {\n    \"run_id\": \"run-001\",\n    \"conversation_id\": \"conversation-001\",\n    \"prompt_id\": \"prompt-foreign\"", 1),
		"binding":           strings.Replace(base, `"command_id": "command-001"`, `"command_id": "command-foreign"`, 1),
		"digest":            strings.ReplaceAll(base, forgeRunnerExecutionIntentCommandSHA256, strings.Repeat("0", 64)),
		"selection":         strings.Replace(base, `"selected_target_id": null`, `"selected_target_id": "runner-1"`, 1),
		"authority":         strings.Replace(base, `"execution_authorized": false`, `"execution_authorized": true`, 1),
		"missing_authority": strings.Replace(base, "    \"audit_published\": false\n", "", 1),
	}
	for name, raw := range mutations {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeForgeRunnerExecutionIntent([]byte(raw)); err == nil {
				t.Fatalf("invalid %s mutation accepted", name)
			}
		})
	}
}
