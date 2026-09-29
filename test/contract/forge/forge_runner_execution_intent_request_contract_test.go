package forge

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

// Aero-ID receives the canonical Runner execution-intent request only as a
// strict, authority-free handoff. It does not authenticate a device, issue a
// lease, dispatch a Runner, execute argv, or grant execution authority.
//
//go:embed testdata/forge-runner-execution-intent-request-v1.json
var forgeRunnerExecutionIntentRequestFixture []byte

const forgeRunnerExecutionIntentRequestCommandSHA256 = "42ed02a535113450e6f2cc757fb9b4e2cce6143724274191bbae159e9ea8de7a"

type forgeRunnerExecutionIntentRequestOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeRunnerExecutionIntentRequestPrompt struct {
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

type forgeRunnerExecutionIntentRequestRun struct {
	RunID          string `json:"run_id"`
	ConversationID string `json:"conversation_id"`
	PromptID       string `json:"prompt_id"`
	CreatedAtMS    uint64 `json:"created_at_ms"`
	LatestSequence uint64 `json:"latest_sequence"`
	Status         string `json:"status"`
}

type forgeRunnerExecutionIntentRequestBinding struct {
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

type forgeRunnerExecutionIntentRequestLeaseProof struct {
	AttemptID    string `json:"attempt_id"`
	TargetID     string `json:"target_id"`
	Epoch        uint64 `json:"epoch"`
	FencingToken string `json:"fencing_token"`
}

type forgeRunnerExecutionIntentRequestCommand struct {
	V              uint64                                      `json:"v"`
	CommandID      string                                      `json:"command_id"`
	LeaseProof     forgeRunnerExecutionIntentRequestLeaseProof `json:"lease_proof"`
	IdempotencyKey string                                      `json:"idempotency_key"`
	WorkspaceRef   string                                      `json:"workspace_ref"`
	Argv           []string                                    `json:"argv"`
	TimeoutMS      uint64                                      `json:"timeout_ms"`
	MaxOutputBytes uint64                                      `json:"max_output_bytes"`
}

type forgeRunnerExecutionIntentRequest struct {
	Owner          forgeRunnerExecutionIntentRequestOwner   `json:"owner"`
	ConversationID string                                   `json:"conversation_id"`
	Prompt         forgeRunnerExecutionIntentRequestPrompt  `json:"prompt_receipt"`
	Run            forgeRunnerExecutionIntentRequestRun     `json:"run_reference"`
	Binding        forgeRunnerExecutionIntentRequestBinding `json:"execution_intent"`
	Command        forgeRunnerExecutionIntentRequestCommand `json:"command"`
}

func decodeForgeRunnerExecutionIntentRequest(raw []byte) (forgeRunnerExecutionIntentRequest, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeRunnerExecutionIntentRequest{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value forgeRunnerExecutionIntentRequest
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

func (value forgeRunnerExecutionIntentRequest) validate() error {
	if value.Owner.Issuer != "https://id.example" || value.Owner.Subject != "user-1" ||
		value.Owner.TenantID != "tenant-1" || value.ConversationID != "conversation-001" {
		return fmt.Errorf("invalid owner or conversation binding")
	}
	prompt := value.Prompt
	if prompt.PromptID != "prompt-001" || prompt.ConversationID != value.ConversationID ||
		prompt.Role != "user" || prompt.AcceptedAtMS != 200 || prompt.IntentID != "intent-001" ||
		prompt.InitialEventID != "event-001" || prompt.InitialEventSequence != 1 ||
		prompt.InitialEventType != "submitted" || prompt.Replayed {
		return fmt.Errorf("invalid prompt receipt binding")
	}
	run := value.Run
	if run.RunID != "run-001" || run.ConversationID != value.ConversationID ||
		run.PromptID != prompt.PromptID || run.CreatedAtMS != 200 || run.LatestSequence != 5 ||
		run.Status != "nonterminal" {
		return fmt.Errorf("invalid run reference binding")
	}
	binding := value.Binding
	if binding.ConversationID != value.ConversationID || binding.PromptID != prompt.PromptID ||
		binding.RunID != run.RunID || binding.AttemptID != "attempt-001" ||
		binding.CommandID != "command-001" || binding.TargetID != "runner-1" ||
		binding.CommandSHA256 != forgeRunnerExecutionIntentRequestCommandSHA256 ||
		binding.IdempotencyKey != "run-001:attempt-001:command-001" || binding.SelectedTargetID != nil {
		return fmt.Errorf("invalid execution intent binding")
	}
	command := value.Command
	if command.V != 1 || command.CommandID != binding.CommandID ||
		command.IdempotencyKey != binding.IdempotencyKey || command.WorkspaceRef != "workspace-001" ||
		len(command.Argv) != 3 || command.Argv[0] != "forge-task" ||
		command.Argv[1] != "--prompt-ref" || command.Argv[2] != prompt.PromptID ||
		command.TimeoutMS != 5000 || command.MaxOutputBytes != 65536 ||
		command.LeaseProof.AttemptID != binding.AttemptID || command.LeaseProof.TargetID != binding.TargetID ||
		command.LeaseProof.Epoch != 1 || command.LeaseProof.FencingToken != "fence-001" {
		return fmt.Errorf("invalid command binding")
	}
	digest, err := forgeRunnerExecutionIntentRequestCommandDigest(command)
	if err != nil || digest != binding.CommandSHA256 {
		return fmt.Errorf("invalid command digest")
	}
	return nil
}

func forgeRunnerExecutionIntentRequestCommandDigest(command forgeRunnerExecutionIntentRequestCommand) (string, error) {
	encoded, err := json.Marshal(command)
	if err != nil {
		return "", err
	}
	preimage := append([]byte("forge.runtime.runner-command.v1\x00"), encoded...)
	digest := sha256.Sum256(preimage)
	return hex.EncodeToString(digest[:]), nil
}

func TestForgeRunnerExecutionIntentRequestCanonical(t *testing.T) {
	value, err := decodeForgeRunnerExecutionIntentRequest(forgeRunnerExecutionIntentRequestFixture)
	if err != nil {
		t.Fatal(err)
	}
	if value.Owner.Subject != "user-1" || value.Binding.RunID != "run-001" ||
		value.Command.LeaseProof.TargetID != "runner-1" {
		t.Fatalf("unexpected execution-intent request: %#v", value)
	}
	digest, err := forgeRunnerExecutionIntentRequestCommandDigest(value.Command)
	if err != nil || digest != forgeRunnerExecutionIntentRequestCommandSHA256 {
		t.Fatalf("unexpected command digest: %q (%v)", digest, err)
	}
}

func TestForgeRunnerExecutionIntentRequestRejectsUnknownDuplicateAndBindingMutations(t *testing.T) {
	base := string(forgeRunnerExecutionIntentRequestFixture)
	mutations := map[string]string{
		"unknown":   strings.TrimSuffix(strings.TrimSpace(base), "}") + ",\"unexpected\":true}",
		"duplicate": strings.TrimSuffix(strings.TrimSpace(base), "}") + ",\"owner\":{}",
		"owner":     strings.Replace(base, "\"subject\": \"user-1\"", "\"subject\": \"\"", 1),
		"run_reference_id": strings.Replace(base,
			"\"run_id\": \"run-001\", \"conversation_id\": \"conversation-001\", \"prompt_id\": \"prompt-001\"",
			"\"run_id\": \"run-foreign\", \"conversation_id\": \"conversation-001\", \"prompt_id\": \"prompt-001\"", 1),
		"run_reference_prompt": strings.Replace(base,
			"\"run_id\": \"run-001\", \"conversation_id\": \"conversation-001\", \"prompt_id\": \"prompt-001\"",
			"\"run_id\": \"run-001\", \"conversation_id\": \"conversation-001\", \"prompt_id\": \"prompt-foreign\"", 1),
		"binding":   strings.Replace(base, "\"command_id\": \"command-001\"", "\"command_id\": \"command-foreign\"", 1),
		"digest":    strings.ReplaceAll(base, forgeRunnerExecutionIntentRequestCommandSHA256, strings.Repeat("0", 64)),
		"selection": strings.Replace(base, "\"selected_target_id\": null", "\"selected_target_id\": \"runner-1\"", 1),
	}
	for name, raw := range mutations {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeForgeRunnerExecutionIntentRequest([]byte(raw)); err == nil {
				t.Fatalf("invalid %s mutation accepted", name)
			}
		})
	}
}
