package forge

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// This receiver-side contract is a read-only restart observation. It does not
// authenticate a caller, persist a Run/Attempt/receipt, issue a lease, select
// a target, schedule/dispatch work, execute a process, or publish Audit.
//
//go:embed testdata/forge-execution-reconciliation-observation-v1.json
var forgeExecutionReconciliationObservationFixture []byte

type forgeExecutionReconciliationOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeExecutionReconciliationAuthority struct {
	IdentityVerified    bool `json:"identity_verified"`
	RunAuthoritative    bool `json:"run_authoritative"`
	AttemptPersisted    bool `json:"attempt_persisted"`
	LeaseIssued         bool `json:"lease_issued"`
	TerminalPersisted   bool `json:"terminal_persisted"`
	ReservationCreated  bool `json:"reservation_created"`
	ExecutionAuthorized bool `json:"execution_authorized"`
	DispatchPerformed   bool `json:"dispatch_performed"`
	AuditPublished      bool `json:"audit_published"`
}

type forgeExecutionReconciliationLease struct {
	Version      int    `json:"v"`
	AttemptID    string `json:"attempt_id"`
	TargetID     string `json:"target_id"`
	Epoch        uint64 `json:"epoch"`
	FencingToken string `json:"fencing_token"`
	IssuedAtMS   int64  `json:"issued_at_ms"`
	ExpiresAtMS  int64  `json:"expires_at_ms"`
}

type forgeExecutionReconciliationProof struct {
	AttemptID    string `json:"attempt_id"`
	TargetID     string `json:"target_id"`
	Epoch        uint64 `json:"epoch"`
	FencingToken string `json:"fencing_token"`
}

type forgeExecutionReconciliationDisposition struct {
	Kind          string `json:"kind"`
	ReceiptSHA256 string `json:"receipt_sha256,omitempty"`
	Reason        string `json:"reason,omitempty"`
}

type forgeExecutionReconciliationTerminal struct {
	Version      int                                     `json:"v"`
	Proof        forgeExecutionReconciliationProof       `json:"proof"`
	Disposition  forgeExecutionReconciliationDisposition `json:"disposition"`
	ObservedAtMS int64                                   `json:"observed_at_ms"`
}

type forgeExecutionReconciliationInput struct {
	Owner          forgeExecutionReconciliationOwner     `json:"owner"`
	ConversationID string                                `json:"conversation_id"`
	RunID          string                                `json:"run_id"`
	AttemptID      string                                `json:"attempt_id"`
	CommandID      string                                `json:"command_id"`
	TargetID       string                                `json:"target_id"`
	RunStatus      string                                `json:"run_status"`
	AttemptState   string                                `json:"attempt_state"`
	Lease          forgeExecutionReconciliationLease     `json:"lease"`
	ObservedAtMS   int64                                 `json:"observed_at_ms"`
	Terminal       *forgeExecutionReconciliationTerminal `json:"terminal"`
}

type forgeExecutionReconciliationExpected struct {
	Accepted               bool   `json:"accepted"`
	NextObservation        string `json:"next_observation"`
	LeaseActive            bool   `json:"lease_active"`
	TerminalObserved       bool   `json:"terminal_observed"`
	TerminalDisposition    string `json:"terminal_disposition"`
	TerminalStateAligned   bool   `json:"terminal_state_aligned"`
	ReconciliationRequired bool   `json:"reconciliation_required"`
}

type forgeExecutionReconciliationCase struct {
	Name     string                               `json:"name"`
	Input    forgeExecutionReconciliationInput    `json:"input"`
	Expected forgeExecutionReconciliationExpected `json:"expected"`
}

type forgeExecutionReconciliationFixture struct {
	SchemaVersion  string                                `json:"schema_version"`
	EvaluationMode string                                `json:"evaluation_mode"`
	Authority      forgeExecutionReconciliationAuthority `json:"authority"`
	Cases          []forgeExecutionReconciliationCase    `json:"cases"`
}

func decodeForgeExecutionReconciliationObservation(raw []byte) (forgeExecutionReconciliationFixture, error) {
	if err := rejectDuplicateForgeExecutionReconciliationJSONKeys(raw); err != nil {
		return forgeExecutionReconciliationFixture{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var fixture forgeExecutionReconciliationFixture
	if err := decoder.Decode(&fixture); err != nil {
		return forgeExecutionReconciliationFixture{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return forgeExecutionReconciliationFixture{}, errors.New("trailing JSON value")
		}
		return forgeExecutionReconciliationFixture{}, err
	}
	return fixture, nil
}

func rejectDuplicateForgeExecutionReconciliationJSONKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanForgeExecutionReconciliationJSONValue(decoder); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func scanForgeExecutionReconciliationJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		keys := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not string")
			}
			if _, exists := keys[key]; exists {
				return fmt.Errorf("duplicate JSON object key %q", key)
			}
			keys[key] = struct{}{}
			if err := scanForgeExecutionReconciliationJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return errors.New("malformed JSON object")
		}
	case '[':
		for decoder.More() {
			if err := scanForgeExecutionReconciliationJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return errors.New("malformed JSON array")
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	return nil
}

func validateForgeExecutionReconciliationFixture(fixture forgeExecutionReconciliationFixture) error {
	if fixture.SchemaVersion != "forge.execution-reconciliation-observation/v1" ||
		fixture.EvaluationMode != "pure_execution_reconciliation_observation" ||
		fixture.Authority != (forgeExecutionReconciliationAuthority{}) || len(fixture.Cases) != 6 {
		return errors.New("invalid execution reconciliation envelope")
	}
	names := map[string]struct{}{}
	for _, testCase := range fixture.Cases {
		if testCase.Name == "" {
			return errors.New("empty execution reconciliation case name")
		}
		if _, exists := names[testCase.Name]; exists {
			return fmt.Errorf("duplicate execution reconciliation case %q", testCase.Name)
		}
		names[testCase.Name] = struct{}{}
		actual, err := observeForgeExecutionReconciliation(testCase.Input)
		if !testCase.Expected.Accepted {
			if err == nil {
				return fmt.Errorf("case %s accepted unexpectedly", testCase.Name)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("case %s: %w", testCase.Name, err)
		}
		if actual != testCase.Expected {
			return fmt.Errorf("case %s actual=%#v expected=%#v", testCase.Name, actual, testCase.Expected)
		}
	}
	return nil
}

func observeForgeExecutionReconciliation(input forgeExecutionReconciliationInput) (forgeExecutionReconciliationExpected, error) {
	if input.Owner.Issuer == "" || input.Owner.Subject == "" || input.Owner.TenantID == "" ||
		input.ConversationID == "" || input.RunID == "" || input.AttemptID == "" || input.CommandID == "" || input.TargetID == "" ||
		input.RunStatus != "nonterminal" || input.Lease.Version != 1 || input.Lease.AttemptID != input.AttemptID ||
		input.Lease.TargetID != input.TargetID || input.Lease.Epoch == 0 || input.Lease.FencingToken == "" ||
		input.Lease.IssuedAtMS <= 0 || input.Lease.ExpiresAtMS <= input.Lease.IssuedAtMS || input.ObservedAtMS < input.Lease.IssuedAtMS {
		return forgeExecutionReconciliationExpected{}, errors.New("invalid execution reconciliation bindings")
	}
	if input.AttemptState != "running" && input.AttemptState != "completed" && input.AttemptState != "failed" && input.AttemptState != "uncertain" {
		return forgeExecutionReconciliationExpected{}, errors.New("invalid attempt state")
	}
	active := input.ObservedAtMS < input.Lease.ExpiresAtMS
	actual := forgeExecutionReconciliationExpected{
		Accepted:             true,
		LeaseActive:          active,
		TerminalDisposition:  "none",
		TerminalStateAligned: true,
	}
	if input.Terminal != nil {
		terminal := input.Terminal
		if terminal.Version != 1 || terminal.ObservedAtMS < input.Lease.IssuedAtMS || terminal.ObservedAtMS > input.ObservedAtMS || terminal.ObservedAtMS >= input.Lease.ExpiresAtMS ||
			terminal.Proof.AttemptID != input.AttemptID || terminal.Proof.TargetID != input.TargetID || terminal.Proof.Epoch != input.Lease.Epoch || terminal.Proof.FencingToken != input.Lease.FencingToken {
			return forgeExecutionReconciliationExpected{}, errors.New("terminal proof or time binding mismatch")
		}
		if terminal.Disposition.Kind != "completed" && terminal.Disposition.Kind != "failed" && terminal.Disposition.Kind != "uncertain" {
			return forgeExecutionReconciliationExpected{}, errors.New("invalid terminal disposition")
		}
		actual.TerminalObserved = true
		actual.TerminalDisposition = terminal.Disposition.Kind
		switch terminal.Disposition.Kind {
		case "completed":
			actual.NextObservation = "terminal_completed"
			actual.TerminalStateAligned = input.AttemptState == "completed"
		case "failed":
			actual.NextObservation = "terminal_failed"
			actual.TerminalStateAligned = input.AttemptState == "failed"
		case "uncertain":
			actual.NextObservation = "terminal_uncertain"
			actual.TerminalStateAligned = input.AttemptState == "uncertain"
		}
		if !actual.TerminalStateAligned {
			actual.NextObservation = "terminal_state_conflict"
		}
		actual.ReconciliationRequired = !actual.TerminalStateAligned || actual.TerminalDisposition == "uncertain"
		return actual, nil
	}
	if input.AttemptState != "running" {
		actual.NextObservation = "attempt_terminal_without_receipt"
		actual.ReconciliationRequired = true
	} else if !active {
		actual.NextObservation = "lease_expired_without_terminal"
		actual.ReconciliationRequired = true
	} else {
		actual.NextObservation = "await_terminal"
	}
	return actual, nil
}

func TestForgeExecutionReconciliationObservationCanonicalFixture(t *testing.T) {
	fixture, err := decodeForgeExecutionReconciliationObservation(forgeExecutionReconciliationObservationFixture)
	if err != nil {
		t.Fatalf("decode execution reconciliation fixture: %v", err)
	}
	if err := validateForgeExecutionReconciliationFixture(fixture); err != nil {
		t.Fatal(err)
	}
	uncertain := fixture.Cases[3]
	if uncertain.Name != "uncertain_terminal_requires_manual_review" ||
		uncertain.Expected.NextObservation != "terminal_uncertain" || !uncertain.Expected.ReconciliationRequired {
		t.Fatalf("uncertain case lost manual-reconciliation signal: %#v", uncertain)
	}
}

func TestForgeExecutionReconciliationObservationRejectsUnknownDuplicateAuthorityAndProofMutation(t *testing.T) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(forgeExecutionReconciliationObservationFixture, &root); err != nil {
		t.Fatal(err)
	}
	root["unexpected"] = json.RawMessage(`true`)
	unknown, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeForgeExecutionReconciliationObservation(unknown); err == nil {
		t.Fatal("unknown field accepted")
	}

	duplicate := strings.TrimSpace(string(forgeExecutionReconciliationObservationFixture))
	duplicate = strings.TrimSuffix(duplicate, "}") + `,"schema_version":"forge.execution-reconciliation-observation/v1"}`
	if _, err := decodeForgeExecutionReconciliationObservation([]byte(duplicate)); err == nil {
		t.Fatal("duplicate field accepted")
	}

	var authority map[string]any
	if err := json.Unmarshal(forgeExecutionReconciliationObservationFixture, &authority); err != nil {
		t.Fatal(err)
	}
	authority["authority"].(map[string]any)["execution_authorized"] = true
	mutated, err := json.Marshal(authority)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeForgeExecutionReconciliationObservation(mutated)
	if err != nil {
		t.Fatalf("authority mutation should retain JSON shape: %v", err)
	}
	if err := validateForgeExecutionReconciliationFixture(decoded); err == nil {
		t.Fatal("authority mutation accepted")
	}

	var proof map[string]any
	if err := json.Unmarshal(forgeExecutionReconciliationObservationFixture, &proof); err != nil {
		t.Fatal(err)
	}
	proof["cases"].([]any)[2].(map[string]any)["input"].(map[string]any)["terminal"].(map[string]any)["proof"].(map[string]any)["target_id"] = "foreign-runner"
	mutated, err = json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err = decodeForgeExecutionReconciliationObservation(mutated)
	if err != nil {
		t.Fatalf("proof mutation should retain JSON shape: %v", err)
	}
	if err := validateForgeExecutionReconciliationFixture(decoded); err == nil {
		t.Fatal("terminal proof mutation accepted")
	}
}

func TestForgeExecutionReconciliationObservationRejectsAutomaticRetry(t *testing.T) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(forgeExecutionReconciliationObservationFixture, &root); err != nil {
		t.Fatal(err)
	}
	root["automatic_retry"] = json.RawMessage(`true`)
	mutated, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeForgeExecutionReconciliationObservation(mutated); err == nil {
		t.Fatal("automatic retry field accepted")
	}
	// The canonical receiver shape has no automatic retry field. Requiring the
	// exact strict shape makes an authority-bearing retry decision unrepresentable.
	if strings.Contains(string(forgeExecutionReconciliationObservationFixture), "automatic_retry") {
		t.Fatal("automatic retry leaked into the canonical receiver fixture")
	}
}
