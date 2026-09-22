package domain

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

// This receiver-side value contract carries only restart/fencing checkpoint
// evidence. It does not issue a lease, persist a terminal, authorize
// execution, dispatch work, or publish Audit.
//
//go:embed testdata/forge-execution-lease-checkpoint-v1.json
var forgeExecutionLeaseCheckpointFixture []byte

type forgeExecutionLeaseCheckpointFixtureValue struct {
	SchemaVersion  string                                     `json:"schema_version"`
	EvaluationMode string                                     `json:"evaluation_mode"`
	Authority      forgeExecutionLeaseCheckpointAuthority     `json:"authority"`
	Cases          []forgeExecutionLeaseCheckpointFixtureCase `json:"cases"`
}

type forgeExecutionLeaseCheckpointAuthority struct {
	LeaseIssued         bool `json:"lease_issued"`
	TerminalPersisted   bool `json:"terminal_persisted"`
	ExecutionAuthorized bool `json:"execution_authorized"`
	DispatchPerformed   bool `json:"dispatch_performed"`
	AuditPublished      bool `json:"audit_published"`
}

type forgeExecutionLeaseCheckpointFixtureCase struct {
	Name       string                                `json:"name"`
	Checkpoint forgeExecutionLeaseCheckpoint         `json:"checkpoint"`
	Expected   forgeExecutionLeaseCheckpointExpected `json:"expected"`
}

type forgeExecutionLeaseCheckpointExpected struct {
	Accepted  bool   `json:"accepted"`
	Error     string `json:"error"`
	Terminal  bool   `json:"terminal"`
	Uncertain bool   `json:"uncertain"`
}

type forgeExecutionLeaseCheckpoint struct {
	SchemaVersion  string                                `json:"schema_version"`
	EvaluationMode string                                `json:"evaluation_mode"`
	Grant          forgeExecutionLeaseCheckpointGrant    `json:"grant"`
	Terminal       *forgeExecutionLeaseCheckpointReceipt `json:"terminal"`
}

type forgeExecutionLeaseCheckpointGrant struct {
	V            uint16 `json:"v"`
	AttemptID    string `json:"attempt_id"`
	TargetID     string `json:"target_id"`
	Epoch        uint64 `json:"epoch"`
	FencingToken string `json:"fencing_token"`
	IssuedAtMS   uint64 `json:"issued_at_ms"`
	ExpiresAtMS  uint64 `json:"expires_at_ms"`
}

type forgeExecutionLeaseCheckpointProof struct {
	AttemptID    string `json:"attempt_id"`
	TargetID     string `json:"target_id"`
	Epoch        uint64 `json:"epoch"`
	FencingToken string `json:"fencing_token"`
}

type forgeExecutionLeaseCheckpointDisposition struct {
	Kind          string `json:"kind"`
	ReceiptSHA256 string `json:"receipt_sha256"`
	Reason        string `json:"reason"`
}

type forgeExecutionLeaseCheckpointReceipt struct {
	V            uint16                                   `json:"v"`
	Proof        forgeExecutionLeaseCheckpointProof       `json:"proof"`
	Disposition  forgeExecutionLeaseCheckpointDisposition `json:"disposition"`
	ObservedAtMS uint64                                   `json:"observed_at_ms"`
}

func decodeForgeExecutionLeaseCheckpoint(raw []byte) (forgeExecutionLeaseCheckpointFixtureValue, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeExecutionLeaseCheckpointFixtureValue{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value forgeExecutionLeaseCheckpointFixtureValue
	if err := decoder.Decode(&value); err != nil {
		return forgeExecutionLeaseCheckpointFixtureValue{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeExecutionLeaseCheckpointFixtureValue{}, io.ErrUnexpectedEOF
		}
		return forgeExecutionLeaseCheckpointFixtureValue{}, err
	}
	if value.Authority != (forgeExecutionLeaseCheckpointAuthority{}) {
		return forgeExecutionLeaseCheckpointFixtureValue{}, io.ErrUnexpectedEOF
	}
	return value, nil
}

func forgeExecutionLeaseCheckpointValid(value forgeExecutionLeaseCheckpoint) (bool, bool) {
	if value.SchemaVersion != "forge.execution-lease-checkpoint/v1" ||
		value.EvaluationMode != "pure_execution_lease_checkpoint_only" ||
		value.Grant.V != 1 || value.Grant.AttemptID == "" || value.Grant.TargetID == "" ||
		value.Grant.Epoch == 0 || value.Grant.FencingToken == "" ||
		value.Grant.ExpiresAtMS <= value.Grant.IssuedAtMS {
		return false, false
	}
	if value.Terminal == nil {
		return true, false
	}
	receipt := value.Terminal
	if receipt.V != 1 || receipt.Proof.AttemptID != value.Grant.AttemptID ||
		receipt.Proof.TargetID != value.Grant.TargetID || receipt.Proof.Epoch != value.Grant.Epoch ||
		receipt.Proof.FencingToken != value.Grant.FencingToken ||
		receipt.ObservedAtMS < value.Grant.IssuedAtMS || receipt.ObservedAtMS >= value.Grant.ExpiresAtMS {
		return false, false
	}
	switch receipt.Disposition.Kind {
	case "completed":
		if len(receipt.Disposition.ReceiptSHA256) != 64 || !isLowerHexCheckpoint(receipt.Disposition.ReceiptSHA256) {
			return false, false
		}
	case "failed", "uncertain":
		if receipt.Disposition.Reason == "" {
			return false, false
		}
	default:
		return false, false
	}
	return true, receipt.Disposition.Kind == "uncertain"
}

func isLowerHexCheckpoint(value string) bool {
	for _, char := range value {
		if !(char >= '0' && char <= '9') && !(char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

func TestForgeExecutionLeaseCheckpointCanonicalFixture(t *testing.T) {
	value, err := decodeForgeExecutionLeaseCheckpoint(forgeExecutionLeaseCheckpointFixture)
	if err != nil {
		t.Fatalf("decode execution lease checkpoint fixture: %v", err)
	}
	if value.SchemaVersion != "forge.execution-lease-checkpoint/v1" ||
		value.EvaluationMode != "pure_execution_lease_checkpoint_only" ||
		value.Authority != (forgeExecutionLeaseCheckpointAuthority{}) || len(value.Cases) != 4 {
		t.Fatalf("checkpoint fixture is not bounded and authority-free: %#v", value)
	}
	seen := make(map[string]bool, len(value.Cases))
	for _, item := range value.Cases {
		if item.Name == "" || seen[item.Name] {
			t.Fatalf("invalid or duplicate checkpoint case: %#v", item)
		}
		seen[item.Name] = true
		accepted, uncertain := forgeExecutionLeaseCheckpointValid(item.Checkpoint)
		if accepted != item.Expected.Accepted || (accepted && item.Checkpoint.Terminal != nil != item.Expected.Terminal) ||
			(accepted && uncertain != item.Expected.Uncertain) {
			t.Fatalf("checkpoint case %q result mismatch: accepted=%v uncertain=%v expected=%#v", item.Name, accepted, uncertain, item.Expected)
		}
		if !accepted && item.Expected.Error != "invalid_checkpoint" {
			t.Fatalf("checkpoint case %q error=%q", item.Name, item.Expected.Error)
		}
	}
	for _, name := range []string{"empty_state", "completed_receipt_survives_restart", "uncertain_receipt_remains_terminal", "foreign_proof_rejected"} {
		if !seen[name] {
			t.Fatalf("checkpoint fixture missing case %q", name)
		}
	}
}

func TestForgeExecutionLeaseCheckpointRejectsUnknownAuthorityAndDuplicateFields(t *testing.T) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(forgeExecutionLeaseCheckpointFixture, &root); err != nil {
		t.Fatal(err)
	}
	root["unexpected"] = json.RawMessage(`true`)
	unknown, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeForgeExecutionLeaseCheckpoint(unknown); err == nil {
		t.Fatal("unknown checkpoint field accepted")
	}
	mutated := strings.Replace(string(forgeExecutionLeaseCheckpointFixture), `"audit_published": false`, `"audit_published": true`, 1)
	if _, err := decodeForgeExecutionLeaseCheckpoint([]byte(mutated)); err == nil {
		t.Fatal("checkpoint authority mutation accepted")
	}
	duplicate := strings.TrimSpace(string(forgeExecutionLeaseCheckpointFixture))
	duplicate = strings.TrimSuffix(duplicate, "}") + `,"schema_version":"forge.execution-lease-checkpoint/v1"}`
	if _, err := decodeForgeExecutionLeaseCheckpoint([]byte(duplicate)); err == nil {
		t.Fatal("duplicate checkpoint field accepted")
	}
}
