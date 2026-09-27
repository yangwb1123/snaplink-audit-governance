package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

type forgeRunnerTerminalReceiptVectorsAuthority struct {
	DeviceIdentityVerified bool `json:"device_identity_verified"`
	CommandPersisted       bool `json:"command_persisted"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
	AuditPublished         bool `json:"audit_published"`
}
type forgeRunnerTerminalReceiptVectorsProof struct {
	AttemptID    string `json:"attempt_id"`
	TargetID     string `json:"target_id"`
	Epoch        uint64 `json:"epoch"`
	FencingToken string `json:"fencing_token"`
}
type forgeRunnerTerminalReceiptVectorsGrant struct {
	V            uint16 `json:"v"`
	AttemptID    string `json:"attempt_id"`
	TargetID     string `json:"target_id"`
	Epoch        uint64 `json:"epoch"`
	FencingToken string `json:"fencing_token"`
	IssuedAtMS   uint64 `json:"issued_at_ms"`
	ExpiresAtMS  uint64 `json:"expires_at_ms"`
}
type forgeRunnerTerminalReceiptVectorsCommand struct {
	V              uint16                                 `json:"v"`
	CommandID      string                                 `json:"command_id"`
	LeaseProof     forgeRunnerTerminalReceiptVectorsProof `json:"lease_proof"`
	IdempotencyKey string                                 `json:"idempotency_key"`
	WorkspaceRef   string                                 `json:"workspace_ref"`
	Argv           []string                               `json:"argv"`
	TimeoutMS      uint64                                 `json:"timeout_ms"`
	MaxOutputBytes uint64                                 `json:"max_output_bytes"`
}
type forgeRunnerTerminalReceiptVectorsDisposition struct {
	Kind          string `json:"kind"`
	ReceiptSHA256 string `json:"receipt_sha256,omitempty"`
	Reason        string `json:"reason,omitempty"`
}
type forgeRunnerTerminalReceiptVectorsReceipt struct {
	V             uint16                                       `json:"v"`
	CommandID     string                                       `json:"command_id"`
	CommandSHA256 string                                       `json:"command_sha256"`
	Proof         forgeRunnerTerminalReceiptVectorsProof       `json:"proof"`
	Disposition   forgeRunnerTerminalReceiptVectorsDisposition `json:"disposition"`
	ObservedAtMS  uint64                                       `json:"observed_at_ms"`
}
type forgeRunnerTerminalReceiptVectorsExpected struct {
	CommandSHA256          string `json:"command_sha256"`
	DispositionKind        string `json:"disposition_kind"`
	ReceiptValid           bool   `json:"receipt_valid"`
	Uncertain              bool   `json:"uncertain"`
	ReconciliationRequired bool   `json:"reconciliation_required"`
	ManualReviewRequired   bool   `json:"manual_review_required"`
	AutomaticRetry         bool   `json:"automatic_retry"`
	FollowUp               string `json:"follow_up"`
}
type forgeRunnerTerminalReceiptVectorsVector struct {
	Name     string                                    `json:"name"`
	Grant    forgeRunnerTerminalReceiptVectorsGrant    `json:"grant"`
	Command  forgeRunnerTerminalReceiptVectorsCommand  `json:"command"`
	Receipt  forgeRunnerTerminalReceiptVectorsReceipt  `json:"receipt"`
	Expected forgeRunnerTerminalReceiptVectorsExpected `json:"expected"`
}
type forgeRunnerTerminalReceiptVectorsFixture struct {
	SchemaVersion  string                                     `json:"schema_version"`
	EvaluationMode string                                     `json:"evaluation_mode"`
	Authority      forgeRunnerTerminalReceiptVectorsAuthority `json:"authority"`
	Vectors        []forgeRunnerTerminalReceiptVectorsVector  `json:"vectors"`
}

func TestForgeRunnerTerminalReceiptVectors(t *testing.T) {
	fixture := readForgeRunnerTerminalReceiptVectorsFixture(t)
	if fixture.SchemaVersion != "forge.runner-terminal-receipt-vectors/v1" || fixture.EvaluationMode != "pure_runner_terminal_receipt_vectors_only" || fixture.Authority != (forgeRunnerTerminalReceiptVectorsAuthority{}) || len(fixture.Vectors) != 3 {
		t.Fatalf("invalid terminal receipt vector envelope: %#v", fixture)
	}
	seen := map[string]bool{}
	for _, vector := range fixture.Vectors {
		if vector.Name == "" || seen[vector.Name] {
			t.Fatalf("duplicate terminal receipt vector name %q", vector.Name)
		}
		seen[vector.Name] = true
		if err := validateForgeRunnerTerminalReceiptVector(vector); err != nil {
			t.Fatalf("vector %q: %v", vector.Name, err)
		}
	}
}
func TestForgeRunnerTerminalReceiptVectorsRejectWireAndOutcomeDrift(t *testing.T) {
	path := os.Getenv("FORGE_RUNNER_TERMINAL_RECEIPT_VECTORS_FIXTURE")
	if path == "" {
		t.Skip("FORGE_RUNNER_TERMINAL_RECEIPT_VECTORS_FIXTURE is set by the contract script")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mutations := map[string][]byte{"unknown": append(bytes.TrimSuffix(bytes.TrimSpace(raw), []byte("}")), []byte(`,"unexpected":true}`)...), "duplicate": append(bytes.TrimSuffix(bytes.TrimSpace(raw), []byte("}")), []byte(`,"schema_version":"forge.runner-terminal-receipt-vectors/v1"}`)...), "trailing": append(bytes.TrimSpace(raw), []byte(" {}")...)}
	for name, mutation := range mutations {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeForgeRunnerTerminalReceiptVectors(mutation); err == nil {
				t.Fatal("wire mutation was accepted")
			}
		})
	}
	fixture := readForgeRunnerTerminalReceiptVectorsFixture(t)
	digestDrift := fixture.Vectors[0]
	digestDrift.Receipt.CommandSHA256 = strings.Repeat("b", 64)
	if err := validateForgeRunnerTerminalReceiptVector(digestDrift); err == nil {
		t.Fatal("digest drift was accepted")
	}
	expired := fixture.Vectors[2]
	expired.Receipt.ObservedAtMS = expired.Grant.ExpiresAtMS
	if err := validateForgeRunnerTerminalReceiptVector(expired); err == nil {
		t.Fatal("expired receipt was accepted")
	}
}
func readForgeRunnerTerminalReceiptVectorsFixture(t *testing.T) forgeRunnerTerminalReceiptVectorsFixture {
	t.Helper()
	path := os.Getenv("FORGE_RUNNER_TERMINAL_RECEIPT_VECTORS_FIXTURE")
	if path == "" {
		t.Skip("FORGE_RUNNER_TERMINAL_RECEIPT_VECTORS_FIXTURE is set by the contract script")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := decodeForgeRunnerTerminalReceiptVectors(raw)
	if err != nil {
		t.Fatalf("decode terminal receipt vectors: %v", err)
	}
	return fixture
}
func decodeForgeRunnerTerminalReceiptVectors(raw []byte) (forgeRunnerTerminalReceiptVectorsFixture, error) {
	if err := rejectDuplicateForgeRunnerTerminalReceiptVectorsKeys(raw); err != nil {
		return forgeRunnerTerminalReceiptVectorsFixture{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var fixture forgeRunnerTerminalReceiptVectorsFixture
	if err := decoder.Decode(&fixture); err != nil {
		return forgeRunnerTerminalReceiptVectorsFixture{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeRunnerTerminalReceiptVectorsFixture{}, fmt.Errorf("trailing JSON")
		}
		return forgeRunnerTerminalReceiptVectorsFixture{}, err
	}
	return fixture, nil
}
func validateForgeRunnerTerminalReceiptVector(vector forgeRunnerTerminalReceiptVectorsVector) error {
	grant, command, receipt, expected := vector.Grant, vector.Command, vector.Receipt, vector.Expected
	if grant.V != 1 || grant.Epoch == 0 || grant.ExpiresAtMS <= grant.IssuedAtMS || grant.ExpiresAtMS-grant.IssuedAtMS < 1000 || grant.ExpiresAtMS-grant.IssuedAtMS > 600000 {
		return fmt.Errorf("invalid grant")
	}
	digest, err := forgeRunnerTerminalReceiptVectorsCommandDigest(command)
	if err != nil || digest != expected.CommandSHA256 || receipt.CommandSHA256 != digest {
		return fmt.Errorf("command digest mismatch")
	}
	if receipt.V != 1 || receipt.CommandID != command.CommandID || receipt.Proof != command.LeaseProof || receipt.Proof.AttemptID != grant.AttemptID || receipt.Proof.TargetID != grant.TargetID || receipt.Proof.Epoch != grant.Epoch || receipt.Proof.FencingToken != grant.FencingToken || receipt.ObservedAtMS < grant.IssuedAtMS || receipt.ObservedAtMS >= grant.ExpiresAtMS || !expected.ReceiptValid {
		return fmt.Errorf("receipt binding mismatch")
	}
	uncertain := receipt.Disposition.Kind == "uncertain"
	if receipt.Disposition.Kind != expected.DispositionKind || expected.Uncertain != uncertain || expected.ReconciliationRequired != uncertain || expected.ManualReviewRequired != uncertain || expected.AutomaticRetry || (uncertain && expected.FollowUp != "reconciliation_manual") || (!uncertain && expected.FollowUp != "none") {
		return fmt.Errorf("outcome flags mismatch")
	}
	if receipt.Disposition.Kind == "completed" && len(receipt.Disposition.ReceiptSHA256) != 64 {
		return fmt.Errorf("completed receipt digest missing")
	}
	if (receipt.Disposition.Kind == "failed" || uncertain) && receipt.Disposition.Reason == "" {
		return fmt.Errorf("terminal reason missing")
	}
	return nil
}
func forgeRunnerTerminalReceiptVectorsCommandDigest(command forgeRunnerTerminalReceiptVectorsCommand) (string, error) {
	encoded, err := json.Marshal(command)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("forge.runtime.runner-command.v1\x00"), encoded...))
	return hex.EncodeToString(digest[:]), nil
}
func rejectDuplicateForgeRunnerTerminalReceiptVectorsKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanForgeRunnerTerminalReceiptVectorsJSON(decoder); err != nil {
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
func scanForgeRunnerTerminalReceiptVectorsJSON(decoder *json.Decoder) error {
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
			if err := scanForgeRunnerTerminalReceiptVectorsJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := scanForgeRunnerTerminalReceiptVectorsJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return fmt.Errorf("invalid JSON delimiter")
	}
}
