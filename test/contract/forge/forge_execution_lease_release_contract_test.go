package forge

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

const forgeExecutionLeaseReleaseMaxSafeInteger = uint64(9007199254740991)

// This receiver-side value contract carries only a terminal scheduler lease
// release observation. It never releases a lease, selects a device, reserves
// capacity, dispatches a Runner, or grants execution/Audit authority.
//
//go:embed testdata/forge-execution-lease-release-v1.json
var forgeExecutionLeaseReleaseFixture []byte

type forgeExecutionLeaseReleaseOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeExecutionLeaseReleaseAuthority struct {
	PlacementSelected   bool `json:"placement_selected"`
	ReservationCreated  bool `json:"reservation_created"`
	LeaseIssued         bool `json:"lease_issued"`
	ExecutionAuthorized bool `json:"execution_authorized"`
	DispatchPerformed   bool `json:"dispatch_performed"`
	AuditPublished      bool `json:"audit_published"`
}

type forgeExecutionLeaseReleaseReceipt struct {
	SchemaVersion  string                              `json:"schema_version"`
	EvaluationMode string                              `json:"evaluation_mode"`
	Owner          forgeExecutionLeaseReleaseOwner     `json:"owner"`
	ConversationID string                              `json:"conversation_id"`
	RunID          string                              `json:"run_id"`
	AttemptID      string                              `json:"attempt_id"`
	DeviceID       string                              `json:"device_id"`
	InstanceID     string                              `json:"instance_id"`
	Epoch          uint64                              `json:"epoch"`
	ReleasedAtMS   uint64                              `json:"released_at_ms"`
	Replayed       bool                                `json:"replayed"`
	Authority      forgeExecutionLeaseReleaseAuthority `json:"authority"`
}

func decodeForgeExecutionLeaseRelease(raw []byte) (forgeExecutionLeaseReleaseReceipt, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeExecutionLeaseReleaseReceipt{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var receipt forgeExecutionLeaseReleaseReceipt
	if err := decoder.Decode(&receipt); err != nil {
		return forgeExecutionLeaseReleaseReceipt{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || len(fields) != 12 {
		return forgeExecutionLeaseReleaseReceipt{}, errors.New("lease release top-level fields are not exact")
	}
	for _, field := range []string{
		"schema_version", "evaluation_mode", "owner", "conversation_id", "run_id", "attempt_id",
		"device_id", "instance_id", "epoch", "released_at_ms", "replayed", "authority",
	} {
		if _, ok := fields[field]; !ok {
			return forgeExecutionLeaseReleaseReceipt{}, errors.New("lease release top-level field is missing")
		}
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeExecutionLeaseReleaseReceipt{}, errors.New("trailing JSON value")
		}
		return forgeExecutionLeaseReleaseReceipt{}, err
	}
	return receipt, nil
}

func validForgeExecutionLeaseReleaseOwnerPart(value string) bool {
	if value == "" || len(value) > 512 || value != strings.TrimSpace(value) {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func validForgeExecutionLeaseReleaseIdentifier(value string) bool {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value {
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

func forgeExecutionLeaseReleaseIsBounded(receipt forgeExecutionLeaseReleaseReceipt) bool {
	return receipt.SchemaVersion == "forge.execution-lease-release/v1" &&
		receipt.EvaluationMode == "durable_scheduler_lease_release" &&
		validForgeExecutionLeaseReleaseOwnerPart(receipt.Owner.Issuer) &&
		validForgeExecutionLeaseReleaseOwnerPart(receipt.Owner.Subject) &&
		validForgeExecutionLeaseReleaseOwnerPart(receipt.Owner.TenantID) &&
		validForgeExecutionLeaseReleaseIdentifier(receipt.ConversationID) &&
		validForgeExecutionLeaseReleaseIdentifier(receipt.RunID) &&
		validForgeExecutionLeaseReleaseIdentifier(receipt.AttemptID) &&
		validForgeExecutionLeaseReleaseIdentifier(receipt.DeviceID) &&
		validForgeExecutionLeaseReleaseIdentifier(receipt.InstanceID) &&
		receipt.Epoch > 0 && receipt.Epoch <= forgeExecutionLeaseReleaseMaxSafeInteger &&
		receipt.ReleasedAtMS > 0 && receipt.ReleasedAtMS <= forgeExecutionLeaseReleaseMaxSafeInteger &&
		!receipt.Authority.PlacementSelected && !receipt.Authority.ReservationCreated &&
		!receipt.Authority.LeaseIssued && !receipt.Authority.ExecutionAuthorized &&
		!receipt.Authority.DispatchPerformed && !receipt.Authority.AuditPublished
}

func forgeExecutionLeaseReleaseMatchesCanonical(receipt forgeExecutionLeaseReleaseReceipt) bool {
	return forgeExecutionLeaseReleaseIsBounded(receipt) &&
		receipt.Owner.Issuer == "https://id.example" &&
		receipt.Owner.Subject == "user-a" &&
		receipt.Owner.TenantID == "tenant-a" &&
		receipt.ConversationID == "conversation-1" && receipt.RunID == "run-1" &&
		receipt.AttemptID == "attempt-1" && receipt.DeviceID == "device-a" &&
		receipt.InstanceID == "runner-a" && receipt.Epoch == 2 &&
		receipt.ReleasedAtMS == 1800000040000 && !receipt.Replayed
}

func TestForgeExecutionLeaseReleaseReceiver(t *testing.T) {
	receipt, err := decodeForgeExecutionLeaseRelease(forgeExecutionLeaseReleaseFixture)
	if err != nil {
		t.Fatalf("decode lease release receipt: %v", err)
	}
	if !forgeExecutionLeaseReleaseMatchesCanonical(receipt) {
		t.Fatalf("lease release receipt crossed a boundary: %#v", receipt)
	}

	var unknown map[string]any
	if err := json.Unmarshal(forgeExecutionLeaseReleaseFixture, &unknown); err != nil {
		t.Fatal(err)
	}
	unknown["unexpected"] = true
	encoded, err := json.Marshal(unknown)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeForgeExecutionLeaseRelease(encoded); err == nil {
		t.Fatal("unknown lease release field accepted")
	}

	mutated := bytes.Replace(forgeExecutionLeaseReleaseFixture,
		[]byte(`"instance_id": "runner-a"`), []byte(`"instance_id": "runner-b"`), 1)
	decoded, err := decodeForgeExecutionLeaseRelease(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if forgeExecutionLeaseReleaseMatchesCanonical(decoded) {
		t.Fatal("instance binding drift accepted")
	}

	mutated = bytes.Replace(forgeExecutionLeaseReleaseFixture,
		[]byte(`"epoch": 2`), []byte(`"epoch": 0`), 1)
	decoded, err = decodeForgeExecutionLeaseRelease(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if forgeExecutionLeaseReleaseIsBounded(decoded) {
		t.Fatal("zero release epoch accepted")
	}

	mutated = bytes.Replace(forgeExecutionLeaseReleaseFixture,
		[]byte(`"lease_issued": false`), []byte(`"lease_issued": true`), 1)
	decoded, err = decodeForgeExecutionLeaseRelease(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if forgeExecutionLeaseReleaseIsBounded(decoded) {
		t.Fatal("lease-authorizing release accepted")
	}

	duplicate := bytes.Replace(forgeExecutionLeaseReleaseFixture,
		[]byte(`"schema_version": "forge.execution-lease-release/v1",`),
		[]byte(`"schema_version": "forge.execution-lease-release/v1","schema_version": "forge.execution-lease-release/v1",`), 1)
	if _, err := decodeForgeExecutionLeaseRelease(duplicate); err == nil {
		t.Fatal("duplicate lease release field accepted")
	}

	trailing := append(append([]byte(nil), forgeExecutionLeaseReleaseFixture...), []byte(` true`)...)
	if _, err := decodeForgeExecutionLeaseRelease(trailing); err == nil {
		t.Fatal("trailing lease release value accepted")
	}
}
