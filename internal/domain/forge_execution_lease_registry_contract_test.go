package domain

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

const forgeExecutionLeaseRegistryMaxSafeInteger = uint64(9007199254740991)

// This receiver-side value contract carries a fenced placement lease receipt.
// It never authorizes execution, dispatches a Runner, or publishes an Audit
// fact.
//
//go:embed testdata/forge-execution-lease-registry-v1.json
var forgeExecutionLeaseRegistryFixture []byte

type forgeSchedulerLeaseRegistryOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeSchedulerLeaseRegistryAuthority struct {
	PlacementSelected   bool `json:"placement_selected"`
	ReservationCreated  bool `json:"reservation_created"`
	LeaseIssued         bool `json:"lease_issued"`
	ExecutionAuthorized bool `json:"execution_authorized"`
	DispatchPerformed   bool `json:"dispatch_performed"`
	AuditPublished      bool `json:"audit_published"`
}

type forgeSchedulerLeaseRegistryGrant struct {
	V            uint16 `json:"v"`
	AttemptID    string `json:"attempt_id"`
	TargetID     string `json:"target_id"`
	Epoch        uint64 `json:"epoch"`
	FencingToken string `json:"fencing_token"`
	IssuedAtMS   uint64 `json:"issued_at_ms"`
	ExpiresAtMS  uint64 `json:"expires_at_ms"`
}

type forgeSchedulerLeaseRegistryReceipt struct {
	SchemaVersion     string                               `json:"schema_version"`
	EvaluationMode    string                               `json:"evaluation_mode"`
	Owner             forgeSchedulerLeaseRegistryOwner     `json:"owner"`
	ConversationID    string                               `json:"conversation_id"`
	RunID             string                               `json:"run_id"`
	AttemptID         string                               `json:"attempt_id"`
	DeviceID          string                               `json:"device_id"`
	InstanceID        string                               `json:"instance_id"`
	InventoryRevision uint64                               `json:"inventory_revision"`
	Generation        uint64                               `json:"generation"`
	HeartbeatSequence uint64                               `json:"heartbeat_sequence"`
	Grant             forgeSchedulerLeaseRegistryGrant     `json:"grant"`
	Replayed          bool                                 `json:"replayed"`
	Authority         forgeSchedulerLeaseRegistryAuthority `json:"authority"`
}

func decodeForgeExecutionLeaseRegistry(raw []byte) (forgeSchedulerLeaseRegistryReceipt, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeSchedulerLeaseRegistryReceipt{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var receipt forgeSchedulerLeaseRegistryReceipt
	if err := decoder.Decode(&receipt); err != nil {
		return forgeSchedulerLeaseRegistryReceipt{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || len(fields) != 14 {
		return forgeSchedulerLeaseRegistryReceipt{}, errors.New("scheduler lease top-level fields are not exact")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeSchedulerLeaseRegistryReceipt{}, errors.New("trailing JSON value")
		}
		return forgeSchedulerLeaseRegistryReceipt{}, err
	}
	return receipt, nil
}

func validForgeSchedulerLeaseOwnerPart(value string) bool {
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

func validForgeSchedulerLeaseIdentifier(value string) bool {
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

func forgeExecutionLeaseRegistryIsBounded(receipt forgeSchedulerLeaseRegistryReceipt) bool {
	if receipt.SchemaVersion != "forge.execution-lease-registry/v1" ||
		receipt.EvaluationMode != "durable_scheduler_lease_claim" ||
		!validForgeSchedulerLeaseOwnerPart(receipt.Owner.Issuer) ||
		!validForgeSchedulerLeaseOwnerPart(receipt.Owner.Subject) ||
		!validForgeSchedulerLeaseOwnerPart(receipt.Owner.TenantID) ||
		!validForgeSchedulerLeaseIdentifier(receipt.ConversationID) ||
		!validForgeSchedulerLeaseIdentifier(receipt.RunID) ||
		!validForgeSchedulerLeaseIdentifier(receipt.AttemptID) ||
		!validForgeSchedulerLeaseIdentifier(receipt.DeviceID) ||
		!validForgeSchedulerLeaseIdentifier(receipt.InstanceID) ||
		receipt.InventoryRevision == 0 || receipt.InventoryRevision > forgeExecutionLeaseRegistryMaxSafeInteger ||
		receipt.Generation == 0 || receipt.Generation > forgeExecutionLeaseRegistryMaxSafeInteger ||
		receipt.HeartbeatSequence == 0 || receipt.HeartbeatSequence > forgeExecutionLeaseRegistryMaxSafeInteger ||
		receipt.Grant.V != 1 ||
		!validForgeSchedulerLeaseIdentifier(receipt.Grant.AttemptID) ||
		!validForgeSchedulerLeaseIdentifier(receipt.Grant.TargetID) ||
		receipt.Grant.AttemptID != receipt.AttemptID ||
		receipt.Grant.TargetID != receipt.InstanceID ||
		receipt.Grant.Epoch == 0 ||
		!validForgeSchedulerLeaseIdentifier(receipt.Grant.FencingToken) ||
		receipt.Grant.IssuedAtMS == 0 ||
		receipt.Grant.IssuedAtMS > forgeExecutionLeaseRegistryMaxSafeInteger ||
		receipt.Grant.ExpiresAtMS <= receipt.Grant.IssuedAtMS ||
		receipt.Grant.ExpiresAtMS > forgeExecutionLeaseRegistryMaxSafeInteger ||
		receipt.Grant.ExpiresAtMS-receipt.Grant.IssuedAtMS < 1000 ||
		receipt.Grant.ExpiresAtMS-receipt.Grant.IssuedAtMS > 600000 {
		return false
	}
	return receipt.Authority.PlacementSelected &&
		receipt.Authority.ReservationCreated &&
		receipt.Authority.LeaseIssued &&
		!receipt.Authority.ExecutionAuthorized &&
		!receipt.Authority.DispatchPerformed &&
		!receipt.Authority.AuditPublished
}

func TestForgeExecutionLeaseRegistryReceiver(t *testing.T) {
	receipt, err := decodeForgeExecutionLeaseRegistry(forgeExecutionLeaseRegistryFixture)
	if err != nil {
		t.Fatalf("decode scheduler lease registry receipt: %v", err)
	}
	if !forgeExecutionLeaseRegistryIsBounded(receipt) ||
		receipt.DeviceID != "device-a" || receipt.InstanceID != "runner-a" ||
		receipt.Grant.Epoch != 1 || receipt.Replayed {
		t.Fatalf("scheduler lease registry receipt crossed a boundary: %#v", receipt)
	}

	var unknown map[string]any
	if err := json.Unmarshal(forgeExecutionLeaseRegistryFixture, &unknown); err != nil {
		t.Fatal(err)
	}
	unknown["unexpected"] = true
	encoded, err := json.Marshal(unknown)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeForgeExecutionLeaseRegistry(encoded); err == nil {
		t.Fatal("unknown scheduler lease field accepted")
	}

	mutated := bytes.Replace(forgeExecutionLeaseRegistryFixture,
		[]byte(`"execution_authorized": false`),
		[]byte(`"execution_authorized": true`), 1)
	decoded, err := decodeForgeExecutionLeaseRegistry(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if forgeExecutionLeaseRegistryIsBounded(decoded) {
		t.Fatal("execution-authorizing scheduler lease accepted")
	}

	mutated = bytes.Replace(forgeExecutionLeaseRegistryFixture,
		[]byte(`"target_id": "runner-a"`),
		[]byte(`"target_id": "runner-b"`), 1)
	decoded, err = decodeForgeExecutionLeaseRegistry(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if forgeExecutionLeaseRegistryIsBounded(decoded) {
		t.Fatal("grant binding drift accepted")
	}

	duplicate := bytes.Replace(forgeExecutionLeaseRegistryFixture,
		[]byte(`"schema_version": "forge.execution-lease-registry/v1",`),
		[]byte(`"schema_version": "forge.execution-lease-registry/v1","schema_version": "forge.execution-lease-registry/v1",`), 1)
	if _, err := decodeForgeExecutionLeaseRegistry(duplicate); err == nil {
		t.Fatal("duplicate scheduler lease field accepted")
	}

	trailing := append(append([]byte(nil), forgeExecutionLeaseRegistryFixture...), []byte(` true`)...)
	if _, err := decodeForgeExecutionLeaseRegistry(trailing); err == nil {
		t.Fatal("trailing scheduler lease value accepted")
	}
}
