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

// This receiver consumes a caller-declared, offline placement batch. It keeps
// the six comparison cases available to governance tooling without turning a
// decision into inventory authority, target selection, reservation, dispatch,
// execution, or an Audit fact.
//
//go:embed testdata/forge-device-inventory-placement-batch-evaluation-v1.json
var forgeDeviceInventoryPlacementBatchEvaluationFixture []byte

const (
	forgeDeviceInventoryPlacementBatchEvaluationSchema = "forge.device-inventory-placement-batch-evaluation/v1"
	forgeDeviceInventoryPlacementBatchEvaluationMode   = "pure_persisted_inventory_placement_dry_run"
	forgeDeviceInventoryPlacementBatchEvaluationSource = "forge-device-inventory-placement-input-v1.json"
	forgeDeviceInventoryPlacementBatchEvaluationSafe   = uint64(9007199254740991)
	forgeDeviceInventoryPlacementBatchEvaluationMax    = 128
)

type forgeDeviceInventoryPlacementBatchEvaluationOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeDeviceInventoryPlacementBatchEvaluationGPURequirement struct {
	Required       bool   `json:"required"`
	MinMemoryBytes uint64 `json:"min_memory_bytes"`
	Runtime        string `json:"runtime"`
}

type forgeDeviceInventoryPlacementBatchEvaluationRequirements struct {
	OS                 string                                                     `json:"os"`
	Architecture       string                                                     `json:"architecture"`
	MinCPUCores        uint64                                                     `json:"min_cpu_cores"`
	MinMemoryBytes     uint64                                                     `json:"min_memory_bytes"`
	MinStorageBytes    uint64                                                     `json:"min_storage_bytes"`
	Runtime            string                                                     `json:"runtime"`
	GPU                forgeDeviceInventoryPlacementBatchEvaluationGPURequirement `json:"gpu"`
	DataResidencyZones []string                                                   `json:"data_residency_zones"`
	MinimumTrustZone   string                                                     `json:"minimum_trust_zone"`
	SandboxFloor       string                                                     `json:"sandbox_floor"`
	ConcurrencySlots   uint64                                                     `json:"concurrency_slots"`
}

type forgeDeviceInventoryPlacementBatchEvaluationExpected struct {
	Revision            uint64   `json:"revision"`
	DeviceID            string   `json:"device_id"`
	InstanceID          string   `json:"instance_id"`
	MatchesRequirements bool     `json:"matches_requirements"`
	ExclusionReasons    []string `json:"exclusion_reasons"`
}

type forgeDeviceInventoryPlacementBatchEvaluationCase struct {
	Name                       string                                               `json:"name"`
	SourceCase                 string                                               `json:"source_case"`
	DeviceID                   string                                               `json:"device_id"`
	InstanceID                 string                                               `json:"instance_id"`
	SnapshotObservedAtMS       *uint64                                              `json:"snapshot_observed_at_ms"`
	CapabilityLeaseExpiresAtMS *uint64                                              `json:"capability_lease_expires_at_ms"`
	Expected                   forgeDeviceInventoryPlacementBatchEvaluationExpected `json:"expected"`
}

type forgeDeviceInventoryPlacementBatchEvaluationAuthority struct {
	IdentityVerified       bool `json:"identity_verified"`
	HeartbeatPersisted     bool `json:"heartbeat_persisted"`
	InventoryAuthoritative bool `json:"inventory_authoritative"`
	PlacementSelected      bool `json:"placement_selected"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
}

type forgeDeviceInventoryPlacementBatchEvaluationErrorCase struct {
	Name  string `json:"name"`
	Error string `json:"error"`
}

type forgeDeviceInventoryPlacementBatchEvaluation struct {
	SchemaVersion      string                                                   `json:"schema_version"`
	EvaluationMode     string                                                   `json:"evaluation_mode"`
	SourceFixture      string                                                   `json:"source_fixture"`
	EvaluationOwner    forgeDeviceInventoryPlacementBatchEvaluationOwner        `json:"evaluation_owner"`
	EvaluatedAtMS      uint64                                                   `json:"evaluated_at_ms"`
	Requirements       forgeDeviceInventoryPlacementBatchEvaluationRequirements `json:"requirements"`
	Cases              []forgeDeviceInventoryPlacementBatchEvaluationCase       `json:"cases"`
	EmptyInputsAllowed bool                                                     `json:"empty_inputs_allowed"`
	SelectedDeviceID   *string                                                  `json:"selected_device_id"`
	SelectedInstanceID *string                                                  `json:"selected_instance_id"`
	Authority          forgeDeviceInventoryPlacementBatchEvaluationAuthority    `json:"authority"`
	ErrorCases         []forgeDeviceInventoryPlacementBatchEvaluationErrorCase  `json:"error_cases"`
}

func decodeForgeDeviceInventoryPlacementBatchEvaluation(raw []byte) (forgeDeviceInventoryPlacementBatchEvaluation, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeDeviceInventoryPlacementBatchEvaluation{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var fixture forgeDeviceInventoryPlacementBatchEvaluation
	if err := decoder.Decode(&fixture); err != nil {
		return forgeDeviceInventoryPlacementBatchEvaluation{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeDeviceInventoryPlacementBatchEvaluation{}, errors.New("trailing JSON value")
		}
		return forgeDeviceInventoryPlacementBatchEvaluation{}, err
	}
	return fixture, validateForgeDeviceInventoryPlacementBatchEvaluation(fixture)
}

func validateForgeDeviceInventoryPlacementBatchEvaluation(fixture forgeDeviceInventoryPlacementBatchEvaluation) error {
	owner := forgeDeviceInventoryPlacementBatchEvaluationOwner{Issuer: "https://id.example", Subject: "user-1", TenantID: "tenant-1"}
	requirements := fixture.Requirements
	if fixture.SchemaVersion != forgeDeviceInventoryPlacementBatchEvaluationSchema ||
		fixture.EvaluationMode != forgeDeviceInventoryPlacementBatchEvaluationMode ||
		fixture.SourceFixture != forgeDeviceInventoryPlacementBatchEvaluationSource ||
		fixture.EvaluationOwner != owner || fixture.EvaluatedAtMS == 0 ||
		fixture.EvaluatedAtMS > forgeDeviceInventoryPlacementBatchEvaluationSafe ||
		!fixture.EmptyInputsAllowed || len(fixture.Cases) != 6 ||
		len(fixture.Cases) > forgeDeviceInventoryPlacementBatchEvaluationMax ||
		fixture.SelectedDeviceID != nil || fixture.SelectedInstanceID != nil ||
		fixture.Authority != (forgeDeviceInventoryPlacementBatchEvaluationAuthority{}) ||
		requirements.OS != "linux" || requirements.Architecture != "amd64" ||
		requirements.MinCPUCores != 4 || requirements.MinMemoryBytes != 4096 ||
		requirements.MinStorageBytes != 1024 || requirements.Runtime != "oci" ||
		requirements.GPU != (forgeDeviceInventoryPlacementBatchEvaluationGPURequirement{}) ||
		len(requirements.DataResidencyZones) != 1 || requirements.DataResidencyZones[0] != "us-west" ||
		requirements.MinimumTrustZone != "standard" || requirements.SandboxFloor != "container" ||
		requirements.ConcurrencySlots != 1 {
		return errors.New("invalid placement batch envelope")
	}
	seenDevices := make(map[string]struct{}, len(fixture.Cases))
	seenInstances := make(map[string]struct{}, len(fixture.Cases))
	for index, item := range fixture.Cases {
		if !validForgeDeviceInventoryPlacementBatchToken(item.Name) ||
			!validForgeDeviceInventoryPlacementBatchToken(item.SourceCase) ||
			!validForgeDeviceInventoryPlacementBatchIdentifier(item.DeviceID) ||
			!validForgeDeviceInventoryPlacementBatchIdentifier(item.InstanceID) ||
			item.Expected.Revision != 7 || item.Expected.DeviceID != item.DeviceID ||
			item.Expected.InstanceID != item.InstanceID || item.Expected.MatchesRequirements ||
			len(item.Expected.ExclusionReasons) == 0 || len(item.Expected.ExclusionReasons) > 64 {
			return fmt.Errorf("invalid placement batch case %d", index)
		}
		if _, exists := seenDevices[item.DeviceID]; exists {
			return fmt.Errorf("duplicate placement batch device %q", item.DeviceID)
		}
		if _, exists := seenInstances[item.InstanceID]; exists {
			return fmt.Errorf("duplicate placement batch instance %q", item.InstanceID)
		}
		seenDevices[item.DeviceID] = struct{}{}
		seenInstances[item.InstanceID] = struct{}{}
		for reasonIndex, reason := range item.Expected.ExclusionReasons {
			if !validForgeDeviceInventoryPlacementBatchToken(reason) ||
				(reasonIndex > 0 && item.Expected.ExclusionReasons[reasonIndex-1] >= reason) {
				return fmt.Errorf("placement batch reasons are not sorted for %q", item.Name)
			}
		}
	}
	if len(fixture.ErrorCases) != 4 {
		return errors.New("invalid placement batch error cases")
	}
	wantErrors := [][2]string{
		{"owner_mismatch", "owner_mismatch"},
		{"duplicate_device", "invalid_persisted_inventory_placement_input"},
		{"duplicate_instance", "invalid_persisted_inventory_placement_input"},
		{"invalid_evaluated_at", "invalid_persisted_inventory_placement_input"},
	}
	for index, item := range fixture.ErrorCases {
		if !validForgeDeviceInventoryPlacementBatchToken(item.Name) ||
			!validForgeDeviceInventoryPlacementBatchToken(item.Error) ||
			item.Name != wantErrors[index][0] || item.Error != wantErrors[index][1] {
			return fmt.Errorf("invalid placement batch error case %d", index)
		}
	}
	return nil
}

func validForgeDeviceInventoryPlacementBatchToken(value string) bool {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') && character != '_' && character != '-' && character != '.' {
			return false
		}
	}
	return true
}

func validForgeDeviceInventoryPlacementBatchIdentifier(value string) bool {
	return validForgeDeviceInventoryPlacementBatchToken(value) && len(value) <= 64
}

func appendForgeDeviceInventoryPlacementBatchRootField(raw []byte) []byte {
	trimmed := bytes.TrimSpace(raw)
	trimmed = bytes.TrimSuffix(trimmed, []byte("}"))
	return append(append([]byte(nil), trimmed...), []byte(`,"unexpected":true}`)...)
}

func TestForgeDeviceInventoryPlacementBatchEvaluationReceiver(t *testing.T) {
	fixture, err := decodeForgeDeviceInventoryPlacementBatchEvaluation(forgeDeviceInventoryPlacementBatchEvaluationFixture)
	if err != nil {
		t.Fatalf("decode placement batch fixture: %v", err)
	}
	if fixture.Cases[0].Name != "online" || fixture.Cases[1].Name != "pending" || fixture.Cases[5].Name != "future" {
		t.Fatalf("placement batch case order changed: %#v", fixture.Cases)
	}
	if fixture.Cases[5].SnapshotObservedAtMS == nil || fixture.Cases[5].CapabilityLeaseExpiresAtMS == nil {
		t.Fatal("future case lost its explicit timestamp declarations")
	}

	t.Run("unknown field", func(t *testing.T) {
		mutated := appendForgeDeviceInventoryPlacementBatchRootField(forgeDeviceInventoryPlacementBatchEvaluationFixture)
		if _, err := decodeForgeDeviceInventoryPlacementBatchEvaluation(mutated); err == nil {
			t.Fatal("unknown placement batch field was accepted")
		}
	})
	t.Run("authority mutation", func(t *testing.T) {
		mutated := bytes.Replace(forgeDeviceInventoryPlacementBatchEvaluationFixture, []byte(`"dispatch_performed": false`), []byte(`"dispatch_performed": true`), 1)
		if _, err := decodeForgeDeviceInventoryPlacementBatchEvaluation(mutated); err == nil {
			t.Fatal("authority-bearing placement batch was accepted")
		}
	})
	t.Run("selected target mutation", func(t *testing.T) {
		mutated := bytes.Replace(forgeDeviceInventoryPlacementBatchEvaluationFixture, []byte(`"selected_device_id": null`), []byte(`"selected_device_id": "device-a"`), 1)
		if _, err := decodeForgeDeviceInventoryPlacementBatchEvaluation(mutated); err == nil {
			t.Fatal("selected placement batch target was accepted")
		}
	})
	t.Run("duplicate field", func(t *testing.T) {
		mutated := bytes.Replace(forgeDeviceInventoryPlacementBatchEvaluationFixture, []byte(`"schema_version": "forge.device-inventory-placement-batch-evaluation/v1",`), []byte(`"schema_version": "forge.device-inventory-placement-batch-evaluation/v1","schema_version": "forge.device-inventory-placement-batch-evaluation/v1",`), 1)
		if _, err := decodeForgeDeviceInventoryPlacementBatchEvaluation(mutated); err == nil {
			t.Fatal("duplicate placement batch field was accepted")
		}
	})
	t.Run("trailing value", func(t *testing.T) {
		mutated := append(append([]byte(nil), forgeDeviceInventoryPlacementBatchEvaluationFixture...), []byte(` true`)...)
		if _, err := decodeForgeDeviceInventoryPlacementBatchEvaluation(mutated); err == nil {
			t.Fatal("trailing placement batch value was accepted")
		}
	})
}
