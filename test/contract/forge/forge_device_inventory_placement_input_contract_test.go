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

// This receiver validates the pure persisted-inventory-to-placement-input
// boundary. The values are unverified declarations and never become device
// identity, inventory authority, selection, reservation, scheduling,
// execution, or an Audit fact.
//
//go:embed testdata/forge-device-inventory-placement-input-v1.json
var forgeDeviceInventoryPlacementInputFixture []byte

const (
	forgeDeviceInventoryPlacementInputSchema = "forge.device-inventory-placement-input/v1"
	forgeDeviceInventoryPlacementInputMode   = "pure_persisted_inventory_to_placement_input"
	forgeDeviceInventoryPlacementInputSafe   = uint64(9007199254740991)
)

type forgeDeviceInventoryPlacementInputOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeDeviceInventoryPlacementInputAuthority struct {
	IdentityVerified       bool `json:"identity_verified"`
	HeartbeatPersisted     bool `json:"heartbeat_persisted"`
	InventoryAuthoritative bool `json:"inventory_authoritative"`
	PlacementSelected      bool `json:"placement_selected"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
}

type forgeDeviceInventoryPlacementInputPolicy struct {
	DataResidencyZones []string `json:"data_residency_zones"`
	MinimumTrustZone   string   `json:"minimum_trust_zone"`
	SandboxFloor       string   `json:"sandbox_floor"`
	ConcurrencySlots   uint64   `json:"concurrency_slots"`
}

type forgeDeviceInventoryPlacementInputGPU struct {
	ID                   string `json:"id"`
	Vendor               string `json:"vendor"`
	MemoryBytes          uint64 `json:"memory_bytes"`
	AvailableMemoryBytes uint64 `json:"available_memory_bytes"`
}

type forgeDeviceInventoryPlacementInputCapabilities struct {
	OS                    string                                  `json:"os"`
	Architecture          string                                  `json:"architecture"`
	CPUCores              uint64                                  `json:"cpu_cores"`
	AvailableCPUCores     uint64                                  `json:"available_cpu_cores"`
	MemoryBytes           uint64                                  `json:"memory_bytes"`
	AvailableMemoryBytes  uint64                                  `json:"available_memory_bytes"`
	StorageBytes          uint64                                  `json:"storage_bytes"`
	AvailableStorageBytes uint64                                  `json:"available_storage_bytes"`
	GPUs                  []forgeDeviceInventoryPlacementInputGPU `json:"gpus"`
	Runtimes              []string                                `json:"runtimes"`
}

type forgeDeviceInventoryPlacementInputDevice struct {
	DeviceID         string                                  `json:"device_id"`
	Owner            forgeDeviceInventoryPlacementInputOwner `json:"owner"`
	ApprovalState    string                                  `json:"approval_state"`
	CordonState      string                                  `json:"cordon_state"`
	ReservationState string                                  `json:"reservation_state"`
}

type forgeDeviceInventoryPlacementInputRunner struct {
	DeviceID                   string                                         `json:"device_id"`
	InstanceID                 string                                         `json:"instance_id"`
	Generation                 uint64                                         `json:"generation"`
	HeartbeatSequence          uint64                                         `json:"heartbeat_sequence"`
	ServerObservedAtMS         uint64                                         `json:"server_observed_at_ms"`
	CapabilityLeaseExpiresAtMS uint64                                         `json:"capability_lease_expires_at_ms"`
	Liveness                   string                                         `json:"liveness"`
	Capabilities               forgeDeviceInventoryPlacementInputCapabilities `json:"capabilities"`
}

type forgeDeviceInventoryPlacementInputState struct {
	Revision uint64                                   `json:"revision"`
	Device   forgeDeviceInventoryPlacementInputDevice `json:"device"`
	Runner   forgeDeviceInventoryPlacementInputRunner `json:"runner"`
}

type forgeDeviceInventoryPlacementInputExpected struct {
	Accepted                   bool     `json:"accepted"`
	Error                      string   `json:"error"`
	Revision                   uint64   `json:"revision"`
	DeviceID                   string   `json:"device_id"`
	InstanceID                 string   `json:"instance_id"`
	Generation                 uint64   `json:"generation"`
	HeartbeatSequence          uint64   `json:"heartbeat_sequence"`
	ApprovalState              string   `json:"approval_state"`
	CordonState                string   `json:"cordon_state"`
	ReservationState           string   `json:"reservation_state"`
	Liveness                   string   `json:"liveness"`
	SnapshotObservedAtMS       int64    `json:"snapshot_observed_at_ms"`
	LeaseExpiresAtMS           int64    `json:"lease_expires_at_ms"`
	OwnerDeclarationUnverified bool     `json:"owner_declaration_unverified"`
	PolicyAttributesUnverified bool     `json:"policy_attributes_unverified"`
	DataResidencyZones         []string `json:"data_residency_zones"`
	TrustZone                  string   `json:"trust_zone"`
	SandboxLevels              []string `json:"sandbox_levels"`
	ConcurrencyLimit           uint64   `json:"concurrency_limit"`
	ActiveConcurrency          uint64   `json:"active_concurrency"`
	PolicyRequirementsMet      bool     `json:"policy_requirements_met"`
}

type forgeDeviceInventoryPlacementInputCase struct {
	Name                       string                                     `json:"name"`
	EvaluationOwner            *forgeDeviceInventoryPlacementInputOwner   `json:"evaluation_owner"`
	RunnerDeviceID             string                                     `json:"runner_device_id"`
	ApprovalState              string                                     `json:"approval_state"`
	CordonState                string                                     `json:"cordon_state"`
	Liveness                   string                                     `json:"liveness"`
	ServerObservedAtMS         *uint64                                    `json:"server_observed_at_ms"`
	CapabilityLeaseExpiresAtMS *uint64                                    `json:"capability_lease_expires_at_ms"`
	Expected                   forgeDeviceInventoryPlacementInputExpected `json:"expected"`
}

type forgeDeviceInventoryPlacementInputFixtureType struct {
	SchemaVersion      string                                      `json:"schema_version"`
	EvaluationMode     string                                      `json:"evaluation_mode"`
	EvaluationOwner    forgeDeviceInventoryPlacementInputOwner     `json:"evaluation_owner"`
	PolicyRequirements forgeDeviceInventoryPlacementInputPolicy    `json:"policy_requirements"`
	Authority          forgeDeviceInventoryPlacementInputAuthority `json:"authority"`
	State              forgeDeviceInventoryPlacementInputState     `json:"state"`
	Cases              []forgeDeviceInventoryPlacementInputCase    `json:"cases"`
}

func decodeForgeDeviceInventoryPlacementInput(raw []byte) (forgeDeviceInventoryPlacementInputFixtureType, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeDeviceInventoryPlacementInputFixtureType{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var fixture forgeDeviceInventoryPlacementInputFixtureType
	if err := decoder.Decode(&fixture); err != nil {
		return forgeDeviceInventoryPlacementInputFixtureType{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeDeviceInventoryPlacementInputFixtureType{}, errors.New("trailing JSON value")
		}
		return forgeDeviceInventoryPlacementInputFixtureType{}, err
	}
	return fixture, validateForgeDeviceInventoryPlacementInput(fixture)
}

func validateForgeDeviceInventoryPlacementInput(fixture forgeDeviceInventoryPlacementInputFixtureType) error {
	owner := forgeDeviceInventoryPlacementInputOwner{Issuer: "https://id.example", Subject: "user-1", TenantID: "tenant-1"}
	if fixture.SchemaVersion != forgeDeviceInventoryPlacementInputSchema ||
		fixture.EvaluationMode != forgeDeviceInventoryPlacementInputMode || fixture.EvaluationOwner != owner ||
		fixture.Authority != (forgeDeviceInventoryPlacementInputAuthority{}) || len(fixture.Cases) != 12 ||
		fixture.PolicyRequirements.MinimumTrustZone != "standard" || fixture.PolicyRequirements.SandboxFloor != "container" ||
		fixture.PolicyRequirements.ConcurrencySlots != 1 || len(fixture.PolicyRequirements.DataResidencyZones) != 1 ||
		fixture.PolicyRequirements.DataResidencyZones[0] != "us-west" {
		return errors.New("invalid placement input envelope")
	}
	state := fixture.State
	if state.Revision != 7 || state.Device.DeviceID != "device-a" || state.Device.Owner != owner ||
		state.Device.ApprovalState != "approved" || state.Device.CordonState != "clear" || state.Device.ReservationState != "none" ||
		state.Runner.DeviceID != "device-a" || state.Runner.InstanceID != "runner-a" || state.Runner.Generation != 3 ||
		state.Runner.HeartbeatSequence != 12 || state.Runner.ServerObservedAtMS != 200000 ||
		state.Runner.CapabilityLeaseExpiresAtMS != 260000 || state.Runner.Liveness != "online" ||
		state.Runner.Capabilities.OS != "linux" || state.Runner.Capabilities.Architecture != "amd64" ||
		state.Runner.Capabilities.CPUCores != 8 || state.Runner.Capabilities.AvailableCPUCores != 6 ||
		state.Runner.Capabilities.MemoryBytes != 16384 || state.Runner.Capabilities.AvailableMemoryBytes != 12288 ||
		state.Runner.Capabilities.StorageBytes != 8192 || state.Runner.Capabilities.AvailableStorageBytes != 4096 ||
		len(state.Runner.Capabilities.GPUs) != 0 || len(state.Runner.Capabilities.Runtimes) != 1 || state.Runner.Capabilities.Runtimes[0] != "oci" {
		return errors.New("invalid persisted inventory state")
	}
	wantNames := []string{"online", "stale", "expired", "pending", "cordoned", "revoked", "offline", "owner_mismatch", "runner_device_binding_mismatch", "missing_policy_attributes_fail_closed", "unsafe_snapshot_observed_at_ms", "unsafe_capability_lease_expires_at_ms"}
	for index, item := range fixture.Cases {
		if item.Name != wantNames[index] || !validForgeDeviceInventoryPlacementInputToken(item.Name) {
			return fmt.Errorf("invalid placement input case %d", index)
		}
		if index < 7 || index == 9 {
			if err := validateForgeDeviceInventoryPlacementInputAcceptedCase(item, state); err != nil {
				return err
			}
			continue
		}
		wantError := "owner_mismatch"
		if index == 8 {
			wantError = "runner_device_mismatch"
		} else if index >= 10 {
			wantError = "invalid_persisted_inventory_placement_input"
		}
		if item.Expected.Accepted || item.Expected.Error != wantError || item.Expected.Revision != 0 || item.Expected.DeviceID != "" || item.Expected.InstanceID != "" {
			return fmt.Errorf("invalid placement input rejection case %q", item.Name)
		}
	}
	return nil
}

func validateForgeDeviceInventoryPlacementInputAcceptedCase(item forgeDeviceInventoryPlacementInputCase, base forgeDeviceInventoryPlacementInputState) error {
	approval, cordon, liveness := base.Device.ApprovalState, base.Device.CordonState, base.Runner.Liveness
	observed, lease := base.Runner.ServerObservedAtMS, base.Runner.CapabilityLeaseExpiresAtMS
	if item.ApprovalState != "" {
		approval = item.ApprovalState
	}
	if item.CordonState != "" {
		cordon = item.CordonState
	}
	if item.Liveness != "" {
		liveness = item.Liveness
	}
	if item.ServerObservedAtMS != nil {
		observed = *item.ServerObservedAtMS
	}
	if item.CapabilityLeaseExpiresAtMS != nil {
		lease = *item.CapabilityLeaseExpiresAtMS
	}
	want := item.Expected
	if !want.Accepted || want.Error != "" || want.Revision != base.Revision || want.DeviceID != base.Device.DeviceID ||
		want.InstanceID != base.Runner.InstanceID || want.Generation != base.Runner.Generation || want.HeartbeatSequence != base.Runner.HeartbeatSequence ||
		want.ApprovalState != approval || want.CordonState != cordon || want.ReservationState != base.Device.ReservationState || want.Liveness != liveness ||
		want.SnapshotObservedAtMS != int64(observed) || want.LeaseExpiresAtMS != int64(lease) || !want.OwnerDeclarationUnverified ||
		!want.PolicyAttributesUnverified || len(want.DataResidencyZones) != 0 || want.TrustZone != "unknown" || len(want.SandboxLevels) != 0 ||
		want.ConcurrencyLimit != 0 || want.ActiveConcurrency != 0 || want.PolicyRequirementsMet {
		return fmt.Errorf("invalid accepted placement input case %q", item.Name)
	}
	return nil
}

func validForgeDeviceInventoryPlacementInputToken(value string) bool {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') && (character < '0' || character > '9') && character != '_' && character != '-' {
			return false
		}
	}
	return true
}

func appendForgeDeviceInventoryPlacementInputRootField(raw []byte) []byte {
	trimmed := bytes.TrimSuffix(bytes.TrimSpace(raw), []byte("}"))
	return append(append([]byte(nil), trimmed...), []byte(`,"unexpected":true}`)...)
}

func TestForgeDeviceInventoryPlacementInputReceiver(t *testing.T) {
	fixture, err := decodeForgeDeviceInventoryPlacementInput(forgeDeviceInventoryPlacementInputFixture)
	if err != nil {
		t.Fatalf("decode placement input fixture: %v", err)
	}
	if len(fixture.Cases) != 12 {
		t.Fatalf("case count=%d", len(fixture.Cases))
	}
	t.Run("unknown field", func(t *testing.T) {
		if _, err := decodeForgeDeviceInventoryPlacementInput(appendForgeDeviceInventoryPlacementInputRootField(forgeDeviceInventoryPlacementInputFixture)); err == nil {
			t.Fatal("unknown field accepted")
		}
	})
	t.Run("authority mutation", func(t *testing.T) {
		mutated := bytes.Replace(forgeDeviceInventoryPlacementInputFixture, []byte(`"dispatch_performed": false`), []byte(`"dispatch_performed": true`), 1)
		if _, err := decodeForgeDeviceInventoryPlacementInput(mutated); err == nil {
			t.Fatal("authority mutation accepted")
		}
	})
	t.Run("owner mutation", func(t *testing.T) {
		mutated := bytes.Replace(forgeDeviceInventoryPlacementInputFixture, []byte(`"subject": "user-1"`), []byte(`"subject": "other-user"`), 1)
		if _, err := decodeForgeDeviceInventoryPlacementInput(mutated); err == nil {
			t.Fatal("owner mutation accepted")
		}
	})
	t.Run("duplicate field", func(t *testing.T) {
		mutated := bytes.Replace(forgeDeviceInventoryPlacementInputFixture, []byte(`"schema_version": "forge.device-inventory-placement-input/v1",`), []byte(`"schema_version": "forge.device-inventory-placement-input/v1","schema_version": "forge.device-inventory-placement-input/v1",`), 1)
		if _, err := decodeForgeDeviceInventoryPlacementInput(mutated); err == nil {
			t.Fatal("duplicate field accepted")
		}
	})
	t.Run("trailing value", func(t *testing.T) {
		mutated := append(append([]byte(nil), forgeDeviceInventoryPlacementInputFixture...), []byte(` true`)...)
		if _, err := decodeForgeDeviceInventoryPlacementInput(mutated); err == nil {
			t.Fatal("trailing value accepted")
		}
	})
}
