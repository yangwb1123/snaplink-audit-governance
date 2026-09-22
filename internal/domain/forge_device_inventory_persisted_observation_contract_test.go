package domain

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"testing"
)

// This receiver-side contract is a bounded, owner-bound projection of already
// restored values. It does not authenticate a device, persist a heartbeat,
// issue credentials, or grant inventory/execution/Audit authority.
//
//go:embed testdata/forge-device-inventory-persisted-observation-v1.json
var forgePersistedInventoryObservationFixture []byte

type forgePersistedInventoryOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}
type forgePersistedInventoryAuthority struct {
	IdentityVerified       bool `json:"identity_verified"`
	HeartbeatPersisted     bool `json:"heartbeat_persisted"`
	InventoryAuthoritative bool `json:"inventory_authoritative"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
}
type forgePersistedInventoryStateGPU struct {
	ID                   string `json:"id"`
	Vendor               string `json:"vendor"`
	MemoryBytes          uint64 `json:"memory_bytes"`
	AvailableMemoryBytes uint64 `json:"available_memory_bytes"`
}
type forgePersistedInventoryCapabilities struct {
	OS                    string                            `json:"os"`
	Architecture          string                            `json:"architecture"`
	CPUCores              uint64                            `json:"cpu_cores"`
	AvailableCPUCores     uint64                            `json:"available_cpu_cores"`
	MemoryBytes           uint64                            `json:"memory_bytes"`
	AvailableMemoryBytes  uint64                            `json:"available_memory_bytes"`
	StorageBytes          uint64                            `json:"storage_bytes"`
	AvailableStorageBytes uint64                            `json:"available_storage_bytes"`
	GPUs                  []forgePersistedInventoryStateGPU `json:"gpus"`
	Runtimes              []string                          `json:"runtimes"`
}
type forgePersistedInventoryDeviceState struct {
	DeviceID         string                       `json:"device_id"`
	Owner            forgePersistedInventoryOwner `json:"owner"`
	ApprovalState    string                       `json:"approval_state"`
	CordonState      string                       `json:"cordon_state"`
	ReservationState string                       `json:"reservation_state"`
}
type forgePersistedInventoryRunnerState struct {
	DeviceID                   string                              `json:"device_id"`
	InstanceID                 string                              `json:"instance_id"`
	Generation                 uint64                              `json:"generation"`
	HeartbeatSequence          uint64                              `json:"heartbeat_sequence"`
	ServerObservedAtMS         uint64                              `json:"server_observed_at_ms"`
	CapabilityLeaseExpiresAtMS uint64                              `json:"capability_lease_expires_at_ms"`
	Liveness                   string                              `json:"liveness"`
	Capabilities               forgePersistedInventoryCapabilities `json:"capabilities"`
}
type forgePersistedInventoryState struct {
	Revision uint64                             `json:"revision"`
	Device   forgePersistedInventoryDeviceState `json:"device"`
	Runner   forgePersistedInventoryRunnerState `json:"runner"`
}
type forgePersistedInventoryGPUDeclaration struct {
	Present     bool   `json:"present"`
	MemoryBytes uint64 `json:"memory_bytes"`
	Runtime     string `json:"runtime"`
}
type forgePersistedInventoryExpectedDevice struct {
	DeviceID              string                                `json:"device_id"`
	Owner                 forgePersistedInventoryOwner          `json:"owner"`
	ApprovalState         string                                `json:"approval_state"`
	CordonState           string                                `json:"cordon_state"`
	Liveness              string                                `json:"liveness"`
	SnapshotObservedAtMS  uint64                                `json:"snapshot_observed_at_ms"`
	LeaseExpiresAtMS      uint64                                `json:"lease_expires_at_ms"`
	OS                    string                                `json:"os"`
	Architecture          string                                `json:"architecture"`
	AvailableCPUCores     uint64                                `json:"available_cpu_cores"`
	AvailableMemoryBytes  uint64                                `json:"available_memory_bytes"`
	AvailableStorageBytes uint64                                `json:"available_storage_bytes"`
	Runtimes              []string                              `json:"runtimes"`
	GPU                   forgePersistedInventoryGPUDeclaration `json:"gpu"`
	DataResidencyZones    []string                              `json:"data_residency_zones"`
	TrustZone             string                                `json:"trust_zone"`
	SandboxLevels         []string                              `json:"sandbox_levels"`
	ConcurrencyLimit      uint64                                `json:"concurrency_limit"`
	ActiveConcurrency     uint64                                `json:"active_concurrency"`
}
type forgePersistedInventoryExpectedCandidate struct {
	InstanceID string                                `json:"instance_id"`
	Device     forgePersistedInventoryExpectedDevice `json:"device"`
}
type forgePersistedInventoryExpected struct {
	SchemaVersion                   string                                     `json:"schema_version"`
	EvaluationMode                  string                                     `json:"evaluation_mode"`
	EvaluatedAtMS                   uint64                                     `json:"evaluated_at_ms"`
	Owner                           forgePersistedInventoryOwner               `json:"owner_declaration"`
	OwnerDeclarationUnverified      bool                                       `json:"owner_declaration_unverified"`
	InventoryDeclarationsUnverified bool                                       `json:"inventory_declarations_unverified"`
	Notice                          string                                     `json:"notice"`
	Devices                         []forgePersistedInventoryExpectedCandidate `json:"devices"`
	ExecutionAuthorized             bool                                       `json:"execution_authorized"`
	ReservationCreated              bool                                       `json:"reservation_created"`
	DispatchPerformed               bool                                       `json:"dispatch_performed"`
}
type forgePersistedInventoryFixture struct {
	SchemaVersion   string                           `json:"schema_version"`
	EvaluationMode  string                           `json:"evaluation_mode"`
	EvaluationOwner forgePersistedInventoryOwner     `json:"evaluation_owner"`
	EvaluatedAtMS   uint64                           `json:"evaluated_at_ms"`
	Authority       forgePersistedInventoryAuthority `json:"authority"`
	States          []forgePersistedInventoryState   `json:"states"`
	Expected        forgePersistedInventoryExpected  `json:"expected"`
}

func decodeForgePersistedInventoryObservation(raw []byte, out *forgePersistedInventoryFixture) error {
	if err := rejectForgePersistedInventoryDuplicateKeys(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	return validateForgePersistedInventoryObservation(out)
}

func validateForgePersistedInventoryObservation(fixture *forgePersistedInventoryFixture) error {
	if fixture.SchemaVersion != "forge.device-inventory-persisted-observation/v1" ||
		fixture.EvaluationMode != "pure_persisted_inventory_to_observation" ||
		fixture.EvaluatedAtMS != 1500 || fixture.Authority != (forgePersistedInventoryAuthority{}) {
		return fmt.Errorf("invalid persisted inventory envelope")
	}
	owner := fixture.EvaluationOwner
	if owner != (forgePersistedInventoryOwner{Issuer: "issuer", Subject: "user", TenantID: "tenant"}) || len(fixture.States) != 2 {
		return fmt.Errorf("invalid owner or state set")
	}
	for _, state := range fixture.States {
		if state.Revision == 0 || state.Device.Owner != owner || state.Device.DeviceID == "" ||
			state.Runner.DeviceID != state.Device.DeviceID || state.Runner.InstanceID == "" ||
			state.Runner.Generation == 0 || state.Runner.HeartbeatSequence == 0 ||
			state.Runner.CapabilityLeaseExpiresAtMS <= state.Runner.ServerObservedAtMS ||
			state.Device.ReservationState != "none" ||
			state.Runner.Capabilities.AvailableCPUCores > state.Runner.Capabilities.CPUCores ||
			state.Runner.Capabilities.AvailableMemoryBytes > state.Runner.Capabilities.MemoryBytes ||
			state.Runner.Capabilities.AvailableStorageBytes > state.Runner.Capabilities.StorageBytes {
			return fmt.Errorf("invalid persisted state binding")
		}
	}
	expected := fixture.Expected
	if expected.SchemaVersion != "forge.device-inventory-observation/v1" || expected.EvaluationMode != "offline_static_only" ||
		expected.EvaluatedAtMS != fixture.EvaluatedAtMS || expected.Owner != owner || !expected.OwnerDeclarationUnverified ||
		!expected.InventoryDeclarationsUnverified || expected.Notice == "" || len(expected.Devices) != 2 ||
		expected.ExecutionAuthorized || expected.ReservationCreated || expected.DispatchPerformed {
		return fmt.Errorf("invalid expected observation boundary")
	}
	for index, candidate := range expected.Devices {
		if candidate.Device.Owner != owner || candidate.InstanceID == "" || candidate.Device.DeviceID == "" ||
			candidate.Device.GPU.Present || candidate.Device.TrustZone != "unknown" || candidate.Device.ConcurrencyLimit != 0 || candidate.Device.ActiveConcurrency != 0 {
			return fmt.Errorf("invalid expected candidate")
		}
		if index > 0 && expected.Devices[index-1].Device.DeviceID >= candidate.Device.DeviceID {
			return fmt.Errorf("expected candidates are not sorted")
		}
	}
	return nil
}

func rejectForgePersistedInventoryDuplicateKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var walk func() error
	walk = func() error {
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
			seen := map[string]struct{}{}
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok {
					return fmt.Errorf("object key is not string")
				}
				if _, exists := seen[name]; exists {
					return fmt.Errorf("duplicate JSON key %q", name)
				}
				seen[name] = struct{}{}
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return fmt.Errorf("unexpected JSON delimiter %q", delim)
		}
	}
	if err := walk(); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	return nil
}

func TestForgeDeviceInventoryPersistedObservationFixture(t *testing.T) {
	var fixture forgePersistedInventoryFixture
	if err := decodeForgePersistedInventoryObservation(forgePersistedInventoryObservationFixture, &fixture); err != nil {
		t.Fatalf("decode persisted inventory observation fixture: %v", err)
	}
	if fixture.States[0].Device.DeviceID != "device-b" || fixture.States[1].Device.DeviceID != "device-a" ||
		fixture.Expected.Devices[0].Device.DeviceID != "device-a" || fixture.Expected.Devices[1].Device.DeviceID != "device-b" {
		t.Fatalf("persisted states or projected devices are not canonical: %#v", fixture)
	}
}

func TestForgeDeviceInventoryPersistedObservationRejectsMutation(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(map[string]json.RawMessage)
	}{
		{"unknown", func(root map[string]json.RawMessage) { root["unexpected"] = json.RawMessage(`true`) }},
		{"authority", func(root map[string]json.RawMessage) {
			root["authority"] = json.RawMessage(`{"inventory_authoritative":true}`)
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			var root map[string]json.RawMessage
			if err := json.Unmarshal(forgePersistedInventoryObservationFixture, &root); err != nil {
				t.Fatal(err)
			}
			mutation.mutate(root)
			raw, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			var fixture forgePersistedInventoryFixture
			err = decodeForgePersistedInventoryObservation(raw, &fixture)
			if mutation.name == "authority" {
				if err == nil {
					t.Fatal("authority mutation unexpectedly accepted")
				}
			} else if err == nil {
				t.Fatal("unknown field unexpectedly accepted")
			}
		})
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(forgePersistedInventoryObservationFixture, &root); err != nil {
		t.Fatal(err)
	}
	root["evaluation_owner"] = json.RawMessage(`{"issuer":"issuer","subject":"foreign","tenant_id":"tenant"}`)
	raw, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	var fixture forgePersistedInventoryFixture
	if err := decodeForgePersistedInventoryObservation(raw, &fixture); err == nil {
		t.Fatal("foreign owner unexpectedly accepted")
	}
	duplicate := bytes.TrimSpace(forgePersistedInventoryObservationFixture)
	duplicate = append(bytes.TrimSuffix(duplicate, []byte("}")), []byte(`,"schema_version":"forge.device-inventory-persisted-observation/v1"}`)...)
	if err := decodeForgePersistedInventoryObservation(duplicate, &fixture); err == nil {
		t.Fatal("duplicate field unexpectedly accepted")
	}
}
