package domain

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

// This receiver-side contract decodes the lossless v2 offline inventory
// observation. It consumes only caller-declared values. It does not
// authenticate an owner, ingest a heartbeat, make inventory authoritative,
// select a device, report schedulability, reserve capacity, or authorize
// execution.
//
//go:embed testdata/forge-device-inventory-observation-v2.json
var forgeDeviceInventoryObservationV2Fixture []byte

const (
	forgeDeviceInventoryObservationV2Schema   = "forge.device-inventory-observation/v2"
	forgeDeviceInventoryObservationV2Mode     = "offline_static_only"
	forgeDeviceInventoryObservationV2Notice   = "Every owner, instance, state, timestamp, resource, GPU, reservation, residency, trust, sandbox, and concurrency value is an unverified caller declaration. This read-only observation selects no target and grants no execution authority."
	forgeDeviceInventoryObservationV2MaxSafe  = uint64(9007199254740991)
	forgeDeviceInventoryObservationV2MaxRows  = 128
	forgeDeviceInventoryObservationV2MaxID    = 128
	forgeDeviceInventoryObservationV2MaxOwner = 512
	forgeDeviceInventoryV2MaxArray            = 32
	forgeDeviceInventoryV2MaxGPUCount         = 32
	forgeDeviceInventoryV2MaxTagBytes         = 64
	forgeDeviceInventoryV2MaxCPU              = 4096
	forgeDeviceInventoryV2MinLeaseTTL         = 1000
	forgeDeviceInventoryV2MaxLeaseTTL         = 600000
)

type forgeDeviceInventoryObservationV2Owner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeDeviceInventoryObservationV2GPU struct {
	ID                   string `json:"id"`
	Vendor               string `json:"vendor"`
	MemoryBytes          uint64 `json:"memory_bytes"`
	AvailableMemoryBytes uint64 `json:"available_memory_bytes"`
}

type forgeDeviceInventoryObservationV2Device struct {
	DeviceID              string                                 `json:"device_id"`
	Owner                 forgeDeviceInventoryObservationV2Owner `json:"owner"`
	ApprovalState         string                                 `json:"approval_state"`
	CordonState           string                                 `json:"cordon_state"`
	ReservationState      string                                 `json:"reservation_state"`
	Liveness              string                                 `json:"liveness"`
	SnapshotObservedAtMS  uint64                                 `json:"snapshot_observed_at_ms"`
	LeaseExpiresAtMS      uint64                                 `json:"lease_expires_at_ms"`
	OS                    string                                 `json:"os"`
	Architecture          string                                 `json:"architecture"`
	AvailableCPUCores     uint64                                 `json:"available_cpu_cores"`
	AvailableMemoryBytes  uint64                                 `json:"available_memory_bytes"`
	AvailableStorageBytes uint64                                 `json:"available_storage_bytes"`
	Runtimes              []string                               `json:"runtimes"`
	GPUs                  []forgeDeviceInventoryObservationV2GPU `json:"gpus"`
	DataResidencyZones    []string                               `json:"data_residency_zones"`
	TrustZone             string                                 `json:"trust_zone"`
	SandboxLevels         []string                               `json:"sandbox_levels"`
	ConcurrencyLimit      uint64                                 `json:"concurrency_limit"`
	ActiveConcurrency     uint64                                 `json:"active_concurrency"`
}

type forgeDeviceInventoryObservationV2Candidate struct {
	InstanceID        string                                  `json:"instance_id"`
	Revision          uint64                                  `json:"revision"`
	Generation        uint64                                  `json:"generation"`
	HeartbeatSequence uint64                                  `json:"heartbeat_sequence"`
	Device            forgeDeviceInventoryObservationV2Device `json:"device"`
}

type forgeDeviceInventoryObservationV2FixtureType struct {
	SchemaVersion                   string                                       `json:"schema_version"`
	EvaluationMode                  string                                       `json:"evaluation_mode"`
	EvaluatedAtMS                   uint64                                       `json:"evaluated_at_ms"`
	Owner                           forgeDeviceInventoryObservationV2Owner       `json:"owner_declaration"`
	OwnerDeclarationUnverified      bool                                         `json:"owner_declaration_unverified"`
	InventoryDeclarationsUnverified bool                                         `json:"inventory_declarations_unverified"`
	Notice                          string                                       `json:"notice"`
	Devices                         []forgeDeviceInventoryObservationV2Candidate `json:"devices"`
	ExecutionAuthorized             bool                                         `json:"execution_authorized"`
	ReservationCreated              bool                                         `json:"reservation_created"`
	DispatchPerformed               bool                                         `json:"dispatch_performed"`
}

func decodeForgeDeviceInventoryObservationV2(raw []byte, out *forgeDeviceInventoryObservationV2FixtureType) error {
	if err := rejectForgeDeviceInventoryObservationV2DuplicateKeys(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	return validateForgeDeviceInventoryObservationV2(out)
}

func validateForgeDeviceInventoryObservationV2(fixture *forgeDeviceInventoryObservationV2FixtureType) error {
	if fixture.SchemaVersion != forgeDeviceInventoryObservationV2Schema ||
		fixture.EvaluationMode != forgeDeviceInventoryObservationV2Mode ||
		fixture.EvaluatedAtMS == 0 || fixture.EvaluatedAtMS > forgeDeviceInventoryObservationV2MaxSafe ||
		fixture.Owner != (forgeDeviceInventoryObservationV2Owner{Issuer: "issuer", Subject: "user", TenantID: "tenant"}) ||
		!validForgeDeviceInventoryObservationV2Owner(fixture.Owner) ||
		!fixture.OwnerDeclarationUnverified || !fixture.InventoryDeclarationsUnverified ||
		fixture.Notice != forgeDeviceInventoryObservationV2Notice || fixture.ExecutionAuthorized ||
		fixture.ReservationCreated || fixture.DispatchPerformed ||
		len(fixture.Devices) > forgeDeviceInventoryObservationV2MaxRows {
		return fmt.Errorf("invalid inventory observation v2 envelope")
	}
	seenDevices := make(map[string]struct{}, len(fixture.Devices))
	seenInstances := make(map[string]struct{}, len(fixture.Devices))
	var previousDeviceID, previousInstanceID string
	for index, candidate := range fixture.Devices {
		if candidate.Revision == 0 || candidate.Generation == 0 || candidate.HeartbeatSequence == 0 ||
			candidate.Revision > forgeDeviceInventoryObservationV2MaxSafe ||
			candidate.Generation > forgeDeviceInventoryObservationV2MaxSafe ||
			candidate.HeartbeatSequence > forgeDeviceInventoryObservationV2MaxSafe ||
			!validForgeDeviceInventoryObservationV2InstanceID(candidate.InstanceID) ||
			!validForgeDeviceInventoryObservationV2Device(candidate.Device, fixture.Owner) {
			return fmt.Errorf("invalid inventory observation v2 candidate %d", index)
		}
		if index > 0 && (previousDeviceID > candidate.Device.DeviceID ||
			(previousDeviceID == candidate.Device.DeviceID && previousInstanceID >= candidate.InstanceID)) {
			return fmt.Errorf("inventory v2 devices must be sorted by device and instance")
		}
		if _, exists := seenDevices[candidate.Device.DeviceID]; exists {
			return fmt.Errorf("duplicate inventory v2 device %q", candidate.Device.DeviceID)
		}
		if _, exists := seenInstances[candidate.InstanceID]; exists {
			return fmt.Errorf("duplicate inventory v2 instance %q", candidate.InstanceID)
		}
		seenDevices[candidate.Device.DeviceID] = struct{}{}
		seenInstances[candidate.InstanceID] = struct{}{}
		previousDeviceID, previousInstanceID = candidate.Device.DeviceID, candidate.InstanceID
	}
	return nil
}

func validForgeDeviceInventoryObservationV2DeviceBody(value forgeDeviceInventoryObservationV2Device) bool {
	if !validForgeDeviceInventoryObservationV2DeviceID(value.DeviceID) ||
		!validForgeDeviceInventoryObservationV2Approval(value.ApprovalState) ||
		!validForgeDeviceInventoryObservationV2Cordon(value.CordonState) ||
		!validForgeDeviceInventoryObservationV2Reservation(value.ReservationState) ||
		!validForgeDeviceInventoryObservationV2Liveness(value.Liveness) ||
		!validForgeDeviceInventoryObservationV2Lease(value.SnapshotObservedAtMS, value.LeaseExpiresAtMS) ||
		!validForgeDeviceInventoryObservationV2CanonicalTag(value.OS) ||
		!validForgeDeviceInventoryObservationV2CanonicalTag(value.Architecture) ||
		value.AvailableCPUCores > forgeDeviceInventoryV2MaxCPU ||
		value.AvailableMemoryBytes > forgeDeviceInventoryObservationV2MaxSafe ||
		value.AvailableStorageBytes > forgeDeviceInventoryObservationV2MaxSafe ||
		!validForgeDeviceInventoryObservationV2Runtimes(value.Runtimes) ||
		len(value.GPUs) > forgeDeviceInventoryV2MaxGPUCount ||
		len(value.DataResidencyZones) != 0 || value.TrustZone != "unknown" ||
		len(value.SandboxLevels) != 0 || value.ConcurrencyLimit != 0 || value.ActiveConcurrency != 0 {
		return false
	}
	var totalAvailable uint64
	var previousID string
	seen := make(map[string]struct{}, len(value.GPUs))
	for index, gpu := range value.GPUs {
		if !validForgeDeviceInventoryObservationV2DeviceID(gpu.ID) ||
			!validForgeDeviceInventoryObservationV2Label(gpu.Vendor) || gpu.MemoryBytes == 0 ||
			gpu.MemoryBytes > forgeDeviceInventoryObservationV2MaxSafe ||
			gpu.AvailableMemoryBytes > gpu.MemoryBytes ||
			gpu.AvailableMemoryBytes > forgeDeviceInventoryObservationV2MaxSafe ||
			(index > 0 && previousID >= gpu.ID) {
			return false
		}
		if _, exists := seen[gpu.ID]; exists {
			return false
		}
		if totalAvailable > forgeDeviceInventoryObservationV2MaxSafe-gpu.AvailableMemoryBytes {
			return false
		}
		totalAvailable += gpu.AvailableMemoryBytes
		seen[gpu.ID] = struct{}{}
		previousID = gpu.ID
	}
	return true
}

func validForgeDeviceInventoryObservationV2Device(value forgeDeviceInventoryObservationV2Device, owner forgeDeviceInventoryObservationV2Owner) bool {
	return value.Owner == owner && validForgeDeviceInventoryObservationV2Owner(value.Owner) && validForgeDeviceInventoryObservationV2DeviceBody(value)
}

func validForgeDeviceInventoryObservationV2Owner(value forgeDeviceInventoryObservationV2Owner) bool {
	return validForgeDeviceInventoryObservationV2OwnerPart(value.Issuer) &&
		validForgeDeviceInventoryObservationV2OwnerPart(value.Subject) &&
		validForgeDeviceInventoryObservationV2OwnerPart(value.TenantID)
}

func validForgeDeviceInventoryObservationV2OwnerPart(value string) bool {
	return utf8.ValidString(value) && value != "" && len(value) <= forgeDeviceInventoryObservationV2MaxOwner &&
		strings.TrimSpace(value) == value && !strings.ContainsFunc(value, unicode.IsControl)
}

func validForgeDeviceInventoryObservationV2DeviceID(value string) bool {
	if len(value) == 0 || len(value) > forgeDeviceInventoryObservationV2MaxID {
		return false
	}
	for index, char := range value {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' {
			continue
		}
		if index == 0 || !strings.ContainsRune("._:-", char) {
			return false
		}
	}
	return true
}

func validForgeDeviceInventoryObservationV2InstanceID(value string) bool {
	if len(value) == 0 || len(value) > forgeDeviceInventoryObservationV2MaxID {
		return false
	}
	for index, char := range value {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' {
			continue
		}
		if index == 0 || !strings.ContainsRune("._:+/-", char) {
			return false
		}
	}
	return true
}

func validForgeDeviceInventoryObservationV2CanonicalTag(value string) bool {
	if value == "" || len(value) > forgeDeviceInventoryV2MaxTagBytes || value != strings.ToLower(value) {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || strings.ContainsRune("._-+", char)) {
			return false
		}
	}
	return true
}

func validForgeDeviceInventoryObservationV2Label(value string) bool {
	return utf8.ValidString(value) && value != "" && len(value) <= forgeDeviceInventoryV2MaxTagBytes &&
		strings.TrimSpace(value) == value && !strings.ContainsFunc(value, unicode.IsControl)
}

func validForgeDeviceInventoryObservationV2Runtimes(values []string) bool {
	if len(values) > forgeDeviceInventoryV2MaxArray {
		return false
	}
	for index, value := range values {
		if !validForgeDeviceInventoryObservationV2CanonicalTag(value) || (index > 0 && values[index-1] >= value) {
			return false
		}
	}
	return true
}

func validForgeDeviceInventoryObservationV2Lease(observed, expires uint64) bool {
	if expires < observed {
		return false
	}
	ttl := expires - observed
	return ttl >= forgeDeviceInventoryV2MinLeaseTTL && ttl <= forgeDeviceInventoryV2MaxLeaseTTL && expires <= forgeDeviceInventoryObservationV2MaxSafe
}

func validForgeDeviceInventoryObservationV2Approval(value string) bool {
	switch value {
	case "approved", "pending", "revoked":
		return true
	default:
		return false
	}
}

func validForgeDeviceInventoryObservationV2Cordon(value string) bool {
	return value == "clear" || value == "cordoned"
}

func validForgeDeviceInventoryObservationV2Reservation(value string) bool {
	return value == "none" || value == "reserved"
}

func validForgeDeviceInventoryObservationV2Liveness(value string) bool {
	return value == "online" || value == "offline"
}

func rejectForgeDeviceInventoryObservationV2DuplicateKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var walk func() error
	walk = func() error {
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
			return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
		}
	}
	if err := walk(); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	return nil
}

func TestForgeDeviceInventoryObservationV2ReceiverContract(t *testing.T) {
	var fixture forgeDeviceInventoryObservationV2FixtureType
	if err := decodeForgeDeviceInventoryObservationV2(forgeDeviceInventoryObservationV2Fixture, &fixture); err != nil {
		t.Fatalf("decode inventory observation v2 fixture: %v", err)
	}
	if len(fixture.Devices) != 2 || fixture.Devices[0].Device.DeviceID != "device-a" ||
		fixture.Devices[1].Device.DeviceID != "device-b" || fixture.Devices[0].Device.ReservationState != "reserved" ||
		fixture.Devices[0].Device.Liveness != "online" || fixture.Devices[1].Device.Liveness != "offline" ||
		len(fixture.Devices[0].Device.GPUs) != 2 || fixture.Devices[0].Device.GPUs[0].ID != "gpu-a" ||
		fixture.Devices[0].Device.GPUs[1].ID != "gpu-b" {
		t.Fatalf("inventory v2 rows are not canonical: %#v", fixture.Devices)
	}
	if fixture.ExecutionAuthorized || fixture.ReservationCreated || fixture.DispatchPerformed {
		t.Fatal("inventory observation v2 unexpectedly grants authority")
	}
}

func TestForgeDeviceInventoryObservationV2ReceiverRejectsMutation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, map[string]json.RawMessage)
	}{
		{name: "unknown", mutate: func(_ *testing.T, root map[string]json.RawMessage) {
			root["unexpected"] = json.RawMessage(`true`)
		}},
		{name: "foreign_owner", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDevice(t, root, func(device map[string]json.RawMessage) {
				device["owner"] = json.RawMessage(`{"issuer":"issuer","subject":"foreign","tenant_id":"tenant"}`)
			})
		}},
		{name: "duplicate_rows", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			var devices []json.RawMessage
			if err := json.Unmarshal(root["devices"], &devices); err != nil {
				t.Fatal(err)
			}
			devices = append(devices, devices[0])
			root["devices"] = mustForgeDeviceInventoryObservationV2JSON(t, devices)
		}},
		{name: "unordered_rows", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			var devices []json.RawMessage
			if err := json.Unmarshal(root["devices"], &devices); err != nil {
				t.Fatal(err)
			}
			devices[0], devices[1] = devices[1], devices[0]
			root["devices"] = mustForgeDeviceInventoryObservationV2JSON(t, devices)
		}},
		{name: "revision_zero", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDevice(t, root, func(device map[string]json.RawMessage) {
				device["revision"] = json.RawMessage(`0`)
			})
		}},
		{name: "revision_unsafe", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDevice(t, root, func(device map[string]json.RawMessage) {
				device["revision"] = json.RawMessage(`9007199254740992`)
			})
		}},
		{name: "generation_unsafe", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDevice(t, root, func(device map[string]json.RawMessage) {
				device["generation"] = json.RawMessage(`9007199254740992`)
			})
		}},
		{name: "heartbeat_unsafe", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDevice(t, root, func(device map[string]json.RawMessage) {
				device["heartbeat_sequence"] = json.RawMessage(`9007199254740992`)
			})
		}},
		{name: "reservation_unknown", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDeviceField(t, root, "reservation_state", json.RawMessage(`"unknown"`))
		}},
		{name: "liveness_unknown", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDeviceField(t, root, "liveness", json.RawMessage(`"unknown"`))
		}},
		{name: "lease_too_short", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDeviceField(t, root, "lease_expires_at_ms", json.RawMessage(`100999`))
		}},
		{name: "lease_too_long", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDeviceField(t, root, "lease_expires_at_ms", json.RawMessage(`800001`))
		}},
		{name: "lease_backward", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDeviceField(t, root, "lease_expires_at_ms", json.RawMessage(`99999`))
		}},
		{name: "canonical_os", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDeviceField(t, root, "os", json.RawMessage(`"Linux"`))
		}},
		{name: "canonical_runtime", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDeviceRuntimes(t, root, []string{"Go", "rust"})
		}},
		{name: "runtime_unordered", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDeviceRuntimes(t, root, []string{"rust", "go"})
		}},
		{name: "gpu_unordered", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDeviceGPUs(t, root, func(gpus []map[string]json.RawMessage) {
				gpus[0], gpus[1] = gpus[1], gpus[0]
			})
		}},
		{name: "gpu_memory_invalid", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDeviceGPUs(t, root, func(gpus []map[string]json.RawMessage) {
				gpus[0]["available_memory_bytes"] = json.RawMessage(`17179869185`)
			})
		}},
		{name: "gpu_memory_zero", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDeviceGPUs(t, root, func(gpus []map[string]json.RawMessage) {
				gpus[0]["memory_bytes"] = json.RawMessage(`0`)
			})
		}},
		{name: "gpu_aggregate_unsafe", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDeviceGPUs(t, root, func(gpus []map[string]json.RawMessage) {
				for _, gpu := range gpus {
					gpu["memory_bytes"] = json.RawMessage(`5000000000000000`)
					gpu["available_memory_bytes"] = json.RawMessage(`5000000000000000`)
				}
			})
		}},
		{name: "owner_unverified_false", mutate: func(_ *testing.T, root map[string]json.RawMessage) {
			root["owner_declaration_unverified"] = json.RawMessage(`false`)
		}},
		{name: "inventory_unverified_false", mutate: func(_ *testing.T, root map[string]json.RawMessage) {
			root["inventory_declarations_unverified"] = json.RawMessage(`false`)
		}},
		{name: "authority_execution", mutate: func(_ *testing.T, root map[string]json.RawMessage) {
			root["execution_authorized"] = json.RawMessage(`true`)
		}},
		{name: "authority_reservation", mutate: func(_ *testing.T, root map[string]json.RawMessage) {
			root["reservation_created"] = json.RawMessage(`true`)
		}},
		{name: "authority_dispatch", mutate: func(_ *testing.T, root map[string]json.RawMessage) {
			root["dispatch_performed"] = json.RawMessage(`true`)
		}},
		{name: "selected_claim", mutate: func(_ *testing.T, root map[string]json.RawMessage) {
			root["selected_device_id"] = json.RawMessage(`"device-a"`)
		}},
		{name: "schedulable_claim", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationV2FirstDeviceField(t, root, "schedulable", json.RawMessage(`true`))
		}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var root map[string]json.RawMessage
			if err := json.Unmarshal(forgeDeviceInventoryObservationV2Fixture, &root); err != nil {
				t.Fatal(err)
			}
			testCase.mutate(t, root)
			raw := mustForgeDeviceInventoryObservationV2JSON(t, root)
			var fixture forgeDeviceInventoryObservationV2FixtureType
			if err := decodeForgeDeviceInventoryObservationV2(raw, &fixture); err == nil {
				t.Fatal("mutated inventory observation v2 unexpectedly accepted")
			}
		})
	}

	t.Run("duplicate_field", func(t *testing.T) {
		duplicate := bytes.TrimSpace(forgeDeviceInventoryObservationV2Fixture)
		duplicate = append(bytes.TrimSuffix(duplicate, []byte("}")), []byte(`,"schema_version":"forge.device-inventory-observation/v2"}`)...)
		var fixture forgeDeviceInventoryObservationV2FixtureType
		if err := decodeForgeDeviceInventoryObservationV2(duplicate, &fixture); err == nil {
			t.Fatal("duplicate field unexpectedly accepted")
		}
	})
}

func mutateForgeDeviceInventoryObservationV2FirstDevice(t *testing.T, root map[string]json.RawMessage, mutate func(map[string]json.RawMessage)) {
	t.Helper()
	var devices []map[string]json.RawMessage
	if err := json.Unmarshal(root["devices"], &devices); err != nil {
		t.Fatal(err)
	}
	mutate(devices[0])
	root["devices"] = mustForgeDeviceInventoryObservationV2JSON(t, devices)
}

func mutateForgeDeviceInventoryObservationV2FirstDeviceField(t *testing.T, root map[string]json.RawMessage, key string, value json.RawMessage) {
	t.Helper()
	mutateForgeDeviceInventoryObservationV2FirstDevice(t, root, func(device map[string]json.RawMessage) {
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(device["device"], &nested); err != nil {
			t.Fatal(err)
		}
		nested[key] = value
		device["device"] = mustForgeDeviceInventoryObservationV2JSON(t, nested)
	})
}

func mutateForgeDeviceInventoryObservationV2FirstDeviceRuntimes(t *testing.T, root map[string]json.RawMessage, runtimes []string) {
	t.Helper()
	mutateForgeDeviceInventoryObservationV2FirstDevice(t, root, func(device map[string]json.RawMessage) {
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(device["device"], &nested); err != nil {
			t.Fatal(err)
		}
		nested["runtimes"] = mustForgeDeviceInventoryObservationV2JSON(t, runtimes)
		device["device"] = mustForgeDeviceInventoryObservationV2JSON(t, nested)
	})
}

func mutateForgeDeviceInventoryObservationV2FirstDeviceGPUs(t *testing.T, root map[string]json.RawMessage, mutate func([]map[string]json.RawMessage)) {
	t.Helper()
	mutateForgeDeviceInventoryObservationV2FirstDevice(t, root, func(device map[string]json.RawMessage) {
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(device["device"], &nested); err != nil {
			t.Fatal(err)
		}
		var gpus []map[string]json.RawMessage
		if err := json.Unmarshal(nested["gpus"], &gpus); err != nil {
			t.Fatal(err)
		}
		mutate(gpus)
		nested["gpus"] = mustForgeDeviceInventoryObservationV2JSON(t, gpus)
		device["device"] = mustForgeDeviceInventoryObservationV2JSON(t, nested)
	})
}

func mustForgeDeviceInventoryObservationV2JSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
