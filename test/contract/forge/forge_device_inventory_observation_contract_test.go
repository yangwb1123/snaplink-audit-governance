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
	"unicode"
	"unicode/utf8"
)

// This receiver-side contract decodes the canonical offline inventory
// observation. It consumes only caller-declared values. It does not
// authenticate an owner, make inventory authoritative, select a device,
// report schedulability, reserve capacity, or authorize execution.
//
//go:embed testdata/forge-device-inventory-observation-v1.json
var forgeDeviceInventoryObservationFixture []byte

const (
	forgeDeviceInventoryObservationSchema   = "forge.device-inventory-observation/v1"
	forgeDeviceInventoryObservationMode     = "offline_static_only"
	forgeDeviceInventoryObservationNotice   = "Every owner, instance, state, timestamp, resource, residency, trust, sandbox, and concurrency value is an unverified caller declaration. This read-only observation selects no target and grants no execution authority."
	forgeDeviceInventoryObservationMaxSafe  = uint64(9007199254740991)
	forgeDeviceInventoryObservationMaxRows  = 128
	forgeDeviceInventoryObservationMaxID    = 128
	forgeDeviceInventoryObservationMaxOwner = 512
	forgeDeviceInventoryObservationMaxItems = 32
)

type forgeDeviceInventoryObservationOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeDeviceInventoryObservationGPU struct {
	Present     bool   `json:"present"`
	MemoryBytes uint64 `json:"memory_bytes"`
	Runtime     string `json:"runtime"`
}

type forgeDeviceInventoryObservationDevice struct {
	DeviceID              string                               `json:"device_id"`
	Owner                 forgeDeviceInventoryObservationOwner `json:"owner"`
	ApprovalState         string                               `json:"approval_state"`
	CordonState           string                               `json:"cordon_state"`
	Liveness              string                               `json:"liveness"`
	SnapshotObservedAtMS  uint64                               `json:"snapshot_observed_at_ms"`
	LeaseExpiresAtMS      uint64                               `json:"lease_expires_at_ms"`
	OS                    string                               `json:"os"`
	Architecture          string                               `json:"architecture"`
	AvailableCPUCores     uint64                               `json:"available_cpu_cores"`
	AvailableMemoryBytes  uint64                               `json:"available_memory_bytes"`
	AvailableStorageBytes uint64                               `json:"available_storage_bytes"`
	Runtimes              []string                             `json:"runtimes"`
	GPU                   forgeDeviceInventoryObservationGPU   `json:"gpu"`
	DataResidencyZones    []string                             `json:"data_residency_zones"`
	TrustZone             string                               `json:"trust_zone"`
	SandboxLevels         []string                             `json:"sandbox_levels"`
	ConcurrencyLimit      uint64                               `json:"concurrency_limit"`
	ActiveConcurrency     uint64                               `json:"active_concurrency"`
}

type forgeDeviceInventoryObservationCandidate struct {
	InstanceID string                                `json:"instance_id"`
	Device     forgeDeviceInventoryObservationDevice `json:"device"`
}

type forgeDeviceInventoryObservationFixtureType struct {
	SchemaVersion                   string                                     `json:"schema_version"`
	EvaluationMode                  string                                     `json:"evaluation_mode"`
	EvaluatedAtMS                   uint64                                     `json:"evaluated_at_ms"`
	Owner                           forgeDeviceInventoryObservationOwner       `json:"owner_declaration"`
	OwnerDeclarationUnverified      bool                                       `json:"owner_declaration_unverified"`
	InventoryDeclarationsUnverified bool                                       `json:"inventory_declarations_unverified"`
	Notice                          string                                     `json:"notice"`
	Devices                         []forgeDeviceInventoryObservationCandidate `json:"devices"`
	ExecutionAuthorized             bool                                       `json:"execution_authorized"`
	ReservationCreated              bool                                       `json:"reservation_created"`
	DispatchPerformed               bool                                       `json:"dispatch_performed"`
}

func decodeForgeDeviceInventoryObservation(raw []byte, out *forgeDeviceInventoryObservationFixtureType) error {
	if err := rejectForgeDeviceInventoryObservationDuplicateKeys(raw); err != nil {
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
	return validateForgeDeviceInventoryObservation(out)
}

func validateForgeDeviceInventoryObservation(fixture *forgeDeviceInventoryObservationFixtureType) error {
	if fixture.SchemaVersion != forgeDeviceInventoryObservationSchema ||
		fixture.EvaluationMode != forgeDeviceInventoryObservationMode ||
		fixture.EvaluatedAtMS == 0 || fixture.EvaluatedAtMS > forgeDeviceInventoryObservationMaxSafe ||
		fixture.Owner != (forgeDeviceInventoryObservationOwner{Issuer: "https://id.example", Subject: "user-1", TenantID: "tenant-1"}) ||
		!validForgeDeviceInventoryObservationOwner(fixture.Owner) ||
		!fixture.OwnerDeclarationUnverified || !fixture.InventoryDeclarationsUnverified ||
		fixture.Notice != forgeDeviceInventoryObservationNotice || fixture.ExecutionAuthorized ||
		fixture.ReservationCreated || fixture.DispatchPerformed || len(fixture.Devices) != 2 ||
		len(fixture.Devices) > forgeDeviceInventoryObservationMaxRows {
		return fmt.Errorf("invalid inventory observation envelope")
	}
	seenDevices := make(map[string]struct{}, len(fixture.Devices))
	seenInstances := make(map[string]struct{}, len(fixture.Devices))
	for index, candidate := range fixture.Devices {
		if !validForgeDeviceInventoryObservationInstanceID(candidate.InstanceID) ||
			!validForgeDeviceInventoryObservationDevice(candidate.Device) || candidate.Device.Owner != fixture.Owner {
			return fmt.Errorf("invalid inventory observation candidate %d", index)
		}
		if _, exists := seenDevices[candidate.Device.DeviceID]; exists {
			return fmt.Errorf("duplicate inventory device %q", candidate.Device.DeviceID)
		}
		if _, exists := seenInstances[candidate.InstanceID]; exists {
			return fmt.Errorf("duplicate inventory instance %q", candidate.InstanceID)
		}
		if index > 0 && fixture.Devices[index-1].Device.DeviceID >= candidate.Device.DeviceID {
			return fmt.Errorf("inventory devices must be sorted by device ID")
		}
		seenDevices[candidate.Device.DeviceID] = struct{}{}
		seenInstances[candidate.InstanceID] = struct{}{}
	}
	return nil
}

func validForgeDeviceInventoryObservationDevice(value forgeDeviceInventoryObservationDevice) bool {
	return validForgeDeviceInventoryObservationDeviceID(value.DeviceID) &&
		validForgeDeviceInventoryObservationOwner(value.Owner) &&
		validForgeDeviceInventoryObservationApproval(value.ApprovalState) &&
		validForgeDeviceInventoryObservationCordon(value.CordonState) &&
		validForgeDeviceInventoryObservationLiveness(value.Liveness) &&
		value.SnapshotObservedAtMS <= forgeDeviceInventoryObservationMaxSafe &&
		value.LeaseExpiresAtMS <= forgeDeviceInventoryObservationMaxSafe &&
		validForgeDeviceInventoryObservationToken(value.OS) &&
		validForgeDeviceInventoryObservationToken(value.Architecture) &&
		value.AvailableCPUCores <= uint64(^uint32(0)) &&
		value.AvailableMemoryBytes <= forgeDeviceInventoryObservationMaxSafe &&
		value.AvailableStorageBytes <= forgeDeviceInventoryObservationMaxSafe &&
		validForgeDeviceInventoryObservationTokens(value.Runtimes, forgeDeviceInventoryObservationMaxItems, validForgeDeviceInventoryObservationToken) &&
		validForgeDeviceInventoryObservationGPU(value.GPU) &&
		validForgeDeviceInventoryObservationTokens(value.DataResidencyZones, forgeDeviceInventoryObservationMaxItems, validForgeDeviceInventoryObservationZone) &&
		validForgeDeviceInventoryObservationTrustZone(value.TrustZone) &&
		validForgeDeviceInventoryObservationTokens(value.SandboxLevels, forgeDeviceInventoryObservationMaxItems, validForgeDeviceInventoryObservationSandbox) &&
		value.ConcurrencyLimit <= uint64(^uint16(0)) && value.ActiveConcurrency <= uint64(^uint16(0)) &&
		value.ActiveConcurrency <= value.ConcurrencyLimit
}

func validForgeDeviceInventoryObservationGPU(value forgeDeviceInventoryObservationGPU) bool {
	if value.Present {
		return value.MemoryBytes <= forgeDeviceInventoryObservationMaxSafe &&
			(value.Runtime == "" || validForgeDeviceInventoryObservationToken(value.Runtime))
	}
	return value.MemoryBytes == 0 && value.Runtime == ""
}

func validForgeDeviceInventoryObservationOwner(value forgeDeviceInventoryObservationOwner) bool {
	return validForgeDeviceInventoryObservationOwnerPart(value.Issuer) &&
		validForgeDeviceInventoryObservationOwnerPart(value.Subject) &&
		validForgeDeviceInventoryObservationOwnerPart(value.TenantID)
}

func validForgeDeviceInventoryObservationOwnerPart(value string) bool {
	return utf8.ValidString(value) && value != "" && len(value) <= forgeDeviceInventoryObservationMaxOwner &&
		strings.TrimSpace(value) == value && !strings.ContainsFunc(value, unicode.IsControl)
}

func validForgeDeviceInventoryObservationDeviceID(value string) bool {
	return validForgeDeviceInventoryObservationIdentifier(value, false)
}

func validForgeDeviceInventoryObservationInstanceID(value string) bool {
	return validForgeDeviceInventoryObservationIdentifier(value, true)
}

func validForgeDeviceInventoryObservationIdentifier(value string, allowPlusSlash bool) bool {
	if len(value) == 0 || len(value) > forgeDeviceInventoryObservationMaxID {
		return false
	}
	for index, char := range value {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' {
			continue
		}
		if index == 0 {
			return false
		}
		if allowPlusSlash && strings.ContainsRune("._:+/-", char) {
			continue
		}
		if !allowPlusSlash && strings.ContainsRune("._:-", char) {
			continue
		}
		return false
	}
	return true
}

func validForgeDeviceInventoryObservationToken(value string) bool {
	if len(value) == 0 || len(value) > forgeDeviceInventoryObservationMaxID {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("._:+/-", char)) {
			return false
		}
	}
	return true
}

func validForgeDeviceInventoryObservationZone(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("._-", char)) {
			return false
		}
	}
	return true
}

func validForgeDeviceInventoryObservationTokens(values []string, maximum int, valid func(string) bool) bool {
	if len(values) > maximum {
		return false
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !valid(value) {
			return false
		}
		if _, exists := seen[value]; exists {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func validForgeDeviceInventoryObservationApproval(value string) bool {
	switch value {
	case "approved", "pending", "revoked", "unknown":
		return true
	default:
		return false
	}
}

func validForgeDeviceInventoryObservationCordon(value string) bool {
	switch value {
	case "clear", "cordoned", "unknown":
		return true
	default:
		return false
	}
}

func validForgeDeviceInventoryObservationLiveness(value string) bool {
	switch value {
	case "online", "offline", "unknown":
		return true
	default:
		return false
	}
}

func validForgeDeviceInventoryObservationTrustZone(value string) bool {
	switch value {
	case "untrusted", "low", "standard", "high", "restricted", "unknown":
		return true
	default:
		return false
	}
}

func validForgeDeviceInventoryObservationSandbox(value string) bool {
	switch value {
	case "process", "container", "microvm":
		return true
	default:
		return false
	}
}

func rejectForgeDeviceInventoryObservationDuplicateKeys(raw []byte) error {
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

func TestForgeDeviceInventoryObservationReceiverContract(t *testing.T) {
	var fixture forgeDeviceInventoryObservationFixtureType
	if err := decodeForgeDeviceInventoryObservation(forgeDeviceInventoryObservationFixture, &fixture); err != nil {
		t.Fatalf("decode inventory observation fixture: %v", err)
	}
	if fixture.Devices[0].Device.DeviceID != "device-a" || fixture.Devices[0].InstanceID != "runner-a" ||
		fixture.Devices[1].Device.DeviceID != "device-b" || fixture.Devices[1].InstanceID != "runner-b" {
		t.Fatalf("inventory rows are not canonical: %#v", fixture.Devices)
	}
	if fixture.Devices[0].Device.ApprovalState != "approved" || fixture.Devices[1].Device.ApprovalState != "pending" {
		t.Fatalf("fixture approval declarations changed: %#v", fixture.Devices)
	}
	// The v1 observation has no selected, schedulable, reservation, or
	// dispatch result. The three authority bits are fixed false above.
	if fixture.ExecutionAuthorized || fixture.ReservationCreated || fixture.DispatchPerformed {
		t.Fatal("inventory observation unexpectedly grants authority")
	}
}

func TestForgeDeviceInventoryObservationReceiverRejectsMutation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, map[string]json.RawMessage)
	}{
		{name: "unknown", mutate: func(_ *testing.T, root map[string]json.RawMessage) {
			root["unexpected"] = json.RawMessage(`true`)
		}},
		{name: "foreign_owner", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationFirstDevice(t, root, func(_ map[string]json.RawMessage, owner map[string]json.RawMessage) {
				owner["subject"] = json.RawMessage(`"foreign-user"`)
			})
		}},
		{name: "duplicate_rows", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			var devices []json.RawMessage
			if err := json.Unmarshal(root["devices"], &devices); err != nil {
				t.Fatal(err)
			}
			devices = append(devices, devices[0])
			root["devices"] = mustForgeDeviceInventoryObservationJSON(t, devices)
		}},
		{name: "unordered_rows", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			var devices []json.RawMessage
			if err := json.Unmarshal(root["devices"], &devices); err != nil {
				t.Fatal(err)
			}
			devices[0], devices[1] = devices[1], devices[0]
			root["devices"] = mustForgeDeviceInventoryObservationJSON(t, devices)
		}},
		{name: "invalid_bounds", mutate: func(t *testing.T, root map[string]json.RawMessage) {
			mutateForgeDeviceInventoryObservationFirstDevice(t, root, func(device map[string]json.RawMessage, _ map[string]json.RawMessage) {
				device["available_memory_bytes"] = json.RawMessage(`9007199254740992`)
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
			mutateForgeDeviceInventoryObservationFirstDevice(t, root, func(device map[string]json.RawMessage, _ map[string]json.RawMessage) {
				device["schedulable"] = json.RawMessage(`true`)
			})
		}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var root map[string]json.RawMessage
			if err := json.Unmarshal(forgeDeviceInventoryObservationFixture, &root); err != nil {
				t.Fatal(err)
			}
			testCase.mutate(t, root)
			raw := mustForgeDeviceInventoryObservationJSON(t, root)
			var fixture forgeDeviceInventoryObservationFixtureType
			if err := decodeForgeDeviceInventoryObservation(raw, &fixture); err == nil {
				t.Fatal("mutated inventory observation unexpectedly accepted")
			}
		})
	}

	t.Run("duplicate_field", func(t *testing.T) {
		duplicate := bytes.TrimSpace(forgeDeviceInventoryObservationFixture)
		duplicate = append(bytes.TrimSuffix(duplicate, []byte("}")), []byte(`,"schema_version":"forge.device-inventory-observation/v1"}`)...)
		var fixture forgeDeviceInventoryObservationFixtureType
		if err := decodeForgeDeviceInventoryObservation(duplicate, &fixture); err == nil {
			t.Fatal("duplicate field unexpectedly accepted")
		}
	})
}

func mutateForgeDeviceInventoryObservationFirstDevice(t *testing.T, root map[string]json.RawMessage, mutate func(map[string]json.RawMessage, map[string]json.RawMessage)) {
	t.Helper()
	var devices []map[string]json.RawMessage
	if err := json.Unmarshal(root["devices"], &devices); err != nil {
		t.Fatal(err)
	}
	var device map[string]json.RawMessage
	if err := json.Unmarshal(devices[0]["device"], &device); err != nil {
		t.Fatal(err)
	}
	var owner map[string]json.RawMessage
	if err := json.Unmarshal(device["owner"], &owner); err != nil {
		t.Fatal(err)
	}
	mutate(device, owner)
	device["owner"] = mustForgeDeviceInventoryObservationJSON(t, owner)
	devices[0]["device"] = mustForgeDeviceInventoryObservationJSON(t, device)
	root["devices"] = mustForgeDeviceInventoryObservationJSON(t, devices)
}

func mustForgeDeviceInventoryObservationJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
