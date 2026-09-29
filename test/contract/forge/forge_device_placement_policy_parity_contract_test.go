package forge

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
)

const forgeDevicePlacementPolicyParityMaxSafeInteger uint64 = 9_007_199_254_740_991

//go:embed testdata/forge-device-placement-policy-parity-v1.json
var forgeDevicePlacementPolicyParityFixtureBytes []byte

type forgeDevicePlacementPolicyParityOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeDevicePlacementPolicyParityGPURequirement struct {
	Required       bool   `json:"required"`
	MinMemoryBytes uint64 `json:"min_memory_bytes"`
	Runtime        string `json:"runtime"`
}

type forgeDevicePlacementPolicyParityRequirements struct {
	OS                 string                                         `json:"os"`
	Architecture       string                                         `json:"architecture"`
	MinCPUCores        uint64                                         `json:"min_cpu_cores"`
	MinMemoryBytes     uint64                                         `json:"min_memory_bytes"`
	MinStorageBytes    uint64                                         `json:"min_storage_bytes"`
	Runtime            string                                         `json:"runtime"`
	GPU                forgeDevicePlacementPolicyParityGPURequirement `json:"gpu"`
	DataResidencyZones []string                                       `json:"data_residency_zones"`
	MinimumTrustZone   string                                         `json:"minimum_trust_zone"`
	SandboxFloor       string                                         `json:"sandbox_floor"`
	ConcurrencySlots   uint64                                         `json:"concurrency_slots"`
}

type forgeDevicePlacementPolicyParityGPUDeclaration struct {
	Present     bool   `json:"present"`
	MemoryBytes uint64 `json:"memory_bytes"`
	Runtime     string `json:"runtime"`
}

type forgeDevicePlacementPolicyParityDevice struct {
	DeviceID              string                                         `json:"device_id"`
	Owner                 forgeDevicePlacementPolicyParityOwner          `json:"owner"`
	ApprovalState         string                                         `json:"approval_state"`
	CordonState           string                                         `json:"cordon_state"`
	Liveness              string                                         `json:"liveness"`
	SnapshotObservedAtMS  uint64                                         `json:"snapshot_observed_at_ms"`
	LeaseExpiresAtMS      uint64                                         `json:"lease_expires_at_ms"`
	OS                    string                                         `json:"os"`
	Architecture          string                                         `json:"architecture"`
	AvailableCPUCores     uint64                                         `json:"available_cpu_cores"`
	AvailableMemoryBytes  uint64                                         `json:"available_memory_bytes"`
	AvailableStorageBytes uint64                                         `json:"available_storage_bytes"`
	Runtimes              []string                                       `json:"runtimes"`
	GPU                   forgeDevicePlacementPolicyParityGPUDeclaration `json:"gpu"`
	DataResidencyZones    []string                                       `json:"data_residency_zones"`
	TrustZone             string                                         `json:"trust_zone"`
	SandboxLevels         []string                                       `json:"sandbox_levels"`
	ConcurrencyLimit      uint64                                         `json:"concurrency_limit"`
	ActiveConcurrency     uint64                                         `json:"active_concurrency"`
}

type forgeDevicePlacementPolicyParityCandidate struct {
	InstanceID string                                 `json:"instance_id"`
	Device     forgeDevicePlacementPolicyParityDevice `json:"device"`
}

type forgeDevicePlacementPolicyParityExpected struct {
	DeviceID            string   `json:"device_id"`
	MatchesRequirements bool     `json:"matches_requirements"`
	ExclusionReasons    []string `json:"exclusion_reasons"`
}

type forgeDevicePlacementPolicyParityEnvelope struct {
	SchemaVersion    string                                       `json:"schema_version"`
	EvaluatedAtMS    uint64                                       `json:"evaluated_at_ms"`
	MaxSnapshotAgeMS uint64                                       `json:"max_snapshot_age_ms"`
	Owner            forgeDevicePlacementPolicyParityOwner        `json:"owner"`
	Requirements     forgeDevicePlacementPolicyParityRequirements `json:"requirements"`
	Candidates       []forgeDevicePlacementPolicyParityCandidate  `json:"candidates"`
	Expected         []forgeDevicePlacementPolicyParityExpected   `json:"expected"`
}

func TestForgeDevicePlacementPolicyParityReceiver(t *testing.T) {
	fixture, err := decodeForgeDevicePlacementPolicyParity(forgeDevicePlacementPolicyParityFixtureBytes)
	if err != nil {
		t.Fatalf("decode placement policy parity fixture: %v", err)
	}
	if err := validateForgeDevicePlacementPolicyParity(fixture); err != nil {
		t.Fatalf("invalid placement policy parity fixture: %v", err)
	}
}

func TestForgeDevicePlacementPolicyParityRejectsWireAuthorityAndBindingDrift(t *testing.T) {
	base := bytes.TrimSpace(forgeDevicePlacementPolicyParityFixtureBytes)
	mutations := map[string][]byte{
		"unknown":   append(bytes.TrimSuffix(base, []byte("}")), []byte(`,"authority":{"execution_authorized":true}}`)...),
		"duplicate": append(bytes.TrimSuffix(base, []byte("}")), []byte(`,"schema_version":"forge.device-placement-policy-parity-test/v1"}`)...),
		"trailing":  append(base, []byte(" {}")...),
	}
	for name, mutation := range mutations {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeForgeDevicePlacementPolicyParity(mutation); err == nil {
				t.Fatal("wire or authority drift was accepted")
			}
		})
	}
	fixture, err := decodeForgeDevicePlacementPolicyParity(base)
	if err != nil {
		t.Fatal(err)
	}
	lifecycleMutations := map[string]func(*forgeDevicePlacementPolicyParityEnvelope){
		"root_owner_drift": func(value *forgeDevicePlacementPolicyParityEnvelope) {
			value.Owner.Subject = "user-foreign"
		},
		"candidate_owner_drift": func(value *forgeDevicePlacementPolicyParityEnvelope) {
			value.Candidates[1].Device.Owner.Subject = "user-foreign"
		},
		"candidate_instance_binding": func(value *forgeDevicePlacementPolicyParityEnvelope) {
			value.Candidates[0].InstanceID = ""
		},
		"candidate_device_binding": func(value *forgeDevicePlacementPolicyParityEnvelope) {
			value.Candidates[0].Device.DeviceID = "candidate-foreign"
		},
		"expected_device_binding": func(value *forgeDevicePlacementPolicyParityEnvelope) {
			value.Expected[0].DeviceID = "candidate-foreign"
		},
		"expected_match_drift": func(value *forgeDevicePlacementPolicyParityEnvelope) {
			value.Expected[0].MatchesRequirements = false
		},
		"reason_drift": func(value *forgeDevicePlacementPolicyParityEnvelope) {
			value.Expected[1].ExclusionReasons[0] = "unexpected_reason"
		},
	}
	for name, mutate := range lifecycleMutations {
		t.Run(name, func(t *testing.T) {
			mutated := fixture
			mutated.Candidates = append([]forgeDevicePlacementPolicyParityCandidate(nil), fixture.Candidates...)
			mutated.Expected = append([]forgeDevicePlacementPolicyParityExpected(nil), fixture.Expected...)
			mutate(&mutated)
			if err := validateForgeDevicePlacementPolicyParity(mutated); err == nil {
				t.Fatal("placement policy binding drift was accepted")
			}
		})
	}
}

func decodeForgeDevicePlacementPolicyParity(raw []byte) (forgeDevicePlacementPolicyParityEnvelope, error) {
	if err := rejectDuplicateForgeDevicePlacementPolicyParityKeys(raw); err != nil {
		return forgeDevicePlacementPolicyParityEnvelope{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var fixture forgeDevicePlacementPolicyParityEnvelope
	if err := decoder.Decode(&fixture); err != nil {
		return forgeDevicePlacementPolicyParityEnvelope{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeDevicePlacementPolicyParityEnvelope{}, fmt.Errorf("trailing JSON")
		}
		return forgeDevicePlacementPolicyParityEnvelope{}, err
	}
	return fixture, nil
}

func validateForgeDevicePlacementPolicyParity(fixture forgeDevicePlacementPolicyParityEnvelope) error {
	if fixture.SchemaVersion != "forge.device-placement-policy-parity-test/v1" ||
		fixture.EvaluatedAtMS > forgeDevicePlacementPolicyParityMaxSafeInteger ||
		fixture.MaxSnapshotAgeMS == 0 ||
		!validForgeDevicePlacementPolicyParityOwner(fixture.Owner) ||
		len(fixture.Candidates) != 9 ||
		len(fixture.Expected) != 9 ||
		!reflect.DeepEqual(fixture.Requirements, forgeDevicePlacementPolicyParityRequirements{
			OS: "linux", Architecture: "x86_64", MinCPUCores: 4, MinMemoryBytes: 8192,
			MinStorageBytes: 4096, Runtime: "oci",
			GPU:                forgeDevicePlacementPolicyParityGPURequirement{},
			DataResidencyZones: []string{"us-west"}, MinimumTrustZone: "standard",
			SandboxFloor: "container", ConcurrencySlots: 1,
		}) {
		return fmt.Errorf("invalid policy parity envelope")
	}
	wantInstances := []string{"runner-a", "runner-b", "runner-c", "runner-d", "runner-e", "runner-f", "runner-g", "runner-h", "runner-i"}
	wantDevices := []string{"candidate-a", "candidate-b", "candidate-c", "candidate-d", "candidate-e", "candidate-f", "candidate-g", "candidate-h", "candidate-i"}
	wantMatches := []bool{true, false, false, false, false, false, false, false, true}
	wantReasons := [][]string{
		{},
		{"concurrency_capacity_insufficient", "cpu_cores_insufficient", "data_residency_zone_mismatch", "memory_insufficient", "runtime_missing", "sandbox_floor_unmet", "storage_insufficient", "trust_zone_below_minimum"},
		{"approval_pending"}, {"device_revoked"}, {"device_cordoned"}, {"declared_offline"},
		{"snapshot_declared_from_future"}, {"declared_lease_expired", "snapshot_stale"}, {},
	}
	seenDevices := map[string]bool{}
	seenInstances := map[string]bool{}
	for index, candidate := range fixture.Candidates {
		if candidate.InstanceID != wantInstances[index] ||
			candidate.Device.DeviceID != wantDevices[index] ||
			candidate.Device.Owner != fixture.Owner ||
			!validForgeDevicePlacementPolicyParityOwner(candidate.Device.Owner) ||
			seenDevices[candidate.Device.DeviceID] ||
			seenInstances[candidate.InstanceID] ||
			!validForgeDevicePlacementPolicyParityDevice(candidate.Device) {
			return fmt.Errorf("invalid candidate binding at index %d", index)
		}
		seenDevices[candidate.Device.DeviceID] = true
		seenInstances[candidate.InstanceID] = true
		expected := fixture.Expected[index]
		if expected.DeviceID != candidate.Device.DeviceID ||
			expected.MatchesRequirements != wantMatches[index] ||
			!reflect.DeepEqual(expected.ExclusionReasons, wantReasons[index]) {
			return fmt.Errorf("invalid expected result at index %d", index)
		}
	}
	return nil
}

func validForgeDevicePlacementPolicyParityDevice(device forgeDevicePlacementPolicyParityDevice) bool {
	return validForgeDevicePlacementPolicyParityIdentifier(device.DeviceID) &&
		device.SnapshotObservedAtMS <= forgeDevicePlacementPolicyParityMaxSafeInteger &&
		device.LeaseExpiresAtMS <= forgeDevicePlacementPolicyParityMaxSafeInteger &&
		device.AvailableCPUCores <= forgeDevicePlacementPolicyParityMaxSafeInteger &&
		device.AvailableMemoryBytes <= forgeDevicePlacementPolicyParityMaxSafeInteger &&
		device.AvailableStorageBytes <= forgeDevicePlacementPolicyParityMaxSafeInteger &&
		device.ConcurrencyLimit > 0 &&
		device.ActiveConcurrency <= device.ConcurrencyLimit &&
		len(device.Runtimes) > 0 &&
		len(device.DataResidencyZones) > 0 &&
		len(device.SandboxLevels) > 0
}

func validForgeDevicePlacementPolicyParityOwner(owner forgeDevicePlacementPolicyParityOwner) bool {
	return validForgeDevicePlacementPolicyParityOwnerPart(owner.Issuer) &&
		validForgeDevicePlacementPolicyParityOwnerPart(owner.Subject) &&
		validForgeDevicePlacementPolicyParityOwnerPart(owner.TenantID)
}

func validForgeDevicePlacementPolicyParityOwnerPart(value string) bool {
	return value != "" && len(value) <= 512 && strings.TrimSpace(value) == value &&
		!strings.ContainsAny(value, "\x00\r\n")
}

func validForgeDevicePlacementPolicyParityIdentifier(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for index, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') {
			continue
		}
		if index == 0 || !strings.ContainsRune("._:+/-", character) {
			return false
		}
	}
	return true
}

func rejectDuplicateForgeDevicePlacementPolicyParityKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanForgeDevicePlacementPolicyParityJSON(decoder); err != nil {
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

func scanForgeDevicePlacementPolicyParityJSON(decoder *json.Decoder) error {
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
			if err := scanForgeDevicePlacementPolicyParityJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := scanForgeDevicePlacementPolicyParityJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return fmt.Errorf("invalid JSON delimiter")
	}
}
