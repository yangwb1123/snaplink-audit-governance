package domain

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
)

// This receiver-side contract consumes the lossless v2 offline placement
// comparison. All owner, inventory, requirement, and decision fields remain
// caller declarations; this value does not authenticate, select, reserve,
// schedule, dispatch, execute, or publish Audit evidence.
//
//go:embed testdata/forge-device-inventory-placement-evaluation-v2.json
var forgeDeviceInventoryPlacementEvaluationV2Fixture []byte

const (
	forgeDeviceInventoryPlacementEvaluationV2Schema = "forge.device-inventory-placement-evaluation/v2"
	forgeDeviceInventoryPlacementEvaluationV2Mode   = "offline_static_only"
	forgeDeviceInventoryPlacementEvaluationV2Source = "forge.device-inventory-observation/v2"
	forgeDeviceInventoryPlacementEvaluationV2Notice = "Every owner, state, timestamp, resource, GPU, reservation, residency, trust, sandbox, and concurrency value is an unverified caller declaration. This read-only comparison selects no target and grants no execution authority."
	forgeDeviceInventoryPlacementEvaluationV2Safe   = uint64(9007199254740991)
)

type forgeDeviceInventoryPlacementEvaluationV2Authority struct {
	IdentityVerified       bool `json:"identity_verified"`
	HeartbeatPersisted     bool `json:"heartbeat_persisted"`
	InventoryAuthoritative bool `json:"inventory_authoritative"`
	PlacementSelected      bool `json:"placement_selected"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
}

type forgeDeviceInventoryPlacementEvaluationV2Expected struct {
	Revision                uint64   `json:"revision"`
	Generation              uint64   `json:"generation"`
	HeartbeatSequence       uint64   `json:"heartbeat_sequence"`
	DeviceID                string   `json:"device_id"`
	InstanceID              string   `json:"instance_id"`
	ReservationState        string   `json:"reservation_state"`
	GPUCount                uint64   `json:"gpu_count"`
	AvailableGPUMemoryBytes uint64   `json:"available_gpu_memory_bytes"`
	MatchesRequirements     bool     `json:"matches_requirements"`
	ExclusionReasons        []string `json:"exclusion_reasons"`
}

type forgeDeviceInventoryPlacementEvaluationV2 struct {
	SchemaVersion          string                                              `json:"schema_version"`
	EvaluationMode         string                                              `json:"evaluation_mode"`
	SourceSchemaVersion    string                                              `json:"source_schema_version"`
	EvaluationOwner        forgeDeviceInventoryObservationV2Owner              `json:"evaluation_owner"`
	EvaluatedAtMS          uint64                                              `json:"evaluated_at_ms"`
	Notice                 string                                              `json:"notice"`
	Requirements           forgeDeviceInventoryPlacementEvaluationRequirements `json:"requirements"`
	Observation            forgeDeviceInventoryObservationV2FixtureType        `json:"observation"`
	Expected               []forgeDeviceInventoryPlacementEvaluationV2Expected `json:"expected"`
	EligibleCandidateCount uint64                                              `json:"eligible_candidate_count"`
	SelectedDeviceID       *string                                             `json:"selected_device_id"`
	SelectedInstanceID     *string                                             `json:"selected_instance_id"`
	Authority              forgeDeviceInventoryPlacementEvaluationV2Authority  `json:"authority"`
}

func decodeForgeDeviceInventoryPlacementEvaluationV2(raw []byte) (forgeDeviceInventoryPlacementEvaluationV2, error) {
	if err := rejectForgeDeviceInventoryObservationV2DuplicateKeys(raw); err != nil {
		return forgeDeviceInventoryPlacementEvaluationV2{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var fixture forgeDeviceInventoryPlacementEvaluationV2
	if err := decoder.Decode(&fixture); err != nil {
		return forgeDeviceInventoryPlacementEvaluationV2{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return forgeDeviceInventoryPlacementEvaluationV2{}, errors.New("trailing JSON value")
		}
		return forgeDeviceInventoryPlacementEvaluationV2{}, err
	}
	return fixture, validateForgeDeviceInventoryPlacementEvaluationV2(fixture)
}

func validateForgeDeviceInventoryPlacementEvaluationV2(fixture forgeDeviceInventoryPlacementEvaluationV2) error {
	if fixture.SchemaVersion != forgeDeviceInventoryPlacementEvaluationV2Schema ||
		fixture.EvaluationMode != forgeDeviceInventoryPlacementEvaluationV2Mode ||
		fixture.SourceSchemaVersion != forgeDeviceInventoryPlacementEvaluationV2Source ||
		fixture.EvaluationOwner != (forgeDeviceInventoryObservationV2Owner{Issuer: "issuer", Subject: "user", TenantID: "tenant"}) ||
		fixture.EvaluatedAtMS != 200000 || fixture.EvaluatedAtMS > forgeDeviceInventoryPlacementEvaluationV2Safe ||
		fixture.Notice != forgeDeviceInventoryPlacementEvaluationV2Notice ||
		fixture.EligibleCandidateCount != 0 || fixture.SelectedDeviceID != nil || fixture.SelectedInstanceID != nil ||
		fixture.Authority != (forgeDeviceInventoryPlacementEvaluationV2Authority{}) {
		return errors.New("invalid v2 placement evaluation envelope")
	}
	if err := validateForgeDeviceInventoryObservationV2(&fixture.Observation); err != nil {
		return fmt.Errorf("invalid v2 placement observation: %w", err)
	}
	if fixture.Observation.SchemaVersion != forgeDeviceInventoryObservationV2Schema ||
		fixture.Observation.EvaluatedAtMS != fixture.EvaluatedAtMS ||
		fixture.Observation.Owner != fixture.EvaluationOwner {
		return errors.New("v2 placement observation binding drift")
	}
	requirements := fixture.Requirements
	if requirements.OS != "linux" || requirements.Architecture != "amd64" ||
		requirements.MinCPUCores != 4 || requirements.MinMemoryBytes != 4294967296 ||
		requirements.MinStorageBytes != 10737418240 || requirements.Runtime != "go" ||
		!requirements.GPU.Required || requirements.GPU.MinMemoryBytes != 4294967296 || requirements.GPU.Runtime != "" ||
		len(requirements.DataResidencyZones) != 1 || requirements.DataResidencyZones[0] != "us-west" ||
		requirements.MinimumTrustZone != "standard" || requirements.SandboxFloor != "container" || requirements.ConcurrencySlots != 1 {
		return errors.New("invalid v2 placement requirements")
	}
	if len(fixture.Expected) != 2 {
		return errors.New("invalid v2 placement decision count")
	}
	wantReasons := [][]string{
		{"concurrency_capacity_insufficient", "data_residency_zone_mismatch", "declared_lease_expired", "device_reserved", "sandbox_floor_unmet", "snapshot_stale", "trust_zone_unconfirmed"},
		{"approval_pending", "concurrency_capacity_insufficient", "data_residency_zone_mismatch", "declared_lease_expired", "declared_offline", "device_cordoned", "gpu_missing", "sandbox_floor_unmet", "snapshot_stale", "trust_zone_unconfirmed"},
	}
	wantIDs := []struct {
		device, instance, reservation                   string
		revision, generation, heartbeat, gpus, gpuBytes uint64
	}{
		{"device-a", "runner-a", "reserved", 1, 1, 1, 2, 17179869184},
		{"device-b", "runner-b", "none", 2, 2, 4, 0, 0},
	}
	for index, expected := range fixture.Expected {
		want := wantIDs[index]
		if expected.DeviceID != want.device || expected.InstanceID != want.instance || expected.ReservationState != want.reservation ||
			expected.Revision != want.revision || expected.Generation != want.generation || expected.HeartbeatSequence != want.heartbeat ||
			expected.GPUCount != want.gpus || expected.AvailableGPUMemoryBytes != want.gpuBytes || expected.MatchesRequirements ||
			len(expected.ExclusionReasons) != len(wantReasons[index]) {
			return fmt.Errorf("invalid v2 placement decision %d", index)
		}
		for reasonIndex, reason := range wantReasons[index] {
			if expected.ExclusionReasons[reasonIndex] != reason {
				return fmt.Errorf("invalid v2 placement reason %d/%d", index, reasonIndex)
			}
		}
		candidate := fixture.Observation.Devices[index]
		if candidate.Device.DeviceID != expected.DeviceID || candidate.InstanceID != expected.InstanceID ||
			candidate.Revision != expected.Revision || candidate.Generation != expected.Generation || candidate.HeartbeatSequence != expected.HeartbeatSequence ||
			candidate.Device.ReservationState != expected.ReservationState || uint64(len(candidate.Device.GPUs)) != expected.GPUCount {
			return fmt.Errorf("v2 placement decision %d is not bound to observation", index)
		}
	}
	return nil
}

func TestForgeDeviceInventoryPlacementEvaluationV2Receiver(t *testing.T) {
	fixture, err := decodeForgeDeviceInventoryPlacementEvaluationV2(forgeDeviceInventoryPlacementEvaluationV2Fixture)
	if err != nil {
		t.Fatalf("decode v2 placement evaluation fixture: %v", err)
	}
	if len(fixture.Expected) != 2 || fixture.Expected[0].DeviceID != "device-a" || fixture.Expected[1].DeviceID != "device-b" {
		t.Fatalf("v2 placement decision binding changed: %#v", fixture.Expected)
	}
	t.Run("unknown field", func(t *testing.T) {
		trimmed := bytes.TrimSuffix(bytes.TrimSpace(forgeDeviceInventoryPlacementEvaluationV2Fixture), []byte("}"))
		mutated := append(append([]byte(nil), trimmed...), []byte(`,"unexpected":true}`)...)
		if _, err := decodeForgeDeviceInventoryPlacementEvaluationV2(mutated); err == nil {
			t.Fatal("unknown v2 placement evaluation field was accepted")
		}
	})
	t.Run("authority mutation", func(t *testing.T) {
		mutated := bytes.Replace(forgeDeviceInventoryPlacementEvaluationV2Fixture, []byte(`"inventory_authoritative": false`), []byte(`"inventory_authoritative": true`), 1)
		if _, err := decodeForgeDeviceInventoryPlacementEvaluationV2(mutated); err == nil {
			t.Fatal("authority-bearing v2 placement evaluation was accepted")
		}
	})
	t.Run("selection mutation", func(t *testing.T) {
		mutated := bytes.Replace(forgeDeviceInventoryPlacementEvaluationV2Fixture, []byte(`"selected_device_id": null`), []byte(`"selected_device_id": "device-a"`), 1)
		if _, err := decodeForgeDeviceInventoryPlacementEvaluationV2(mutated); err == nil {
			t.Fatal("selected v2 placement evaluation was accepted")
		}
	})
	t.Run("observation binding mutation", func(t *testing.T) {
		mutated := bytes.Replace(forgeDeviceInventoryPlacementEvaluationV2Fixture, []byte(`"device_id": "device-a"`), []byte(`"device_id": "device-c"`), 1)
		if _, err := decodeForgeDeviceInventoryPlacementEvaluationV2(mutated); err == nil {
			t.Fatal("observation drift was accepted")
		}
	})
	t.Run("duplicate field", func(t *testing.T) {
		mutated := bytes.Replace(forgeDeviceInventoryPlacementEvaluationV2Fixture, []byte(`"schema_version": "forge.device-inventory-placement-evaluation/v2",`), []byte(`"schema_version": "forge.device-inventory-placement-evaluation/v2","schema_version": "forge.device-inventory-placement-evaluation/v2",`), 1)
		if _, err := decodeForgeDeviceInventoryPlacementEvaluationV2(mutated); err == nil {
			t.Fatal("duplicate v2 placement evaluation field was accepted")
		}
	})
	t.Run("trailing value", func(t *testing.T) {
		mutated := append(append([]byte(nil), forgeDeviceInventoryPlacementEvaluationV2Fixture...), []byte(` true`)...)
		if _, err := decodeForgeDeviceInventoryPlacementEvaluationV2(mutated); err == nil {
			t.Fatal("trailing v2 placement evaluation value was accepted")
		}
	})
}
