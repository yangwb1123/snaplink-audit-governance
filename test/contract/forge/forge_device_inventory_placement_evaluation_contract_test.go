package forge

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
)

// This receiver consumes one deterministic, caller-declared placement result.
// It is an offline comparison value and never grants inventory authority,
// target selection, reservation, scheduling, dispatch, execution, or Audit
// publication.
//
//go:embed testdata/forge-device-inventory-placement-evaluation-v1.json
var forgeDeviceInventoryPlacementEvaluationFixture []byte

const (
	forgeDeviceInventoryPlacementEvaluationSchema = "forge.device-inventory-placement-evaluation/v1"
	forgeDeviceInventoryPlacementEvaluationMode   = "pure_persisted_inventory_offline_evaluation"
	forgeDeviceInventoryPlacementEvaluationSource = "forge-device-inventory-placement-input-v1.json"
	forgeDeviceInventoryPlacementEvaluationSafe   = uint64(9007199254740991)
)

type forgeDeviceInventoryPlacementEvaluationGPURequirement struct {
	Required       bool   `json:"required"`
	MinMemoryBytes uint64 `json:"min_memory_bytes"`
	Runtime        string `json:"runtime"`
}

type forgeDeviceInventoryPlacementEvaluationRequirements struct {
	OS                 string                                                `json:"os"`
	Architecture       string                                                `json:"architecture"`
	MinCPUCores        uint64                                                `json:"min_cpu_cores"`
	MinMemoryBytes     uint64                                                `json:"min_memory_bytes"`
	MinStorageBytes    uint64                                                `json:"min_storage_bytes"`
	Runtime            string                                                `json:"runtime"`
	GPU                forgeDeviceInventoryPlacementEvaluationGPURequirement `json:"gpu"`
	DataResidencyZones []string                                              `json:"data_residency_zones"`
	MinimumTrustZone   string                                                `json:"minimum_trust_zone"`
	SandboxFloor       string                                                `json:"sandbox_floor"`
	ConcurrencySlots   uint64                                                `json:"concurrency_slots"`
}

type forgeDeviceInventoryPlacementEvaluationAuthority struct {
	PlacementEvaluated  bool `json:"placement_evaluated"`
	PlacementSelected   bool `json:"placement_selected"`
	ReservationCreated  bool `json:"reservation_created"`
	ExecutionAuthorized bool `json:"execution_authorized"`
	DispatchPerformed   bool `json:"dispatch_performed"`
}

type forgeDeviceInventoryPlacementEvaluationExpected struct {
	Accepted                   bool     `json:"accepted"`
	Error                      string   `json:"error"`
	Revision                   uint64   `json:"revision"`
	DeviceID                   string   `json:"device_id"`
	InstanceID                 string   `json:"instance_id"`
	MatchesRequirements        bool     `json:"matches_requirements"`
	ExclusionReasons           []string `json:"exclusion_reasons"`
	OwnerDeclarationUnverified bool     `json:"owner_declaration_unverified"`
	DeviceAttributesUnverified bool     `json:"device_attributes_unverified"`
}

type forgeDeviceInventoryPlacementEvaluation struct {
	SchemaVersion      string                                              `json:"schema_version"`
	EvaluationMode     string                                              `json:"evaluation_mode"`
	SourceFixture      string                                              `json:"source_fixture"`
	SourceCase         string                                              `json:"source_case"`
	EvaluatedAtMS      uint64                                              `json:"evaluated_at_ms"`
	PolicyRequirements forgeDeviceInventoryPlacementEvaluationRequirements `json:"policy_requirements"`
	Authority          forgeDeviceInventoryPlacementEvaluationAuthority    `json:"authority"`
	Expected           forgeDeviceInventoryPlacementEvaluationExpected     `json:"expected"`
}

func decodeForgeDeviceInventoryPlacementEvaluation(raw []byte) (forgeDeviceInventoryPlacementEvaluation, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeDeviceInventoryPlacementEvaluation{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var fixture forgeDeviceInventoryPlacementEvaluation
	if err := decoder.Decode(&fixture); err != nil {
		return forgeDeviceInventoryPlacementEvaluation{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeDeviceInventoryPlacementEvaluation{}, errors.New("trailing JSON value")
		}
		return forgeDeviceInventoryPlacementEvaluation{}, err
	}
	return fixture, validateForgeDeviceInventoryPlacementEvaluation(fixture)
}

func validateForgeDeviceInventoryPlacementEvaluation(fixture forgeDeviceInventoryPlacementEvaluation) error {
	requirements := fixture.PolicyRequirements
	wantReasons := []string{
		"concurrency_capacity_insufficient",
		"data_residency_zone_mismatch",
		"sandbox_floor_unmet",
		"trust_zone_unconfirmed",
	}
	if fixture.SchemaVersion != forgeDeviceInventoryPlacementEvaluationSchema ||
		fixture.EvaluationMode != forgeDeviceInventoryPlacementEvaluationMode ||
		fixture.SourceFixture != forgeDeviceInventoryPlacementEvaluationSource ||
		fixture.SourceCase != "online" || fixture.EvaluatedAtMS != 200500 ||
		fixture.EvaluatedAtMS > forgeDeviceInventoryPlacementEvaluationSafe ||
		fixture.Authority != (forgeDeviceInventoryPlacementEvaluationAuthority{}) ||
		requirements.OS != "linux" || requirements.Architecture != "amd64" ||
		requirements.MinCPUCores != 4 || requirements.MinMemoryBytes != 4096 ||
		requirements.MinStorageBytes != 1024 || requirements.Runtime != "oci" ||
		requirements.GPU != (forgeDeviceInventoryPlacementEvaluationGPURequirement{}) ||
		len(requirements.DataResidencyZones) != 1 || requirements.DataResidencyZones[0] != "us-west" ||
		requirements.MinimumTrustZone != "standard" || requirements.SandboxFloor != "container" ||
		requirements.ConcurrencySlots != 1 {
		return errors.New("invalid placement evaluation envelope")
	}
	expected := fixture.Expected
	if !expected.Accepted || expected.Error != "" || expected.Revision != 7 ||
		expected.DeviceID != "device-a" || expected.InstanceID != "runner-a" ||
		expected.MatchesRequirements || len(expected.ExclusionReasons) != len(wantReasons) ||
		!expected.OwnerDeclarationUnverified || !expected.DeviceAttributesUnverified {
		return errors.New("invalid placement evaluation result")
	}
	for index, reason := range wantReasons {
		if expected.ExclusionReasons[index] != reason {
			return fmt.Errorf("invalid placement evaluation reason %d", index)
		}
	}
	return nil
}

func appendForgeDeviceInventoryPlacementEvaluationRootField(raw []byte) []byte {
	trimmed := bytes.TrimSuffix(bytes.TrimSpace(raw), []byte("}"))
	return append(append([]byte(nil), trimmed...), []byte(`,"unexpected":true}`)...)
}

func TestForgeDeviceInventoryPlacementEvaluationReceiver(t *testing.T) {
	fixture, err := decodeForgeDeviceInventoryPlacementEvaluation(forgeDeviceInventoryPlacementEvaluationFixture)
	if err != nil {
		t.Fatalf("decode placement evaluation fixture: %v", err)
	}
	if fixture.Expected.DeviceID != "device-a" || fixture.Expected.InstanceID != "runner-a" {
		t.Fatalf("placement evaluation binding changed: %#v", fixture.Expected)
	}

	t.Run("unknown field", func(t *testing.T) {
		if _, err := decodeForgeDeviceInventoryPlacementEvaluation(appendForgeDeviceInventoryPlacementEvaluationRootField(forgeDeviceInventoryPlacementEvaluationFixture)); err == nil {
			t.Fatal("unknown placement evaluation field was accepted")
		}
	})
	t.Run("authority mutation", func(t *testing.T) {
		mutated := bytes.Replace(forgeDeviceInventoryPlacementEvaluationFixture, []byte(`"dispatch_performed": false`), []byte(`"dispatch_performed": true`), 1)
		if _, err := decodeForgeDeviceInventoryPlacementEvaluation(mutated); err == nil {
			t.Fatal("authority-bearing placement evaluation was accepted")
		}
	})
	t.Run("source mutation", func(t *testing.T) {
		mutated := bytes.Replace(forgeDeviceInventoryPlacementEvaluationFixture, []byte(`"source_case": "online"`), []byte(`"source_case": "stale"`), 1)
		if _, err := decodeForgeDeviceInventoryPlacementEvaluation(mutated); err == nil {
			t.Fatal("source mutation accepted")
		}
	})
	t.Run("duplicate field", func(t *testing.T) {
		mutated := bytes.Replace(forgeDeviceInventoryPlacementEvaluationFixture, []byte(`"schema_version": "forge.device-inventory-placement-evaluation/v1",`), []byte(`"schema_version": "forge.device-inventory-placement-evaluation/v1","schema_version": "forge.device-inventory-placement-evaluation/v1",`), 1)
		if _, err := decodeForgeDeviceInventoryPlacementEvaluation(mutated); err == nil {
			t.Fatal("duplicate placement evaluation field was accepted")
		}
	})
	t.Run("trailing value", func(t *testing.T) {
		mutated := append(append([]byte(nil), forgeDeviceInventoryPlacementEvaluationFixture...), []byte(` true`)...)
		if _, err := decodeForgeDeviceInventoryPlacementEvaluation(mutated); err == nil {
			t.Fatal("trailing placement evaluation value was accepted")
		}
	})
}
