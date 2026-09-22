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
)

// This receiver-side contract is a bounded, owner-bound resource summary. It
// carries caller-declared inventory and placement observations only; it does
// not authenticate a device, persist heartbeat/inventory state, select a
// target, reserve capacity, dispatch work, or publish Audit authority.
//
//go:embed testdata/forge-device-resource-summary-v1.json
var forgeDeviceResourceSummaryFixture []byte

type forgeResourceSummaryOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeResourceSummaryAuthority struct {
	IdentityVerified       bool `json:"identity_verified"`
	HeartbeatPersisted     bool `json:"heartbeat_persisted"`
	InventoryAuthoritative bool `json:"inventory_authoritative"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
}

type forgeResourceSummaryGPU struct {
	Present     bool   `json:"present"`
	MemoryBytes uint64 `json:"memory_bytes"`
	Runtime     string `json:"runtime"`
}

type forgeResourceSummaryDevice struct {
	DeviceID              string                    `json:"device_id"`
	Owner                 forgeResourceSummaryOwner `json:"owner"`
	ApprovalState         string                    `json:"approval_state"`
	CordonState           string                    `json:"cordon_state"`
	Liveness              string                    `json:"liveness"`
	SnapshotObservedAtMS  int64                     `json:"snapshot_observed_at_ms"`
	LeaseExpiresAtMS      int64                     `json:"lease_expires_at_ms"`
	OS                    string                    `json:"os"`
	Architecture          string                    `json:"architecture"`
	AvailableCPUCores     uint64                    `json:"available_cpu_cores"`
	AvailableMemoryBytes  uint64                    `json:"available_memory_bytes"`
	AvailableStorageBytes uint64                    `json:"available_storage_bytes"`
	Runtimes              []string                  `json:"runtimes"`
	GPU                   forgeResourceSummaryGPU   `json:"gpu"`
	DataResidencyZones    []string                  `json:"data_residency_zones"`
	TrustZone             string                    `json:"trust_zone"`
	SandboxLevels         []string                  `json:"sandbox_levels"`
	ConcurrencyLimit      uint64                    `json:"concurrency_limit"`
	ActiveConcurrency     uint64                    `json:"active_concurrency"`
}

type forgeResourceSummaryInventoryRow struct {
	InstanceID string                     `json:"instance_id"`
	Device     forgeResourceSummaryDevice `json:"device"`
}

type forgeResourceSummaryInventory struct {
	SchemaVersion                   string                             `json:"schema_version"`
	EvaluationMode                  string                             `json:"evaluation_mode"`
	EvaluatedAtMS                   int64                              `json:"evaluated_at_ms"`
	OwnerDeclaration                forgeResourceSummaryOwner          `json:"owner_declaration"`
	OwnerDeclarationUnverified      bool                               `json:"owner_declaration_unverified"`
	InventoryDeclarationsUnverified bool                               `json:"inventory_declarations_unverified"`
	Notice                          string                             `json:"notice"`
	Devices                         []forgeResourceSummaryInventoryRow `json:"devices"`
	ExecutionAuthorized             bool                               `json:"execution_authorized"`
	ReservationCreated              bool                               `json:"reservation_created"`
	DispatchPerformed               bool                               `json:"dispatch_performed"`
}

type forgeResourceSummaryDecision struct {
	DeviceID            string   `json:"device_id"`
	InstanceID          string   `json:"instance_id"`
	MatchesRequirements bool     `json:"matches_requirements"`
	ExclusionReasons    []string `json:"exclusion_reasons"`
}

type forgeResourceSummaryPlacement struct {
	SchemaVersion              string                         `json:"schema_version"`
	EvaluationMode             string                         `json:"evaluation_mode"`
	Owner                      forgeResourceSummaryOwner      `json:"owner"`
	ConversationID             string                         `json:"conversation_id"`
	RunID                      string                         `json:"run_id"`
	EvaluatedAtMS              int64                          `json:"evaluated_at_ms"`
	OwnerDeclarationUnverified bool                           `json:"owner_declaration_unverified"`
	DeviceAttributesUnverified bool                           `json:"device_attributes_unverified"`
	Decisions                  []forgeResourceSummaryDecision `json:"decisions"`
	SelectedDeviceID           *string                        `json:"selected_device_id"`
	SelectedInstanceID         *string                        `json:"selected_instance_id"`
	Authority                  forgeResourceSummaryAuthority  `json:"authority"`
}

type forgeResourceSummaryExpected struct {
	SchemaVersion                   string                        `json:"schema_version"`
	EvaluationMode                  string                        `json:"evaluation_mode"`
	ConversationID                  string                        `json:"conversation_id"`
	RunID                           string                        `json:"run_id"`
	EvaluatedAtMS                   int64                         `json:"evaluated_at_ms"`
	OwnerDeclarationUnverified      bool                          `json:"owner_declaration_unverified"`
	InventoryDeclarationsUnverified bool                          `json:"inventory_declarations_unverified"`
	PlacementDeclarationUnverified  bool                          `json:"placement_declaration_unverified"`
	Notice                          string                        `json:"notice"`
	DeviceCount                     int                           `json:"device_count"`
	RunnerInstanceCount             int                           `json:"runner_instance_count"`
	AvailableCPUCores               uint64                        `json:"available_cpu_cores"`
	AvailableMemoryBytes            uint64                        `json:"available_memory_bytes"`
	AvailableStorageBytes           uint64                        `json:"available_storage_bytes"`
	AvailableGPUCount               int                           `json:"available_gpu_count"`
	AvailableGPUMemoryBytes         uint64                        `json:"available_gpu_memory_bytes"`
	EligibleDeviceCount             int                           `json:"eligible_device_count"`
	EligibleInstanceCount           int                           `json:"eligible_instance_count"`
	SelectedDeviceID                *string                       `json:"selected_device_id"`
	SelectedInstanceID              *string                       `json:"selected_instance_id"`
	Authority                       forgeResourceSummaryAuthority `json:"authority"`
}

type forgeResourceSummaryFixture struct {
	APIVersion               string                        `json:"api_version"`
	InventoryContractFixture string                        `json:"inventory_contract_fixture"`
	PlacementContractFixture string                        `json:"placement_contract_fixture"`
	Owner                    forgeResourceSummaryOwner     `json:"owner"`
	Inventory                forgeResourceSummaryInventory `json:"inventory"`
	Placement                forgeResourceSummaryPlacement `json:"placement_observation"`
	Expected                 forgeResourceSummaryExpected  `json:"expected"`
}

func decodeForgeResourceSummary(raw []byte) (forgeResourceSummaryFixture, error) {
	if err := rejectForgeResourceSummaryDuplicateKeys(raw); err != nil {
		return forgeResourceSummaryFixture{}, err
	}
	var fixture forgeResourceSummaryFixture
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fixture); err != nil {
		return forgeResourceSummaryFixture{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeResourceSummaryFixture{}, errors.New("trailing JSON value")
		}
		return forgeResourceSummaryFixture{}, err
	}
	return fixture, nil
}

func rejectForgeResourceSummaryDuplicateKeys(raw []byte) error {
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
					return errors.New("object key is not string")
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
			return nil
		}
	}
	if err := walk(); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func (a forgeResourceSummaryAuthority) isZero() bool {
	return !a.IdentityVerified && !a.HeartbeatPersisted && !a.InventoryAuthoritative &&
		!a.ReservationCreated && !a.ExecutionAuthorized && !a.DispatchPerformed
}

func forgeResourceSummarySemanticallyValid(fixture forgeResourceSummaryFixture) bool {
	if fixture.Inventory.OwnerDeclaration != fixture.Owner ||
		fixture.Inventory.ExecutionAuthorized || fixture.Inventory.ReservationCreated ||
		fixture.Inventory.DispatchPerformed || fixture.Placement.Owner != fixture.Owner ||
		fixture.Placement.SelectedDeviceID != nil ||
		fixture.Placement.SelectedInstanceID != nil ||
		!fixture.Placement.Authority.isZero() ||
		fixture.Expected.SelectedDeviceID != nil ||
		fixture.Expected.SelectedInstanceID != nil ||
		!fixture.Expected.Authority.isZero() {
		return false
	}
	for _, row := range fixture.Inventory.Devices {
		if row.Device.Owner != fixture.Owner {
			return false
		}
	}
	return true
}

func requireForgeResourceSummaryShape(t *testing.T, fixture forgeResourceSummaryFixture) {
	t.Helper()
	if fixture.APIVersion != "forgeos.device-resource-summary-contract/v1" ||
		fixture.InventoryContractFixture != "forge-device-inventory-observation-v1" ||
		fixture.PlacementContractFixture != "forge-session-placement-observation-v1" {
		t.Fatalf("unexpected resource-summary contract identity: %#v", fixture)
	}
	owner := fixture.Owner
	if owner.Issuer == "" || owner.Subject == "" || owner.TenantID == "" {
		t.Fatalf("resource-summary owner is incomplete: %#v", owner)
	}
	inventory := fixture.Inventory
	if inventory.SchemaVersion != "forge.device-inventory-observation/v1" ||
		inventory.EvaluationMode != "offline_static_only" || inventory.EvaluatedAtMS <= 0 ||
		inventory.OwnerDeclaration != owner || !inventory.OwnerDeclarationUnverified ||
		!inventory.InventoryDeclarationsUnverified || inventory.Notice == "" ||
		inventory.ExecutionAuthorized || inventory.ReservationCreated || inventory.DispatchPerformed ||
		len(inventory.Devices) != 9 {
		t.Fatalf("inventory declaration is not bounded/read-only: %#v", inventory)
	}
	for i, row := range inventory.Devices {
		device := row.Device
		if row.InstanceID == "" || device.DeviceID == "" || device.Owner != owner ||
			device.SnapshotObservedAtMS <= 0 || device.LeaseExpiresAtMS <= 0 ||
			device.AvailableCPUCores == 0 || device.AvailableMemoryBytes == 0 ||
			device.AvailableStorageBytes == 0 || device.ConcurrencyLimit == 0 ||
			device.ActiveConcurrency > device.ConcurrencyLimit ||
			(i > 0 && (inventory.Devices[i-1].InstanceID >= row.InstanceID ||
				inventory.Devices[i-1].Device.DeviceID >= device.DeviceID)) {
			t.Fatalf("inventory row %d is invalid or unordered: %#v", i, row)
		}
	}
	placement := fixture.Placement
	if placement.SchemaVersion != "forge.session-placement-observation/v1" ||
		placement.EvaluationMode != "offline_static_only" || placement.Owner != owner ||
		placement.ConversationID == "" || placement.RunID == "" || placement.EvaluatedAtMS <= 0 ||
		!placement.OwnerDeclarationUnverified || !placement.DeviceAttributesUnverified ||
		placement.SelectedDeviceID != nil || placement.SelectedInstanceID != nil ||
		!placement.Authority.isZero() || len(placement.Decisions) != 9 {
		t.Fatalf("placement declaration is not bounded/read-only: %#v", placement)
	}
	for i, decision := range placement.Decisions {
		if decision.DeviceID == "" || decision.InstanceID == "" ||
			(i > 0 && (placement.Decisions[i-1].DeviceID >= decision.DeviceID ||
				placement.Decisions[i-1].InstanceID >= decision.InstanceID)) {
			t.Fatalf("placement decision %d is invalid or unordered: %#v", i, decision)
		}
	}
	expected := fixture.Expected
	if expected.SchemaVersion != "forge.device-resource-summary/v1" ||
		expected.EvaluationMode != "offline_static_only" || expected.ConversationID != placement.ConversationID ||
		expected.RunID != placement.RunID || expected.EvaluatedAtMS != placement.EvaluatedAtMS ||
		!expected.OwnerDeclarationUnverified || !expected.InventoryDeclarationsUnverified ||
		!expected.PlacementDeclarationUnverified || expected.Notice == "" || expected.DeviceCount != 9 ||
		expected.RunnerInstanceCount != 9 || expected.AvailableCPUCores != 66 ||
		expected.AvailableMemoryBytes != 135168 || expected.AvailableStorageBytes != 67584 ||
		expected.AvailableGPUCount != 1 || expected.AvailableGPUMemoryBytes != 4096 ||
		expected.EligibleDeviceCount != 2 || expected.EligibleInstanceCount != 2 ||
		expected.SelectedDeviceID != nil || expected.SelectedInstanceID != nil || !expected.Authority.isZero() {
		t.Fatalf("resource-summary projection is not canonical/read-only: %#v", expected)
	}
}

func TestForgeDeviceResourceSummaryContractFixture(t *testing.T) {
	fixture, err := decodeForgeResourceSummary(forgeDeviceResourceSummaryFixture)
	if err != nil {
		t.Fatalf("decode resource-summary fixture: %v", err)
	}
	requireForgeResourceSummaryShape(t, fixture)
}

func TestForgeDeviceResourceSummaryRejectsUnknownDuplicateAuthorityForeignOwnerAndSelection(t *testing.T) {
	mutate := func(name string, fn func(map[string]json.RawMessage)) {
		t.Run(name, func(t *testing.T) {
			var root map[string]json.RawMessage
			if err := json.Unmarshal(forgeDeviceResourceSummaryFixture, &root); err != nil {
				t.Fatal(err)
			}
			fn(root)
			encoded, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			fixture, err := decodeForgeResourceSummary(encoded)
			if err == nil && forgeResourceSummarySemanticallyValid(fixture) {
				t.Fatal("mutated resource-summary fixture unexpectedly accepted")
			}
		})
	}
	mutate("unknown field", func(root map[string]json.RawMessage) {
		root["prompt"] = json.RawMessage(`"raw prompt"`)
	})
	mutate("authority mutation", func(root map[string]json.RawMessage) {
		root["expected"] = json.RawMessage(strings.Replace(string(root["expected"]), `"execution_authorized": false`, `"execution_authorized": true`, 1))
	})
	mutate("foreign inventory owner", func(root map[string]json.RawMessage) {
		var inventory map[string]json.RawMessage
		if err := json.Unmarshal(root["inventory"], &inventory); err != nil {
			t.Fatal(err)
		}
		var devices []map[string]json.RawMessage
		if err := json.Unmarshal(inventory["devices"], &devices); err != nil {
			t.Fatal(err)
		}
		var device map[string]json.RawMessage
		if err := json.Unmarshal(devices[0]["device"], &device); err != nil {
			t.Fatal(err)
		}
		device["owner"] = json.RawMessage(`{"issuer":"https://id.example","subject":"other","tenant_id":"tenant-1"}`)
		devices[0]["device"], _ = json.Marshal(device)
		inventory["devices"], _ = json.Marshal(devices)
		root["inventory"], _ = json.Marshal(inventory)
	})
	mutate("selected target", func(root map[string]json.RawMessage) {
		var placement map[string]json.RawMessage
		if err := json.Unmarshal(root["placement_observation"], &placement); err != nil {
			t.Fatal(err)
		}
		placement["selected_device_id"] = json.RawMessage(`"candidate-a"`)
		root["placement_observation"], _ = json.Marshal(placement)
	})
	duplicate := strings.TrimSpace(string(forgeDeviceResourceSummaryFixture))
	duplicate = strings.TrimSuffix(duplicate, "}") + `,"api_version":"forgeos.device-resource-summary-contract/v1"}`
	if _, err := decodeForgeResourceSummary([]byte(duplicate)); err == nil {
		t.Fatal("duplicate resource-summary field accepted")
	}
}
