package domain

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"testing"
)

const (
	forgeRegistryPlacementMaxSafeInteger = uint64(9007199254740991)
	forgeRegistryPlacementMaxDecisions   = 128
	forgeRegistryPlacementMaxReasons     = 64
	forgeRegistryPlacementMaxGPUCount    = 32
)

// The registry placement candidate is a display-only v2 evaluation. Aero-ID
// may inspect the value for compatibility, but it does not turn it into an
// audit fact, device identity, reservation, scheduling, or execution proof.
//
//go:embed testdata/forge-device-inventory-registry-placement-preview-v1.json
var forgeDeviceInventoryRegistryPlacementPreviewFixture []byte

type forgeRegistryPlacementOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeRegistryPlacementAuthority struct {
	IdentityVerified       bool `json:"identity_verified"`
	HeartbeatPersisted     bool `json:"heartbeat_persisted"`
	InventoryAuthoritative bool `json:"inventory_authoritative"`
	PlacementSelected      bool `json:"placement_selected"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
}

type forgeRegistryPlacementDecision struct {
	Revision                   uint64   `json:"revision"`
	Generation                 uint64   `json:"generation"`
	HeartbeatSequence          uint64   `json:"heartbeat_sequence"`
	DeviceID                   string   `json:"device_id"`
	InstanceID                 string   `json:"instance_id"`
	ReservationState           string   `json:"reservation_state"`
	GPUCount                   uint64   `json:"gpu_count"`
	AvailableGPUMemoryBytes    uint64   `json:"available_gpu_memory_bytes"`
	MatchesRequirements        bool     `json:"matches_requirements"`
	ExclusionReasons           []string `json:"exclusion_reasons"`
	OwnerDeclarationUnverified bool     `json:"owner_declaration_unverified"`
	DeviceAttributesUnverified bool     `json:"device_attributes_unverified"`
}

type forgeRegistryPlacementPreview struct {
	SchemaVersion          string                           `json:"schema_version"`
	EvaluationMode         string                           `json:"evaluation_mode"`
	SourceSchemaVersion    string                           `json:"source_schema_version"`
	EvaluationOwner        forgeRegistryPlacementOwner      `json:"evaluation_owner"`
	EvaluatedAtMS          uint64                           `json:"evaluated_at_ms"`
	Notice                 string                           `json:"notice"`
	Decisions              []forgeRegistryPlacementDecision `json:"decisions"`
	EligibleCandidateCount uint64                           `json:"eligible_candidate_count"`
	SelectedDeviceID       *string                          `json:"selected_device_id"`
	SelectedInstanceID     *string                          `json:"selected_instance_id"`
	Authority              forgeRegistryPlacementAuthority  `json:"authority"`
}

func decodeForgeRegistryPlacementPreview(raw []byte) (forgeRegistryPlacementPreview, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeRegistryPlacementPreview{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var preview forgeRegistryPlacementPreview
	if err := decoder.Decode(&preview); err != nil {
		return forgeRegistryPlacementPreview{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || len(fields) != 11 {
		return forgeRegistryPlacementPreview{}, errors.New("registry placement top-level fields are not exact")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeRegistryPlacementPreview{}, errors.New("trailing JSON value")
		}
		return forgeRegistryPlacementPreview{}, err
	}
	return preview, nil
}

func (authority forgeRegistryPlacementAuthority) isZero() bool {
	return !authority.IdentityVerified && !authority.HeartbeatPersisted &&
		!authority.InventoryAuthoritative && !authority.PlacementSelected &&
		!authority.ReservationCreated && !authority.ExecutionAuthorized &&
		!authority.DispatchPerformed
}

func forgeRegistryPlacementPreviewIsDisplayOnly(preview forgeRegistryPlacementPreview) bool {
	return preview.SchemaVersion == "forge.device-inventory-placement-evaluation/v2" &&
		preview.EvaluationMode == "offline_static_only" &&
		preview.SourceSchemaVersion == "forge.device-inventory-observation/v2" &&
		preview.EvaluatedAtMS > 0 && preview.EvaluatedAtMS <= forgeRegistryPlacementMaxSafeInteger &&
		preview.Notice == "Every owner, state, timestamp, resource, GPU, reservation, residency, trust, sandbox, and concurrency value is an unverified caller declaration. This read-only comparison selects no target and grants no execution authority." &&
		validForgeRegistryOwnerPart(preview.EvaluationOwner.Issuer) &&
		validForgeRegistryOwnerPart(preview.EvaluationOwner.Subject) &&
		validForgeRegistryOwnerPart(preview.EvaluationOwner.TenantID) && preview.SelectedDeviceID == nil &&
		preview.SelectedInstanceID == nil && preview.Authority.isZero()
}

func validForgeRegistryOwnerPart(value string) bool {
	if value == "" || len(value) > 512 || value != string(bytes.TrimSpace([]byte(value))) {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func validForgeRegistryIdentifier(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for index, character := range value {
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') && !(index > 0 && (character == '.' ||
			character == '_' || character == ':' || character == '-')) {
			return false
		}
	}
	return true
}

func validForgeRegistryReason(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '.' || character == '_' ||
			character == ':' || character == '+' || character == '/' || character == '-') {
			return false
		}
	}
	return true
}

func TestForgeDeviceInventoryRegistryPlacementPreviewReceiver(t *testing.T) {
	preview, err := decodeForgeRegistryPlacementPreview(forgeDeviceInventoryRegistryPlacementPreviewFixture)
	if err != nil {
		t.Fatalf("decode registry placement preview: %v", err)
	}
	if !forgeRegistryPlacementPreviewIsDisplayOnly(preview) ||
		preview.EligibleCandidateCount > uint64(len(preview.Decisions)) {
		t.Fatalf("registry placement preview crossed an authority boundary: %#v", preview)
	}
	if len(preview.Decisions) > forgeRegistryPlacementMaxDecisions ||
		preview.EligibleCandidateCount > forgeRegistryPlacementMaxDecisions ||
		len(preview.Decisions) != 2 {
		t.Fatalf("registry placement preview owner/rows are invalid: %#v", preview)
	}
	eligible := uint64(0)
	devices := make(map[string]struct{}, len(preview.Decisions))
	instances := make(map[string]struct{}, len(preview.Decisions))
	for index, decision := range preview.Decisions {
		if decision.Revision == 0 || decision.Revision > forgeRegistryPlacementMaxSafeInteger ||
			decision.Generation == 0 || decision.Generation > forgeRegistryPlacementMaxSafeInteger ||
			decision.HeartbeatSequence == 0 || decision.HeartbeatSequence > forgeRegistryPlacementMaxSafeInteger ||
			!validForgeRegistryIdentifier(decision.DeviceID) || !validForgeRegistryIdentifier(decision.InstanceID) ||
			!func() bool { _, exists := devices[decision.DeviceID]; return !exists }() ||
			!func() bool { _, exists := instances[decision.InstanceID]; return !exists }() ||
			(index > 0 && (preview.Decisions[index-1].DeviceID > decision.DeviceID ||
				(preview.Decisions[index-1].DeviceID == decision.DeviceID &&
					preview.Decisions[index-1].InstanceID >= decision.InstanceID))) ||
			(decision.ReservationState != "none" && decision.ReservationState != "reserved") ||
			decision.GPUCount > forgeRegistryPlacementMaxGPUCount ||
			decision.AvailableGPUMemoryBytes > forgeRegistryPlacementMaxSafeInteger ||
			!decision.OwnerDeclarationUnverified || !decision.DeviceAttributesUnverified ||
			len(decision.ExclusionReasons) > forgeRegistryPlacementMaxReasons ||
			decision.MatchesRequirements != (len(decision.ExclusionReasons) == 0) {
			t.Fatalf("invalid registry placement decision %d: %#v", index, decision)
		}
		devices[decision.DeviceID] = struct{}{}
		instances[decision.InstanceID] = struct{}{}
		for reasonIndex, reason := range decision.ExclusionReasons {
			if !validForgeRegistryReason(reason) ||
				(reasonIndex > 0 && decision.ExclusionReasons[reasonIndex-1] >= reason) {
				t.Fatalf("registry placement reasons are not sorted: %#v", decision)
			}
		}
		if decision.MatchesRequirements {
			eligible++
		}
	}
	if eligible != preview.EligibleCandidateCount {
		t.Fatalf("eligible count=%d want %d", preview.EligibleCandidateCount, eligible)
	}

	t.Run("unknown field", func(t *testing.T) {
		var root map[string]json.RawMessage
		if err := json.Unmarshal(forgeDeviceInventoryRegistryPlacementPreviewFixture, &root); err != nil {
			t.Fatal(err)
		}
		root["unexpected"] = json.RawMessage(`true`)
		mutated, err := json.Marshal(root)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeForgeRegistryPlacementPreview(mutated); err == nil {
			t.Fatal("unknown registry placement field was accepted")
		}
	})
	t.Run("authority mutation", func(t *testing.T) {
		mutated := bytes.Replace(
			forgeDeviceInventoryRegistryPlacementPreviewFixture,
			[]byte(`"dispatch_performed": false`),
			[]byte(`"dispatch_performed": true`), 1)
		decoded, err := decodeForgeRegistryPlacementPreview(mutated)
		if err != nil {
			t.Fatal(err)
		}
		if forgeRegistryPlacementPreviewIsDisplayOnly(decoded) {
			t.Fatal("authority-bearing registry placement preview was accepted")
		}
	})
	t.Run("selected target mutation", func(t *testing.T) {
		mutated := bytes.Replace(
			forgeDeviceInventoryRegistryPlacementPreviewFixture,
			[]byte(`"selected_device_id": null`),
			[]byte(`"selected_device_id": "device-a"`), 1)
		decoded, err := decodeForgeRegistryPlacementPreview(mutated)
		if err != nil {
			t.Fatal(err)
		}
		if forgeRegistryPlacementPreviewIsDisplayOnly(decoded) {
			t.Fatal("selected registry placement target was accepted")
		}
	})
	t.Run("duplicate field", func(t *testing.T) {
		mutated := bytes.Replace(
			forgeDeviceInventoryRegistryPlacementPreviewFixture,
			[]byte(`"schema_version": "forge.device-inventory-placement-evaluation/v2",`),
			[]byte(`"schema_version": "forge.device-inventory-placement-evaluation/v2","schema_version": "forge.device-inventory-placement-evaluation/v2",`), 1)
		if _, err := decodeForgeRegistryPlacementPreview(mutated); err == nil {
			t.Fatal("duplicate registry placement field was accepted")
		}
	})
	t.Run("trailing value", func(t *testing.T) {
		mutated := append(append([]byte(nil), forgeDeviceInventoryRegistryPlacementPreviewFixture...), []byte(` true`)...)
		if _, err := decodeForgeRegistryPlacementPreview(mutated); err == nil {
			t.Fatal("trailing registry placement value was accepted")
		}
	})
}
