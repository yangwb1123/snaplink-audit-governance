package forge

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"testing"
)

// This receiver-side contract is a complete, owner-bound value image only. It
// does not authenticate a device, persist heartbeat/inventory state, issue a
// credential, or grant inventory/execution/Audit authority.
//
//go:embed testdata/forge-device-enrollment-heartbeat-lifecycle-persistence-v1.json
var forgeDeviceEnrollmentHeartbeatLifecyclePersistenceFixture []byte

type forgeLifecycleOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}
type forgeLifecycleAuthority struct {
	IdentityVerified       bool `json:"identity_verified"`
	ChallengeConsumed      bool `json:"challenge_consumed"`
	HeartbeatPersisted     bool `json:"heartbeat_persisted"`
	InventoryAuthoritative bool `json:"inventory_authoritative"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
}
type forgeLifecycleDevice struct {
	DeviceID         string `json:"device_id"`
	ApprovalState    string `json:"approval_state"`
	CredentialState  string `json:"credential_state"`
	CordonState      string `json:"cordon_state"`
	ReservationState string `json:"reservation_state"`
}
type forgeLifecycleGPU struct {
	ID                   string `json:"id"`
	Vendor               string `json:"vendor"`
	MemoryBytes          uint64 `json:"memory_bytes"`
	AvailableMemoryBytes uint64 `json:"available_memory_bytes"`
}
type forgeLifecycleCapability struct {
	OS                    string              `json:"os"`
	Architecture          string              `json:"architecture"`
	CPUCores              uint64              `json:"cpu_cores"`
	AvailableCPUCores     uint64              `json:"available_cpu_cores"`
	MemoryBytes           uint64              `json:"memory_bytes"`
	AvailableMemoryBytes  uint64              `json:"available_memory_bytes"`
	StorageBytes          uint64              `json:"storage_bytes"`
	AvailableStorageBytes uint64              `json:"available_storage_bytes"`
	GPUs                  []forgeLifecycleGPU `json:"gpus"`
	Runtimes              []string            `json:"runtimes"`
}
type forgeLifecycleHeartbeat struct {
	Revision                   uint64 `json:"revision"`
	DeviceID                   string `json:"device_id"`
	InstanceID                 string `json:"instance_id"`
	Generation                 uint64 `json:"generation"`
	HeartbeatSequence          uint64 `json:"heartbeat_sequence"`
	ServerObservedAtMS         uint64 `json:"server_observed_at_ms"`
	CapabilityLeaseExpiresAtMS uint64 `json:"capability_lease_expires_at_ms"`
	Liveness                   string `json:"liveness"`
}
type forgeLifecycleInventory struct {
	Revision                         uint64 `json:"revision"`
	DeviceID                         string `json:"device_id"`
	RunnerDeviceID                   string `json:"runner_device_id"`
	RunnerInstanceID                 string `json:"runner_instance_id"`
	RunnerGeneration                 uint64 `json:"runner_generation"`
	RunnerHeartbeatSequence          uint64 `json:"runner_heartbeat_sequence"`
	RunnerServerObservedAtMS         uint64 `json:"runner_server_observed_at_ms"`
	RunnerCapabilityLeaseExpiresAtMS uint64 `json:"runner_capability_lease_expires_at_ms"`
	RunnerLiveness                   string `json:"runner_liveness"`
	Reserved                         bool   `json:"reserved"`
}
type forgeLifecycleImageExpected struct {
	Accepted          bool   `json:"accepted"`
	Revision          uint64 `json:"revision"`
	HeartbeatRevision uint64 `json:"heartbeat_revision"`
	InventoryRevision uint64 `json:"inventory_revision"`
}
type forgeLifecycleImage struct {
	Name      string                      `json:"name"`
	Revision  uint64                      `json:"revision"`
	Heartbeat forgeLifecycleHeartbeat     `json:"heartbeat"`
	Inventory forgeLifecycleInventory     `json:"inventory"`
	Expected  forgeLifecycleImageExpected `json:"expected"`
}
type forgeLifecycleCaseExpected struct {
	Accepted bool   `json:"accepted"`
	Error    string `json:"error"`
}
type forgeLifecycleCase struct {
	Name              string                     `json:"name"`
	OuterRevision     uint64                     `json:"outer_revision"`
	HeartbeatRevision uint64                     `json:"heartbeat_revision"`
	InventoryRevision uint64                     `json:"inventory_revision"`
	RunnerDeviceID    string                     `json:"runner_device_id,omitempty"`
	RunnerGeneration  uint64                     `json:"runner_generation,omitempty"`
	OwnerSubject      string                     `json:"owner_subject,omitempty"`
	Expected          forgeLifecycleCaseExpected `json:"expected"`
}
type forgeLifecycleFixture struct {
	SchemaVersion  string                   `json:"schema_version"`
	EvaluationMode string                   `json:"evaluation_mode"`
	Notice         string                   `json:"notice"`
	Authority      forgeLifecycleAuthority  `json:"authority"`
	Owner          forgeLifecycleOwner      `json:"owner"`
	Device         forgeLifecycleDevice     `json:"device"`
	Capabilities   forgeLifecycleCapability `json:"capabilities"`
	Images         []forgeLifecycleImage    `json:"images"`
	Cases          []forgeLifecycleCase     `json:"cases"`
}

func decodeForgeLifecyclePersistence(raw []byte, out *forgeLifecycleFixture) error {
	if err := rejectForgeLifecycleDuplicateKeys(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if out.Authority != (forgeLifecycleAuthority{}) {
		return fmt.Errorf("lifecycle persistence fixture grants authority")
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

func rejectForgeLifecycleDuplicateKeys(raw []byte) error {
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
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	return nil
}

func TestForgeDeviceEnrollmentHeartbeatLifecyclePersistenceFixture(t *testing.T) {
	var fixture forgeLifecycleFixture
	if err := decodeForgeLifecyclePersistence(forgeDeviceEnrollmentHeartbeatLifecyclePersistenceFixture, &fixture); err != nil {
		t.Fatalf("decode lifecycle persistence fixture: %v", err)
	}
	if fixture.SchemaVersion != "forge.device-enrollment-heartbeat-lifecycle-persistence/v1" || fixture.EvaluationMode != "pure_joined_value_replacement" || fixture.Notice == "" || fixture.Authority != (forgeLifecycleAuthority{}) {
		t.Fatalf("invalid authority-free envelope: %#v", fixture)
	}
	if fixture.Owner.Issuer == "" || fixture.Owner.Subject == "" || fixture.Owner.TenantID == "" || fixture.Device.DeviceID == "" || fixture.Device.ApprovalState != "approved" || fixture.Device.CredentialState != "active" {
		t.Fatalf("invalid owner/device binding: %#v %#v", fixture.Owner, fixture.Device)
	}
	if fixture.Capabilities.AvailableCPUCores > fixture.Capabilities.CPUCores || fixture.Capabilities.AvailableMemoryBytes > fixture.Capabilities.MemoryBytes || fixture.Capabilities.AvailableStorageBytes > fixture.Capabilities.StorageBytes {
		t.Fatal("capability availability exceeds capacity")
	}
	if len(fixture.Images) != 2 || fixture.Images[0].Revision != 1 || fixture.Images[1].Revision != 2 {
		t.Fatalf("unexpected image sequence: %#v", fixture.Images)
	}
	for index, image := range fixture.Images {
		if image.Revision == 0 || image.Heartbeat.Revision != image.Revision || image.Inventory.Revision != image.Revision || image.Expected.Revision != image.Revision || image.Expected.HeartbeatRevision != image.Revision || image.Expected.InventoryRevision != image.Revision {
			t.Fatalf("image %d has split revisions: %#v", index, image)
		}
		if image.Heartbeat.DeviceID != fixture.Device.DeviceID || image.Inventory.DeviceID != fixture.Device.DeviceID || image.Inventory.RunnerDeviceID != image.Heartbeat.DeviceID || image.Inventory.RunnerInstanceID != image.Heartbeat.InstanceID || image.Inventory.RunnerGeneration != image.Heartbeat.Generation || image.Inventory.RunnerHeartbeatSequence != image.Heartbeat.HeartbeatSequence {
			t.Fatalf("image %d has split binding: %#v", index, image)
		}
		if image.Heartbeat.Generation == 0 || image.Heartbeat.HeartbeatSequence == 0 || image.Heartbeat.CapabilityLeaseExpiresAtMS <= image.Heartbeat.ServerObservedAtMS || image.Inventory.Reserved {
			t.Fatalf("image %d has invalid heartbeat/inventory: %#v", index, image)
		}
	}
	if len(fixture.Cases) != 7 {
		t.Fatalf("cases=%d, want 7", len(fixture.Cases))
	}
	for _, testCase := range fixture.Cases {
		if testCase.Name == "" || testCase.Expected.Accepted || testCase.Expected.Error == "" {
			t.Fatalf("case is not a rejection vector: %#v", testCase)
		}
	}
}

func TestForgeDeviceEnrollmentHeartbeatLifecyclePersistenceRejectsMutation(t *testing.T) {
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
			if err := json.Unmarshal(forgeDeviceEnrollmentHeartbeatLifecyclePersistenceFixture, &root); err != nil {
				t.Fatal(err)
			}
			mutation.mutate(root)
			raw, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			var fixture forgeLifecycleFixture
			if err := decodeForgeLifecyclePersistence(raw, &fixture); err == nil {
				t.Fatal("mutated lifecycle persistence fixture unexpectedly accepted")
			}
		})
	}
	duplicate := bytes.TrimSpace(forgeDeviceEnrollmentHeartbeatLifecyclePersistenceFixture)
	duplicate = append(bytes.TrimSuffix(duplicate, []byte("}")), []byte(`,"schema_version":"forge.device-enrollment-heartbeat-lifecycle-persistence/v1"}`)...)
	var fixture forgeLifecycleFixture
	if err := decodeForgeLifecyclePersistence(duplicate, &fixture); err == nil {
		t.Fatal("duplicate field unexpectedly accepted")
	}
}
