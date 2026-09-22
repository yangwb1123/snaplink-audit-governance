package domain

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"testing"
)

// This receiver-side contract consumes a bounded persisted-inventory CAS and
// projection image. It does not authenticate a device, persist a row, make
// inventory authoritative, reserve capacity, schedule work, or grant Audit
// authority.
//
//go:embed testdata/forge-device-inventory-persistence-v1.json
var forgeDeviceInventoryPersistenceFixture []byte

type forgeInventoryPersistenceOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeInventoryPersistenceAuthority struct {
	IdentityVerified       bool `json:"identity_verified"`
	HeartbeatPersisted     bool `json:"heartbeat_persisted"`
	InventoryAuthoritative bool `json:"inventory_authoritative"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
}

type forgeInventoryPersistenceCapabilities struct {
	OS                    string   `json:"os"`
	Architecture          string   `json:"architecture"`
	CPUCores              uint64   `json:"cpu_cores"`
	AvailableCPUCores     uint64   `json:"available_cpu_cores"`
	MemoryBytes           uint64   `json:"memory_bytes"`
	AvailableMemoryBytes  uint64   `json:"available_memory_bytes"`
	StorageBytes          uint64   `json:"storage_bytes"`
	AvailableStorageBytes uint64   `json:"available_storage_bytes"`
	GPUs                  []any    `json:"gpus"`
	Runtimes              []string `json:"runtimes"`
}

type forgeInventoryPersistenceDevice struct {
	DeviceID         string                         `json:"device_id"`
	Owner            forgeInventoryPersistenceOwner `json:"owner"`
	ApprovalState    string                         `json:"approval_state"`
	CordonState      string                         `json:"cordon_state"`
	ReservationState string                         `json:"reservation_state"`
}

type forgeInventoryPersistenceRunner struct {
	DeviceID                   string                                `json:"device_id"`
	InstanceID                 string                                `json:"instance_id"`
	Generation                 uint64                                `json:"generation"`
	HeartbeatSequence          uint64                                `json:"heartbeat_sequence"`
	ServerObservedAtMS         uint64                                `json:"server_observed_at_ms"`
	CapabilityLeaseExpiresAtMS uint64                                `json:"capability_lease_expires_at_ms"`
	Liveness                   string                                `json:"liveness"`
	Capabilities               forgeInventoryPersistenceCapabilities `json:"capabilities"`
}

type forgeInventoryPersistenceState struct {
	Revision uint64                          `json:"revision"`
	Device   forgeInventoryPersistenceDevice `json:"device"`
	Runner   forgeInventoryPersistenceRunner `json:"runner"`
}

type forgeInventoryPersistenceReplacement struct {
	HeartbeatSequence          uint64 `json:"heartbeat_sequence"`
	ServerObservedAtMS         uint64 `json:"server_observed_at_ms"`
	CapabilityLeaseExpiresAtMS uint64 `json:"capability_lease_expires_at_ms"`
}

type forgeInventoryPersistenceExpected struct {
	Accepted                   bool   `json:"accepted"`
	Error                      string `json:"error"`
	Revision                   uint64 `json:"revision"`
	DeviceID                   string `json:"device_id"`
	InstanceID                 string `json:"instance_id"`
	HeartbeatSequence          uint64 `json:"heartbeat_sequence"`
	ServerObservedAtMS         uint64 `json:"server_observed_at_ms"`
	CapabilityLeaseExpiresAtMS uint64 `json:"capability_lease_expires_at_ms"`
	Status                     string `json:"status"`
	Fresh                      bool   `json:"fresh"`
	DeclaredEligible           bool   `json:"declared_eligible"`
}

type forgeInventoryPersistenceCase struct {
	Name                string                               `json:"name"`
	Operation           string                               `json:"operation"`
	EvaluationOwner     string                               `json:"evaluation_owner"`
	EvaluatedAtMS       uint64                               `json:"evaluated_at_ms"`
	ExpectedRevision    uint64                               `json:"expected_revision"`
	StateRevision       *uint64                              `json:"state_revision"`
	RunnerDeviceID      string                               `json:"runner_device_id"`
	DeviceApprovalState string                               `json:"device_approval_state"`
	DeviceCordonState   string                               `json:"device_cordon_state"`
	RunnerLiveness      string                               `json:"runner_liveness"`
	Replacement         forgeInventoryPersistenceReplacement `json:"replacement"`
	Expected            forgeInventoryPersistenceExpected    `json:"expected"`
}

type forgeInventoryPersistenceFixture struct {
	SchemaVersion  string                             `json:"schema_version"`
	EvaluationMode string                             `json:"evaluation_mode"`
	StaleAfterMS   uint64                             `json:"stale_after_ms"`
	Authority      forgeInventoryPersistenceAuthority `json:"authority"`
	State          forgeInventoryPersistenceState     `json:"state"`
	Cases          []forgeInventoryPersistenceCase    `json:"cases"`
}

func decodeForgeDeviceInventoryPersistence(raw []byte, out *forgeInventoryPersistenceFixture) error {
	if err := rejectForgeDeviceInventoryPersistenceDuplicateKeys(raw); err != nil {
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
	return validateForgeDeviceInventoryPersistence(out)
}

func validateForgeDeviceInventoryPersistence(fixture *forgeInventoryPersistenceFixture) error {
	if fixture.SchemaVersion != "forge.device-inventory-persistence/v1" ||
		fixture.EvaluationMode != "pure_persisted_inventory_cas_projection" ||
		fixture.StaleAfterMS != 90000 || fixture.Authority != (forgeInventoryPersistenceAuthority{}) ||
		fixture.State.Revision != 3 || len(fixture.Cases) != 12 {
		return fmt.Errorf("invalid inventory persistence envelope")
	}
	owner := forgeInventoryPersistenceOwner{Issuer: "issuer", Subject: "user", TenantID: "tenant"}
	state := fixture.State
	if state.Device != (forgeInventoryPersistenceDevice{
		DeviceID: "device-a", Owner: owner, ApprovalState: "approved", CordonState: "clear", ReservationState: "none",
	}) || state.Runner.DeviceID != "device-a" || state.Runner.InstanceID != "runner-a" ||
		state.Runner.Generation != 1 || state.Runner.HeartbeatSequence != 1 ||
		state.Runner.ServerObservedAtMS != 1000 || state.Runner.CapabilityLeaseExpiresAtMS != 5000 ||
		state.Runner.Liveness != "online" || state.Runner.Capabilities.OS != "linux" ||
		state.Runner.Capabilities.Architecture != "amd64" || state.Runner.Capabilities.CPUCores != 8 ||
		state.Runner.Capabilities.AvailableCPUCores != 7 || state.Runner.Capabilities.MemoryBytes != 17179869184 ||
		state.Runner.Capabilities.AvailableMemoryBytes != 8589934592 || state.Runner.Capabilities.StorageBytes != 107374182400 ||
		state.Runner.Capabilities.AvailableStorageBytes != 53687091200 || len(state.Runner.Capabilities.GPUs) != 0 ||
		len(state.Runner.Capabilities.Runtimes) != 2 || state.Runner.Capabilities.Runtimes[0] != "go" ||
		state.Runner.Capabilities.Runtimes[1] != "rust" {
		return fmt.Errorf("invalid persisted state")
	}
	wantNames := []string{"project_online", "commit_replacement", "revision_conflict", "runner_device_mismatch", "owner_mismatch", "invalid_evaluation_owner", "stale_projection", "pending_projection", "cordoned_projection", "offline_projection", "revoked_projection", "revision_overflow"}
	seen := make(map[string]struct{}, len(fixture.Cases))
	for index, testCase := range fixture.Cases {
		if testCase.Name != wantNames[index] {
			return fmt.Errorf("case ordering drift at %d", index)
		}
		if _, exists := seen[testCase.Name]; exists {
			return fmt.Errorf("duplicate case %q", testCase.Name)
		}
		seen[testCase.Name] = struct{}{}
		if testCase.Expected.Accepted {
			if testCase.Expected.Error != "" {
				return fmt.Errorf("accepted case %q has error", testCase.Name)
			}
		} else {
			if testCase.Expected.Error == "" || testCase.Expected.Revision != 0 || testCase.Expected.DeviceID != "" ||
				testCase.Expected.InstanceID != "" || testCase.Expected.HeartbeatSequence != 0 ||
				testCase.Expected.ServerObservedAtMS != 0 || testCase.Expected.CapabilityLeaseExpiresAtMS != 0 ||
				testCase.Expected.Status != "" || testCase.Expected.Fresh || testCase.Expected.DeclaredEligible {
				return fmt.Errorf("invalid rejected case %q", testCase.Name)
			}
		}
		switch testCase.Name {
		case "project_online":
			if err := validateInventoryProjectionCase(testCase, 1500, "online", true, true, "", "", ""); err != nil {
				return err
			}
		case "stale_projection":
			if err := validateInventoryProjectionCase(testCase, 100001, "stale", false, false, "", "", ""); err != nil {
				return err
			}
		case "pending_projection":
			if err := validateInventoryProjectionCase(testCase, 1500, "pending", true, false, "pending", "", ""); err != nil {
				return err
			}
		case "cordoned_projection":
			if err := validateInventoryProjectionCase(testCase, 1500, "cordoned", true, false, "", "cordoned", ""); err != nil {
				return err
			}
		case "offline_projection":
			if err := validateInventoryProjectionCase(testCase, 1500, "offline", true, false, "", "", "offline"); err != nil {
				return err
			}
		case "revoked_projection":
			if err := validateInventoryProjectionCase(testCase, 1500, "revoked", true, false, "revoked", "", ""); err != nil {
				return err
			}
		case "commit_replacement":
			if testCase.Operation != "commit" || testCase.ExpectedRevision != 3 || !testCase.Expected.Accepted ||
				testCase.Expected.Revision != 4 || testCase.Replacement.HeartbeatSequence != 2 ||
				testCase.Replacement.ServerObservedAtMS != 2000 || testCase.Replacement.CapabilityLeaseExpiresAtMS != 6000 {
				return fmt.Errorf("invalid replacement case")
			}
			if testCase.Expected.HeartbeatSequence != 2 || testCase.Expected.ServerObservedAtMS != 2000 ||
				testCase.Expected.CapabilityLeaseExpiresAtMS != 6000 || testCase.Expected.DeviceID != "" ||
				testCase.Expected.Status != "" || testCase.Expected.Fresh || testCase.Expected.DeclaredEligible {
				return fmt.Errorf("invalid replacement expectation")
			}
		case "revision_conflict":
			if testCase.Operation != "commit" || testCase.ExpectedRevision != 2 || testCase.Expected.Error != "revision_conflict" {
				return fmt.Errorf("invalid conflict case")
			}
		case "runner_device_mismatch":
			if testCase.Operation != "restore" || testCase.RunnerDeviceID != "device-b" || testCase.Expected.Error != "runner_device_mismatch" {
				return fmt.Errorf("invalid runner mismatch case")
			}
		case "owner_mismatch":
			if testCase.Operation != "project" || testCase.EvaluationOwner != "foreign" || testCase.Expected.Error != "owner_mismatch" {
				return fmt.Errorf("invalid owner mismatch case")
			}
		case "invalid_evaluation_owner":
			if testCase.Operation != "project" || testCase.EvaluationOwner != "invalid" || testCase.Expected.Error != "invalid_evaluation_owner" {
				return fmt.Errorf("invalid evaluation-owner case")
			}
		case "revision_overflow":
			if testCase.Operation != "commit" || testCase.StateRevision == nil || *testCase.StateRevision != ^uint64(0) ||
				testCase.ExpectedRevision != ^uint64(0) || testCase.Expected.Error != "revision_overflow" {
				return fmt.Errorf("invalid overflow case")
			}
		}
	}
	return nil
}

func validateInventoryProjectionCase(testCase forgeInventoryPersistenceCase, evaluatedAt uint64, status string, fresh, eligible bool, approval, cordon, liveness string) error {
	if testCase.Operation != "project" || testCase.EvaluationOwner != "same" || testCase.EvaluatedAtMS != evaluatedAt ||
		!testCase.Expected.Accepted || testCase.Expected.Revision != 3 || testCase.Expected.DeviceID != "device-a" ||
		testCase.Expected.InstanceID != "runner-a" || testCase.Expected.Status != status || testCase.Expected.Fresh != fresh ||
		testCase.Expected.DeclaredEligible != eligible || testCase.Expected.HeartbeatSequence != 0 ||
		testCase.Expected.ServerObservedAtMS != 0 || testCase.Expected.CapabilityLeaseExpiresAtMS != 0 ||
		testCase.DeviceApprovalState != approval || testCase.DeviceCordonState != cordon || testCase.RunnerLiveness != liveness {
		return fmt.Errorf("invalid projection case %q", testCase.Name)
	}
	return nil
}

func rejectForgeDeviceInventoryPersistenceDuplicateKeys(raw []byte) error {
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
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	return nil
}

func TestForgeDeviceInventoryPersistenceReceiverContract(t *testing.T) {
	var fixture forgeInventoryPersistenceFixture
	if err := decodeForgeDeviceInventoryPersistence(forgeDeviceInventoryPersistenceFixture, &fixture); err != nil {
		t.Fatalf("decode inventory persistence receiver fixture: %v", err)
	}
	if fixture.Cases[0].Expected.Status != "online" || fixture.Cases[6].Expected.Status != "stale" || fixture.Cases[11].Expected.Error != "revision_overflow" {
		t.Fatalf("fixture lost projection/CAS vectors: %#v", fixture.Cases)
	}
}

func TestForgeDeviceInventoryPersistenceReceiverRejectsMutation(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"unknown", func(root map[string]any) { root["unexpected"] = true }},
		{"authority", func(root map[string]any) { root["authority"].(map[string]any)["inventory_authoritative"] = true }},
		{"foreign_owner", func(root map[string]any) {
			root["state"].(map[string]any)["device"].(map[string]any)["owner"].(map[string]any)["subject"] = "foreign"
		}},
		{"foreign_runner", func(root map[string]any) {
			root["state"].(map[string]any)["runner"].(map[string]any)["device_id"] = "device-b"
		}},
		{"invalid_revision", func(root map[string]any) { root["state"].(map[string]any)["revision"] = 0 }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			var root map[string]any
			if err := json.Unmarshal(forgeDeviceInventoryPersistenceFixture, &root); err != nil {
				t.Fatal(err)
			}
			mutation.mutate(root)
			raw, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			var fixture forgeInventoryPersistenceFixture
			if err := decodeForgeDeviceInventoryPersistence(raw, &fixture); err == nil {
				t.Fatal("mutated inventory persistence fixture unexpectedly accepted")
			}
		})
	}
	duplicate := bytes.TrimSpace(forgeDeviceInventoryPersistenceFixture)
	duplicate = append(bytes.TrimSuffix(duplicate, []byte("}")), []byte(`,"schema_version":"forge.device-inventory-persistence/v1"}`)...)
	var fixture forgeInventoryPersistenceFixture
	if err := decodeForgeDeviceInventoryPersistence(duplicate, &fixture); err == nil {
		t.Fatal("duplicate field unexpectedly accepted")
	}
}
