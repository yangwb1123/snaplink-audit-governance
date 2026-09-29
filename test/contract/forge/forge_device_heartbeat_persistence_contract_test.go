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

// This receiver-side contract consumes a caller-declared compare-and-swap
// image. It does not persist a heartbeat, authenticate a device, make
// inventory authoritative, reserve capacity, dispatch work, or grant Audit
// authority.
//
//go:embed testdata/forge-device-heartbeat-persistence-contract-v1.json
var forgeHeartbeatPersistenceReceiverFixture []byte

type forgeHeartbeatPersistenceReceiverAuthority struct {
	IdentityVerified       bool `json:"identity_verified"`
	HeartbeatPersisted     bool `json:"heartbeat_persisted"`
	InventoryAuthoritative bool `json:"inventory_authoritative"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
}

type forgeHeartbeatPersistenceReceiverDevice struct {
	DeviceID      string `json:"device_id"`
	TenantID      string `json:"tenant_id"`
	ApprovalState string `json:"approval_state"`
}

type forgeHeartbeatPersistenceReceiverHeartbeat struct {
	DeviceID   string `json:"device_id"`
	InstanceID string `json:"instance_id"`
	Generation uint64 `json:"generation"`
	Sequence   uint64 `json:"sequence"`
}

type forgeHeartbeatPersistenceReceiverState struct {
	Revision                   uint64 `json:"revision"`
	DeviceID                   string `json:"device_id"`
	InstanceID                 string `json:"instance_id"`
	Generation                 uint64 `json:"generation"`
	HeartbeatSequence          uint64 `json:"heartbeat_sequence"`
	ServerObservedAtMS         uint64 `json:"server_observed_at_ms"`
	CapabilityLeaseExpiresAtMS uint64 `json:"capability_lease_expires_at_ms"`
}

type forgeHeartbeatPersistenceReceiverExpected struct {
	Accepted                   bool   `json:"accepted"`
	Error                      string `json:"error"`
	Revision                   uint64 `json:"revision"`
	Generation                 uint64 `json:"generation"`
	HeartbeatSequence          uint64 `json:"heartbeat_sequence"`
	ServerObservedAtMS         uint64 `json:"server_observed_at_ms"`
	CapabilityLeaseExpiresAtMS uint64 `json:"capability_lease_expires_at_ms"`
}

type forgeHeartbeatPersistenceReceiverCase struct {
	Name                string                                     `json:"name"`
	ExpectedRevision    uint64                                     `json:"expected_revision"`
	DeviceApprovalState string                                     `json:"device_approval_state"`
	Current             *forgeHeartbeatPersistenceReceiverState    `json:"current"`
	Heartbeat           forgeHeartbeatPersistenceReceiverHeartbeat `json:"heartbeat"`
	ServerObservedAtMS  uint64                                     `json:"server_observed_at_ms"`
	LeaseTTLMS          uint64                                     `json:"lease_ttl_ms"`
	Expected            forgeHeartbeatPersistenceReceiverExpected  `json:"expected"`
}

type forgeHeartbeatPersistenceReceiverFixtureType struct {
	SchemaVersion  string                                     `json:"schema_version"`
	EvaluationMode string                                     `json:"evaluation_mode"`
	Authority      forgeHeartbeatPersistenceReceiverAuthority `json:"authority"`
	Device         forgeHeartbeatPersistenceReceiverDevice    `json:"device"`
	Cases          []forgeHeartbeatPersistenceReceiverCase    `json:"cases"`
}

func decodeForgeHeartbeatPersistenceReceiver(raw []byte, out *forgeHeartbeatPersistenceReceiverFixtureType) error {
	if err := rejectForgeHeartbeatPersistenceReceiverDuplicateKeys(raw); err != nil {
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
	return validateForgeHeartbeatPersistenceReceiver(out)
}

func validateForgeHeartbeatPersistenceReceiver(fixture *forgeHeartbeatPersistenceReceiverFixtureType) error {
	if fixture.SchemaVersion != "forge.device-heartbeat-persistence-contract/v1" ||
		fixture.EvaluationMode != "pure_compare_and_swap_plan" ||
		fixture.Authority != (forgeHeartbeatPersistenceReceiverAuthority{}) ||
		fixture.Device != (forgeHeartbeatPersistenceReceiverDevice{DeviceID: "device-a", TenantID: "tenant-1", ApprovalState: "approved"}) ||
		len(fixture.Cases) != 10 {
		return fmt.Errorf("invalid heartbeat persistence envelope")
	}
	seen := make(map[string]struct{}, len(fixture.Cases))
	for _, testCase := range fixture.Cases {
		if testCase.Name == "" {
			return fmt.Errorf("heartbeat persistence case name is empty")
		}
		if _, exists := seen[testCase.Name]; exists {
			return fmt.Errorf("duplicate heartbeat persistence case %q", testCase.Name)
		}
		seen[testCase.Name] = struct{}{}
		if testCase.Heartbeat.DeviceID == "" || testCase.Heartbeat.InstanceID == "" ||
			testCase.Heartbeat.Generation == 0 || testCase.Heartbeat.Sequence == 0 ||
			testCase.ServerObservedAtMS == 0 || testCase.LeaseTTLMS == 0 {
			return fmt.Errorf("invalid heartbeat persistence input %q", testCase.Name)
		}
		if testCase.Current != nil {
			current := testCase.Current
			if (current.Revision == 0 && testCase.Name != "zero_persisted_revision_rejected") || current.DeviceID == "" || current.InstanceID == "" ||
				current.Generation == 0 || current.HeartbeatSequence == 0 ||
				current.CapabilityLeaseExpiresAtMS <= current.ServerObservedAtMS {
				return fmt.Errorf("invalid persisted state %q", testCase.Name)
			}
		}
		if testCase.Expected.Accepted {
			if testCase.Expected.Error != "" || testCase.Expected.Revision == 0 ||
				testCase.Expected.Generation != testCase.Heartbeat.Generation ||
				testCase.Expected.HeartbeatSequence != testCase.Heartbeat.Sequence ||
				testCase.Expected.ServerObservedAtMS != testCase.ServerObservedAtMS ||
				testCase.Expected.CapabilityLeaseExpiresAtMS != testCase.ServerObservedAtMS+testCase.LeaseTTLMS {
				return fmt.Errorf("invalid accepted expectation %q", testCase.Name)
			}
		} else if testCase.Expected.Error == "" {
			return fmt.Errorf("rejected case %q has no stable error", testCase.Name)
		}
	}
	if fixture.Cases[0].Name != "initial_insert" || !fixture.Cases[0].Expected.Accepted || fixture.Cases[0].Current != nil || fixture.Cases[0].Expected.Revision != 1 {
		return fmt.Errorf("initial CAS vector is invalid")
	}
	if fixture.Cases[1].Name != "next_sequence_same_revision_chain" || !fixture.Cases[1].Expected.Accepted || fixture.Cases[1].Expected.Revision != 2 {
		return fmt.Errorf("revision chain vector is invalid")
	}
	return nil
}

func rejectForgeHeartbeatPersistenceReceiverDuplicateKeys(raw []byte) error {
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

func TestForgeDeviceHeartbeatPersistenceReceiverContract(t *testing.T) {
	var fixture forgeHeartbeatPersistenceReceiverFixtureType
	if err := decodeForgeHeartbeatPersistenceReceiver(forgeHeartbeatPersistenceReceiverFixture, &fixture); err != nil {
		t.Fatalf("decode heartbeat persistence receiver fixture: %v", err)
	}
	if fixture.Cases[0].Heartbeat.DeviceID != fixture.Device.DeviceID || fixture.Cases[0].Heartbeat.InstanceID != "runner-a" {
		t.Fatalf("fixture lost device/Runner binding: %#v", fixture.Cases[0])
	}
	for _, testCase := range fixture.Cases {
		if testCase.Expected.Accepted && testCase.Expected.Revision <= testCase.ExpectedRevision {
			t.Fatalf("accepted CAS result did not advance revision: %#v", testCase)
		}
		if !testCase.Expected.Accepted && testCase.Expected.Revision != 0 {
			t.Fatalf("rejected CAS result exposed a replacement revision: %#v", testCase)
		}
	}
}

func TestForgeDeviceHeartbeatPersistenceReceiverRejectsMutation(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(map[string]json.RawMessage)
	}{
		{"unknown", func(root map[string]json.RawMessage) { root["unexpected"] = json.RawMessage(`true`) }},
		{"authority", func(root map[string]json.RawMessage) {
			root["authority"] = json.RawMessage(`{"heartbeat_persisted":true}`)
		}},
		{"foreign_device", func(root map[string]json.RawMessage) {
			root["device"] = json.RawMessage(`{"device_id":"device-foreign","tenant_id":"tenant-1","approval_state":"approved"}`)
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			var root map[string]json.RawMessage
			if err := json.Unmarshal(forgeHeartbeatPersistenceReceiverFixture, &root); err != nil {
				t.Fatal(err)
			}
			mutation.mutate(root)
			raw, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			var fixture forgeHeartbeatPersistenceReceiverFixtureType
			if err := decodeForgeHeartbeatPersistenceReceiver(raw, &fixture); err == nil {
				t.Fatal("mutated heartbeat persistence fixture unexpectedly accepted")
			}
		})
	}
	duplicate := bytes.TrimSpace(forgeHeartbeatPersistenceReceiverFixture)
	duplicate = append(bytes.TrimSuffix(duplicate, []byte("}")), []byte(`,"schema_version":"forge.device-heartbeat-persistence-contract/v1"}`)...)
	var fixture forgeHeartbeatPersistenceReceiverFixtureType
	if err := decodeForgeHeartbeatPersistenceReceiver(duplicate, &fixture); err == nil {
		t.Fatal("duplicate field unexpectedly accepted")
	}
}
