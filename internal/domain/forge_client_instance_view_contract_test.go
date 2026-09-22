package domain

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

// These mirrors are receiver-side compatibility evidence for the display-only
// client-instance views. Aero-ID does not authenticate an instance, publish
// an audit event, enroll a device, or grant execution authority from either
// value.
//
//go:embed testdata/forge-client-instance-session-view-v1.json
var forgeClientInstanceSessionViewFixture []byte

//go:embed testdata/forge-client-instance-resource-view-v1.json
var forgeClientInstanceResourceViewFixture []byte

type forgeClientInstanceOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeClientInstanceViewInstance struct {
	InstanceID   string   `json:"instance_id"`
	ClientKind   string   `json:"client_kind"`
	SessionIDs   []string `json:"session_ids"`
	ObservedAtMS int64    `json:"observed_at_ms"`
	Status       string   `json:"status"`
}

type forgeClientInstanceViewAuthority struct {
	OwnerAuthenticated     bool `json:"owner_authenticated"`
	SessionReadAuthorized  bool `json:"session_read_authorized"`
	PromptWriteAuthorized  bool `json:"prompt_write_authorized"`
	DeviceIdentityVerified bool `json:"device_identity_verified"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
	AuditPublished         bool `json:"audit_published"`
}

type forgeClientInstanceSessionView struct {
	SchemaVersion              string                            `json:"schema_version"`
	EvaluationMode             string                            `json:"evaluation_mode"`
	Owner                      forgeClientInstanceOwner          `json:"owner_declaration"`
	OwnerDeclarationUnverified bool                              `json:"owner_declaration_unverified"`
	Instances                  []forgeClientInstanceViewInstance `json:"instances"`
	ReadOnly                   bool                              `json:"read_only"`
	Authority                  forgeClientInstanceViewAuthority  `json:"authority"`
}

type forgeClientInstanceResourceDevice struct {
	DeviceID                string                   `json:"device_id"`
	RunnerInstanceID        string                   `json:"runner_instance_id"`
	Owner                   forgeClientInstanceOwner `json:"owner"`
	Revision                uint64                   `json:"revision"`
	Generation              uint64                   `json:"generation"`
	HeartbeatSequence       uint64                   `json:"heartbeat_sequence"`
	ObservedAtMS            int64                    `json:"observed_at_ms"`
	ApprovalState           string                   `json:"approval_state"`
	CordonState             string                   `json:"cordon_state"`
	ReservationState        string                   `json:"reservation_state"`
	Liveness                string                   `json:"liveness"`
	OS                      string                   `json:"os"`
	Architecture            string                   `json:"architecture"`
	CPUCores                uint32                   `json:"cpu_cores"`
	AvailableCPUCores       uint32                   `json:"available_cpu_cores"`
	MemoryBytes             uint64                   `json:"memory_bytes"`
	AvailableMemoryBytes    uint64                   `json:"available_memory_bytes"`
	StorageBytes            uint64                   `json:"storage_bytes"`
	AvailableStorageBytes   uint64                   `json:"available_storage_bytes"`
	GPUCount                uint32                   `json:"gpu_count"`
	AvailableGPUMemoryBytes uint64                   `json:"available_gpu_memory_bytes"`
}

type forgeClientInstanceResourceView struct {
	SchemaVersion              string                            `json:"schema_version"`
	EvaluationMode             string                            `json:"evaluation_mode"`
	Owner                      forgeClientInstanceOwner          `json:"owner_declaration"`
	OwnerDeclarationUnverified bool                              `json:"owner_declaration_unverified"`
	Instances                  []forgeClientInstanceViewInstance `json:"instances"`
	Devices                    []forgeClientResourceDevice       `json:"devices"`
	DeviceAttributesUnverified bool                              `json:"device_attributes_unverified"`
	ReadOnly                   bool                              `json:"read_only"`
	Authority                  forgeClientInstanceViewAuthority  `json:"authority"`
}

// forgeClientResourceDevice is kept as an alias so both view types share the
// exact owner and resource field contract without duplicating validation code.
type forgeClientResourceDevice = forgeClientInstanceResourceDevice

func decodeForgeClientInstanceView(raw []byte, value any) error {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	switch typed := value.(type) {
	case *forgeClientInstanceSessionView:
		if typed.SchemaVersion != "forge.client-instance-session-view/v1" ||
			typed.EvaluationMode != "owner_bound_session_view_only" ||
			!typed.OwnerDeclarationUnverified || !typed.ReadOnly ||
			typed.Authority != (forgeClientInstanceViewAuthority{}) {
			return errors.New("client-instance session view is not authority-free")
		}
	case *forgeClientInstanceResourceView:
		if typed.SchemaVersion != "forge.client-instance-resource-view/v1" ||
			typed.EvaluationMode != "owner_bound_instance_resource_view_only" ||
			!typed.OwnerDeclarationUnverified || !typed.DeviceAttributesUnverified ||
			!typed.ReadOnly || typed.Authority != (forgeClientInstanceViewAuthority{}) {
			return errors.New("client-instance resource view is not authority-free")
		}
		for _, device := range typed.Devices {
			if device.Owner != typed.Owner {
				return errors.New("client-instance resource device has a foreign owner")
			}
		}
	}
	return nil
}

func TestForgeClientInstanceViewsAreCanonicalAndAuthorityFree(t *testing.T) {
	var session forgeClientInstanceSessionView
	if err := decodeForgeClientInstanceView(forgeClientInstanceSessionViewFixture, &session); err != nil {
		t.Fatalf("decode client-instance session view: %v", err)
	}
	if session.SchemaVersion != "forge.client-instance-session-view/v1" ||
		session.EvaluationMode != "owner_bound_session_view_only" ||
		!session.OwnerDeclarationUnverified || !session.ReadOnly ||
		session.Authority != (forgeClientInstanceViewAuthority{}) {
		t.Fatalf("session view boundary = %#v", session)
	}
	assertClientInstanceRows(t, session.Owner, session.Instances)

	var resource forgeClientInstanceResourceView
	if err := decodeForgeClientInstanceView(forgeClientInstanceResourceViewFixture, &resource); err != nil {
		t.Fatalf("decode client-instance resource view: %v", err)
	}
	if resource.SchemaVersion != "forge.client-instance-resource-view/v1" ||
		resource.EvaluationMode != "owner_bound_instance_resource_view_only" ||
		!resource.OwnerDeclarationUnverified || !resource.DeviceAttributesUnverified ||
		!resource.ReadOnly || resource.Authority != (forgeClientInstanceViewAuthority{}) {
		t.Fatalf("resource view boundary = %#v", resource)
	}
	assertClientInstanceRows(t, resource.Owner, resource.Instances)
	if len(resource.Devices) != 2 || resource.Devices[0].DeviceID != "device-a" ||
		resource.Devices[1].DeviceID != "device-b" {
		t.Fatalf("resource rows are not canonical: %#v", resource.Devices)
	}
	for index, device := range resource.Devices {
		if device.Owner != resource.Owner || device.Revision == 0 || device.Generation == 0 ||
			device.HeartbeatSequence == 0 || device.ObservedAtMS <= 0 ||
			device.AvailableCPUCores > device.CPUCores ||
			device.AvailableMemoryBytes > device.MemoryBytes ||
			device.AvailableStorageBytes > device.StorageBytes {
			t.Fatalf("resource device %d is invalid or foreign: %#v", index, device)
		}
	}
}

func assertClientInstanceRows(t *testing.T, owner forgeClientInstanceOwner, rows []forgeClientInstanceViewInstance) {
	t.Helper()
	if owner.Issuer == "" || owner.Subject == "" || owner.TenantID == "" || len(rows) != 5 {
		t.Fatalf("owner/rows are not bounded: owner=%#v rows=%#v", owner, rows)
	}
	wantKinds := []string{"app", "cli", "mobile", "tui", "web"}
	for index, row := range rows {
		if row.InstanceID == "" || row.ObservedAtMS <= 0 || row.Status == "" ||
			row.ClientKind != wantKinds[index] ||
			(index > 0 && rows[index-1].InstanceID >= row.InstanceID) {
			t.Fatalf("client instance ordering/bounds invalid: %#v", rows)
		}
		for sessionIndex, sessionID := range row.SessionIDs {
			if sessionID == "" || (sessionIndex > 0 && row.SessionIDs[sessionIndex-1] >= sessionID) {
				t.Fatalf("session ordering/bounds invalid: %#v", row)
			}
		}
	}
}

func TestForgeClientInstanceViewsRejectUnknownAuthorityDuplicateAndForeignOwner(t *testing.T) {
	tests := []struct {
		name   string
		base   []byte
		mutate func(map[string]json.RawMessage)
	}{
		{"session authority", forgeClientInstanceSessionViewFixture, func(root map[string]json.RawMessage) {
			root["authority"] = json.RawMessage(`{"audit_published":true}`)
		}},
		{"resource unknown", forgeClientInstanceResourceViewFixture, func(root map[string]json.RawMessage) {
			root["unexpected"] = json.RawMessage(`true`)
		}},
		{"resource foreign device owner", forgeClientInstanceResourceViewFixture, func(root map[string]json.RawMessage) {
			var devices []map[string]json.RawMessage
			if err := json.Unmarshal(root["devices"], &devices); err != nil {
				t.Fatal(err)
			}
			devices[0]["owner"] = json.RawMessage(`{"issuer":"https://id.example","subject":"other","tenant_id":"tenant-1"}`)
			encoded, err := json.Marshal(devices)
			if err != nil {
				t.Fatal(err)
			}
			root["devices"] = encoded
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var root map[string]json.RawMessage
			if err := json.Unmarshal(tc.base, &root); err != nil {
				t.Fatal(err)
			}
			tc.mutate(root)
			mutated, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			var value any
			if bytes.Contains(tc.base, []byte("resource-view")) {
				value = &forgeClientInstanceResourceView{}
			} else {
				value = &forgeClientInstanceSessionView{}
			}
			if err := decodeForgeClientInstanceView(mutated, value); err == nil {
				t.Fatal("mutated view unexpectedly accepted")
			}
		})
	}
	duplicate := strings.TrimSpace(string(forgeClientInstanceSessionViewFixture))
	duplicate = strings.TrimSuffix(duplicate, "}") + `,"schema_version":"forge.client-instance-session-view/v1"}`
	if err := decodeForgeClientInstanceView([]byte(duplicate), &forgeClientInstanceSessionView{}); err == nil {
		t.Fatal("duplicate client-instance field accepted")
	}
}

func TestForgeClientInstanceViewDoesNotCarrySensitiveOrEffectFields(t *testing.T) {
	for name, fixture := range map[string][]byte{
		"session":  forgeClientInstanceSessionViewFixture,
		"resource": forgeClientInstanceResourceViewFixture,
	} {
		t.Run(name, func(t *testing.T) {
			var root map[string]json.RawMessage
			if err := json.Unmarshal(fixture, &root); err != nil {
				t.Fatal(err)
			}
			for _, field := range []string{"prompt", "result", "output", "token", "credential", "argv", "workspace", "receipt", "lease_token"} {
				if _, ok := root[field]; ok {
					t.Fatalf("sensitive/effect field %q crossed view boundary", field)
				}
			}
		})
	}
}
