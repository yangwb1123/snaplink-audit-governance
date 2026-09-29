package forge

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
)

// This receiver-side contract is a pure owner/device lifecycle replacement
// plan. It authenticates no caller, persists no enrollment state, issues no
// credential, and grants no inventory or execution authority.
//
//go:embed testdata/forge-device-approval-rotation-v1.json
var forgeDeviceApprovalRotationFixture []byte

type forgeApprovalOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeApprovalState struct {
	DeviceID        string             `json:"device_id"`
	Owner           forgeApprovalOwner `json:"owner"`
	ApprovalState   string             `json:"approval_state"`
	KeyID           string             `json:"key_id"`
	PublicKeySHA256 string             `json:"public_key_sha256"`
	KeyGeneration   uint64             `json:"key_generation"`
}

type forgeApprovalAuthority struct {
	OwnerAuthenticated     bool `json:"owner_authenticated"`
	Persisted              bool `json:"persisted"`
	CredentialIssued       bool `json:"credential_issued"`
	InventoryAuthoritative bool `json:"inventory_authoritative"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
}

type forgeApprovalCase struct {
	Name                string              `json:"name"`
	Action              string              `json:"action"`
	Owner               forgeApprovalOwner  `json:"owner"`
	DeviceID            string              `json:"device_id"`
	NextKeyID           string              `json:"next_key_id"`
	NextPublicKeySHA256 string              `json:"next_public_key_sha256"`
	ExpectedState       *forgeApprovalState `json:"expected_state"`
	ExpectedError       string              `json:"expected_error"`
}

type forgeApprovalFixture struct {
	SchemaVersion  string                 `json:"schema_version"`
	EvaluationMode string                 `json:"evaluation_mode"`
	Notice         string                 `json:"notice"`
	Initial        forgeApprovalState     `json:"initial"`
	Authority      forgeApprovalAuthority `json:"authority"`
	Cases          []forgeApprovalCase    `json:"cases"`
}

func decodeForgeDeviceApprovalRotation(raw []byte) (forgeApprovalFixture, error) {
	if err := rejectForgeDeviceApprovalRotationDuplicateKeys(raw); err != nil {
		return forgeApprovalFixture{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var fixture forgeApprovalFixture
	if err := decoder.Decode(&fixture); err != nil {
		return forgeApprovalFixture{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return forgeApprovalFixture{}, errors.New("trailing JSON value")
		}
		return forgeApprovalFixture{}, err
	}
	return fixture, nil
}

func validForgeDeviceApprovalRotationFixture(fixture forgeApprovalFixture) bool {
	if fixture.SchemaVersion != "forge.device-approval-rotation/v1" ||
		fixture.EvaluationMode != "pure_owner_device_lifecycle" ||
		fixture.Notice == "" || fixture.Initial.DeviceID == "" ||
		fixture.Initial.Owner.Issuer == "" || fixture.Initial.Owner.Subject == "" ||
		fixture.Initial.Owner.TenantID == "" || fixture.Initial.ApprovalState != "pending" ||
		fixture.Initial.KeyID == "" || len(fixture.Initial.PublicKeySHA256) != 64 ||
		fixture.Initial.KeyGeneration != 1 || fixture.Authority != (forgeApprovalAuthority{}) ||
		len(fixture.Cases) != 7 {
		return false
	}
	for _, testCase := range fixture.Cases {
		if testCase.Name == "" || testCase.Action == "" || testCase.DeviceID == "" {
			return false
		}
		if testCase.ExpectedError != "" {
			if testCase.ExpectedState != nil {
				return false
			}
			continue
		}
		if testCase.Owner != fixture.Initial.Owner || testCase.DeviceID != fixture.Initial.DeviceID {
			return false
		}
		if testCase.ExpectedState == nil || testCase.ExpectedState.DeviceID != fixture.Initial.DeviceID ||
			testCase.ExpectedState.Owner != fixture.Initial.Owner ||
			testCase.ExpectedState.KeyID == "" || len(testCase.ExpectedState.PublicKeySHA256) != 64 ||
			testCase.ExpectedState.KeyGeneration == 0 {
			return false
		}
	}
	return true
}

func TestForgeDeviceApprovalRotationContractFixture(t *testing.T) {
	fixture, err := decodeForgeDeviceApprovalRotation(forgeDeviceApprovalRotationFixture)
	if err != nil {
		t.Fatalf("decode device approval/rotation fixture: %v", err)
	}
	if !validForgeDeviceApprovalRotationFixture(fixture) {
		t.Fatalf("invalid device approval/rotation fixture: %#v", fixture)
	}
	for _, testCase := range fixture.Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			if testCase.ExpectedError != "" {
				if testCase.ExpectedState != nil {
					t.Fatal("rejected case carries a replacement state")
				}
				return
			}
			if testCase.ExpectedState == nil || testCase.ExpectedState.Owner != fixture.Initial.Owner ||
				testCase.ExpectedState.DeviceID != fixture.Initial.DeviceID {
				t.Fatalf("accepted case loses immutable owner/device binding: %#v", testCase.ExpectedState)
			}
		})
	}
}

func TestForgeDeviceApprovalRotationRejectsMutation(t *testing.T) {
	var unknown map[string]json.RawMessage
	if err := json.Unmarshal(forgeDeviceApprovalRotationFixture, &unknown); err != nil {
		t.Fatal(err)
	}
	unknown["unexpected"] = json.RawMessage(`true`)
	mutated, err := json.Marshal(unknown)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeForgeDeviceApprovalRotation(mutated); err == nil {
		t.Fatal("unknown field accepted")
	}

	duplicate := []byte(`{"schema_version":"forge.device-approval-rotation/v1","schema_version":"forge.device-approval-rotation/v1"}`)
	if _, err := decodeForgeDeviceApprovalRotation(duplicate); err == nil {
		t.Fatal("duplicate field accepted")
	}

	var authority map[string]any
	if err := json.Unmarshal(forgeDeviceApprovalRotationFixture, &authority); err != nil {
		t.Fatal(err)
	}
	authority["authority"].(map[string]any)["persisted"] = true
	mutated, err = json.Marshal(authority)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeForgeDeviceApprovalRotation(mutated)
	if err != nil {
		t.Fatalf("authority mutation should retain shape: %v", err)
	}
	if reflect.DeepEqual(decoded.Authority, forgeApprovalAuthority{}) {
		t.Fatal("authority mutation was not observable")
	}
	if validForgeDeviceApprovalRotationFixture(decoded) {
		t.Fatal("authority mutation accepted as authority-free")
	}
}

func rejectForgeDeviceApprovalRotationDuplicateKeys(raw []byte) error {
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
			return nil
		}
	}
	if err := walk(); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}
