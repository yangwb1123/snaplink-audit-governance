package forge

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// This receiver-side contract is a pure device credential metadata plan. It
// authenticates no caller, creates no bearer material, persists no state, and
// grants no inventory or execution authority.
//
//go:embed testdata/forge-device-credential-lifecycle-v1.json
var forgeDeviceCredentialLifecycleFixture []byte

const forgeDeviceCredentialLifecycleNotice = "This fixture is a pure device credential metadata replacement plan. It authenticates no caller, creates no secret, writes no state, and grants no inventory or execution authority."

type forgeCredentialOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeCredentialState struct {
	CredentialID    string               `json:"credential_id"`
	DeviceID        string               `json:"device_id"`
	Owner           forgeCredentialOwner `json:"owner"`
	ApprovalState   string               `json:"approval_state"`
	CredentialState string               `json:"credential_state"`
	KeyID           string               `json:"key_id"`
	PublicKeySHA256 string               `json:"public_key_sha256"`
	KeyGeneration   uint64               `json:"key_generation"`
	IssuedAtMS      uint64               `json:"issued_at_ms"`
	ExpiresAtMS     uint64               `json:"expires_at_ms"`
}

type forgeCredentialAuthority struct {
	OwnerAuthenticated     bool `json:"owner_authenticated"`
	CredentialMaterialMade bool `json:"credential_material_made"`
	Persisted              bool `json:"persisted"`
	InventoryAuthoritative bool `json:"inventory_authoritative"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
}

type forgeCredentialCase struct {
	Name                string                `json:"name"`
	Action              string                `json:"action"`
	Current             *forgeCredentialState `json:"current,omitempty"`
	Owner               forgeCredentialOwner  `json:"owner"`
	DeviceID            string                `json:"device_id"`
	ApprovalState       string                `json:"approval_state"`
	CredentialID        string                `json:"credential_id"`
	KeyID               string                `json:"key_id"`
	PublicKeySHA256     string                `json:"public_key_sha256"`
	KeyGeneration       uint64                `json:"key_generation"`
	IssuedAtMS          uint64                `json:"issued_at_ms"`
	ExpiresAtMS         uint64                `json:"expires_at_ms"`
	NextCredentialID    string                `json:"next_credential_id"`
	NextKeyID           string                `json:"next_key_id"`
	NextPublicKeySHA256 string                `json:"next_public_key_sha256"`
	ObservedAtMS        uint64                `json:"observed_at_ms"`
	ExpectedState       *forgeCredentialState `json:"expected_state,omitempty"`
	ExpectedError       string                `json:"expected_error,omitempty"`
}

type forgeCredentialFixture struct {
	SchemaVersion  string                   `json:"schema_version"`
	EvaluationMode string                   `json:"evaluation_mode"`
	Notice         string                   `json:"notice"`
	Authority      forgeCredentialAuthority `json:"authority"`
	Cases          []forgeCredentialCase    `json:"cases"`
}

func decodeForgeDeviceCredentialLifecycle(raw []byte) (forgeCredentialFixture, error) {
	if err := rejectForgeDeviceCredentialLifecycleDuplicateKeys(raw); err != nil {
		return forgeCredentialFixture{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var fixture forgeCredentialFixture
	if err := decoder.Decode(&fixture); err != nil {
		return forgeCredentialFixture{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return forgeCredentialFixture{}, errors.New("trailing JSON value")
		}
		return forgeCredentialFixture{}, err
	}
	return fixture, nil
}

func validForgeCredentialToken(value string, max int) bool {
	return value != "" && len(value) <= max && value == string(bytes.TrimSpace([]byte(value))) && !strings.ContainsAny(value, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f")
}

func validForgeCredentialDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == string(bytes.ToLower([]byte(value)))
}

func validForgeCredentialOwner(owner forgeCredentialOwner) bool {
	return validForgeCredentialToken(owner.Issuer, 512) && validForgeCredentialToken(owner.Subject, 512) && validForgeCredentialToken(owner.TenantID, 512)
}

func validForgeCredentialState(state *forgeCredentialState) bool {
	if state == nil || !validForgeCredentialToken(state.CredentialID, 128) || !validForgeCredentialToken(state.DeviceID, 128) || !validForgeCredentialOwner(state.Owner) || !validForgeCredentialToken(state.KeyID, 128) || !validForgeCredentialDigest(state.PublicKeySHA256) || state.KeyGeneration == 0 || state.IssuedAtMS >= state.ExpiresAtMS {
		return false
	}
	if state.ApprovalState != "pending" && state.ApprovalState != "approved" && state.ApprovalState != "revoked" {
		return false
	}
	return state.CredentialState == "active" || state.CredentialState == "expired" || state.CredentialState == "revoked"
}

func validForgeCredentialLifecycleFixture(fixture forgeCredentialFixture) bool {
	if fixture.SchemaVersion != "forge.device-credential-lifecycle/v1" || fixture.EvaluationMode != "pure_device_credential_lifecycle" || fixture.Notice != forgeDeviceCredentialLifecycleNotice || fixture.Authority != (forgeCredentialAuthority{}) || len(fixture.Cases) != 6 {
		return false
	}
	names := map[string]struct{}{}
	for _, testCase := range fixture.Cases {
		if !validForgeCredentialToken(testCase.Name, 128) || testCase.Action == "" || !validForgeCredentialOwner(testCase.Owner) || !validForgeCredentialToken(testCase.DeviceID, 128) {
			return false
		}
		if _, exists := names[testCase.Name]; exists {
			return false
		}
		names[testCase.Name] = struct{}{}
		if testCase.ExpectedError != "" {
			if testCase.ExpectedState != nil || (testCase.ExpectedError != "credential_terminal" && testCase.ExpectedError != "credential_lifetime_too_long") {
				return false
			}
			continue
		}
		if !validForgeCredentialState(testCase.ExpectedState) || testCase.ExpectedState.Owner != testCase.Owner || testCase.ExpectedState.DeviceID != testCase.DeviceID {
			return false
		}
		switch testCase.Action {
		case "issue":
			if testCase.Current != nil || (testCase.ApprovalState != "pending" && testCase.ApprovalState != "approved") || testCase.ObservedAtMS < testCase.IssuedAtMS || testCase.ObservedAtMS >= testCase.ExpiresAtMS || testCase.ExpectedState.CredentialState != "active" || testCase.ExpectedState.ApprovalState != testCase.ApprovalState || testCase.ExpectedState.CredentialID != testCase.CredentialID || testCase.ExpectedState.KeyID != testCase.KeyID || testCase.ExpectedState.PublicKeySHA256 != testCase.PublicKeySHA256 || testCase.ExpectedState.KeyGeneration != testCase.KeyGeneration || testCase.ExpectedState.IssuedAtMS != testCase.IssuedAtMS || testCase.ExpectedState.ExpiresAtMS != testCase.ExpiresAtMS {
				return false
			}
		case "revoke":
			if testCase.Current == nil || !validForgeCredentialState(testCase.Current) || testCase.ExpectedState.CredentialState != "revoked" || testCase.ExpectedState.CredentialID != testCase.Current.CredentialID || testCase.ExpectedState.KeyID != testCase.Current.KeyID || testCase.ExpectedState.KeyGeneration != testCase.Current.KeyGeneration {
				return false
			}
		case "rotate":
			if testCase.Current == nil || !validForgeCredentialState(testCase.Current) || testCase.ObservedAtMS < testCase.IssuedAtMS || testCase.ObservedAtMS >= testCase.ExpiresAtMS || testCase.ExpectedState.CredentialState != "active" || testCase.ExpectedState.ApprovalState != testCase.Current.ApprovalState || testCase.ExpectedState.CredentialID != testCase.NextCredentialID || testCase.ExpectedState.KeyID != testCase.NextKeyID || testCase.ExpectedState.PublicKeySHA256 != testCase.NextPublicKeySHA256 || testCase.ExpectedState.KeyGeneration != testCase.Current.KeyGeneration+1 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func TestForgeDeviceCredentialLifecycleContractFixture(t *testing.T) {
	fixture, err := decodeForgeDeviceCredentialLifecycle(forgeDeviceCredentialLifecycleFixture)
	if err != nil {
		t.Fatalf("decode device credential lifecycle fixture: %v", err)
	}
	if !validForgeCredentialLifecycleFixture(fixture) {
		t.Fatalf("invalid device credential lifecycle fixture: %#v", fixture)
	}
	for _, testCase := range fixture.Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			if testCase.ExpectedError != "" {
				if testCase.ExpectedState != nil {
					t.Fatal("rejected case carries replacement state")
				}
				return
			}
			if testCase.ExpectedState == nil || testCase.ExpectedState.Owner != testCase.Owner || testCase.ExpectedState.DeviceID != testCase.DeviceID {
				t.Fatalf("accepted case loses immutable binding: %#v", testCase.ExpectedState)
			}
		})
	}
}

func TestForgeDeviceCredentialLifecycleRejectsMutation(t *testing.T) {
	var unknown map[string]json.RawMessage
	if err := json.Unmarshal(forgeDeviceCredentialLifecycleFixture, &unknown); err != nil {
		t.Fatal(err)
	}
	unknown["credential_material"] = json.RawMessage(`"secret"`)
	mutated, err := json.Marshal(unknown)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeForgeDeviceCredentialLifecycle(mutated); err == nil {
		t.Fatal("secret-bearing unknown field accepted")
	}

	duplicate := []byte(`{"schema_version":"forge.device-credential-lifecycle/v1","schema_version":"forge.device-credential-lifecycle/v1"}`)
	if _, err := decodeForgeDeviceCredentialLifecycle(duplicate); err == nil {
		t.Fatal("duplicate field accepted")
	}

	var authority map[string]any
	if err := json.Unmarshal(forgeDeviceCredentialLifecycleFixture, &authority); err != nil {
		t.Fatal(err)
	}
	authority["authority"].(map[string]any)["persisted"] = true
	mutated, err = json.Marshal(authority)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeForgeDeviceCredentialLifecycle(mutated)
	if err != nil {
		t.Fatalf("authority mutation should retain shape: %v", err)
	}
	if validForgeCredentialLifecycleFixture(decoded) {
		t.Fatal("authority mutation accepted")
	}

	trailing := append(append([]byte(nil), forgeDeviceCredentialLifecycleFixture...), []byte(` true`)...)
	if _, err := decodeForgeDeviceCredentialLifecycle(trailing); err == nil {
		t.Fatal("trailing JSON value accepted")
	}
}

func rejectForgeDeviceCredentialLifecycleDuplicateKeys(raw []byte) error {
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
