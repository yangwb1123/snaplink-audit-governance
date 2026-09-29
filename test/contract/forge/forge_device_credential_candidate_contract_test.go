package forge

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

// This receiver-side contract is a metadata-only response from the injected
// credential lifecycle candidate. It authenticates no caller, creates no
// secret, and grants no inventory or execution authority.
//
//go:embed testdata/forge-device-credential-candidate-v1.json
var forgeDeviceCredentialCandidateFixture []byte

type forgeCredentialCandidateOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeCredentialCandidateState struct {
	CredentialID    string                        `json:"credential_id"`
	DeviceID        string                        `json:"device_id"`
	Owner           forgeCredentialCandidateOwner `json:"owner"`
	ApprovalState   string                        `json:"approval_state"`
	CredentialState string                        `json:"credential_state"`
	KeyID           string                        `json:"key_id"`
	PublicKeySHA256 string                        `json:"public_key_sha256"`
	KeyGeneration   uint64                        `json:"key_generation"`
	IssuedAtMS      uint64                        `json:"issued_at_ms"`
	ExpiresAtMS     uint64                        `json:"expires_at_ms"`
}

type forgeCredentialCandidateAuthority struct {
	OwnerBindingMatched    bool `json:"owner_binding_matched"`
	OwnerAuthenticated     bool `json:"owner_authenticated"`
	CredentialMaterialMade bool `json:"credential_material_made"`
	Persisted              bool `json:"persisted"`
	InventoryAuthoritative bool `json:"inventory_authoritative"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
}

type forgeCredentialCandidate struct {
	SchemaVersion      string                            `json:"schema_version"`
	EvaluationMode     string                            `json:"evaluation_mode"`
	Owner              forgeCredentialCandidateOwner     `json:"owner"`
	DeviceID           string                            `json:"device_id"`
	Action             string                            `json:"action"`
	Revision           uint64                            `json:"revision"`
	Previous           *forgeCredentialCandidateState    `json:"previous,omitempty"`
	Next               forgeCredentialCandidateState     `json:"next"`
	PreviewOnly        bool                              `json:"preview_only"`
	CandidatePublished bool                              `json:"candidate_published"`
	Authority          forgeCredentialCandidateAuthority `json:"authority"`
}

func decodeForgeDeviceCredentialCandidate(raw []byte) (forgeCredentialCandidate, error) {
	if err := rejectForgeDeviceCredentialCandidateDuplicateKeys(raw); err != nil {
		return forgeCredentialCandidate{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var candidate forgeCredentialCandidate
	if err := decoder.Decode(&candidate); err != nil {
		return forgeCredentialCandidate{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return forgeCredentialCandidate{}, errors.New("trailing JSON value")
		}
		return forgeCredentialCandidate{}, err
	}
	return candidate, nil
}

func validForgeCredentialCandidateIdentifier(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for index, character := range value {
		alphaNumeric := character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9'
		if index == 0 {
			if !alphaNumeric {
				return false
			}
			continue
		}
		if !alphaNumeric && character != '.' && character != '_' && character != ':' && character != '-' {
			return false
		}
	}
	return true
}

func validForgeCredentialCandidateOwner(owner forgeCredentialCandidateOwner) bool {
	return validForgeCredentialCandidateOwnerPart(owner.Issuer) &&
		validForgeCredentialCandidateOwnerPart(owner.Subject) &&
		validForgeCredentialCandidateOwnerPart(owner.TenantID)
}

func validForgeCredentialCandidateOwnerPart(value string) bool {
	return utf8.ValidString(value) && value != "" && len(value) <= 512 &&
		strings.TrimSpace(value) == value && !strings.ContainsFunc(value, unicode.IsControl)
}

func validForgeCredentialCandidateDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return false
		}
	}
	return true
}

const (
	forgeCredentialCandidateMaxSafeInteger uint64 = 9_007_199_254_740_991
	forgeCredentialCandidateMinLifetimeMS  uint64 = 1_000
	forgeCredentialCandidateMaxLifetimeMS  uint64 = 3_600_000
)

func validForgeCredentialCandidateState(state forgeCredentialCandidateState) bool {
	if !validForgeCredentialCandidateIdentifier(state.CredentialID) ||
		!validForgeCredentialCandidateIdentifier(state.DeviceID) ||
		!validForgeCredentialCandidateOwner(state.Owner) ||
		(state.ApprovalState != "pending" && state.ApprovalState != "approved" && state.ApprovalState != "revoked") ||
		(state.CredentialState != "active" && state.CredentialState != "expired" && state.CredentialState != "revoked") ||
		!validForgeCredentialCandidateIdentifier(state.KeyID) ||
		!validForgeCredentialCandidateDigest(state.PublicKeySHA256) ||
		state.KeyGeneration == 0 || state.KeyGeneration > forgeCredentialCandidateMaxSafeInteger ||
		state.IssuedAtMS > forgeCredentialCandidateMaxSafeInteger ||
		state.ExpiresAtMS > forgeCredentialCandidateMaxSafeInteger ||
		state.ExpiresAtMS <= state.IssuedAtMS {
		return false
	}
	lifetime := state.ExpiresAtMS - state.IssuedAtMS
	return lifetime >= forgeCredentialCandidateMinLifetimeMS && lifetime <= forgeCredentialCandidateMaxLifetimeMS
}

func validForgeDeviceCredentialCandidate(candidate forgeCredentialCandidate) bool {
	if candidate.SchemaVersion != "forge.device-credential-lifecycle/v1" ||
		candidate.EvaluationMode != "pure_device_credential_lifecycle" ||
		!validForgeCredentialCandidateOwner(candidate.Owner) ||
		!validForgeCredentialCandidateIdentifier(candidate.DeviceID) ||
		(candidate.Action != "issue" && candidate.Action != "revoke" && candidate.Action != "rotate") ||
		candidate.Revision == 0 || candidate.Revision > forgeCredentialCandidateMaxSafeInteger ||
		!validForgeCredentialCandidateState(candidate.Next) ||
		candidate.Next.Owner != candidate.Owner || candidate.Next.DeviceID != candidate.DeviceID ||
		!candidate.PreviewOnly || !candidate.CandidatePublished ||
		candidate.Authority != (forgeCredentialCandidateAuthority{}) {
		return false
	}
	if candidate.Previous != nil && (!validForgeCredentialCandidateState(*candidate.Previous) ||
		candidate.Previous.Owner != candidate.Owner || candidate.Previous.DeviceID != candidate.DeviceID) {
		return false
	}
	switch candidate.Action {
	case "issue":
		return candidate.Previous == nil && candidate.Next.CredentialState == "active" &&
			candidate.Next.ApprovalState != "revoked"
	case "revoke":
		prior := candidate.Previous
		return prior != nil && prior.CredentialState != "revoked" &&
			candidate.Next.CredentialState == "revoked" &&
			forgeCredentialCandidateSameMetadata(*prior, candidate.Next)
	case "rotate":
		prior := candidate.Previous
		if prior == nil || prior.CredentialState == "revoked" ||
			prior.KeyGeneration >= forgeCredentialCandidateMaxSafeInteger {
			return false
		}
		return candidate.Next.CredentialState == "active" &&
			candidate.Next.KeyGeneration == prior.KeyGeneration+1 &&
			candidate.Next.ApprovalState == prior.ApprovalState &&
			(candidate.Next.CredentialID != prior.CredentialID ||
				candidate.Next.KeyID != prior.KeyID ||
				candidate.Next.PublicKeySHA256 != prior.PublicKeySHA256)
	}
	return false
}

func forgeCredentialCandidateSameMetadata(left, right forgeCredentialCandidateState) bool {
	return left.CredentialID == right.CredentialID && left.DeviceID == right.DeviceID &&
		left.Owner == right.Owner && left.ApprovalState == right.ApprovalState &&
		left.KeyID == right.KeyID && left.PublicKeySHA256 == right.PublicKeySHA256 &&
		left.KeyGeneration == right.KeyGeneration && left.IssuedAtMS == right.IssuedAtMS &&
		left.ExpiresAtMS == right.ExpiresAtMS
}

func TestForgeDeviceCredentialCandidateContractFixture(t *testing.T) {
	candidate, err := decodeForgeDeviceCredentialCandidate(forgeDeviceCredentialCandidateFixture)
	if err != nil {
		t.Fatalf("decode device credential candidate fixture: %v", err)
	}
	if !validForgeDeviceCredentialCandidate(candidate) {
		t.Fatalf("invalid device credential candidate fixture: %#v", candidate)
	}
	if candidate.Owner.Issuer != "https://id.example" || candidate.Owner.Subject != "user-1" ||
		candidate.Owner.TenantID != "tenant-1" || candidate.DeviceID != "device-1" ||
		candidate.Action != "issue" || candidate.Next.CredentialID != "credential-1" {
		t.Fatalf("fixture binding drifted: %#v", candidate)
	}
}

func TestForgeDeviceCredentialCandidateRejectsMutation(t *testing.T) {
	mutate := func(mutator func(map[string]any)) []byte {
		var value map[string]any
		if err := json.Unmarshal(forgeDeviceCredentialCandidateFixture, &value); err != nil {
			t.Fatal(err)
		}
		mutator(value)
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}

	tests := []struct {
		name  string
		bytes []byte
	}{
		{"unknown_field", mutate(func(value map[string]any) { value["credential_material"] = "secret" })},
		{"foreign_owner", mutate(func(value map[string]any) {
			value["owner"].(map[string]any)["subject"] = "foreign-user"
		})},
		{"nested_device_drift", mutate(func(value map[string]any) {
			value["next"].(map[string]any)["device_id"] = "device-foreign"
		})},
		{"preview_disabled", mutate(func(value map[string]any) { value["preview_only"] = false })},
		{"candidate_not_published", mutate(func(value map[string]any) { value["candidate_published"] = false })},
		{"authority_enabled", mutate(func(value map[string]any) {
			value["authority"].(map[string]any)["persisted"] = true
		})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate, err := decodeForgeDeviceCredentialCandidate(test.bytes)
			if err != nil {
				if test.name == "unknown_field" {
					return
				}
				t.Fatalf("mutation should decode before semantic validation: %v", err)
			}
			if validForgeDeviceCredentialCandidate(candidate) {
				t.Fatal("credential candidate mutation accepted")
			}
		})
	}

	duplicate := bytes.TrimSpace(forgeDeviceCredentialCandidateFixture)
	duplicate = append(bytes.TrimSuffix(duplicate, []byte("}")), []byte(`,"schema_version":"forge.device-credential-lifecycle/v1"}`)...)
	if _, err := decodeForgeDeviceCredentialCandidate(duplicate); err == nil {
		t.Fatal("duplicate root field accepted")
	}
	trailing := append(append([]byte(nil), forgeDeviceCredentialCandidateFixture...), []byte(` true`)...)
	if _, err := decodeForgeDeviceCredentialCandidate(trailing); err == nil {
		t.Fatal("trailing JSON value accepted")
	}
}

func rejectForgeDeviceCredentialCandidateDuplicateKeys(raw []byte) error {
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
