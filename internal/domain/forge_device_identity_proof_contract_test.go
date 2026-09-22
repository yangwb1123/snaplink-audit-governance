package domain

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"testing"
)

const forgeDeviceIdentityProofNotice = "This fixture checks exact device-owner-key and one-time challenge binding only. The proof digest is a test-vector label; no cryptographic verifier, credential issuer, persistence, network, approval write, inventory authority, or execution authority is present."

// This receiver consumes a pure binding test vector. It authenticates no
// device, consumes no challenge, issues no credential, and grants no authority.
//
//go:embed testdata/forge-device-identity-proof-contract-v1.json
var forgeDeviceIdentityProofFixture []byte

type forgeIdentityOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}
type forgeIdentityDevice struct {
	DeviceID        string             `json:"device_id"`
	Owner           forgeIdentityOwner `json:"owner"`
	KeyID           string             `json:"key_id"`
	PublicKeySHA256 string             `json:"public_key_sha256"`
	ApprovalState   string             `json:"approval_state"`
	CredentialState string             `json:"credential_state"`
}
type forgeIdentityChallenge struct {
	ChallengeID     string `json:"challenge_id"`
	ChallengeSHA256 string `json:"challenge_sha256"`
	IssuedAtMS      uint64 `json:"issued_at_ms"`
	ExpiresAtMS     uint64 `json:"expires_at_ms"`
	Consumed        bool   `json:"consumed"`
}
type forgeIdentityAuthority struct {
	IdentityVerified       bool `json:"identity_verified"`
	ChallengeConsumed      bool `json:"challenge_consumed"`
	EnrollmentPersisted    bool `json:"enrollment_persisted"`
	OwnerApprovalRecorded  bool `json:"owner_approval_recorded"`
	CredentialIssued       bool `json:"credential_issued"`
	InventoryAuthoritative bool `json:"inventory_authoritative"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
}
type forgeIdentityProof struct {
	DeviceID        string             `json:"device_id"`
	KeyID           string             `json:"key_id"`
	PublicKeySHA256 string             `json:"public_key_sha256"`
	Owner           forgeIdentityOwner `json:"owner"`
	ChallengeID     string             `json:"challenge_id"`
	ChallengeSHA256 string             `json:"challenge_sha256"`
	ProofSHA256     string             `json:"proof_sha256"`
	IssuedAtMS      uint64             `json:"issued_at_ms"`
	ExpiresAtMS     uint64             `json:"expires_at_ms"`
}
type forgeIdentityExpected struct {
	Accepted         bool   `json:"accepted"`
	Reason           string `json:"reason"`
	IdentityBound    bool   `json:"identity_bound"`
	ApprovalRequired bool   `json:"approval_required"`
}
type forgeIdentityCase struct {
	Name                  string                `json:"name"`
	NowMS                 uint64                `json:"now_ms"`
	ChallengeConsumed     bool                  `json:"challenge_consumed"`
	DeviceApprovalState   string                `json:"device_approval_state"`
	DeviceCredentialState string                `json:"device_credential_state"`
	Proof                 forgeIdentityProof    `json:"proof"`
	Expected              forgeIdentityExpected `json:"expected"`
}
type forgeIdentityFixture struct {
	SchemaVersion  string                 `json:"schema_version"`
	EvaluationMode string                 `json:"evaluation_mode"`
	Notice         string                 `json:"notice"`
	Owner          forgeIdentityOwner     `json:"owner_declaration"`
	Device         forgeIdentityDevice    `json:"device"`
	Challenge      forgeIdentityChallenge `json:"challenge"`
	Authority      forgeIdentityAuthority `json:"authority"`
	Cases          []forgeIdentityCase    `json:"cases"`
}

func decodeForgeDeviceIdentityProof(raw []byte) (forgeIdentityFixture, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeIdentityFixture{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var fixture forgeIdentityFixture
	if err := decoder.Decode(&fixture); err != nil {
		return forgeIdentityFixture{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return forgeIdentityFixture{}, errors.New("trailing JSON value")
		}
		return forgeIdentityFixture{}, err
	}
	return fixture, nil
}

func validForgeIdentityOwner(owner forgeIdentityOwner) bool {
	return owner.Issuer != "" && owner.Subject != "" && owner.TenantID != ""
}
func validForgeIdentityToken(value string) bool {
	return value != "" && len(value) <= 128 && value == string(bytes.TrimSpace([]byte(value)))
}
func validForgeIdentityDigest(value string) bool { return len(value) == 64 }

func forgeDeviceIdentityProofDisplayOnly(f forgeIdentityFixture) bool {
	return f.SchemaVersion == "forge.device-identity-proof-contract/v1" && f.EvaluationMode == "pure_binding_only" && f.Notice == forgeDeviceIdentityProofNotice && validForgeIdentityOwner(f.Owner) && f.Device.Owner == f.Owner && validForgeIdentityToken(f.Device.DeviceID) && validForgeIdentityToken(f.Device.KeyID) && validForgeIdentityDigest(f.Device.PublicKeySHA256) && f.Device.ApprovalState == "approved" && f.Device.CredentialState == "active" && validForgeIdentityToken(f.Challenge.ChallengeID) && validForgeIdentityDigest(f.Challenge.ChallengeSHA256) && f.Challenge.IssuedAtMS > 0 && f.Challenge.ExpiresAtMS > f.Challenge.IssuedAtMS && !f.Challenge.Consumed && f.Authority == (forgeIdentityAuthority{})
}

func TestForgeDeviceIdentityProofReceiver(t *testing.T) {
	fixture, err := decodeForgeDeviceIdentityProof(forgeDeviceIdentityProofFixture)
	if err != nil {
		t.Fatalf("decode identity proof fixture: %v", err)
	}
	if !forgeDeviceIdentityProofDisplayOnly(fixture) || len(fixture.Cases) != 12 {
		t.Fatalf("identity proof crossed authority boundary: %#v", fixture)
	}
	seen := map[string]bool{}
	for _, testCase := range fixture.Cases {
		if testCase.Name == "" || seen[testCase.Name] || testCase.NowMS == 0 || testCase.NowMS > 9007199254740991 || !validForgeIdentityToken(testCase.Proof.DeviceID) || !validForgeIdentityToken(testCase.Proof.KeyID) || !validForgeIdentityDigest(testCase.Proof.PublicKeySHA256) || !validForgeIdentityToken(testCase.Proof.ChallengeID) || !validForgeIdentityDigest(testCase.Proof.ChallengeSHA256) || !validForgeIdentityDigest(testCase.Proof.ProofSHA256) || testCase.Proof.IssuedAtMS == 0 || testCase.Proof.ExpiresAtMS <= testCase.Proof.IssuedAtMS || !validForgeIdentityOwner(testCase.Proof.Owner) {
			t.Fatalf("invalid identity proof case: %#v", testCase)
		}
		seen[testCase.Name] = true
		if testCase.Expected.Accepted {
			if testCase.Expected.Reason != "bound_approved" && testCase.Expected.Reason != "bound_pending_approval" {
				t.Fatalf("unexpected accepted reason: %#v", testCase.Expected)
			}
			if !testCase.Expected.IdentityBound {
				t.Fatal("accepted case lost identity binding")
			}
		} else if testCase.Expected.Reason == "" || testCase.Expected.IdentityBound || testCase.Expected.ApprovalRequired {
			t.Fatalf("rejected case grants binding/approval: %#v", testCase.Expected)
		}
	}
}

func TestForgeDeviceIdentityProofRejectsMutation(t *testing.T) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(forgeDeviceIdentityProofFixture, &root); err != nil {
		t.Fatal(err)
	}
	root["unexpected"] = json.RawMessage(`true`)
	mutated, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeForgeDeviceIdentityProof(mutated); err == nil {
		t.Fatal("unknown identity proof field accepted")
	}
	duplicate := bytes.Replace(forgeDeviceIdentityProofFixture, []byte(`"schema_version": "forge.device-identity-proof-contract/v1",`), []byte(`"schema_version": "forge.device-identity-proof-contract/v1","schema_version": "forge.device-identity-proof-contract/v1",`), 1)
	if _, err := decodeForgeDeviceIdentityProof(duplicate); err == nil {
		t.Fatal("duplicate identity proof field accepted")
	}
	authority := bytes.Replace(forgeDeviceIdentityProofFixture, []byte(`"identity_verified": false`), []byte(`"identity_verified": true`), 1)
	decoded, err := decodeForgeDeviceIdentityProof(authority)
	if err != nil {
		t.Fatal(err)
	}
	if forgeDeviceIdentityProofDisplayOnly(decoded) {
		t.Fatal("identity proof authority mutation accepted")
	}
	trailing := append(append([]byte(nil), forgeDeviceIdentityProofFixture...), []byte(` true`)...)
	if _, err := decodeForgeDeviceIdentityProof(trailing); err == nil {
		t.Fatal("trailing identity proof value accepted")
	}
}
