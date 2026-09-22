package domain

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"testing"
)

const forgeSignedIdentityNotice = "This fixture checks Ed25519 proof encoding, public-key digest, domain-separated binding bytes, and signature validity only. It consumes no challenge, issues no credential, persists no enrollment, records no approval, accepts no heartbeat, grants no inventory authority, and grants no execution authority."

//go:embed testdata/forge-device-identity-proof-ed25519-v1.json
var forgeSignedIdentityFixture []byte

type forgeSignedOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}
type forgeSignedDevice struct {
	DeviceID        string           `json:"device_id"`
	Owner           forgeSignedOwner `json:"owner"`
	KeyID           string           `json:"key_id"`
	PublicKeySHA256 string           `json:"public_key_sha256"`
	ApprovalState   string           `json:"approval_state"`
	CredentialState string           `json:"credential_state"`
}
type forgeSignedChallenge struct {
	ChallengeID     string `json:"challenge_id"`
	ChallengeSHA256 string `json:"challenge_sha256"`
	IssuedAtMS      uint64 `json:"issued_at_ms"`
	ExpiresAtMS     uint64 `json:"expires_at_ms"`
	Consumed        bool   `json:"consumed"`
}
type forgeSignedProof struct {
	DeviceID           string           `json:"device_id"`
	KeyID              string           `json:"key_id"`
	PublicKeyBase64URL string           `json:"public_key_base64url"`
	Owner              forgeSignedOwner `json:"owner"`
	ChallengeID        string           `json:"challenge_id"`
	ChallengeSHA256    string           `json:"challenge_sha256"`
	IssuedAtMS         uint64           `json:"issued_at_ms"`
	ExpiresAtMS        uint64           `json:"expires_at_ms"`
	SignatureBase64URL string           `json:"signature_base64url"`
}
type forgeSignedExpected struct {
	Accepted         bool   `json:"accepted"`
	Reason           string `json:"reason"`
	IdentityBound    bool   `json:"identity_bound"`
	ApprovalRequired bool   `json:"approval_required"`
}
type forgeSignedAuthority struct {
	IdentityVerified       bool `json:"identity_verified"`
	ChallengeConsumed      bool `json:"challenge_consumed"`
	EnrollmentPersisted    bool `json:"enrollment_persisted"`
	OwnerApprovalRecorded  bool `json:"owner_approval_recorded"`
	CredentialIssued       bool `json:"credential_issued"`
	InventoryAuthoritative bool `json:"inventory_authoritative"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
}
type forgeSignedFixture struct {
	SchemaVersion  string               `json:"schema_version"`
	EvaluationMode string               `json:"evaluation_mode"`
	Notice         string               `json:"notice"`
	Owner          forgeSignedOwner     `json:"owner_declaration"`
	Device         forgeSignedDevice    `json:"device"`
	Challenge      forgeSignedChallenge `json:"challenge"`
	Proof          forgeSignedProof     `json:"proof"`
	Expected       forgeSignedExpected  `json:"expected"`
	Authority      forgeSignedAuthority `json:"authority"`
}
type forgeSignedPayload struct {
	Domain          string           `json:"domain"`
	DeviceID        string           `json:"device_id"`
	KeyID           string           `json:"key_id"`
	PublicKeySHA256 string           `json:"public_key_sha256"`
	Owner           forgeSignedOwner `json:"owner"`
	ChallengeID     string           `json:"challenge_id"`
	ChallengeSHA256 string           `json:"challenge_sha256"`
	IssuedAtMS      uint64           `json:"issued_at_ms"`
	ExpiresAtMS     uint64           `json:"expires_at_ms"`
}

func decodeForgeSignedIdentity(raw []byte) (forgeSignedFixture, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeSignedFixture{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var fixture forgeSignedFixture
	if err := decoder.Decode(&fixture); err != nil {
		return forgeSignedFixture{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return forgeSignedFixture{}, errors.New("trailing JSON value")
		}
		return forgeSignedFixture{}, err
	}
	return fixture, nil
}
func validForgeSignedToken(value string) bool {
	return value != "" && len(value) <= 512 && value == string(bytes.TrimSpace([]byte(value)))
}
func validForgeSignedDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
func forgeSignedAuthorityZero(value forgeSignedAuthority) bool {
	return value == (forgeSignedAuthority{})
}
func forgeSignedDisplayOnly(f forgeSignedFixture) bool {
	if f.SchemaVersion != "forge.device-identity-proof/ed25519/v1" || f.EvaluationMode != "pure_cryptographic_binding_only" || f.Notice != forgeSignedIdentityNotice || f.Owner != f.Device.Owner || f.Device.ApprovalState != "approved" || f.Device.CredentialState != "active" || f.Challenge.Consumed || f.Challenge.ExpiresAtMS <= f.Challenge.IssuedAtMS || f.Proof.DeviceID != f.Device.DeviceID || f.Proof.KeyID != f.Device.KeyID || f.Proof.Owner != f.Owner || f.Proof.ChallengeID != f.Challenge.ChallengeID || f.Proof.ChallengeSHA256 != f.Challenge.ChallengeSHA256 || f.Proof.IssuedAtMS >= f.Proof.ExpiresAtMS || !forgeSignedAuthorityZero(f.Authority) || !f.Expected.Accepted || f.Expected.Reason != "bound_approved" || !f.Expected.IdentityBound || f.Expected.ApprovalRequired {
		return false
	}
	pub, err := base64.RawURLEncoding.DecodeString(f.Proof.PublicKeyBase64URL)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return false
	}
	digest := sha256.Sum256(pub)
	if hex.EncodeToString(digest[:]) != f.Device.PublicKeySHA256 || !validForgeSignedDigest(f.Device.PublicKeySHA256) {
		return false
	}
	signature, err := base64.RawURLEncoding.DecodeString(f.Proof.SignatureBase64URL)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return false
	}
	payload, err := json.Marshal(forgeSignedPayload{"forge.device-identity-proof/ed25519/v1", f.Proof.DeviceID, f.Proof.KeyID, hex.EncodeToString(digest[:]), f.Proof.Owner, f.Proof.ChallengeID, f.Proof.ChallengeSHA256, f.Proof.IssuedAtMS, f.Proof.ExpiresAtMS})
	if err != nil {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(pub), payload, signature)
}

func TestForgeSignedDeviceIdentityProofReceiver(t *testing.T) {
	fixture, err := decodeForgeSignedIdentity(forgeSignedIdentityFixture)
	if err != nil {
		t.Fatalf("decode signed identity fixture: %v", err)
	}
	if !forgeSignedDisplayOnly(fixture) {
		t.Fatalf("signed identity proof crossed authority boundary: %#v", fixture)
	}
}
func TestForgeSignedDeviceIdentityProofRejectsMutation(t *testing.T) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(forgeSignedIdentityFixture, &root); err != nil {
		t.Fatal(err)
	}
	root["unexpected"] = json.RawMessage(`true`)
	mutated, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeForgeSignedIdentity(mutated); err == nil {
		t.Fatal("unknown signed identity field accepted")
	}
	duplicate := bytes.Replace(forgeSignedIdentityFixture, []byte(`"schema_version": "forge.device-identity-proof/ed25519/v1",`), []byte(`"schema_version": "forge.device-identity-proof/ed25519/v1","schema_version": "forge.device-identity-proof/ed25519/v1",`), 1)
	if _, err := decodeForgeSignedIdentity(duplicate); err == nil {
		t.Fatal("duplicate signed identity field accepted")
	}
	signature := bytes.Replace(forgeSignedIdentityFixture, []byte(`"signature_base64url":"eBX`), []byte(`"signature_base64url":"fBX`), 1)
	decoded, err := decodeForgeSignedIdentity(signature)
	if err != nil {
		t.Fatal(err)
	}
	if forgeSignedDisplayOnly(decoded) {
		t.Fatal("signature mutation accepted")
	}
	authority := bytes.Replace(forgeSignedIdentityFixture, []byte(`"identity_verified":false`), []byte(`"identity_verified":true`), 1)
	decoded, err = decodeForgeSignedIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	if forgeSignedDisplayOnly(decoded) {
		t.Fatal("authority mutation accepted")
	}
	trailing := append(append([]byte(nil), forgeSignedIdentityFixture...), []byte(` true`)...)
	if _, err := decodeForgeSignedIdentity(trailing); err == nil {
		t.Fatal("trailing signed identity value accepted")
	}
}
