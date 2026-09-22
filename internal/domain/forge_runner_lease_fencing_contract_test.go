package domain

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

// This receiver-side value contract carries only capability-lease/fencing
// evidence. It does not authenticate a Runner, persist a lease, reserve a
// device, dispatch work, or grant execution/Audit authority.
//
//go:embed testdata/forge-runner-lease-fencing-v1.json
var forgeRunnerLeaseFencingFixture []byte

type forgeRunnerLeaseFencing struct {
	SchemaVersion  string                    `json:"schema_version"`
	EvaluationMode string                    `json:"evaluation_mode"`
	Authority      forgeRunnerLeaseAuthority `json:"authority"`
	Grant          forgeRunnerLeaseGrant     `json:"grant"`
	Cases          []forgeRunnerLeaseCase    `json:"cases"`
}

type forgeRunnerLeaseAuthority struct {
	DeviceIdentityVerified bool `json:"device_identity_verified"`
	CommandPersisted       bool `json:"command_persisted"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
	AuditPublished         bool `json:"audit_published"`
}

type forgeRunnerLeaseGrant struct {
	V            uint16 `json:"v"`
	AttemptID    string `json:"attempt_id"`
	TargetID     string `json:"target_id"`
	Epoch        uint64 `json:"epoch"`
	FencingToken string `json:"fencing_token"`
	IssuedAtMS   uint64 `json:"issued_at_ms"`
	ExpiresAtMS  uint64 `json:"expires_at_ms"`
}

type forgeRunnerLeaseCase struct {
	Name             string                       `json:"name"`
	Operation        string                       `json:"operation"`
	ObservedAtMS     uint64                       `json:"observed_at_ms"`
	FencingToken     string                       `json:"fencing_token"`
	TTLMS            uint64                       `json:"ttl_ms"`
	Proof            *forgeRunnerLeaseGrant       `json:"proof"`
	Disposition      *forgeRunnerLeaseDisposition `json:"disposition"`
	SeedDisposition  *forgeRunnerLeaseDisposition `json:"seed_disposition"`
	SeedObservedAtMS uint64                       `json:"seed_observed_at_ms"`
	Expected         forgeRunnerLeaseExpected     `json:"expected"`
}

type forgeRunnerLeaseDisposition struct {
	Kind          string `json:"kind"`
	ReceiptSHA256 string `json:"receipt_sha256"`
	Reason        string `json:"reason"`
}

type forgeRunnerLeaseExpected struct {
	Active         *bool  `json:"active"`
	Accepted       *bool  `json:"accepted"`
	Error          string `json:"error"`
	Epoch          uint64 `json:"epoch"`
	IssuedAtMS     uint64 `json:"issued_at_ms"`
	ExpiresAtMS    uint64 `json:"expires_at_ms"`
	Replayed       *bool  `json:"replayed"`
	Uncertain      *bool  `json:"uncertain"`
	AutomaticRetry *bool  `json:"automatic_retry"`
}

func decodeForgeRunnerLeaseFencing(raw []byte) (forgeRunnerLeaseFencing, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeRunnerLeaseFencing{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value forgeRunnerLeaseFencing
	if err := decoder.Decode(&value); err != nil {
		return forgeRunnerLeaseFencing{}, err
	}
	if value.Authority != (forgeRunnerLeaseAuthority{}) {
		return forgeRunnerLeaseFencing{}, io.ErrUnexpectedEOF
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeRunnerLeaseFencing{}, io.ErrUnexpectedEOF
		}
		return forgeRunnerLeaseFencing{}, err
	}
	return value, nil
}

func TestForgeRunnerLeaseFencingCanonicalFixture(t *testing.T) {
	value, err := decodeForgeRunnerLeaseFencing(forgeRunnerLeaseFencingFixture)
	if err != nil {
		t.Fatalf("decode Runner lease/fencing fixture: %v", err)
	}
	if value.SchemaVersion != "forge.runner-lease-fencing/v1" || value.EvaluationMode != "pure_lease_fencing_only" ||
		value.Authority != (forgeRunnerLeaseAuthority{}) || value.Grant.V != 1 || value.Grant.AttemptID == "" ||
		value.Grant.TargetID == "" || value.Grant.Epoch == 0 || value.Grant.FencingToken == "" ||
		value.Grant.ExpiresAtMS <= value.Grant.IssuedAtMS || len(value.Cases) != 16 {
		t.Fatalf("lease/fencing evidence is not bounded and authority-free: %#v", value)
	}
	seen := make(map[string]bool, len(value.Cases))
	for _, item := range value.Cases {
		if item.Name == "" || item.Operation == "" || seen[item.Name] {
			t.Fatalf("invalid or duplicate lease case: %#v", item)
		}
		seen[item.Name] = true
		if strings.ContainsAny(item.Name+item.Operation+item.FencingToken, "\x00\r\n") {
			t.Fatalf("unsafe lease case: %#v", item)
		}
	}
	for _, name := range []string{"active_at_issue", "inactive_at_expiry", "renew_valid", "renew_token_reused", "renew_clock_rollback", "renew_expired", "proof_attempt_mismatch", "proof_target_mismatch", "proof_epoch_mismatch", "proof_token_mismatch", "proof_expired", "terminal_completed", "terminal_uncertain", "terminal_replay", "terminal_conflict", "uncertain_is_terminal"} {
		if !seen[name] {
			t.Fatalf("lease fixture missing case %q", name)
		}
	}
}

func TestForgeRunnerLeaseFencingRejectsUnknownAuthorityAndDuplicateFields(t *testing.T) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(forgeRunnerLeaseFencingFixture, &root); err != nil {
		t.Fatal(err)
	}
	root["output"] = json.RawMessage(`"raw output must not cross the lease boundary"`)
	unknown, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeForgeRunnerLeaseFencing(unknown); err == nil {
		t.Fatal("unknown lease field accepted")
	}
	mutated := strings.Replace(string(forgeRunnerLeaseFencingFixture), `"audit_published": false`, `"audit_published": true`, 1)
	if _, err := decodeForgeRunnerLeaseFencing([]byte(mutated)); err == nil {
		t.Fatal("lease authority mutation accepted")
	}
	duplicate := strings.TrimSpace(string(forgeRunnerLeaseFencingFixture))
	duplicate = strings.TrimSuffix(duplicate, "}") + `,"schema_version":"forge.runner-lease-fencing/v1"}`
	if _, err := decodeForgeRunnerLeaseFencing([]byte(duplicate)); err == nil {
		t.Fatal("duplicate lease field accepted")
	}
}
