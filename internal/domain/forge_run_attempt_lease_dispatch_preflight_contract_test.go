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

// This receiver-side contract is metadata-only. It does not authenticate an
// instance, persist a Run or Attempt, issue a lease, select a target, or grant
// execution/Audit authority.
//
//go:embed testdata/forge-run-attempt-lease-dispatch-preflight-v1.json
var forgeRunAttemptLeaseDispatchPreflightFixture []byte

type forgePreflightOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgePreflightAuthority struct {
	IdentityVerified    bool `json:"identity_verified"`
	RunAuthoritative    bool `json:"run_authoritative"`
	AttemptPersisted    bool `json:"attempt_persisted"`
	LeaseIssued         bool `json:"lease_issued"`
	ReservationCreated  bool `json:"reservation_created"`
	ExecutionAuthorized bool `json:"execution_authorized"`
	DispatchPerformed   bool `json:"dispatch_performed"`
	AuditPublished      bool `json:"audit_published"`
}

type forgeRunAttemptLeaseDispatchPreflight struct {
	SchemaVersion             string                  `json:"schema_version"`
	EvaluationMode            string                  `json:"evaluation_mode"`
	Owner                     forgePreflightOwner     `json:"owner"`
	ConversationID            string                  `json:"conversation_id"`
	RunID                     string                  `json:"run_id"`
	RunStatus                 string                  `json:"run_status"`
	RunStateAdmissible        bool                    `json:"run_state_admissible"`
	AttemptID                 string                  `json:"attempt_id"`
	AttemptState              string                  `json:"attempt_state"`
	AttemptStateAdmissible    bool                    `json:"attempt_state_admissible"`
	CommandID                 string                  `json:"command_id"`
	TargetID                  string                  `json:"intent_target_id"`
	LeaseEpoch                uint64                  `json:"lease_epoch"`
	LeaseActive               bool                    `json:"lease_active"`
	EvaluatedAtMS             int64                   `json:"evaluated_at_ms"`
	CandidateCount            int                     `json:"candidate_count"`
	DeclarativeReadyCount     int                     `json:"declarative_ready_count"`
	DeclarativePreflightReady bool                    `json:"declarative_preflight_ready"`
	RejectionReasons          []string                `json:"rejection_reasons"`
	SelectedTargetID          *string                 `json:"selected_target_id"`
	PreviewOnly               bool                    `json:"preview_only"`
	Authority                 forgePreflightAuthority `json:"authority"`
}

func decodeForgeRunAttemptLeaseDispatchPreflight(raw []byte) (forgeRunAttemptLeaseDispatchPreflight, error) {
	if err := rejectDuplicateForgePreflightJSONKeys(raw); err != nil {
		return forgeRunAttemptLeaseDispatchPreflight{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value forgeRunAttemptLeaseDispatchPreflight
	if err := decoder.Decode(&value); err != nil {
		return forgeRunAttemptLeaseDispatchPreflight{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeRunAttemptLeaseDispatchPreflight{}, errors.New("trailing JSON")
		}
		return forgeRunAttemptLeaseDispatchPreflight{}, err
	}
	if value.SelectedTargetID != nil || value.Authority != (forgePreflightAuthority{}) || !value.PreviewOnly {
		return forgeRunAttemptLeaseDispatchPreflight{}, errors.New("preflight carries selection or authority")
	}
	return value, nil
}

func rejectDuplicateForgePreflightJSONKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanForgePreflightJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return errors.New("trailing JSON")
		}
		return err
	}
	return nil
}

func scanForgePreflightJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	switch delimiter := token.(type) {
	case json.Delim:
		switch delimiter {
		case '{':
			keys := map[string]struct{}{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("object key is not string")
				}
				if _, exists := keys[key]; exists {
					return errors.New("duplicate JSON object key")
				}
				keys[key] = struct{}{}
				if err := scanForgePreflightJSONValue(decoder); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return errors.New("malformed object")
			}
		case '[':
			for decoder.More() {
				if err := scanForgePreflightJSONValue(decoder); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return errors.New("malformed array")
			}
		default:
			return errors.New("unexpected JSON delimiter")
		}
	}
	return nil
}

func TestForgeRunAttemptLeaseDispatchPreflightCanonicalObservation(t *testing.T) {
	value, err := decodeForgeRunAttemptLeaseDispatchPreflight(forgeRunAttemptLeaseDispatchPreflightFixture)
	if err != nil {
		t.Fatalf("decode preflight fixture: %v", err)
	}
	if value.SchemaVersion != "forge.run-attempt-lease-dispatch-preflight/v1" ||
		value.EvaluationMode != "pure_run_attempt_lease_dispatch_preflight" ||
		value.Owner.Issuer == "" || value.Owner.Subject == "" || value.Owner.TenantID == "" ||
		value.ConversationID == "" || value.RunID == "" || value.RunStatus != "nonterminal" ||
		!value.RunStateAdmissible || value.AttemptID == "" || value.AttemptState != "accepted" ||
		!value.AttemptStateAdmissible || value.CommandID == "" || value.TargetID == "" ||
		value.LeaseEpoch == 0 || !value.LeaseActive || value.EvaluatedAtMS <= 0 ||
		value.CandidateCount < 0 || value.DeclarativeReadyCount <= 0 ||
		!value.DeclarativePreflightReady || len(value.RejectionReasons) != 0 ||
		value.SelectedTargetID != nil || !value.PreviewOnly || value.Authority != (forgePreflightAuthority{}) {
		t.Fatalf("preflight is not bounded display-only metadata: %#v", value)
	}
}

func TestForgeRunAttemptLeaseDispatchPreflightRejectsUnknownDuplicateAndEffectFields(t *testing.T) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(forgeRunAttemptLeaseDispatchPreflightFixture, &root); err != nil {
		t.Fatal(err)
	}
	root["unexpected"] = json.RawMessage(`true`)
	unknown, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeForgeRunAttemptLeaseDispatchPreflight(unknown); err == nil {
		t.Fatal("unknown field accepted")
	}

	duplicate := strings.TrimSpace(string(forgeRunAttemptLeaseDispatchPreflightFixture))
	duplicate = strings.TrimSuffix(duplicate, "}") + `,"run_id":"run-001"}`
	if _, err := decodeForgeRunAttemptLeaseDispatchPreflight([]byte(duplicate)); err == nil {
		t.Fatal("duplicate field accepted")
	}

	root = map[string]json.RawMessage{}
	err = json.Unmarshal(forgeRunAttemptLeaseDispatchPreflightFixture, &root)
	if err != nil {
		t.Fatal(err)
	}
	root["selected_target_id"] = json.RawMessage(`"runner-1"`)
	mutated, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeForgeRunAttemptLeaseDispatchPreflight(mutated); err == nil {
		t.Fatal("selected target crossed display-only boundary")
	}
}
