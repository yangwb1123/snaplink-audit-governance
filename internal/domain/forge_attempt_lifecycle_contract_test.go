package domain

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"testing"
)

// This receiver consumes only the authority-neutral Attempt state graph. It
// never persists an Attempt, changes a lease, contacts a Runner, or grants
// execution authority.
//
//go:embed testdata/forge-attempt-lifecycle-v1.json
var forgeAttemptLifecycleFixture []byte

type forgeAttemptLifecycleEnvelope struct {
	SchemaVersion  string                         `json:"schema_version"`
	EvaluationMode string                         `json:"evaluation_mode"`
	Authority      forgeAttemptLifecycleAuthority `json:"authority"`
	Cases          []forgeAttemptLifecycleCase    `json:"cases"`
}

type forgeAttemptLifecycleAuthority struct {
	DeviceIdentityVerified bool `json:"device_identity_verified"`
	CommandPersisted       bool `json:"command_persisted"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
	AuditPublished         bool `json:"audit_published"`
}

type forgeAttemptLifecycleCase struct {
	Name     string `json:"name"`
	From     string `json:"from"`
	To       string `json:"to"`
	Accepted bool   `json:"accepted"`
	Error    string `json:"error"`
}

func TestForgeAttemptLifecycleReceiver(t *testing.T) {
	value, err := decodeForgeAttemptLifecycle(forgeAttemptLifecycleFixture)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateForgeAttemptLifecycle(value); err != nil {
		t.Fatal(err)
	}
	if value.SchemaVersion != "forge.attempt-lifecycle/v1" ||
		value.EvaluationMode != "pure_attempt_lifecycle_only" ||
		len(value.Cases) != 19 {
		t.Fatalf("canonical Attempt lifecycle drifted: %#v", value)
	}

	unknown := append(bytes.TrimSuffix(bytes.TrimSpace(forgeAttemptLifecycleFixture), []byte("}")), []byte(`,"unexpected":true}`)...)
	if _, err := decodeForgeAttemptLifecycle(unknown); err == nil {
		t.Fatal("unknown Attempt lifecycle field accepted")
	}
	duplicate := append(bytes.TrimSuffix(bytes.TrimSpace(forgeAttemptLifecycleFixture), []byte("}")), []byte(`,"schema_version":"forge.attempt-lifecycle/v1"}`)...)
	if _, err := decodeForgeAttemptLifecycle(duplicate); err == nil {
		t.Fatal("duplicate Attempt lifecycle field accepted")
	}
	if _, err := decodeForgeAttemptLifecycle(append(bytes.TrimSpace(forgeAttemptLifecycleFixture), []byte(" {}")...)); err == nil {
		t.Fatal("trailing Attempt lifecycle value accepted")
	}

	mutated := bytes.Replace(forgeAttemptLifecycleFixture, []byte(`"dispatch_performed": false`), []byte(`"dispatch_performed": true`), 1)
	value, err = decodeForgeAttemptLifecycle(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if validateForgeAttemptLifecycle(value) == nil {
		t.Fatal("execution authority drift accepted")
	}
	mutated = bytes.Replace(forgeAttemptLifecycleFixture, []byte(`"accepted":true`), []byte(`"accepted":false`), 1)
	value, err = decodeForgeAttemptLifecycle(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if validateForgeAttemptLifecycle(value) == nil {
		t.Fatal("lifecycle acceptance drift accepted")
	}
}

func decodeForgeAttemptLifecycle(raw []byte) (forgeAttemptLifecycleEnvelope, error) {
	if err := rejectForgeAttemptLifecycleDuplicateKeys(raw); err != nil {
		return forgeAttemptLifecycleEnvelope{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value forgeAttemptLifecycleEnvelope
	if err := decoder.Decode(&value); err != nil {
		return forgeAttemptLifecycleEnvelope{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || len(fields) != 4 {
		return forgeAttemptLifecycleEnvelope{}, errors.New("Attempt lifecycle fields are not exact")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeAttemptLifecycleEnvelope{}, errors.New("trailing JSON value")
		}
		return forgeAttemptLifecycleEnvelope{}, err
	}
	return value, nil
}

func validateForgeAttemptLifecycle(value forgeAttemptLifecycleEnvelope) error {
	if value.SchemaVersion != "forge.attempt-lifecycle/v1" ||
		value.EvaluationMode != "pure_attempt_lifecycle_only" ||
		value.Authority != (forgeAttemptLifecycleAuthority{}) ||
		len(value.Cases) != 19 {
		return errors.New("invalid Attempt lifecycle envelope")
	}
	seen := make(map[string]struct{}, len(value.Cases))
	for _, item := range value.Cases {
		if item.Name == "" {
			return errors.New("Attempt lifecycle case name is empty")
		}
		if _, ok := seen[item.Name]; ok {
			return errors.New("duplicate Attempt lifecycle case name")
		}
		seen[item.Name] = struct{}{}
		accepted, rejection := forgeAttemptLifecycleEdge(item.From, item.To)
		if item.Accepted != accepted {
			return errors.New("Attempt lifecycle acceptance drift")
		}
		if accepted {
			if item.Error != "" {
				return errors.New("accepted Attempt lifecycle edge carries rejection")
			}
		} else if item.Error != rejection {
			return errors.New("Attempt lifecycle rejection drift")
		}
	}
	return nil
}

func forgeAttemptLifecycleEdge(from, to string) (bool, string) {
	if !validForgeAttemptLifecycleState(from) || !validForgeAttemptLifecycleState(to) {
		return false, "pc_state_invalid"
	}
	switch from {
	case "requested":
		return to == "accepted", "pc_transition_invalid"
	case "accepted":
		return to == "starting" || to == "interrupted" || to == "failed" || to == "uncertain", "pc_transition_invalid"
	case "starting":
		return to == "running" || to == "interrupted" || to == "failed" || to == "uncertain", "pc_transition_invalid"
	case "running":
		return to == "interrupted" || to == "completed" || to == "failed" || to == "uncertain", "pc_transition_invalid"
	default:
		return false, "pc_transition_invalid"
	}
}

func validForgeAttemptLifecycleState(value string) bool {
	switch value {
	case "requested", "accepted", "starting", "running", "interrupted", "completed", "failed", "uncertain":
		return true
	default:
		return false
	}
}

func rejectForgeAttemptLifecycleDuplicateKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanForgeAttemptLifecycleJSON(decoder); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func scanForgeAttemptLifecycleJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	switch token {
	case json.Delim('{'):
		seen := map[string]bool{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok || seen[key] {
				return errors.New("duplicate JSON key")
			}
			seen[key] = true
			if err := scanForgeAttemptLifecycleJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case json.Delim('['):
		for decoder.More() {
			if err := scanForgeAttemptLifecycleJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return nil
	}
}
