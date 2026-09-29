package forge

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

// This archival receiver retains the canonical input that produces a bounded
// Run/Attempt/lease preflight observation. It neither evaluates placement nor
// authenticates an instance, persists state, issues a lease, selects a
// Runner, reserves capacity, dispatches work, or grants audit authority.

//go:embed testdata/forge-run-attempt-lease-dispatch-preflight-request-v1.json
var forgeRunAttemptLeaseDispatchPreflightRequestFixture []byte

type forgePreflightRequestArchive struct {
	Owner          forgePreflightOwner                      `json:"owner"`
	ConversationID string                                   `json:"conversation_id"`
	RunID          string                                   `json:"run_id"`
	RunStatus      string                                   `json:"run_status"`
	DispatchPlan   forgePreflightRequestArchiveDispatchPlan `json:"dispatch_plan"`
}

type forgePreflightRequestArchiveDispatchPlan struct {
	AttemptState string                             `json:"attempt_state"`
	Placement    json.RawMessage                    `json:"placement_request"`
	Intent       forgePreflightRequestArchiveIntent `json:"runner_execution_intent"`
	Lease        forgePreflightRequestArchiveLease  `json:"lease"`
}

type forgePreflightRequestArchiveIntent struct {
	SchemaVersion    string                                `json:"schema_version"`
	EvaluationMode   string                                `json:"evaluation_mode"`
	Owner            forgePreflightOwner                   `json:"owner"`
	ConversationID   string                                `json:"conversation_id"`
	PromptID         string                                `json:"prompt_id"`
	RunID            string                                `json:"run_id"`
	AttemptID        string                                `json:"attempt_id"`
	CommandID        string                                `json:"command_id"`
	TargetID         string                                `json:"target_id"`
	CommandSHA256    string                                `json:"command_sha256"`
	IdempotencyKey   string                                `json:"idempotency_key"`
	PromptRunBinding bool                                  `json:"prompt_run_binding_valid"`
	CommandBinding   bool                                  `json:"runner_command_binding_valid"`
	PreviewOnly      bool                                  `json:"preview_only"`
	SelectedTargetID *string                               `json:"selected_target_id"`
	Authority        forgePreflightRequestArchiveAuthority `json:"authority"`
}

type forgePreflightRequestArchiveAuthority struct {
	DeviceIdentityVerified bool `json:"device_identity_verified"`
	CommandPersisted       bool `json:"command_persisted"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
	AuditPublished         bool `json:"audit_published"`
}

type forgePreflightRequestArchiveLease struct {
	V            uint64 `json:"v"`
	AttemptID    string `json:"attempt_id"`
	TargetID     string `json:"target_id"`
	Epoch        uint64 `json:"epoch"`
	FencingToken string `json:"fencing_token"`
	IssuedAtMS   int64  `json:"issued_at_ms"`
	ExpiresAtMS  int64  `json:"expires_at_ms"`
}

type forgePreflightRequestArchivePlacement struct {
	SchemaVersion string              `json:"schema_version"`
	EvaluatedAtMS int64               `json:"evaluated_at_ms"`
	Owner         forgePreflightOwner `json:"owner"`
	MaxAgeMS      int64               `json:"max_snapshot_age_ms"`
	Requirements  json.RawMessage     `json:"requirements"`
	Devices       json.RawMessage     `json:"devices"`
}

func decodeForgeRunAttemptLeaseDispatchPreflightRequestArchive(raw []byte) (forgePreflightRequestArchive, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgePreflightRequestArchive{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value forgePreflightRequestArchive
	if err := decoder.Decode(&value); err != nil {
		return forgePreflightRequestArchive{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgePreflightRequestArchive{}, fmt.Errorf("preflight request has trailing JSON")
		}
		return forgePreflightRequestArchive{}, err
	}
	if err := validateForgeRunAttemptLeaseDispatchPreflightRequestArchive(value); err != nil {
		return forgePreflightRequestArchive{}, err
	}
	return value, nil
}

func validateForgeRunAttemptLeaseDispatchPreflightRequestArchive(value forgePreflightRequestArchive) error {
	if value.Owner != (forgePreflightOwner{Issuer: "https://id.example", Subject: "user-1", TenantID: "tenant-1"}) ||
		value.ConversationID != "conversation-001" || value.RunID != "run-001" || value.RunStatus != "nonterminal" ||
		value.DispatchPlan.AttemptState != "accepted" {
		return fmt.Errorf("invalid preflight request owner or run binding")
	}
	placement, err := decodeForgePreflightRequestArchivePlacement(value.DispatchPlan.Placement)
	if err != nil {
		return err
	}
	intent := value.DispatchPlan.Intent
	lease := value.DispatchPlan.Lease
	if placement.SchemaVersion != "forge.device-placement-dry-run/v1" || placement.EvaluatedAtMS != 200500 ||
		placement.Owner != value.Owner || placement.MaxAgeMS != 60000 || len(placement.Requirements) == 0 || len(placement.Devices) == 0 ||
		intent.SchemaVersion != "forge.runner-execution-intent/v1" || intent.EvaluationMode != "pure_runner_binding_only" ||
		intent.Owner != value.Owner || intent.ConversationID != value.ConversationID || intent.RunID != value.RunID ||
		intent.PromptID != "prompt-001" || intent.AttemptID != "attempt-001" || intent.CommandID != "command-001" || intent.TargetID != "runner-1" ||
		len(intent.CommandSHA256) != 64 || intent.IdempotencyKey != "run-001:attempt-001:command-001" ||
		!intent.PromptRunBinding || !intent.CommandBinding || !intent.PreviewOnly || intent.SelectedTargetID != nil ||
		intent.Authority != (forgePreflightRequestArchiveAuthority{}) || lease.V != 1 || lease.AttemptID != intent.AttemptID ||
		lease.TargetID != intent.TargetID || lease.Epoch != 1 || lease.FencingToken != "fence-001" ||
		lease.IssuedAtMS != 199500 || lease.ExpiresAtMS != 205500 {
		return fmt.Errorf("preflight request crossed its archival boundary")
	}
	return nil
}

func decodeForgePreflightRequestArchivePlacement(raw []byte) (forgePreflightRequestArchivePlacement, error) {
	if len(raw) == 0 {
		return forgePreflightRequestArchivePlacement{}, fmt.Errorf("missing placement declaration")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value forgePreflightRequestArchivePlacement
	if err := decoder.Decode(&value); err != nil {
		return forgePreflightRequestArchivePlacement{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgePreflightRequestArchivePlacement{}, fmt.Errorf("placement declaration has trailing JSON")
		}
		return forgePreflightRequestArchivePlacement{}, err
	}
	return value, nil
}

func TestForgePreflightRequestArchiveCanonical(t *testing.T) {
	value, err := decodeForgeRunAttemptLeaseDispatchPreflightRequestArchive(forgeRunAttemptLeaseDispatchPreflightRequestFixture)
	if err != nil {
		t.Fatal(err)
	}
	if value.DispatchPlan.Intent.TargetID != value.DispatchPlan.Lease.TargetID {
		t.Fatalf("lease target drifted from archival intent: %#v", value)
	}
}

func TestForgePreflightRequestArchiveRejectsEffectsAndBindings(t *testing.T) {
	base := string(forgeRunAttemptLeaseDispatchPreflightRequestFixture)
	mutations := map[string]string{
		"unknown":      strings.TrimSuffix(strings.TrimSpace(base), "}") + `,"unexpected":true}`,
		"duplicate":    strings.TrimSuffix(strings.TrimSpace(base), "}") + `,"run_id":"run-001"}`,
		"authority":    strings.Replace(base, `"dispatch_performed": false`, `"dispatch_performed": true`, 1),
		"selection":    strings.Replace(base, `"selected_target_id": null`, `"selected_target_id": "runner-1"`, 1),
		"lease_target": strings.Replace(base, `"target_id": "runner-1"`, `"target_id": "runner-2"`, 1),
	}
	for name, raw := range mutations {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeForgeRunAttemptLeaseDispatchPreflightRequestArchive([]byte(raw)); err == nil {
				t.Fatalf("invalid %s mutation accepted", name)
			}
		})
	}
}
