package forge

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
)

// This receiver-side contract is an effect-free projection of caller-declared
// inventory values. It does not authenticate a device, persist a heartbeat,
// make inventory authoritative, reserve capacity, dispatch work, or grant
// execution/Audit authority.
//
//go:embed testdata/forge-device-inventory-status-contract-v1.json
var forgeInventoryStatusReceiverFixture []byte

const forgeInventoryStatusReceiverMaxStaleAfterMS uint64 = 24 * 60 * 60 * 1000

type forgeInventoryStatusReceiverAuthority struct {
	IdentityVerified       bool `json:"identity_verified"`
	HeartbeatPersisted     bool `json:"heartbeat_persisted"`
	InventoryAuthoritative bool `json:"inventory_authoritative"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
}

type forgeInventoryStatusReceiverObservation struct {
	ApprovalState        string `json:"approval_state"`
	CordonState          string `json:"cordon_state"`
	Liveness             string `json:"liveness"`
	ReservationState     string `json:"reservation_state"`
	SnapshotObservedAtMS uint64 `json:"snapshot_observed_at_ms"`
	LeaseExpiresAtMS     uint64 `json:"lease_expires_at_ms"`
	EvaluatedAtMS        uint64 `json:"evaluated_at_ms"`
}

type forgeInventoryStatusReceiverExpected struct {
	Status           string `json:"status"`
	Fresh            bool   `json:"fresh"`
	DeclaredEligible bool   `json:"declared_eligible"`
	Error            string `json:"error"`
}

type forgeInventoryStatusReceiverCase struct {
	Name     string                                  `json:"name"`
	Input    forgeInventoryStatusReceiverObservation `json:"input"`
	Expected forgeInventoryStatusReceiverExpected    `json:"expected"`
}

type forgeInventoryStatusReceiverFixtureType struct {
	SchemaVersion  string                                `json:"schema_version"`
	EvaluationMode string                                `json:"evaluation_mode"`
	StaleAfterMS   uint64                                `json:"stale_after_ms"`
	Authority      forgeInventoryStatusReceiverAuthority `json:"authority"`
	Cases          []forgeInventoryStatusReceiverCase    `json:"cases"`
}

type forgeInventoryStatusReceiverProjection struct {
	Status           string
	Fresh            bool
	DeclaredEligible bool
}

func decodeForgeInventoryStatusReceiver(raw []byte, out *forgeInventoryStatusReceiverFixtureType) error {
	if err := rejectForgeInventoryStatusReceiverDuplicateKeys(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return validateForgeInventoryStatusReceiver(out)
}

func validateForgeInventoryStatusReceiver(fixture *forgeInventoryStatusReceiverFixtureType) error {
	if fixture.SchemaVersion != "forge.device-inventory-status-contract/v1" ||
		fixture.EvaluationMode != "pure_projection_only" ||
		fixture.StaleAfterMS != 90_000 ||
		fixture.Authority != (forgeInventoryStatusReceiverAuthority{}) ||
		len(fixture.Cases) != 11 {
		return errors.New("invalid inventory status receiver envelope")
	}

	wantNames := []string{
		"approved_online_is_declared_eligible",
		"pending_approval",
		"revoked_wins_over_other_states",
		"cordoned_wins_over_liveness",
		"offline_wins_over_freshness",
		"old_snapshot_is_stale",
		"expired_lease_is_stale",
		"reserved_is_not_declared_eligible",
		"future_snapshot_rejected",
		"invalid_lease_window_rejected",
		"unknown_liveness_rejected",
	}
	for index, testCase := range fixture.Cases {
		if testCase.Name != wantNames[index] {
			return fmt.Errorf("unexpected inventory status case %q at index %d", testCase.Name, index)
		}
		projection, err := projectForgeInventoryStatusReceiver(testCase.Input, fixture.StaleAfterMS)
		if testCase.Expected.Error != "" {
			if err == nil || err.Error() != testCase.Expected.Error ||
				testCase.Expected.Status != "" || testCase.Expected.Fresh || testCase.Expected.DeclaredEligible {
				return fmt.Errorf("invalid rejected inventory status case %q", testCase.Name)
			}
			continue
		}
		if err != nil || projection.Status != testCase.Expected.Status ||
			projection.Fresh != testCase.Expected.Fresh ||
			projection.DeclaredEligible != testCase.Expected.DeclaredEligible {
			return fmt.Errorf("invalid accepted inventory status case %q", testCase.Name)
		}
	}
	return nil
}

func projectForgeInventoryStatusReceiver(value forgeInventoryStatusReceiverObservation, staleAfterMS uint64) (forgeInventoryStatusReceiverProjection, error) {
	if value.EvaluatedAtMS == 0 {
		return forgeInventoryStatusReceiverProjection{}, errors.New("invalid_evaluation_time")
	}
	if staleAfterMS == 0 || staleAfterMS > forgeInventoryStatusReceiverMaxStaleAfterMS {
		return forgeInventoryStatusReceiverProjection{}, errors.New("invalid_stale_after")
	}
	if value.SnapshotObservedAtMS > value.EvaluatedAtMS {
		return forgeInventoryStatusReceiverProjection{}, errors.New("snapshot_from_future")
	}
	if value.LeaseExpiresAtMS < value.SnapshotObservedAtMS {
		return forgeInventoryStatusReceiverProjection{}, errors.New("lease_before_snapshot")
	}
	if !forgeInventoryStatusReceiverValid(value.ApprovalState, "approved", "pending", "revoked") {
		return forgeInventoryStatusReceiverProjection{}, errors.New("unknown_approval")
	}
	if !forgeInventoryStatusReceiverValid(value.CordonState, "clear", "cordoned") {
		return forgeInventoryStatusReceiverProjection{}, errors.New("unknown_cordon")
	}
	if !forgeInventoryStatusReceiverValid(value.Liveness, "online", "offline") {
		return forgeInventoryStatusReceiverProjection{}, errors.New("unknown_liveness")
	}
	if !forgeInventoryStatusReceiverValid(value.ReservationState, "none", "reserved") {
		return forgeInventoryStatusReceiverProjection{}, errors.New("unknown_reservation")
	}

	age := value.EvaluatedAtMS - value.SnapshotObservedAtMS
	fresh := age <= staleAfterMS && value.LeaseExpiresAtMS > value.EvaluatedAtMS
	status := "online"
	switch {
	case value.ApprovalState == "revoked":
		status = "revoked"
	case value.CordonState == "cordoned":
		status = "cordoned"
	case value.Liveness == "offline":
		status = "offline"
	case !fresh:
		status = "stale"
	case value.ApprovalState == "pending":
		status = "pending"
	case value.ReservationState == "reserved":
		status = "reserved"
	}
	return forgeInventoryStatusReceiverProjection{
		Status:           status,
		Fresh:            fresh,
		DeclaredEligible: status == "online",
	}, nil
}

func forgeInventoryStatusReceiverValid(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func rejectForgeInventoryStatusReceiverDuplicateKeys(raw []byte) error {
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
					return errors.New("object key is not string")
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
			return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
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

func TestForgeDeviceInventoryStatusReceiverContract(t *testing.T) {
	var fixture forgeInventoryStatusReceiverFixtureType
	if err := decodeForgeInventoryStatusReceiver(forgeInventoryStatusReceiverFixture, &fixture); err != nil {
		t.Fatalf("decode inventory status receiver fixture: %v", err)
	}
	for _, testCase := range fixture.Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			projection, err := projectForgeInventoryStatusReceiver(testCase.Input, fixture.StaleAfterMS)
			if testCase.Expected.Error != "" {
				if err == nil || err.Error() != testCase.Expected.Error {
					t.Fatalf("error=%v, want %q", err, testCase.Expected.Error)
				}
				return
			}
			if err != nil {
				t.Fatalf("projection rejected: %v", err)
			}
			if projection.Status != testCase.Expected.Status || projection.Fresh != testCase.Expected.Fresh ||
				projection.DeclaredEligible != testCase.Expected.DeclaredEligible {
				t.Fatalf("projection=%#v, want %#v", projection, testCase.Expected)
			}
		})
	}
}

func TestForgeDeviceInventoryStatusReceiverRejectsMutation(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(map[string]json.RawMessage)
	}{
		{"unknown", func(root map[string]json.RawMessage) {
			root["unexpected"] = json.RawMessage(`true`)
		}},
		{"authority_identity_verified", func(root map[string]json.RawMessage) {
			root["authority"] = json.RawMessage(`{"identity_verified":true}`)
		}},
		{"authority_heartbeat_persisted", func(root map[string]json.RawMessage) {
			root["authority"] = json.RawMessage(`{"heartbeat_persisted":true}`)
		}},
		{"authority_inventory_authoritative", func(root map[string]json.RawMessage) {
			root["authority"] = json.RawMessage(`{"inventory_authoritative":true}`)
		}},
		{"authority_reservation_created", func(root map[string]json.RawMessage) {
			root["authority"] = json.RawMessage(`{"reservation_created":true}`)
		}},
		{"authority_execution_authorized", func(root map[string]json.RawMessage) {
			root["authority"] = json.RawMessage(`{"execution_authorized":true}`)
		}},
		{"authority_dispatch_performed", func(root map[string]json.RawMessage) {
			root["authority"] = json.RawMessage(`{"dispatch_performed":true}`)
		}},
		{"unknown_nested", func(root map[string]json.RawMessage) {
			var cases []map[string]json.RawMessage
			if err := json.Unmarshal(root["cases"], &cases); err != nil {
				return
			}
			var input map[string]json.RawMessage
			if err := json.Unmarshal(cases[0]["input"], &input); err != nil {
				return
			}
			input["unexpected"] = json.RawMessage(`true`)
			cases[0]["input"], _ = json.Marshal(input)
			root["cases"], _ = json.Marshal(cases)
		}},
		{"eligible_is_not_authority", func(root map[string]json.RawMessage) {
			var cases []map[string]json.RawMessage
			if err := json.Unmarshal(root["cases"], &cases); err != nil {
				return
			}
			var expected map[string]json.RawMessage
			if err := json.Unmarshal(cases[1]["expected"], &expected); err != nil {
				return
			}
			expected["declared_eligible"] = json.RawMessage(`true`)
			cases[1]["expected"], _ = json.Marshal(expected)
			root["cases"], _ = json.Marshal(cases)
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			var root map[string]json.RawMessage
			if err := json.Unmarshal(forgeInventoryStatusReceiverFixture, &root); err != nil {
				t.Fatal(err)
			}
			mutation.mutate(root)
			raw, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			var fixture forgeInventoryStatusReceiverFixtureType
			if err := decodeForgeInventoryStatusReceiver(raw, &fixture); err == nil {
				t.Fatal("mutated inventory status fixture unexpectedly accepted")
			}
		})
	}

	duplicate := bytes.TrimSpace(forgeInventoryStatusReceiverFixture)
	duplicate = append(bytes.TrimSuffix(duplicate, []byte("}")), []byte(`,"schema_version":"forge.device-inventory-status-contract/v1"}`)...)
	var fixture forgeInventoryStatusReceiverFixtureType
	if err := decodeForgeInventoryStatusReceiver(duplicate, &fixture); err == nil {
		t.Fatal("duplicate field unexpectedly accepted")
	}
}

func TestForgeDeviceInventoryStatusReceiverRejectsInvalidProjection(t *testing.T) {
	base := forgeInventoryStatusReceiverObservation{
		ApprovalState:        "approved",
		CordonState:          "clear",
		Liveness:             "online",
		ReservationState:     "none",
		SnapshotObservedAtMS: 150_000,
		LeaseExpiresAtMS:     210_000,
		EvaluatedAtMS:        200_000,
	}
	cases := []struct {
		name  string
		value forgeInventoryStatusReceiverObservation
		stale uint64
		want  string
	}{
		{"zero_evaluation_time", forgeInventoryStatusReceiverObservation{}, 90_000, "invalid_evaluation_time"},
		{"zero_stale_after", base, 0, "invalid_stale_after"},
		{"oversized_stale_after", base, forgeInventoryStatusReceiverMaxStaleAfterMS + 1, "invalid_stale_after"},
		{"unknown_approval", func() forgeInventoryStatusReceiverObservation {
			value := base
			value.ApprovalState = "unknown"
			return value
		}(), 90_000, "unknown_approval"},
		{"unknown_cordon", func() forgeInventoryStatusReceiverObservation {
			value := base
			value.CordonState = "unknown"
			return value
		}(), 90_000, "unknown_cordon"},
		{"unknown_liveness", func() forgeInventoryStatusReceiverObservation {
			value := base
			value.Liveness = "unknown"
			return value
		}(), 90_000, "unknown_liveness"},
		{"unknown_reservation", func() forgeInventoryStatusReceiverObservation {
			value := base
			value.ReservationState = "unknown"
			return value
		}(), 90_000, "unknown_reservation"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := projectForgeInventoryStatusReceiver(testCase.value, testCase.stale)
			if err == nil || err.Error() != testCase.want {
				t.Fatalf("error=%v, want %q", err, testCase.want)
			}
		})
	}
}
