package forge

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"testing"
)

//go:embed testdata/forge-client-instance-session-resource-convergence-v1.json
var forgeClientInstanceSessionResourceConvergenceFixture []byte

type forgeClientInstanceSessionResourceConvergenceEnvelope struct {
	SchemaVersion  string                           `json:"schema_version"`
	EvaluationMode string                           `json:"evaluation_mode"`
	SessionView    json.RawMessage                  `json:"session_view"`
	ResourceView   json.RawMessage                  `json:"resource_view"`
	Converged      bool                             `json:"converged"`
	ReadOnly       bool                             `json:"read_only"`
	Authority      forgeClientInstanceViewAuthority `json:"authority"`
}

func decodeForgeClientInstanceSessionResourceConvergence(raw []byte) (forgeClientInstanceSessionResourceConvergenceEnvelope, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return forgeClientInstanceSessionResourceConvergenceEnvelope{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var envelope forgeClientInstanceSessionResourceConvergenceEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return forgeClientInstanceSessionResourceConvergenceEnvelope{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeClientInstanceSessionResourceConvergenceEnvelope{}, io.ErrUnexpectedEOF
		}
		return forgeClientInstanceSessionResourceConvergenceEnvelope{}, err
	}
	if envelope.SchemaVersion != "forge.client-instance-session-resource-convergence/v1" ||
		envelope.EvaluationMode != "owner_bound_client_instance_session_resource_convergence_only" ||
		!envelope.Converged || !envelope.ReadOnly ||
		envelope.Authority != (forgeClientInstanceViewAuthority{}) {
		return forgeClientInstanceSessionResourceConvergenceEnvelope{}, io.ErrUnexpectedEOF
	}
	var session forgeClientInstanceSessionView
	if err := decodeForgeClientInstanceView(envelope.SessionView, &session); err != nil {
		return forgeClientInstanceSessionResourceConvergenceEnvelope{}, err
	}
	var resource forgeClientInstanceResourceView
	if err := decodeForgeClientInstanceView(envelope.ResourceView, &resource); err != nil {
		return forgeClientInstanceSessionResourceConvergenceEnvelope{}, err
	}
	if !reflect.DeepEqual(session.Owner, resource.Owner) ||
		!reflect.DeepEqual(session.Instances, resource.Instances) {
		return forgeClientInstanceSessionResourceConvergenceEnvelope{}, io.ErrUnexpectedEOF
	}
	return envelope, nil
}

func TestForgeClientInstanceSessionResourceConvergenceCanonical(t *testing.T) {
	envelope, err := decodeForgeClientInstanceSessionResourceConvergence(
		forgeClientInstanceSessionResourceConvergenceFixture,
	)
	if err != nil {
		t.Fatalf("decode client-instance session/resource convergence: %v", err)
	}
	if len(envelope.SessionView) == 0 || len(envelope.ResourceView) == 0 {
		t.Fatal("convergence envelope omitted one source view")
	}
}

func TestForgeClientInstanceSessionResourceConvergenceRejectsDriftAndUnknownFields(t *testing.T) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(forgeClientInstanceSessionResourceConvergenceFixture, &root); err != nil {
		t.Fatal(err)
	}
	root["unexpected"] = json.RawMessage(`true`)
	mutated, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeForgeClientInstanceSessionResourceConvergence(mutated); err == nil {
		t.Fatal("unknown convergence field unexpectedly accepted")
	}

	var invalidRoot map[string]json.RawMessage
	if err := json.Unmarshal(forgeClientInstanceSessionResourceConvergenceFixture, &invalidRoot); err != nil {
		t.Fatal(err)
	}
	var invalidResource map[string]json.RawMessage
	if err := json.Unmarshal(invalidRoot["resource_view"], &invalidResource); err != nil {
		t.Fatal(err)
	}
	var instances []map[string]json.RawMessage
	if err := json.Unmarshal(invalidResource["instances"], &instances); err != nil {
		t.Fatal(err)
	}
	instances[0]["observed_at_ms"] = json.RawMessage(`0`)
	invalidResource["instances"], err = json.Marshal(instances)
	if err != nil {
		t.Fatal(err)
	}
	invalidRoot["resource_view"], err = json.Marshal(invalidResource)
	if err != nil {
		t.Fatal(err)
	}
	invalidMutated, err := json.Marshal(invalidRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeForgeClientInstanceSessionResourceConvergence(invalidMutated); err == nil {
		t.Fatal("invalid client-instance observation time unexpectedly accepted")
	}

	var resource map[string]json.RawMessage
	if err := json.Unmarshal(root["resource_view"], &resource); err != nil {
		t.Fatal(err)
	}
	resource["instances"] = json.RawMessage(`[]`)
	root["resource_view"], err = json.Marshal(resource)
	if err != nil {
		t.Fatal(err)
	}
	mutated, err = json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeForgeClientInstanceSessionResourceConvergence(mutated); err == nil {
		t.Fatal("instance-row drift unexpectedly accepted")
	}

	duplicate := strings.TrimSpace(string(forgeClientInstanceSessionResourceConvergenceFixture))
	duplicate = strings.TrimSuffix(duplicate, "}") + `,"schema_version":"forge.client-instance-session-resource-convergence/v1"}`
	if _, err := decodeForgeClientInstanceSessionResourceConvergence([]byte(duplicate)); err == nil {
		t.Fatal("duplicate convergence field unexpectedly accepted")
	}
}
