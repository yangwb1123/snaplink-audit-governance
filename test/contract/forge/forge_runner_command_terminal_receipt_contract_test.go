package forge

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"unicode/utf8"
)

// This receiver accepts only an immutable command/terminal observation. It
// never contacts a Runner, persists a command or receipt, or grants effect
// authority.
//
//go:embed testdata/forge-runner-command-terminal-receipt-v1.json
var forgeRunnerCommandTerminalReceiptFixture []byte

const forgeRunnerCommandTerminalReceiptMaxSafeInteger = uint64(9007199254740991)

type forgeRunnerCommandTerminalReceiptAuthority struct {
	DeviceIdentityVerified bool `json:"device_identity_verified"`
	CommandPersisted       bool `json:"command_persisted"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
	AuditPublished         bool `json:"audit_published"`
}
type forgeRunnerCommandTerminalReceiptProof struct {
	AttemptID    string `json:"attempt_id"`
	TargetID     string `json:"target_id"`
	Epoch        uint64 `json:"epoch"`
	FencingToken string `json:"fencing_token"`
}
type forgeRunnerCommandTerminalReceiptGrant struct {
	V            uint16 `json:"v"`
	AttemptID    string `json:"attempt_id"`
	TargetID     string `json:"target_id"`
	Epoch        uint64 `json:"epoch"`
	FencingToken string `json:"fencing_token"`
	IssuedAtMS   uint64 `json:"issued_at_ms"`
	ExpiresAtMS  uint64 `json:"expires_at_ms"`
}
type forgeRunnerCommandTerminalReceiptCommand struct {
	V              uint16                                 `json:"v"`
	CommandID      string                                 `json:"command_id"`
	LeaseProof     forgeRunnerCommandTerminalReceiptProof `json:"lease_proof"`
	IdempotencyKey string                                 `json:"idempotency_key"`
	WorkspaceRef   string                                 `json:"workspace_ref"`
	Argv           []string                               `json:"argv"`
	TimeoutMS      uint64                                 `json:"timeout_ms"`
	MaxOutputBytes uint64                                 `json:"max_output_bytes"`
}
type forgeRunnerCommandTerminalReceiptDisposition struct {
	Kind          string `json:"kind"`
	ReceiptSHA256 string `json:"receipt_sha256,omitempty"`
	Reason        string `json:"reason,omitempty"`
}
type forgeRunnerCommandTerminalReceiptValue struct {
	V             uint16                                       `json:"v"`
	CommandID     string                                       `json:"command_id"`
	CommandSHA256 string                                       `json:"command_sha256"`
	Proof         forgeRunnerCommandTerminalReceiptProof       `json:"proof"`
	Disposition   forgeRunnerCommandTerminalReceiptDisposition `json:"disposition"`
	ObservedAtMS  uint64                                       `json:"observed_at_ms"`
}
type forgeRunnerCommandTerminalReceiptExpected struct {
	CommandSHA256 string `json:"command_sha256"`
	ReceiptValid  bool   `json:"receipt_valid"`
}
type forgeRunnerCommandTerminalReceiptFixtureValue struct {
	SchemaVersion  string                                     `json:"schema_version"`
	EvaluationMode string                                     `json:"evaluation_mode"`
	Authority      forgeRunnerCommandTerminalReceiptAuthority `json:"authority"`
	Grant          forgeRunnerCommandTerminalReceiptGrant     `json:"grant"`
	Command        forgeRunnerCommandTerminalReceiptCommand   `json:"command"`
	Receipt        forgeRunnerCommandTerminalReceiptValue     `json:"receipt"`
	Expected       forgeRunnerCommandTerminalReceiptExpected  `json:"expected"`
}

func TestForgeRunnerCommandTerminalReceiptCanonical(t *testing.T) {
	fixture, err := decodeForgeRunnerCommandTerminalReceipt(forgeRunnerCommandTerminalReceiptFixture)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.SchemaVersion != "forge.runner-command-terminal-receipt/v1" ||
		fixture.EvaluationMode != "pure_runner_command_receipt_only" ||
		fixture.Authority != (forgeRunnerCommandTerminalReceiptAuthority{}) {
		t.Fatalf("invalid terminal receipt envelope: %#v", fixture)
	}
	if fixture.Grant.V != 1 || fixture.Grant.Epoch == 0 ||
		fixture.Grant.ExpiresAtMS-fixture.Grant.IssuedAtMS != 10000 {
		t.Fatalf("invalid lease grant: %#v", fixture.Grant)
	}
	if err := validateForgeRunnerCommandTerminalReceipt(fixture); err != nil {
		t.Fatal(err)
	}
}

func TestForgeRunnerCommandTerminalReceiptRejectsWireAndBindingDrift(t *testing.T) {
	mutations := map[string][]byte{
		"unknown":   append(bytes.TrimSuffix(bytes.TrimSpace(forgeRunnerCommandTerminalReceiptFixture), []byte("}")), []byte(`,"unexpected":true}`)...),
		"duplicate": append(bytes.TrimSuffix(bytes.TrimSpace(forgeRunnerCommandTerminalReceiptFixture), []byte("}")), []byte(`,"schema_version":"forge.runner-command-terminal-receipt/v1"}`)...),
		"trailing":  append(bytes.TrimSpace(forgeRunnerCommandTerminalReceiptFixture), []byte(" {}")...),
		"authority": bytes.Replace(forgeRunnerCommandTerminalReceiptFixture, []byte(`"execution_authorized": false`), []byte(`"execution_authorized": true`), 1),
	}
	for name, raw := range mutations {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeForgeRunnerCommandTerminalReceipt(raw); err == nil {
				t.Fatal("wire mutation was accepted")
			}
		})
	}
	fixture, err := decodeForgeRunnerCommandTerminalReceipt(forgeRunnerCommandTerminalReceiptFixture)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*forgeRunnerCommandTerminalReceiptFixtureValue){
		"command digest": func(value *forgeRunnerCommandTerminalReceiptFixtureValue) {
			value.Receipt.CommandSHA256 = strings.Repeat("b", 64)
		},
		"foreign proof": func(value *forgeRunnerCommandTerminalReceiptFixtureValue) {
			value.Receipt.Proof.TargetID = "runner-foreign"
		},
		"expired observation": func(value *forgeRunnerCommandTerminalReceiptFixtureValue) {
			value.Receipt.ObservedAtMS = value.Grant.ExpiresAtMS
		},
	} {
		t.Run(name, func(t *testing.T) {
			mutated := fixture
			mutate(&mutated)
			if err := validateForgeRunnerCommandTerminalReceipt(mutated); err == nil {
				t.Fatal("binding mutation was accepted")
			}
		})
	}
}

func decodeForgeRunnerCommandTerminalReceipt(raw []byte) (forgeRunnerCommandTerminalReceiptFixtureValue, error) {
	if err := rejectForgeRunnerCommandTerminalReceiptDuplicateKeys(raw); err != nil {
		return forgeRunnerCommandTerminalReceiptFixtureValue{}, err
	}
	var shape map[string]json.RawMessage
	if err := json.Unmarshal(raw, &shape); err != nil {
		return forgeRunnerCommandTerminalReceiptFixtureValue{}, err
	}
	if err := requireForgeRunnerCommandTerminalReceiptShape(shape, []string{"schema_version", "evaluation_mode", "authority", "grant", "command", "receipt", "expected"}); err != nil {
		return forgeRunnerCommandTerminalReceiptFixtureValue{}, err
	}
	if err := requireForgeRunnerCommandTerminalReceiptNestedShapes(shape); err != nil {
		return forgeRunnerCommandTerminalReceiptFixtureValue{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var fixture forgeRunnerCommandTerminalReceiptFixtureValue
	if err := decoder.Decode(&fixture); err != nil {
		return forgeRunnerCommandTerminalReceiptFixtureValue{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgeRunnerCommandTerminalReceiptFixtureValue{}, errors.New("trailing JSON value")
		}
		return forgeRunnerCommandTerminalReceiptFixtureValue{}, err
	}
	if err := validateForgeRunnerCommandTerminalReceipt(fixture); err != nil {
		return forgeRunnerCommandTerminalReceiptFixtureValue{}, err
	}
	return fixture, nil
}

func requireForgeRunnerCommandTerminalReceiptNestedShapes(root map[string]json.RawMessage) error {
	exact := []struct {
		name  string
		keys  []string
		child string
	}{
		{"authority", []string{"device_identity_verified", "command_persisted", "reservation_created", "execution_authorized", "dispatch_performed", "audit_published"}, ""},
		{"grant", []string{"v", "attempt_id", "target_id", "epoch", "fencing_token", "issued_at_ms", "expires_at_ms"}, ""},
		{"command", []string{"v", "command_id", "lease_proof", "idempotency_key", "workspace_ref", "argv", "timeout_ms", "max_output_bytes"}, "lease_proof"},
		{"receipt", []string{"v", "command_id", "command_sha256", "proof", "disposition", "observed_at_ms"}, "proof"},
		{"expected", []string{"command_sha256", "receipt_valid"}, ""},
	}
	for _, item := range exact {
		var value map[string]json.RawMessage
		if err := json.Unmarshal(root[item.name], &value); err != nil {
			return err
		}
		if err := requireForgeRunnerCommandTerminalReceiptShape(value, item.keys); err != nil {
			return errors.New(item.name + ": " + err.Error())
		}
		if item.child != "" {
			var child map[string]json.RawMessage
			if err := json.Unmarshal(value[item.child], &child); err != nil {
				return err
			}
			if err := requireForgeRunnerCommandTerminalReceiptShape(child, []string{"attempt_id", "target_id", "epoch", "fencing_token"}); err != nil {
				return errors.New(item.name + "." + item.child + ": " + err.Error())
			}
		}
	}
	var receiptObject map[string]json.RawMessage
	if err := json.Unmarshal(root["receipt"], &receiptObject); err != nil {
		return err
	}
	var disposition map[string]json.RawMessage
	if err := json.Unmarshal(receiptObject["disposition"], &disposition); err != nil {
		return err
	}
	return requireForgeRunnerCommandTerminalReceiptShape(disposition, []string{"kind", "receipt_sha256"})
}

func requireForgeRunnerCommandTerminalReceiptShape(value map[string]json.RawMessage, fields []string) error {
	if len(value) != len(fields) {
		return errors.New("object fields are not exact")
	}
	for _, field := range fields {
		if _, ok := value[field]; !ok {
			return errors.New("missing field " + field)
		}
	}
	return nil
}

func validateForgeRunnerCommandTerminalReceipt(fixture forgeRunnerCommandTerminalReceiptFixtureValue) error {
	if !validForgeRunnerCommandTerminalReceiptAuthority(fixture.Authority) ||
		!validForgeRunnerCommandTerminalReceiptText(fixture.Grant.AttemptID, 128) ||
		!validForgeRunnerCommandTerminalReceiptText(fixture.Grant.TargetID, 128) ||
		!validForgeRunnerCommandTerminalReceiptText(fixture.Grant.FencingToken, 256) ||
		fixture.Grant.ExpiresAtMS <= fixture.Grant.IssuedAtMS ||
		fixture.Grant.ExpiresAtMS-fixture.Grant.IssuedAtMS < 1000 ||
		fixture.Grant.ExpiresAtMS-fixture.Grant.IssuedAtMS > 600000 ||
		fixture.Grant.ExpiresAtMS > forgeRunnerCommandTerminalReceiptMaxSafeInteger {
		return errors.New("invalid lease grant")
	}
	command := fixture.Command
	if command.V != 1 || !validForgeRunnerCommandTerminalReceiptText(command.CommandID, 128) || !validForgeRunnerCommandTerminalReceiptText(command.IdempotencyKey, 256) || !validForgeRunnerCommandTerminalReceiptText(command.WorkspaceRef, 256) || len(command.Argv) == 0 || len(command.Argv) > 64 || command.TimeoutMS == 0 || command.TimeoutMS > 600000 || command.MaxOutputBytes == 0 || command.MaxOutputBytes > 8*1024*1024 || command.LeaseProof != (forgeRunnerCommandTerminalReceiptProof{AttemptID: fixture.Grant.AttemptID, TargetID: fixture.Grant.TargetID, Epoch: fixture.Grant.Epoch, FencingToken: fixture.Grant.FencingToken}) {
		return errors.New("invalid command binding")
	}
	for index, argument := range command.Argv {
		if index > 0 && argument == "" {
			continue
		}
		if !validForgeRunnerCommandTerminalReceiptText(argument, 4096) || (index > 0 && strings.TrimSpace(argument) != argument) {
			return errors.New("invalid command argv")
		}
	}
	digest, err := forgeRunnerCommandTerminalReceiptCommandDigest(command)
	if err != nil || digest != fixture.Expected.CommandSHA256 || fixture.Receipt.CommandSHA256 != digest {
		return errors.New("command digest mismatch")
	}
	receipt := fixture.Receipt
	if receipt.V != 1 || receipt.CommandID != command.CommandID || receipt.Proof != command.LeaseProof || receipt.ObservedAtMS < fixture.Grant.IssuedAtMS || receipt.ObservedAtMS >= fixture.Grant.ExpiresAtMS || !validForgeRunnerCommandTerminalReceiptDisposition(receipt.Disposition) || !fixture.Expected.ReceiptValid {
		return errors.New("invalid terminal receipt")
	}
	return nil
}

func validForgeRunnerCommandTerminalReceiptAuthority(value forgeRunnerCommandTerminalReceiptAuthority) bool {
	return value == (forgeRunnerCommandTerminalReceiptAuthority{})
}
func validForgeRunnerCommandTerminalReceiptText(value string, limit int) bool {
	return value != "" && len(value) <= limit && utf8.ValidString(value) && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}
func validForgeRunnerCommandTerminalReceiptDisposition(value forgeRunnerCommandTerminalReceiptDisposition) bool {
	switch value.Kind {
	case "completed":
		return validForgeRunnerCommandTerminalReceiptDigest(value.ReceiptSHA256) && value.Reason == ""
	case "failed", "uncertain":
		return value.ReceiptSHA256 == "" && validForgeRunnerCommandTerminalReceiptText(value.Reason, 256)
	default:
		return false
	}
}
func validForgeRunnerCommandTerminalReceiptDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}
func forgeRunnerCommandTerminalReceiptCommandDigest(command forgeRunnerCommandTerminalReceiptCommand) (string, error) {
	encoded, err := json.Marshal(command)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("forge.runtime.runner-command.v1\x00"), encoded...))
	return hex.EncodeToString(digest[:]), nil
}

func rejectForgeRunnerCommandTerminalReceiptDuplicateKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanForgeRunnerCommandTerminalReceiptJSON(decoder); err != nil {
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
func scanForgeRunnerCommandTerminalReceiptJSON(decoder *json.Decoder) error {
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
		seen := map[string]bool{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return errors.New("duplicate JSON object key")
			}
			seen[name] = true
			if err := scanForgeRunnerCommandTerminalReceiptJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := scanForgeRunnerCommandTerminalReceiptJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return errors.New("invalid JSON delimiter")
	}
}
