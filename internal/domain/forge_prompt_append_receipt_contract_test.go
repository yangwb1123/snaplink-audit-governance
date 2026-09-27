package domain

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

// This receiver consumes only a content-minimized append receipt. It does not
// authenticate an owner, append a Prompt, create a Run, select a device, or
// publish Audit evidence.
//
//go:embed testdata/forge-prompt-append-receipt-v1.json
var forgePromptAppendReceiptFixture []byte

const forgePromptAppendReceiptMaxSafeInteger = uint64(9007199254740991)

type forgePromptAppendReceiptOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgePromptAppendReceiptRequest struct {
	ConversationID       string `json:"conversation_id"`
	ExpectedVersion      uint64 `json:"expected_version"`
	Role                 string `json:"role"`
	ContentSHA256        string `json:"content_sha256"`
	IdempotencyKeySHA256 string `json:"idempotency_key_sha256"`
}

type forgePromptAppendReceiptReceipt struct {
	ConversationID        string `json:"conversation_id"`
	PromptID              string `json:"prompt_id"`
	Role                  string `json:"role"`
	AggregateVersion      uint64 `json:"aggregate_version"`
	CreatedAtMS           uint64 `json:"created_at_ms"`
	Replayed              bool   `json:"replayed"`
	StorageCommitObserved bool   `json:"storage_commit_observed"`
	ContentIncluded       bool   `json:"content_included"`
}

type forgePromptAppendReceiptAuthority struct {
	RunCreated          bool `json:"run_created"`
	DeviceSelected      bool `json:"device_selected"`
	ReservationCreated  bool `json:"reservation_created"`
	DispatchPerformed   bool `json:"dispatch_performed"`
	ExecutionAuthorized bool `json:"execution_authorized"`
	AuditPublished      bool `json:"audit_published"`
}

type forgePromptAppendReceiptEnvelope struct {
	SchemaVersion string                            `json:"schema_version"`
	Owner         forgePromptAppendReceiptOwner     `json:"owner"`
	Request       forgePromptAppendReceiptRequest   `json:"request"`
	Receipt       forgePromptAppendReceiptReceipt   `json:"receipt"`
	Authority     forgePromptAppendReceiptAuthority `json:"authority"`
}

func TestForgePromptAppendReceiptReceiver(t *testing.T) {
	value, err := decodeForgePromptAppendReceipt(forgePromptAppendReceiptFixture)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateForgePromptAppendReceipt(value); err != nil {
		t.Fatal(err)
	}
	if value.Owner.Issuer != "https://id.example" || value.Owner.Subject != "user-1" ||
		value.Owner.TenantID != "tenant-1" || value.Request.ConversationID != "conversation-001" ||
		value.Request.ExpectedVersion != 2 || value.Receipt.PromptID != "prompt-003" ||
		value.Receipt.AggregateVersion != 3 || !value.Receipt.StorageCommitObserved ||
		value.Receipt.ContentIncluded || value.Receipt.Replayed {
		t.Fatalf("canonical Prompt append receipt drifted: %#v", value)
	}

	unknown := append(bytes.TrimSuffix(bytes.TrimSpace(forgePromptAppendReceiptFixture), []byte("}")), []byte(`,"unexpected":true}`)...)
	if _, err := decodeForgePromptAppendReceipt(unknown); err == nil {
		t.Fatal("unknown Prompt append receipt field accepted")
	}
	duplicate := append(bytes.TrimSuffix(bytes.TrimSpace(forgePromptAppendReceiptFixture), []byte("}")), []byte(`,"schema_version":"forge.prompt-append-receipt/v1"}`)...)
	if _, err := decodeForgePromptAppendReceipt(duplicate); err == nil {
		t.Fatal("duplicate Prompt append receipt field accepted")
	}
	if _, err := decodeForgePromptAppendReceipt(append(bytes.TrimSpace(forgePromptAppendReceiptFixture), []byte(" {}")...)); err == nil {
		t.Fatal("trailing Prompt append receipt value accepted")
	}

	mutated := bytes.Replace(forgePromptAppendReceiptFixture, []byte(`"aggregate_version": 3`), []byte(`"aggregate_version": 4`), 1)
	value, err = decodeForgePromptAppendReceipt(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if validateForgePromptAppendReceipt(value) == nil {
		t.Fatal("aggregate-version drift accepted")
	}
	mutated = bytes.Replace(forgePromptAppendReceiptFixture, []byte(`"audit_published": false`), []byte(`"audit_published": true`), 1)
	value, err = decodeForgePromptAppendReceipt(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if validateForgePromptAppendReceipt(value) == nil {
		t.Fatal("authority drift accepted")
	}
	mutated = bytes.Replace(forgePromptAppendReceiptFixture, []byte(`"content_included": false`), []byte(`"content_included": true`), 1)
	value, err = decodeForgePromptAppendReceipt(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if validateForgePromptAppendReceipt(value) == nil {
		t.Fatal("content disclosure accepted")
	}
}

func decodeForgePromptAppendReceipt(raw []byte) (forgePromptAppendReceiptEnvelope, error) {
	if err := rejectForgePromptAppendReceiptDuplicateKeys(raw); err != nil {
		return forgePromptAppendReceiptEnvelope{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value forgePromptAppendReceiptEnvelope
	if err := decoder.Decode(&value); err != nil {
		return forgePromptAppendReceiptEnvelope{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || len(fields) != 5 {
		return forgePromptAppendReceiptEnvelope{}, errors.New("Prompt append receipt fields are not exact")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return forgePromptAppendReceiptEnvelope{}, errors.New("trailing JSON value")
		}
		return forgePromptAppendReceiptEnvelope{}, err
	}
	return value, nil
}

func validateForgePromptAppendReceipt(value forgePromptAppendReceiptEnvelope) error {
	if value.SchemaVersion != "forge.prompt-append-receipt/v1" ||
		!validForgePromptAppendReceiptText(value.Owner.Issuer, 512) ||
		!validForgePromptAppendReceiptText(value.Owner.Subject, 512) ||
		!validForgePromptAppendReceiptText(value.Owner.TenantID, 512) ||
		!validForgePromptAppendReceiptIdentifier(value.Request.ConversationID) ||
		value.Request.ExpectedVersion == 0 || value.Request.ExpectedVersion > forgePromptAppendReceiptMaxSafeInteger ||
		value.Request.Role != "user" || !validForgePromptAppendReceiptDigest(value.Request.ContentSHA256) ||
		!validForgePromptAppendReceiptDigest(value.Request.IdempotencyKeySHA256) ||
		value.Receipt.ConversationID != value.Request.ConversationID ||
		!validForgePromptAppendReceiptIdentifier(value.Receipt.PromptID) || value.Receipt.Role != value.Request.Role ||
		value.Receipt.AggregateVersion != value.Request.ExpectedVersion+1 ||
		value.Receipt.AggregateVersion > forgePromptAppendReceiptMaxSafeInteger ||
		value.Receipt.CreatedAtMS > forgePromptAppendReceiptMaxSafeInteger || !value.Receipt.StorageCommitObserved ||
		value.Receipt.ContentIncluded || value.Authority != (forgePromptAppendReceiptAuthority{}) {
		return errors.New("invalid Prompt append receipt envelope")
	}
	return nil
}

func validForgePromptAppendReceiptText(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && value == strings.TrimSpace(value) &&
		!strings.ContainsAny(value, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x7f")
}

func validForgePromptAppendReceiptIdentifier(value string) bool {
	if !validForgePromptAppendReceiptText(value, 128) {
		return false
	}
	for index, character := range value {
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') && !(index > 0 && strings.ContainsRune(".:_+/-", character)) {
			return false
		}
	}
	return true
}

func validForgePromptAppendReceiptDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func rejectForgePromptAppendReceiptDuplicateKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	return scanForgePromptAppendReceiptJSON(decoder)
}

func scanForgePromptAppendReceiptJSON(decoder *json.Decoder) error {
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
			if err := scanForgePromptAppendReceiptJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case json.Delim('['):
		for decoder.More() {
			if err := scanForgePromptAppendReceiptJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return nil
	}
}
