package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/snaplink/audit-governance/internal/domain"
)

// EventSchema identifies the event envelope accepted on a contract topic.
// An empty schema means that a non-contract topic keeps the legacy consumer
// behavior; the two contract topics are always selected by exact address.
type EventSchema string

const (
	AcceptedEventSchema EventSchema = "AcceptedEvent"
	LedgeredEventSchema EventSchema = "LedgeredEvent"
)

var envelopeRequiredFields = []string{
	"event_id", "source_system", "event_type", "schema_id", "schema_version",
	"occurred_at", "actor", "action", "outcome", "data_classification",
	"retention_class", "idempotency_key",
}

var envelopeStringFields = []string{
	"event_id", "source_system", "event_type", "schema_id", "action", "outcome",
	"data_classification", "retention_class", "idempotency_key", "operation_id",
	"causation_id", "correlation_id", "trace_id", "span_id", "aggregate_type",
	"aggregate_id", "workflow_instance_id", "execution_run_id", "reason", "payload_ref",
}

var envelopeNonEmptyFields = []string{
	"event_id", "source_system", "event_type", "schema_id", "action", "outcome",
	"data_classification", "retention_class", "idempotency_key",
}

// ValidateEventJSON decodes one JSON object and validates the selected
// AsyncAPI event schema. It deliberately validates the bytes, rather than a
// pre-decoded domain value, so omitted required fields and canonicalization
// changes cannot bypass the channel contract.
func ValidateEventJSON(schema EventSchema, value []byte) (domain.Event, error) {
	var event domain.Event
	if schema != AcceptedEventSchema && schema != LedgeredEventSchema {
		return event, fmt.Errorf("unsupported event schema %q", schema)
	}

	fields, err := decodeJSONObject(value, &event)
	if err != nil {
		return event, err
	}
	if err := validateEnvelope(fields); err != nil {
		return event, fmt.Errorf("%s: %w", schema, err)
	}
	if schema == LedgeredEventSchema {
		if err := validateLedgerState(fields, event); err != nil {
			return event, fmt.Errorf("%s: %w", schema, err)
		}
	}
	return event, nil
}

// eventSchemaForTopic maps only the exact public event addresses. A custom
// topic is intentionally left unvalidated unless its owner explicitly adds
// WithInputSchema; silently treating an unknown topic as accepted would make
// channel drift indistinguishable from valid traffic.
func eventSchemaForTopic(topic string) EventSchema {
	switch topic {
	case TopicAccepted:
		return AcceptedEventSchema
	case TopicLedgered:
		return LedgeredEventSchema
	default:
		return ""
	}
}

func decodeJSONObject(value []byte, event *domain.Event) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	if err := decoder.Decode(&fields); err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, fmt.Errorf("event value must be a JSON object")
	}
	if err := requireSingleJSONValue(decoder); err != nil {
		return nil, err
	}

	decoder = json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	if err := decoder.Decode(event); err != nil {
		return nil, err
	}
	return fields, nil
}

func requireSingleJSONValue(decoder *json.Decoder) error {
	var extra json.RawMessage
	err := decoder.Decode(&extra)
	if err == io.EOF {
		return nil
	}
	if err == nil {
		return fmt.Errorf("message value must contain one JSON value")
	}
	return err
}

func validateEnvelope(fields map[string]json.RawMessage) error {
	for _, name := range envelopeRequiredFields {
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("required field %q is missing", name)
		}
	}
	for _, name := range envelopeStringFields {
		if raw, ok := fields[name]; ok {
			if err := validateString(raw, name, 8192); err != nil {
				return err
			}
		}
	}
	if raw, ok := fields["tenant_id"]; ok {
		if err := validateString(raw, "tenant_id", 0); err != nil {
			return err
		}
	}
	for _, name := range []string{"stream_id", "prev_hash", "hash"} {
		if raw, ok := fields[name]; ok {
			if err := validateString(raw, name, 0); err != nil {
				return err
			}
		}
	}
	if raw, ok := fields["sequence"]; ok {
		if err := validateInteger(raw, "sequence", -1<<63); err != nil {
			return err
		}
	}
	if raw, ok := fields["aggregate_version"]; ok {
		if err := validateInteger(raw, "aggregate_version", -1<<63); err != nil {
			return err
		}
	}
	for _, name := range envelopeNonEmptyFields {
		if strings.TrimSpace(stringValue(fields[name])) == "" {
			return fmt.Errorf("%s must be non-empty", name)
		}
	}
	if err := validateInteger(fields["schema_version"], "schema_version", 1); err != nil {
		return err
	}
	if err := validateTimestamp(fields["occurred_at"], "occurred_at"); err != nil {
		return err
	}
	if receivedAt, ok := fields["received_at"]; ok {
		if err := validateTimestamp(receivedAt, "received_at"); err != nil {
			return err
		}
	}
	if err := validateActor(fields["actor"]); err != nil {
		return err
	}
	if err := validateTargets(fields["targets"]); err != nil {
		return err
	}
	if err := validateChangedFields(fields["changed_fields"]); err != nil {
		return err
	}

	payload, hasPayload := fields["payload"]
	payloadRef, hasPayloadRef := fields["payload_ref"]
	if !hasPayload && !hasPayloadRef {
		return fmt.Errorf("one of payload or payload_ref is required")
	}
	if hasPayload {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(payload, &object); err != nil || object == nil {
			return fmt.Errorf("payload must be a JSON object")
		}
	}
	if hasPayloadRef {
		if err := validateString(payloadRef, "payload_ref", 8192); err != nil {
			return err
		}
	}
	return nil
}

func validateActor(raw json.RawMessage) error {
	var actor map[string]json.RawMessage
	if err := json.Unmarshal(raw, &actor); err != nil || actor == nil || stringIsNull(raw) {
		return fmt.Errorf("actor must be a JSON object")
	}
	id, ok := actor["id"]
	if !ok {
		return fmt.Errorf("actor.id is required")
	}
	if err := validateString(id, "actor.id", 8192); err != nil {
		return err
	}
	if strings.TrimSpace(stringValue(id)) == "" {
		return fmt.Errorf("actor.id must be non-empty")
	}
	for _, name := range []string{"type", "name", "department"} {
		if raw, ok := actor[name]; ok {
			if err := validateString(raw, "actor."+name, 8192); err != nil {
				return err
			}
		}
	}
	if roles, ok := actor["roles"]; ok {
		if err := validateStringArray(roles, "actor.roles", 64); err != nil {
			return err
		}
	}
	return nil
}

func validateTargets(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}
	var targets []json.RawMessage
	if stringIsNull(raw) || json.Unmarshal(raw, &targets) != nil {
		return fmt.Errorf("targets must be an array")
	}
	if len(targets) > 64 {
		return fmt.Errorf("targets exceeds 64 items")
	}
	for index, rawTarget := range targets {
		var target map[string]json.RawMessage
		if stringIsNull(rawTarget) || json.Unmarshal(rawTarget, &target) != nil || target == nil {
			return fmt.Errorf("targets[%d] must be a JSON object", index)
		}
		for _, name := range []string{"type", "id", "name"} {
			if value, ok := target[name]; ok {
				if err := validateString(value, fmt.Sprintf("targets[%d].%s", index, name), 8192); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateStringArray(raw json.RawMessage, name string, maxItems int) error {
	var values []json.RawMessage
	if stringIsNull(raw) || json.Unmarshal(raw, &values) != nil {
		return fmt.Errorf("%s must be an array", name)
	}
	if len(values) > maxItems {
		return fmt.Errorf("%s exceeds %d items", name, maxItems)
	}
	for index, value := range values {
		if err := validateString(value, fmt.Sprintf("%s[%d]", name, index), 8192); err != nil {
			return err
		}
	}
	return nil
}

func validateChangedFields(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}
	var fields map[string]json.RawMessage
	if stringIsNull(raw) || json.Unmarshal(raw, &fields) != nil || fields == nil {
		return fmt.Errorf("changed_fields must be a JSON object")
	}
	if len(fields) > 256 {
		return fmt.Errorf("changed_fields exceeds 256 properties")
	}
	for name, change := range fields {
		if len(name) > 8192 {
			return fmt.Errorf("changed_fields property name exceeds 8192 UTF-8 bytes")
		}
		var object map[string]json.RawMessage
		if stringIsNull(change) || json.Unmarshal(change, &object) != nil || object == nil {
			return fmt.Errorf("changed_fields[%q] must be a JSON object", name)
		}
	}
	return nil
}

func validateLedgerState(fields map[string]json.RawMessage, event domain.Event) error {
	for _, name := range []string{"stream_id", "sequence", "hash"} {
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("required ledger field %q is missing", name)
		}
	}
	if err := validateString(fields["stream_id"], "stream_id", 0); err != nil {
		return err
	}
	if strings.TrimSpace(event.StreamID) == "" {
		return fmt.Errorf("stream_id must be non-empty")
	}
	if err := validateInteger(fields["sequence"], "sequence", 1); err != nil {
		return err
	}
	if event.Sequence < 1 {
		return fmt.Errorf("sequence must be at least 1")
	}
	if err := validateString(fields["hash"], "hash", 0); err != nil {
		return err
	}
	if strings.TrimSpace(event.Hash) == "" {
		return fmt.Errorf("hash must be non-empty")
	}
	if prevHash, ok := fields["prev_hash"]; ok {
		if err := validateString(prevHash, "prev_hash", 0); err != nil {
			return err
		}
	}
	if event.Sequence > 1 && strings.TrimSpace(event.PrevHash) == "" {
		return fmt.Errorf("prev_hash must be non-empty for sequence greater than 1")
	}
	return nil
}

func validateString(raw json.RawMessage, name string, maxBytes int) error {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || stringIsNull(raw) {
		return fmt.Errorf("%s must be a string", name)
	}
	if maxBytes > 0 && len(value) > maxBytes {
		return fmt.Errorf("%s exceeds %d UTF-8 bytes", name, maxBytes)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must contain valid UTF-8", name)
	}
	return nil
}

func stringValue(raw json.RawMessage) string {
	var value string
	_ = json.Unmarshal(raw, &value)
	return value
}

func validateInteger(raw json.RawMessage, name string, minimum int64) error {
	var value int64
	if err := json.Unmarshal(raw, &value); err != nil || stringIsNull(raw) {
		return fmt.Errorf("%s must be an integer", name)
	}
	if value < minimum {
		return fmt.Errorf("%s must be at least %d", name, minimum)
	}
	return nil
}

func validateTimestamp(raw json.RawMessage, name string) error {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || stringIsNull(raw) {
		return fmt.Errorf("%s must be a date-time string", name)
	}
	if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
		return fmt.Errorf("%s must be an RFC3339 date-time: %w", name, err)
	}
	return nil
}

func stringIsNull(raw []byte) bool { return bytes.Equal(bytes.TrimSpace(raw), []byte("null")) }

// WithInputSchema selects the schema applied after JSON syntax and single
// value parsing but before the consumer invokes its ingest function.
func WithInputSchema(schema EventSchema) ConsumerOption {
	return func(c *Consumer) { c.inputSchema = schema }
}
