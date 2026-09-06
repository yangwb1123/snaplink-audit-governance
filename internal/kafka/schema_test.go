package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/outbox"
)

type recordingProducerWriter struct {
	messages []kafka.Message
	contexts []context.Context
}

func (w *recordingProducerWriter) WriteMessages(ctx context.Context, messages ...kafka.Message) error {
	w.contexts = append(w.contexts, ctx)
	for _, message := range messages {
		message.Key = append([]byte(nil), message.Key...)
		message.Value = append([]byte(nil), message.Value...)
		w.messages = append(w.messages, message)
	}
	return nil
}

func (w *recordingProducerWriter) Close() error { return nil }

func contractEvent() domain.Event {
	return domain.Event{
		EventID:            "evt-contract-1",
		TenantID:           "tenant-a",
		SourceSystem:       "crm",
		EventType:          "audit.event",
		SchemaID:           "audit.event",
		SchemaVersion:      1,
		OccurredAt:         time.Unix(1_700_000_000, 0).UTC(),
		ReceivedAt:         time.Unix(1_700_000_001, 0).UTC(),
		Actor:              domain.Actor{ID: "user-1"},
		Action:             "update",
		Outcome:            "success",
		DataClassification: "internal",
		RetentionClass:     "standard",
		IdempotencyKey:     "idem-contract-1",
		Payload:            map[string]any{"resource": "file", "value": int64(1)},
	}
}

func canonicalTestEvent(t *testing.T, event domain.Event) []byte {
	t.Helper()
	value, err := domain.CanonicalJSON(event)
	if err != nil {
		t.Fatalf("canonical event: %v", err)
	}
	return value
}

func TestValidateEventJSONChannelSchemas(t *testing.T) {
	accepted := contractEvent()
	acceptedJSON := canonicalTestEvent(t, accepted)
	if _, err := ValidateEventJSON(AcceptedEventSchema, acceptedJSON); err != nil {
		t.Fatalf("accepted event without chain state rejected: %v", err)
	}
	var acceptedFields map[string]any
	if err := json.Unmarshal(acceptedJSON, &acceptedFields); err != nil {
		t.Fatal(err)
	}
	delete(acceptedFields, "received_at")
	withoutReceivedAt, err := json.Marshal(acceptedFields)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateEventJSON(AcceptedEventSchema, withoutReceivedAt); err != nil {
		t.Fatalf("accepted event without optional received_at rejected: %v", err)
	}

	ledgered := accepted
	ledgered.StreamID = "tenant-a:source:crm"
	ledgered.Sequence = 1
	ledgered.Hash = "hash-1"
	ledgeredJSON, err := encodeEventForSchema(LedgeredEventSchema, ledgered)
	if err != nil {
		t.Fatalf("encode first ledgered event: %v", err)
	}
	if _, err := ValidateEventJSON(LedgeredEventSchema, ledgeredJSON); err != nil {
		t.Fatalf("valid first ledgered event rejected: %v", err)
	}
	if !bytes.Contains(ledgeredJSON, []byte(`"prev_hash":""`)) {
		t.Fatalf("first ledgered event bytes=%s, want explicit empty prev_hash", ledgeredJSON)
	}

	subsequent := ledgered
	subsequent.Sequence = 2
	subsequent.PrevHash = "hash-1"
	subsequentJSON, err := encodeEventForSchema(LedgeredEventSchema, subsequent)
	if err != nil {
		t.Fatalf("encode subsequent ledgered event: %v", err)
	}
	if _, err := ValidateEventJSON(LedgeredEventSchema, subsequentJSON); err != nil {
		t.Fatalf("valid subsequent ledgered event rejected: %v", err)
	}

	for _, tc := range []struct {
		name  string
		value []byte
		field string
	}{
		{name: "first_sequence_missing_stream_id", value: ledgeredJSON, field: "stream_id"},
		{name: "first_sequence_missing_sequence", value: ledgeredJSON, field: "sequence"},
		{name: "first_sequence_missing_prev_hash", value: ledgeredJSON, field: "prev_hash"},
		{name: "first_sequence_missing_hash", value: ledgeredJSON, field: "hash"},
		{name: "subsequent_missing_prev_hash", value: subsequentJSON, field: "prev_hash"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := map[string]any{}
			if err := json.Unmarshal(tc.value, &value); err != nil {
				t.Fatal(err)
			}
			delete(value, tc.field)
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ValidateEventJSON(LedgeredEventSchema, encoded); err == nil {
				t.Fatalf("missing %s accepted", tc.field)
			}
		})
	}

	for _, sequence := range []int64{0, -1} {
		t.Run("sequence_boundary", func(t *testing.T) {
			value := ledgered
			value.Sequence = sequence
			value.PrevHash = ""
			encoded, err := encodeEventForSchema(LedgeredEventSchema, value)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ValidateEventJSON(LedgeredEventSchema, encoded); err == nil {
				t.Fatalf("sequence %d accepted", sequence)
			}
		})
	}

	for _, field := range []string{"stream_id", "hash"} {
		t.Run("empty_"+field, func(t *testing.T) {
			value := subsequent
			if field == "stream_id" {
				value.StreamID = ""
			} else {
				value.Hash = ""
			}
			if _, err := ValidateEventJSON(LedgeredEventSchema, canonicalTestEvent(t, value)); err == nil {
				t.Fatalf("empty %s accepted", field)
			}
		})
	}

	value := subsequent
	value.PrevHash = ""
	if _, err := ValidateEventJSON(LedgeredEventSchema, canonicalTestEvent(t, value)); err == nil {
		t.Fatal("subsequent ledgered event with empty prev_hash accepted")
	}

	firstWithPrev := ledgered
	firstWithPrev.PrevHash = "previous-hash"
	if _, err := ValidateEventJSON(LedgeredEventSchema, canonicalTestEvent(t, firstWithPrev)); err == nil {
		t.Fatal("first ledgered event with a predecessor hash accepted")
	}
	firstWithWhitespacePrev := ledgered
	firstWithWhitespacePrev.PrevHash = "  "
	if _, err := ValidateEventJSON(LedgeredEventSchema, canonicalTestEvent(t, firstWithWhitespacePrev)); err == nil {
		t.Fatal("first ledgered event with whitespace predecessor accepted")
	}
	secondWithWhitespacePrev := subsequent
	secondWithWhitespacePrev.PrevHash = " \t"
	if _, err := ValidateEventJSON(LedgeredEventSchema, canonicalTestEvent(t, secondWithWhitespacePrev)); err == nil {
		t.Fatal("subsequent ledgered event with whitespace predecessor accepted")
	}
}

func TestValidateEventMessageRequiresMatchingKafkaKey(t *testing.T) {
	value := canonicalTestEvent(t, contractEvent())
	for _, test := range []struct {
		name string
		key  []byte
		want bool
	}{
		{name: "matching", key: []byte("evt-contract-1"), want: true},
		{name: "empty", key: nil},
		{name: "mismatched", key: []byte("stale-event-id")},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ValidateEventMessage(AcceptedEventSchema, test.key, value)
			if (err == nil) != test.want {
				t.Fatalf("ValidateEventMessage() error=%v, want success=%v", err, test.want)
			}
		})
	}
}

func TestProducerRepublishNormalizesKeyAndValidatesAcceptedValue(t *testing.T) {
	writer := &recordingProducerWriter{}
	producer := &Producer{writer: writer, topic: TopicAccepted}
	value := canonicalTestEvent(t, contractEvent())
	if err := producer.Republish(context.Background(), []byte("stale-key"), value); err != nil {
		t.Fatalf("Republish() error = %v", err)
	}
	if len(writer.messages) != 1 {
		t.Fatalf("writer messages=%d, want 1", len(writer.messages))
	}
	if got := string(writer.messages[0].Key); got != "evt-contract-1" {
		t.Fatalf("republish key=%q, want payload event_id", got)
	}
	if string(writer.messages[0].Value) != string(value) {
		t.Fatalf("republish value=%s, want byte-identical %s", writer.messages[0].Value, value)
	}

	invalid := append(append([]byte(nil), value...), value...)
	if err := producer.Republish(context.Background(), nil, invalid); err == nil {
		t.Fatal("Republish() accepted multiple JSON values")
	}
	if len(writer.messages) != 1 {
		t.Fatalf("writer messages=%d, want no write for invalid recovered value", len(writer.messages))
	}
}

func TestProducerDeliverValidatesTopicSchemaBeforeWrite(t *testing.T) {
	cases := []struct {
		name  string
		topic string
		event domain.Event
	}{
		{name: "accepted missing payload", topic: TopicAccepted, event: contractEvent()},
		{name: "ledgered missing chain", topic: TopicLedgered, event: contractEvent()},
		{name: "unknown topic", topic: "audit.events.unknown.v1", event: contractEvent()},
	}
	cases[0].event.Payload = nil

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writer := &recordingProducerWriter{}
			producer := &Producer{writer: writer, topic: tc.topic}
			_, err := producer.Deliver(context.Background(), tc.event)
			if err == nil {
				t.Fatal("Deliver() error = nil, want permanent validation error")
			}
			var deliveryErr *outbox.DeliveryError
			if !errors.As(err, &deliveryErr) || !deliveryErr.Permanent {
				t.Fatalf("error=%v, want permanent DeliveryError", err)
			}
			if len(writer.messages) != 0 {
				t.Fatalf("writer messages=%d, want zero on rejected event", len(writer.messages))
			}
		})
	}
}

func TestProducerPublishFailureWritesDLQContract(t *testing.T) {
	writer := &recordingProducerWriter{}
	producer := &Producer{writer: writer, topic: TopicDLQ}
	ctx := context.WithValue(context.Background(), struct{}{}, "publish-context")
	failure := Failure{
		EventID:      "evt-failure",
		ErrorCode:    ErrorCodeAttemptsExhausted,
		ErrorMessage: "temporary API outage",
		TenantID:     "tenant-a",
	}
	if err := producer.PublishFailure(ctx, failure); err != nil {
		t.Fatalf("PublishFailure() error = %v", err)
	}
	if producer.topic != TopicDLQ {
		t.Fatalf("producer topic=%q, want %q", producer.topic, TopicDLQ)
	}
	if len(writer.messages) != 1 || len(writer.contexts) != 1 {
		t.Fatalf("writer messages=%d contexts=%d, want 1/1", len(writer.messages), len(writer.contexts))
	}
	message := writer.messages[0]
	if string(message.Key) != failure.EventID {
		t.Fatalf("DLQ Kafka key=%q, want event_id %q", message.Key, failure.EventID)
	}
	want, err := json.Marshal(failure)
	if err != nil {
		t.Fatalf("marshal expected failure: %v", err)
	}
	if string(message.Value) != string(want) {
		t.Fatalf("DLQ Kafka value=%s, want %s", message.Value, want)
	}
	if writer.contexts[0] != ctx {
		t.Fatal("PublishFailure did not pass the caller context to the writer")
	}
}

func TestProducerDeliverWritesCanonicalEventWithEventIDKey(t *testing.T) {
	for _, topic := range []string{TopicAccepted, TopicLedgered} {
		t.Run(topic, func(t *testing.T) {
			event := contractEvent()
			if topic == TopicLedgered {
				event.StreamID = "tenant-a:source:crm"
				event.Sequence = 1
				event.Hash = "hash-1"
			}
			writer := &recordingProducerWriter{}
			producer := &Producer{writer: writer, topic: topic}
			if _, err := producer.Deliver(context.Background(), event); err != nil {
				t.Fatalf("Deliver() error = %v", err)
			}
			if len(writer.messages) != 1 {
				t.Fatalf("writer messages=%d, want 1", len(writer.messages))
			}
			message := writer.messages[0]
			if string(message.Key) != event.EventID {
				t.Fatalf("Kafka key=%q, want event_id %q", message.Key, event.EventID)
			}
			schema := AcceptedEventSchema
			if topic == TopicLedgered {
				schema = LedgeredEventSchema
			}
			want, err := encodeEventForSchema(schema, event)
			if err != nil {
				t.Fatalf("encode event for schema %s: %v", schema, err)
			}
			if string(message.Value) != string(want) {
				t.Fatalf("Kafka value=%s, want canonical contract bytes %s", message.Value, want)
			}
			if topic == TopicLedgered && !bytes.Contains(message.Value, []byte(`"prev_hash":""`)) {
				t.Fatalf("Kafka value=%s, want explicit empty prev_hash on ledgered topic", message.Value)
			}
			if _, err := ValidateEventJSON(schema, message.Value); err != nil {
				t.Fatalf("written value fails %s validation: %v", schema, err)
			}
		})
	}
}

func TestConsumerRejectsAcceptedShapeBeforeIngestForLedgeredSchema(t *testing.T) {
	message := validMessage("evt-accepted-shape", 1)
	reader := &fakeReader{messages: []kafka.Message{message}}
	ingested := 0
	consumer := runConsumerWithMetrics(t, reader, func(context.Context, domain.Event) error {
		ingested++
		return nil
	}, WithInputSchema(LedgeredEventSchema), WithDLQ(reader), WithMaxAttempts(3))
	if ingested != 0 {
		t.Fatalf("ingested=%d, want zero for accepted-shaped ledgered input", ingested)
	}
	if len(reader.published) != 1 || reader.published[0].ErrorCode != ErrorCodePermanentError {
		t.Fatalf("published=%v, want one permanent_error", reader.published)
	}
	if len(reader.commits) != 1 {
		t.Fatalf("commits=%d, want one commit after DLQ classification", len(reader.commits))
	}
	if metrics := consumer.Metrics(); metrics.IngestFailures != 0 || metrics.DeadLettered != 1 {
		t.Fatalf("metrics=%+v, want schema rejection outside ingest retry counters", metrics)
	}
}

func TestConsumerRejectsValidAcceptedEventOnLedgeredSchema(t *testing.T) {
	value := canonicalTestEvent(t, contractEvent())
	reader := &fakeReader{messages: []kafka.Message{{Key: []byte("evt-contract-1"), Value: value, Offset: 1}}}
	ingested := 0
	runConsumerWithMetrics(t, reader, func(context.Context, domain.Event) error {
		ingested++
		return nil
	}, WithInputSchema(LedgeredEventSchema), WithDLQ(reader))
	if ingested != 0 {
		t.Fatalf("ingested=%d, want zero for a valid AcceptedEvent on ledgered input", ingested)
	}
	if len(reader.published) != 1 || reader.published[0].ErrorCode != ErrorCodePermanentError {
		t.Fatalf("published=%v, want one permanent_error", reader.published)
	}
	if len(reader.commits) != 1 {
		t.Fatalf("commits=%d, want one commit after schema rejection", len(reader.commits))
	}
}

func TestConsumerClassifiesCompleteSchemaViolationAsPermanent(t *testing.T) {
	event := contractEvent()
	event.StreamID = "tenant-a:source:crm"
	event.Sequence = 1
	event.Hash = "hash-1"
	value := canonicalTestEvent(t, event)
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(value, &fields); err != nil {
		t.Fatal(err)
	}
	fields["sequence"] = json.RawMessage(`"not-an-integer"`)
	value, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	reader := &fakeReader{messages: []kafka.Message{{Key: []byte(event.EventID), Value: value, Offset: 1}}}
	ingested := 0
	runConsumerWithMetrics(t, reader, func(context.Context, domain.Event) error {
		ingested++
		return nil
	}, WithInputSchema(LedgeredEventSchema), WithDLQ(reader))
	if ingested != 0 {
		t.Fatalf("ingested=%d, want zero for schema violation", ingested)
	}
	if len(reader.published) != 1 || reader.published[0].ErrorCode != ErrorCodePermanentError {
		t.Fatalf("published=%v, want one permanent_error", reader.published)
	}
	if reader.published[0].EventID != event.EventID {
		t.Fatalf("failure event_id=%q, want %q", reader.published[0].EventID, event.EventID)
	}
	if len(reader.commits) != 1 {
		t.Fatalf("commits=%d, want one commit after schema rejection", len(reader.commits))
	}
}

func TestConsumerRejectsEmptyOrMismatchedKafkaKey(t *testing.T) {
	value := canonicalTestEvent(t, contractEvent())
	for _, test := range []struct {
		name string
		key  []byte
	}{
		{name: "empty", key: nil},
		{name: "mismatched", key: []byte("stale-event-id")},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := &fakeReader{messages: []kafka.Message{{Key: test.key, Value: value, Offset: 1}}}
			ingested := 0
			runConsumerWithMetrics(t, reader, func(context.Context, domain.Event) error {
				ingested++
				return nil
			}, WithInputSchema(AcceptedEventSchema), WithDLQ(reader))
			if ingested != 0 {
				t.Fatalf("ingested=%d, want zero for invalid Kafka key", ingested)
			}
			if len(reader.published) != 1 || reader.published[0].ErrorCode != ErrorCodePermanentError {
				t.Fatalf("published=%v, want one permanent_error", reader.published)
			}
			if len(reader.commits) != 1 {
				t.Fatalf("commits=%d, want one commit after key rejection", len(reader.commits))
			}
		})
	}
}
