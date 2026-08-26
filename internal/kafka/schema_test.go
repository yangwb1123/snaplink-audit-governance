package kafka

import (
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
}

func (w *recordingProducerWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
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
	ledgeredJSON := canonicalTestEvent(t, ledgered)
	if _, err := ValidateEventJSON(LedgeredEventSchema, ledgeredJSON); err != nil {
		t.Fatalf("valid first ledgered event rejected: %v", err)
	}

	ledgered.Sequence = 2
	ledgered.PrevHash = "hash-1"
	if _, err := ValidateEventJSON(LedgeredEventSchema, canonicalTestEvent(t, ledgered)); err != nil {
		t.Fatalf("valid subsequent ledgered event rejected: %v", err)
	}

	for _, field := range []string{"stream_id", "sequence", "hash"} {
		t.Run("missing_"+field, func(t *testing.T) {
			value := map[string]any{}
			if err := json.Unmarshal(ledgeredJSON, &value); err != nil {
				t.Fatal(err)
			}
			delete(value, field)
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ValidateEventJSON(LedgeredEventSchema, encoded); err == nil {
				t.Fatalf("missing %s accepted", field)
			}
		})
	}

	for _, sequence := range []int64{0, -1} {
		t.Run("sequence_boundary", func(t *testing.T) {
			value := ledgered
			value.Sequence = sequence
			value.PrevHash = ""
			if _, err := ValidateEventJSON(LedgeredEventSchema, canonicalTestEvent(t, value)); err == nil {
				t.Fatalf("sequence %d accepted", sequence)
			}
		})
	}

	for _, field := range []string{"stream_id", "hash"} {
		t.Run("empty_"+field, func(t *testing.T) {
			value := ledgered
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

	value := ledgered
	value.Sequence = 2
	value.PrevHash = ""
	if _, err := ValidateEventJSON(LedgeredEventSchema, canonicalTestEvent(t, value)); err == nil {
		t.Fatal("subsequent ledgered event with empty prev_hash accepted")
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
			want := canonicalTestEvent(t, event)
			if string(message.Value) != string(want) {
				t.Fatalf("Kafka value=%s, want canonical %s", message.Value, want)
			}
			schema := AcceptedEventSchema
			if topic == TopicLedgered {
				schema = LedgeredEventSchema
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
