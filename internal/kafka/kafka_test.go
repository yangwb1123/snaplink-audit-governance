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

type fakeReader struct {
	messages        []kafka.Message
	commits         []kafka.Message
	committedOffset int64
	publishCalls    int
	published       []Failure
	publishFunc     func(Failure) error
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	// 模拟真实 reader：只返回已提交 offset 之后的消息；失败且未提交的
	// 消息会再次返回（背压重试）；无可消费消息时阻塞到上下文取消。
	for _, message := range f.messages {
		if message.Offset > f.committedOffset {
			return message, nil
		}
	}
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (f *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	for _, message := range msgs {
		if message.Offset > f.committedOffset {
			f.committedOffset = message.Offset
		}
		f.commits = append(f.commits, message)
	}
	return nil
}

func (f *fakeReader) Config() kafka.ReaderConfig { return kafka.ReaderConfig{Topic: "test-topic"} }
func (f *fakeReader) Close() error               { return nil }

// PublishFailure lets tests attach the fake as the consumer's DLQ publisher.
func (f *fakeReader) PublishFailure(_ context.Context, failure Failure) error {
	f.publishCalls++
	if f.publishFunc != nil {
		if err := f.publishFunc(failure); err != nil {
			return err
		}
	}
	f.published = append(f.published, failure)
	return nil
}

func runConsumer(t *testing.T, reader *fakeReader, ingest IngestFunc, options ...ConsumerOption) *fakeReader {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	consumer := newConsumerWithReader(reader, ingest, time.Millisecond, options...)
	// EOF 结束循环；错误返回前 ctx 未取消则视为失败。
	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	return reader
}

func validMessage(eventID string, offset int64) kafka.Message {
	event := domain.Event{EventID: eventID, TenantID: "demo", SourceSystem: "demo", Action: "update", Outcome: "success"}
	encoded, _ := domain.CanonicalJSON(event)
	return kafka.Message{Key: []byte(eventID), Value: encoded, Partition: 0, Offset: offset}
}

func TestConsumerCommitsAfterSuccessfulIngest(t *testing.T) {
	reader := &fakeReader{messages: []kafka.Message{validMessage("evt-1", 1), validMessage("evt-2", 2)}}
	ingested := 0
	runConsumer(t, reader, func(_ context.Context, event domain.Event) error {
		ingested++
		return nil
	})
	if ingested != 2 || len(reader.commits) != 2 {
		t.Fatalf("ingested=%d commits=%d, want 2/2", ingested, len(reader.commits))
	}
}

func TestConsumerBackpressureRetriesFailedMessage(t *testing.T) {
	reader := &fakeReader{messages: []kafka.Message{validMessage("evt-1", 1)}}
	ingested := 0
	runConsumer(t, reader, func(_ context.Context, event domain.Event) error {
		ingested++
		if ingested == 1 {
			return errors.New("api unavailable")
		}
		return nil
	})
	if ingested != 2 {
		t.Fatalf("ingested=%d, want 2 (one failure then retry)", ingested)
	}
	if len(reader.commits) != 1 {
		t.Fatalf("commits=%d, want 1 (only the successful attempt)", len(reader.commits))
	}
}

func TestConsumerDeadLettersUnparsableMessage(t *testing.T) {
	reader := &fakeReader{messages: []kafka.Message{{Key: []byte("bad"), Value: []byte("not-json"), Partition: 0, Offset: 9}}}
	ingested := 0
	runConsumer(t, reader, func(_ context.Context, _ domain.Event) error {
		ingested++
		return nil
	})
	if ingested != 0 {
		t.Fatalf("unparsable message reached ingest %d times", ingested)
	}
	if len(reader.commits) != 1 {
		t.Fatalf("unparsable message must be committed as dead-letter evidence, commits=%d", len(reader.commits))
	}
}

// T1: a permanent error (outbox.DeliveryError{Permanent}) is dead-lettered on
// the first attempt with error_code=permanent_error and the partition offset
// advances past the poison message.
func TestConsumerDeadLettersPermanentErrorAndAdvancesPartition(t *testing.T) {
	reader := &fakeReader{messages: []kafka.Message{validMessage("evt-1", 1), validMessage("evt-2", 2)}}
	ingested := 0
	runConsumer(t, reader, func(_ context.Context, event domain.Event) error {
		ingested++
		if event.EventID == "evt-1" {
			return &outbox.DeliveryError{Permanent: true, Err: errors.New("audit api returned 422 Unprocessable Entity")}
		}
		return nil
	}, WithDLQ(reader), WithMaxAttempts(3))
	if ingested != 2 {
		t.Fatalf("ingested=%d, want 2 (poison message dead-lettered on first attempt)", ingested)
	}
	if reader.committedOffset != 2 {
		t.Fatalf("committedOffset=%d, want 2 (partition must advance past the poison message)", reader.committedOffset)
	}
	if len(reader.published) != 1 {
		t.Fatalf("published=%d, want 1", len(reader.published))
	}
	failure := reader.published[0]
	if failure.EventID != "evt-1" || failure.ErrorCode != ErrorCodePermanentError || failure.ErrorMessage == "" {
		t.Fatalf("published failure=%+v, want event_id=evt-1 code=%s with message", failure, ErrorCodePermanentError)
	}
}

// T2: a transient failure is retried per (partition, offset) and dead-lettered
// with error_code=attempts_exhausted after exactly maxAttempts ingest calls.
func TestConsumerDeadLettersAfterMaxAttempts(t *testing.T) {
	reader := &fakeReader{messages: []kafka.Message{validMessage("evt-1", 1)}}
	ingested := 0
	runConsumer(t, reader, func(_ context.Context, _ domain.Event) error {
		ingested++
		return errors.New("api unavailable")
	}, WithDLQ(reader), WithMaxAttempts(3))
	if ingested != 3 {
		t.Fatalf("ingested=%d, want exactly 3 (cap before dead-letter)", ingested)
	}
	if len(reader.commits) != 1 {
		t.Fatalf("commits=%d, want 1 (dead-lettered message committed)", len(reader.commits))
	}
	if len(reader.published) != 1 {
		t.Fatalf("published=%d, want 1", len(reader.published))
	}
	if reader.published[0].ErrorCode != ErrorCodeAttemptsExhausted {
		t.Fatalf("error_code=%s, want %s", reader.published[0].ErrorCode, ErrorCodeAttemptsExhausted)
	}
}

// T3: the Failure payload serializes to exactly the AsyncAPI key set
// (event_id, error_code, error_message) with the pinned error-code values.
func TestFailurePayloadMatchesAsyncAPISchema(t *testing.T) {
	failure := Failure{EventID: "evt-1", ErrorCode: ErrorCodePermanentError, ErrorMessage: "audit api returned 422"}
	encoded, err := json.Marshal(failure)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// api/asyncapi/asyncapi.yaml components.messages.Failure requires exactly
	// these three fields; the exact string pins the key set and order.
	const want = `{"event_id":"evt-1","error_code":"permanent_error","error_message":"audit api returned 422"}`
	if string(encoded) != want {
		t.Fatalf("payload=%s, want %s", encoded, want)
	}
	var keys map[string]any
	if err := json.Unmarshal(encoded, &keys); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	expected := map[string]bool{"event_id": true, "error_code": true, "error_message": true}
	if len(keys) != len(expected) {
		t.Fatalf("key set=%v, want exactly %v", keys, expected)
	}
	for key := range expected {
		if _, ok := keys[key]; !ok {
			t.Fatalf("missing key %q in %v", key, keys)
		}
	}
	if ErrorCodePermanentError != "permanent_error" || ErrorCodeAttemptsExhausted != "attempts_exhausted" {
		t.Fatalf("error-code vocabulary drift: %q / %q", ErrorCodePermanentError, ErrorCodeAttemptsExhausted)
	}
}

// T5: a DLQ publish failure degrades to commit + log; partition progress is
// never blocked on the DLQ.
func TestConsumerDegradesToCommitWhenDLQPublishFails(t *testing.T) {
	reader := &fakeReader{
		messages: []kafka.Message{validMessage("evt-1", 1), validMessage("evt-2", 2)},
		publishFunc: func(Failure) error {
			return errors.New("dlq broker unavailable")
		},
	}
	ingested := 0
	runConsumer(t, reader, func(_ context.Context, event domain.Event) error {
		ingested++
		if event.EventID == "evt-1" {
			return &outbox.DeliveryError{Permanent: true, Err: errors.New("audit api returned 400 Bad Request")}
		}
		return nil
	}, WithDLQ(reader))
	if reader.publishCalls != 1 {
		t.Fatalf("publishCalls=%d, want 1 (publish attempted once)", reader.publishCalls)
	}
	if len(reader.published) != 0 {
		t.Fatalf("published=%d, want 0 (publish failed)", len(reader.published))
	}
	if reader.committedOffset != 2 {
		t.Fatalf("committedOffset=%d, want 2 (DLQ failure must not block partition progress)", reader.committedOffset)
	}
}

// T6: a transient failure followed by a success commits once and never
// dead-letters, even with a DLQ publisher attached.
func TestConsumerRetriesTransientThenSucceeds(t *testing.T) {
	reader := &fakeReader{messages: []kafka.Message{validMessage("evt-1", 1)}}
	ingested := 0
	runConsumer(t, reader, func(_ context.Context, _ domain.Event) error {
		ingested++
		if ingested == 1 {
			return errors.New("api unavailable")
		}
		return nil
	}, WithDLQ(reader), WithMaxAttempts(3))
	if ingested != 2 {
		t.Fatalf("ingested=%d, want 2 (one failure then retry)", ingested)
	}
	if len(reader.commits) != 1 {
		t.Fatalf("commits=%d, want 1 (only the successful attempt)", len(reader.commits))
	}
	if len(reader.published) != 0 {
		t.Fatalf("published=%d, want 0 (message recovered, no dead-letter)", len(reader.published))
	}
}

// T7: without WithMaxAttempts the transient cap defaults to 8.
func TestConsumerDefaultMaxAttempts(t *testing.T) {
	reader := &fakeReader{messages: []kafka.Message{validMessage("evt-1", 1)}}
	ingested := 0
	runConsumer(t, reader, func(_ context.Context, _ domain.Event) error {
		ingested++
		return errors.New("api unavailable")
	}, WithDLQ(reader))
	if ingested != 8 {
		t.Fatalf("ingested=%d, want default cap of 8", ingested)
	}
	if len(reader.published) != 1 || reader.published[0].ErrorCode != ErrorCodeAttemptsExhausted {
		t.Fatalf("want one attempts_exhausted dead-letter, published=%v", reader.published)
	}
}

// T8: with no publisher attached (audit-projector style), dead-lettering
// degrades to commit + log and the partition still advances.
func TestConsumerDeadLettersWithoutPublisherCommitsAndLogs(t *testing.T) {
	reader := &fakeReader{messages: []kafka.Message{validMessage("evt-1", 1), validMessage("evt-2", 2)}}
	ingested := 0
	runConsumer(t, reader, func(_ context.Context, event domain.Event) error {
		ingested++
		if event.EventID == "evt-1" {
			return &outbox.DeliveryError{Permanent: true, Err: errors.New("audit api returned 422 Unprocessable Entity")}
		}
		return nil
	}, WithMaxAttempts(3))
	if ingested != 2 || reader.committedOffset != 2 {
		t.Fatalf("ingested=%d committedOffset=%d, want 2/2 (commit+log degradation)", ingested, reader.committedOffset)
	}
	if reader.publishCalls != 0 || len(reader.published) != 0 {
		t.Fatalf("no publisher attached, want zero publish activity, calls=%d published=%v", reader.publishCalls, reader.published)
	}
}
