package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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
	fetchIndex      int
	publishCalls    int
	published       []Failure
	publishFunc     func(Failure) error
}

// FetchMessage 模拟真实 kafka-go reader 的语义（reader.go:846）：fetch 位置
// 在每次返回消息后单调前进（r.offset = msg.Offset + 1），与是否提交无关；
// 因此已取出但未提交的消息在本次会话内不会再次返回。消费端必须先把手中
// 的消息解析完毕（提交或死信）才能取到下一条。队列耗尽后阻塞到上下文取消。
func (f *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if f.fetchIndex < len(f.messages) {
		message := f.messages[f.fetchIndex]
		f.fetchIndex++
		return message, nil
	}
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (f *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	for _, message := range msgs {
		// kafka-go 提交的是 msg.Offset+1（下一条待消费位置，commit.go:17）；
		// fake 记录已提交的最高消息 offset 供断言使用。
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
	}, WithDLQ(reader))
	if ingested != 0 {
		t.Fatalf("unparsable message reached ingest %d times", ingested)
	}
	if len(reader.commits) != 1 {
		t.Fatalf("unparsable message must be committed as dead-letter evidence, commits=%d", len(reader.commits))
	}
	if len(reader.published) != 1 {
		t.Fatalf("unparsable message must publish a DLQ Failure record, published=%d", len(reader.published))
	}
	if reader.published[0].ErrorCode != ErrorCodeUnparsable {
		t.Fatalf("DLQ record code=%s, want %s", reader.published[0].ErrorCode, ErrorCodeUnparsable)
	}
	if reader.published[0].EventID != "bad" {
		t.Fatalf("DLQ record event_id=%s, want bad", reader.published[0].EventID)
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

// F-1 回归：真实 kafka-go reader 的 fetch 位置在每次 FetchMessage 后都会
// 越过该消息（无论是否提交）。瞬态失败后若重新 FetchMessage，会取到下一
// 条消息；下一条成功提交后，失败消息被静默跳过且无任何死信证据。消费端
// 必须原地重试手中的消息。此测试在“失败后重新拉取”的循环上失败（evt-1
// 只会被 ingest 一次、分区在无证据的情况下越过它），修复后通过。
func TestConsumerRetriesFailedMessageInPlaceWithoutRefetch(t *testing.T) {
	reader := &fakeReader{messages: []kafka.Message{validMessage("evt-1", 1), validMessage("evt-2", 2)}}
	attempts := map[string]int{}
	var order []string
	runConsumer(t, reader, func(_ context.Context, event domain.Event) error {
		attempts[event.EventID]++
		order = append(order, event.EventID)
		if event.EventID == "evt-1" && attempts["evt-1"] == 1 {
			return errors.New("api unavailable")
		}
		return nil
	}, WithDLQ(reader), WithMaxAttempts(3))
	// evt-1 必须被原地重试（两次 ingest），而不是在失败后重新拉取而跳过。
	if attempts["evt-1"] != 2 {
		t.Fatalf("evt-1 ingested %d times, want 2 (failed message must be retried in place, not skipped by re-fetch)", attempts["evt-1"])
	}
	// evt-2 只能在 evt-1 解析（提交）之后被 ingest：顺序必须是
	// evt-1（失败）→ evt-1（重试成功）→ evt-2。
	if len(order) != 3 || order[0] != "evt-1" || order[1] != "evt-1" || order[2] != "evt-2" {
		t.Fatalf("ingest order=%v, want [evt-1 evt-1 evt-2]", order)
	}
	if len(reader.commits) != 2 || reader.committedOffset != 2 {
		t.Fatalf("commits=%d committedOffset=%d, want 2/2 (both messages committed once, in order)", len(reader.commits), reader.committedOffset)
	}
	if len(reader.published) != 0 {
		t.Fatalf("published=%v, want no DLQ evidence (message recovered on retry)", reader.published)
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

// AC-1 (REQ-1): a message value containing two JSON objects is a malformed
// envelope — the whole message is dead-lettered as unparsable, the first
// value is never ingested alone (silent partial ingest), and the message is
// committed as dead-letter evidence.
func TestConsumerRejectsMultiValueMessageAsUnparsable(t *testing.T) {
	value := []byte(`{"event_id":"evt-1","tenant_id":"demo","source_system":"demo","action":"update","outcome":"success"}` +
		`{"event_id":"evt-2","tenant_id":"demo","source_system":"demo","action":"update","outcome":"success"}`)
	reader := &fakeReader{messages: []kafka.Message{{Key: []byte("evt-1"), Value: value, Partition: 0, Offset: 9}}}
	ingested := 0
	runConsumer(t, reader, func(_ context.Context, _ domain.Event) error {
		ingested++
		return nil
	}, WithDLQ(reader))
	if ingested != 0 {
		t.Fatalf("two-value message reached ingest %d times, want 0", ingested)
	}
	if len(reader.commits) != 1 || reader.committedOffset != 9 {
		t.Fatalf("commits=%d committedOffset=%d, want 1 commit at offset 9 (dead-letter evidence)", len(reader.commits), reader.committedOffset)
	}
	if len(reader.published) != 1 || reader.published[0].ErrorCode != ErrorCodeUnparsable {
		t.Fatalf("published=%v, want one unparsable_message Failure", reader.published)
	}
	failure := reader.published[0]
	if failure.EventID != "evt-1" {
		t.Fatalf("event_id=%q, want evt-1 (key fallback)", failure.EventID)
	}
	if failure.ErrorMessage == "" {
		t.Fatal("ErrorMessage must be non-empty (synthesized single-value error)")
	}
}

// AC-2 (REQ-1/REQ-2): valid JSON followed by non-whitespace garbage is also
// a malformed envelope — dead-lettered whole, never ingested, with the second
// decode's parse error surfaced in ErrorMessage so operators see why.
func TestConsumerRejectsTrailingGarbageAsUnparsable(t *testing.T) {
	value := []byte(`{"event_id":"evt-1","tenant_id":"demo","source_system":"demo","action":"update","outcome":"success"} garbage`)
	reader := &fakeReader{messages: []kafka.Message{{Key: []byte("evt-1"), Value: value, Partition: 0, Offset: 10}}}
	ingested := 0
	runConsumer(t, reader, func(_ context.Context, _ domain.Event) error {
		ingested++
		return nil
	}, WithDLQ(reader))
	if ingested != 0 {
		t.Fatalf("trailing-garbage message reached ingest %d times, want 0", ingested)
	}
	if len(reader.commits) != 1 || reader.committedOffset != 10 {
		t.Fatalf("commits=%d committedOffset=%d, want 1/10", len(reader.commits), reader.committedOffset)
	}
	if len(reader.published) != 1 || reader.published[0].ErrorCode != ErrorCodeUnparsable {
		t.Fatalf("published=%v, want one unparsable_message Failure", reader.published)
	}
	if reader.published[0].EventID != "evt-1" {
		t.Fatalf("event_id=%q, want evt-1", reader.published[0].EventID)
	}
	if !strings.Contains(reader.published[0].ErrorMessage, "invalid character 'g'") {
		t.Fatalf("ErrorMessage=%q, want trailing-data parse error text", reader.published[0].ErrorMessage)
	}
}

// AC-4 (REQ-4 positive control): a single value followed by trailing
// whitespace stays legal — the second decode hits io.EOF and the message
// takes the normal ingest -> commit path (boundary between legal whitespace
// and illegal trailing data).
func TestConsumerAllowsTrailingWhitespaceAfterSingleValue(t *testing.T) {
	message := validMessage("evt-1", 11)
	message.Value = append(message.Value, '\n')
	reader := &fakeReader{messages: []kafka.Message{message}}
	ingested := 0
	runConsumer(t, reader, func(_ context.Context, _ domain.Event) error {
		ingested++
		return nil
	}, WithDLQ(reader))
	if ingested != 1 {
		t.Fatalf("ingested=%d, want 1 (trailing whitespace is legal)", ingested)
	}
	if len(reader.commits) != 1 || reader.committedOffset != 11 {
		t.Fatalf("commits=%d committedOffset=%d, want 1/11", len(reader.commits), reader.committedOffset)
	}
	if len(reader.published) != 0 {
		t.Fatalf("published=%v, want no dead-letter for a legal message", reader.published)
	}
}

// AC-3 (REQ-3/REQ-4): the consumer never publishes a DLQ Failure with an
// empty event_id. On the unparsable path the payload event_id wins over the
// key; when neither yields an ID the message degrades to commit + log. A
// blank payload event_id on the dead-letter path (decoded event) also
// degrades to commit + log instead of publishing an empty-ID record.
func TestConsumerNeverPublishesEmptyEventID(t *testing.T) {
	t.Run("unparsable with empty key skips publish", func(t *testing.T) {
		reader := &fakeReader{messages: []kafka.Message{{Key: nil, Value: []byte("not-json"), Partition: 0, Offset: 1}}}
		runConsumer(t, reader, func(_ context.Context, _ domain.Event) error {
			t.Fatal("unparsable message must not reach ingest")
			return nil
		}, WithDLQ(reader))
		if reader.publishCalls != 0 || len(reader.published) != 0 {
			t.Fatalf("publishCalls=%d published=%v, want 0/none (no empty event_id Failure)", reader.publishCalls, reader.published)
		}
		if len(reader.commits) != 1 {
			t.Fatalf("commits=%d, want 1 (commit + log degradation)", len(reader.commits))
		}
	})
	t.Run("payload event_id wins over empty key on unparsable", func(t *testing.T) {
		// schema_version "bad" is not an int, so the consumer's event decode
		// fails — but the event_id probe succeeds and must be used instead of
		// the empty key.
		reader := &fakeReader{messages: []kafka.Message{{Key: nil, Value: []byte(`{"event_id":"evt-v","schema_version":"bad"}`), Partition: 0, Offset: 1}}}
		runConsumer(t, reader, func(_ context.Context, _ domain.Event) error {
			t.Fatal("unparsable message must not reach ingest")
			return nil
		}, WithDLQ(reader))
		if reader.publishCalls != 1 || len(reader.published) != 1 {
			t.Fatalf("publishCalls=%d published=%v, want 1 record", reader.publishCalls, reader.published)
		}
		if reader.published[0].EventID != "evt-v" {
			t.Fatalf("published event_id=%q, want evt-v (payload probe, not the empty key)", reader.published[0].EventID)
		}
		if len(reader.commits) != 1 {
			t.Fatalf("commits=%d, want 1", len(reader.commits))
		}
	})
	t.Run("blank payload event_id on permanent dead-letter skips publish", func(t *testing.T) {
		reader := &fakeReader{messages: []kafka.Message{validMessage("", 1)}}
		runConsumer(t, reader, func(_ context.Context, _ domain.Event) error {
			return &outbox.DeliveryError{Permanent: true, Err: errors.New("audit api rejected")}
		}, WithDLQ(reader))
		if reader.publishCalls != 0 || len(reader.published) != 0 {
			t.Fatalf("publishCalls=%d published=%v, want 0/none (blank event_id must not be published)", reader.publishCalls, reader.published)
		}
		if len(reader.commits) != 1 {
			t.Fatalf("commits=%d, want 1 (commit + log degradation)", len(reader.commits))
		}
	})
}
