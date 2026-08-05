package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/snaplink/audit-governance/internal/domain"
)

type fakeReader struct {
	messages        []kafka.Message
	commits         []kafka.Message
	committedOffset int64
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

func runConsumer(t *testing.T, reader *fakeReader, ingest IngestFunc) *fakeReader {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	consumer := newConsumerWithReader(reader, ingest, time.Millisecond)
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
