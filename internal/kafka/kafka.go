// Package kafka implements the transport layer promised by the architecture
// plan: the ingest path writes normalized events to audit.events.accepted.v1
// and consumers rely on event_id idempotency (at-least-once, never
// exactly-once over the network). Topics follow the AsyncAPI contract.
package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/snaplink/audit-governance/internal/domain"
)

// TopicAccepted is the AsyncAPI topic for validated, normalized events.
const TopicAccepted = "audit.events.accepted.v1"

// Producer writes canonical events to the accepted topic. It implements
// outbox.DeliverFunc so the relay can choose Kafka instead of HTTP delivery
// without changing its retry/dead-letter state machine.
type Producer struct {
	writer *kafka.Writer
	topic  string
}

// NewProducer creates a synchronous acks=all producer. Events are partitioned
// by event_id (hash balancer), which keeps one event's retries ordered.
func NewProducer(brokers []string, topic string) *Producer {
	if topic == "" {
		topic = TopicAccepted
	}
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	return &Producer{writer: writer, topic: topic}
}

// Deliver serializes the event to canonical JSON and writes it to Kafka.
func (p *Producer) Deliver(ctx context.Context, event domain.Event) error {
	encoded, err := domain.CanonicalJSON(event)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(event.EventID), Value: encoded})
}

func (p *Producer) Close() error { return p.writer.Close() }

// IngestFunc delivers one canonical event into the ledger path. The audit
// API is idempotent per event_id, so redelivery after a crash is safe.
type IngestFunc func(ctx context.Context, event domain.Event) error

// messageReader is the minimal Kafka surface Consumer needs; kafka.Reader
// implements it and tests use a fake to exercise the backpressure/dead-letter
// state machine without a broker.
type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Config() kafka.ReaderConfig
	Close() error
}

// Consumer reads the accepted topic with manual offset commits. A message is
// committed only after ingest succeeds; failures back off and retry the same
// message (backpressure, never silent drops), matching the degradation
// policy for an unavailable ledger path. Unparsable messages are committed
// and logged as dead-letter evidence.
type Consumer struct {
	reader  messageReader
	ingest  IngestFunc
	backoff time.Duration
	logger  *log.Logger
}

// NewConsumer creates a consumer group reader with manual commit.
func NewConsumer(brokers []string, topic, groupID string, ingest IngestFunc, backoff time.Duration, logger *log.Logger) *Consumer {
	if topic == "" {
		topic = TopicAccepted
	}
	if backoff <= 0 {
		backoff = time.Second
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       1 << 20,
		CommitInterval: 0, // manual commits only
		StartOffset:    kafka.FirstOffset,
	})
	return &Consumer{reader: reader, ingest: ingest, backoff: backoff, logger: logger}
}

func (c *Consumer) logf(format string, args ...any) {
	if c.logger != nil {
		c.logger.Printf(format, args...)
	}
}

// newConsumerWithReader is the test seam for the backpressure state machine.
func newConsumerWithReader(reader messageReader, ingest IngestFunc, backoff time.Duration) *Consumer {
	if backoff <= 0 {
		backoff = time.Millisecond
	}
	return &Consumer{reader: reader, ingest: ingest, backoff: backoff, logger: log.New(io.Discard, "", 0)}
}

// Run consumes until ctx is cancelled or a fatal reader error occurs.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		var event domain.Event
		// UseNumber: the consumer re-ingests the event, so payload numbers
		// must survive as exact json.Number values to keep the derived
		// digest identical to the producer's digest for the same event.
		decoder := json.NewDecoder(bytes.NewReader(message.Value))
		decoder.UseNumber()
		if err := decoder.Decode(&event); err != nil {
			// 不可解析的消息没有重试价值：提交并记录死信证据。
			c.logf("dead-letter topic=%s partition=%d offset=%d error=%v", c.reader.Config().Topic, message.Partition, message.Offset, err)
			if commitErr := c.reader.CommitMessages(ctx, message); commitErr != nil {
				return commitErr
			}
			continue
		}
		if err := c.ingest(ctx, event); err != nil {
			// 背压：不提交，退避后重试同一条消息。幂等消费保证
			// 重试不会产生重复事实。
			c.logf("ingest failed topic=%s offset=%d event_id=%s error=%v (backoff=%s)", c.reader.Config().Topic, message.Offset, event.EventID, err, c.backoff)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.backoff):
			}
			continue
		}
		if err := c.reader.CommitMessages(ctx, message); err != nil {
			return err
		}
		c.logf("ingested topic=%s offset=%d event_id=%s", c.reader.Config().Topic, message.Offset, event.EventID)
	}
}

func (c *Consumer) Close() error { return c.reader.Close() }
