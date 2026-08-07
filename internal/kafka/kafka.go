// Package kafka implements the transport layer promised by the architecture
// plan: the ingest path writes normalized events to audit.events.accepted.v1
// and consumers rely on event_id idempotency (at-least-once, never
// exactly-once over the network). Topics follow the AsyncAPI contract.
package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/outbox"
)

// TopicAccepted is the AsyncAPI topic for validated, normalized events.
const TopicAccepted = "audit.events.accepted.v1"

// TopicDLQ is the AsyncAPI topic for events whose retries were exhausted or
// that failed permanently (api/asyncapi/asyncapi.yaml channel dlq).
const TopicDLQ = "audit.events.dlq.v1"

// Dead-letter error codes carried in Failure.ErrorCode, matching the
// AsyncAPI Failure payload vocabulary.
const (
	// ErrorCodePermanentError marks a client-rejected event (4xx except 429)
	// that can never succeed on retry.
	ErrorCodePermanentError = "permanent_error"
	// ErrorCodeAttemptsExhausted marks a transiently failing event that hit
	// the per-message attempt cap.
	ErrorCodeAttemptsExhausted = "attempts_exhausted"
)

// Failure is the dead-letter payload declared by the AsyncAPI contract
// (api/asyncapi/asyncapi.yaml, components.messages.Failure). The JSON key set
// is exactly the three required fields.
type Failure struct {
	EventID      string `json:"event_id"`
	ErrorCode    string `json:"error_code"`
	ErrorMessage string `json:"error_message"`
}

// FailurePublisher writes dead-letter Failure records to the DLQ topic.
// *Producer implements it; the consumer degrades to commit + log when no
// publisher is attached or publishing fails.
type FailurePublisher interface {
	PublishFailure(ctx context.Context, failure Failure) error
}

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

// PublishFailure serializes a dead-letter Failure and writes it to the
// producer's topic. The message key is the failing event ID so downstream
// replay can route by event.
func (p *Producer) PublishFailure(ctx context.Context, failure Failure) error {
	encoded, err := json.Marshal(failure)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(failure.EventID), Value: encoded})
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

// defaultMaxAttempts caps transient retries per message when no option is
// given, mirroring outbox.Relay.maxAttempts.
const defaultMaxAttempts = 8

// Consumer reads the accepted topic with manual offset commits. The kafka-go
// reader advances its fetch position past every message FetchMessage returns,
// committed or not, so a message that fails ingest can never be re-fetched
// within the session. Run therefore holds each message in-process and calls
// FetchMessage again only after the message is resolved. A message is
// committed only after ingest succeeds; transient failures back off and
// retry the held message up to the per-(partition, offset) attempt cap, then
// dead-letter it. Permanent failures (outbox.DeliveryError with Permanent
// set) are dead-lettered immediately. Unparsable messages are committed and
// logged as dead-letter evidence. Dead-lettering publishes a Failure record
// to the DLQ topic (when one is attached) and always commits, so partition
// progress is never blocked by a slow or unavailable DLQ.
type Consumer struct {
	reader      messageReader
	ingest      IngestFunc
	backoff     time.Duration
	logger      *log.Logger
	maxAttempts int
	dlq         FailurePublisher
	attempts    map[messageKey]int
}

// messageKey identifies one in-flight message for attempt accounting.
type messageKey struct {
	partition int
	offset    int64
}

// ConsumerOption tunes a Consumer created by NewConsumer. Options are
// additive; unset options keep the defaults.
type ConsumerOption func(*Consumer)

// WithMaxAttempts caps transient retries per (partition, offset) before the
// message is dead-lettered. Non-positive values restore the default of 8.
func WithMaxAttempts(maxAttempts int) ConsumerOption {
	return func(c *Consumer) {
		if maxAttempts > 0 {
			c.maxAttempts = maxAttempts
		}
	}
}

// WithDLQ attaches the dead-letter publisher. Without one (or when
// publishing fails) dead-lettering degrades to commit + log.
func WithDLQ(publisher FailurePublisher) ConsumerOption {
	return func(c *Consumer) {
		c.dlq = publisher
	}
}

// NewConsumer creates a consumer group reader with manual commit.
func NewConsumer(brokers []string, topic, groupID string, ingest IngestFunc, backoff time.Duration, logger *log.Logger, options ...ConsumerOption) *Consumer {
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
	consumer := &Consumer{reader: reader, ingest: ingest, backoff: backoff, logger: logger, attempts: make(map[messageKey]int)}
	for _, option := range options {
		option(consumer)
	}
	return consumer
}

func (c *Consumer) logf(format string, args ...any) {
	if c.logger != nil {
		c.logger.Printf(format, args...)
	}
}

// newConsumerWithReader is the test seam for the backpressure/dead-letter
// state machine.
func newConsumerWithReader(reader messageReader, ingest IngestFunc, backoff time.Duration, options ...ConsumerOption) *Consumer {
	if backoff <= 0 {
		backoff = time.Millisecond
	}
	consumer := &Consumer{reader: reader, ingest: ingest, backoff: backoff, logger: log.New(io.Discard, "", 0), attempts: make(map[messageKey]int)}
	for _, option := range options {
		option(consumer)
	}
	return consumer
}

func (c *Consumer) attemptCap() int {
	if c.maxAttempts <= 0 {
		return defaultMaxAttempts
	}
	return c.maxAttempts
}

// Run consumes until ctx is cancelled or a fatal reader error occurs.
// FetchMessage is called once per message: a message is held in-process and
// resolved (committed or dead-lettered) before the next fetch, because the
// kafka-go reader never returns a fetched-but-uncommitted message again.
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
		if err := c.consume(ctx, message, event); err != nil {
			return err
		}
	}
}

// consume resolves one fetched message in place. Ingest is retried on the
// held message — FetchMessage is not called again, because the reader has
// already advanced past it — until the ingest succeeds (commit), the error
// is permanent (immediate dead-letter), or the per-message attempt cap is
// reached (attempts-exhausted dead-letter). The attempt-map entry is removed
// on every resolution path, bounding the map by in-flight messages.
func (c *Consumer) consume(ctx context.Context, message kafka.Message, event domain.Event) error {
	key := messageKey{partition: message.Partition, offset: message.Offset}
	defer delete(c.attempts, key)
	for {
		err := c.ingest(ctx, event)
		if err == nil {
			if commitErr := c.reader.CommitMessages(ctx, message); commitErr != nil {
				return commitErr
			}
			c.logf("ingested topic=%s offset=%d event_id=%s", c.reader.Config().Topic, message.Offset, event.EventID)
			return nil
		}
		// 永久错误（4xx 语义拒绝）没有重试价值：立即死信。
		var deliveryErr *outbox.DeliveryError
		if errors.As(err, &deliveryErr) && deliveryErr.Permanent {
			return c.deadLetter(ctx, message, event, ErrorCodePermanentError, err)
		}
		// 瞬态错误：按 (partition, offset) 计数，达到上限后死信；否则
		// 退避并重试同一条消息（背压；API 按 event_id 幂等，重试不会
		// 产生重复事实）。
		c.attempts[key]++
		if c.attempts[key] >= c.attemptCap() {
			return c.deadLetter(ctx, message, event, ErrorCodeAttemptsExhausted, err)
		}
		c.logf("ingest failed topic=%s offset=%d event_id=%s error=%v (backoff=%s attempt=%d/%d)", c.reader.Config().Topic, message.Offset, event.EventID, err, c.backoff, c.attempts[key], c.attemptCap())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.backoff):
		}
	}
}

// deadLetter publishes the Failure to the DLQ (when one is attached), then
// commits the message so the partition advances. A publish failure degrades
// to commit + log: the DLQ must never block ledger progress.
func (c *Consumer) deadLetter(ctx context.Context, message kafka.Message, event domain.Event, code string, cause error) error {
	failure := Failure{EventID: event.EventID, ErrorCode: code, ErrorMessage: cause.Error()}
	if c.dlq != nil {
		if err := c.dlq.PublishFailure(ctx, failure); err != nil {
			c.logf("dlq publish failed topic=%s partition=%d offset=%d event_id=%s code=%s error=%v (degrading to commit+log)", c.reader.Config().Topic, message.Partition, message.Offset, event.EventID, code, err)
		}
	}
	c.logf("dead-lettered topic=%s partition=%d offset=%d event_id=%s code=%s error=%v", c.reader.Config().Topic, message.Partition, message.Offset, event.EventID, code, cause)
	return c.reader.CommitMessages(ctx, message)
}

func (c *Consumer) Close() error { return c.reader.Close() }
