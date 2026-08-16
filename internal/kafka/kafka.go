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
	"net/http"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/outbox"
	"github.com/snaplink/audit-governance/internal/projection"
)

// TopicAccepted is the AsyncAPI topic for validated, normalized events.
const TopicAccepted = "audit.events.accepted.v1"

// TopicDLQ is the AsyncAPI topic for events whose retries were exhausted or
// that failed permanently (api/asyncapi/asyncapi.yaml channel dlq).
const TopicDLQ = "audit.events.dlq.v1"

// TopicLedgered is the AsyncAPI topic for events committed to the immutable
// ledger (post-ledger chain-linked events). Declared to satisfy the AsyncAPI
// channel gate (checks/asyncapi_channels.py); no producer or consumer wires
// this topic yet — see docs/proposals/ledgered-projection-pipeline.md.
const TopicLedgered = "audit.events.ledgered.v1"

// TopicProjection is the AsyncAPI topic for projection indexing signals.
// Declared to satisfy the AsyncAPI channel gate; not yet wired — see
// docs/proposals/ledgered-projection-pipeline.md.
const TopicProjection = "audit.events.projection.v1"

// TopicArchive is the AsyncAPI topic for archival-pass completion signals.
// Declared to satisfy the AsyncAPI channel gate; not yet wired — see
// docs/proposals/ledgered-projection-pipeline.md.
const TopicArchive = "audit.events.archive.v1"

// Dead-letter error codes carried in Failure.ErrorCode, matching the
// AsyncAPI Failure payload vocabulary.
const (
	// ErrorCodePermanentError marks a client-rejected event (4xx except 429)
	// that can never succeed on retry.
	ErrorCodePermanentError = "permanent_error"
	// ErrorCodeAttemptsExhausted marks a transiently failing event that hit
	// the per-message attempt cap.
	ErrorCodeAttemptsExhausted = "attempts_exhausted"
	// ErrorCodeUnparsable marks a message that could not be decoded into an
	// event at all; it is dead-lettered immediately (never retried).
	ErrorCodeUnparsable = "unparsable_message"
	// ErrorCodeUnauthorized marks a transiently-failing event whose attempts
	// were exhausted while the ingest API was rejecting with 401: a
	// credential configuration problem (empty/rotated AUDIT_OUTBOX_TOKEN, or
	// an IdP/JWKS outage), not an API outage. Replay treats it as
	// config-fixable — blocked until the credential is restored and the
	// operator re-admits it with -replay-auth-blocked. Additive to the
	// Failure vocabulary (error_code is a free string in the AsyncAPI
	// contract).
	ErrorCodeUnauthorized = "unauthorized"
)

// Failure is the dead-letter payload declared by the AsyncAPI contract
// (api/asyncapi/asyncapi.yaml, components.messages.Failure). The JSON key set
// is the three required fields plus the optional tenant_id: present on
// event-decoded failures, omitted (omitempty) on unparsable payloads and on
// records written by older binaries (both record shapes decode — replay's
// json.Decoder ignores unknown fields). TenantID is the tenant CLAIMED by the
// event envelope at dead-letter time — an untrusted topic value, never
// server-verified (service.Ingest's resolved tenant is the only
// authorization authority); consumers must use it for display/filtering
// only, never for scoping or routing without API re-enforcement.
type Failure struct {
	EventID      string `json:"event_id"`
	ErrorCode    string `json:"error_code"`
	ErrorMessage string `json:"error_message"`
	TenantID     string `json:"tenant_id,omitempty"`
}

// FailurePublisher writes dead-letter Failure records to the DLQ topic.
// *Producer implements it; the consumer degrades to commit + log when no
// publisher is attached or publishing fails.
type FailurePublisher interface {
	PublishFailure(ctx context.Context, failure Failure) error
}

// Producer writes canonical events to the accepted topic. It implements
// outbox.DeliverFunc so the relay can choose Kafka instead of HTTP delivery
// without changing its retry/dead-letter state machine. A topic write is
// delivery, but Kafka carries no audit receipt, so Deliver returns (nil, nil)
// on success and the relay records no fabricated receipt fields.
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
// On success it returns (nil, nil): the write is the delivery proof, and no
// audit receipt exists in this transport (outbox.DeliverFunc contract).
func (p *Producer) Deliver(ctx context.Context, event domain.Event) (*domain.EventReceipt, error) {
	encoded, err := domain.CanonicalJSON(event)
	if err != nil {
		return nil, err
	}
	if err := p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(event.EventID), Value: encoded}); err != nil {
		return nil, err
	}
	return nil, nil
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

// Republish writes a recovered accepted-topic message back to the producer's
// topic byte-for-byte, preserving the original canonical encoding so the
// digest chain stays intact.
func (p *Producer) Republish(ctx context.Context, key, value []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{Key: key, Value: value})
}

func (p *Producer) Close() error { return p.writer.Close() }

// IngestFunc delivers one canonical event into the ledger path. The audit
// API is idempotent per event_id, so redelivery after a crash is safe.
type IngestFunc func(ctx context.Context, event domain.Event) error

// eventIDFromValue probes a message value for a canonical event_id without
// full event validation. Shared by the consumer (REQ-3) and the replayer
// (REQ-1/REQ-2); "" means the value carries no usable event_id (not JSON,
// not an object, or blank).
func eventIDFromValue(value []byte) string {
	var probe struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(value, &probe); err != nil {
		return ""
	}
	return probe.EventID
}

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
// set) and deterministic domain rejections (domain.ErrOccurredAtOutOfRange,
// projection.ErrNotLedgered) are dead-lettered immediately. Unparsable
// messages are committed and logged as dead-letter evidence. Dead-lettering
// publishes a Failure record
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
	tenant      string // optional tenant scope; "" = no filtering (REQ-1)

	ingestFailures       atomic.Uint64
	deadLettered         atomic.Uint64
	unauthorized         atomic.Uint64 // dead-lettered with error_code=unauthorized (401-class cap exhaustion)
	dlqPublished         atomic.Uint64
	messagesCommitted    atomic.Uint64
	foreignTenantSkipped atomic.Uint64 // disjoint third class: skipped+committed, never ingested/dead-lettered
}

// messageKey identifies one in-flight message for attempt accounting.
type messageKey struct {
	partition int
	offset    int64
}

// ConsumerOption tunes a Consumer created by NewConsumer. Options are
// additive; unset options keep the defaults.
type ConsumerOption func(*Consumer)

// ConsumerMetrics is a snapshot of the consumer's domain counters for the
// /metrics endpoint and DLQ traffic alerting.
type ConsumerMetrics struct {
	IngestFailures       uint64
	DeadLettered         uint64
	Unauthorized         uint64 // dead-lettered with error_code=unauthorized (401-class cap exhaustion)
	DLQPublished         uint64
	MessagesCommitted    uint64
	ForeignTenantSkipped uint64 // skipped+committed, disjoint from ingest/commit and dead-letter
}

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

// WithTenant scopes the consumer to one tenant: messages whose event.TenantID
// differs (and is non-empty) are skipped and committed without ingest,
// dead-lettering, or retry. An empty value (the default) disables filtering —
// behavior is byte-identical to a consumer built without this option (REQ-7).
func WithTenant(tenant string) ConsumerOption {
	return func(c *Consumer) { c.tenant = tenant }
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
			if deadErr := c.deadLetterUnparsable(ctx, message, err); deadErr != nil {
				return deadErr
			}
			continue
		}
		// REQ-1: the accepted topic's envelope is exactly one JSON value per
		// message — the HTTP API enforces the same contract in decodeBody
		// (internal/httpapi/server.go). A second value or trailing
		// non-whitespace data makes the whole message malformed: the first
		// value must never be ingested alone (silent partial ingest).
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			if err == nil {
				// Second decode succeeded: a second JSON value exists.
				err = errors.New("message value must contain one JSON value")
			}
			if deadErr := c.deadLetterUnparsable(ctx, message, err); deadErr != nil {
				return deadErr
			}
			continue
		}
		if err := c.consume(ctx, message, event); err != nil {
			return err
		}
	}
}

// deadLetterUnparsable routes a message that could not be decoded into an
// event (or violated the single-value contract) through the unparsable
// dead-letter path: dead-letter evidence is recorded, the Failure is
// published to the DLQ when a publisher is attached and an event_id is
// recoverable, and the message is always committed so the partition
// advances. The message is never retried and never reaches ingest.
func (c *Consumer) deadLetterUnparsable(ctx context.Context, message kafka.Message, cause error) error {
	// 不可解析的消息没有重试价值：先记录死信证据（DLQ 有挂载时），
	// 再提交；绝不静默丢弃。event_id 优先取 payload 探针，key 仅作
	// 回退；两者皆空时只提交+记日志，避免发布空 event_id 的 Failure
	//（replay 无法路由它，只会制造噪音）。
	c.deadLettered.Add(1)
	failure := Failure{ErrorCode: ErrorCodeUnparsable, ErrorMessage: cause.Error()}
	if id := eventIDFromValue(message.Value); id != "" {
		failure.EventID = id
	} else {
		failure.EventID = string(message.Key)
	}
	if failure.EventID == "" {
		c.logf("dlq publish skipped topic=%s partition=%d offset=%d reason=no-event-id-recoverable (commit+log)", c.reader.Config().Topic, message.Partition, message.Offset)
	} else if c.dlq != nil {
		if publishErr := c.dlq.PublishFailure(ctx, failure); publishErr != nil {
			c.logf("dlq publish failed topic=%s partition=%d offset=%d event_id=%s code=%s error=%v (degrading to commit+log)", c.reader.Config().Topic, message.Partition, message.Offset, failure.EventID, ErrorCodeUnparsable, publishErr)
		} else {
			c.dlqPublished.Add(1)
		}
	}
	c.logf("dead-letter topic=%s partition=%d offset=%d event_id=%s error=%v", c.reader.Config().Topic, message.Partition, message.Offset, failure.EventID, cause)
	return c.reader.CommitMessages(ctx, message)
}

// consume resolves one fetched message in place. Ingest is retried on the
// held message — FetchMessage is not called again, because the reader has
// already advanced past it — until the ingest succeeds (commit), the error
// is permanent (immediate dead-letter: outbox.DeliveryError with Permanent
// set, or a deterministic domain rejection — domain.ErrOccurredAtOutOfRange,
// projection.ErrNotLedgered), or the per-message attempt cap is reached
// (attempts-exhausted dead-letter). The attempt-map entry is removed on
// every resolution path, bounding the map by in-flight messages.
func (c *Consumer) consume(ctx context.Context, message kafka.Message, event domain.Event) error {
	// REQ-1 tenant scope gate: a message whose envelope tenant differs from the
	// instance's configured tenant is skipped and committed — never ingested,
	// never dead-lettered, never retried. An empty event tenant is stamped
	// server-side with the token's tenant (service.Ingest, DS-08) and so belongs
	// to this instance by construction; an empty configured tenant filters
	// nothing (backward compatible, REQ-7).
	if c.tenant != "" && event.TenantID != "" && event.TenantID != c.tenant {
		c.foreignTenantSkipped.Add(1)
		c.logf("skipped foreign-tenant topic=%s partition=%d offset=%d event_id=%s tenant=%s consumer_tenant=%s",
			c.reader.Config().Topic, message.Partition, message.Offset, event.EventID, event.TenantID, c.tenant)
		return c.reader.CommitMessages(ctx, message)
	}
	key := messageKey{partition: message.Partition, offset: message.Offset}
	defer delete(c.attempts, key)
	for {
		err := c.ingest(ctx, event)
		if err == nil {
			if commitErr := c.reader.CommitMessages(ctx, message); commitErr != nil {
				return commitErr
			}
			c.messagesCommitted.Add(1)
			c.logf("ingested topic=%s offset=%d event_id=%s", c.reader.Config().Topic, message.Offset, event.EventID)
			return nil
		}
		c.ingestFailures.Add(1)
		// 永久错误（4xx 语义拒绝）没有重试价值：立即死信。
		var deliveryErr *outbox.DeliveryError
		if errors.As(err, &deliveryErr) && deliveryErr.Permanent {
			return c.deadLetter(ctx, message, event, ErrorCodePermanentError, err)
		}
		// 域边界拒绝（occurred_at 超出账本时间窗）与 4xx 语义拒绝同级：没有
		// 重试价值，立即死信，避免对这类毒消息烧掉 max-attempts 次退避。
		// 预修复前已入账的超范围事件由此获得自描述 trace（错误文本携带
		// 时间窗），而不是等到 attempts_exhausted。同一类的第二个成员是
		// projection.ErrNotLedgered（投影所需的账本链状态缺失）：错误文本
		// 自带诊断（"projection: event lacks ledger-assigned chain state"）。
		if errors.Is(err, domain.ErrOccurredAtOutOfRange) {
			return c.deadLetter(ctx, message, event, ErrorCodePermanentError, err)
		}
		// 域边界拒绝（投影所需的账本链状态缺失）与上者同级：链状态由账本在
		// ingest 提交时服务端分配，投影端任何重试都无法补上，没有重试价值，
		// 立即死信为 permanent_error（第一次尝试）——不烧 max-attempts 次退避，
		// 不产生 attempts_exhausted（重放会因此对预入账原件循环重试）。错误
		// 文本自带诊断（"projection: event lacks ledger-assigned chain state"）。
		if errors.Is(err, projection.ErrNotLedgered) {
			return c.deadLetter(ctx, message, event, ErrorCodePermanentError, err)
		}
		// 瞬态错误：按 (partition, offset) 计数，达到上限后死信；否则
		// 退避并重试同一条消息（背压；API 按 event_id 幂等，重试不会
		// 产生重复事实）。
		c.attempts[key]++
		if c.attempts[key] >= c.attemptCap() {
			// 401 类（凭证问题，可配置修复）与其它瞬态失败分开计数与编码；
			// Permanent 分支已提前返回，这里的 DeliveryError 必然是瞬态分类
			// （!Permanent 守卫为防御性自文档）。注意：若未来的 deliverer 未
			// 用 %w 包装 DeliveryError，errors.As 失败会优雅退化为
			// attempts_exhausted（改动前行为）。
			if errors.As(err, &deliveryErr) && !deliveryErr.Permanent &&
				deliveryErr.StatusCode == http.StatusUnauthorized {
				c.unauthorized.Add(1)
				return c.deadLetter(ctx, message, event, ErrorCodeUnauthorized, err)
			}
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
// to commit + log: the DLQ must never block ledger progress. A blank payload
// event_id never yields a Failure record: the payload was parsed and is
// authoritative, and an empty-ID record cannot be routed by replay — commit
// + durable log instead (the key is an untrusted producer hint).
func (c *Consumer) deadLetter(ctx context.Context, message kafka.Message, event domain.Event, code string, cause error) error {
	failure := Failure{EventID: event.EventID, ErrorCode: code, ErrorMessage: cause.Error(), TenantID: event.TenantID}
	if failure.EventID == "" {
		c.logf("dlq publish skipped topic=%s partition=%d offset=%d reason=empty-event-id code=%s (commit+log)", c.reader.Config().Topic, message.Partition, message.Offset, code)
	} else if c.dlq != nil {
		if err := c.dlq.PublishFailure(ctx, failure); err != nil {
			c.logf("dlq publish failed topic=%s partition=%d offset=%d event_id=%s code=%s error=%v (degrading to commit+log)", c.reader.Config().Topic, message.Partition, message.Offset, event.EventID, code, err)
		} else {
			c.dlqPublished.Add(1)
		}
	}
	c.deadLettered.Add(1)
	c.logf("dead-lettered topic=%s partition=%d offset=%d event_id=%s code=%s error=%v", c.reader.Config().Topic, message.Partition, message.Offset, event.EventID, code, cause)
	return c.reader.CommitMessages(ctx, message)
}

func (c *Consumer) Close() error { return c.reader.Close() }

// Metrics returns a snapshot of the consumer's counters for /metrics.
func (c *Consumer) Metrics() ConsumerMetrics {
	return ConsumerMetrics{
		IngestFailures:       c.ingestFailures.Load(),
		DeadLettered:         c.deadLettered.Load(),
		Unauthorized:         c.unauthorized.Load(),
		DLQPublished:         c.dlqPublished.Load(),
		MessagesCommitted:    c.messagesCommitted.Load(),
		ForeignTenantSkipped: c.foreignTenantSkipped.Load(),
	}
}
