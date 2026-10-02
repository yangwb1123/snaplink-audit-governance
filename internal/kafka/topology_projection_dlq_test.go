package kafka

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/snaplink/audit-governance/internal/domain"
)

// The projection pipeline and the ingest pipeline used to share one
// dead-letter topic whose only recovery tool resolves originals from the
// ACCEPTED topic, while a projection failure's original lives on the
// LEDGERED topic. The tests below pin the separation: a projection failure
// is published to TopicProjectionDLQ, and the ingest replayer (built over
// TopicDLQ) can neither observe nor close it.

// topicRecordingDLQ is the FailurePublisher stand-in for
// kafka.NewProducer(brokers, topic): it records the topic it was built for,
// which is what proves the publisher is bound to the projection DLQ.
type topicRecordingDLQ struct {
	topic     string
	published []Failure
}

func (p *topicRecordingDLQ) PublishFailure(_ context.Context, failure Failure) error {
	p.published = append(p.published, failure)
	return nil
}

// ledgeredProjectionMessage builds the exact byte encoding the ledgered
// channel carries, so the consumer reaches the ingest func (a transient
// projection error) instead of the ledgered input-schema guard.
func ledgeredProjectionMessage(t *testing.T, eventID string, offset int64) kafka.Message {
	t.Helper()
	event := contractEvent()
	event.EventID = eventID
	event.StreamID = "tenant-a:source:crm"
	event.Sequence = 1
	event.Hash = "hash-1"
	value, err := encodeEventForSchema(LedgeredEventSchema, event)
	if err != nil {
		t.Fatalf("encode ledgered event: %v", err)
	}
	return kafka.Message{Key: []byte(eventID), Value: value, Partition: 0, Offset: offset}
}

// TestProjectionFailureDeadLettersToProjectionDLQ is the producer half: a
// ledgered event whose projection fails transiently is dead-lettered, and the
// publisher it was wired with is the PROJECTION DLQ — not the ingest DLQ the
// replay tool drains.
func TestProjectionFailureDeadLettersToProjectionDLQ(t *testing.T) {
	const eventID = "evt-projection-dlq"
	publisher := &topicRecordingDLQ{topic: TopicProjectionDLQ}
	reader := &fakeReader{messages: []kafka.Message{ledgeredProjectionMessage(t, eventID, 1)}}
	attempts := 0
	consumer := runConsumerWithMetrics(t, reader, func(context.Context, domain.Event) error {
		attempts++
		return errors.New("clickhouse: dial tcp 127.0.0.1:9000: connect: connection refused")
	}, WithInputSchema(LedgeredEventSchema), WithDLQ(publisher), WithMaxAttempts(1))

	if publisher.topic != TopicProjectionDLQ {
		t.Fatalf("DLQ publisher topic = %q, want %q", publisher.topic, TopicProjectionDLQ)
	}
	if len(publisher.published) != 1 {
		t.Fatalf("published=%d records, want exactly 1", len(publisher.published))
	}
	published := publisher.published[0]
	if published.EventID != eventID {
		t.Fatalf("published event_id=%q, want %q", published.EventID, eventID)
	}
	if published.ErrorCode != ErrorCodeAttemptsExhausted {
		t.Fatalf("published error_code=%q, want %q (transient projection failure at the attempt cap)", published.ErrorCode, ErrorCodeAttemptsExhausted)
	}
	if metrics := consumer.Metrics(); metrics.DeadLettered != 1 || metrics.DLQPublished != 1 || metrics.DLQPublishFailures != 0 {
		t.Fatalf("metrics=%+v, want one dead letter published with no publish failure", metrics)
	}
	// Publish-before-commit is the no-drop invariant: the ledgered offset is
	// committed only after the Failure record reached the projection DLQ.
	if len(reader.commits) != 1 || reader.commits[0].Offset != 1 {
		t.Fatalf("commits=%v, want the single ledgered message committed after publication", reader.commits)
	}
}

// TestIngestReplayerCannotReachProjectionFailure is the consumer half: an
// ingest replayer over the ingest DLQ sees no counter advance and no durable
// mark for a projection failure, because the record lives on a topic this
// replayer never reads.
func TestIngestReplayerCannotReachProjectionFailure(t *testing.T) {
	const eventID = "evt-projection-unreachable"
	state, err := LoadReplayState(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	// The ingest DLQ is EMPTY: the projection failure was published to the
	// projection DLQ, so nothing landed where the replayer drains.
	ingestBroker := &brokerDLQ{topic: TopicDLQ}
	replayer := newTenantAwareReplayerWithFactories(
		ingestBroker.freshDLQSession(),
		freshAcceptedReader([]kafka.Message{acceptedMessage("other")}, nil),
		state,
		func(context.Context, []byte, []byte) error {
			t.Error("republish must never be called for a projection failure")
			return nil
		},
		nil,
	)
	replayer.drainTimeout = 10 * time.Millisecond
	defer replayer.Close()

	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if count != 0 {
		t.Fatalf("round replayed=%d, want 0", count)
	}
	if got := replayer.Metrics(); got != (ReplayerMetrics{}) {
		t.Fatalf("metrics=%+v, want every counter zero: no replayer counter may advance for a projection failure", got)
	}
	// The replayer is bound to the ingest DLQ, and the projection DLQ is not
	// an address it can be pointed at (pinned by ValidateReplayTopology).
	if topic := replayer.dlqReader.Config().Topic; topic != TopicDLQ || topic == TopicProjectionDLQ {
		t.Fatalf("replayer DLQ topic = %q, want %q", topic, TopicDLQ)
	}
	// The unresolvable-closure path was never entered: no durable mark and
	// no DLQ commit for the projection failure's event id.
	if state.marked(eventID) {
		t.Fatal("projection failure must not be marked in the replay state")
	}
	if len(ingestBroker.commits) != 0 {
		t.Fatalf("ingest DLQ commits=%v, want none", ingestBroker.commits)
	}
}

// TestIngestReplayerWouldCloseProjectionFailureOnTheIngestDLQ is the control
// for the test above: the same Failure record, had it been published to the
// ingest DLQ, is closed as a permanent unresolvable loss. It keeps
// TestIngestReplayerCannotReachProjectionFailure from passing vacuously and
// documents the defect the dedicated topic removes — accepted-original replay
// can never re-drive a projection, so the record is closed, not retried.
func TestIngestReplayerWouldCloseProjectionFailureOnTheIngestDLQ(t *testing.T) {
	const eventID = "evt-projection-on-ingest-dlq"
	state, err := LoadReplayState(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{
		tenantFailureMessage(eventID, "tenant-a", ErrorCodeAttemptsExhausted, 0),
	}}
	replayer := newTenantAwareReplayerWithFactories(
		broker.freshDLQSession(),
		freshAcceptedReader([]kafka.Message{acceptedMessage("other")}, nil),
		state,
		func(context.Context, []byte, []byte) error {
			t.Error("republish must never be called for an unresolvable record")
			return nil
		},
		nil,
	)
	replayer.drainTimeout = 10 * time.Millisecond
	defer replayer.Close()

	if _, err := replayer.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	metrics := replayer.Metrics()
	if metrics.Unresolvable != 1 || metrics.Replayed != 0 {
		t.Fatalf("metrics=%+v, want the record closed as unresolvable (not replayed)", metrics)
	}
	if !state.marked(eventID) {
		t.Fatal("control: the unresolvable path must mark the event id")
	}
	if len(broker.commits) != 1 {
		t.Fatalf("ingest DLQ commits=%d, want 1 (offset closed, never retried)", len(broker.commits))
	}
}

// TestProjectionDLQTopologyInvariants pins the address separation and the
// directional guards as one table: the projection DLQ is its own address, both
// pipelines refuse to borrow the other's, and the shared ValidateTopicTopology
// rule set is unchanged.
func TestProjectionDLQTopologyInvariants(t *testing.T) {
	for _, test := range []struct {
		name   string
		actual string
		other  string
	}{
		{name: "projection DLQ differs from ingest DLQ", actual: TopicProjectionDLQ, other: TopicDLQ},
		{name: "projection DLQ differs from accepted", actual: TopicProjectionDLQ, other: TopicAccepted},
		{name: "projection DLQ differs from ledgered", actual: TopicProjectionDLQ, other: TopicLedgered},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.actual == test.other {
				t.Fatalf("addresses are equal: %q", test.actual)
			}
		})
	}
	if strings.TrimSpace(TopicProjectionDLQ) == "" || !strings.Contains(TopicProjectionDLQ, ".") {
		t.Fatalf("projection DLQ address %q must be a dotted topic address", TopicProjectionDLQ)
	}

	// The ingest replay tool must refuse the projection DLQ outright.
	if err := ValidateReplayTopology(TopicAccepted, TopicProjectionDLQ, "group"); err == nil {
		t.Fatal("ValidateReplayTopology must reject the projection DLQ")
	} else if !strings.Contains(err.Error(), "projection") {
		t.Fatalf("replay error = %v, want a diagnostic naming the projection DLQ", err)
	}
	if err := ValidateIngestDLQ(TopicProjectionDLQ); err == nil {
		t.Fatal("ValidateIngestDLQ must reject the projection DLQ")
	} else if !strings.Contains(err.Error(), "projection") {
		t.Fatalf("ingest error = %v, want a diagnostic naming the projection DLQ", err)
	}

	// The projector must refuse the ingest replay DLQ, and must accept its
	// own topic plus the library's no-publisher compatibility mode.
	if err := ValidateProjectionDLQ(TopicLedgered, TopicDLQ); err == nil {
		t.Fatal("ValidateProjectionDLQ must reject the ingest replay DLQ")
	} else if !strings.Contains(err.Error(), "ingest") || !strings.Contains(err.Error(), TopicProjectionDLQ) {
		t.Fatalf("projection error = %v, want both offending and intended addresses named", err)
	}
	if err := ValidateProjectionDLQ(TopicLedgered, TopicProjectionDLQ); err != nil {
		t.Fatalf("ValidateProjectionDLQ(ledgered, projection DLQ) error = %v, want nil", err)
	}
	if err := ValidateProjectionDLQ(TopicLedgered, ""); err != nil {
		t.Fatalf("ValidateProjectionDLQ(ledgered, empty) error = %v, want nil (compatibility mode)", err)
	}
	// The shared rule set is unchanged: a projector-shaped source with the
	// projection DLQ stays acceptable to it, which is why the directional
	// rules cannot live there.
	if err := ValidateTopicTopology(TopicLedgered, TopicProjectionDLQ); err != nil {
		t.Fatalf("ValidateTopicTopology error = %v, want nil (unchanged rule set)", err)
	}
	// A custom projection address is still a per-environment escape hatch.
	if err := ValidateProjectionDLQ(TopicLedgered, "audit.events.projection.dlq.staging.v1"); err != nil {
		t.Fatalf("custom projection DLQ rejected: %v", err)
	}
}
