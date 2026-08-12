package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/snaplink/audit-governance/internal/outbox"
)

// fakeReplayReader is a messageReader whose FetchMessage returns queued
// messages and then blocks until the context expires (drain window), exactly
// like the real kafka-go reader at the end of a topic.
type fakeReplayReader struct {
	messages []kafka.Message
	index    int
	commits  []kafka.Message
	topic    string
	// delay makes each queued message available only after delay, simulating
	// sustained ingest whose arrival rate is below the drain window.
	delay time.Duration
	// fetchErr is returned once the queue is exhausted instead of blocking,
	// simulating a transport failure mid-scan.
	fetchErr error
	// fetchLog, when non-nil, records every fetched message offset (T2 round
	// re-fetch proof).
	fetchLog *[]int64
	// closeErr, when non-nil, is returned by Close (F4: reset abort path).
	closeErr error
}

func (f *fakeReplayReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if f.index < len(f.messages) {
		message := f.messages[f.index]
		f.index++
		if f.fetchLog != nil {
			*f.fetchLog = append(*f.fetchLog, message.Offset)
		}
		if f.delay > 0 {
			select {
			case <-time.After(f.delay):
			case <-ctx.Done():
				return kafka.Message{}, ctx.Err()
			}
		}
		return message, nil
	}
	if f.fetchErr != nil {
		return kafka.Message{}, f.fetchErr
	}
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (f *fakeReplayReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	f.commits = append(f.commits, msgs...)
	return nil
}

func (f *fakeReplayReader) Config() kafka.ReaderConfig { return kafka.ReaderConfig{Topic: f.topic} }
func (f *fakeReplayReader) Close() error               { return f.closeErr }

// freshAcceptedReader returns a factory that hands out a NEW fake reader per
// call with the same queue, modeling the production per-round accepted-reader
// recreation: a fresh reader starts at the first retained offset (index 0)
// even though the previous round's reader consumed the whole queue. fetchLog,
// when non-nil, is shared across the readers so a test can prove round N+1
// re-fetched offsets round N already consumed.
func freshAcceptedReader(messages []kafka.Message, fetchLog *[]int64) func() messageReader {
	return func() messageReader {
		return &fakeReplayReader{topic: TopicAccepted, messages: messages, fetchLog: fetchLog}
	}
}

// brokerDLQ models the broker-side state Kafka keeps for a DLQ partition: ONE
// committed offset, advanced to max(committed, offset+1) per commit, shared
// by every reader session. A fresh session only delivers messages at or
// above the committed offset — so a record committed past a pending record
// is never re-delivered (the exact mid-batch loss the commit barrier must
// prevent).
type brokerDLQ struct {
	topic     string
	messages  []kafka.Message // ascending offsets within one partition
	committed int64           // broker-side committed offset (starts at 0)
	commits   []kafka.Message
}

// brokerReplayReader is one DLQ reader session against a brokerDLQ. The
// session position (index) starts at the committed offset and never rewinds;
// recreating the reader (a fresh session) is the only way a later round sees
// records that were fetched-but-uncommitted by an earlier session — the F1
// per-round DLQ reader recreation the production code performs.
type brokerReplayReader struct {
	broker *brokerDLQ
	index  int
}

func (b *brokerReplayReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	for b.index < len(b.broker.messages) {
		message := b.broker.messages[b.index]
		b.index++
		if message.Offset < b.broker.committed {
			continue // already committed: a fresh session would not deliver it
		}
		return message, nil
	}
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (b *brokerReplayReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	b.broker.commits = append(b.broker.commits, msgs...)
	for _, m := range msgs {
		if m.Offset+1 > b.broker.committed {
			b.broker.committed = m.Offset + 1
		}
	}
	return nil
}

func (b *brokerReplayReader) Config() kafka.ReaderConfig {
	return kafka.ReaderConfig{Topic: b.broker.topic}
}
func (b *brokerReplayReader) Close() error { return nil }

// freshDLQSession returns a factory handing out a NEW DLQ reader session per
// round against the same broker state (the committed offset persists across
// sessions, exactly like a real broker), modeling the F1 per-round DLQ
// reader recreation: each round drains from the committed offset, so pending
// records are re-delivered and converged records are not.
func (b *brokerDLQ) freshDLQSession() func() messageReader {
	return func() messageReader { return &brokerReplayReader{broker: b} }
}

func dlqMessage(eventID string) kafka.Message {
	return dlqMessageAt(eventID, 1)
}

// dlqMessageAt builds a dead-letter Failure record at an explicit offset for
// broker-semantics tests: offsets are unique per partition, so tests that
// mix pending and resolved records in one partition must use distinct
// offsets.
func dlqMessageAt(eventID string, offset int64) kafka.Message {
	failure := Failure{EventID: eventID, ErrorCode: ErrorCodeAttemptsExhausted, ErrorMessage: "boom"}
	encoded, _ := json.Marshal(failure)
	return kafka.Message{Key: []byte(eventID), Value: encoded, Partition: 0, Offset: offset}
}

func acceptedMessage(eventID string) kafka.Message {
	value := []byte(`{"event_id":"` + eventID + `","source_system":"crm"}`)
	return kafka.Message{Key: []byte(eventID), Value: value, Partition: 0, Offset: 1}
}

// TestReplayOnceRecoversAndRepublishes pins the happy path: a DLQ failure
// for event X is matched against the accepted topic and the original message
// is re-published byte-for-byte exactly once.
func TestReplayOnceRecoversAndRepublishes(t *testing.T) {
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-a"), dlqMessage("evt-b")}}
	accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{acceptedMessage("evt-a"), acceptedMessage("other"), acceptedMessage("evt-b")}}
	state, err := LoadReplayState(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var republished []kafka.Message
	replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		republished = append(republished, kafka.Message{Key: key, Value: value})
		return nil
	})
	replayer.drainTimeout = 20 * time.Millisecond
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("replayed=%d, want 2", count)
	}
	if len(republished) != 2 {
		t.Fatalf("republished=%d, want 2", len(republished))
	}
	if got := string(republished[0].Key); got != "evt-a" {
		t.Fatalf("first republish key=%s, want evt-a", got)
	}
	// The un-wanted message was not re-published, and both replayed IDs are
	// persisted so a second round is a no-op.
	if _, ok := state.Replayed["other"]; ok {
		t.Fatal("un-wanted message marked replayed")
	}
	if !state.Replayed["evt-a"] || !state.Replayed["evt-b"] {
		t.Fatal("replayed IDs not persisted")
	}
	dlq.index = 0 // second round: the DLQ is drained (no new records)
	second, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second != 0 || len(republished) != 2 {
		t.Fatalf("second round replayed=%d republished=%d, want 0/2 (idempotent)", second, len(republished))
	}
}

// TestReplayPersistsStateAcrossRestarts pins recovery: a new Replayer with a
// re-loaded state file skips already-replayed events.
func TestReplayPersistsStateAcrossRestarts(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	state, err := LoadReplayState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Mark("evt-done"); err != nil {
		t.Fatal(err)
	}
	reloaded, err := LoadReplayState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if !reloaded.Replayed["evt-done"] {
		t.Fatal("replayed set lost across restart")
	}
}

// TestReplayTransientRepublishFailureRetriesNextRound pins convergence: a
// transient republish error leaves the event pending (DLQ offset NOT
// committed, and the per-partition barrier keeps the committed offset below
// it), and the next round re-reads the same record and retries instead of
// losing it. Re-delivery works because RunOnce recreates the DLQ reader per
// round (resetDLQToCommittedOffset): kafka-go never re-delivers a fetched
// record within one group session, so a fresh session starting at the
// committed offset is the only mechanism that can re-read a pending record.
// The accepted reader is recreated too (freshAcceptedReader), so the
// original is re-scanned from the first retained offset.
func TestReplayTransientRepublishFailureRetriesNextRound(t *testing.T) {
	broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{dlqMessageAt("evt-t", 1)}}
	acceptedQueue := []kafka.Message{acceptedMessage("evt-t")}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	attempts := 0
	replayer := newReplayerWithFactories(broker.freshDLQSession(), freshAcceptedReader(acceptedQueue, nil), state, func(ctx context.Context, key, value []byte) error {
		attempts++
		if attempts == 1 {
			return context.DeadlineExceeded
		}
		return nil
	}, log.New(io.Discard, "", 0))
	replayer.drainTimeout = 20 * time.Millisecond
	if _, err := replayer.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state.Replayed["evt-t"] {
		t.Fatal("transient failure must not mark the event replayed")
	}
	if broker.committed != 0 {
		t.Fatalf("DLQ committed offset=%d after transient failure, want 0: the record must stay pending for the next round", broker.committed)
	}
	// Next round: the recreated DLQ session re-delivers the uncommitted
	// record from the committed offset and the event converges.
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || attempts != 2 {
		t.Fatalf("second round replayed=%d attempts=%d, want 1/2", count, attempts)
	}
	if !state.Replayed["evt-t"] {
		t.Fatal("evt-t must be marked replayed after the converged round")
	}
	if broker.committed != 2 {
		t.Fatalf("DLQ committed offset=%d after converged round, want 2", broker.committed)
	}
}

// TestReplayPermanentFailureConverges pins the anti-loop rule: an API-mode
// permanent rejection is marked replayed so the round converges and the
// operator investigates via the DLQ record instead of retrying forever.
func TestReplayPermanentFailureConverges(t *testing.T) {
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-p")}}
	accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{acceptedMessage("evt-p")}}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		return &outbox.DeliveryError{Permanent: true}
	})
	replayer.drainTimeout = 20 * time.Millisecond
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || !state.Replayed["evt-p"] {
		t.Fatalf("permanent failure must converge: replayed=%d marked=%v", count, state.Replayed["evt-p"])
	}
	dlqRecords, acceptedSeen, replayed, republishFailures, pending := replayer.Metrics()
	if dlqRecords != 1 || acceptedSeen != 1 || replayed != 1 || republishFailures != 1 || pending != 0 {
		t.Fatalf("metrics dlq=%d accepted=%d replayed=%d failures=%d pending=%d, want 1/1/1/1/0", dlqRecords, acceptedSeen, replayed, republishFailures, pending)
	}
}

// AC-1 (REQ-1): an accepted-topic message with a missing key is still matched
// by its payload event_id: the original is republished byte-for-byte (nil key
// preserved) and the DLQ record is committed. A second round is a replay
// no-op: zero accepted fetches, zero republishes; the re-read record is only
// re-committed (a real broker's committed group offset would not re-deliver
// it).
func TestReplayEmptyKeyAcceptedMessageCommits(t *testing.T) {
	original := []byte(`{"event_id":"evt-e","source_system":"crm"}`)
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-e")}}
	accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{{Key: nil, Value: original, Partition: 0, Offset: 1}}}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	var republished []kafka.Message
	replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		republished = append(republished, kafka.Message{Key: key, Value: value})
		return nil
	})
	replayer.drainTimeout = 20 * time.Millisecond
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("replayed=%d, want 1", count)
	}
	if len(republished) != 1 {
		t.Fatalf("republished=%d, want 1 (empty-key message must match by payload event_id)", len(republished))
	}
	if republished[0].Key != nil {
		t.Fatalf("republish key=%q, want nil (original key preserved byte-for-byte)", republished[0].Key)
	}
	if !bytes.Equal(republished[0].Value, original) {
		t.Fatalf("republished value=%s, want byte-identical original %s", republished[0].Value, original)
	}
	if !state.Replayed["evt-e"] {
		t.Fatal("payload event_id must be marked replayed")
	}
	if len(dlq.commits) != 1 {
		t.Fatalf("DLQ commits=%d, want 1", len(dlq.commits))
	}
	// Round 2 re-reads the DLQ record (the fake re-delivers it because the
	// fake never persists offsets): the state file makes it a replay no-op
	// — zero accepted fetches, zero republishes — and the already-resolved
	// record is re-committed (2 total).
	dlq.index = 0
	second, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second != 0 {
		t.Fatalf("second round replayed=%d, want 0 (idempotent)", second)
	}
	if len(republished) != 1 {
		t.Fatalf("second round republished=%d, want 1 (no re-replay)", len(republished))
	}
	if got := replayer.acceptedSeen.Load(); got != 1 {
		t.Fatalf("acceptedSeen=%d, want 1 (second round must not re-scan)", got)
	}
	if len(dlq.commits) != 2 {
		t.Fatalf("DLQ commits=%d, want 2 (re-read record re-committed, never replayed)", len(dlq.commits))
	}
}

// AC-2 (REQ-1): a key that differs from the payload event_id is overridden
// by the payload: the accepted message is republished exactly once with its
// original key preserved, the DLQ record is committed under the payload ID,
// and the stale key is never marked replayed.
func TestReplayMismatchedKeyResolvedByPayloadEventID(t *testing.T) {
	original := []byte(`{"event_id":"evt-m","source_system":"crm"}`)
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-m")}}
	accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{{Key: []byte("stale-key"), Value: original, Partition: 0, Offset: 1}}}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	var republished []kafka.Message
	replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		republished = append(republished, kafka.Message{Key: key, Value: value})
		return nil
	})
	replayer.drainTimeout = 20 * time.Millisecond
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(republished) != 1 {
		t.Fatalf("replayed=%d republished=%d, want 1/1", count, len(republished))
	}
	if got := string(republished[0].Key); got != "stale-key" {
		t.Fatalf("republish key=%q, want stale-key (original key preserved byte-for-byte)", got)
	}
	if !bytes.Equal(republished[0].Value, original) {
		t.Fatalf("republished value=%s, want byte-identical original", republished[0].Value)
	}
	if !state.Replayed["evt-m"] {
		t.Fatal("payload event_id must be marked replayed")
	}
	if state.Replayed["stale-key"] {
		t.Fatal("stale key must never be marked replayed")
	}
	if len(dlq.commits) != 1 {
		t.Fatalf("DLQ commits=%d, want 1", len(dlq.commits))
	}
	// Round 2: replay no-op — zero accepted fetches, zero republishes.
	dlq.index = 0
	second, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second != 0 || len(republished) != 1 {
		t.Fatalf("second round replayed=%d republished=%d, want 0/1 (idempotent)", second, len(republished))
	}
	if got := replayer.acceptedSeen.Load(); got != 1 {
		t.Fatalf("acceptedSeen=%d, want 1 (no re-scan)", got)
	}
}

// AC-4 (REQ-2): a wanted DLQ record whose original is absent from the
// accepted topic converges in one bounded scan: when the topic goes quiet,
// the record is marked unresolvable (with a durable log line carrying the
// event_id) and committed. A second round performs zero accepted fetches.
func TestReplayUnresolvableRecordConvergesWithBoundedScans(t *testing.T) {
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-x")}}
	accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{
		{Key: []byte("k1"), Value: []byte(`{"event_id":"other-1","source_system":"crm"}`), Partition: 0, Offset: 1},
		{Key: []byte("k2"), Value: []byte("not-json"), Partition: 0, Offset: 2},
		{Key: nil, Value: []byte(`{"event_id":"other-2","source_system":"crm"}`), Partition: 0, Offset: 3},
	}}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	replayer := newReplayerWithReadersAndLogger(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		t.Fatal("republish must never be called: no accepted message matches evt-x")
		return nil
	}, log.New(&logs, "", 0))
	replayer.drainTimeout = 20 * time.Millisecond
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("replayed=%d, want 1 (unresolvable record converges in one round)", count)
	}
	if !state.Replayed["evt-x"] {
		t.Fatal("absent original must be marked unresolvable at drain")
	}
	if len(dlq.commits) != 1 {
		t.Fatalf("DLQ commits=%d, want 1", len(dlq.commits))
	}
	if got := replayer.acceptedSeen.Load(); got != 3 {
		t.Fatalf("acceptedSeen=%d, want 3 (exactly one bounded scan)", got)
	}
	logged := logs.String()
	if !strings.Contains(logged, "unresolvable") || !strings.Contains(logged, "evt-x") {
		t.Fatalf("log missing unresolvable evidence for evt-x, got:\n%s", logged)
	}
	// Round 2: evt-x is already replayed, so no accepted fetches happen.
	dlq.index = 0
	second, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second != 0 {
		t.Fatalf("second round replayed=%d, want 0", second)
	}
	if got := replayer.acceptedSeen.Load(); got != 3 {
		t.Fatalf("acceptedSeen=%d, want 3 (round 2 must not scan)", got)
	}
}

// AC-5 (failure mode: sustained ingest / cut-off): a topic that never goes
// quiet within 2x drainTimeout cuts the round off WITHOUT declaring it
// drained: nothing is marked, DLQ records stay pending, the round returns no
// error, and the next round retries. The scan is bounded by the 2-window cap
// (3 messages at 45ms cadence inside two 100ms windows). Once the topic goes
// quiet, the same record converges as unresolvable.
func TestReplaySustainedIngestCutsRoundOffWithoutMarking(t *testing.T) {
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-y")}}
	// delay=45ms < window=100ms: messages keep arriving inside each fetch
	// window, so no quiet window is observable before the 2x cap fires.
	accepted := &fakeReplayReader{topic: TopicAccepted, delay: 45 * time.Millisecond, messages: []kafka.Message{
		{Key: []byte("o1"), Value: []byte(`{"event_id":"other-1","source_system":"crm"}`), Partition: 0, Offset: 1},
		{Key: []byte("o2"), Value: []byte(`{"event_id":"other-2","source_system":"crm"}`), Partition: 0, Offset: 2},
		{Key: []byte("o3"), Value: []byte(`{"event_id":"other-3","source_system":"crm"}`), Partition: 0, Offset: 3},
	}}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		t.Fatal("no accepted message matches evt-y; republish must never be called")
		return nil
	})
	replayer.drainTimeout = 100 * time.Millisecond
	for round := 1; round <= 2; round++ {
		count, err := replayer.RunOnce(context.Background())
		if err != nil {
			t.Fatalf("round %d: cut-off must not be an error, got %v", round, err)
		}
		if count != 0 {
			t.Fatalf("round %d: replayed=%d, want 0 (nothing marked on cut-off)", round, count)
		}
		if state.Replayed["evt-y"] {
			t.Fatalf("round %d: evt-y marked replayed on cut-off, want pending", round)
		}
		if len(dlq.commits) != 0 {
			t.Fatalf("round %d: DLQ commits=%d, want 0 (record must stay pending)", round, len(dlq.commits))
		}
		dlq.index = 0
		accepted.index = 0
	}
	// The round is bounded by the 2x drainTimeout cap, not by a quiet window.
	if got := replayer.acceptedSeen.Load(); got != 6 {
		t.Fatalf("acceptedSeen=%d, want 6 (two bounded 2-window scans of 3 messages)", got)
	}
	// Once the topic goes quiet the same record converges as unresolvable.
	accepted.index = len(accepted.messages)
	dlq.index = 0
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || !state.Replayed["evt-y"] || len(dlq.commits) != 1 {
		t.Fatalf("quiet round replayed=%d marked=%v commits=%d, want 1/true/1", count, state.Replayed["evt-y"], len(dlq.commits))
	}
}

// AC-6 (failure mode: transport error): a non-drain FetchMessage error
// aborts the round with NO DLQ commits — even events already resolved stay
// pending (at-least-once; the state file keeps replay idempotent) — and the
// next round retries. This pins the promised scanErr behavior.
func TestReplayTransportErrorLeavesRecordsPending(t *testing.T) {
	sentinel := errors.New("accepted topic transport failure")
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-a"), dlqMessage("evt-b")}}
	// The transport error strikes after evt-a's original was fetched but
	// before evt-b's original is reached: evt-a is resolved, evt-b is not.
	accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{acceptedMessage("evt-a")}, fetchErr: sentinel}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	republished := 0
	replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		republished++
		return nil
	})
	replayer.drainTimeout = 20 * time.Millisecond
	count, err := replayer.RunOnce(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("round 1 err=%v, want the transport sentinel", err)
	}
	if count != 1 || republished != 1 {
		t.Fatalf("round 1 replayed=%d republished=%d, want 1/1 (evt-a resolved before the error)", count, republished)
	}
	if !state.Replayed["evt-a"] {
		t.Fatal("evt-a must be marked replayed (idempotency) even though the round errored")
	}
	if len(dlq.commits) != 0 {
		t.Fatalf("round 1 DLQ commits=%d, want 0 (transport error commits nothing, not even resolved records)", len(dlq.commits))
	}
	// Round 2 with the transport restored: the pending record is retried and
	// both records reach a durable decision.
	accepted.fetchErr = nil
	accepted.messages = append(accepted.messages, acceptedMessage("evt-b"))
	dlq.index = 0
	accepted.index = 0
	count, err = replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || republished != 2 {
		t.Fatalf("round 2 replayed=%d republished=%d, want 1/2", count, republished)
	}
	if !state.Replayed["evt-b"] {
		t.Fatal("evt-b must be replayed in round 2")
	}
	if len(dlq.commits) != 2 {
		t.Fatalf("round 2 DLQ commits=%d, want 2 (both records reach a durable decision)", len(dlq.commits))
	}
	if got := replayer.acceptedSeen.Load(); got != 3 {
		t.Fatalf("acceptedSeen=%d, want 3 (1 in round 1, 2 in round 2)", got)
	}
}

// AC-7 (drained-vs-cutoff boundary): the scan declares drained only after a
// full quiet window with the outer context still live; a last message inside
// the final window is a cut-off, not a drain. The boundary cases:
//   - exactly one full quiet window => drained => unresolvable marked;
//   - last message < one window before the 2x cap => cut-off, nothing marked;
//   - outer cancellation mid-scan => never marked;
//   - drained but found this round (transient republish failure) => pending.
func TestReplayDrainedVsCutoffBoundary(t *testing.T) {
	t.Run("full quiet window marks unresolvable", func(t *testing.T) {
		// Boundary equality: the last message is fetched, then exactly one
		// full window passes with nothing arriving => drained=true.
		dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-x")}}
		accepted := &fakeReplayReader{topic: TopicAccepted}
		state, err := LoadReplayState("")
		if err != nil {
			t.Fatal(err)
		}
		var logs bytes.Buffer
		replayer := newReplayerWithReadersAndLogger(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
			t.Fatal("republish must never be called")
			return nil
		}, log.New(&logs, "", 0))
		replayer.drainTimeout = 60 * time.Millisecond
		count, err := replayer.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if count != 1 || !state.Replayed["evt-x"] || len(dlq.commits) != 1 {
			t.Fatalf("quiet drain replayed=%d marked=%v commits=%d, want 1/true/1", count, state.Replayed["evt-x"], len(dlq.commits))
		}
		if !strings.Contains(logs.String(), "unresolvable") {
			t.Fatalf("log missing unresolvable evidence:\n%s", logs.String())
		}
	})
	t.Run("last message inside final window is cut-off, not drain", func(t *testing.T) {
		// 45ms message cadence < 60ms window: the second message lands at
		// 90ms (> one window), so the 2x cap (120ms) fires with the last
		// message only 30ms old => drained=false, nothing marked, no commits.
		dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-x")}}
		accepted := &fakeReplayReader{topic: TopicAccepted, delay: 45 * time.Millisecond, messages: []kafka.Message{
			{Key: []byte("o1"), Value: []byte(`{"event_id":"other-1","source_system":"crm"}`), Partition: 0, Offset: 1},
			{Key: []byte("o2"), Value: []byte(`{"event_id":"other-2","source_system":"crm"}`), Partition: 0, Offset: 2},
		}}
		state, err := LoadReplayState("")
		if err != nil {
			t.Fatal(err)
		}
		replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
			t.Fatal("republish must never be called")
			return nil
		})
		replayer.drainTimeout = 60 * time.Millisecond
		count, err := replayer.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 || state.Replayed["evt-x"] || len(dlq.commits) != 0 {
			t.Fatalf("cut-off replayed=%d marked=%v commits=%d, want 0/false/0", count, state.Replayed["evt-x"], len(dlq.commits))
		}
		if got := replayer.acceptedSeen.Load(); got != 2 {
			t.Fatalf("acceptedSeen=%d, want 2 (two messages inside the 2-window scan)", got)
		}
		// Once the topic goes quiet the same record converges.
		accepted.index = len(accepted.messages)
		dlq.index = 0
		count, err = replayer.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if count != 1 || !state.Replayed["evt-x"] || len(dlq.commits) != 1 {
			t.Fatalf("quiet round replayed=%d marked=%v commits=%d, want 1/true/1", count, state.Replayed["evt-x"], len(dlq.commits))
		}
	})
	t.Run("outer cancellation never marks", func(t *testing.T) {
		dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-x")}}
		accepted := &fakeReplayReader{topic: TopicAccepted}
		state, err := LoadReplayState("")
		if err != nil {
			t.Fatal(err)
		}
		replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
			t.Fatal("republish must never be called")
			return nil
		})
		replayer.drainTimeout = 100 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()
		count, err := replayer.RunOnce(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 || state.Replayed["evt-x"] || len(dlq.commits) != 0 {
			t.Fatalf("cancelled round replayed=%d marked=%v commits=%d, want 0/false/0", count, state.Replayed["evt-x"], len(dlq.commits))
		}
	})
	t.Run("drained but found stays pending", func(t *testing.T) {
		// D-2: at drain only never-seen wanted IDs are marked unresolvable.
		// evt-y was found this round (transient republish failure), so it
		// stays pending even though the topic went quiet; evt-x was never
		// seen and converges. Distinct offsets (1 and 2) model real Kafka
		// offsets (unique per partition); with the commit barrier, evt-x is
		// committable (offset 1 < pending offset 2) and evt-y stays
		// uncommitted.
		dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessageAt("evt-x", 1), dlqMessageAt("evt-y", 2)}}
		accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{acceptedMessage("evt-y")}}
		state, err := LoadReplayState("")
		if err != nil {
			t.Fatal(err)
		}
		attempts := 0
		replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
			attempts++
			if attempts == 1 {
				return context.DeadlineExceeded // transient: found but not resolved
			}
			return nil
		})
		replayer.drainTimeout = 20 * time.Millisecond
		count, err := replayer.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if count != 1 || !state.Replayed["evt-x"] || state.Replayed["evt-y"] {
			t.Fatalf("drain replayed=%d evt-x=%v evt-y=%v, want 1/true/false", count, state.Replayed["evt-x"], state.Replayed["evt-y"])
		}
		if len(dlq.commits) != 1 {
			t.Fatalf("DLQ commits=%d, want 1 (only evt-x; found-but-failed evt-y stays pending)", len(dlq.commits))
		}
		// Round 2: evt-y retries and succeeds; both records converge.
		dlq.index = 0
		accepted.index = 0
		count, err = replayer.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if count != 1 || !state.Replayed["evt-y"] || len(dlq.commits) != 3 {
			t.Fatalf("round 2 replayed=%d evt-y=%v commits=%d, want 1/true/3", count, state.Replayed["evt-y"], len(dlq.commits))
		}
	})
}

// T1 / AC-1 (REQ-3): daemon-mode two-round regression. Round 1 resolves
// evt-a; round 2 collects a NEW DLQ record for evt-b whose accepted original
// predates round 1's scan end (it was consumed, not wanted, in round 1). The
// accepted reader must be recreated per round, so round 2 re-scans from the
// first retained offset and evt-b is actually re-published — NOT logged
// unresolvable and dropped. Fails pre-fix: without the reset, round 2's scan
// starts where round 1 ended, evt-b is marked unresolvable, the DLQ offset is
// committed, and republished stays 1.
func TestReplayDaemonRound2RepublishesOlderAcceptedOriginal(t *testing.T) {
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-a")}}
	// evt-b's original is at a higher offset than evt-a's: both are consumed
	// by round 1's scan, so evt-b predates round 1's scan end.
	acceptedQueue := []kafka.Message{acceptedMessage("evt-a"), acceptedMessage("evt-b")}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	republished := 0
	replayer := newReplayerWithAcceptedFactory(dlq, freshAcceptedReader(acceptedQueue, nil), state, func(ctx context.Context, key, value []byte) error {
		republished++
		return nil
	}, log.New(&logs, "", 0))
	replayer.drainTimeout = 20 * time.Millisecond

	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || republished != 1 {
		t.Fatalf("round 1 replayed=%d republished=%d, want 1/1", count, republished)
	}
	if !state.Replayed["evt-a"] {
		t.Fatal("round 1 must mark evt-a replayed")
	}
	if len(dlq.commits) != 1 {
		t.Fatalf("round 1 DLQ commits=%d, want 1", len(dlq.commits))
	}

	// Round 2: a NEW DLQ record for evt-b arrives. The DLQ fake's persistent
	// index models the real reader's advanced position; accepted.index is
	// deliberately NOT reset — the per-round accepted-reader recreation is
	// exactly what must make evt-b's older original reachable again.
	dlq.messages = append(dlq.messages, dlqMessage("evt-b"))
	count, err = replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || republished != 2 {
		t.Fatalf("round 2 replayed=%d republished=%d, want 1/2 (evt-b must be actually re-published, not marked unresolvable)", count, republished)
	}
	if !state.Replayed["evt-b"] {
		t.Fatal("evt-b must be marked replayed")
	}
	if len(dlq.commits) != 2 {
		t.Fatalf("DLQ commits=%d, want 2 (both records reach a durable decision)", len(dlq.commits))
	}
	if logged := logs.String(); strings.Contains(logged, "unresolvable") {
		t.Fatalf("round 2 must not log unresolvable for evt-b, got:\n%s", logged)
	}
}

// T2 / AC-2 (REQ-4): round 2 re-fetches every message round 1 already
// consumed, proving the accepted reader was re-seeked to the first retained
// offset instead of resuming at round 1's end. Both proofs are asserted:
// acceptedSeen grows by N again (3 -> 6) and round 2's first fetched offset
// equals round 1's first fetched offset.
func TestReplayRound2RefetchesRound1Messages(t *testing.T) {
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-1")}}
	acceptedQueue := []kafka.Message{
		{Key: []byte("evt-1"), Value: []byte(`{"event_id":"evt-1","source_system":"crm"}`), Partition: 0, Offset: 1},
		{Key: []byte("evt-2"), Value: []byte(`{"event_id":"evt-2","source_system":"crm"}`), Partition: 0, Offset: 2},
		{Key: []byte("other"), Value: []byte(`{"event_id":"other","source_system":"crm"}`), Partition: 0, Offset: 3},
	}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	var fetchLog []int64
	republished := 0
	replayer := newReplayerWithAcceptedFactory(dlq, freshAcceptedReader(acceptedQueue, &fetchLog), state, func(ctx context.Context, key, value []byte) error {
		republished++
		return nil
	}, log.New(io.Discard, "", 0))
	replayer.drainTimeout = 20 * time.Millisecond

	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || republished != 1 {
		t.Fatalf("round 1 replayed=%d republished=%d, want 1/1", count, republished)
	}
	if got := replayer.acceptedSeen.Load(); got != 3 {
		t.Fatalf("round 1 acceptedSeen=%d, want 3", got)
	}

	// Round 2 wants evt-2, whose original (offset 2) was consumed in round 1.
	dlq.messages = append(dlq.messages, dlqMessage("evt-2"))
	count, err = replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || republished != 2 {
		t.Fatalf("round 2 replayed=%d republished=%d, want 1/2", count, republished)
	}
	if got := replayer.acceptedSeen.Load(); got != 6 {
		t.Fatalf("round 2 acceptedSeen=%d, want 6 (round 2 must re-fetch all 3 messages from the first retained offset)", got)
	}
	if !state.Replayed["evt-2"] {
		t.Fatal("evt-2 must be replayed")
	}
	if len(fetchLog) < 6 {
		t.Fatalf("fetchLog=%v, want 6 fetches across two rounds", fetchLog)
	}
	if fetchLog[0] != fetchLog[3] {
		t.Fatalf("round 2 first fetched offset=%d, want round 1 first offset %d (re-seek to the first retained offset)", fetchLog[3], fetchLog[0])
	}
}

// T3 / AC-3 (REQ-5): daemon mode (one Replayer, consecutive rounds with the
// per-round reset) and -once mode (a fresh Replayer per round, mirroring
// main.go's -once fresh-reader pattern) must produce identical OUTCOMES for
// identical DLQ input referencing older accepted messages. The assertion is
// on republished outcomes / state marks / DLQ commits / absence of the
// unresolvable log — NOT the raw replayed count: unresolvable marks inflate
// the daemon count, so a count-only comparison passes on buggy code.
func TestReplayDaemonEqualsOnceOutcomes(t *testing.T) {
	daemonStatePath := filepath.Join(t.TempDir(), "daemon-state.json")
	onceStatePath := filepath.Join(t.TempDir(), "once-state.json")
	acceptedQueue := []kafka.Message{acceptedMessage("evt-a"), acceptedMessage("evt-b")}

	// Scenario (a): daemon mode — one Replayer, two consecutive rounds.
	daemonState, err := LoadReplayState(daemonStatePath)
	if err != nil {
		t.Fatal(err)
	}
	daemonDLQ := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-a")}}
	daemonRepublished := 0
	daemonLogs := &bytes.Buffer{}
	daemon := newReplayerWithAcceptedFactory(daemonDLQ, freshAcceptedReader(acceptedQueue, nil), daemonState, func(ctx context.Context, key, value []byte) error {
		daemonRepublished++
		return nil
	}, log.New(daemonLogs, "", 0))
	daemon.drainTimeout = 20 * time.Millisecond
	if _, err := daemon.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	daemonDLQ.messages = append(daemonDLQ.messages, dlqMessage("evt-b"))
	if _, err := daemon.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Scenario (b): -once mode — a fresh Replayer (fresh readers) per round,
	// sharing the -state file across runs exactly like the binary does.
	onceRepublished := 0
	onceLogs := &bytes.Buffer{}
	onceCommits := 0
	for round, dlqInput := range [][]kafka.Message{{dlqMessage("evt-a")}, {dlqMessage("evt-b")}} {
		onceState, err := LoadReplayState(onceStatePath)
		if err != nil {
			t.Fatal(err)
		}
		onceDLQ := &fakeReplayReader{topic: TopicDLQ, messages: dlqInput}
		once := newReplayerWithAcceptedFactory(onceDLQ, freshAcceptedReader(acceptedQueue, nil), onceState, func(ctx context.Context, key, value []byte) error {
			onceRepublished++
			return nil
		}, log.New(onceLogs, "", 0))
		once.drainTimeout = 20 * time.Millisecond
		if _, err := once.RunOnce(context.Background()); err != nil {
			t.Fatalf("once round %d: %v", round+1, err)
		}
		onceCommits += len(onceDLQ.commits)
	}

	// Outcome equivalence — republished deliveries, state marks, DLQ commits,
	// and the absence of unresolvable marks. These fail on buggy code (the
	// daemon marks evt-b unresolvable instead of re-publishing it), while the
	// raw replayed counts (2 == 2) would not.
	if daemonRepublished != 2 || onceRepublished != 2 {
		t.Fatalf("republished daemon=%d once=%d, want 2/2 (both events actually delivered in both modes)", daemonRepublished, onceRepublished)
	}
	if !daemonState.Replayed["evt-a"] || !daemonState.Replayed["evt-b"] {
		t.Fatalf("daemon state=%v, want {evt-a, evt-b}", daemonState.Replayed)
	}
	onceStateFinal, err := LoadReplayState(onceStatePath)
	if err != nil {
		t.Fatal(err)
	}
	if !onceStateFinal.Replayed["evt-a"] || !onceStateFinal.Replayed["evt-b"] {
		t.Fatalf("once state=%v, want {evt-a, evt-b}", onceStateFinal.Replayed)
	}
	if len(daemonDLQ.commits) != 2 || onceCommits != 2 {
		t.Fatalf("DLQ commits daemon=%d once=%d, want 2/2", len(daemonDLQ.commits), onceCommits)
	}
	if strings.Contains(daemonLogs.String(), "unresolvable") || strings.Contains(onceLogs.String(), "unresolvable") {
		t.Fatalf("neither mode may mark unresolvable:\ndaemon:\n%s\nonce:\n%s", daemonLogs.String(), onceLogs.String())
	}
}

// F1 / F-A: a transiently-failed mid-batch DLQ record must never be
// leapfrogged by a later commit in the same partition. Kafka stores ONE
// committed offset per (group, partition) and kafka-go's CommitMessages
// commits offset+1 of the highest message passed, so committing the resolved
// record at offset 3 would advance the partition to 4 and the next round
// would never re-deliver the pending record at offset 1 — silent permanent
// loss. The commitResolved per-partition barrier leaves resolved records at
// or above the first pending offset uncommitted, so round 2's recreated DLQ
// session (freshDLQSession) re-reads both records, evt-p converges, and the
// committed offset only advances once nothing is pending. Fails pre-fix:
// without the barrier, round 1 commits offset 3 -> committed=4, round 2
// delivers nothing, and evt-p stays unmarked forever (attempts==1).
func TestReplayMidBatchPendingRecordIsNotSkipped(t *testing.T) {
	broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{
		dlqMessageAt("evt-p", 1), // transient republish failure in round 1
		dlqMessageAt("evt-r", 3), // resolved in round 1
	}}
	acceptedQueue := []kafka.Message{acceptedMessage("evt-p"), acceptedMessage("evt-r")}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	attempts := map[string]int{}
	replayer := newReplayerWithFactories(broker.freshDLQSession(), freshAcceptedReader(acceptedQueue, nil), state, func(ctx context.Context, key, value []byte) error {
		attempts[string(key)]++
		if string(key) == "evt-p" && attempts["evt-p"] == 1 {
			return context.DeadlineExceeded // transient: retried next round
		}
		return nil
	}, log.New(io.Discard, "", 0))
	replayer.drainTimeout = 20 * time.Millisecond

	// Round 1: evt-p fails (pending), evt-r succeeds (resolved). The barrier
	// keeps BOTH uncommitted — committing evt-r at offset 3 would cross the
	// pending offset 1 — so the partition's committed offset stays 0.
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("round 1 replayed=%d, want 1 (evt-r)", count)
	}
	if !state.Replayed["evt-r"] || state.Replayed["evt-p"] {
		t.Fatalf("round 1 state=%v, want {evt-r} only (evt-p pending)", state.Replayed)
	}
	if broker.committed != 0 {
		t.Fatalf("round 1 committed offset=%d, want 0: the partition must never commit past a pending record", broker.committed)
	}
	if len(broker.commits) != 0 {
		t.Fatalf("round 1 commits=%d, want 0 (evt-r must stay uncommitted behind the barrier)", len(broker.commits))
	}

	// Round 2: the recreated DLQ session re-reads both records from the
	// committed offset; evt-p is retried and converges, evt-r (already
	// replayed) is re-committed without a re-republish. The committed offset
	// advances only now that nothing is pending.
	count, err = replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("round 2 replayed=%d, want 1 (evt-p)", count)
	}
	if !state.Replayed["evt-p"] {
		t.Fatal("evt-p must converge in round 2")
	}
	if attempts["evt-p"] != 2 || attempts["evt-r"] != 1 {
		t.Fatalf("attempts evt-p=%d evt-r=%d, want 2/1 (evt-p retried exactly once, evt-r never re-republished)", attempts["evt-p"], attempts["evt-r"])
	}
	if broker.committed != 4 {
		t.Fatalf("committed offset=%d after convergence, want 4 (both records durable)", broker.committed)
	}
}

// F4: a reader close failure on either per-round reset aborts the round
// BEFORE anything is collected, republished, marked, or committed; the
// pending metric stays 0 (nothing is in flight), and the next round retries.
// kafka-go Close is idempotent and returns nil, so these paths are
// defensive — but the abort must not leak a stale audit_dlq_pending reading.
func TestReplayResetFailureAbortsRound(t *testing.T) {
	t.Run("dlq reader close failure", func(t *testing.T) {
		closeErr := errors.New("dlq close failure")
		dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessageAt("evt-a", 1)}, closeErr: closeErr}
		state, err := LoadReplayState("")
		if err != nil {
			t.Fatal(err)
		}
		republishCalled := false
		replayer := newReplayerWithFactories(func() messageReader { return dlq }, freshAcceptedReader([]kafka.Message{acceptedMessage("evt-a")}, nil), state, func(ctx context.Context, key, value []byte) error {
			republishCalled = true
			return nil
		}, log.New(io.Discard, "", 0))
		replayer.drainTimeout = 20 * time.Millisecond
		_, err = replayer.RunOnce(context.Background())
		if !errors.Is(err, closeErr) {
			t.Fatalf("err=%v, want the dlq close error", err)
		}
		if republishCalled {
			t.Fatal("republish must never be called after a reset abort")
		}
		if len(state.Replayed) != 0 || len(dlq.commits) != 0 {
			t.Fatalf("aborted round must not mark or commit: state=%v commits=%d", state.Replayed, len(dlq.commits))
		}
		if _, _, _, _, pending := replayer.Metrics(); pending != 0 {
			t.Fatalf("pending=%d after aborted round, want 0 (no stale metric)", pending)
		}
	})
	t.Run("accepted reader close failure", func(t *testing.T) {
		closeErr := errors.New("accepted close failure")
		dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessageAt("evt-a", 1)}}
		accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{acceptedMessage("evt-a")}, closeErr: closeErr}
		state, err := LoadReplayState("")
		if err != nil {
			t.Fatal(err)
		}
		republishCalled := false
		replayer := newReplayerWithFactories(func() messageReader { return dlq }, func() messageReader { return accepted }, state, func(ctx context.Context, key, value []byte) error {
			republishCalled = true
			return nil
		}, log.New(io.Discard, "", 0))
		replayer.drainTimeout = 20 * time.Millisecond
		_, err = replayer.RunOnce(context.Background())
		if !errors.Is(err, closeErr) {
			t.Fatalf("err=%v, want the accepted close error", err)
		}
		if republishCalled {
			t.Fatal("republish must never be called after a reset abort")
		}
		if len(state.Replayed) != 0 || len(dlq.commits) != 0 {
			t.Fatalf("aborted round must not mark or commit: state=%v commits=%d", state.Replayed, len(dlq.commits))
		}
		if _, _, _, _, pending := replayer.Metrics(); pending != 0 {
			t.Fatalf("pending=%d after aborted round, want 0 (no stale metric)", pending)
		}
	})
}
