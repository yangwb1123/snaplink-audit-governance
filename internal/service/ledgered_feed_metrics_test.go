package service

// Regression coverage for the publisher-side counters and the durable-outbox
// backlog gauge (audit_ledgered_publish_failures_total /
// audit_ledgered_publish_success_total / audit_ledgered_outbox_pending).
//
// The post-ledger publication is the only channel feeding the ClickHouse
// projection, and every one of its failure modes used to be log-only. These
// tests pin three things a /metrics renderer cannot see for itself:
//
//   - each failure exit is counted exactly once (no double counting between the
//     shared publish path and the per-path acknowledgement sites);
//   - the gauge is a real backlog read out of band — omitted before the first
//     successful round, and UNTRUNCATED, so it is never the batch size;
//   - at-least-once is unchanged: a counted failure still leaves the durable
//     intent in place for the next round.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// TestLedgeredOutboxPendingUnsetUntilFirstRound pins the omit rule (R3/I4):
// before any round has sampled the backlog the gauge has no meaningful value,
// so the renderer must omit the series rather than emit a fabricated 0 — and a
// no-op flush on a publisher-less service must not manufacture a sample.
func TestLedgeredOutboxPendingUnsetUntilFirstRound(t *testing.T) {
	svc := testService(t, false)
	if pending, sampled := svc.LedgeredOutboxPending(); sampled || pending != 0 {
		t.Fatalf("pending=%d sampled=%v on a fresh service, want (0,false)", pending, sampled)
	}

	// No publisher configured: FlushLedgeredOutbox is a documented no-op and
	// must leave the gauge unsampled, otherwise a deployment that can never
	// publish would advertise an empty backlog it never observed.
	if flushed, err := svc.FlushLedgeredOutbox(testCtx, 10); err != nil || flushed != 0 {
		t.Fatalf("flush=%d err=%v, want a no-op", flushed, err)
	}
	if pending, sampled := svc.LedgeredOutboxPending(); sampled || pending != 0 {
		t.Fatalf("pending=%d sampled=%v after a publisher-less flush, want (0,false)", pending, sampled)
	}
}

// TestLedgeredOutboxPendingIsUntruncatedBacklog pins R3/I5: the sample is taken
// before the round's batch truncation, so it is the durable backlog and not the
// number of intents this one round happened to process.
func TestLedgeredOutboxPendingIsUntruncatedBacklog(t *testing.T) {
	svc := testService(t, false)
	seedLedgeredOutbox(t, svc, "ledgered-gauge-a", "ledgered-gauge-b", "ledgered-gauge-c")
	publisher := &recordingLedgeredPublisher{}
	svc.Config.LedgeredPublisher = publisher

	flushed, err := svc.FlushLedgeredOutbox(testCtx, 1)
	if err != nil || flushed != 1 {
		t.Fatalf("flush=%d err=%v, want the batch limit of one publication", flushed, err)
	}
	if pending, sampled := svc.LedgeredOutboxPending(); !sampled || pending != 3 {
		t.Fatalf("pending=%d sampled=%v, want the untruncated backlog 3", pending, sampled)
	}

	// The gauge is a round-start sample: this round drained one intent, so the
	// next round is what publishes the drained value. That is the documented
	// cadence, not a stale-read bug.
	if flushed, err := svc.FlushLedgeredOutbox(testCtx, 10); err != nil || flushed != 2 {
		t.Fatalf("drain flush=%d err=%v, want the remaining two intents", flushed, err)
	}
	if pending, sampled := svc.LedgeredOutboxPending(); !sampled || pending != 2 {
		t.Fatalf("pending=%d sampled=%v, want the previous round's backlog 2", pending, sampled)
	}
	if flushed, err := svc.FlushLedgeredOutbox(testCtx, 10); err != nil || flushed != 0 {
		t.Fatalf("final flush=%d err=%v, want an empty outbox", flushed, err)
	}
	if pending, sampled := svc.LedgeredOutboxPending(); !sampled || pending != 0 {
		t.Fatalf("pending=%d sampled=%v, want the drained backlog 0", pending, sampled)
	}
}

// TestPublishCloneFailureIncrementsFailures pins R1's clone-failure site, which
// neither the broker nor the acknowledgement path can reach: the event graph
// clone fails closed, so the publication never even reaches the broker. The
// failure must be counted and the durable intent must survive.
func TestPublishCloneFailureIncrementsFailures(t *testing.T) {
	svc := testService(t, false)
	publisher := &recordingLedgeredPublisher{}
	svc.Config.LedgeredPublisher = publisher

	event := testEvent("ledgered-clone-failure", "op-clone", time.Unix(1_700_000_560, 0).UTC())
	// domain.CloneEvent fails closed on an uncloneable kind, which is exactly
	// the defect-triage signal this counter exists to surface.
	event.Payload = map[string]any{"resource": make(chan struct{})}
	if svc.publishLedgeredNow(testCtx, event) {
		t.Fatal("publishLedgeredNow reported a delivery for an uncloneable event graph")
	}
	if failures := svc.LedgeredPublishFailures(); failures != 1 {
		t.Fatalf("failures=%d, want 1 for the uncloneable event graph", failures)
	}
	if successes := svc.LedgeredPublishSuccesses(); successes != 0 {
		t.Fatalf("successes=%d, want 0 when the publication never left the process", successes)
	}
	if len(publisher.events) != 0 {
		t.Fatalf("publisher was called %d times, want 0 (clone failed first)", len(publisher.events))
	}
}

// TestRequestAckFailureIncrementsFailures pins R1's request-path
// acknowledgement site: the broker accepted the event but the durable
// acknowledgement could not be persisted, so the intent must stay owed and the
// attempt must count as a failure, not as a success.
func TestRequestAckFailureIncrementsFailures(t *testing.T) {
	backend := newFailSaveBackend()
	svc := newServiceOn(t, store.NewWithBackend(backend))
	seedTestDomain(t, svc)
	publisher := &recordingLedgeredPublisher{}
	svc.Config.LedgeredPublisher = publisher
	backend.mu.Lock()
	ackSave := backend.saves + 2 // ledger commit, then publication acknowledgement
	backend.errorFrom, backend.errorTo, backend.sentinel = ackSave, ackSave, errors.New("ack persistence unavailable")
	backend.mu.Unlock()

	event := testEvent("ledgered-ack-metric", "op-ack", time.Unix(1_700_000_561, 0).UTC())
	receipt, err := svc.Ingest(context.Background(), "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err != nil || receipt.StreamID == "" {
		t.Fatalf("receipt=%+v err=%v, want successful ingest despite ack failure", receipt, err)
	}
	if failures := svc.LedgeredPublishFailures(); failures != 1 {
		t.Fatalf("failures=%d, want 1 for the failed acknowledgement", failures)
	}
	if successes := svc.LedgeredPublishSuccesses(); successes != 0 {
		t.Fatalf("successes=%d, want 0 when the intent was not durably removed", successes)
	}
	if pending := pendingLedgeredEvents(t, svc); len(pending) != 1 {
		t.Fatalf("pending outbox=%d, want the intent to survive at-least-once", len(pending))
	}
}

// TestFlushAckFailureIncrementsFailures pins R1's flush-round acknowledgement
// site, which the request-path test cannot reach: the broker accepted the event
// during the background round, but persisting the acknowledgement failed, so the
// round must report the error, keep the intent, and count the failure.
func TestFlushAckFailureIncrementsFailures(t *testing.T) {
	backend := newFailSaveBackend()
	svc := newServiceOn(t, store.NewWithBackend(backend))
	seedTestDomain(t, svc)
	seedLedgeredOutbox(t, svc, "ledgered-flush-ack-failure")
	publisher := &recordingLedgeredPublisher{}
	svc.Config.LedgeredPublisher = publisher
	backend.mu.Lock()
	// The seeded intent already exists, so the next Save is the acknowledgement.
	ackSave := backend.saves + 1
	backend.errorFrom, backend.errorTo, backend.sentinel = ackSave, ackSave, errors.New("ack persistence unavailable")
	backend.mu.Unlock()

	flushed, err := svc.FlushLedgeredOutbox(testCtx, 10)
	if err == nil {
		t.Fatalf("flush=%d err=nil, want the acknowledgement error surfaced to the caller", flushed)
	}
	if flushed != 0 {
		t.Fatalf("flush=%d, want 0 published when the acknowledgement failed", flushed)
	}
	if failures := svc.LedgeredPublishFailures(); failures != 1 {
		t.Fatalf("failures=%d, want 1 for the failed acknowledgement", failures)
	}
	if successes := svc.LedgeredPublishSuccesses(); successes != 0 {
		t.Fatalf("successes=%d, want 0 when the intent was not durably removed", successes)
	}
	if len(publisher.events) != 1 {
		t.Fatalf("publisher calls=%d, want 1 (delivery happened before the ack)", len(publisher.events))
	}
}

// TestFlushAckFailureAddsNoLogLine pins the log-surface contract shared with
// TestPublishFailureDoesNotFailIngest: the flush acknowledgement failure is
// already logged by the flush loop's owner (cmd/audit-api/ledgered.go), which
// prints the error this round returns, so the service must not add a second
// diagnostic for the same attempt. The only line the service may emit here is
// the pre-existing publication-success line.
func TestFlushAckFailureAddsNoLogLine(t *testing.T) {
	backend := newFailSaveBackend()
	svc := newServiceOn(t, store.NewWithBackend(backend))
	seedTestDomain(t, svc)
	seedLedgeredOutbox(t, svc, "ledgered-flush-ack-nolog")
	svc.Config.LedgeredPublisher = &recordingLedgeredPublisher{}
	var logs []string
	svc.Config.Logf = func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	}
	backend.mu.Lock()
	backend.errorFrom, backend.errorTo, backend.sentinel = backend.saves+1, backend.saves+1, errors.New("ack persistence unavailable")
	backend.mu.Unlock()

	if _, err := svc.FlushLedgeredOutbox(testCtx, 10); err == nil {
		t.Fatal("want the acknowledgement error surfaced to the flush loop")
	}
	if failures := svc.LedgeredPublishFailures(); failures != 1 {
		t.Fatalf("failures=%d, want 1 for the failed acknowledgement", failures)
	}
	if len(logs) != 1 || !strings.Contains(logs[0], "ledgered published") {
		t.Fatalf("flush acknowledgement must not add a service log line, got %v", logs)
	}
}
