package service

// Regression coverage for the per-pending-event publication gate.
//
// The defect these tests pin: one process-wide mutex (ledgeredPublishMu) was
// taken by both the synchronous post-commit publisher and FlushLedgeredOutbox,
// and the flusher held it for a WHOLE round — a snapshot read plus up to
// ledgeredOutboxBatchSize publishes and acknowledgements. Because the request
// path waits on that mutex with no bound of its own (and the HTTP/gRPC ingest
// context is deliberately detached from client cancellation), one slow broker
// stalled every concurrent ingest.
//
// AC-2 option (a) is the implemented shape: bounded, non-blocking acquisition
// with a durable-row deferral. Every wait below is a select with a timer, so
// a regression against the pre-change tree fails in seconds with a named
// message instead of hanging until Go's package timeout.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

const (
	// ingestBackstop is the wall-clock backstop for an ingest that is supposed
	// to return without waiting for a flush round. It is deliberately below
	// ledgeredPublishTimeout so even a per-key BLOCKING Lock half-fix (which
	// waits at most one publish) fails here rather than passing.
	ingestBackstop = 3 * time.Second
)

// gatedLedgeredPublisher blocks the ids named in block, and reports the first
// entry for each id through entered. It is the deterministic seam that lets a
// test hold a flush round inside a publish without sleeping and hoping.
type gatedLedgeredPublisher struct {
	mu      sync.Mutex
	block   map[string]chan struct{}
	entered map[string]chan struct{}
	seen    map[string]int
	order   []string
}

func newGatedLedgeredPublisher(blocked ...string) *gatedLedgeredPublisher {
	publisher := &gatedLedgeredPublisher{
		block:   make(map[string]chan struct{}, len(blocked)),
		entered: make(map[string]chan struct{}),
		seen:    make(map[string]int),
	}
	for _, id := range blocked {
		publisher.block[id] = make(chan struct{})
		publisher.entered[id] = make(chan struct{})
	}
	return publisher
}

func (p *gatedLedgeredPublisher) Publish(ctx context.Context, event domain.Event) error {
	p.mu.Lock()
	p.seen[event.EventID]++
	p.order = append(p.order, event.EventID)
	gate, gated := p.block[event.EventID]
	entered := p.entered[event.EventID]
	if gated && entered != nil {
		select {
		case <-entered:
		default:
			close(entered)
		}
	}
	p.mu.Unlock()
	if !gated {
		return nil
	}
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// counts returns how many times each event id reached the publisher.
func (p *gatedLedgeredPublisher) counts() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]int, len(p.seen))
	for id, count := range p.seen {
		out[id] = count
	}
	return out
}

// seedLedgeredOutbox writes durable intents directly, standing in for events
// whose ingest-time publication never reached the broker.
func seedLedgeredOutbox(t *testing.T, svc *Service, ids ...string) {
	t.Helper()
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
		for index, id := range ids {
			event := testEvent(id, "op-ledgered-gate", time.Unix(1_700_000_600+int64(index), 0).UTC())
			event.TenantID = "tenant-a"
			data.LedgeredOutbox[store.EventKey(event.TenantID, event.EventID)] = event
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// startFlushRound runs one flush round in the background and returns a done
// channel that is closed when the round returns, plus a channel carrying any
// round error.
func startFlushRound(svc *Service, limit int) (done chan struct{}, failures chan error) {
	done, failures = make(chan struct{}), make(chan error, 1)
	go func() {
		defer close(done)
		if _, err := svc.FlushLedgeredOutbox(testCtx, limit); err != nil {
			failures <- err
		}
	}()
	return done, failures
}

// awaitChannel fails the test if signal is not closed soon, so a broken gate
// produces a named failure instead of a package timeout.
func awaitChannel(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(ingestBackstop):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// awaitRound waits for a flush round to finish and fails the test if it
// reported an error. The round closes done before the test reads the failure
// channel, so the error is collected non-blockingly rather than with a timer
// that would add three seconds to every green run.
func awaitRound(t *testing.T, done <-chan struct{}, failures <-chan error, what string) {
	t.Helper()
	awaitChannel(t, done, what)
	select {
	case err := <-failures:
		t.Fatalf("%s: %v", what, err)
	default:
	}
}

// TestFlushRoundDoesNotBlockConcurrentIngest is AC-1/FR-1: while a flush round
// is parked inside a publish, a concurrent Ingest's publication step returns
// immediately, with the round still open. On the pre-change tree the ingest
// blocks on the round-wide mutex and never returns within the backstop.
func TestFlushRoundDoesNotBlockConcurrentIngest(t *testing.T) {
	svc := testService(t, false)
	publisher := newGatedLedgeredPublisher("hl-flush-1")
	svc.Config.LedgeredPublisher = publisher
	seedLedgeredOutbox(t, svc, "hl-flush-1", "hl-flush-2")

	flushDone, _ := startFlushRound(svc, 100)
	awaitChannel(t, publisher.entered["hl-flush-1"], "the flush round to enter its first publish")

	ingestDone := make(chan error, 1)
	var receipt domain.EventReceipt
	go func() {
		var err error
		receipt, err = svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("hl-ingest", "op-ledgered-gate", time.Unix(1_700_000_610, 0).UTC()), domain.StatusLedgered)
		ingestDone <- err
	}()
	select {
	case err := <-ingestDone:
		if err != nil {
			t.Fatalf("ingest under a blocked flush round failed: %v", err)
		}
	case <-time.After(ingestBackstop):
		// The pre-change symptom: the round-wide gate is still held.
		t.Fatal("ingest did not return while a flush round was blocked inside its publish")
	}
	if receipt.EventID != "hl-ingest" || receipt.StreamID == "" {
		t.Fatalf("receipt=%+v, want a committed ledger receipt", receipt)
	}

	// Primary oracle: the flush round was still open at the instant the ingest
	// returned. A wait that merely finished early would still show the round
	// complete, so this is what actually pins "did not wait for the round".
	select {
	case <-flushDone:
		t.Fatal("flush round finished before the ingest returned: ingest waited on the round")
	default:
	}

	// The ingest published its own event (not the one the fake is blocking)
	// and acknowledged it, while the round is still parked in its publish:
	// it neither queued behind the round nor gave up its own publication.
	if counts := publisher.counts(); counts["hl-ingest"] != 1 {
		t.Fatalf("ingest publish count=%d, want 1 while the flush round holds its own key", counts["hl-ingest"])
	}
	if pending := pendingLedgeredEvents(t, svc); len(pending) != 2 {
		t.Fatalf("pending outbox=%v, want only the two rows the round still owns", pending)
	}
}

// TestDeferredLedgeredPublishDrainsOnNextFlush is AC-2 option (a): the deferred
// durable intent survives the ingest response and the next round publishes it,
// so at-least-once delivery holds and the outbox ends empty. The comparison is
// a set over the publisher's received ids, not a count.
//
// The deferral needs SAME-KEY contention, because that is the only property the
// gate has: a durable intent for hl-drain exists with no receipt, the round
// takes that key's gate and parks inside its publish, and the ingest of
// hl-drain then finds its own key owned. Two distinct event ids would map to
// distinct store.EventKey values and could never contend.
func TestDeferredLedgeredPublishDrainsOnNextFlush(t *testing.T) {
	svc := testService(t, false)
	publisher := newGatedLedgeredPublisher("hl-drain")
	svc.Config.LedgeredPublisher = publisher
	seedLedgeredOutbox(t, svc, "hl-drain")

	flushDone, flushErr := startFlushRound(svc, 100)
	awaitChannel(t, publisher.entered["hl-drain"], "the flush round to enter its publish")

	deferred := testEvent("hl-drain", "op-ledgered-gate", time.Unix(1_700_000_620, 0).UTC())
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, deferred, domain.StatusLedgered)
	if err != nil || receipt.EventID != deferred.EventID || receipt.StreamID == "" {
		t.Fatalf("receipt=%+v err=%v, want a committed receipt despite the deferral", receipt, err)
	}
	deferredKey := store.EventKey("tenant-a", deferred.EventID)
	if _, pending := pendingLedgeredEvents(t, svc)[deferredKey]; !pending {
		t.Fatal("deferred publication lost its durable intent: the outbox row must survive the ingest response")
	}
	if got := svc.LedgeredPublishDeferrals(); got != 1 {
		t.Fatalf("deferrals=%d, want 1", got)
	}
	if got := publisher.counts()[deferred.EventID]; got != 1 {
		t.Fatalf("the deferred event reached the publisher %d times on the request path, want 0", got)
	}

	// Release the round. It owns the key, so it delivers the publication the
	// request path deferred — exactly once overall, which is the whole point of
	// leaving the durable row in place.
	close(publisher.block["hl-drain"])
	awaitRound(t, flushDone, flushErr, "the first flush round to finish")

	if flushed, err := svc.FlushLedgeredOutbox(testCtx, 100); err != nil || flushed != 0 {
		t.Fatalf("post-drain flush=%d err=%v, want an already-drained outbox", flushed, err)
	}
	if pending := pendingLedgeredEvents(t, svc); len(pending) != 0 {
		t.Fatalf("pending after drain=%v, want an empty outbox", pending)
	}

	want := map[string]bool{"hl-drain": true}
	for id, count := range publisher.counts() {
		if !want[id] {
			t.Fatalf("publisher received unexpected event %q", id)
		}
		if count != 1 {
			t.Fatalf("event %q published %d times, want exactly once", id, count)
		}
		delete(want, id)
	}
	if len(want) != 0 {
		t.Fatalf("publisher never received %v: at-least-once delivery lost an event", want)
	}
}

// concurrencyTrackingPublisher records in-flight publishes per event id, so a
// test can detect two publishers inside the same pending event at once. It
// yields inside Publish so an overlap is actually attempted rather than merely
// possible.
type concurrencyTrackingPublisher struct {
	mu         sync.Mutex
	inflight   map[string]int
	peak       map[string]int
	peaks      int
	block      map[string]chan struct{}
	entered    map[string]chan struct{}
	firstBlock string
}

func newConcurrencyTrackingPublisher() *concurrencyTrackingPublisher {
	return &concurrencyTrackingPublisher{
		inflight: make(map[string]int),
		peak:     make(map[string]int),
		block:    make(map[string]chan struct{}),
		entered:  make(map[string]chan struct{}),
	}
}

func (p *concurrencyTrackingPublisher) Publish(ctx context.Context, event domain.Event) error {
	p.mu.Lock()
	p.inflight[event.EventID]++
	if p.inflight[event.EventID] > p.peak[event.EventID] {
		p.peak[event.EventID] = p.inflight[event.EventID]
	}
	if p.inflight[event.EventID] > 1 {
		p.peaks = p.inflight[event.EventID]
	}
	gate, gated := p.block[event.EventID]
	entered := p.entered[event.EventID]
	if gated && entered != nil {
		select {
		case <-entered:
		default:
			close(entered)
		}
	}
	p.mu.Unlock()
	if gated {
		select {
		case <-gate:
		case <-ctx.Done():
			p.leave(event.EventID)
			return ctx.Err()
		}
	}
	p.leave(event.EventID)
	return nil
}

func (p *concurrencyTrackingPublisher) leave(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inflight[id]--
}

// worstPeak is the highest simultaneous in-flight publish count seen for any
// single event id. The invariant is that this never exceeds 1.
func (p *concurrencyTrackingPublisher) worstPeak() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	worst := 0
	for _, count := range p.peak {
		if count > worst {
			worst = count
		}
	}
	return worst
}

func (p *concurrencyTrackingPublisher) delivered() map[string]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]bool, len(p.peak))
	for id := range p.peak {
		out[id] = true
	}
	return out
}

// TestNoConcurrentPublishPerPendingEvent is AC-3: the reason the gate exists is
// "never two publishers inside the same pending event", so the replacement
// invariant is asserted mechanically on SAME-KEY contention. Distinct event ids
// would map to distinct store.EventKey values and never contend at all, so
// each iteration parks the REQUEST path inside Publish(K) — with the durable
// row already written by that same ingest transaction — and then runs a flush
// round over the identical outbox. Stubbing tryAcquireLedgeredKey to always
// succeed makes the round publish K concurrently and this test fails.
func TestNoConcurrentPublishPerPendingEvent(t *testing.T) {
	svc := testService(t, false)
	publisher := newConcurrencyTrackingPublisher()
	svc.Config.LedgeredPublisher = publisher

	const keys = 8
	for index := range keys {
		id := fmt.Sprintf("ac3-%d", index)
		release := make(chan struct{})
		entered := make(chan struct{})
		publisher.mu.Lock()
		publisher.block[id] = release
		publisher.entered[id] = entered
		publisher.mu.Unlock()

		ingestDone := make(chan struct{})
		go func() {
			defer close(ingestDone)
			if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent(id, "op-ledgered-gate", time.Unix(1_700_000_700+int64(index), 0).UTC()), domain.StatusLedgered); err != nil {
				t.Errorf("ingest %s failed: %v", id, err)
			}
		}()
		awaitChannel(t, entered, "the request path to enter its publish")

		// The outbox now holds exactly this key, owned by the request path.
		if flushed, err := svc.FlushLedgeredOutbox(testCtx, 100); err != nil || flushed != 0 {
			t.Fatalf("flush over an owned key published %d (err=%v), want a skip", flushed, err)
		}
		close(release)
		awaitChannel(t, ingestDone, "the request path to finish")
	}

	if worst := publisher.worstPeak(); worst > 1 {
		t.Fatalf("peak concurrent publishes for one pending event=%d, want 1", worst)
	}
	// No starvation: every committed event reached the publisher, exactly once
	// through the request path (the round skipped each owned key).
	delivered := publisher.delivered()
	for index := range keys {
		id := fmt.Sprintf("ac3-%d", index)
		if !delivered[id] {
			t.Fatalf("event %q never reached the publisher", id)
		}
	}
	if pending := pendingLedgeredEvents(t, svc); len(pending) != 0 {
		t.Fatalf("pending outbox=%v, want every acknowledged event removed", pending)
	}
}

// TestConcurrentFlushRoundsPublishEachEventOnce pins the round-level mutex. Two
// overlapping rounds would read the same outbox snapshot and could each publish
// the same key from their own stale copy — the per-event gate cannot prevent
// that, because the gate is free again once the first round acknowledges the
// key. Round A parks inside the first publish while round B runs: with the
// round mutex B is a no-op, and without it B publishes the second key, which
// round A then publishes again.
func TestConcurrentFlushRoundsPublishEachEventOnce(t *testing.T) {
	svc := testService(t, false)
	// Sorted key order puts "aa-round-busy" first, so round A parks on it and
	// leaves "zz-round-free" for whoever gets there next.
	publisher := newGatedLedgeredPublisher("aa-round-busy")
	svc.Config.LedgeredPublisher = publisher
	seedLedgeredOutbox(t, svc, "aa-round-busy", "zz-round-free")

	firstDone, firstErr := startFlushRound(svc, 100)
	awaitChannel(t, publisher.entered["aa-round-busy"], "the first flush round to enter its publish")

	if flushed, err := svc.FlushLedgeredOutbox(testCtx, 100); err != nil || flushed != 0 {
		t.Fatalf("second concurrent round published %d (err=%v), want a no-op", flushed, err)
	}
	if got := publisher.counts()["zz-round-free"]; got != 0 {
		t.Fatalf("a second overlapping round published %d times while the first round was open", got)
	}

	close(publisher.block["aa-round-busy"])
	awaitRound(t, firstDone, firstErr, "the first flush round to finish")

	counts := publisher.counts()
	for _, id := range []string{"aa-round-busy", "zz-round-free"} {
		if counts[id] != 1 {
			t.Fatalf("event %q published %d times across two overlapping rounds, want exactly once", id, counts[id])
		}
	}
	if pending := pendingLedgeredEvents(t, svc); len(pending) != 0 {
		t.Fatalf("pending outbox=%v, want an empty outbox", pending)
	}
}

// TestLedgeredKeyTableDoesNotLeak is QA-F-05: the reference count is the only
// thing keeping the table bounded, and the failure path of a contended
// acquisition allocates a table entry too. Thousands of contended acquires on
// one key, mixed with successful ones, must leave no entry behind. This is the
// test that fails if releaseLedgeredKey ever drops its reference before
// unlocking.
func TestLedgeredKeyTableDoesNotLeak(t *testing.T) {
	svc := &Service{}
	const goroutines, rounds = 8, 200
	var wg sync.WaitGroup
	for worker := range goroutines {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := range rounds {
				key := fmt.Sprintf("tenant-a:leak-%d", round%(goroutines+1))
				gate, acquired := svc.tryAcquireLedgeredKey(key)
				if !acquired {
					if gate != nil {
						t.Errorf("a failed acquisition returned gate %p, want nil", gate)
					}
					continue
				}
				if worker%2 == 0 {
					// Hold the gate long enough for a peer to contend.
					time.Sleep(time.Microsecond)
				}
				svc.releaseLedgeredKey(key, gate)
			}
		}(worker)
	}
	wg.Wait()

	svc.ledgeredGatesMu.Lock()
	leftover := len(svc.ledgeredGates)
	svc.ledgeredGatesMu.Unlock()
	if leftover != 0 {
		t.Fatalf("ledgered gate table kept %d entries after all acquisitions finished, want 0", leftover)
	}
}

// countingBackend is a synchronized in-memory store.Backend that counts Load
// calls, i.e. Store.Read calls. LoadForUpdate is the write path and is counted
// separately, so the only cost the revalidation assertions observe is the read
// path; only Load honours the scripted failure.
type countingBackend struct {
	mu        sync.Mutex
	data      *store.Snapshot
	reads     int
	updates   int
	errAfter  int // 1-indexed read ordinal from which Load fails; 0 = never
	loadError error
}

func newCountingBackend() *countingBackend {
	return &countingBackend{data: store.NewSnapshot(), loadError: errors.New("snapshot unavailable")}
}

func (b *countingBackend) Load(ctx context.Context) (*store.Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reads++
	if b.errAfter > 0 && b.reads >= b.errAfter {
		return nil, b.loadError
	}
	return cloneTestSnapshot(b.data)
}

func (b *countingBackend) LoadForUpdate(ctx context.Context) (*store.Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.updates++
	return cloneTestSnapshot(b.data)
}

func (b *countingBackend) Save(ctx context.Context, data *store.Snapshot) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.updates++
	committed, err := cloneTestSnapshot(data)
	if err != nil {
		return err
	}
	b.data = committed
	return nil
}

func (b *countingBackend) readCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.reads
}

// TestFlushRevalidationReadsOncePerRound is QA-F-02: Store.Read materialises
// the whole control snapshot, so re-validating a key per read would turn one
// read per flush tick into up to ledgeredOutboxBatchSize under exactly the
// request-path load that makes revalidation necessary. A round that revalidates
// must pay ONE extra read, not one per key.
//
// The construction moves the request-path ack counter mid-round: the round
// parks inside the first publish, the test performs a request-path
// publication for a LATER key in the snapshot (which deletes that row), and
// only then is the round released. The round must notice and skip the stale
// key — but with a single shared revalidation read.
func TestFlushRevalidationReadsOncePerRound(t *testing.T) {
	backend := newCountingBackend()
	svc := newServiceOn(t, store.NewWithBackend(backend))
	seedTestDomain(t, svc)
	publisher := newGatedLedgeredPublisher("rv-0")
	svc.Config.LedgeredPublisher = publisher
	seedLedgeredOutbox(t, svc, "rv-0", "rv-1", "rv-2", "rv-3", "rv-4", "rv-5")

	flushDone, flushErr := startFlushRound(svc, 100)
	awaitChannel(t, publisher.entered["rv-0"], "the flush round to enter its publish")

	// A request-path acknowledgement lands while the round holds its snapshot.
	deferred := testEvent("rv-3", "op-ledgered-gate", time.Unix(1_700_000_800, 0).UTC())
	deferred.TenantID = "tenant-a"
	svc.publishLedgered(testCtx, deferred)
	if got := svc.LedgeredPublishDeferrals(); got != 0 {
		t.Fatalf("deferrals=%d, want 0: the request path owns no key the round holds", got)
	}

	close(publisher.block["rv-0"])
	awaitRound(t, flushDone, flushErr, "the revalidating flush round to finish")

	// Six keys: the round's snapshot read plus exactly one revalidation read.
	if reads := backend.readCount(); reads != 2 {
		t.Fatalf("flush round performed %d store reads, want 2 (one snapshot + one hoisted revalidation)", reads)
	}
	counts := publisher.counts()
	if counts["rv-3"] != 1 {
		t.Fatalf("rv-3 published %d times, want exactly once across the request path and the round", counts["rv-3"])
	}
	for index := range 6 {
		id := fmt.Sprintf("rv-%d", index)
		if id == "rv-3" {
			continue
		}
		if counts[id] != 1 {
			t.Fatalf("event %q published %d times, want 1", id, counts[id])
		}
	}
	if pending := pendingLedgeredEvents(t, svc); len(pending) != 0 {
		t.Fatalf("pending outbox=%v, want an empty outbox", pending)
	}
}

// TestFlushRevalidationFailsOpen is the other half of QA-F-02: revalidation is
// an optimisation, so an unreadable snapshot must publish rather than skip —
// at-least-once delivery must never depend on it. The snapshot read succeeds
// and only the revalidation read fails.
func TestFlushRevalidationFailsOpen(t *testing.T) {
	backend := newCountingBackend()
	svc := newServiceOn(t, store.NewWithBackend(backend))
	seedTestDomain(t, svc)
	publisher := newGatedLedgeredPublisher("fo-0")
	svc.Config.LedgeredPublisher = publisher
	seedLedgeredOutbox(t, svc, "fo-0", "fo-1", "fo-2")

	flushDone, flushErr := startFlushRound(svc, 100)
	awaitChannel(t, publisher.entered["fo-0"], "the flush round to enter its publish")

	svc.ledgeredRequestAcks.Add(1)
	backend.mu.Lock()
	backend.errAfter = backend.reads + 1 // the next Load is the revalidation read
	backend.mu.Unlock()
	close(publisher.block["fo-0"])
	awaitRound(t, flushDone, flushErr, "the fail-open flush round to finish")
	backend.mu.Lock()
	backend.errAfter = 0 // the assertion below reads the outbox through the same seam
	backend.mu.Unlock()

	counts := publisher.counts()
	for index := range 3 {
		if id := fmt.Sprintf("fo-%d", index); counts[id] != 1 {
			t.Fatalf("event %q published %d times after a failed revalidation read, want 1 (fail open)", id, counts[id])
		}
	}
	if pending := pendingLedgeredEvents(t, svc); len(pending) != 0 {
		t.Fatalf("pending outbox=%v, want an empty outbox", pending)
	}
}

// TestFlushRoundSkipsQuietlyWithoutStarvation is the documented contract of a
// round whose every key is skipped: it returns 0, nil and leaves the rows for
// the next round instead of reporting a failure the flush loop would log as
// one. The gate is held by the request path — an ingest parked inside its own
// publish — which is the only situation a real round can skip in.
func TestFlushRoundSkipsQuietlyWithoutStarvation(t *testing.T) {
	svc := testService(t, false)
	publisher := newGatedLedgeredPublisher("sk-request")
	svc.Config.LedgeredPublisher = publisher

	// The ingest commits, writes its durable intent and then parks inside the
	// publish, so it owns sk-request for as long as the test wants.
	ingestDone := make(chan struct{})
	go func() {
		defer close(ingestDone)
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("sk-request", "op-ledgered-gate", time.Unix(1_700_000_900, 0).UTC()), domain.StatusLedgered); err != nil {
			t.Errorf("ingest failed: %v", err)
		}
	}()
	awaitChannel(t, publisher.entered["sk-request"], "the request path to enter its publish")

	if flushed, err := svc.FlushLedgeredOutbox(testCtx, 100); err != nil || flushed != 0 {
		t.Fatalf("round over a fully owned outbox published %d (err=%v), want a quiet skip", flushed, err)
	}
	if pending := pendingLedgeredEvents(t, svc); len(pending) != 1 {
		t.Fatalf("pending outbox=%v, want the owned row retained for the next round", pending)
	}

	close(publisher.block["sk-request"])
	awaitChannel(t, ingestDone, "the request path to finish")
	if flushed, err := svc.FlushLedgeredOutbox(testCtx, 100); err != nil || flushed != 0 {
		t.Fatalf("post-ingest flush=%d err=%v, want the request path to have acknowledged the row", flushed, err)
	}
	if pending := pendingLedgeredEvents(t, svc); len(pending) != 0 {
		t.Fatalf("pending outbox=%v, want an empty outbox", pending)
	}
	if counts := publisher.counts(); counts["sk-request"] != 1 {
		t.Fatalf("event published %d times, want exactly once", counts["sk-request"])
	}
}
