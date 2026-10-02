package service

import (
	"context"
	"sort"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// ledgeredPublishTimeout prevents a broker outage from holding the ingest
// response open indefinitely after the ledger transaction has committed.
const ledgeredPublishTimeout = 5 * time.Second

const ledgeredOutboxBatchSize = 100

// LedgeredPublisher delivers a chain-linked event after it has been durably
// committed. Implementations must tolerate at-least-once delivery by event ID.
type LedgeredPublisher interface {
	Publish(context.Context, domain.Event) error
}

func (s *Service) logf(format string, args ...any) {
	if s.Config.Logf != nil {
		s.Config.Logf(format, args...)
	}
}

// publishLedgered is deliberately best effort: the immutable ledger is the
// source of truth, while the ledgered topic is a rebuildable projection feed.
// The event is first retained in Snapshot.LedgeredOutbox by the ingest
// transaction. A successful immediate delivery removes that durable retry
// record; a broker failure leaves it for FlushLedgeredOutbox after restart.
//
// The gate is per pending event and is never waited on (see ledgered_gate.go):
// this path must not queue behind another publisher, because the ledger commit
// has already answered the caller. It adds no store call of its own — the
// durable intent is written by the ingest transaction, not here.
func (s *Service) publishLedgered(ctx context.Context, event domain.Event) {
	if s.Config.LedgeredPublisher == nil {
		return
	}
	key := store.EventKey(event.TenantID, event.EventID)
	gate, acquired := s.tryAcquireLedgeredKey(key)
	if !acquired {
		// Another publisher owns this exact event right now, so its durable
		// intent is still owed by whoever holds the gate: leaving the row in
		// place cannot lose the event, and that publisher's acknowledgement
		// (or the next flush round) removes it. Nothing about the ingest
		// answer depends on this publication.
		s.ledgeredPublishDeferrals.Add(1)
		s.logf("ledgered publish deferred event_id=%s", event.EventID)
		return
	}
	defer s.releaseLedgeredKey(key, gate)
	if !s.publishLedgeredNow(ctx, event) {
		return
	}
	if err := s.ackLedgered(ctx, event); err != nil {
		s.logf("ledgered outbox acknowledgement failed event_id=%s error=%v", event.EventID, err)
		return
	}
	s.ledgeredRequestAcks.Add(1)
}

// LedgeredPublishDeferrals reports how many post-commit publications this
// process declined because another publisher already owned the event's key
// gate. It is exposed as audit_ledgered_publish_deferred_total.
//
// With a consistent durable outbox the request path does not contend: the row
// is written by the ingest transaction, and a retry of the same event_id is
// answered as a duplicate before any publication is attempted. A non-zero
// value is therefore not routine backpressure — it is evidence that the
// invariant this gate defends was breached (two commits mapping onto one
// outbox key, or a lost intent re-created while a flush was publishing it)
// and that at-least-once delivery absorbed it. The deferred publication is
// still owed: the durable row survives and the next round drains it.
func (s *Service) LedgeredPublishDeferrals() uint64 { return s.ledgeredPublishDeferrals.Load() }

func (s *Service) publishLedgeredNow(ctx context.Context, event domain.Event) bool {
	published, err := domain.CloneEvent(event)
	if err != nil {
		s.logf("ledgered publish clone failed event_id=%s sequence=%d error=%v", event.EventID, event.Sequence, err)
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	publishCtx, cancel := context.WithTimeout(ctx, ledgeredPublishTimeout)
	defer cancel()
	if err := s.Config.LedgeredPublisher.Publish(publishCtx, published); err != nil {
		s.logf("ledgered publish failed event_id=%s sequence=%d error=%v", event.EventID, event.Sequence, err)
		return false
	}
	s.logf("ledgered published event_id=%s sequence=%d", event.EventID, event.Sequence)
	return true
}

func (s *Service) ackLedgered(ctx context.Context, event domain.Event) error {
	key := store.EventKey(event.TenantID, event.EventID)
	return s.Store.Update(ctx, func(data *store.Snapshot) error {
		delete(data.LedgeredOutbox, key)
		return nil
	})
}

// ledgeredKeyOutcome separates "this round deliberately did nothing for this
// key" from "the broker refused it", because only the second ends the round.
type ledgeredKeyOutcome int

const (
	// ledgeredKeySkipped: the request path already owns this key, or the row
	// was acknowledged after this round's snapshot read.
	ledgeredKeySkipped ledgeredKeyOutcome = iota
	// ledgeredKeyPublished: published and acknowledged here.
	ledgeredKeyPublished
	// ledgeredKeyFailed: the broker refused it, or the acknowledgement could
	// not be persisted. The durable row survives.
	ledgeredKeyFailed
)

// pendingLedgeredOutbox returns the durable intents in stable key order.
func (s *Service) pendingLedgeredOutbox(ctx context.Context) ([]string, map[string]domain.Event, error) {
	keys := make([]string, 0, ledgeredOutboxBatchSize)
	events := make(map[string]domain.Event, ledgeredOutboxBatchSize)
	if err := s.Store.Read(ctx, func(data *store.Snapshot) error {
		for key, event := range data.LedgeredOutbox {
			keys = append(keys, key)
			events[key] = event
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	sort.Strings(keys)
	return keys, events, nil
}

// ledgeredRevalidator answers "is this key still owed a publication?" for one
// flush round, at the cost of at most ONE extra store read for the whole
// round, taken lazily: a round that saw no request-path acknowledgement never
// reads again. Store.Read materialises the entire control snapshot, so a
// per-key read would turn one read per tick into up to ledgeredOutboxBatchSize
// under exactly the request-path load that makes revalidation necessary.
//
// A failed read fails OPEN: at-least-once delivery must never depend on this
// optimisation, so an unreadable snapshot is published, not skipped.
type ledgeredRevalidator struct {
	svc     *Service
	pending map[string]struct{}
	read    bool
	failed  bool
}

func (r *ledgeredRevalidator) stillPending(ctx context.Context, key string) bool {
	if !r.read {
		r.read = true
		if err := r.svc.Store.Read(ctx, func(data *store.Snapshot) error {
			r.pending = make(map[string]struct{}, len(data.LedgeredOutbox))
			for pending := range data.LedgeredOutbox {
				r.pending[pending] = struct{}{}
			}
			return nil
		}); err != nil {
			r.failed = true
			r.svc.logf("ledgered outbox revalidation failed error=%v", err)
		}
	}
	if r.failed {
		return true
	}
	_, ok := r.pending[key]
	return ok
}

// flushLedgeredKey publishes and acknowledges one pending event and reports
// what the round should do next.
func (s *Service) flushLedgeredKey(ctx context.Context, key string, event domain.Event, acksAtSnapshot uint64, revalidate *ledgeredRevalidator) (ledgeredKeyOutcome, error) {
	gate, acquired := s.tryAcquireLedgeredKey(key)
	if !acquired {
		// The request path owns this key: it is publishing (or has already
		// published) this exact event and deletes the durable row on success.
		// A row it fails to publish is retried by the next round, so skipping
		// costs nothing — and blocking here would make the flusher wait on the
		// request path, the mirror image of the defect this gate removes.
		return ledgeredKeySkipped, nil
	}
	defer s.releaseLedgeredKey(key, gate)
	// Only a request-path acknowledgement can make a key in this round's
	// snapshot stale, so the counter decides whether revalidation is needed
	// at all.
	if s.ledgeredRequestAcks.Load() != acksAtSnapshot && !revalidate.stillPending(ctx, key) {
		return ledgeredKeySkipped, nil
	}
	if !s.publishLedgeredNow(ctx, event) {
		// The publication may fail because the bounded child timeout expired,
		// in which case the caller can continue its next scheduled pass. A
		// caller cancellation is different: surface it while retaining the
		// unacknowledged outbox record.
		if err := ctx.Err(); err != nil {
			return ledgeredKeyFailed, err
		}
		return ledgeredKeyFailed, nil
	}
	if err := s.ackLedgered(ctx, event); err != nil {
		return ledgeredKeyFailed, err
	}
	return ledgeredKeyPublished, nil
}

// FlushLedgeredOutbox retries durable post-ledger publications in stable key
// order. It stops at the first broker failure so one outage cannot turn a
// timer tick into a long sequence of blocked five-second calls.
//
// The returned count is what THIS round delivered: a key owned by the request
// path, or already acknowledged since the round's snapshot read, is skipped
// and not counted, so a round can legitimately return 0 against a non-empty
// outbox. Skips are no-ops, not failures — a fully skipped round needs no
// backoff and is not an error.
//
// Only one round runs at a time per process; a call that cannot take the round
// mutex returns immediately rather than publishing from a snapshot another
// round is already draining. The round mutex is never shared with the request
// path, which is why a long round no longer delays an ingest.
//
// The method is safe to call from a restart recovery loop and is a no-op when
// publication is not configured.
//
// Known limitation: the caller retries on a fixed tick with no backoff or
// jitter, so N replicas retry a dead broker in lockstep. Left as-is here
// because the retry cadence belongs to the flush loop's owner
// (cmd/audit-api/ledgered.go).
func (s *Service) FlushLedgeredOutbox(ctx context.Context, limit int) (int, error) {
	if s.Config.LedgeredPublisher == nil || s.Store == nil {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if limit <= 0 || limit > ledgeredOutboxBatchSize {
		limit = ledgeredOutboxBatchSize
	}
	if !s.ledgeredFlushMu.TryLock() {
		return 0, nil
	}
	defer s.ledgeredFlushMu.Unlock()
	// Sampled before the snapshot: only a request-path acknowledgement can
	// make a key in this snapshot stale, so this is what decides whether any
	// key needs revalidating at all.
	acksAtSnapshot := s.ledgeredRequestAcks.Load()
	keys, events, err := s.pendingLedgeredOutbox(ctx)
	if err != nil {
		return 0, err
	}
	if len(keys) > limit {
		keys = keys[:limit]
	}
	revalidate := &ledgeredRevalidator{svc: s}
	published := 0
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return published, err
		}
		outcome, err := s.flushLedgeredKey(ctx, key, events[key], acksAtSnapshot, revalidate)
		if err != nil {
			return published, err
		}
		if outcome == ledgeredKeyFailed {
			return published, nil
		}
		if outcome == ledgeredKeyPublished {
			published++
		}
	}
	return published, nil
}
