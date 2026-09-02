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
func (s *Service) publishLedgered(ctx context.Context, event domain.Event) {
	if s.Config.LedgeredPublisher == nil {
		return
	}
	s.ledgeredPublishMu.Lock()
	defer s.ledgeredPublishMu.Unlock()
	if !s.publishLedgeredNow(ctx, event) {
		return
	}
	if err := s.ackLedgered(event); err != nil {
		s.logf("ledgered outbox acknowledgement failed event_id=%s error=%v", event.EventID, err)
	}
}

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

func (s *Service) ackLedgered(event domain.Event) error {
	key := store.EventKey(event.TenantID, event.EventID)
	return s.Store.Update(func(data *store.Snapshot) error {
		delete(data.LedgeredOutbox, key)
		return nil
	})
}

// FlushLedgeredOutbox retries durable post-ledger publications in stable key
// order. It stops at the first broker failure so one outage cannot turn a
// timer tick into a long sequence of blocked five-second calls. The method is
// safe to call from a restart recovery loop and is a no-op when publication
// is not configured.
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
	s.ledgeredPublishMu.Lock()
	defer s.ledgeredPublishMu.Unlock()
	keys := make([]string, 0, limit)
	events := make(map[string]domain.Event, limit)
	if err := s.Store.Read(func(data *store.Snapshot) error {
		for key, event := range data.LedgeredOutbox {
			keys = append(keys, key)
			events[key] = event
		}
		return nil
	}); err != nil {
		return 0, err
	}
	sort.Strings(keys)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	published := 0
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return published, err
		}
		if !s.publishLedgeredNow(ctx, events[key]) {
			// The publication may fail because the bounded child timeout
			// expired, in which case the caller can continue its next
			// scheduled pass. A caller cancellation is different: surface it
			// while retaining the unacknowledged outbox record.
			if err := ctx.Err(); err != nil {
				return published, err
			}
			break
		}
		if err := s.ackLedgered(events[key]); err != nil {
			return published, err
		}
		published++
	}
	return published, nil
}
