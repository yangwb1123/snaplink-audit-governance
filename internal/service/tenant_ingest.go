package service

import (
	"context"
	"fmt"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// ingestTenant writes the event and its receipt/segment/checkpoint metadata
// through the tenant-scoped split API. The closure never scans another
// tenant or materializes the cold ledger; receipt idempotency is served by
// TenantLedger's in-memory index.
func (s *Service) ingestTenant(ctx context.Context, tenantHint string, principal domain.IngestPrincipal, tenantID string, event domain.Event, inputDigest string) (domain.Event, domain.EventReceipt, []domain.Segment, error) {
	var receipt domain.EventReceipt
	var sealed []domain.Segment
	err := s.Store.UpdateTenant(tenantID, store.HotFirst, func(view *store.TenantView) error {
		sealed = sealed[:0]
		if view.Global == nil {
			return fmt.Errorf("%w: tenant control view unavailable", domain.ErrInvalid)
		}
		commitTenant, accessErr := resolveIngestTenantFromData(view.Global, tenantHint, principal.ClientID, event.SourceSystem)
		if accessErr != nil || commitTenant != tenantID {
			return sourceAccessError()
		}
		key := store.EventKey(tenantID, event.EventID)
		if existing, found, existingErr := s.checkExistingTenantIngest(ctx, view, tenantID, event.EventID, inputDigest); found {
			receipt = existing
			return existingErr
		}
		if idempotencyKeyReusedTenant(view, tenantID, key, event.IdempotencyKey) {
			// The colliding (rejected) caller observes a freshly built,
			// self-describing conflict receipt. The pre-existing owner event's
			// receipt is never read or persisted here (R1/R3), so a successful
			// ingest cannot be corrupted by a later idempotency-key collision.
			receipt = conflictReceipt(tenantID, event)
			return fmt.Errorf("%w: idempotency_key is already associated with another event", domain.ErrConflict)
		}
		now := event.ReceivedAt
		receipt = domain.EventReceipt{EventID: event.EventID, TenantID: tenantID, IdempotencyKey: event.IdempotencyKey, Status: domain.StatusLedgered, AcceptedAt: now, LedgeredAt: now}
		streamID := event.Stream()
		streamKey := store.StreamKey(tenantID, streamID)
		stream := view.Hot.Streams[streamKey]
		if stream.StreamID == "" {
			stream = store.StreamState{TenantID: tenantID, StreamID: streamID, NextSequence: 1}
		}
		event.StreamID = streamID
		event.Sequence = stream.NextSequence
		event.PrevHash = stream.HeadHash
		var err error
		event.Hash, err = s.eventHash(event)
		if err != nil {
			return err
		}
		stream.NextSequence++
		stream.HeadHash = event.Hash
		stream.PendingHashes = append(stream.PendingHashes, event.Hash)
		stream.PendingEvents = append(stream.PendingEvents, key)
		if len(stream.PendingHashes) == 1 {
			stream.PendingPrevHash = event.PrevHash
		}
		view.Hot.Streams[streamKey] = stream
		view.Hot.Events[key] = event
		receipt.StreamID = event.StreamID
		receipt.Sequence = event.Sequence
		receipt.Hash = event.Hash
		view.Ledger.SetReceipt(receipt)
		if len(stream.PendingHashes) < s.Config.SegmentSize {
			return nil
		}
		segment, checkpoint, sealErr := s.sealSegment(ctx, stream, now)
		if sealErr != nil {
			return sealErr
		}
		sealed = append(sealed, segment)
		view.Ledger.AppendSegment(segment)
		view.Ledger.AppendCheckpoint(checkpoint)
		stream.PendingHashes = nil
		stream.PendingEvents = nil
		stream.PendingPrevHash = ""
		view.Hot.Streams[streamKey] = stream
		return nil
	})
	if err != nil {
		return event, receipt, sealed, err
	}
	if receipt.Duplicate {
		return event, receipt, sealed, nil
	}
	if s.Config.LedgeredPublisher != nil {
		if err := s.Store.UpdateControl(func(data *store.Snapshot) error {
			if data.LedgeredOutbox == nil {
				data.LedgeredOutbox = map[string]domain.Event{}
			}
			data.LedgeredOutbox[store.EventKey(tenantID, event.EventID)] = event
			return nil
		}); err != nil {
			return event, receipt, sealed, err
		}
	}
	return event, receipt, sealed, nil
}

func idempotencyKeyReusedTenant(view *store.TenantView, tenantID, eventKey, idempotencyKey string) bool {
	if receipt, ok := view.Ledger.FindReceiptByIdempotencyKey(idempotencyKey); ok {
		key := store.EventKey(tenantID, receipt.EventID)
		if key != eventKey {
			return true
		}
	}
	for key, event := range view.Hot.Events {
		if key != eventKey && event.TenantID == tenantID && event.IdempotencyKey == idempotencyKey {
			if _, ok := view.Ledger.Receipt(key); ok {
				return true
			}
		}
	}
	return false
}

func (s *Service) checkExistingTenantIngest(ctx context.Context, view *store.TenantView, tenantID, eventID, inputDigest string) (domain.EventReceipt, bool, error) {
	key := store.EventKey(tenantID, eventID)
	stored, hasReceipt := view.Ledger.Receipt(key)
	existing, hot := view.Hot.Events[key]
	if !hot {
		if !hasReceipt {
			return domain.EventReceipt{}, false, nil
		}
		if stored.Status != domain.StatusArchived {
			return domain.EventReceipt{}, true, fmt.Errorf("event %s has receipt but no hot payload", eventID)
		}
		var err error
		existing, err = s.archivedEventContext(ctx, stored)
		if err != nil {
			return domain.EventReceipt{}, true, err
		}
	}
	if !hasReceipt {
		stored = receiptFromEvent(existing)
	}
	digest, err := s.reconstructAndDerive(existing, view.Global.Schemas)
	if err != nil {
		digest, err = domain.EventDigest(existing)
		if err != nil {
			return domain.EventReceipt{}, true, err
		}
	}
	if digest == inputDigest {
		// Duplicate is response metadata, not durable event state. Do not
		// rewrite the receipt on an idempotent retry.
		stored.Duplicate = true
		return stored, true, nil
	}
	stored.Conflict = true
	stored.ErrorCode = "event_id_content_conflict"
	stored.ErrorMessage = "event_id already exists with different canonical content"
	view.Ledger.SetReceipt(stored)
	return stored, true, fmt.Errorf("%w: event_id content differs", domain.ErrConflict)
}

func receiptFromEvent(event domain.Event) domain.EventReceipt {
	return domain.EventReceipt{
		EventID: event.EventID, TenantID: event.TenantID, IdempotencyKey: event.IdempotencyKey,
		Status: domain.StatusLedgered, AcceptedAt: event.ReceivedAt, LedgeredAt: event.ReceivedAt,
		StreamID: event.StreamID, Sequence: event.Sequence, Hash: event.Hash,
	}
}

func (s *Service) transitionTenantReceipt(tenantID, eventID, status string, evict bool) (domain.EventReceipt, error) {
	var result domain.EventReceipt
	now := s.Now()
	order := store.HotFirst
	if evict {
		order = store.ColdFirst
	}
	err := s.Store.UpdateTenant(tenantID, order, func(view *store.TenantView) error {
		key := store.EventKey(tenantID, eventID)
		receipt, ok := view.Ledger.Receipt(key)
		if !ok {
			event, hot := view.Hot.Events[key]
			if !hot {
				return domain.ErrNotFound
			}
			receipt = receiptFromEvent(event)
		}
		receipt.Status = status
		receipt.IndexedAt = now
		if evict {
			receipt.ArchivedAt = now
			delete(view.Hot.Events, key)
		}
		view.Ledger.SetReceipt(receipt)
		result = receipt
		return nil
	})
	return result, err
}

func (s *Service) transitionReceipt(tenantID, eventID, status string, evict bool) (domain.EventReceipt, error) {
	if s.Store.HotCold() {
		return s.transitionTenantReceipt(tenantID, eventID, status, evict)
	}
	var result domain.EventReceipt
	now := s.Now()
	err := s.Store.Update(func(data *store.Snapshot) error {
		key := store.EventKey(tenantID, eventID)
		receipt, ok := data.Receipts[key]
		if !ok {
			return domain.ErrNotFound
		}
		receipt.Status = status
		receipt.IndexedAt = now
		if evict {
			receipt.ArchivedAt = now
			delete(data.Events, key)
		}
		data.Receipts[key] = receipt
		result = receipt
		return nil
	})
	return result, err
}
