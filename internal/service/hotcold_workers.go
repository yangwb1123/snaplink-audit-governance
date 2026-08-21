package service

import (
	"context"
	"sort"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// sealPendingTenant keeps the low-volume worker on the tenant-scoped path.
// ColdFirst is deliberate: clearing pending hashes from the hot stream is
// safe only after the immutable segment and checkpoint are durable.
func (s *Service) sealPendingTenant(ctx context.Context, tenantID string) error {
	now := s.Now()
	return s.Store.UpdateTenant(tenantID, store.ColdFirst, func(view *store.TenantView) error {
		keys := make([]string, 0, len(view.Hot.Streams))
		for key, stream := range view.Hot.Streams {
			if stream.TenantID == tenantID && len(stream.PendingHashes) > 0 {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		for _, key := range keys {
			stream := view.Hot.Streams[key]
			segment, checkpoint, err := s.sealSegment(ctx, stream, now)
			if err != nil {
				return err
			}
			view.Ledger.AppendSegment(segment)
			view.Ledger.AppendCheckpoint(checkpoint)
			stream.PendingHashes = nil
			stream.PendingEvents = nil
			stream.PendingPrevHash = ""
			view.Hot.Streams[key] = stream
		}
		return nil
	})
}

type archiveTenantBatch struct {
	events      []domain.Event
	evictedKeys []string
	segments    []domain.Segment
	deadLetters map[string]domain.DeadLetter
}

func (s *Service) collectArchiveTenantBatch(tenantID string) (archiveTenantBatch, error) {
	batch := archiveTenantBatch{deadLetters: map[string]domain.DeadLetter{}}
	err := s.Store.ReadTenant(tenantID, func(view *store.TenantView) error {
		for key, event := range view.Hot.Events {
			if event.TenantID != tenantID {
				continue
			}
			if _, dead := view.Global.DeadLetters[key]; dead {
				continue
			}
			receipt, ok := view.Ledger.Receipt(key)
			if ok && receipt.Status == domain.StatusArchived {
				batch.evictedKeys = append(batch.evictedKeys, key)
				continue
			}
			batch.events = append(batch.events, event)
		}
		for _, record := range view.Ledger.Records() {
			if record.RecordType != store.LedgerSegment || record.Segment == nil {
				continue
			}
			key, _ := segmentDeadLetterKey(*record.Segment)
			if _, dead := view.Global.DeadLetters[key]; !dead {
				batch.segments = append(batch.segments, *record.Segment)
			}
		}
		return nil
	})
	if err != nil {
		return archiveTenantBatch{}, err
	}
	sort.Slice(batch.events, func(i, j int) bool { return batch.events[i].EventID < batch.events[j].EventID })
	sort.Strings(batch.evictedKeys)
	sort.Slice(batch.segments, func(i, j int) bool {
		left, right := batch.segments[i], batch.segments[j]
		if left.StreamID != right.StreamID {
			return left.StreamID < right.StreamID
		}
		return left.FirstSequence < right.FirstSequence
	})
	return batch, nil
}

func (s *Service) archivePendingTenant(ctx context.Context, tenantID string) (int, error) {
	batch, err := s.collectArchiveTenantBatch(tenantID)
	if err != nil {
		return 0, err
	}
	now := s.Now()
	for _, segment := range batch.segments {
		if err := s.archiveSegment(ctx, segment); err != nil {
			if !isPermanentArchiveError(err) {
				return 0, err
			}
			key, eventID := segmentDeadLetterKey(segment)
			batch.deadLetters[key] = domain.DeadLetter{TenantID: tenantID, EventID: eventID, StreamID: segment.StreamID, Sequence: segment.FirstSequence, Reason: archiveErrorReason(err), ErrorMessage: boundedErrorMessage(err), At: now}
		}
	}
	archivedEvents := make([]domain.Event, 0, len(batch.events))
	for _, event := range batch.events {
		if err := s.archiveEvent(ctx, event); err != nil {
			if !isPermanentArchiveError(err) {
				return 0, err
			}
			key := store.EventKey(tenantID, event.EventID)
			batch.deadLetters[key] = domain.DeadLetter{TenantID: tenantID, EventID: event.EventID, StreamID: event.StreamID, Sequence: event.Sequence, Reason: archiveErrorReason(err), ErrorMessage: boundedErrorMessage(err), At: now}
			continue
		}
		archivedEvents = append(archivedEvents, event)
	}
	if len(archivedEvents) == 0 && len(batch.deadLetters) == 0 && len(batch.evictedKeys) == 0 {
		return 0, nil
	}
	err = s.Store.UpdateTenant(tenantID, store.ColdFirst, func(view *store.TenantView) error {
		for _, key := range batch.evictedKeys {
			if receipt, ok := view.Ledger.Receipt(key); ok && receipt.Status == domain.StatusArchived {
				delete(view.Hot.Events, key)
			}
		}
		for _, event := range archivedEvents {
			key := store.EventKey(tenantID, event.EventID)
			receipt, ok := view.Ledger.Receipt(key)
			if !ok {
				return domain.ErrNotFound
			}
			receipt.Status = domain.StatusArchived
			receipt.IndexedAt = now
			receipt.ArchivedAt = now
			view.Ledger.SetReceipt(receipt)
			delete(view.Hot.Events, key)
		}
		for key, dead := range batch.deadLetters {
			view.Global.DeadLetters[key] = dead
			if receipt, ok := view.Ledger.Receipt(key); ok {
				receipt.ErrorCode = "archive_dead_letter"
				receipt.ErrorMessage = dead.ErrorMessage
				view.Ledger.SetReceipt(receipt)
			}
		}
		view.Global.ArchiveConflictFailures[tenantID] = 0
		return nil
	})
	if err != nil {
		return 0, err
	}
	return len(archivedEvents), nil
}
