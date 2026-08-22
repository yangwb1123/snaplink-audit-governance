package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// archiveReadTimeout bounds every WORM read used by a fallback surface. A
// missing or black-holed archive must fail the read, not retain the store
// read lock or HTTP handler forever. The package variable is a narrow test
// seam so cancellation behavior can be tested without waiting 30 seconds.
var archiveReadTimeout = 30 * time.Second

// eventArchiveKey is the single archive-key format used by both writers and
// readers. The encoded components are injective, so an archived event cannot
// collide with another tenant or stream through lossy path sanitization.
func eventArchiveKey(tenantID, streamID string, sequence int64, eventID string) string {
	return fmt.Sprintf("events/%s/%s/%020d-%s.json", safeName(tenantID), safeName(streamID), sequence, safeName(eventID))
}

// archivedEvent loads and verifies an archived event. A receipt is the
// retained linkage anchor after hot eviction; any missing, malformed, or
// mismatched object is an integrity error rather than a silent 404.
func (s *Service) archivedEvent(receipt domain.EventReceipt) (domain.Event, error) {
	return s.archivedEventContext(context.Background(), receipt)
}

func (s *Service) archivedEventContext(ctx context.Context, receipt domain.EventReceipt) (domain.Event, error) {
	if !archive.Configured(s.Config.Archive) {
		return domain.Event{}, fmt.Errorf("archived event %s: archive is not configured", receipt.EventID)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	readCtx, cancel := context.WithTimeout(ctx, archiveReadTimeout)
	defer cancel()
	raw, err := s.Config.Archive.Get(readCtx, eventArchiveKey(receipt.TenantID, receipt.StreamID, receipt.Sequence, receipt.EventID))
	if err != nil {
		return domain.Event{}, fmt.Errorf("archived event %s: %w", receipt.EventID, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var event domain.Event
	if err := decoder.Decode(&event); err != nil {
		return domain.Event{}, fmt.Errorf("archived event %s: decode: %w", receipt.EventID, err)
	}
	if event.EventID != receipt.EventID || event.TenantID != receipt.TenantID ||
		event.StreamID != receipt.StreamID || event.Sequence != receipt.Sequence {
		return domain.Event{}, fmt.Errorf("archived event %s: identity mismatch", receipt.EventID)
	}
	hash, err := s.eventHash(event)
	if err != nil {
		return domain.Event{}, fmt.Errorf("archived event %s: hash derivation: %w", receipt.EventID, err)
	}
	if receipt.Hash == "" || hash != receipt.Hash {
		return domain.Event{}, fmt.Errorf("archived event %s: hash does not match retained receipt", receipt.EventID)
	}
	return event, nil
}

// eventFromSnapshot resolves a hot event first and then performs a verified
// archive lookup for an evicted archived receipt. It never fetches an object
// for an indexed or otherwise incomplete receipt.
func (s *Service) eventFromSnapshot(ctx context.Context, data *store.Snapshot, tenantID, eventID string) (domain.Event, error) {
	key := store.EventKey(tenantID, eventID)
	if event, ok := data.Events[key]; ok {
		return event, nil
	}
	receipt, ok := data.Receipts[key]
	if !ok || receipt.Status != domain.StatusArchived {
		return domain.Event{}, domain.ErrNotFound
	}
	return s.archivedEventContext(ctx, receipt)
}

// eventsFromSnapshot returns hot events plus verified archived events whose
// receipts no longer have a hot payload. Callers provide the predicate so
// timeline, export and legal-hold reads share one fallback implementation.
func (s *Service) eventsFromSnapshot(ctx context.Context, data *store.Snapshot, tenantID string, predicate func(domain.Event) bool) ([]domain.Event, error) {
	events := make([]domain.Event, 0)
	for _, event := range data.Events {
		if (tenantID == "" || event.TenantID == tenantID) && predicate(event) {
			events = append(events, event)
		}
	}
	keys := make([]string, 0, len(data.Receipts))
	for key, receipt := range data.Receipts {
		if (tenantID == "" || receipt.TenantID == tenantID) && receipt.Status == domain.StatusArchived {
			if _, hot := data.Events[key]; !hot {
				keys = append(keys, key)
			}
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		receipt := data.Receipts[key]
		event, err := s.archivedEventContext(ctx, receipt)
		if err != nil {
			return nil, err
		}
		if predicate(event) {
			events = append(events, event)
		}
	}
	return events, nil
}

// idempotencyConflictKey checks both hot events and retained cold receipts.
// Old snapshots may not have the additive receipt field, so their hot event
// remains the compatibility fallback.
func idempotencyConflictKey(data *store.Snapshot, tenantID, eventKey, idempotencyKey string) (string, bool) {
	for key, event := range data.Events {
		if key != eventKey && event.TenantID == tenantID && event.IdempotencyKey == idempotencyKey {
			return key, true
		}
	}
	for key, receipt := range data.Receipts {
		if key != eventKey && receipt.TenantID == tenantID && receipt.IdempotencyKey != "" && receipt.IdempotencyKey == idempotencyKey {
			return key, true
		}
	}
	return "", false
}

// checkExistingIngest handles event-id idempotency for both retained and
// evicted events. found=false means the caller may allocate a new sequence.
func (s *Service) checkExistingIngest(ctx context.Context, data *store.Snapshot, tenantID, eventID string, inputDigest string) (receipt domain.EventReceipt, found bool, err error) {
	key := store.EventKey(tenantID, eventID)
	existing, hot := data.Events[key]
	if !hot {
		stored, ok := data.Receipts[key]
		if !ok || stored.Status != domain.StatusArchived {
			return domain.EventReceipt{}, false, nil
		}
		existing, err = s.archivedEventContext(ctx, stored)
		if err != nil {
			return domain.EventReceipt{}, true, err
		}
	}
	existingDigest, digestErr := s.reconstructAndDerive(existing, data.Schemas)
	if digestErr != nil {
		existingDigest, digestErr = domain.EventDigest(existing)
		if digestErr != nil {
			return domain.EventReceipt{}, true, digestErr
		}
	}
	receipt = data.Receipts[key]
	if existingDigest == inputDigest {
		receipt.Duplicate = true
		data.Receipts[key] = receipt
		return receipt, true, nil
	}
	receipt.Conflict = true
	receipt.ErrorCode = "event_id_content_conflict"
	receipt.ErrorMessage = "event_id already exists with different canonical content"
	data.Receipts[key] = receipt
	return receipt, true, fmt.Errorf("%w: event_id content differs", domain.ErrConflict)
}
