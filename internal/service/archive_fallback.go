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

// archiveReadTimeout caps every WORM read used by a fallback surface. A
// missing or black-holed archive must fail the read, not retain the store
// read lock or HTTP handler forever. It is an UPPER CEILING, not the bound:
// the effective bound is archiveReadBound. The package variable is a narrow
// test seam so cancellation behavior can be tested without waiting 30 seconds.
var archiveReadTimeout = 30 * time.Second

// archiveReadBound is the single source of the WORM fallback bound: the
// tighter of archiveReadTimeout and Store.OperationTimeout(). There is no new
// constant, no new Config field and no new environment variable — the store's
// existing operator knob (AUDIT_STORE_OPERATION_TIMEOUT) already is the number,
// and archiveReadTimeout still caps it from above.
//
// A nil Store degrades to archiveReadTimeout alone instead of panicking:
// service.New(nil, cfg) is a shipped path (audit-api -check-config resolves a
// configuration without ever opening a store).
func (s *Service) archiveReadBound() time.Duration {
	if s.Store == nil {
		return archiveReadTimeout
	}
	if bound := s.Store.OperationTimeout(); bound > 0 && bound < archiveReadTimeout {
		return bound
	}
	return archiveReadTimeout
}

// archiveFallbackContext derives ONE bound for ONE service operation: a child
// of ctx carrying archiveReadBound, so an earlier caller deadline still wins
// unchanged and caller cancellation still aborts the read.
//
// PLACEMENT IS LOAD-BEARING: establish the Store operation context first,
// either with WithOperation for lock-free evidence reads or inside a store
// closure. Store.begin arms op.ctx with operationTimeout at t0; deriving this
// from op.ctx at t1 >= t0 cannot extend that deadline. When that bound is
// exhausted, op.ctx.Err() is already context.DeadlineExceeded and Store.end
// deterministically classifies the operation error as
// store.ErrOperationDeadline: the retryable 503 store_timeout every affected
// route already declares, plus audit_store_operation_deadline_exceeded_total.
// Deriving before the Store operation context is armed inverts that order — op.ctx is still live
// when the fallback fails, Store.end does not classify, and the client gets an
// opaque 500 instead of the retryable status.
func (s *Service) archiveFallbackContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, s.archiveReadBound())
}

// eventArchiveKey is the single archive-key format used by both writers and
// readers. The encoded components are injective, so an archived event cannot
// collide with another tenant or stream through lossy path sanitization.
func eventArchiveKey(tenantID, streamID string, sequence int64, eventID string) string {
	return fmt.Sprintf("events/%s/%s/%020d-%s.json", safeName(tenantID), safeName(streamID), sequence, safeName(eventID))
}

// archivedEventContext loads and verifies an archived event. A receipt is the
// retained linkage anchor after hot eviction; any missing, malformed, or
// mismatched object is an integrity error rather than a silent 404.
func (s *Service) archivedEventContext(ctx context.Context, receipt domain.EventReceipt) (domain.Event, error) {
	if !archive.Configured(s.Config.Archive) {
		return domain.Event{}, fmt.Errorf("archived event %s: archive is not configured", receipt.EventID)
	}
	// Every WORM read is bounded here, so no caller can reach Archive.Get
	// unbounded — including a nil ctx, which falls back to the BOUNDED
	// derivation rather than to an unbounded background read. A caller that
	// already derived once for its whole operation (eventsFromSnapshot,
	// GetEventAcrossTenants) inherits the same bound; re-deriving here can
	// only produce an equal-or-sooner deadline, never a later one.
	readCtx, cancel := s.archiveFallbackContext(ctx)
	defer cancel()
	// Short-circuit an already-exhausted bound before any remote work: a doomed
	// read must not issue another Get. Every archived read funnels through this
	// function, so this single guard also bounds the multi-receipt loops.
	if err := readCtx.Err(); err != nil {
		return domain.Event{}, fmt.Errorf("archived event %s: %w", receipt.EventID, err)
	}
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
		return domain.CloneEvent(event)
	}
	receipt, ok := data.Receipts[key]
	if !ok || receipt.Status != domain.StatusArchived {
		return domain.Event{}, domain.ErrNotFound
	}
	event, err := s.archivedEventContext(ctx, receipt)
	if err != nil {
		return domain.Event{}, err
	}
	return domain.CloneEvent(event)
}

// eventsFromSnapshot returns hot events plus verified archived events whose
// receipts no longer have a hot payload. Callers provide the predicate so
// timeline, export and legal-hold reads share one fallback implementation.
func (s *Service) eventsFromSnapshot(ctx context.Context, data *store.Snapshot, tenantID string, predicate func(domain.Event) bool) ([]domain.Event, error) {
	return s.resolveSnapshotEvents(ctx, data, tenantID, predicate, true)
}

// Integrity verification must examine corrupted committed hot bodies to return
// an invalid verdict and self-audit it, not turn corruption into a 409 or hide
// it. This mode never exposes the payload and still excludes uncommitted hot.
func (s *Service) resolveSnapshotEvents(ctx context.Context, data *store.Snapshot, tenantID string, predicate func(domain.Event) bool, checkCoordinates bool) ([]domain.Event, error) {
	events := make([]domain.Event, 0)
	for key, event := range data.Events {
		if tenantID != "" && event.TenantID != tenantID {
			continue
		}
		if data.LayoutVersion >= 2 {
			receipt, committed := data.Receipts[key]
			if !committed {
				continue
			} // a hot recovery marker is not ledger truth
			if checkCoordinates && (receipt.TenantID != event.TenantID || receipt.EventID != event.EventID || receipt.StreamID != event.StreamID || receipt.Sequence != event.Sequence || receipt.Hash != event.Hash) {
				return nil, domain.ErrConflict
			}
		}
		cloned, err := domain.CloneEvent(event)
		if err != nil {
			return nil, err
		}
		if predicate(cloned) {
			events = append(events, cloned)
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
	if len(keys) == 0 {
		return events, nil
	}
	// ONE bound for the whole pass, derived here once instead of re-armed per
	// receipt: this is what converts N receipts × archiveReadTimeout into at
	// most one bound. archivedEventContext re-derives a child of this context,
	// which can only make the deadline earlier.
	readCtx, cancel := s.archiveFallbackContext(ctx)
	defer cancel()
	for _, key := range keys {
		receipt := data.Receipts[key]
		event, err := s.archivedEventContext(readCtx, receipt)
		if err != nil {
			return nil, err
		}
		cloned, err := domain.CloneEvent(event)
		if err != nil {
			return nil, err
		}
		if predicate(cloned) {
			events = append(events, cloned)
		}
	}
	return events, nil
}

// idempotencyKeyReused reports whether idempotencyKey is already bound to a
// different event for the tenant (hot events and retained cold receipts).
// Old snapshots may not have the additive receipt field, so their hot event
// remains the compatibility fallback. The predicate intentionally returns
// only a bool: callers must build a fresh conflict receipt for the colliding
// ingest and must NOT read or mutate the pre-existing owner receipt.
func idempotencyKeyReused(data *store.Snapshot, tenantID, eventKey, idempotencyKey string) bool {
	for key, event := range data.Events {
		if key != eventKey && event.TenantID == tenantID && event.IdempotencyKey == idempotencyKey {
			return true
		}
	}
	for key, receipt := range data.Receipts {
		if key != eventKey && receipt.TenantID == tenantID && receipt.IdempotencyKey != "" && receipt.IdempotencyKey == idempotencyKey {
			return true
		}
	}
	return false
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
		// Stored SourceDigest is not a substitute for reconstructing the
		// authenticated content. A missing schema, clone failure, or decrypt
		// error must fail closed rather than produce a duplicate response.
		return domain.EventReceipt{}, true, digestErr
	}
	receipt = data.Receipts[key]
	if existingDigest == inputDigest {
		// Duplicate is response metadata, not durable event state. Do not
		// rewrite the receipt on an idempotent retry.
		receipt.Duplicate = true
		return receipt, true, nil
	}
	receipt.Conflict = true
	receipt.ErrorCode = "event_id_content_conflict"
	receipt.ErrorMessage = "event_id already exists with different canonical content"
	data.Receipts[key] = receipt
	return receipt, true, fmt.Errorf("%w: event_id content differs", domain.ErrConflict)
}
