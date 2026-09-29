package service

// WORM archival: sealing partial streams into signed segments, the atomic
// batch that writes events, segments and receipts to the archive and then
// evicts the event bodies from the hot tier, the dead-letter records for
// evidence that can never be archived, and the archive-conflict counter that
// stops a tenant from retrying a pass the store keeps losing.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// SealPendingSegments creates a checkpoint for each non-empty partial stream.
// The ingest path seals full segments; the governance worker calls this method
// periodically so low-volume streams also receive a durable checkpoint.
//
// A pass that seals nothing (no pending hashes for the tenant) persists
// nothing: Store.UpdateChecked skips the Save on an idle tick (FR-2).
func (s *Service) SealPendingSegments(ctx context.Context, tenantID string) error {
	if err := requireTenantScope(tenantID); err != nil {
		return err
	}
	if s.Store.HotCold() {
		return s.sealPendingTenant(ctx, tenantID)
	}
	now := s.Now()
	return s.Store.UpdateChecked(func(data *store.Snapshot) (bool, error) {
		mutated := false
		for key, stream := range data.Streams {
			if stream.TenantID != tenantID || len(stream.PendingHashes) == 0 {
				continue
			}
			segment, checkpoint, err := s.sealSegment(ctx, stream, now)
			if err != nil {
				return false, err
			}
			data.Segments[key] = append(data.Segments[key], segment)
			data.Checkpoints[key] = append(data.Checkpoints[key], checkpoint)
			stream.PendingHashes = nil
			stream.PendingEvents = nil
			stream.PendingPrevHash = ""
			data.Streams[key] = stream
			mutated = true
		}
		return mutated, nil
	})
}

// exportPutTimeout bounds one fire-and-forget export write. runExport has no
// pass context (it outlives the request that created it), so a black-holed
// endpoint must not be able to keep the goroutine (and its finishExport
// write) alive for minutes per object. Mirrors vaultCallBudget's

func (s *Service) ArchivePending(ctx context.Context, tenantID string) (int, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return 0, err
	}
	if !archive.Configured(s.Config.Archive) {
		return 0, fmt.Errorf("%w: archive directory is not configured", domain.ErrInvalid)
	}
	if s.Store.HotCold() {
		return s.archivePendingTenant(ctx, tenantID)
	}
	var events []domain.Event
	var evictedKeys []string
	var segments []domain.Segment
	if err := s.Store.Read(func(data *store.Snapshot) error {
		for key, event := range data.Events {
			if event.TenantID != tenantID {
				continue
			}
			if _, dead := data.DeadLetters[key]; dead {
				continue // already dead-lettered: never retried
			}
			receipt := data.Receipts[key]
			if receipt.Status == domain.StatusArchived {
				// A legacy snapshot or a recovered two-phase deployment can
				// contain the archived receipt and hot body together. The
				// archive is already verified, so only schedule the safe,
				// idempotent hot-body eviction; never rewrite the WORM object.
				evictedKeys = append(evictedKeys, key)
				continue
			}
			events = append(events, event)
		}
		for key, values := range data.Segments {
			// Exact-component membership (see CreateAggregateCheckpoint); the
			// former StreamKey(tenantID, "") shortcut is subsumed: that key
			// parses as (tenantID, "", true).
			tid, _, ok := store.SplitTenantKey(key)
			if !ok || tid != tenantID {
				continue
			}
			for _, segment := range values {
				// Same dead-letter exclusion as events: a dead-lettered
				// segment (over-limit key, or a byte-different object at its
				// manifest key) can never succeed and is never re-attempted,
				// so one doomed segment cannot churn a Stat/verify (or abort
				// the pass) on every run.
				dlKey, _ := segmentDeadLetterKey(segment)
				if _, dead := data.DeadLetters[dlKey]; dead {
					continue
				}
				segments = append(segments, segment)
			}
		}
		return nil
	}); err != nil {
		return 0, err
	}
	now := s.Now()
	deadLetters := map[string]domain.DeadLetter{}
	for _, segment := range segments {
		if err := s.archiveSegment(ctx, segment); err != nil {
			if !isPermanentArchiveError(err) {
				return 0, err
			}
			key, eventID := segmentDeadLetterKey(segment)
			deadLetters[key] = domain.DeadLetter{TenantID: tenantID, EventID: eventID, StreamID: segment.StreamID, Sequence: segment.FirstSequence, Reason: archiveErrorReason(err), ErrorMessage: boundedErrorMessage(err), At: now}
		}
	}
	archivedEvents := []domain.Event{}
	for _, event := range events {
		if err := s.archiveEvent(ctx, event); err != nil {
			if !isPermanentArchiveError(err) {
				return 0, err
			}
			key := store.EventKey(tenantID, event.EventID)
			deadLetters[key] = domain.DeadLetter{TenantID: tenantID, EventID: event.EventID, StreamID: event.StreamID, Sequence: event.Sequence, Reason: archiveErrorReason(err), ErrorMessage: boundedErrorMessage(err), At: now}
			continue
		}
		archivedEvents = append(archivedEvents, event)
	}
	if len(archivedEvents) == 0 && len(deadLetters) == 0 && len(evictedKeys) == 0 {
		// Nothing to mark; the conflict counter is deliberately not reset on
		// an empty pass (it counts consecutive passes aborted by exhaustion,
		// and an empty pass cannot have been aborted).
		return 0, nil
	}
	// One batch Update for the whole pass: the captured event list (not a
	// re-derivation from the fresh snapshot) keeps the closure re-runnable
	// — Store.Update re-runs it on a fresh snapshot after each conflict and
	// a failed Save commits nothing, so re-runs apply the same mutations.
	// The reset of the tenant's conflict counter rides in the same atomic
	// write as the receipt marking (no extra window).
	if err := s.Store.Update(func(data *store.Snapshot) error {
		for _, key := range evictedKeys {
			if receipt, ok := data.Receipts[key]; ok && receipt.Status == domain.StatusArchived {
				delete(data.Events, key)
			}
		}
		for _, event := range archivedEvents {
			key := store.EventKey(tenantID, event.EventID)
			receipt, ok := data.Receipts[key]
			if !ok {
				return domain.ErrNotFound
			}
			receipt.Status = domain.StatusArchived
			receipt.IndexedAt = now
			receipt.ArchivedAt = now
			data.Receipts[key] = receipt
			// The verified WORM object is now the payload source of truth. Keep
			// the receipt/hash linkage, but evict the immutable event body from
			// the rewritten control-plane snapshot in the same atomic Save.
			delete(data.Events, key)
		}
		for key, dead := range deadLetters {
			data.DeadLetters[key] = dead
			receipt, ok := data.Receipts[key]
			if !ok {
				if isSegmentDeadLetter(dead) {
					continue // segments have no receipt
				}
				return domain.ErrNotFound // event receipt vanished: fail atomically
			}
			receipt.ErrorCode = "archive_dead_letter"
			receipt.ErrorMessage = dead.ErrorMessage
			data.Receipts[key] = receipt
		}
		data.ArchiveConflictFailures[tenantID] = 0
		return nil
	}); err != nil {
		return 0, err
	}
	return len(archivedEvents), nil
}

// isPermanentArchiveError reports whether an archive failure can never
// succeed on retry: the key exceeds the backend limits (FM-1) or a
// byte-different object already occupies the key (F-2). Everything else is
// treated as transient and aborts the pass so the worker retries it.
func isPermanentArchiveError(err error) bool {
	return errors.Is(err, archive.ErrArchiveKeyTooLong) || errors.Is(err, archive.ErrObjectConflict)
}

// archiveErrorReason is the stable dead-letter reason code for a permanent
// archive failure; it is what operators see in ListDeadLetters and the
// receipt record.
func archiveErrorReason(err error) string {
	switch {
	case errors.Is(err, archive.ErrArchiveKeyTooLong):
		return "archive_key_too_long"
	case errors.Is(err, archive.ErrObjectConflict):
		return "archive_object_conflict"
	}
	return "archive_permanent_failure"
}

// boundedErrorMessage caps a dead-letter error message at 512 bytes so a
// stored message never embeds an unbounded attacker-controlled value (the
// FileStore conflict error embeds the archive path, which can be long).
func boundedErrorMessage(err error) string {
	message := err.Error()
	if len(message) > 512 {
		message = message[:512]
	}
	return message
}

// segmentDeadLetterKey returns the DeadLetters map key for a sealed segment
// (segments have no receipt). The synthetic event id embeds store.KeySeparator
// (0x1F), which ValidKeyComponent forbids in real event ids, so a segment
// dead-letter can never alias an event dead-letter in the same map.
func segmentDeadLetterKey(segment domain.Segment) (key, eventID string) {
	eventID = "seg" + store.KeySeparator + segment.StreamID + ":" + fmt.Sprintf("%d-%d", segment.FirstSequence, segment.LastSequence)
	return store.EventKey(segment.TenantID, eventID), eventID
}

// isSegmentDeadLetter reports whether a DeadLetter entry is the synthetic
// segment record (no receipt exists for it).
func isSegmentDeadLetter(dead domain.DeadLetter) bool {
	return strings.HasPrefix(dead.EventID, "seg"+store.KeySeparator)
}

// ListDeadLetters returns the tenant's persisted dead-letter entries (FM-1
// over-limit keys, F-2 byte-conflicting objects) ordered by time then event
// id. This is the operator detection surface: entries whose receipt carries
// ErrorCode "archive_dead_letter" exactly flag the state that will never
// converge without operator action.
func (s *Service) ListDeadLetters(tenantID string) ([]domain.DeadLetter, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return nil, err
	}
	var out []domain.DeadLetter
	err := s.Store.Read(func(data *store.Snapshot) error {
		for _, dead := range data.DeadLetters {
			if dead.TenantID == tenantID {
				out = append(out, dead)
			}
		}
		sort.Slice(out, func(i, j int) bool {
			if !out[i].At.Equal(out[j].At) {
				return out[i].At.Before(out[j].At)
			}
			return out[i].EventID < out[j].EventID
		})
		return nil
	})
	return out, err
}

// ClearDeadLetter removes a dead-letter entry (and the receipt's typed
// error) so the next ArchivePending pass retries the event. Intended for
// operator remediation after the root cause is resolved; the retry
// re-dead-letters the event if the conflict persists, keeping the loop
// bounded.
func (s *Service) ClearDeadLetter(tenantID, eventID string) error {
	if err := requireTenantScope(tenantID); err != nil {
		return err
	}
	return s.Store.Update(func(data *store.Snapshot) error {
		key := store.EventKey(tenantID, eventID)
		delete(data.DeadLetters, key)
		receipt, ok := data.Receipts[key]
		if ok {
			receipt.ErrorCode = ""
			receipt.ErrorMessage = ""
			data.Receipts[key] = receipt
		}
		return nil
	})
}

// RecordArchivePassConflict increments the tenant's persisted archive-pass
// conflict counter. Best-effort: the worker invokes it after ArchivePending
// returned an exhausted ErrSnapshotConflict; if this write itself exhausts
// under sustained load it fails without masking the original error. The
// closure is conflict-safe by construction: Store.Update re-runs it on the
// fresh snapshot, and a failed Save commits nothing, so each committed
// attempt applies exactly one increment.
func (s *Service) RecordArchivePassConflict(tenantID string) error {
	if err := requireTenantScope(tenantID); err != nil {
		return err
	}
	return s.Store.Update(func(data *store.Snapshot) error {
		data.ArchiveConflictFailures[tenantID]++
		return nil
	})
}

// ArchivePassConflictFailures returns the tenant's persisted archive-pass
// conflict counter, for the worker's log line and for tests.
func (s *Service) ArchivePassConflictFailures(tenantID string) (int, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return 0, err
	}
	var count int
	err := s.Store.Read(func(data *store.Snapshot) error {
		count = data.ArchiveConflictFailures[tenantID]
		return nil
	})
	return count, err
}
