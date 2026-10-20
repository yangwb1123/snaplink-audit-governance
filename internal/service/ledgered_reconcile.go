package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// LedgeredReconcileResult describes a bounded live scan, not a publication or
// projection watermark. Failed evidence is not queued and is revisited on the
// next full sweep. After restart the caller starts from zero; confirmed events
// are skipped even when their receipt still says ledgered/indexed/archived.
type LedgeredReconcileResult struct {
	Scanned  int
	Repaired int
	Failed   int
	Next     store.LedgerPageCursor
	Done     bool
}

type publicationCandidate struct {
	receipt domain.EventReceipt
	event   domain.Event
	hot     bool
	schemas map[string]domain.EventSchema
}

// ReconcileLedgeredPublications repairs lost intents without a client retry.
// It examines at most limit committed cold records of one tenant. Receipt
// versions resolve to the LATEST receipt before resolving payloads; old pages
// must not mistake hot eviction for lost evidence. Archive reads are bounded
// by one pass-wide deadline and occur AFTER releasing Store.mu. A corrupt or
// missing payload cannot enter Kafka and cannot block all later candidates.
// Scan positions are deliberately volatile: callers must retain both After
// and Through, then wrap at Done. Through is finite even under continuous
// ingest. Append IDs are not commit watermarks; file replacement may reorder.
func (s *Service) ReconcileLedgeredPublications(ctx context.Context, tenantID string, after store.LedgerPageCursor, limit int) (result LedgeredReconcileResult, err error) {
	result.Next = after
	if s.Config.LedgeredPublisher == nil || s.Store == nil {
		result.Done = true
		return result, nil
	}
	defer func() {
		if result.Failed > 0 {
			s.ledgeredReconcileEvidenceFailures.Add(uint64(result.Failed))
		}
		if err != nil {
			s.ledgeredReconcileErrors.Add(1)
		}
	}()
	if err := requireTenantScope(tenantID); err != nil {
		return result, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	passCtx, cancel := s.archiveFallbackContext(ctx)
	defer cancel()
	page, err := s.Store.ReadLedgerPage(passCtx, tenantID, after, limit)
	if err != nil {
		return result, err
	}
	result.Scanned, result.Next, result.Done = len(page.Records), page.Next, page.Done
	s.ledgeredReconcileScanned.Add(uint64(len(page.Records)))
	if len(page.Records) == 0 {
		return result, nil
	}
	candidates, failures, err := s.publicationCandidates(passCtx, tenantID, page.Records)
	result.Failed = failures
	if err != nil {
		// No cursor advance on a failed store/confirmation read.
		result.Next, result.Done = after, false
		return result, err
	}
	events := make([]domain.Event, 0, len(candidates))
	for _, candidate := range candidates {
		if err := passCtx.Err(); err != nil {
			result.Next, result.Done = after, false
			return result, err
		}
		event := candidate.event
		var evidenceErr error
		if !candidate.hot {
			event, evidenceErr = s.archivedEventContext(passCtx, candidate.receipt)
		}
		if evidenceErr == nil {
			evidenceErr = s.validatePublicationEvent(event, candidate.receipt, candidate.schemas)
		}
		if evidenceErr != nil {
			result.Failed++
			// Do not log remote errors: they may contain a URL/credential or
			// object content. Identity plus a fixed reason is sufficient.
			s.logf("ledgered reconciliation evidence unavailable event_id=%s", candidate.receipt.EventID)
			continue
		}
		events = append(events, event)
	}
	if err := passCtx.Err(); err != nil {
		result.Next, result.Done = after, false
		return result, err
	}
	if len(events) == 0 {
		return result, nil
	}
	// Confirmations can arrive after the read above. In that case an extra
	// intent is harmless: the flusher rechecks and cleans it without publishing.
	err = s.Store.UpdateControl(passCtx, func(data *store.Snapshot) error {
		result.Repaired = 0 // PostgreSQL may retry this closure on a fresh view.
		if data.LedgeredOutbox == nil {
			data.LedgeredOutbox = map[string]domain.Event{}
		}
		for _, event := range events {
			key := store.EventKey(tenantID, event.EventID)
			if _, pending := data.LedgeredOutbox[key]; pending {
				continue
			}
			data.LedgeredOutbox[key] = event
			result.Repaired++
		}
		return nil
	})
	if err != nil {
		result.Repaired, result.Next, result.Done = 0, after, false
		return result, err
	}
	if result.Repaired > 0 {
		s.ledgeredIntentRepairs.Add(uint64(result.Repaired))
	}
	return result, nil
}

// Reconciliation counters report work/diagnostics, never completeness or a
// projected watermark. Metrics handlers read only atomics, with no store IO.
func (s *Service) LedgeredReconcileScanned() uint64 { return s.ledgeredReconcileScanned.Load() }
func (s *Service) LedgeredReconcileEvidenceFailures() uint64 {
	return s.ledgeredReconcileEvidenceFailures.Load()
}
func (s *Service) LedgeredReconcileErrors() uint64 { return s.ledgeredReconcileErrors.Load() }

func (s *Service) publicationCandidates(ctx context.Context, tenantID string, records []store.LedgerRecord) ([]publicationCandidate, int, error) {
	candidates := make([]publicationCandidate, 0, len(records))
	failures := 0
	err := s.Store.ReadTenant(ctx, tenantID, func(view *store.TenantView) error {
		seen := map[string]bool{}
		for _, record := range records {
			if record.Receipt == nil || seen[record.Key] {
				continue
			}
			seen[record.Key] = true
			if _, pending := view.Global.LedgeredOutbox[record.Key]; pending {
				continue
			}
			receipt, found := view.Ledger.Receipt(record.Key)
			if !found {
				if err := view.Ledger.Err(); err != nil {
					return err
				}
				return fmt.Errorf("reconciliation receipt unavailable")
			}
			confirmed, err := view.PublicationConfirmed(receipt)
			if errors.Is(err, store.ErrPublicationConflict) || errors.Is(err, store.ErrPublicationCorrupt) || errors.Is(err, domain.ErrInvalid) {
				failures++
				continue
			}
			if err != nil {
				return err
			}
			if confirmed {
				continue
			}
			event, hot := view.Hot.Events[record.Key]
			candidates = append(candidates, publicationCandidate{receipt: receipt, event: event, hot: hot, schemas: view.Global.Schemas})
		}
		return nil
	})
	return candidates, failures, err
}
