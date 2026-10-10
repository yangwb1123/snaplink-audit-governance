package service

import (
	"context"
	"fmt"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// ProjectionWriter is a derived sink. Rebuild never publishes to Kafka,
// modifies receipts/outbox/approvals, or invokes business recovery APIs.
type ProjectionWriter interface {
	Insert(context.Context, domain.Event) error
}

type ProjectionRebuildPage struct {
	Scanned int
	Written int
	Failed  int
	Next    store.LedgerPageCursor
	Done    bool
}

// RebuildProjectionPage reads a bounded COMMITTED cold page and reconstructs
// protected events using the latest receipts and exact schemas. Remote archive
// and destination IO run outside Store.mu under one shared pass deadline.
// Positions are scan coordinates, not a complete immutable evidence snapshot:
// late commits require a subsequent zero-origin sweep. Repeated writes must be
// idempotent in the destination. Missing/corrupt evidence is never projected.
func (s *Service) RebuildProjectionPage(ctx context.Context, tenantID string, cursor store.LedgerPageCursor, limit int, writer ProjectionWriter) (result ProjectionRebuildPage, err error) {
	result.Next = cursor
	if err := requireTenantScope(tenantID); err != nil {
		return result, err
	}
	if writer == nil || s.Store == nil {
		return result, fmt.Errorf("%w: projection writer and ledger required", domain.ErrInvalid)
	}
	passCtx, cancel := s.archiveFallbackContext(ctx)
	defer cancel()
	page, err := s.Store.ReadLedgerPage(passCtx, tenantID, cursor, limit)
	if err != nil {
		return result, err
	}
	result.Scanned = len(page.Records)
	candidates, err := s.projectionCandidates(passCtx, tenantID, page.Records)
	if err != nil {
		return result, err
	}
	for _, candidate := range candidates {
		if err := passCtx.Err(); err != nil {
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
		if err := passCtx.Err(); err != nil {
			return result, err
		}
		if evidenceErr != nil {
			result.Failed++
			s.logf("projection rebuild evidence unavailable event_id=%s", candidate.receipt.EventID)
			continue
		}
		if err := writer.Insert(passCtx, event); err != nil {
			// Keep the original cursor on a transient destination failure.
			// Earlier successful writes may repeat, never disappear.
			return result, fmt.Errorf("projection rebuild destination unavailable: %w", err)
		}
		result.Written++
	}
	if err := passCtx.Err(); err != nil {
		return result, err
	}
	result.Next, result.Done = page.Next, page.Done
	return result, nil
}

func (s *Service) projectionCandidates(ctx context.Context, tenantID string, records []store.LedgerRecord) ([]publicationCandidate, error) {
	candidates := make([]publicationCandidate, 0, len(records))
	if len(records) == 0 {
		return candidates, nil
	}
	err := s.Store.ReadTenant(ctx, tenantID, func(view *store.TenantView) error {
		seen := map[string]bool{}
		for _, record := range records {
			if record.Receipt == nil || seen[record.Key] {
				continue
			}
			seen[record.Key] = true
			receipt, found := view.Ledger.Receipt(record.Key)
			if !found {
				if err := view.Ledger.Err(); err != nil {
					return err
				}
				return fmt.Errorf("projection rebuild receipt unavailable")
			}
			event, hot := view.Hot.Events[record.Key]
			candidates = append(candidates, publicationCandidate{
				receipt: receipt, event: event, hot: hot, schemas: view.Global.Schemas,
			})
		}
		return nil
	})
	return candidates, err
}
