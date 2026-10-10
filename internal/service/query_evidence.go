package service

import (
	"context"
	"fmt"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

type queryEvidence struct {
	receipt domain.EventReceipt
	event   *domain.Event
	schemas map[string]domain.EventSchema
}

// The reference first page still scans its history. Only ownership/catalog
// capture occurs under Store.mu: no external Get, decryption or sorting there.
// Production-sized historical search will use the explicit projection adapter.
func (s *Service) referenceQueryEvents(ctx context.Context, tenantID string, query domain.Query) ([]domain.Event, error) {
	var candidates []queryEvidence
	err := s.Store.Read(ctx, func(data *store.Snapshot) error {
		schemas := map[string]domain.EventSchema{}
		for key, schema := range data.Schemas {
			if schema.TenantID == tenantID {
				schemas[key] = schema
			}
		}
		for key, event := range data.Events {
			if event.TenantID != tenantID || !s.matches(event, query, schemas) {
				continue
			}
			receipt, exists := data.Receipts[key]
			// Hot-only residue is not a committed cold fact; never mint query truth.
			if !exists {
				continue
			}
			cloned, err := domain.CloneEvent(event)
			if err != nil {
				return err
			}
			candidates = append(candidates, queryEvidence{receipt: receipt, event: &cloned, schemas: schemas})
		}
		for key, receipt := range data.Receipts {
			if receipt.TenantID != tenantID || receipt.Status != domain.StatusArchived {
				continue
			}
			if _, hot := data.Events[key]; hot {
				continue
			}
			candidates = append(candidates, queryEvidence{receipt: receipt, schemas: schemas})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	result := []domain.Event{}
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		event, err := s.resolveQueryEvidence(ctx, candidate)
		if err != nil {
			return nil, err
		}
		if s.matches(event, query, candidate.schemas) {
			result = append(result, event)
		}
		if len(result) > store.MaxQuerySnapshotEvents {
			return nil, fmt.Errorf("%w: query result budget exceeded; narrow filters", domain.ErrQuotaExceeded)
		}
	}
	return result, nil
}

// readQueryEvidence resolves at most a page of exact identities, never a
// receipt-history scan. V2 PostgreSQL uses the cold receipt O(1) index. V1 is
// compatibility only and still materializes its legacy snapshot.
func (s *Service) readQueryEvidence(ctx context.Context, tenantID string, references []store.EventReference) ([]domain.Event, error) {
	candidates := make([]queryEvidence, 0, len(references))
	err := s.Store.ReadTenant(ctx, tenantID, func(view *store.TenantView) error {
		for _, reference := range references {
			key := store.EventKey(tenantID, reference.EventID)
			receipt, found := view.Ledger.Receipt(key)
			if !found {
				if err := view.Ledger.Err(); err != nil {
					return err
				}
				return domain.ErrNotFound
			}
			if reference.StreamID != "" && (reference.StreamID != receipt.StreamID || reference.Sequence != receipt.Sequence || reference.Hash != receipt.Hash) {
				return fmt.Errorf("%w: query evidence coordinates changed", domain.ErrConflict)
			}
			candidate := queryEvidence{receipt: receipt, schemas: map[string]domain.EventSchema{}}
			if event, hot := view.Hot.Events[key]; hot {
				cloned, err := domain.CloneEvent(event)
				if err != nil {
					return err
				}
				candidate.event = &cloned
				schemaKey := store.SchemaKey(tenantID, event.SchemaID, event.SchemaVersion)
				if schema, found := view.Global.Schemas[schemaKey]; found {
					candidate.schemas[schemaKey] = schema
				}
			} else {
				if receipt.Status != domain.StatusArchived {
					return domain.ErrNotFound
				}
				// Receipt does not carry schema identity. Retain only this tenant's
				// catalog; archive envelope supplies the exact version after lock release.
				for key, schema := range view.Global.Schemas {
					if schema.TenantID == tenantID {
						candidate.schemas[key] = schema
					}
				}
			}
			candidates = append(candidates, candidate)
		}
		return view.Ledger.Err()
	})
	if err != nil {
		return nil, err
	}
	result := make([]domain.Event, 0, len(candidates))
	for i, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		event, err := s.resolveQueryEvidence(ctx, candidate)
		if err != nil {
			return nil, err
		}
		if at := references[i].OccurredAt; !at.IsZero() && !at.Equal(event.OccurredAt) {
			return nil, fmt.Errorf("%w: projected occurrence differs from ledger evidence", domain.ErrConflict)
		}
		result = append(result, event)
	}
	return result, nil
}

func (s *Service) resolveQueryEvidence(ctx context.Context, candidate queryEvidence) (domain.Event, error) {
	var event domain.Event
	if candidate.event != nil {
		event = *candidate.event
	} else {
		var err error
		event, err = s.archivedEventContext(ctx, candidate.receipt)
		if err != nil {
			return domain.Event{}, err
		}
	}
	if err := s.validatePublicationEvent(event, candidate.receipt, candidate.schemas); err != nil {
		return domain.Event{}, err
	}
	return event, nil
}
