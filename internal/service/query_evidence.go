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

// The reference first selection shares Console's receipt-anchored authentication
// before predicates, with the existing matched-identity budget. It still scans
// history; capture owns the graph and all crypto/Get/filter work is lock-free.
// Fixed-member continuation and the opt-in projection path do not reselect here.
func (s *Service) referenceQueryEvents(ctx context.Context, tenantID string, query domain.Query) ([]domain.Event, error) {
	return s.authenticatedQueryEvents(ctx, tenantID, query, store.MaxQuerySnapshotEvents)
}

// readQueryEvidence resolves at most a page of exact identities, never a
// receipt-history scan. V2 PostgreSQL uses the cold receipt O(1) index. V1 is
// compatibility only and still materializes its legacy snapshot.
func (s *Service) readQueryEvidence(ctx context.Context, tenantID string, references []store.EventReference) ([]domain.Event, error) {
	candidates := make([]queryEvidence, 0, len(references))
	err := s.Store.ReadTenant(ctx, tenantID, func(view *store.TenantView) error {
		for _, reference := range references {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := store.EventKey(tenantID, reference.EventID)
			receipt, found := view.Ledger.Receipt(key)
			if !found {
				if err := view.Ledger.Err(); err != nil {
					return err
				}
				return domain.ErrNotFound
			}
			// A valid lookup key is not proof that the receipt body belongs to
			// the requested identity, including single-event reads without coordinates.
			if receipt.TenantID != tenantID || receipt.EventID != reference.EventID {
				return fmt.Errorf("%w: query evidence receipt identity mismatch", domain.ErrConflict)
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
	if err := ctx.Err(); err != nil {
		return nil, err
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
	if err := ctx.Err(); err != nil {
		return domain.Event{}, err
	}
	// Authenticate the exact catalog body, not just the versioned map key.
	// Disabled historical definitions remain readable; newest is never a fallback.
	schema, found := candidate.schemas[store.SchemaKey(event.TenantID, event.SchemaID, event.SchemaVersion)]
	if !found || schema.TenantID != event.TenantID || schema.SchemaID != event.SchemaID || schema.Version != event.SchemaVersion {
		return domain.Event{}, fmt.Errorf("%w: query evidence exact schema identity mismatch", domain.ErrConflict)
	}
	if err := s.validatePublicationEvent(event, candidate.receipt, candidate.schemas); err != nil {
		return domain.Event{}, err
	}
	return event, nil
}
