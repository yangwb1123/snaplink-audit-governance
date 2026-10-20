package service

import (
	"context"
	"fmt"
	"sort"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Optional/all-tenant Console scope is authorized by the transport, not here.
// Console retains its live semantics and has no new matched-event limit.
func (s *Service) consoleEvents(ctx context.Context, tenantID string, query domain.Query) ([]domain.Event, error) {
	return s.authenticatedQueryEvents(ctx, tenantID, query, 0)
}

// authenticatedQueryEvents captures an owned graph, then authenticates committed
// evidence BEFORE any query predicate. Only capture/materialization holds
// Store.mu; Get, crypto and predicates inherit the caller's single deadline.
// maxMatches bounds selected identities for ordinary first queries, not source
// history, bytes or memory. Console uses zero to retain its existing behavior.
func (s *Service) authenticatedQueryEvents(ctx context.Context, tenantID string, query domain.Query, maxMatches int) ([]domain.Event, error) {
	data, err := s.Store.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	keys, err := referenceReceiptKeys(ctx, data, tenantID)
	if err != nil {
		return nil, err
	}
	events := make([]domain.Event, 0)
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		receipt := data.Receipts[key]
		candidate := queryEvidence{receipt: receipt, schemas: data.Schemas}
		if event, hot := data.Events[key]; hot {
			candidate.event = &event
		} else if receipt.Status != domain.StatusArchived {
			return nil, domain.ErrNotFound
		}
		event, err := s.resolveQueryEvidence(ctx, candidate)
		if err != nil {
			return nil, err
		}
		schema := data.Schemas[store.SchemaKey(receipt.TenantID, event.SchemaID, event.SchemaVersion)]
		if schema.TenantID != receipt.TenantID || schema.SchemaID != event.SchemaID || schema.Version != event.SchemaVersion {
			return nil, fmt.Errorf("%w: reference query schema identity mismatch", domain.ErrConflict)
		}
		source := event
		if query.PayloadField != "" {
			// The sidecar is excluded from SourceDigest. Reuse the authenticated
			// condition-format matching used by Hold, including encrypted values;
			// never replace protected fields in the returned event with plaintext.
			source, err = s.reconstructEventForDigest(event, data.Schemas)
			if err != nil {
				return nil, err
			}
		}
		selected, err := s.matchesExportHoldEvidence(source, schema, query)
		if err != nil {
			return nil, err
		}
		if selected {
			if maxMatches > 0 && len(events) >= maxMatches {
				return nil, fmt.Errorf("%w: query result budget exceeded; narrow filters", domain.ErrQuotaExceeded)
			}
			events = append(events, event)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func referenceReceiptKeys(ctx context.Context, data *store.Snapshot, tenantID string) ([]string, error) {
	keys := make([]string, 0)
	for key, receipt := range data.Receipts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		keyTenant, _, validKey := store.SplitTenantKey(key)
		if tenantID != "" && receipt.TenantID != tenantID && (!validKey || keyTenant != tenantID) {
			continue
		}
		if !validKey || keyTenant != receipt.TenantID || key != store.EventKey(receipt.TenantID, receipt.EventID) {
			return nil, fmt.Errorf("%w: reference query receipt identity mismatch", domain.ErrConflict)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}
