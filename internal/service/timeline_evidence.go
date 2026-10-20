package service

import (
	"context"
	"fmt"
	"sort"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// readTimelineEvidence is the authenticated reference source for paged and
// bounded whole timelines, operation summary and both local replay methods
// (including operation replay's internal restore callers).
// Snapshot owns its graph; only capture/materialization holds Store.mu. Remote
// Get, exact-schema/content/chain authentication and predicates run afterwards
// under the caller's shared operation deadline. This still scans tenant history
// and does not turn live timeline cursors into fixed query membership.
func (s *Service) readTimelineEvidence(ctx context.Context, tenantID string, predicate func(domain.Event) bool) ([]domain.Event, error) {
	data, err := s.Store.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0)
	for key, receipt := range data.Receipts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		keyTenant, _, validKey := store.SplitTenantKey(key)
		if receipt.TenantID != tenantID && (!validKey || keyTenant != tenantID) {
			continue
		}
		if receipt.TenantID != tenantID || key != store.EventKey(tenantID, receipt.EventID) {
			return nil, fmt.Errorf("%w: timeline receipt identity mismatch", domain.ErrConflict)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
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
		schema := data.Schemas[store.SchemaKey(tenantID, event.SchemaID, event.SchemaVersion)]
		if schema.TenantID != tenantID || schema.SchemaID != event.SchemaID || schema.Version != event.SchemaVersion {
			return nil, fmt.Errorf("%w: timeline schema identity mismatch", domain.ErrConflict)
		}
		// Authenticate before selection: a corrupted operation/aggregate field
		// must not hide committed evidence or turn corruption into a 404.
		if predicate(event) {
			events = append(events, event)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, domain.ErrNotFound
	}
	return events, nil
}
