package service

import (
	"context"
	"fmt"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// QueryConsoleEvents returns a newest-first page for the interactive Console.
// QueryEvents remains chronological for reconstruction, replay, and export.
func (s *Service) QueryConsoleEvents(tenantID, actor string, query domain.Query) (domain.QueryResult, error) {
	if err := validateConsoleReadQuery(query); err != nil {
		return domain.QueryResult{}, err
	}
	events, err := s.consoleEvents(tenantID, query)
	if err != nil {
		return domain.QueryResult{}, err
	}
	sortEvents(events)
	reverseEvents(events)
	if query.Cursor != "" {
		events, err = consoleEventsBeforeCursor(events, query.Cursor)
		if err != nil {
			return domain.QueryResult{}, err
		}
	}
	result := consoleEventPage(events, query.PageSize)
	if err := s.recordReadAction(tenantID, actor, domain.AdminActionEventRead, "query", "", "snaplink-console"); err != nil {
		return domain.QueryResult{}, err
	}
	return result, nil
}

// QueryEventFacets aggregates the same filtered event set as QueryEvents in a
// single audited store read. SourceSystem is the governance-owned equivalent
// of the legacy Console client column; provider has no trusted counterpart in
// the canonical event envelope and therefore remains an empty map.
func (s *Service) QueryEventFacets(tenantID, actor string, query domain.Query) (domain.EventFacets, error) {
	if err := validateConsoleReadQuery(query); err != nil {
		return domain.EventFacets{}, err
	}
	result := domain.EventFacets{
		Outcomes: map[string]int{}, Types: map[string]int{},
		Clients: map[string]int{}, Providers: map[string]int{},
	}
	events, err := s.consoleEvents(tenantID, query)
	if err == nil {
		result.Total = len(events)
		for _, event := range events {
			result.Outcomes[event.Outcome]++
			result.Types[event.EventType]++
			if event.SourceSystem != "" {
				result.Clients[event.SourceSystem]++
			}
		}
	}
	if err != nil {
		return domain.EventFacets{}, err
	}
	if err := s.recordReadAction(tenantID, actor, domain.AdminActionEventRead, "query", "", "facets"); err != nil {
		return domain.EventFacets{}, err
	}
	return result, nil
}

func validateConsoleReadQuery(query domain.Query) error {
	if query.From.IsZero() || query.To.IsZero() {
		return fmt.Errorf("%w: from and to are required", domain.ErrInvalid)
	}
	if !query.From.Before(query.To) {
		return fmt.Errorf("%w: from must be before to", domain.ErrInvalid)
	}
	if (query.PayloadField == "") != (query.PayloadDigest == "") {
		return fmt.Errorf("%w: payload_field and payload_digest must be provided together", domain.ErrInvalid)
	}
	if query.PageSize <= 0 || query.PageSize > domain.MaxPageSize {
		return fmt.Errorf("%w: invalid page size", domain.ErrInvalid)
	}
	return nil
}

func (s *Service) consoleEvents(tenantID string, query domain.Query) ([]domain.Event, error) {
	var events []domain.Event
	err := s.Store.Read(func(data *store.Snapshot) error {
		schemas := make(map[string]domain.EventSchema, len(data.Schemas))
		for key, schema := range data.Schemas {
			schemas[key] = schema
		}
		var readErr error
		events, readErr = s.eventsFromSnapshot(context.Background(), data, tenantID, func(event domain.Event) bool {
			return s.matches(event, query, schemas)
		})
		return readErr
	})
	return events, err
}

func reverseEvents(events []domain.Event) {
	for left, right := 0, len(events)-1; left < right; left, right = left+1, right-1 {
		events[left], events[right] = events[right], events[left]
	}
}

func consoleEventsBeforeCursor(events []domain.Event, raw string) ([]domain.Event, error) {
	cursor, err := domain.DecodeCursor(raw)
	if err != nil {
		return nil, err
	}
	if cursor.Legacy {
		return nil, fmt.Errorf("%w: stale cursor", domain.ErrInvalid)
	}
	filtered := events[:0]
	for _, event := range events {
		if eventBeforeCursor(event, cursor) {
			filtered = append(filtered, event)
		}
	}
	return filtered, nil
}

func eventBeforeCursor(event domain.Event, cursor domain.Cursor) bool {
	if !event.OccurredAt.Equal(cursor.OccurredAt) {
		return event.OccurredAt.Before(cursor.OccurredAt)
	}
	return event.Sequence < cursor.Sequence ||
		(event.Sequence == cursor.Sequence && event.EventID < cursor.EventID)
}

func consoleEventPage(events []domain.Event, limit int) domain.QueryResult {
	result := domain.QueryResult{Count: len(events), Items: []domain.Event{}}
	end := min(limit, len(events))
	result.Items = events[:end]
	if end < len(events) {
		last := events[end-1]
		result.NextCursor = domain.EncodeCursor(domain.Cursor{
			OccurredAt: last.OccurredAt, Sequence: last.Sequence, EventID: last.EventID,
		})
	}
	return result
}

// GetEventAcrossTenants resolves a globally unique event id for a platform
// reader. A duplicate id across tenants is intentionally ambiguous and fails
// closed rather than selecting a tenant by map iteration order.
func (s *Service) GetEventAcrossTenants(actor, eventID string) (domain.Event, error) {
	var matches []domain.Event
	err := s.Store.Read(func(data *store.Snapshot) error {
		for _, event := range data.Events {
			if event.EventID == eventID {
				matches = append(matches, event)
			}
		}
		for key, receipt := range data.Receipts {
			if receipt.EventID != eventID || receipt.Status != domain.StatusArchived {
				continue
			}
			if _, hot := data.Events[key]; hot {
				continue
			}
			event, readErr := s.archivedEventContext(context.Background(), receipt)
			if readErr != nil {
				return readErr
			}
			matches = append(matches, event)
		}
		return nil
	})
	if err != nil {
		return domain.Event{}, err
	}
	if len(matches) == 0 {
		return domain.Event{}, domain.ErrNotFound
	}
	if len(matches) != 1 {
		return domain.Event{}, fmt.Errorf("%w: event id is ambiguous across tenants", domain.ErrConflict)
	}
	event := matches[0]
	if err := s.recordReadAction(event.TenantID, actor, domain.AdminActionEventRead, "event", eventID, ""); err != nil {
		return domain.Event{}, err
	}
	return event, nil
}

func (s *Service) GetReceiptAcrossTenants(actor, eventID string) (domain.EventReceipt, error) {
	var matches []domain.EventReceipt
	err := s.Store.Read(func(data *store.Snapshot) error {
		for _, receipt := range data.Receipts {
			if receipt.EventID == eventID {
				matches = append(matches, receipt)
			}
		}
		return nil
	})
	if err != nil {
		return domain.EventReceipt{}, err
	}
	if len(matches) == 0 {
		return domain.EventReceipt{}, domain.ErrNotFound
	}
	if len(matches) != 1 {
		return domain.EventReceipt{}, fmt.Errorf("%w: event id is ambiguous across tenants", domain.ErrConflict)
	}
	receipt := matches[0]
	if err := s.recordReadAction(receipt.TenantID, actor, domain.AdminActionEventRead, "event", eventID, "receipt"); err != nil {
		return domain.EventReceipt{}, err
	}
	return receipt, nil
}
