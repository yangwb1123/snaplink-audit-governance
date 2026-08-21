package service

import (
	"fmt"
	"sort"

	"github.com/snaplink/audit-governance/internal/domain"
)

// operationTimelineEventsNoAudit is the un-audited source shared by the
// bounded legacy method, the paged API, and replay. Keeping the source read
// separate lets replay retain its safety cap while HTTP clients walk large
// timelines page by page.
func (s *Service) operationTimelineEventsNoAudit(tenantID, operationID string) ([]domain.Event, error) {
	events, err := s.eventsFor(tenantID, func(event domain.Event) bool { return event.OperationID == operationID })
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, domain.ErrNotFound
	}
	sortEvents(events)
	return events, nil
}

// operationTimelineNoAudit is the un-audited core shared by the bounded
// compatibility method and ReplayOperation, so a single replay call appends
// exactly one fact (FR-3: no delegation path may append twice).
func (s *Service) operationTimelineNoAudit(tenantID, operationID string) ([]domain.Event, error) {
	events, err := s.operationTimelineEventsNoAudit(tenantID, operationID)
	if err != nil {
		return nil, err
	}
	if len(events) > domain.MaxTimelineEvents {
		return nil, fmt.Errorf("%w: operation timeline exceeds %d events; use query or export", domain.ErrInvalid, domain.MaxTimelineEvents)
	}
	return events, nil
}

// aggregateTimelineEventsNoAudit mirrors operationTimelineEventsNoAudit for
// aggregates. Aggregate-version ordering is the domain order for this path.
func (s *Service) aggregateTimelineEventsNoAudit(tenantID, aggregateType, aggregateID string) ([]domain.Event, error) {
	events, err := s.eventsFor(tenantID, func(event domain.Event) bool {
		return event.AggregateType == aggregateType && event.AggregateID == aggregateID
	})
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, domain.ErrNotFound
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].AggregateVersion == events[j].AggregateVersion {
			return events[i].EventID < events[j].EventID
		}
		return events[i].AggregateVersion < events[j].AggregateVersion
	})
	return events, nil
}

// aggregateTimelineNoAudit preserves the bounded behavior of the original
// service method and keeps replay fail-closed for unexpectedly huge inputs.
func (s *Service) aggregateTimelineNoAudit(tenantID, aggregateType, aggregateID string) ([]domain.Event, error) {
	events, err := s.aggregateTimelineEventsNoAudit(tenantID, aggregateType, aggregateID)
	if err != nil {
		return nil, err
	}
	if len(events) > domain.MaxTimelineEvents {
		return nil, fmt.Errorf("%w: aggregate timeline exceeds %d events; use query or export", domain.ErrInvalid, domain.MaxTimelineEvents)
	}
	return events, nil
}

func timelinePageSize(pageSize int) (int, error) {
	if pageSize <= 0 {
		return domain.DefaultPageSize, nil
	}
	if pageSize > domain.MaxPageSize {
		return 0, fmt.Errorf("%w: page_size exceeds %d", domain.ErrInvalid, domain.MaxPageSize)
	}
	return pageSize, nil
}

func paginateTimeline(events []domain.Event, kind, scope, cursor string, pageSize int) (domain.QueryResult, error) {
	pageSize, err := timelinePageSize(pageSize)
	if err != nil {
		return domain.QueryResult{}, err
	}
	start := 0
	var decoded domain.TimelineCursor
	if cursor != "" {
		decoded, err = domain.DecodeTimelineCursor(cursor)
		if err != nil {
			return domain.QueryResult{}, err
		}
		if decoded.Kind != kind || decoded.Scope != scope {
			return domain.QueryResult{}, fmt.Errorf("%w: cursor belongs to a different timeline", domain.ErrInvalid)
		}
		for start < len(events) && !afterTimelineCursor(events[start], decoded) {
			start++
		}
	}
	result := domain.QueryResult{Count: len(events)}
	remaining := events[start:]
	if len(remaining) <= pageSize {
		result.Items = remaining
		return result, nil
	}
	result.Items = remaining[:pageSize]
	last := result.Items[len(result.Items)-1]
	result.NextCursor = domain.EncodeTimelineCursor(timelineCursorFor(kind, scope, last))
	return result, nil
}

func afterTimelineCursor(event domain.Event, cursor domain.TimelineCursor) bool {
	if cursor.Kind == "aggregate" {
		if event.AggregateVersion != cursor.AggregateVersion {
			return event.AggregateVersion > cursor.AggregateVersion
		}
		return event.EventID > cursor.EventID
	}
	if !event.OccurredAt.Equal(cursor.OccurredAt) {
		return event.OccurredAt.After(cursor.OccurredAt)
	}
	if event.Sequence != cursor.Sequence {
		return event.Sequence > cursor.Sequence
	}
	return event.EventID > cursor.EventID
}

func timelineCursorFor(kind, scope string, event domain.Event) domain.TimelineCursor {
	return domain.TimelineCursor{Kind: kind, Scope: scope, OccurredAt: event.OccurredAt, AggregateVersion: event.AggregateVersion, Sequence: event.Sequence, EventID: event.EventID}
}

// OperationTimelinePage returns one deterministic operation page and records
// exactly one read fact for the page request. A page walk therefore remains
// auditable without exposing an unbounded response body.
func (s *Service) OperationTimelinePage(tenantID, actor, operationID string, pageSize int, cursor string) (domain.QueryResult, error) {
	events, err := s.operationTimelineEventsNoAudit(tenantID, operationID)
	if err != nil {
		return domain.QueryResult{}, err
	}
	result, err := paginateTimeline(events, "operation", operationID, cursor, pageSize)
	if err != nil {
		return domain.QueryResult{}, err
	}
	if err := s.recordReadAction(tenantID, actor, domain.AdminActionEventRead, "operation", operationID, "timeline"); err != nil {
		return domain.QueryResult{}, err
	}
	return result, nil
}

// AggregateTimelinePage is the aggregate counterpart to OperationTimelinePage.
// Its cursor follows aggregate_version/event_id ordering, not chronological
// occurred_at ordering used by event queries and operation timelines.
func (s *Service) AggregateTimelinePage(tenantID, actor, aggregateType, aggregateID string, pageSize int, cursor string) (domain.QueryResult, error) {
	events, err := s.aggregateTimelineEventsNoAudit(tenantID, aggregateType, aggregateID)
	if err != nil {
		return domain.QueryResult{}, err
	}
	scope := aggregateType + "\x1f" + aggregateID
	result, err := paginateTimeline(events, "aggregate", scope, cursor, pageSize)
	if err != nil {
		return domain.QueryResult{}, err
	}
	if err := s.recordReadAction(tenantID, actor, domain.AdminActionEventRead, "aggregate", aggregateID, aggregateType); err != nil {
		return domain.QueryResult{}, err
	}
	return result, nil
}

func (s *Service) OperationTimeline(tenantID, actor, operationID string) ([]domain.Event, error) {
	events, err := s.operationTimelineNoAudit(tenantID, operationID)
	if err != nil {
		return nil, err
	}
	// Read self-audit (F-06): append-before-serve, fail-closed — a timeline
	// read that cannot leave its governance fact is not served.
	if err := s.recordReadAction(tenantID, actor, domain.AdminActionEventRead, "operation", operationID, "timeline"); err != nil {
		return nil, err
	}
	return events, nil
}

func (s *Service) AggregateTimeline(tenantID, actor, aggregateType, aggregateID string) ([]domain.Event, error) {
	events, err := s.aggregateTimelineNoAudit(tenantID, aggregateType, aggregateID)
	if err != nil {
		return nil, err
	}
	if err := s.recordReadAction(tenantID, actor, domain.AdminActionEventRead, "aggregate", aggregateID, aggregateType); err != nil {
		return nil, err
	}
	return events, nil
}
