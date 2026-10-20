package service

import (
	"context"
	"fmt"
	"sort"

	"github.com/snaplink/audit-governance/internal/domain"
)

// operationTimelineEventsNoAudit authenticates owned committed evidence before
// selection, outside Store.mu, without adding a delegated read fact.
func (s *Service) operationTimelineEventsNoAudit(ctx context.Context, tenantID, operationID string) ([]domain.Event, error) {
	events, err := s.readTimelineEvidence(ctx, tenantID, func(event domain.Event) bool { return event.OperationID == operationID })
	if err != nil {
		return nil, err
	}
	sortEvents(events)
	return events, nil
}

// operationTimelineNoAudit preserves the legacy whole-timeline safety cap.
// Operation replay independently retains the same cap and one read fact.
func (s *Service) operationTimelineNoAudit(ctx context.Context, tenantID, operationID string) ([]domain.Event, error) {
	events, err := s.operationTimelineEventsNoAudit(ctx, tenantID, operationID)
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
func (s *Service) aggregateTimelineEventsNoAudit(ctx context.Context, tenantID, aggregateType, aggregateID string) ([]domain.Event, error) {
	events, err := s.readTimelineEvidence(ctx, tenantID, func(event domain.Event) bool {
		return event.AggregateType == aggregateType && event.AggregateID == aggregateID
	})
	if err != nil {
		return nil, err
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
func (s *Service) aggregateTimelineNoAudit(ctx context.Context, tenantID, aggregateType, aggregateID string) ([]domain.Event, error) {
	events, err := s.aggregateTimelineEventsNoAudit(ctx, tenantID, aggregateType, aggregateID)
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
func (s *Service) OperationTimelinePage(ctx context.Context, tenantID, actor, operationID string, pageSize int, cursor string) (domain.QueryResult, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.QueryResult{}, err
	}
	var result domain.QueryResult
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		events, err := s.readTimelineEvidence(opCtx, tenantID, func(event domain.Event) bool { return event.OperationID == operationID })
		if err != nil {
			return err
		}
		sortEvents(events)
		result, err = paginateTimeline(events, "operation", operationID, cursor, pageSize)
		if err != nil {
			return err
		}
		if err := s.recordReadAction(opCtx, tenantID, actor, domain.AdminActionEventRead, "operation", operationID, "timeline"); err != nil {
			return err
		}
		return opCtx.Err()
	})
	if err != nil {
		return domain.QueryResult{}, err
	}
	return result, nil
}

// AggregateTimelinePage is the aggregate counterpart to OperationTimelinePage.
// Its cursor follows aggregate_version/event_id ordering, not chronological
// occurred_at ordering used by event queries and operation timelines.
func (s *Service) AggregateTimelinePage(ctx context.Context, tenantID, actor, aggregateType, aggregateID string, pageSize int, cursor string) (domain.QueryResult, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.QueryResult{}, err
	}
	var result domain.QueryResult
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		events, err := s.readTimelineEvidence(opCtx, tenantID, func(event domain.Event) bool {
			return event.AggregateType == aggregateType && event.AggregateID == aggregateID
		})
		if err != nil {
			return err
		}
		sort.Slice(events, func(i, j int) bool {
			if events[i].AggregateVersion == events[j].AggregateVersion {
				return events[i].EventID < events[j].EventID
			}
			return events[i].AggregateVersion < events[j].AggregateVersion
		})
		scope := aggregateType + "\x1f" + aggregateID
		result, err = paginateTimeline(events, "aggregate", scope, cursor, pageSize)
		if err != nil {
			return err
		}
		if err := s.recordReadAction(opCtx, tenantID, actor, domain.AdminActionEventRead, "aggregate", aggregateID, aggregateType); err != nil {
			return err
		}
		return opCtx.Err()
	})
	if err != nil {
		return domain.QueryResult{}, err
	}
	return result, nil
}

func (s *Service) OperationTimeline(ctx context.Context, tenantID, actor, operationID string) ([]domain.Event, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return nil, err
	}
	var events []domain.Event
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		var err error
		events, err = s.operationTimelineNoAudit(opCtx, tenantID, operationID)
		if err != nil {
			return err
		}
		// Append-before-serve once, under the original capture/Get budget.
		if err := s.recordReadAction(opCtx, tenantID, actor, domain.AdminActionEventRead, "operation", operationID, "timeline"); err != nil {
			return err
		}
		return opCtx.Err()
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

func (s *Service) AggregateTimeline(ctx context.Context, tenantID, actor, aggregateType, aggregateID string) ([]domain.Event, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return nil, err
	}
	var events []domain.Event
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		var err error
		events, err = s.aggregateTimelineNoAudit(opCtx, tenantID, aggregateType, aggregateID)
		if err != nil {
			return err
		}
		if err := s.recordReadAction(opCtx, tenantID, actor, domain.AdminActionEventRead, "aggregate", aggregateID, aggregateType); err != nil {
			return err
		}
		return opCtx.Err()
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}
