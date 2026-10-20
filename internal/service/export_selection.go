package service

import (
	"context"
	"fmt"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func (s *Service) selectExportEvidence(ctx context.Context, job domain.ExportJob) ([]domain.Event, domain.LegalHold, error) {
	events, hold, _, err := s.selectExportEvidenceSnapshot(ctx, job)
	return events, hold, err
}

// Capture an independent snapshot under Store.mu, then authenticate BEFORE
// filtering. Derived search sidecars cannot silently alter durable membership.
// Historical scan cost remains. Get never retains Store.mu and all reads share
// one operation deadline, including callers constructing a persistent plan.
func (s *Service) selectExportEvidenceSnapshot(ctx context.Context, job domain.ExportJob) ([]domain.Event, domain.LegalHold, *store.Snapshot, error) {
	var events []domain.Event
	var blocker domain.LegalHold
	var data *store.Snapshot
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		var err error
		data, err = s.Store.Snapshot(opCtx)
		if err != nil {
			return err
		}
		hold, blocked, err := s.holdBlockingExportEvidence(opCtx, data, job)
		if err != nil {
			return err
		}
		if blocked {
			blocker = hold
			return nil
		}
		for key, receipt := range data.Receipts {
			if err := opCtx.Err(); err != nil {
				return err
			}
			if receipt.TenantID != job.TenantID {
				continue
			}
			if key != store.EventKey(job.TenantID, receipt.EventID) {
				return errExportPlanInvalid
			}
			candidate := queryEvidence{receipt: receipt, schemas: data.Schemas}
			if event, hot := data.Events[key]; hot {
				candidate.event = &event
			} else if receipt.Status != domain.StatusArchived {
				return domain.ErrNotFound
			}
			event, err := s.resolveQueryEvidence(opCtx, candidate)
			if err != nil {
				return err
			}
			schema := data.Schemas[store.SchemaKey(job.TenantID, event.SchemaID, event.SchemaVersion)]
			if schema.TenantID != job.TenantID || schema.SchemaID != event.SchemaID || schema.Version != event.SchemaVersion {
				return errExportPlanInvalid
			}
			source, err := s.reconstructEventForDigest(event, data.Schemas)
			if err != nil {
				return err
			}
			selected, err := s.matchesExportHoldEvidence(source, schema, job.Query)
			if err != nil {
				return err
			}
			if selected {
				events = append(events, event)
			}
			if len(events) > store.MaxExportPlanEvents {
				return fmt.Errorf("%w: export evidence budget exceeded; narrow filters", domain.ErrQuotaExceeded)
			}
		}
		sortEvents(events)
		return opCtx.Err()
	})
	return events, blocker, data, err
}
