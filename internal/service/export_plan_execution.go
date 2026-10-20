package service

import (
	"context"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// A restart skips selection. It regenerates only the not-yet-checkpointed
// whole JSONL/sealing stage from exact original members; it is not chunk-level
// byte progress. No short-lived query cursor is consumed here.
func (s *Service) exportExecutionEvidence(ctx context.Context, claim exportClaim) (exportClaim, []domain.Event, bool) {
	if claim.Plan.Version == 2 {
		// The page executor authenticates committed progress and only reads
		// ungenerated members; never materialize/regenerate the whole plan.
		return claim, nil, true
	}
	var events []domain.Event
	var blocker domain.LegalHold
	var plan store.ExportPlan
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		if claim.Plan.Version != 0 {
			if err := s.checkExportPlan(opCtx, claim); err != nil {
				return err
			}
			var err error
			events, err = s.readExportPlanEvidence(opCtx, claim)
			if err != nil {
				return err
			}
			data, err := s.Store.Snapshot(opCtx)
			if err != nil {
				return err
			}
			blocker, _, err = s.holdBlockingExportEvidence(opCtx, data, claim.ExportJob)
			if err != nil || blocker.ID != "" {
				return err
			}
			if err := s.recordReadAction(opCtx, claim.TenantID, "governance-worker", domain.AdminActionEventRead, "export", claim.ID, "stage=selected verification"); err != nil {
				return err
			}
			return s.checkExportPlan(opCtx, claim)
		}
		var data *store.Snapshot
		var err error
		events, blocker, data, err = s.selectExportEvidenceSnapshot(opCtx, claim.ExportJob)
		if err != nil || blocker.ID != "" {
			return err
		}
		candidate, err := s.newExportPlan(claim.ExportJob, events, data.Schemas)
		if err != nil {
			return err
		}
		if err := s.recordReadAction(opCtx, claim.TenantID, "governance-worker", domain.AdminActionEventRead, "export", claim.ID, "stage=selected verification"); err != nil {
			return err
		}
		plan = candidate // marks the actual Save/unknown-result boundary
		return s.saveExportPlan(opCtx, claim, plan)
	})
	if err != nil {
		if claim.Plan.Version != 0 || plan.Version != 0 {
			// Missing/unavailable/currently unauthentic source evidence and
			// unknown control writes never become empty output or a reselection.
			s.logf("export plan execution deferred job_id=%s category=verification", claim.ID)
		} else {
			s.finishExport(ctx, claim.ID, "failed", "", "", 0, err.Error())
		}
		return claim, nil, false
	}
	if blocker.ID != "" {
		s.failExportBlocked(ctx, claim.ID, blocker)
		return claim, nil, false
	}
	if plan.Version != 0 {
		claim.Plan = plan
	}
	if claim.Plan.Version == 2 {
		events = nil // selection is durable; page reads have their own bounds
	}
	return claim, events, true
}

func (s *Service) readExportPlanEvidence(ctx context.Context, claim exportClaim) ([]domain.Event, error) {
	return s.readExportPlanMembers(ctx, claim, claim.Plan.Members)
}

func (s *Service) readExportPlanMembers(ctx context.Context, claim exportClaim, members []store.ExportPlanMember) ([]domain.Event, error) {
	candidates := make([]queryEvidence, 0, len(members))
	err := s.Store.ReadTenant(ctx, claim.TenantID, func(view *store.TenantView) error {
		for _, member := range members {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := store.EventKey(claim.TenantID, member.EventID)
			receipt, exists := view.Ledger.Receipt(key)
			if err := view.Ledger.Err(); err != nil {
				return err
			}
			if !exists || receipt.TenantID != claim.TenantID || receipt.EventID != member.EventID ||
				receipt.StreamID != member.StreamID || receipt.Sequence != member.Sequence || receipt.Hash != member.Hash {
				return errExportPlanInvalid
			}
			schemaKey := store.SchemaKey(claim.TenantID, member.SchemaID, member.SchemaVersion)
			schema := view.Global.Schemas[schemaKey]
			if schema.TenantID != claim.TenantID || schema.SchemaID != member.SchemaID || schema.Version != member.SchemaVersion ||
				s.exportPlanSchemaDigest(schema) != member.SchemaDigest {
				return errExportPlanInvalid
			}
			candidate := queryEvidence{receipt: receipt, schemas: map[string]domain.EventSchema{schemaKey: schema}}
			if event, hot := view.Hot.Events[key]; hot {
				cloned, err := domain.CloneEvent(event)
				if err != nil {
					return err
				}
				candidate.event = &cloned
			} else if receipt.Status != domain.StatusArchived {
				return domain.ErrNotFound
			}
			candidates = append(candidates, candidate)
		}
		return view.Ledger.Err()
	})
	if err != nil {
		return nil, err
	}
	events := make([]domain.Event, 0, len(candidates))
	for i, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		event, err := s.resolveQueryEvidence(ctx, candidate)
		if err != nil {
			return nil, err
		}
		member := members[i]
		if event.SchemaID != member.SchemaID || event.SchemaVersion != member.SchemaVersion || !event.OccurredAt.Equal(member.OccurredAt) {
			return nil, errExportPlanInvalid
		}
		events = append(events, event)
	}
	return events, nil
}
