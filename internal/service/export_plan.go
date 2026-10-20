package service

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"fmt"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

var errExportPlanInvalid = fmt.Errorf("%w: invalid export execution plan", domain.ErrConflict)

func (s *Service) exportPlanBinding(job domain.ExportJob) string {
	return s.queryMAC("export.plan.scope.v1", s.exportCheckpointBinding(job))
}

func (s *Service) signExportPlan(plan store.ExportPlan) string {
	plan.Signature = ""
	return s.queryMAC("export.plan.members.v1", plan)
}

func (s *Service) exportPlanSchemaDigest(schema domain.EventSchema) string {
	return s.queryMAC("export.plan.schema.v1", schema)
}

func (s *Service) validExportPlan(job domain.ExportJob, plan store.ExportPlan, now time.Time) bool {
	if (plan.Version != 1 && plan.Version != 2) || plan.FormatVersion != 1 || job.ID == "" || job.TenantID == "" ||
		plan.JobID != job.ID || plan.TenantID != job.TenantID || job.Status != "running" || job.FinishedAt != nil ||
		job.CreatedAt.IsZero() || job.StartedAt == nil || job.StartedAt.IsZero() ||
		job.ObjectPath != "" || job.Digest != "" || job.EventCount != 0 ||
		plan.CreatedAt.IsZero() || plan.CreatedAt.Before(*job.StartedAt) || now.Before(plan.CreatedAt) ||
		len(plan.Members) > store.MaxExportPlanEvents ||
		!exportDigestPattern.MatchString(plan.Binding) || !exportDigestPattern.MatchString(plan.Signature) ||
		!hmac.Equal([]byte(plan.Binding), []byte(s.exportPlanBinding(job))) {
		return false
	}
	seen := make(map[string]bool, len(plan.Members))
	for i, member := range plan.Members {
		if member.EventID == "" || member.StreamID == "" || member.Sequence <= 0 || member.OccurredAt.IsZero() ||
			!exportDigestPattern.MatchString(member.Hash) || member.SchemaID == "" || member.SchemaVersion <= 0 ||
			!exportDigestPattern.MatchString(member.SchemaDigest) || seen[member.EventID] ||
			domain.ValidKeyComponent("event id", member.EventID) != nil || domain.ValidKeyComponent("stream id", member.StreamID) != nil ||
			(i > 0 && !exportPlanMemberBefore(plan.Members[i-1], member)) {
			return false
		}
		seen[member.EventID] = true
	}
	if !s.validExportGeneration(plan, now) {
		return false
	}
	raw, err := json.Marshal(plan)
	return err == nil && len(raw) <= store.MaxExportPlanBytes &&
		hmac.Equal([]byte(plan.Signature), []byte(s.signExportPlan(plan)))
}

func exportPlanMemberBefore(a, b store.ExportPlanMember) bool {
	if !a.OccurredAt.Equal(b.OccurredAt) {
		return a.OccurredAt.Before(b.OccurredAt)
	}
	return a.Sequence < b.Sequence || (a.Sequence == b.Sequence && a.EventID < b.EventID)
}

func copyExportPlan(plan store.ExportPlan) store.ExportPlan {
	plan.Members = append([]store.ExportPlanMember{}, plan.Members...)
	plan.Chunks = append([]store.ExportPlanChunk(nil), plan.Chunks...)
	return plan
}

func (s *Service) newExportPlan(job domain.ExportJob, events []domain.Event, schemas map[string]domain.EventSchema) (store.ExportPlan, error) {
	plan := store.ExportPlan{Version: 1, FormatVersion: 1, JobID: job.ID, TenantID: job.TenantID,
		Binding: s.exportPlanBinding(job), CreatedAt: s.Now().UTC(), Members: make([]store.ExportPlanMember, 0, len(events))}
	if len(events) > store.MaxExportPlanEvents {
		return store.ExportPlan{}, domain.ErrQuotaExceeded
	}
	for _, event := range events {
		schema := schemas[store.SchemaKey(job.TenantID, event.SchemaID, event.SchemaVersion)]
		if schema.TenantID != job.TenantID || schema.SchemaID != event.SchemaID || schema.Version != event.SchemaVersion {
			return store.ExportPlan{}, errExportPlanInvalid
		}
		plan.Members = append(plan.Members, store.ExportPlanMember{EventReference: store.EventReference{
			EventID: event.EventID, StreamID: event.StreamID, Sequence: event.Sequence, Hash: event.Hash, OccurredAt: event.OccurredAt.UTC()},
			SchemaID: event.SchemaID, SchemaVersion: event.SchemaVersion, SchemaDigest: s.exportPlanSchemaDigest(schema)})
	}
	if len(events) > store.ExportGenerationPageEvents {
		plan.Version = 2
	}
	plan.Signature = s.signExportPlan(plan)
	raw, err := json.Marshal(plan)
	// Reserve bounded reference metadata for every possible generated page,
	// within (not in addition to) the existing 4 MiB control-plan allowance.
	reserved := 0
	if plan.Version == 2 {
		reserved = ((len(events) + store.ExportGenerationPageEvents - 1) / store.ExportGenerationPageEvents) * 1024
	}
	if err != nil || len(raw)+reserved > store.MaxExportPlanBytes {
		return store.ExportPlan{}, fmt.Errorf("%w: export execution plan byte budget exceeded", domain.ErrQuotaExceeded)
	}
	if !s.validExportPlan(job, plan, s.Now()) {
		return store.ExportPlan{}, errExportPlanInvalid
	}
	return plan, nil
}

// The selected stage and its fact commit together. An unknown Save must not
// proceed to Put: a later expired owner reads the actual durable plan instead.
func (s *Service) saveExportPlan(ctx context.Context, claim exportClaim, plan store.ExportPlan) error {
	return s.Store.UpdateControlChecked(ctx, func(data *store.Snapshot) (bool, error) {
		job := data.Exports[claim.ID]
		if !ownsExportLease(data, job, claim, s.Now()) {
			return false, errExportLeaseLost
		}
		_, sealed := data.ExportCheckpoints[job.ID]
		if sealed || !s.validExportPlan(job, plan, s.Now()) || !s.exportPlanSchemasMatch(data, plan) || !exportCheckpointHasCapacity(data.Exports, job.TenantID) {
			return false, errExportPlanInvalid
		}
		if previous, exists := data.ExportPlans[job.ID]; exists {
			if !s.validExportPlan(job, previous, s.Now()) || previous.Signature != plan.Signature {
				return false, errExportPlanInvalid
			}
			return false, nil
		}
		data.ExportPlans[job.ID] = copyExportPlan(plan)
		s.appendAdminAction(data, s.adminAction(job.TenantID, "governance-worker", domain.AdminActionExportPlanned, "export", job.ID, fmt.Sprintf("stage=selected version=%d", plan.Version)))
		return true, nil
	})
}

func (s *Service) exportPlanMatches(data *store.Snapshot, job domain.ExportJob, claim exportClaim) bool {
	plan, exists := data.ExportPlans[job.ID]
	if !exists {
		return claim.Plan.Version == 0
	}
	return claim.Plan.Version != 0 && plan.Signature == claim.Plan.Signature &&
		s.validExportPlan(job, plan, s.Now()) && s.validExportPlan(job, claim.Plan, s.Now())
}

func (s *Service) checkExportPlan(ctx context.Context, claim exportClaim) error {
	return s.Store.ReadControl(ctx, func(data *store.Snapshot) error {
		job := data.Exports[claim.ID]
		if !ownsExportLease(data, job, claim, s.Now()) {
			return errExportLeaseLost
		}
		if !s.exportPlanMatches(data, job, claim) || (claim.Plan.Version != 0 && !s.exportPlanSchemasMatch(data, claim.Plan)) {
			return errExportPlanInvalid
		}
		return nil
	})
}

// Both artifacts are private and mutually exclusive. Unknown metadata is not
// silently cleared or treated as uncheckpointed work by recovery/cancellation.
func (s *Service) validExportArtifacts(data *store.Snapshot, job domain.ExportJob, now time.Time) error {
	plan, planned := data.ExportPlans[job.ID]
	checkpoint, sealed := data.ExportCheckpoints[job.ID]
	if planned && (sealed || !s.validExportPlan(job, plan, now)) {
		return errExportPlanInvalid
	}
	if sealed && !s.validExportCheckpoint(job, checkpoint, now) {
		return errExportCheckpointInvalid
	}
	return nil
}
