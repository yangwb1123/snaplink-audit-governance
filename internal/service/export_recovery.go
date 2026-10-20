package service

import (
	"context"
	"crypto/rand"
	"fmt"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// ResumePendingExports schedules durable export jobs that survived an API
// restart before their in-process worker started. The returned count is the
// observed tenant backlog (pending plus expired selected/sealed checkpoints), NOT a
// claim count. A bounded fair dispatcher claims
// shared durable work by CAS; no goroutine is allocated per pending record.
func (s *Service) ResumePendingExports(ctx context.Context, tenantID string) (int, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return 0, err
	}
	backlog := 0
	if err := s.Store.ReadControl(ctx, func(data *store.Snapshot) error {
		for id, job := range data.Exports {
			if job.TenantID != tenantID {
				continue
			}
			if job.Status == "pending" {
				backlog++
			} else if job.Status == "running" {
				_, sealed := data.ExportCheckpoints[id]
				_, planned := data.ExportPlans[id]
				if !sealed && !planned {
					continue
				}
				lease := data.ExportLeases[id]
				if job.ID != id || !validExportLease(job, lease) {
					return errExportPlanInvalid
				}
				if err := s.validExportArtifacts(data, job, s.Now()); err != nil {
					return err
				}
				if !lease.ExpiresAt.After(s.Now()) {
					backlog++
				}
			}
		}
		return nil
	}); err != nil {
		return 0, err
	}
	if backlog != 0 {
		s.wakeExports(ctx)
	}
	return backlog, nil
}

func (s *Service) claimPendingExport(ctx context.Context, jobID string) (exportClaim, bool, error) {
	return s.claimExport(ctx, jobID, false)
}

func (s *Service) claimExport(ctx context.Context, jobID string, allowCheckpoint bool) (exportClaim, bool, error) {
	var job exportClaim
	claimed := false
	owner := rand.Text()
	err := s.Store.UpdateControlChecked(ctx, func(data *store.Snapshot) (bool, error) {
		job = exportClaim{}
		claimed = false
		value, ok := data.Exports[jobID]
		if !ok || (value.Status != "pending" && (!allowCheckpoint || value.Status != "running")) {
			return false, nil
		}
		if value.ID != jobID || value.TenantID == "" || value.FinishedAt != nil {
			return false, fmt.Errorf("%w: invalid export identity or terminal marker", domain.ErrConflict)
		}
		now := s.Now()
		checkpoint, checkpointed := data.ExportCheckpoints[jobID]
		plan, planned := data.ExportPlans[jobID]
		if value.Status == "running" {
			if !checkpointed && !planned {
				return false, nil
			}
			lease := data.ExportLeases[jobID]
			if !validExportLease(value, lease) {
				return false, errExportPlanInvalid
			}
			if err := s.validExportArtifacts(data, value, now); err != nil {
				return false, err
			}
			if lease.ExpiresAt.After(now) || !exportCheckpointHasCapacity(data.Exports, value.TenantID) {
				return false, nil
			}
			detail := "stage=sealed version=1"
			if planned {
				detail = fmt.Sprintf("stage=selected version=%d", plan.Version)
				if len(plan.Chunks) != 0 {
					detail = "stage=generating version=1"
				}
			}
			s.appendAdminAction(data, s.adminAction(value.TenantID, "governance-worker", domain.AdminActionExportResumed, "export", value.ID, detail))
		} else {
			if _, exists := data.ExportLeases[jobID]; exists || checkpointed || planned {
				return false, fmt.Errorf("%w: pending export has unexpected execution metadata", domain.ErrConflict)
			}
			if !exportClaimHasCapacity(data.Exports, value.TenantID) {
				return false, nil
			}
			value.Status = "running"
			value.StartedAt = &now
		}
		data.Exports[jobID] = value
		data.ExportLeases[jobID] = store.ExportLease{Version: 1, JobID: jobID, TenantID: value.TenantID,
			Owner: owner, HeartbeatAt: now, ExpiresAt: now.Add(ExportLeaseDuration)}
		job = exportClaim{ExportJob: value, Owner: owner, Checkpoint: checkpoint, Plan: copyExportPlan(plan)}
		claimed = true
		return true, nil
	})
	if err != nil {
		return exportClaim{}, false, err
	}
	return job, claimed, nil
}
