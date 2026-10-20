package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func exportActionScope(tenantID, actor, jobID string) error {
	if err := requireTenantScope(tenantID); err != nil {
		return err
	}
	if strings.TrimSpace(actor) == "" || strings.TrimSpace(jobID) == "" {
		return fmt.Errorf("%w: actor and export id are required", domain.ErrInvalid)
	}
	return nil
}

// Owner control is deliberately narrower than tenant-wide export read/create.
// Unknown legacy requesters do not become owned by a platform/admin caller.
func ownedExport(data *store.Snapshot, tenantID, actor, jobID string) (domain.ExportJob, error) {
	job, ok := data.Exports[jobID]
	if !ok || job.TenantID != tenantID {
		return domain.ExportJob{}, domain.ErrNotFound
	}
	if job.RequestedBy == "" || job.RequestedBy != actor {
		return domain.ExportJob{}, domain.ErrForbidden
	}
	if job.ID != jobID {
		return domain.ExportJob{}, fmt.Errorf("%w: export control identity mismatch", domain.ErrConflict)
	}
	return job, nil
}

// CancelExport revokes the durable claim, not the external I/O. No evidence
// read or Hold bypass: only safe operational summary is returned, and no WORM
// object is deleted. Other replicas observe revocation through their heartbeat
// and terminal fence. A repeated same-owner cancellation is a zero-write no-op.
func (s *Service) CancelExport(ctx context.Context, tenantID, actor, jobID string) (domain.ExportSummary, error) {
	if err := exportActionScope(tenantID, actor, jobID); err != nil {
		return domain.ExportSummary{}, err
	}
	var result domain.ExportSummary
	cancelled := false
	err := s.Store.UpdateControlChecked(ctx, func(data *store.Snapshot) (bool, error) {
		result = domain.ExportSummary{}
		cancelled = false
		job, err := ownedExport(data, tenantID, actor, jobID)
		if err != nil {
			return false, err
		}
		if job.Status == domain.ExportStatusCancelled {
			if job.FinishedAt == nil || job.FinishedAt.IsZero() || job.ObjectPath != "" || job.Digest != "" || job.EventCount != 0 {
				return false, fmt.Errorf("%w: invalid cancelled export", domain.ErrConflict)
			}
			_, checkpointed := data.ExportCheckpoints[jobID]
			_, planned := data.ExportPlans[jobID]
			if _, leased := data.ExportLeases[jobID]; leased || checkpointed || planned {
				return false, fmt.Errorf("%w: cancelled export still has execution metadata", domain.ErrConflict)
			}
			result = domain.SummarizeExport(job)
			return false, nil
		}
		if job.FinishedAt != nil || (job.Status != "pending" && job.Status != "running") {
			return false, fmt.Errorf("%w: export is not cancellable", domain.ErrConflict)
		}
		lease, leased := data.ExportLeases[jobID]
		if err := s.validExportArtifacts(data, job, s.Now()); err != nil {
			return false, err
		}
		if (job.Status == "pending" && leased) || (job.Status == "running" && (!leased || !validExportLease(job, lease))) {
			return false, fmt.Errorf("%w: export cancellation requires valid execution metadata", domain.ErrConflict)
		}
		now := s.Now()
		job.Status, job.FinishedAt = domain.ExportStatusCancelled, &now
		job.FailureCode, job.Error = domain.ExportFailureCancelled, ""
		job.ObjectPath, job.Digest, job.EventCount = "", "", 0
		data.Exports[jobID] = job
		delete(data.ExportLeases, jobID)
		delete(data.ExportCheckpoints, jobID)
		delete(data.ExportPlans, jobID)
		s.appendAdminAction(data, s.adminAction(tenantID, actor, domain.AdminActionExportCancelled, "export", jobID, "status=cancelled"))
		result, cancelled = domain.SummarizeExport(job), true
		return true, nil
	})
	if err != nil {
		return domain.ExportSummary{}, err // Save may be unknown, never a success body
	}
	if cancelled {
		s.wakeExports(context.WithoutCancel(ctx))
	}
	return result, nil
}
