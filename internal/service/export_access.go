package service

import (
	"context"
	"fmt"
	"reflect"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// An access observation is request-local, never another persisted fact source
// or an execution claim. It cannot publish output or change a terminal job.
type exportAccess struct {
	Job         domain.ExportJob
	Hold        domain.LegalHold
	Blocked     bool
	HoldBinding string
}

func exportAccessJob(data *store.Snapshot, tenantID, jobID string) (domain.ExportJob, error) {
	job, ok := data.Exports[jobID]
	if !ok || job.TenantID != tenantID {
		return domain.ExportJob{}, domain.ErrNotFound
	}
	if job.ID != jobID {
		return domain.ExportJob{}, fmt.Errorf("%w: invalid export identity", domain.ErrConflict)
	}
	return job, nil
}

// Snapshot is a deep owned copy in every supported layout. Historical
// materialization remains, but remote Get, authentication and predicates run
// outside Store.mu so a slow status/download gate cannot block lease renewal.
// The caller supplies its one service operation context, including self-audit.
func (s *Service) captureExportAccess(ctx context.Context, tenantID, jobID string) (exportAccess, error) {
	data, err := s.Store.Snapshot(ctx)
	if err != nil {
		return exportAccess{}, err
	}
	job, err := exportAccessJob(data, tenantID, jobID)
	if err != nil {
		return exportAccess{}, err
	}
	hold, blocked, err := s.holdBlockingExportEvidence(ctx, data, job)
	if err != nil {
		return exportAccess{}, err
	}
	return exportAccess{Job: job, Hold: hold, Blocked: blocked, HoldBinding: s.exportHoldBinding(data, tenantID)}, nil
}

// Recheck after lock-free work, not by saving the old full snapshot. Only this
// tenant's active Hold set and job selection are bound; unrelated writes and
// heartbeats remain possible. Status polling can observe a newer lifecycle
// state for the same query. Download authorization and any captured terminal
// job instead require an exact record. There is no hidden retry or state reset.
func (s *Service) revalidateExportAccess(ctx context.Context, access exportAccess, exact bool) (domain.ExportJob, error) {
	var current domain.ExportJob
	err := s.Store.ReadControl(ctx, func(data *store.Snapshot) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		job, err := exportAccessJob(data, access.Job.TenantID, access.Job.ID)
		if err != nil {
			return err
		}
		original := access.Job
		selectionChanged := job.RequestedBy != original.RequestedBy || job.RetryOf != original.RetryOf ||
			!job.CreatedAt.Equal(original.CreatedAt) || !reflect.DeepEqual(job.Query, original.Query)
		terminal := original.Status != "pending" && original.Status != "running"
		backward := original.Status == "running" && job.Status == "pending"
		if selectionChanged || backward || ((exact || terminal) && !reflect.DeepEqual(job, original)) ||
			s.exportHoldBinding(data, job.TenantID) != access.HoldBinding {
			return fmt.Errorf("%w: export access changed during verification; refresh before retrying", domain.ErrConflict)
		}
		current = job
		return nil
	})
	if err != nil {
		return domain.ExportJob{}, err
	}
	return current, nil
}
