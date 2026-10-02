package service

import (
	"context"
	"sort"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// ResumePendingExports schedules durable export jobs that survived an API
// restart before their in-process worker started. runExport claims each job
// with a CAS transition, so an API worker and a governance worker cannot both
// execute the same pending job.
func (s *Service) ResumePendingExports(ctx context.Context, tenantID string) (int, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return 0, err
	}
	var jobIDs []string
	if err := s.Store.Read(ctx, func(data *store.Snapshot) error {
		for jobID, job := range data.Exports {
			if job.TenantID == tenantID && job.Status == "pending" {
				jobIDs = append(jobIDs, jobID)
			}
		}
		return nil
	}); err != nil {
		return 0, err
	}
	sort.Strings(jobIDs)
	for _, jobID := range jobIDs {
		go s.runExport(ctx, jobID)
	}
	return len(jobIDs), nil
}

func (s *Service) claimPendingExport(ctx context.Context, jobID string) (domain.ExportJob, bool, error) {
	var job domain.ExportJob
	claimed := false
	err := s.Store.UpdateChecked(ctx, func(data *store.Snapshot) (bool, error) {
		job = domain.ExportJob{}
		claimed = false
		value, ok := data.Exports[jobID]
		if !ok || value.Status != "pending" {
			return false, nil
		}
		value.Status = "running"
		data.Exports[jobID] = value
		job = value
		claimed = true
		return true, nil
	})
	return job, claimed, err
}
