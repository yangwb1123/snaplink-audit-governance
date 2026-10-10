package service

import (
	"context"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// ResumePendingExports schedules durable export jobs that survived an API
// restart before their in-process worker started. The returned count is the
// observed tenant backlog, NOT a claim count. A bounded fair dispatcher claims
// shared durable work by CAS; no goroutine is allocated per pending record.
func (s *Service) ResumePendingExports(ctx context.Context, tenantID string) (int, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return 0, err
	}
	backlog := 0
	if err := s.Store.ReadControl(ctx, func(data *store.Snapshot) error {
		for _, job := range data.Exports {
			if job.TenantID == tenantID && job.Status == "pending" {
				backlog++
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

func (s *Service) claimPendingExport(ctx context.Context, jobID string) (domain.ExportJob, bool, error) {
	var job domain.ExportJob
	claimed := false
	err := s.Store.UpdateChecked(ctx, func(data *store.Snapshot) (bool, error) {
		job = domain.ExportJob{}
		claimed = false
		value, ok := data.Exports[jobID]
		if !ok || value.Status != "pending" || !exportClaimHasCapacity(data.Exports, value.TenantID) {
			return false, nil
		}
		now := s.Now()
		value.Status = "running"
		value.StartedAt = &now
		data.Exports[jobID] = value
		job = value
		claimed = true
		return true, nil
	})
	return job, claimed, err
}
