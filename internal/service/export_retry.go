package service

import (
	"context"
	"fmt"
	"reflect"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func retryableExport(job domain.ExportJob) bool {
	return (job.Status == "failed" || job.Status == domain.ExportStatusCancelled) &&
		job.FinishedAt != nil && !job.FinishedAt.IsZero()
}

func ownedRetrySource(data *store.Snapshot, tenantID, actor, jobID string) (domain.ExportJob, error) {
	job, err := ownedExport(data, tenantID, actor, jobID)
	if err != nil {
		return domain.ExportJob{}, err
	}
	_, leased := data.ExportLeases[jobID]
	_, checkpointed := data.ExportCheckpoints[jobID]
	_, planned := data.ExportPlans[jobID]
	if !retryableExport(job) || leased || checkpointed || planned {
		return domain.ExportJob{}, fmt.Errorf("%w: export is not retryable", domain.ErrConflict)
	}
	return job, nil
}

// There is at most one successor per original job in this control CAS. Retries
// of a failed successor name that successor explicitly, forming a lineage;
// repeating an unknown-result request must not create another child.
func exportRetrySuccessor(ctx context.Context, data *store.Snapshot, source domain.ExportJob) (domain.ExportSummary, bool, error) {
	var result domain.ExportSummary
	found := false
	query := source.Query
	query.Cursor = "" // fresh selection, never revive the old fixed member set
	for key, child := range data.Exports {
		if err := ctx.Err(); err != nil {
			return domain.ExportSummary{}, false, err
		}
		if child.RetryOf != source.ID {
			continue
		}
		if found || child.ID != key || child.ID == source.ID || child.TenantID != source.TenantID || child.RequestedBy != source.RequestedBy || !reflect.DeepEqual(child.Query, query) {
			return domain.ExportSummary{}, false, fmt.Errorf("%w: invalid export retry linkage", domain.ErrConflict)
		}
		result, found = domain.SummarizeExport(child), true
	}
	return result, found, nil
}

// RetryExport never changes the original terminal job. A new child re-runs
// the same audited read/Hold preflight and the shared queue admission. The
// existing child result is metadata only: no evidence/Hold gate is bypassed.
func (s *Service) RetryExport(ctx context.Context, tenantID, actor, jobID string) (domain.ExportSummary, error) {
	if err := exportActionScope(tenantID, actor, jobID); err != nil {
		return domain.ExportSummary{}, err
	}
	var result domain.ExportSummary
	created := false
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		var source domain.ExportJob
		found := false
		if err := s.Store.ReadControl(opCtx, func(data *store.Snapshot) error {
			var err error
			source, err = ownedRetrySource(data, tenantID, actor, jobID)
			if err != nil {
				return err
			}
			result, found, err = exportRetrySuccessor(opCtx, data, source)
			return err
		}); err != nil || found {
			return err
		}
		query := source.Query
		query.Cursor = ""
		holdBinding, err := s.prepareExport(opCtx, tenantID, actor, query)
		if err != nil {
			return err
		}
		child := domain.ExportJob{ID: newID("export"), TenantID: tenantID, RequestedBy: actor,
			Query: query, Status: "pending", RetryOf: source.ID, CreatedAt: s.Now()}
		return s.Store.UpdateControlChecked(opCtx, func(data *store.Snapshot) (bool, error) {
			created = false // CAS callbacks can be replayed
			result = domain.ExportSummary{}
			current, err := ownedRetrySource(data, tenantID, actor, jobID)
			if err != nil {
				return false, err
			}
			if !reflect.DeepEqual(source, current) {
				return false, fmt.Errorf("%w: export changed during retry preflight", domain.ErrConflict)
			}
			var found bool
			result, found, err = exportRetrySuccessor(opCtx, data, current)
			if err != nil || found {
				return false, err
			}
			// An existing successor is metadata-only. Only creation needs
			// the unchanged authenticated current-Hold observation.
			if err := s.checkExportPreflight(data, tenantID, holdBinding); err != nil {
				return false, err
			}
			if !exportQueueHasCapacity(data.Exports, tenantID) {
				return false, fmt.Errorf("%w: export queue capacity exhausted", domain.ErrQuotaExceeded)
			}
			if _, exists := data.Exports[child.ID]; exists {
				return false, fmt.Errorf("%w: export retry identity collision", domain.ErrConflict)
			}
			data.Exports[child.ID] = child
			s.appendAdminAction(data, s.adminAction(tenantID, actor, domain.AdminActionExportCreated, "export", child.ID, "retry_of="+source.ID))
			s.appendAdminAction(data, s.adminAction(tenantID, actor, domain.AdminActionExportRetried, "export", child.ID, "retry_of="+source.ID))
			result, created = domain.SummarizeExport(child), true
			return true, nil
		})
	})
	if err != nil {
		return domain.ExportSummary{}, err
	}
	if created {
		s.wakeExports(context.WithoutCancel(ctx))
	}
	return result, nil
}
