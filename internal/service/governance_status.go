package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// GovernanceStatusAccess is supplied by the authenticated transport, never
// by request parameters. Section visibility follows the existing list/policy
// permissions; a decision count additionally needs restore read permission.
type GovernanceStatusAccess struct {
	Exports          bool
	Restores         bool
	RestoreDecisions bool
	LegalHolds       bool
	Retention        bool
}

// GovernanceStatus reads only control metadata and records one fail-closed
// read fact. No evidence selection, Hold overlap inference or remote IO occurs.
// ReadControl and self-audit share a single operation deadline, without a
// nested Store method inside a closure. Legacy v1 still loads its old snapshot.
func (s *Service) GovernanceStatus(ctx context.Context, tenantID, actor string, access GovernanceStatusAccess) (domain.GovernanceStatus, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.GovernanceStatus{}, err
	}
	if actor == "" {
		return domain.GovernanceStatus{}, fmt.Errorf("%w: actor is required", domain.ErrInvalid)
	}
	sections := governanceStatusSections(access)
	if sections == "" {
		return domain.GovernanceStatus{}, domain.ErrForbidden
	}
	result := domain.GovernanceStatus{TenantID: tenantID, Source: "control", Completeness: "not_asserted"}
	err := s.Store.WithOperation(ctx, func(ctx context.Context) error {
		if err := s.Store.ReadControl(ctx, func(data *store.Snapshot) error {
			tenant, ok := data.Tenants[tenantID]
			if !ok {
				return domain.ErrNotFound
			}
			if tenant.ID != tenantID {
				return fmt.Errorf("%w: control tenant identity mismatch", domain.ErrConflict)
			}
			if err := collectGovernanceStatus(ctx, data, actor, access, &result); err != nil {
				return err
			}
			result.ObservedAt = s.Now().UTC()
			return ctx.Err()
		}); err != nil {
			return err
		}
		return s.recordReadAction(ctx, tenantID, actor, domain.AdminActionGovernanceStatusRead, "governance", "", sections)
	})
	if err != nil {
		return domain.GovernanceStatus{}, err
	}
	return result, nil
}

func governanceStatusSections(access GovernanceStatusAccess) string {
	sections := []string{}
	if access.Exports {
		sections = append(sections, "exports")
	}
	if access.Restores {
		sections = append(sections, "restores")
		if access.RestoreDecisions {
			sections = append(sections, "actionable")
		}
	}
	if access.LegalHolds {
		sections = append(sections, "legal_holds")
	}
	if access.Retention {
		sections = append(sections, "retention")
	}
	return strings.Join(sections, ",")
}

func collectGovernanceStatus(ctx context.Context, data *store.Snapshot, actor string, access GovernanceStatusAccess, result *domain.GovernanceStatus) error {
	if access.Exports {
		var err error
		result.Exports, err = governanceExportStatus(ctx, data.Exports, result.TenantID)
		if err != nil {
			return err
		}
	}
	if access.Restores {
		var err error
		result.Restores, err = governanceRestoreStatus(ctx, data.RestoreRuns, result.TenantID, actor, access.RestoreDecisions)
		if err != nil {
			return err
		}
	}
	if access.LegalHolds {
		result.LegalHolds = &domain.GovernanceHoldStatus{}
		for _, hold := range data.LegalHolds {
			if err := ctx.Err(); err != nil {
				return err
			}
			if hold.TenantID != result.TenantID {
				continue
			}
			result.LegalHolds.Total++
			if hold.ReleasedAt == nil {
				result.LegalHolds.Active++
			} else {
				result.LegalHolds.Released++
			}
		}
	}
	if access.Retention {
		result.Retention = &domain.GovernanceRetentionStatus{}
		if policy, ok := data.Policies[result.TenantID]; ok {
			if policy.TenantID != result.TenantID {
				return fmt.Errorf("%w: control policy identity mismatch", domain.ErrConflict)
			}
			result.Retention.Configured = true
			result.Retention.Policy = &policy
		}
	}
	return ctx.Err()
}

func governanceExportStatus(ctx context.Context, jobs map[string]domain.ExportJob, tenantID string) (*domain.GovernanceExportStatus, error) {
	result := &domain.GovernanceExportStatus{TenantQueueLimit: MaxPendingExportsPerTenant, TenantRunningLimit: ExportTenantConcurrency}
	for _, job := range jobs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if job.TenantID != tenantID {
			continue
		}
		result.Total++
		switch job.Status {
		case "pending":
			result.Pending++
			if !job.CreatedAt.IsZero() && (result.OldestPendingAt == nil || job.CreatedAt.Before(*result.OldestPendingAt)) {
				at := job.CreatedAt.UTC()
				result.OldestPendingAt = &at
			}
		case "running":
			result.Running++
		case "completed":
			result.Completed++
		case "failed":
			result.Failed++
		case domain.ExportStatusCancelled:
			result.Cancelled++
		default:
			result.Unknown++
		}
	}
	return result, nil
}

func governanceRestoreStatus(ctx context.Context, runs map[string]domain.RestoreRun, tenantID, actor string, decisions bool) (*domain.GovernanceRestoreStatus, error) {
	result := &domain.GovernanceRestoreStatus{Execution: "not_tracked"}
	if decisions {
		result.Actionable = new(int)
	}
	for _, run := range runs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if run.TenantID != tenantID {
			continue
		}
		result.Total++
		switch run.Status {
		case domain.RestoreStatusPendingApproval:
			result.PendingApproval++
			if run.CreatedBy == actor {
				result.MinePendingApproval++
			} else if decisions && run.CreatedBy != "" {
				(*result.Actionable)++
			}
		case domain.RestoreStatusApproved:
			result.Approved++
		case domain.RestoreStatusRejected:
			result.Rejected++
		default:
			result.Unknown++
		}
	}
	return result, nil
}
