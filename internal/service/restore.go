package service

// Restore requests and their approval flow. The approval fact and the
// business execution are separate: this service records the decision, and the
// business system performs the compensating action through its own API.

import (
	"fmt"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func (s *Service) PreviewRestore(tenantID string, request domain.RestoreRequest) (domain.RestorePreview, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.RestorePreview{}, err
	}
	// F1 (security review): restore preview replays event-derived state with
	// the same read permission as the audited timeline/replay endpoints, but
	// the design gate records an explicit rejection of audit here: the actor
	// stays empty, so recordReadAction appends nothing (compat constraint 3,
	// pinned by TestReadSelfAuditRestoreFlowRecordsNothing). A restore
	// preview is a proposal for a compensating action, not a content read;
	// changing this requires a product decision, not a code change alone.
	result, err := s.ReplayOperation(tenantID, "", request.OperationID)
	if err != nil {
		return domain.RestorePreview{}, err
	}
	return domain.RestorePreview{ID: newID("restore-preview"), TenantID: tenantID, OperationID: request.OperationID, ProposedState: result.State, ExternalCalls: []string{}, RequiresApproval: true}, nil
}

func (s *Service) CreateRestore(tenantID string, request domain.RestoreRequest, requestedBy string) (domain.RestoreRun, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.RestoreRun{}, err
	}
	if request.OperationID == "" || request.Reason == "" {
		return domain.RestoreRun{}, fmt.Errorf("%w: operation_id and reason are required", domain.ErrInvalid)
	}
	if _, err := s.ReplayOperation(tenantID, "", request.OperationID); err != nil {
		return domain.RestoreRun{}, err
	}
	run := domain.RestoreRun{ID: newID("restore"), TenantID: tenantID, OperationID: request.OperationID, Status: domain.RestoreStatusPendingApproval, Reason: request.Reason, CreatedBy: requestedBy, CreatedAt: s.Now()}
	if err := s.Store.Update(func(data *store.Snapshot) error {
		data.RestoreRuns[run.ID] = run
		s.appendAdminAction(data, s.adminAction(tenantID, requestedBy, domain.AdminActionRestoreCreated, "restore", run.ID, request.OperationID))
		return nil
	}); err != nil {
		return domain.RestoreRun{}, err
	}
	return run, nil
}

// ApproveRestore moves a pending restore run to approved. Approval only
// records the decision fact; executing the compensating business calls is a
// separate, later stage and is never triggered here.

func (s *Service) ApproveRestore(tenantID, runID, approvedBy string) (domain.RestoreRun, error) {
	return s.transitionRestore(tenantID, runID, approvedBy, domain.RestoreStatusApproved)
}

// RejectRestore moves a pending restore run to rejected.

func (s *Service) RejectRestore(tenantID, runID, rejectedBy string) (domain.RestoreRun, error) {
	return s.transitionRestore(tenantID, runID, rejectedBy, domain.RestoreStatusRejected)
}

func (s *Service) transitionRestore(tenantID, runID, actor, target string) (domain.RestoreRun, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.RestoreRun{}, err
	}
	var run domain.RestoreRun
	err := s.Store.Update(func(data *store.Snapshot) error {
		value, ok := data.RestoreRuns[runID]
		if !ok || value.TenantID != tenantID {
			return domain.ErrNotFound
		}
		if value.Status != domain.RestoreStatusPendingApproval {
			return fmt.Errorf("%w: restore run is not pending approval", domain.ErrConflict)
		}
		// Separation of duties: the deciding actor must differ from the
		// requesting actor. Inside the Update closure, so a refusal commits
		// nothing (no version bump, no admin action). Precedence is pinned
		// NotFound → Conflict → Forbidden: 404 masks cross-tenant existence,
		// 409 dominates for decided runs.
		if value.CreatedBy == actor {
			return domain.ErrForbidden
		}
		now := s.Now()
		if target == domain.RestoreStatusApproved {
			value.Status = domain.RestoreStatusApproved
			value.ApprovedBy = actor
			value.ApprovedAt = &now
			s.appendAdminAction(data, s.adminAction(value.TenantID, actor, domain.AdminActionRestoreApproved, "restore", runID, value.OperationID))
		} else {
			value.Status = domain.RestoreStatusRejected
			value.RejectedBy = actor
			value.RejectedAt = &now
			s.appendAdminAction(data, s.adminAction(value.TenantID, actor, domain.AdminActionRestoreRejected, "restore", runID, value.OperationID))
		}
		data.RestoreRuns[runID] = value
		run = value
		return nil
	})
	return run, err
}

func (s *Service) GetRestore(tenantID, runID string) (domain.RestoreRun, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.RestoreRun{}, err
	}
	var run domain.RestoreRun
	err := s.Store.Read(func(data *store.Snapshot) error {
		value, ok := data.RestoreRuns[runID]
		if !ok || value.TenantID != tenantID {
			return domain.ErrNotFound
		}
		run = value
		return nil
	})
	return run, err
}

// DefaultStuckExportAge is the default age past which an export job stuck
// in "running" is failed by the governance worker (measured from CreatedAt,
// the only pre-terminal timestamp). Deliberately generous so a legitimately
// long export is never failed by a slow pass cycle; operators can tighten it
// per deployment with AUDIT_GOVERNANCE_STUCK_EXPORT_AGE; values <= 0 select
// this default.
const DefaultStuckExportAge = 24 * time.Hour
