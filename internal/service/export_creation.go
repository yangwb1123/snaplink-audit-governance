package service

import (
	"context"
	"fmt"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Shared first-attempt/retry preflight. A retry is a fresh evidence selection,
// not a continuation of an old cursor or a bypass of read self-audit/Hold.
// The Hold denial fact must commit separately: returning an error from a
// mutation closure discards its changes. The returned binding is request-local,
// not a persisted plan or authorization: new-job CAS must recheck it.
func (s *Service) prepareExport(ctx context.Context, tenantID, actor string, query domain.Query) (string, error) {
	var holdBinding string
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		if _, err := s.QueryEvents(opCtx, tenantID, actor, query); err != nil {
			return err
		}
		// Snapshot is a deep owned copy in all layouts. Capture only while
		// holding Store.mu, then resolve archive evidence outside the lock so
		// a slow Hold preflight cannot block renewal or owner cancellation.
		// Full history materialization remains a separate capacity limitation.
		data, err := s.Store.Snapshot(opCtx)
		if err != nil {
			return err
		}
		hold, blocked, err := s.holdBlockingExportEvidence(opCtx, data, domain.ExportJob{TenantID: tenantID, Query: query})
		if err != nil {
			return err
		}
		if !blocked {
			holdBinding = s.exportHoldBinding(data, tenantID)
			return opCtx.Err()
		}
		// The separate denial fact owns no tenant evidence. Merge it into
		// fresh control on conflict, never save the captured full history.
		if err := s.Store.UpdateControl(opCtx, func(data *store.Snapshot) error {
			s.appendAdminAction(data, s.adminAction(tenantID, actor, domain.AdminActionExportBlocked, "legal_hold", hold.ID, fmt.Sprintf("export_blocked reason=%s", hold.Reason)))
			return nil
		}); err != nil {
			return err
		}
		return fmt.Errorf("%w", exportBlockedError{holdID: hold.ID})
	})
	if err != nil {
		return "", err
	}
	return holdBinding, nil
}

// Only local control metadata is examined inside the current CAS. A change
// requires an explicit fresh attempt, not hidden reselection or a new budget.
func (s *Service) checkExportPreflight(data *store.Snapshot, tenantID, holdBinding string) error {
	if holdBinding == "" || holdBinding != s.exportHoldBinding(data, tenantID) {
		return fmt.Errorf("%w: export Hold changed during preflight; refresh before retrying", domain.ErrConflict)
	}
	return nil
}
