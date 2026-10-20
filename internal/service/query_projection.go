package service

import (
	"context"
	"fmt"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func (s *Service) verifyQueryReferences(ctx context.Context, tenantID string, references []store.EventReference) error {
	if len(references) > store.MaxQuerySnapshotEvents {
		return domain.ErrQuotaExceeded
	}
	return s.Store.ReadTenant(ctx, tenantID, func(view *store.TenantView) error {
		seen := map[string]bool{}
		for _, reference := range references {
			if err := ctx.Err(); err != nil {
				return err
			}
			if reference.EventID == "" || reference.OccurredAt.IsZero() || seen[reference.EventID] {
				return fmt.Errorf("%w: invalid projected identity", domain.ErrConflict)
			}
			seen[reference.EventID] = true
			receipt, found := view.Ledger.Receipt(store.EventKey(tenantID, reference.EventID))
			if err := view.Ledger.Err(); err != nil {
				return err
			}
			if !found || receipt.TenantID != tenantID || receipt.EventID != reference.EventID || reference.StreamID != receipt.StreamID || reference.Sequence != receipt.Sequence || reference.Hash != receipt.Hash {
				return fmt.Errorf("%w: projected identity differs from committed ledger", domain.ErrConflict)
			}
		}
		return view.Ledger.Err()
	})
}
