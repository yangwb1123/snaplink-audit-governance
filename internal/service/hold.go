package service

// Legal Holds: pinning a query's events against retention and export. A hold
// is released explicitly and never expires on its own; every tenant-scoped
// read of it is audited like any other governance fact.

import (
	"context"
	"fmt"
	"sort"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func (s *Service) CreateLegalHold(ctx context.Context, hold domain.LegalHold) (domain.LegalHold, error) {
	if err := requireTenantScope(hold.TenantID); err != nil {
		return domain.LegalHold{}, err
	}
	if hold.TenantID == "" || hold.Name == "" || hold.Reason == "" {
		return domain.LegalHold{}, fmt.Errorf("%w: tenant_id, name and reason are required", domain.ErrInvalid)
	}
	if !hold.Filter.From.IsZero() || !hold.Filter.To.IsZero() {
		if hold.Filter.From.IsZero() || hold.Filter.To.IsZero() || !hold.Filter.From.Before(hold.Filter.To) {
			return domain.LegalHold{}, fmt.Errorf("%w: legal hold filter requires from before to", domain.ErrInvalid)
		}
	}
	if (hold.Filter.PayloadField == "") != (hold.Filter.PayloadDigest == "") {
		return domain.LegalHold{}, fmt.Errorf("%w: payload_field and payload_digest must be provided together", domain.ErrInvalid)
	}
	if hold.ID == "" {
		hold.ID = newID("hold")
	}
	if hold.CreatedAt.IsZero() {
		hold.CreatedAt = s.Now()
	}
	if err := s.Store.Update(ctx, func(data *store.Snapshot) error {
		if _, ok := data.Tenants[hold.TenantID]; !ok {
			return domain.ErrNotFound
		}
		if _, exists := data.LegalHolds[hold.ID]; exists {
			return fmt.Errorf("%w: legal hold already exists", domain.ErrConflict)
		}
		data.LegalHolds[hold.ID] = hold
		s.appendAdminAction(data, s.adminAction(hold.TenantID, hold.CreatedBy, domain.AdminActionLegalHoldCreated, "legal_hold", hold.ID, hold.Reason))
		return nil
	}); err != nil {
		return domain.LegalHold{}, err
	}
	return hold, nil
}

func (s *Service) ListLegalHolds(ctx context.Context, tenantID string) ([]domain.LegalHold, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return nil, err
	}
	var result []domain.LegalHold
	err := s.Store.Read(ctx, func(data *store.Snapshot) error {
		for _, hold := range data.LegalHolds {
			if hold.TenantID == tenantID {
				result = append(result, hold)
			}
		}
		return nil
	})
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result, err
}

func (s *Service) ReleaseLegalHold(ctx context.Context, tenantID, holdID, releasedBy string) (domain.LegalHold, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.LegalHold{}, err
	}
	var hold domain.LegalHold
	if err := s.Store.Update(ctx, func(data *store.Snapshot) error {
		value, ok := data.LegalHolds[holdID]
		if !ok || value.TenantID != tenantID {
			return domain.ErrNotFound
		}
		if value.ReleasedAt != nil {
			hold = value
			return nil
		}
		now := s.Now()
		value.ReleasedAt = &now
		value.ReleasedBy = releasedBy
		data.LegalHolds[holdID] = value
		hold = value
		s.appendAdminAction(data, s.adminAction(value.TenantID, releasedBy, domain.AdminActionLegalHoldReleased, "legal_hold", holdID, ""))
		return nil
	}); err != nil {
		return domain.LegalHold{}, err
	}
	return hold, nil
}

// CreateAggregateCheckpoint builds a signed Merkle root over the latest
// checkpoint of every stream of a tenant and appends it to the evidence
// trail (architecture plan section 10). A tenant without any checkpoint
// yields no record.
//
// Change dedup (FR-1): when the candidate record (root AND signature) is
// identical to the tenant's most recent record, nothing is appended and the
// pass persists nothing (Store.UpdateChecked skips the Save). The
// comparison runs inside the optimistic-lock closure, so it always sees the
// snapshot that will be written: a concurrent writer's append is observed
// on the retried run instead of producing a duplicate (FR-3).
//
// Retention (FR-4): per-tenant history is capped (drop-oldest,
// Config.AggregateCheckpointRetention, default
// DefaultAggregateCheckpointRetention). The cap is applied in the same
// atomic write as the append, and legacy over-cap histories are trimmed
// even on a dedup skip (one-time write after deploy), so VerifyIntegrity's
// aggregate work is bounded by the cap (FR-5). Retained records keep
// today's layout and stay individually verifiable (C4).
