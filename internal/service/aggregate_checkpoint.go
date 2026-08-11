package service

import (
	"github.com/snaplink/audit-governance/internal/domain"
)

// DefaultAggregateCheckpointRetention is the per-tenant cap on retained
// aggregate checkpoints. Bounded history keeps VerifyIntegrity's
// aggregate-checkpoint work bounded (FR-5) and the shared snapshot row
// bounded, while the most recent record — the anchor for the current ledger
// state — is always retained (floor = 1). Records are dropped oldest-first;
// the WORM archive remains the deep evidence trail. Operators can override
// the cap per deployment with AUDIT_AGGREGATE_CHECKPOINT_HISTORY; values
// <= 0 select this default.
const DefaultAggregateCheckpointRetention = 1000

// trimAggregateCheckpoints keeps at most n records for tenantID, dropping
// the oldest. Append order of retained records and of other tenants'
// records is preserved (the rebuild selects a subsequence of the original
// slice). Returns the new slice and whether anything was removed; when the
// tenant's count is already <= n the original slice is returned unchanged
// (no copy), so steady-state passes stay O(1) apart from the scan.
func trimAggregateCheckpoints(items []domain.AggregateCheckpoint, tenantID string, n int) ([]domain.AggregateCheckpoint, bool) {
	count := 0
	for i := len(items) - 1; i >= 0; i-- {
		if items[i].TenantID == tenantID {
			count++
		}
	}
	if count <= n {
		return items, false
	}
	keep := n
	out := make([]domain.AggregateCheckpoint, 0, len(items)-(count-n))
	for i := len(items) - 1; i >= 0; i-- {
		if items[i].TenantID == tenantID {
			if keep == 0 {
				continue
			}
			keep--
		}
		out = append(out, items[i])
	}
	// out was collected newest-first; reverse it to restore append order.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, true
}

// lastAggregateCheckpoint returns the tenant's most recent record (the
// slice is append-ordered), or nil. The returned value is a copy so the
// caller cannot mutate the snapshot through it.
func lastAggregateCheckpoint(items []domain.AggregateCheckpoint, tenantID string) *domain.AggregateCheckpoint {
	for i := len(items) - 1; i >= 0; i-- {
		if items[i].TenantID == tenantID {
			value := items[i]
			return &value
		}
	}
	return nil
}

// retentionCap returns the effective per-tenant retention cap: the
// configured value when it is at least the floor of 1 (the most recent
// record is always retained), otherwise the documented default.
func (s *Service) retentionCap() int {
	if n := s.Config.AggregateCheckpointRetention; n >= 1 {
		return n
	}
	return DefaultAggregateCheckpointRetention
}
