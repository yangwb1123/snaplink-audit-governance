package store

import (
	"context"
	"fmt"
	"reflect"

	"github.com/snaplink/audit-governance/internal/domain"
)

// Pending hot metadata is a recovery intention, NEVER committed ledger truth.
// Ordinary tenant mutations cannot overwrite an unresolved intention.
var ErrTenantCommitPending = fmt.Errorf("%w: tenant commit requires recovery", ErrSnapshotConflict)

const MaxTenantCommitRecords = 100

// TenantCommitCandidate is a private capture for validation outside Store.mu.
// It is not an API response and must never be logged or persisted elsewhere.
type LedgerCoordinate struct {
	StreamID string
	Sequence int64
}

type TenantCommitCandidate struct {
	TenantID        string
	Version         int64
	Records         []LedgerRecord
	Hot             *Snapshot
	Schemas         map[string]domain.EventSchema
	Anchors         map[LedgerCoordinate]domain.EventReceipt
	PendingReceipts map[string]domain.EventReceipt
}

func cloneLedgerRecords(records []LedgerRecord) []LedgerRecord {
	result := make([]LedgerRecord, len(records))
	for i, record := range records {
		result[i] = cloneLedgerRecord(record)
	}
	return result
}

func validateTenantCommitRecords(tenantID string, records []LedgerRecord) error {
	if len(records) > MaxTenantCommitRecords {
		return fmt.Errorf("%w: tenant commit record budget exceeded", domain.ErrInvalid)
	}
	for _, record := range records {
		if record.TenantID != tenantID {
			return fmt.Errorf("%w: tenant commit identity mismatch", domain.ErrInvalid)
		}
		if err := validateLedgerRecord(record); err != nil {
			return err
		}
	}
	return nil
}

func unresolvedTenantCommit(state *tenantState) (bool, error) {
	if len(state.pending) == 0 {
		return false, nil
	}
	if err := validateTenantCommitRecords(state.pending[0].TenantID, state.pending); err != nil {
		return true, err
	}
	missing, err := state.ledger.recordsToAppend(state.pending)
	return len(missing) != 0, err
}

// ReadPendingTenantCommit captures one file tenant's exact lost append. PG v2
// HotFirst now commits hot+ledger in one SQL transaction; legacy stores carry
// no marker, and are deliberately not inferred from an uncommitted hot body.
func (s *Store) ReadPendingTenantCommit(ctx context.Context, tenantID string) (candidate TenantCommitCandidate, err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	if err := domain.ValidKeyComponent("tenant_id", tenantID); err != nil {
		return candidate, err
	}
	if err := op.ctx.Err(); err != nil {
		return candidate, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.split == nil {
		return candidate, nil
	}
	state := s.split.tenant[tenantID]
	if state == nil || len(state.pending) == 0 {
		return candidate, nil
	}
	unresolved, err := unresolvedTenantCommit(state)
	if err != nil {
		return candidate, err
	}
	if !unresolved {
		return candidate, nil
	}
	hot, err := cloneTenantHot(state.hot)
	if err != nil {
		return candidate, err
	}
	control, err := cloneControl(s.split.control)
	if err != nil {
		return candidate, err
	}
	candidate = TenantCommitCandidate{TenantID: tenantID, Version: state.version, Records: cloneLedgerRecords(state.pending), Hot: hot, Schemas: control.Schemas, Anchors: map[LedgerCoordinate]domain.EventReceipt{}, PendingReceipts: map[string]domain.EventReceipt{}}
	if err := captureRecoveryAnchors(op.ctx, state, &candidate); err != nil {
		return TenantCommitCandidate{}, err
	}
	return candidate, nil
}

// CommitPendingTenantCommit must only be called AFTER content/chain/signature
// validation of the private capture. It rechecks the exact version and marker,
// appends the original immutable identities, then clears only recovery metadata.
// Cancellation is inherited, never detached here. No stream is re-sequenced.
func (s *Store) CommitPendingTenantCommit(ctx context.Context, candidate TenantCommitCandidate) (recovered int, err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	if err := domain.ValidKeyComponent("tenant_id", candidate.TenantID); err != nil {
		return 0, err
	}
	if err := validateTenantCommitRecords(candidate.TenantID, candidate.Records); err != nil {
		return 0, err
	}
	if err := op.ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.split == nil {
		return 0, ErrTenantCommitPending
	}
	state := s.split.tenant[candidate.TenantID]
	if state == nil {
		return 0, ErrSnapshotConflict
	}
	if len(state.pending) == 0 {
		return 0, nil
	}
	if state.version != candidate.Version || !reflect.DeepEqual(state.pending, candidate.Records) {
		return 0, ErrSnapshotConflict
	}
	missing, err := state.ledger.recordsToAppend(state.pending)
	if err != nil {
		return 0, err
	}
	if err := s.split.appendLedger(op.ctx, candidate.TenantID, state.ledger, state.pending); err != nil {
		return 0, err
	}
	// Failure to clear leaves a harmless, exactly idempotent marker. The cold
	// append remains authoritative; never report or roll it back as hot-only.
	if err := s.split.saveTenantHot(candidate.TenantID, state.hot, state.version+1); err != nil {
		return len(missing), err
	}
	state.version++
	state.pending = nil
	return len(missing), nil
}
