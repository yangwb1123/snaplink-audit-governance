package service

import (
	"context"
	"fmt"
	"math"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// RecoverHotTenantCommit resumes an exact journaled HotFirst append, not an
// Ingest replay. It neither assigns a new sequence nor publishes/archives or
// executes business operations. Old hot-only bodies without a durable marker
// are unknown and must not be converted into committed facts by inference.
func (s *Service) RecoverHotTenantCommit(ctx context.Context, tenantID string) (recovered int, err error) {
	if err := requireTenantScope(tenantID); err != nil {
		return 0, err
	}
	if s.Store == nil {
		return 0, nil
	}
	err = s.Store.WithOperation(ctx, func(ctx context.Context) error {
		candidate, readErr := s.Store.ReadPendingTenantCommit(ctx, tenantID)
		if readErr != nil {
			return readErr
		}
		if len(candidate.Records) == 0 {
			return nil
		}
		if err := s.validateHotTenantCommit(ctx, candidate); err != nil {
			return err
		}
		var commitErr error
		recovered, commitErr = s.Store.CommitPendingTenantCommit(ctx, candidate)
		return commitErr
	})
	if recovered > 0 {
		s.hotCommitRecovered.Add(uint64(recovered))
	}
	if err != nil {
		s.hotCommitRecoveryFailures.Add(1)
	}
	return recovered, err
}

type hotRecoveryRecords struct {
	receipt    *domain.EventReceipt
	segment    *domain.Segment
	checkpoint *domain.Checkpoint
}

func recoveryRecords(c store.TenantCommitCandidate) (proof hotRecoveryRecords, err error) {
	if c.Hot == nil || len(c.Records) > 3 {
		return proof, fmt.Errorf("%w: unsupported hot commit recovery", domain.ErrConflict)
	}
	for _, record := range c.Records {
		switch record.RecordType {
		case store.LedgerReceipt:
			if proof.receipt != nil {
				return proof, domain.ErrConflict
			}
			proof.receipt = record.Receipt
		case store.LedgerSegment:
			if proof.segment != nil {
				return proof, domain.ErrConflict
			}
			proof.segment = record.Segment
		case store.LedgerCheckpoint:
			if proof.checkpoint != nil {
				return proof, domain.ErrConflict
			}
			proof.checkpoint = record.Checkpoint
		default:
			return proof, domain.ErrConflict
		}
	}
	receipt := proof.receipt
	if receipt == nil || receipt.Status != domain.StatusLedgered || receipt.Duplicate || receipt.Conflict || receipt.ErrorCode != "" || receipt.LedgeredAt.IsZero() || receipt.Sequence <= 0 || receipt.Sequence == math.MaxInt64 {
		return proof, fmt.Errorf("%w: invalid recovery receipt", domain.ErrConflict)
	}
	return proof, nil
}

func (s *Service) validateHotTenantCommit(ctx context.Context, c store.TenantCommitCandidate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	proof, err := recoveryRecords(c)
	if err != nil {
		return err
	}
	stream, err := s.validateRecoveryHead(c, *proof.receipt)
	if err != nil {
		return err
	}
	if proof.segment == nil && proof.checkpoint == nil {
		return validateRecoveryPending(ctx, c, stream, *proof.receipt)
	}
	return s.validateRecoverySeal(ctx, c, stream, proof)
}

func (s *Service) validateRecoveryHead(c store.TenantCommitCandidate, receipt domain.EventReceipt) (stream store.StreamState, err error) {
	key := store.EventKey(c.TenantID, receipt.EventID)
	event, ok := c.Hot.Events[key]
	if !ok {
		return stream, domain.ErrConflict
	}
	if err := s.validatePublicationEvent(event, receipt, c.Schemas); err != nil {
		return stream, fmt.Errorf("%w: invalid hot recovery evidence", domain.ErrConflict)
	}
	stream, ok = c.Hot.Streams[store.StreamKey(c.TenantID, receipt.StreamID)]
	if !ok || stream.TenantID != c.TenantID || stream.StreamID != receipt.StreamID || stream.NextSequence != receipt.Sequence+1 || stream.HeadHash != receipt.Hash {
		return stream, fmt.Errorf("%w: hot recovery stream mismatch", domain.ErrConflict)
	}
	if receipt.Sequence == 1 {
		if event.PrevHash != "" {
			return stream, domain.ErrConflict
		}
	} else {
		anchor, ok := c.Anchors[store.LedgerCoordinate{StreamID: receipt.StreamID, Sequence: receipt.Sequence - 1}]
		if !ok || anchor.TenantID != c.TenantID || anchor.StreamID != receipt.StreamID || anchor.Sequence != receipt.Sequence-1 || anchor.Hash == "" || anchor.Hash != event.PrevHash {
			return stream, fmt.Errorf("%w: recovery predecessor unavailable", domain.ErrConflict)
		}
	}
	return stream, nil
}

func (s *Service) validateRecoverySeal(ctx context.Context, c store.TenantCommitCandidate, stream store.StreamState, proof hotRecoveryRecords) error {
	tenantID := c.TenantID
	segment, checkpoint, receipt := proof.segment, proof.checkpoint, proof.receipt
	if segment == nil || checkpoint == nil || len(stream.PendingHashes) != 0 || len(stream.PendingEvents) != 0 {
		return domain.ErrConflict
	}
	if segment.TenantID != tenantID || segment.StreamID != receipt.StreamID || segment.LastSequence != receipt.Sequence || segment.LastHash != receipt.Hash || segment.FirstSequence <= 0 || segment.FirstSequence > segment.LastSequence || segment.EventCount != int(segment.LastSequence-segment.FirstSequence+1) {
		return domain.ErrConflict
	}
	if checkpoint.TenantID != tenantID || checkpoint.StreamID != receipt.StreamID || checkpoint.Sequence != receipt.Sequence || checkpoint.MerkleRoot != segment.MerkleRoot || checkpoint.Signature != segment.Signature || checkpoint.Algorithm != s.Config.Signer.Algorithm() {
		return domain.ErrConflict
	}
	if !recoveryPrefixMatches(c, segment.StreamID, segment.FirstSequence, segment.FirstPrevHash) {
		return domain.ErrConflict
	}
	manifest := fmt.Sprintf("%s:%d:%d:%s:%s", segment.TenantID, segment.FirstSequence, segment.LastSequence, segment.LastHash, segment.MerkleRoot)
	digest := domain.HashBytes([]byte(manifest))
	if digest != segment.ManifestHash {
		return domain.ErrConflict
	}
	valid, err := s.Config.Signer.Verify(ctx, []byte(digest), segment.Signature)
	if err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("%w: hot recovery signature mismatch", domain.ErrConflict)
	}
	return ctx.Err()
}

func (s *Service) HotCommitRecovered() uint64        { return s.hotCommitRecovered.Load() }
func (s *Service) HotCommitRecoveryFailures() uint64 { return s.hotCommitRecoveryFailures.Load() }
