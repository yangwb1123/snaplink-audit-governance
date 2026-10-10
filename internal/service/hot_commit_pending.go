package service

import (
	"context"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func recoveryPrefixMatches(c store.TenantCommitCandidate, streamID string, firstSequence int64, prevHash string) bool {
	if firstSequence == 1 {
		return prevHash == ""
	}
	if firstSequence < 1 {
		return false
	}
	anchor, ok := c.Anchors[store.LedgerCoordinate{StreamID: streamID, Sequence: firstSequence - 1}]
	return ok && anchor.TenantID == c.TenantID && anchor.StreamID == streamID && anchor.Sequence == firstSequence-1 && anchor.Hash != "" && anchor.Hash == prevHash
}

func validateRecoveryPending(ctx context.Context, c store.TenantCommitCandidate, stream store.StreamState, receipt domain.EventReceipt) error {
	n := len(stream.PendingHashes)
	if n == 0 || len(stream.PendingEvents) != n || stream.PendingHashes[n-1] != receipt.Hash || stream.PendingEvents[n-1] != store.EventKey(c.TenantID, receipt.EventID) {
		return domain.ErrConflict
	}
	first := stream.NextSequence - int64(n)
	if !recoveryPrefixMatches(c, stream.StreamID, first, stream.PendingPrevHash) {
		return domain.ErrConflict
	}
	for i, key := range stream.PendingEvents {
		if err := ctx.Err(); err != nil {
			return err
		}
		value, ok := c.PendingReceipts[key]
		if i == n-1 {
			value, ok = receipt, true
		}
		if !ok || value.TenantID != c.TenantID || value.StreamID != stream.StreamID || value.Sequence != first+int64(i) || value.Hash == "" || value.Hash != stream.PendingHashes[i] || key != store.EventKey(c.TenantID, value.EventID) {
			return domain.ErrConflict
		}
	}
	return nil
}
