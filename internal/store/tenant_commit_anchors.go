package store

import "context"

// This recovery-only scan of the file index is not an ingest-path history
// scan. Exact coordinates and ambiguous predecessors fail closed.
func captureRecoveryAnchors(ctx context.Context, state *tenantState, c *TenantCommitCandidate) error {
	wanted := map[LedgerCoordinate]bool{}
	for _, record := range c.Records {
		if receipt := record.Receipt; receipt != nil {
			wanted[LedgerCoordinate{receipt.StreamID, receipt.Sequence - 1}] = true
			if stream, ok := c.Hot.Streams[StreamKey(c.TenantID, receipt.StreamID)]; ok {
				wanted[LedgerCoordinate{receipt.StreamID, stream.NextSequence - int64(len(stream.PendingHashes)) - 1}] = true
				for _, key := range stream.PendingEvents {
					if value, ok := state.ledger.receipts[key]; ok {
						c.PendingReceipts[key] = value
					}
				}
			}
		}
		if segment := record.Segment; segment != nil {
			wanted[LedgerCoordinate{segment.StreamID, segment.FirstSequence - 1}] = true
		}
	}
	for _, anchor := range state.ledger.receipts {
		if err := ctx.Err(); err != nil {
			return err
		}
		coord := LedgerCoordinate{anchor.StreamID, anchor.Sequence}
		if !wanted[coord] {
			continue
		}
		if previous, ok := c.Anchors[coord]; ok && (previous.EventID != anchor.EventID || previous.Hash != anchor.Hash) {
			return ErrLedgerRecordConflict
		}
		c.Anchors[coord] = anchor
	}
	return nil
}
