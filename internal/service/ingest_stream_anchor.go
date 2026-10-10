package service

import (
	"fmt"
	"math"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Refuse to extend a pre-journal hot-only head. Recovery cannot invent the
// missing receipt from its body, and a different event must not bypass that
// uncertainty. No cold-history scan or nested Store operation is performed.
func validateIngestHotHead(view *store.TenantView, stream store.StreamState) error {
	if stream.NextSequence <= 0 || stream.NextSequence == math.MaxInt64 {
		return fmt.Errorf("%w: invalid ingest stream sequence", domain.ErrConflict)
	}
	if stream.NextSequence == 1 {
		if stream.HeadHash != "" {
			return domain.ErrConflict
		}
		return nil
	}
	for key, event := range view.Hot.Events {
		if event.StreamID != stream.StreamID || event.Sequence != stream.NextSequence-1 {
			continue
		}
		receipt, ok := view.Ledger.Receipt(key)
		if !ok || receipt.TenantID != stream.TenantID || receipt.EventID != event.EventID || receipt.StreamID != stream.StreamID || receipt.Sequence != event.Sequence || receipt.Hash != event.Hash || receipt.Hash != stream.HeadHash {
			return fmt.Errorf("%w: ingest head has no matching committed receipt", domain.ErrConflict)
		}
	}
	return view.Ledger.Err()
}
