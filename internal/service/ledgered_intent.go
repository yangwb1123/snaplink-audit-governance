package service

// Intent repair never infers delivery from receipt status, hot eviction or a
// process-local set. Only an identity-bound persistent broker confirmation can
// prove this event no longer owes a publication. A missing confirmation is
// unknown: historical deliveries may be repeated under at-least-once semantics.

import (
	"fmt"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// ensureLedgeredIntent mutates only the private control view. Never call a
// Store method from a tenant closure: Store.mu is non-reentrant.
func (s *Service) ensureLedgeredIntent(view *store.TenantView, event domain.Event, key string) {
	if s.Config.LedgeredPublisher == nil {
		return
	}
	if view.Global.LedgeredOutbox == nil {
		view.Global.LedgeredOutbox = map[string]domain.Event{}
	}
	view.Global.LedgeredOutbox[key] = event
}

// repairLedgeredIntent retains the fast idempotent-retry recovery path for hot
// events. Archived payloads are handled by the periodic reconciler outside the
// store lock. The caller counts repairs only AFTER a successful commit; a CAS
// retry or failed control save must not suppress a later attempt.
func (s *Service) repairLedgeredIntent(view *store.TenantView, receipt domain.EventReceipt, key string) (bool, error) {
	if s.Config.LedgeredPublisher == nil || !receipt.Duplicate {
		return false, nil
	}
	if _, pending := view.Global.LedgeredOutbox[key]; pending {
		return false, nil
	}
	confirmed, err := view.PublicationConfirmed(receipt)
	if err != nil || confirmed {
		return false, err
	}
	stored, hot := view.Hot.Events[key]
	if !hot {
		return false, nil
	}
	if err := s.validatePublicationEvent(stored, receipt, view.Global.Schemas); err != nil {
		return false, err
	}
	s.ensureLedgeredIntent(view, stored, key)
	return true, nil
}

func (s *Service) validatePublicationEvent(event domain.Event, receipt domain.EventReceipt, schemas map[string]domain.EventSchema) error {
	if receipt.TenantID == "" || receipt.EventID == "" || receipt.StreamID == "" || receipt.Sequence <= 0 || receipt.Hash == "" ||
		event.TenantID != receipt.TenantID || event.EventID != receipt.EventID || event.StreamID != receipt.StreamID ||
		event.Sequence != receipt.Sequence || event.Hash != receipt.Hash {
		return fmt.Errorf("publication evidence identity mismatch")
	}
	// EventDigest may use the retained SourceDigest. Recomputing only the
	// chain hash would therefore miss payload/ciphertext tampering. Verify
	// reconstructed content with the exact schema/key first, without mutating
	// the protected event that will be sent to Kafka.
	if err := s.verifyContentDigest(event, schemas); err != nil {
		return fmt.Errorf("publication evidence content mismatch")
	}
	hash, err := s.eventHash(event)
	if err != nil || hash != receipt.Hash {
		return fmt.Errorf("publication evidence hash mismatch")
	}
	return nil
}

// LedgeredIntentRepairs counts committed re-creations by client retries and
// active reconciliation. It is an operation counter, not a distinct-event count.
func (s *Service) LedgeredIntentRepairs() uint64 { return s.ledgeredIntentRepairs.Load() }
