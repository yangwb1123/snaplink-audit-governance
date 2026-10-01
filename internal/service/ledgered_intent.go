package service

// The durable publication intent for a ledgered event is the control plane's
// Snapshot.LedgeredOutbox row. On the tenant path it is written by the same
// Store.UpdateTenant closure that appends the cold receipt, so one store
// operation carries both and no application-level window separates them.
//
// The physical window is still there — both backends persist hot document,
// then cold ledger, then control document — so an interrupted commit can
// still leave a committed event with no intent. These helpers keep that
// recoverable: an idempotent retry re-creates the missing row from the stored
// event instead of answering "duplicate" with nothing owed to the projection.

import (
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// ledgeredIntentRepairLimit bounds the process-local set of event keys whose
// lost publication intent has already been re-created. At-least-once
// publication does not depend on the set — it exists so one repair (and one
// counter increment) happens per event per process instead of on every later
// retry. At the limit one arbitrary tracked key is evicted, which can only
// allow an extra repair, never a lost one: the durable gate never consults the
// set to decide whether an intent is owed.
const ledgeredIntentRepairLimit = 4096

// ensureLedgeredIntent upserts the durable publication intent for a committed
// event. It mutates only view.Global, which every UpdateTenant backend persists
// as part of the same tenant commit.
//
// It must never call a Store method: Store.mu is a non-reentrant mutex held for
// the whole UpdateTenant call, so a store call from inside the closure would
// deadlock. Callers must already have established that view and view.Global are
// non-nil.
func (s *Service) ensureLedgeredIntent(view *store.TenantView, event domain.Event, key string) {
	if s.Config.LedgeredPublisher == nil {
		return
	}
	if view.Global.LedgeredOutbox == nil {
		view.Global.LedgeredOutbox = map[string]domain.Event{}
	}
	view.Global.LedgeredOutbox[key] = event
}

// repairLedgeredIntent re-creates a publication intent that an interrupted
// ingest lost between its durable cold append and its control-plane write. It
// reports whether it queued the re-creation.
//
// repairLedgeredIntent only reads the view and the process-local repair ledger,
// so it is safe inside a closure that a commit may re-run (PostgreSQL
// ErrSnapshotConflict retries the closure against a freshly cloned
// view.Global) and safe to abandon when the commit fails. The caller records
// the counter strictly after UpdateTenant returned nil — claiming the key here
// would permanently block the retry that re-creates the intent.
func (s *Service) repairLedgeredIntent(view *store.TenantView, receipt domain.EventReceipt, key string) bool {
	if s.Config.LedgeredPublisher == nil || !receipt.Duplicate {
		return false
	}
	// StatusLedgered is the discriminator. A completed ingest transitions the
	// receipt to indexed/archived before it answers, so a benign retry of an
	// already-published event does not match; an ingest interrupted in the
	// post-ledger window is still ledgered, which is exactly the lost-intent
	// state.
	if receipt.Status != domain.StatusLedgered {
		return false
	}
	// A pending row means the flusher still owes the publication, which is why
	// the repair counter measures lost intents rather than retries.
	if _, pending := view.Global.LedgeredOutbox[key]; pending {
		return false
	}
	// A missing hot payload means the receipt was archived, and archiving
	// already means the event reached the projection feed.
	stored, hot := view.Hot.Events[key]
	if !hot {
		return false
	}
	if !s.ledgeredIntentOwed(key) {
		return false
	}
	// Publish the stored event, never the caller-supplied input, so the
	// envelope carries the committed stream/sequence/hash.
	s.ensureLedgeredIntent(view, stored, key)
	return true
}

// ledgeredIntentOwed reports whether key has not been repaired by this process
// yet. It is a pure read of process-local state.
func (s *Service) ledgeredIntentOwed(key string) bool {
	s.repairedMu.Lock()
	defer s.repairedMu.Unlock()
	_, repaired := s.repairedIntents[key]
	return !repaired
}

// rememberLedgeredIntentRepair records one committed re-creation and increments
// the repair counter. Callers must invoke it only after UpdateTenant returned
// nil.
func (s *Service) rememberLedgeredIntentRepair(key string) {
	s.repairedMu.Lock()
	defer s.repairedMu.Unlock()
	if s.repairedIntents == nil {
		s.repairedIntents = map[string]struct{}{}
	}
	if _, tracked := s.repairedIntents[key]; tracked {
		return
	}
	if len(s.repairedIntents) >= ledgeredIntentRepairLimit {
		for oldest := range s.repairedIntents {
			delete(s.repairedIntents, oldest)
			break
		}
	}
	s.repairedIntents[key] = struct{}{}
	s.ledgeredIntentRepairs.Add(1)
}

// LedgeredIntentRepairs reports how many publication intents this process has
// re-created after an interrupted ingest. It is exposed as
// audit_ledgered_intent_repaired_total; a non-zero value is direct evidence
// that a tenant commit died between its durable cold append and its
// control-plane write.
func (s *Service) LedgeredIntentRepairs() uint64 { return s.ledgeredIntentRepairs.Load() }
