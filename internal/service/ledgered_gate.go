package service

// Post-ledger publication is serialized per pending event, not per process.
//
// The invariant is a property of one durable outbox row: at most one publisher
// may be inside the publish for a given store.EventKey at a time. The previous
// implementation enforced it with a single process-wide mutex that
// FlushLedgeredOutbox held for a whole round (one snapshot read plus up to
// ledgeredOutboxBatchSize publishes and acknowledgements), so a broker that
// had stopped responding stalled every concurrent ingest by an amount bounded
// by nothing in the request path — the ledger commit had already answered the
// caller, so the caller simply waited.
//
// The gate below is the smallest structure that still provides the property:
// acquisition never blocks, a contended caller does not queue, and the durable
// row it leaves behind is exactly what FlushLedgeredOutbox exists to drain.
// Nothing here is durable state; losing the table on restart is harmless
// because the table only ever serializes publishers, never records them.

import "sync"

// ledgeredKeyGate serializes the publishers of exactly one pending event. It
// is a mutex plus a reference count: refs is guarded by
// Service.ledgeredGatesMu, mu is not.
//
// The unlock-before-unref order in releaseLedgeredKey is load-bearing. A gate
// is freed exactly once, by the reference that drops refs to zero, so the mutex
// MUST be unlocked before that reference is dropped. Releasing the reference
// first would let a concurrent ledgeredGateFor delete the entry while this
// publisher still holds it, after which a second publisher would build a fresh
// gate for the same key and both would publish concurrently — a silent
// violation of the invariant with no error anywhere.
type ledgeredKeyGate struct {
	mu   sync.Mutex
	refs int
}

// tryAcquireLedgeredKey takes the gate for one pending event without waiting.
// acquired == false means another publisher is inside the publish for this
// exact event, and the caller must leave the durable intent in place rather
// than queue behind it. The reference taken here is released on the failure
// path too, so a contended key cannot leak a table entry.
func (s *Service) tryAcquireLedgeredKey(key string) (gate *ledgeredKeyGate, acquired bool) {
	gate = s.ledgeredGateFor(key)
	if !gate.mu.TryLock() {
		s.unrefLedgeredKey(key, gate)
		return nil, false
	}
	return gate, true
}

// releaseLedgeredKey is the single exit for an acquired gate. A nil gate is
// accepted so a caller that already returned on failure can still defer this
// unconditionally.
func (s *Service) releaseLedgeredKey(key string, gate *ledgeredKeyGate) {
	if gate == nil {
		return
	}
	gate.mu.Unlock()
	s.unrefLedgeredKey(key, gate)
}

// ledgeredGateFor returns the gate for key, creating it if needed, and takes a
// reference. The table entry exists only while some publisher holds a
// reference, so the table is bounded by in-flight publications rather than by
// the events a process has ever published.
func (s *Service) ledgeredGateFor(key string) *ledgeredKeyGate {
	s.ledgeredGatesMu.Lock()
	defer s.ledgeredGatesMu.Unlock()
	if s.ledgeredGates == nil {
		s.ledgeredGates = make(map[string]*ledgeredKeyGate)
	}
	gate := s.ledgeredGates[key]
	if gate == nil {
		gate = &ledgeredKeyGate{}
		s.ledgeredGates[key] = gate
	}
	gate.refs++
	return gate
}

// unrefLedgeredKey drops one reference and removes the table entry when the
// last one goes. The entry is only removed while no publisher references the
// gate, so a goroutine that already looked one up can never end up holding a
// detached gate.
func (s *Service) unrefLedgeredKey(key string, gate *ledgeredKeyGate) {
	s.ledgeredGatesMu.Lock()
	defer s.ledgeredGatesMu.Unlock()
	gate.refs--
	if gate.refs <= 0 {
		delete(s.ledgeredGates, key)
	}
}
