package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/snaplink/audit-governance/internal/outbox"
)

// failureWanted keeps the old event-ID-only behavior in the compatibility
// seam. Production collection cannot consult state yet: the DLQ claim has no
// trusted tenant, so only the accepted canonical envelope can identify which
// state entry is relevant.
func (r *Replayer) failureWanted(eventID string) bool {
	if r.tenantAware {
		return true
	}
	return !r.state.marked(eventID)
}

// scanAccepted dispatches to the compatibility implementation or the
// production tenant-aware implementation. Keeping the old seam separate
// preserves Kafka byte-for-byte replay tests while NewReplayer always takes
// the strict path.
func (r *Replayer) scanAccepted(ctx context.Context, wanted wantedSet) (int, map[dlqRecordID]bool, error) {
	if !r.tenantAware {
		return r.scanAcceptedLegacy(ctx, wanted)
	}
	return r.scanAcceptedTenantAware(ctx, wanted)
}

// newTenantAwareReplayerWithFactories is the broker-free acceptance seam for
// production semantics: both readers are recreated per round and canonical
// tenant identity is required before any state or commit decision.
func newTenantAwareReplayerWithFactories(dlqNew, acceptedNew func() messageReader, state *ReplayState, republish RepublishFunc, logger *log.Logger) *Replayer {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Replayer{dlqReader: dlqNew(), dlqNew: dlqNew, accepted: acceptedNew(), acceptedNew: acceptedNew, republish: republish, state: state, tenantAware: true, logger: logger, drainTimeout: defaultDrainTimeout}
}

type tenantEventKey struct {
	tenantID string
	eventID  string
}

type tenantAcceptedCandidate struct {
	message  kafka.Message
	tenantID string
	eventID  string
}

// tenantCandidateIndex keeps canonical tenant/event identity authoritative
// while retaining an event-ID-only index solely for candidate lookup.
type tenantCandidateIndex struct {
	byTenantEvent map[tenantEventKey]tenantAcceptedCandidate
	byEventID     map[string][]tenantAcceptedCandidate
}

// scanAcceptedTenantAware never delivers while the accepted topic is still
// being scanned. A later candidate with the same event_id but another tenant
// would make an early delivery an unsafe guess, so candidate collection and
// resolution are deliberately separate phases.
func (r *Replayer) scanAcceptedTenantAware(ctx context.Context, wanted wantedSet) (int, map[dlqRecordID]bool, error) {
	candidates, found, drained, scanErr := r.collectTenantCandidates(ctx, wanted)
	if scanErr != nil {
		return 0, map[dlqRecordID]bool{}, scanErr
	}
	if !drained {
		r.tenantScopePending.Store(uint64(len(wanted.replay)))
		return 0, map[dlqRecordID]bool{}, nil
	}
	return r.resolveTenantCandidates(ctx, wanted, candidates, found, drained)
}

// collectTenantCandidates scans from the first retained accepted offset and
// records at most two distinct tenant identities for each wanted event ID.
// Two are enough to prove ambiguity while bounding memory on duplicate or
// adversarial accepted traffic.
func (r *Replayer) collectTenantCandidates(ctx context.Context, wanted wantedSet) (map[string][]tenantAcceptedCandidate, map[string]bool, bool, error) {
	window := r.drainWindow()
	scanCtx, cancel := context.WithTimeout(ctx, maxScanWindows*window)
	defer cancel()
	candidates := map[string][]tenantAcceptedCandidate{}
	found := map[string]bool{}
	lastMessage := time.Now()
	for {
		fetchCtx, fetchCancel := context.WithTimeout(scanCtx, window)
		message, err := r.accepted.FetchMessage(fetchCtx)
		fetchCancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil && time.Since(lastMessage) >= window {
				return candidates, found, true, nil
			}
			if ctx.Err() == nil {
				return candidates, found, false, err
			}
			return candidates, found, false, nil
		}
		lastMessage = time.Now()
		r.acceptedSeen.Add(1)
		// A message whose KEY is a wanted event_id occupies that event's
		// canonical accepted slot even when its payload decodes to a different
		// event_id (the key-fallback/versioning case). The original is then NOT
		// proven gone, so the record must stay pending and must never be declared
		// unresolvable — this preserves the tenant-replay regression where a
		// valid payload with a different event_id defeats key fallback.
		if key := string(message.Key); len(wanted.byEventID[key]) > 0 {
			found[key] = true
		}
		event, err := decodeCanonicalReplayEvent(message.Value)
		if err != nil {
			// A malformed/key-only value is evidence that the wanted record
			// cannot be safely recovered, not a tenant identity. Keep it
			// pending rather than letting the key create a state mark.
			if id := eventIDFromValue(message.Value); len(wanted.byEventID[id]) > 0 {
				found[id] = true
			}
			continue
		}
		if event.EventID == "" || len(wanted.byEventID[event.EventID]) == 0 {
			continue
		}
		found[event.EventID] = true
		if event.TenantID == "" {
			continue
		}
		addTenantCandidate(candidates, event.EventID, tenantAcceptedCandidate{message: message, tenantID: event.TenantID, eventID: event.EventID})
	}
}

func addTenantCandidate(candidates map[string][]tenantAcceptedCandidate, eventID string, candidate tenantAcceptedCandidate) {
	items := candidates[eventID]
	for _, existing := range items {
		if existing.tenantID == candidate.tenantID {
			return
		}
	}
	if len(items) < 2 {
		candidates[eventID] = append(items, candidate)
	}
}

// tenantCandidateForRecord associates one physical DLQ record with a
// canonical accepted candidate. A claim can select a matching candidate, but
// never creates one. With one DLQ record, a unique canonical candidate also
// preserves the legacy optional-claim behavior: a misleading claim is
// ignored rather than becoming authorization. Once that canonical identity
// already has a durable mark, however, a mismatching claim is an incompatible
// sibling signal and must remain pending across rounds and restarts. With
// duplicate records, a mismatching claim is also left pending when another
// record has a compatible claim; this prevents that record from inheriting
// its sibling's association.
func tenantCandidateForRecord(record dlqRecord, wanted wantedSet, candidates map[string][]tenantAcceptedCandidate, state *ReplayState) (tenantAcceptedCandidate, bool) {
	items := candidates[record.eventID]
	if len(items) == 0 {
		return tenantAcceptedCandidate{}, false
	}
	if record.claimedTenantID != "" {
		for _, candidate := range items {
			if candidate.tenantID == record.claimedTenantID {
				// A claim may disambiguate only in the context of multiple
				// physical records. A lone record must not turn an untrusted
				// claim into authorization, and remains pending if canonical
				// candidates are ambiguous.
				if len(items) == 1 || len(wanted.byEventID[record.eventID]) > 1 {
					return candidate, true
				}
				return tenantAcceptedCandidate{}, false
			}
		}
	}
	if len(items) != 1 {
		return tenantAcceptedCandidate{}, false
	}
	if record.claimedTenantID == "" {
		return items[0], true
	}
	// A single physical record retains backward-compatible behavior for the
	// optional, unverified claim. For duplicate records, a mismatch is safe
	// only if no sibling has a claim compatible with the canonical candidate;
	// otherwise resolving it would make the sibling's identity reusable.
	if len(wanted.byEventID[record.eventID]) == 1 {
		// A durable canonical mark proves that an earlier physical sibling
		// already resolved this event for the candidate tenant. Do not let a
		// later record with an incompatible claim inherit that mark after the
		// sibling has fallen below the DLQ commit offset.
		if state != nil && state.Marked(items[0].tenantID, items[0].eventID) {
			return tenantAcceptedCandidate{}, false
		}
		return items[0], true
	}
	for _, siblingID := range wanted.byEventID[record.eventID] {
		sibling := wanted.records[siblingID]
		if sibling.claimedTenantID == items[0].tenantID {
			return tenantAcceptedCandidate{}, false
		}
	}
	return tenantAcceptedCandidate{}, false
}

// resolveTenantCandidates applies durable state and delivery policy only
// after the full accepted scan proved the event_id→tenant correlation. A
// missing candidate, ambiguous candidates, missing resolver credential, and
// tenant_mismatch all remain pending and are counted in the scope backlog.
// When the scan is drained and a wanted event_id has no usable canonical
// candidate (the original is genuinely absent from the accepted topic, i.e.
// retention expiry or never published there), the record is durably marked
// unresolvable so commitResolved advances the DLQ offset and the loss signal
// fires — mirroring scanAcceptedLegacy:1430-1448 (R1/R5/R6/R7).
func (r *Replayer) resolveTenantCandidates(ctx context.Context, wanted wantedSet, candidates map[string][]tenantAcceptedCandidate, found map[string]bool, drained bool) (int, map[dlqRecordID]bool, error) {
	resolved := map[dlqRecordID]bool{}
	scopePending := 0
	replayed := 0
	for _, id := range wanted.orderedIDs {
		record := wanted.records[id]
		items := candidates[record.eventID]
		if len(items) == 0 {
			// No canonical accepted candidate for this event_id.
			if !drained || found[record.eventID] {
				// R2: a quiet-topic scan has not proven absence, so the
				// "original gone" conclusion is not definitive; stay pending
				// and re-collect next round.
				// R4: a key-matched (unparsable) original was scanned this round,
				// so the record is governed by the pending/unparsable paths,
				// never a permanent loss.
				scopePending++
				r.logTenantPendingRecord(record, found[record.eventID], 0)
				continue
			}
			// R1: the original is definitively absent from the accepted topic.
			// Record a durable unresolvable drop so commitResolved advances the
			// DLQ offset and the loss signal fires — mirroring
			// scanAcceptedLegacy:1430-1448.
			if r.tenantUnresolvableAlreadyMarked(record) {
				// FM-7: a prior round/restart already recorded this loss; let
				// the offset commit converge without re-counting it.
				resolved[id] = true
				continue
			}
			r.logger.Printf("unresolvable event_id=%s tenant=%s reason=original-not-found-in-accepted-topic round=%s",
				sanitizeLogField(record.eventID, 64), sanitizeLogField(record.claimedTenantID, 64), time.Now().Format(time.RFC3339))
			if err := r.markTenantUnresolvable(record); err != nil {
				return replayed, resolved, err
			}
			resolved[id] = true // commitResolved now advances the DLQ offset (R6)
			r.unresolvable.Add(1)
			replayed++ // legacy return-parity only; NOT r.replayed (R7)
			continue
		}
		candidate, ok := tenantCandidateForRecord(record, wanted, candidates, r.state)
		if !ok {
			// R3: ambiguous candidates (>=2 tenants) or a mismatched untrusted
			// claim → tenant-safety pending, never unresolvable.
			scopePending++
			r.logTenantPendingRecord(record, found[record.eventID], len(items))
			continue
		}
		if r.state.Marked(candidate.tenantID, candidate.eventID) {
			resolved[id] = true
			continue
		}
		if r.republish == nil {
			scopePending++
			r.logTenantPendingRecord(record, true, 0)
			continue
		}
		// The parsed payload identity is authoritative. Pass its normalized
		// key to every replay implementation; Producer.Republish repeats this
		// safeguard at the Kafka write boundary.
		err := r.republish(ctx, []byte(candidate.eventID), candidate.message.Value)
		if err != nil {
			r.republishFail.Add(1)
			if isTenantScopeFailure(err) {
				r.tenantScopeMismatches.Add(1)
				scopePending++
				r.logTenantMismatch(candidate.tenantID, candidate.eventID, err)
				continue
			}
			closed, closeErr := r.closeTenantDeliveryFailure(id, candidate, wanted, err, resolved)
			if closeErr != nil {
				return replayed, resolved, closeErr
			}
			if closed {
				if !wanted.oneShot[id] {
					replayed++
				}
				continue
			}
			continue
		}
		if err := r.state.MarkTenant(candidate.tenantID, candidate.eventID); err != nil {
			return replayed, resolved, err
		}
		resolved[id] = true
		r.replayed.Add(1)
		replayed++
		r.logger.Printf("replayed tenant=%s event_id=%s", sanitizeLogField(candidate.tenantID, 64), sanitizeLogField(candidate.eventID, 64))
	}
	r.tenantScopePending.Store(uint64(scopePending))
	return replayed, resolved, nil
}

// tenantUnresolvableAlreadyMarked reports whether this record's durable
// unresolvable drop was already recorded in a prior round/restart, so the
// unresolvable counter is not inflated across re-deliveries (FM-7).
func (r *Replayer) tenantUnresolvableAlreadyMarked(record dlqRecord) bool {
	if record.claimedTenantID != "" {
		return r.state.Marked(record.claimedTenantID, record.eventID)
	}
	return r.state.marked(record.eventID) // unscoped idempotency probe only
}

// markTenantUnresolvable durably records the loss. With a trusted tenant claim
// we write a scoped (tenant,event) mark; with no tenant identity available
// (optional Failure.tenant_id) we fall back to an unscoped mark used purely
// for round/restart idempotency — Marked() only honors unscoped marks under an
// explicit legacyTenantScope, so it never suppresses another tenant's
// resolution (REQ-tenant-safety).
func (r *Replayer) markTenantUnresolvable(record dlqRecord) error {
	if record.claimedTenantID != "" {
		return r.state.MarkTenant(record.claimedTenantID, record.eventID)
	}
	return r.state.Mark(record.eventID)
}

// closeTenantDeliveryFailure preserves the existing permanent/one-shot
// policies after the tenant-scope guard has already been applied. It returns
// true only when a durable closure was recorded.
func (r *Replayer) closeTenantDeliveryFailure(id dlqRecordID, candidate tenantAcceptedCandidate, wanted wantedSet, err error, resolved map[dlqRecordID]bool) (bool, error) {
	var deliveryErr *outbox.DeliveryError
	oneShot := wanted.oneShot[id]
	liveRejected := !oneShot && errors.As(err, &deliveryErr) && deliveryErr.Permanent
	if !oneShot && !liveRejected {
		r.logger.Printf("republish failed tenant=%s event_id=%s error=%v; will retry next round", sanitizeLogField(candidate.tenantID, 64), sanitizeLogField(candidate.eventID, 64), err)
		return false, nil
	}
	if oneShot {
		r.logger.Printf("PERMANENT closure tenant=%s event_id=%s code=permanent_error one-shot republish attempt failed: %s; DLQ offset committed", sanitizeLogField(candidate.tenantID, 64), sanitizeLogField(candidate.eventID, 64), sanitizeLogField(fmt.Sprintf("%v", err), 200))
	} else {
		r.logger.Printf("PERMANENT closure tenant=%s event_id=%s republish rejected (permanent): %s; DLQ offset committed", sanitizeLogField(candidate.tenantID, 64), sanitizeLogField(candidate.eventID, 64), sanitizeLogField(fmt.Sprintf("%v", err), 200))
	}
	if markErr := r.state.MarkTenant(candidate.tenantID, candidate.eventID); markErr != nil {
		return false, markErr
	}
	resolved[id] = true
	r.permanent.Add(1)
	return true, nil
}

func (r *Replayer) logTenantPendingRecord(record dlqRecord, found bool, candidateCount int) {
	reason := "canonical tenant correlation unavailable"
	if found && candidateCount > 1 {
		reason = "multiple canonical tenants share event_id"
	}
	r.logger.Printf("tenant-scope pending topic=%s partition=%d offset=%d event_id=%s claimed_tenant=%s reason=%s",
		sanitizeLogField(record.id.topic, 64), record.id.partition, record.id.offset,
		sanitizeLogField(record.eventID, 64), sanitizeLogField(record.claimedTenantID, 64), reason)
}

func (r *Replayer) logTenantMismatch(tenantID, eventID string, err error) {
	r.logger.Printf("tenant-scope pending tenant=%s event_id=%s reason=%s", sanitizeLogField(tenantID, 64), sanitizeLogField(eventID, 64), sanitizeLogField(fmt.Sprintf("%v", err), 200))
}

func isTenantScopeFailure(err error) bool {
	var deliveryErr *outbox.DeliveryError
	return errors.As(err, &deliveryErr) && (deliveryErr.TenantMismatch || deliveryErr.TenantScopeBlocked)
}

// decodeCanonicalReplayEvent requires one complete JSON value and both
// identity fields. It intentionally does not validate the whole domain
// envelope: HTTPDeliverer/API validation remains authoritative, while replay
// correlation needs only a safely decoded canonical event with a tenant.
func decodeCanonicalReplayEvent(value []byte) (event canonicalReplayEvent, err error) {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	if err := decoder.Decode(&event); err != nil {
		return canonicalReplayEvent{}, fmt.Errorf("decode canonical replay event: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != nil {
		if errors.Is(err, io.EOF) {
			return event, nil
		}
		return canonicalReplayEvent{}, fmt.Errorf("canonical replay event has trailing data: %w", err)
	}
	return canonicalReplayEvent{}, fmt.Errorf("canonical replay event contains multiple JSON values")
}

type canonicalReplayEvent struct {
	EventID  string `json:"event_id"`
	TenantID string `json:"tenant_id"`
}
