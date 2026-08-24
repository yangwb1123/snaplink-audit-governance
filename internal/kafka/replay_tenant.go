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
func (r *Replayer) scanAccepted(ctx context.Context, wanted wantedSet) (int, map[string]bool, error) {
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

type tenantAcceptedCandidate struct {
	message  kafka.Message
	tenantID string
	eventID  string
}

// scanAcceptedTenantAware never delivers while the accepted topic is still
// being scanned. A later candidate with the same event_id but another tenant
// would make an early delivery an unsafe guess, so candidate collection and
// resolution are deliberately separate phases.
func (r *Replayer) scanAcceptedTenantAware(ctx context.Context, wanted wantedSet) (int, map[string]bool, error) {
	candidates, found, drained, scanErr := r.collectTenantCandidates(ctx, wanted)
	if scanErr != nil {
		return 0, map[string]bool{}, scanErr
	}
	if !drained {
		r.tenantScopePending.Store(uint64(len(wanted.replay)))
		return 0, map[string]bool{}, nil
	}
	return r.resolveTenantCandidates(ctx, wanted, candidates, found)
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
		event, err := decodeCanonicalReplayEvent(message.Value)
		if err != nil {
			// A malformed/key-only value is evidence that the wanted record
			// cannot be safely recovered, not a tenant identity. Keep it
			// pending rather than letting the key create a state mark.
			if id := eventIDFromValue(message.Value); wanted.replay[id] {
				found[id] = true
			}
			continue
		}
		if event.EventID == "" || !wanted.replay[event.EventID] {
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

// resolveTenantCandidates applies durable state and delivery policy only
// after the full accepted scan proved the event_id→tenant correlation. A
// missing candidate, ambiguous candidates, missing resolver credential, and
// tenant_mismatch all remain pending and are counted in the scope backlog.
func (r *Replayer) resolveTenantCandidates(ctx context.Context, wanted wantedSet, candidates map[string][]tenantAcceptedCandidate, found map[string]bool) (int, map[string]bool, error) {
	resolved := map[string]bool{}
	scopePending := 0
	replayed := 0
	for eventID := range wanted.replay {
		items := candidates[eventID]
		if len(items) != 1 {
			scopePending++
			r.logTenantPending(eventID, found[eventID], len(items))
			continue
		}
		candidate := items[0]
		if r.state.Marked(candidate.tenantID, eventID) {
			resolved[eventID] = true
			continue
		}
		if r.republish == nil {
			scopePending++
			r.logTenantPending(eventID, true, 0)
			continue
		}
		err := r.republish(ctx, candidate.message.Key, candidate.message.Value)
		if err != nil {
			r.republishFail.Add(1)
			if isTenantScopeFailure(err) {
				r.tenantScopeMismatches.Add(1)
				scopePending++
				r.logTenantMismatch(candidate.tenantID, eventID, err)
				continue
			}
			closed, closeErr := r.closeTenantDeliveryFailure(eventID, candidate.tenantID, wanted, err, resolved)
			if closeErr != nil {
				return replayed, resolved, closeErr
			}
			if closed {
				if !wanted.oneShot[eventID] {
					replayed++
				}
				continue
			}
			continue
		}
		if err := r.state.MarkTenant(candidate.tenantID, eventID); err != nil {
			return replayed, resolved, err
		}
		resolved[eventID] = true
		r.replayed.Add(1)
		replayed++
		r.logger.Printf("replayed tenant=%s event_id=%s", sanitizeLogField(candidate.tenantID, 64), sanitizeLogField(eventID, 64))
	}
	r.tenantScopePending.Store(uint64(scopePending))
	return replayed, resolved, nil
}

// closeTenantDeliveryFailure preserves the existing permanent/one-shot
// policies after the tenant-scope guard has already been applied. It returns
// true only when a durable closure was recorded.
func (r *Replayer) closeTenantDeliveryFailure(eventID, tenantID string, wanted wantedSet, err error, resolved map[string]bool) (bool, error) {
	var deliveryErr *outbox.DeliveryError
	oneShot := wanted.oneShot[eventID]
	liveRejected := !oneShot && errors.As(err, &deliveryErr) && deliveryErr.Permanent
	if !oneShot && !liveRejected {
		r.logger.Printf("republish failed tenant=%s event_id=%s error=%v; will retry next round", sanitizeLogField(tenantID, 64), sanitizeLogField(eventID, 64), err)
		return false, nil
	}
	if oneShot {
		r.logger.Printf("PERMANENT closure tenant=%s event_id=%s code=permanent_error one-shot republish attempt failed: %s; DLQ offset committed", sanitizeLogField(tenantID, 64), sanitizeLogField(eventID, 64), sanitizeLogField(fmt.Sprintf("%v", err), 200))
	} else {
		r.logger.Printf("PERMANENT closure tenant=%s event_id=%s republish rejected (permanent): %s; DLQ offset committed", sanitizeLogField(tenantID, 64), sanitizeLogField(eventID, 64), sanitizeLogField(fmt.Sprintf("%v", err), 200))
	}
	if markErr := r.state.MarkTenant(tenantID, eventID); markErr != nil {
		return false, markErr
	}
	resolved[eventID] = true
	r.permanent.Add(1)
	return true, nil
}

func (r *Replayer) logTenantPending(eventID string, found bool, candidateCount int) {
	reason := "canonical tenant correlation unavailable"
	if found && candidateCount > 1 {
		reason = "multiple canonical tenants share event_id"
	}
	r.logger.Printf("tenant-scope pending event_id=%s reason=%s", sanitizeLogField(eventID, 64), reason)
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
