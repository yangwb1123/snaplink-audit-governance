package kafka

// One replay round's bookkeeping: which DLQ records this round wants, how a
// recovered event id resolves to the records that asked for it, and the two
// scans (DLQ collect, accepted-topic recovery) they drive. RunOnce in
// replay.go owns the ordering; the rules that decide *which* records are
// in scope and when a round may stop live here.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/snaplink/audit-governance/internal/outbox"
)

// dlqRecordID identifies one physical DLQ record. It deliberately contains
// only the effective topic, partition, and offset: neither an untrusted Kafka
// key nor the Failure event_id can collapse two records into one.
type dlqRecordID struct {
	topic     string
	partition int
	offset    int64
	// sequence is normally zero because Kafka guarantees a unique
	// (topic, partition, offset). It only disambiguates duplicate identities
	// supplied by broker-free test readers; production records never need it.
	sequence uint32
}

// dlqRecord is one collected DLQ failure with its parsed event metadata and
// physical record identity. The collector retains only id/reason for malformed
// records and no fetched message bytes; message is a compatibility fallback
// for same-package fixtures that construct records directly.
type dlqRecord struct {
	id              dlqRecordID
	message         kafka.Message
	class           dlqRecordClass
	reason          failureReason
	eventID         string
	claimedTenantID string
	errorCode       string
	wanted          bool
	// authBlocked marks an error_code=unauthorized record held blocked this
	// round: excluded from the wanted set (scanAccepted and the
	// drain-to-unresolvable loop never touch it) yet kept wanted=true so the
	// commit barrier keeps its offset pending (never marked, never
	// committed, never resolvable) until the credential is fixed and
	// -replay-auth-blocked re-admits it.
	authBlocked bool
}

// wantedSet is the round's replay target set. replay, oneShot, orderedIDs,
// and commit decisions are all keyed by physical DLQ record identity.
// byEventID is only a lookup index for locating accepted-topic candidates; it
// is never used to authorize a mark or commit an offset. records retains the
// claim and event metadata needed by tenant-aware correlation.
type wantedSet struct {
	replay     map[dlqRecordID]bool
	oneShot    map[dlqRecordID]bool
	byEventID  map[string][]dlqRecordID
	orderedIDs []dlqRecordID
	records    map[dlqRecordID]dlqRecord
}

// wantedRecordID supplies deterministic identities for same-package tests
// that construct dlqRecord values directly. Kafka-collected records always
// receive their topic/partition/offset identity in collectFailures.
func wantedRecordID(record dlqRecord, index int) dlqRecordID {
	if record.id != (dlqRecordID{}) {
		return record.id
	}
	return dlqRecordID{partition: index, offset: int64(index)}
}

// wantedEvents returns the round's replay target records: collected records
// not yet marked replayed and not held auth-blocked, plus the per-record
// subset whose DLQ record carries error_code=permanent_error. error_code must
// never be echoed into a log line or metric label at the classification sites
// — the vocabulary is a free string from an untrusted boundary (CWE-117).
func wantedEvents(collected []dlqRecord) wantedSet {
	wanted := wantedSet{
		replay:     map[dlqRecordID]bool{},
		oneShot:    map[dlqRecordID]bool{},
		byEventID:  map[string][]dlqRecordID{},
		orderedIDs: []dlqRecordID{},
		records:    map[dlqRecordID]dlqRecord{},
	}
	for index, record := range collected {
		if record.class == dlqRecordMalformed || !record.wanted || record.authBlocked {
			continue
		}
		id := wantedRecordID(record, index)
		if wanted.replay[id] {
			continue
		}
		record.id = id
		wanted.replay[id] = true
		wanted.records[id] = record
		wanted.byEventID[record.eventID] = append(wanted.byEventID[record.eventID], id)
		wanted.orderedIDs = append(wanted.orderedIDs, id)
		if record.errorCode == ErrorCodePermanentError {
			wanted.oneShot[id] = true
		}
	}
	return wanted
}

// resolveLegacyEventRecords bridges the historical event-ID replay seam to
// the physical commit set. Legacy mode may still share one state mark for
// duplicate event IDs, but every collected DLQ record gets its own durable
// commit decision.
func resolveLegacyEventRecords(wanted wantedSet, eventID string, resolved map[dlqRecordID]bool) {
	for _, id := range wanted.byEventID[eventID] {
		resolved[id] = true
	}
}

func legacyEventResolved(wanted wantedSet, eventID string, resolved map[dlqRecordID]bool) bool {
	ids := wanted.byEventID[eventID]
	if len(ids) == 0 {
		return false
	}
	for _, id := range ids {
		if !resolved[id] {
			return false
		}
	}
	return true
}

func legacyEventOneShot(wanted wantedSet, eventID string) bool {
	for _, id := range wanted.byEventID[eventID] {
		if wanted.oneShot[id] {
			return true
		}
	}
	return false
}

// commitResolved advances the DLQ reader past every collected record whose
// event reached a durable decision (record.wanted false — already replayed
// or unparsable at collect time — or present in resolved this round) AND
// whose offset is below the first still-pending record in its partition.
//
// Kafka consumer groups track ONE committed offset per partition, and
// kafka-go's CommitMessages commits offset+1 of the highest message passed
// (offsetStash.merge keeps the per-partition max), so committing a later
// record silently commits past every earlier record in the partition —
// including a pending one, which the next round would then never re-deliver
// (permanent loss). The barrier therefore leaves resolved records at or
// above the first pending offset uncommitted: the next round re-reads them
// and the state file makes the re-read idempotent (no re-republish).
func (r *Replayer) commitResolved(ctx context.Context, collected []dlqRecord, resolved map[dlqRecordID]bool) error {
	type partitionKey struct {
		topic     string
		partition int
	}
	// firstPending[partition] = lowest offset of a record that must stay
	// pending; no commit may cross it.
	firstPending := map[partitionKey]int64{}
	for _, record := range collected {
		if record.class == dlqRecordMalformed {
			continue
		}
		id := dlqRecordIdentity(record)
		if record.authBlocked || (record.wanted && !resolved[id]) {
			key := partitionKey{topic: id.topic, partition: id.partition}
			if offset, ok := firstPending[key]; !ok || id.offset < offset {
				firstPending[key] = id.offset
			}
		}
	}
	for _, record := range collected {
		id := dlqRecordIdentity(record)
		key := partitionKey{topic: id.topic, partition: id.partition}
		if pending, ok := firstPending[key]; ok && id.offset >= pending {
			continue // resolved but at/above the barrier: committing would leapfrog the pending record
		}
		if record.class != dlqRecordMalformed && (record.authBlocked || (record.wanted && !resolved[id])) {
			continue // auth-blocked: config-fixable, stays pending until the credential is restored (or -replay-auth-blocked)
		}
		if err := r.dlqReader.CommitMessages(ctx, dlqRecordCommitMessage(record)); err != nil {
			return err
		}
	}
	return nil
}

// isDrained reports whether err is the drain window expiring (or the outer
// context being cancelled), as opposed to a transport failure.
func isDrained(ctx context.Context, err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || ctx.Err() != nil
}

// collectFailures drains the DLQ topic (until the drain window expires)
// WITHOUT committing offsets — records are committed by RunOnce only after
// the round's replay work, so transient republish failures or crashes leave
// them pending for the next round. Returns the held messages and the event
// IDs that are not yet marked replayed.
func (r *Replayer) collectFailures(ctx context.Context) ([]dlqRecord, error) {
	r.malformedOverflow = nil
	drainCtx, cancel := context.WithTimeout(ctx, r.drainWindow())
	defer cancel()
	// Per-round budget for the per-record auth-blocked detail log lines:
	// the first maxAuthBlockedLogDetails blocked records of THIS round get a
	// detail line; every round still counts and summarizes the total.
	r.authBlockedDetailBudget.Store(0)
	var collected []dlqRecord
	seenPhysical := map[dlqRecordID]uint32{}
	malformedCount, malformedBytes := 0, uint64(0)
	defer func() {
		if malformedCount > 0 {
			logMalformedSummary(r.logger, malformedCount)
		}
	}()
	for {
		message, err := r.dlqReader.FetchMessage(drainCtx)
		if err != nil {
			return collected, err
		}
		effectiveTopic := message.Topic
		if effectiveTopic == "" {
			effectiveTopic = r.dlqReader.Config().Topic
		}
		physicalID := dlqRecordID{topic: effectiveTopic, partition: message.Partition, offset: message.Offset}
		if sequence := seenPhysical[physicalID]; sequence > 0 {
			physicalID.sequence = sequence
		}
		seenPhysical[dlqRecordID{topic: effectiveTopic, partition: message.Partition, offset: message.Offset}]++
		record := dlqRecord{id: physicalID}
		validation := decodeStrictFailure(message.Value)
		if validation.Reason != "" {
			malformedCount++
			r.malformed.Add(1)
			malformedBytes += malformedPayloadBytes(message)
			if malformedCount <= maxMalformedLogDetails {
				logMalformedRecord(r.logger, effectiveTopic, message.Partition, message.Offset, validation.Reason)
			}
			record.class = dlqRecordMalformed
			record.reason = validation.Reason
			if malformedCollectionExceeded(malformedCount, malformedBytes, message) {
				// Keep only the physical identity and fixed reason. RunOnce merges
				// this one overflow record before applying partition barriers so a
				// safe malformed offset is still quarantined.
				r.malformedOverflow = append(r.malformedOverflow, record)
				return collected, nil
			}
		} else {
			failure := validation.Failure
			r.dlqRecords.Add(1)
			record.eventID = failure.EventID
			record.claimedTenantID = failure.TenantID
			record.errorCode = failure.ErrorCode
			// REQ-PERM-3: per-round collection-time traffic counter for
			// error_code=attempts_exhausted records, mirroring dlqRecords
			// semantics (re-delivered records count again per round).
			if failure.ErrorCode == ErrorCodeAttemptsExhausted {
				r.attemptsExhausted.Add(1)
			}
			record.wanted = r.failureWanted(failure.EventID)
			// error_code=unauthorized is config-fixable: while the operator has
			// not acknowledged the credential is fixed (-replay-auth-blocked
			// off, the default), the record is blocked — excluded from the
			// wanted set, never marked, never committed. The per-record detail
			// line is budgeted (H1); the echoed fields are sanitized because
			// the DLQ topic is an untrusted input boundary (M3/CWE-117).
			record.authBlocked = failure.ErrorCode == ErrorCodeUnauthorized &&
				record.wanted && !r.replayAuthBlocked.Load()
			if record.authBlocked {
				r.authBlocked.Add(1)
				if r.authBlockedDetailBudget.Add(1) <= maxAuthBlockedLogDetails {
					r.logger.Printf("dlq record auth-blocked topic=%s partition=%d offset=%d event_id=%s error=%s (fix AUDIT_OUTBOX_TOKEN before replay)",
						effectiveTopic, message.Partition, message.Offset,
						sanitizeLogField(failure.EventID, 64), sanitizeLogField(failure.ErrorMessage, 200))
				}
			}
		}
		collected = append(collected, record)
		if len(collected) >= maxDLQRecordsPerRound {
			return collected, nil
		}
	}
}

// scanAccepted walks the accepted topic from its first retained offset and
// re-publishes every message whose payload event_id (authoritative) or,
// only when the payload probe is empty, the key (fallback for unparsable
// values) matches a wanted event ID. The key fallback never overrides a
// valid payload event_id, even a conflicting one: a message whose payload
// carries a non-empty but unwanted event_id is skipped, so a DLQ record
// whose only trace is a stale key cannot close the wrong DLQ record. Replay
// is idempotent: events already in the state file are skipped. A key-matched
// but value-unparsable message is marked replayed with a log line, mirroring
// the consumer's anti-loop rule.
//
// The scan start is guaranteed by RunOnce's per-round
// resetAcceptedToFirstOffset: a fresh accepted reader (StartOffset:
// kafka.FirstOffset, no committed offsets) is created before every scan, so
// a wanted event whose original predates an earlier round's scan end is
// still reachable. Without that reset the drained path below would mark such
// records unresolvable even though their originals are retained.
//
// The scan ends when (a) one full drain window passes with no message
// (drained=true: the reader is caught up to the high watermark, so the
// retained topic was fully scanned) or (b) the round reaches
// maxScanWindows x drainTimeout (drained=false: sustained ingest cut the
// round off — nothing is marked unresolvable and everything unresolved
// stays pending for the next round). A transport error sets scanErr and
// leaves everything pending. Returns the number of events actually
// re-published this round (permanent closures excluded — REQ-PERM-2) and
// the set of event IDs that reached a durable decision this round
// (replayed, permanently closed, unparsable, or converged-as-unresolvable
// — NOT transiently failed).
func (r *Replayer) scanAcceptedLegacy(ctx context.Context, wanted wantedSet) (int, map[dlqRecordID]bool, error) {
	window := r.drainWindow()
	scanCtx, cancel := context.WithTimeout(ctx, maxScanWindows*window)
	defer cancel()
	replayed := 0
	resolved := map[dlqRecordID]bool{}
	found := map[string]bool{} // wanted IDs whose message was matched this round
	drained := false           // true only after a full quiet window with no message
	var scanErr error
	lastMessage := time.Now()
	for {
		fetchCtx, fetchCancel := context.WithTimeout(scanCtx, window)
		message, err := r.accepted.FetchMessage(fetchCtx)
		fetchCancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil && time.Since(lastMessage) >= window {
				drained = true // topic quiet: the retained topic was fully scanned
			} else if ctx.Err() == nil {
				scanErr = err // transport failure: leave everything pending
			}
			break
		}
		lastMessage = time.Now()
		r.acceptedSeen.Add(1)
		payloadID := eventIDFromValue(message.Value) // decode once per message
		eventID := ""
		if payloadID != "" && len(wanted.byEventID[payloadID]) > 0 && !r.state.marked(payloadID) {
			eventID = payloadID // payload event_id is authoritative
		} else if keyID := string(message.Key); payloadID == "" && len(wanted.byEventID[keyID]) > 0 && !r.state.marked(keyID) {
			eventID = keyID // fallback only when the payload probe is empty (unparsable values whose only signal is the key; mirrors deadLetterUnparsable)
		}
		if eventID == "" {
			continue
		}
		found[eventID] = true
		if payloadID == "" {
			// Key-matched, value not canonical: existing anti-loop mark + log.
			r.logger.Printf("accepted message unparsable topic=%s partition=%d offset=%d event_id=%s; marking replayed to avoid loop", r.accepted.Config().Topic, message.Partition, message.Offset, eventID)
			if err := r.state.Mark(eventID); err != nil {
				return replayed, resolved, err
			}
			resolveLegacyEventRecords(wanted, eventID, resolved)
			r.unparsableMarks.Add(1)
			replayed++
			continue
		}
		// The payload event_id is authoritative. Normalize the outgoing
		// transport key here as well as in Producer.Republish so every
		// RepublishFunc implementation receives the contract key, regardless
		// of stale or missing source-topic metadata.
		if err := r.republish(ctx, []byte(payloadID), message.Value); err != nil {
			r.republishFail.Add(1) // inclusive semantics unchanged (2026-08-15)
			// REQ-PERM-1: a wanted error_code=permanent_error record gets ONE
			// republish attempt in its lifetime; any failure closes it as a
			// permanent closure (poison-pill policy). Non-permanent codes keep
			// the legacy policy: only a live DeliveryError.Permanent rejection
			// closes; transient failures retry next round. The one-shot check
			// runs BEFORE the live-rejection check so a permanent_error record
			// never reaches the legacy path (which would count replayed++).
			var deliveryErr *outbox.DeliveryError
			oneShot := legacyEventOneShot(wanted, eventID)
			liveRejected := !oneShot && errors.As(err, &deliveryErr) && deliveryErr.Permanent
			if oneShot || liveRejected {
				if oneShot {
					r.logger.Printf("PERMANENT closure event_id=%s code=permanent_error one-shot republish attempt failed: %s; DLQ offset committed",
						sanitizeLogField(eventID, 64), sanitizeLogField(fmt.Sprintf("%v", err), 200))
				} else {
					// Legacy live-rejection closure for non-permanent-code
					// records: behavior unchanged (design §2.3), log modernized.
					r.logger.Printf("PERMANENT closure event_id=%s republish rejected (permanent): %s; DLQ offset committed",
						sanitizeLogField(eventID, 64), sanitizeLogField(fmt.Sprintf("%v", err), 200))
				}
				if markErr := r.state.Mark(eventID); markErr != nil {
					return replayed, resolved, markErr
				}
				resolveLegacyEventRecords(wanted, eventID, resolved)
				r.permanent.Add(1)
				if liveRejected {
					replayed++ // legacy return contribution preserved; the one-shot never counts (REQ-PERM-2)
				}
				continue
			}
			r.logger.Printf("republish failed event_id=%s error=%v; will retry next round", eventID, err)
			continue // found => stays pending, never marked unresolvable
		}
		if err := r.state.Mark(eventID); err != nil {
			return replayed, resolved, err
		}
		resolveLegacyEventRecords(wanted, eventID, resolved)
		r.replayed.Add(1)
		replayed++
		r.logger.Printf("replayed event_id=%s", eventID)
	}
	if drained {
		// REQ-2: a quiet-topic scan from the first retained offset without a
		// match is definitive (the consumer published the Failure only after
		// fetching the original). Mark unresolvable this round so
		// commitResolved commits the DLQ offset — no attempt cap needed.
		// Found-but-failed events (transient republish error) stay pending.
		for eventID := range wanted.byEventID {
			if found[eventID] || legacyEventResolved(wanted, eventID, resolved) {
				continue
			}
			r.logger.Printf("unresolvable event_id=%s reason=original-not-found-in-accepted-topic round=%s", eventID, time.Now().Format(time.RFC3339))
			if err := r.state.Mark(eventID); err != nil {
				return replayed, resolved, err
			}
			resolveLegacyEventRecords(wanted, eventID, resolved)
			r.unresolvable.Add(1)
			replayed++
		}
	}
	return replayed, resolved, scanErr
}
