package kafka

// The replay round driver: reader construction, the per-round offset resets
// that keep a rescan from reusing the previous round's end position,
// metrics, and the scheduler loop. Record bookkeeping and the mark set are in
// replay_scan.go and replay_state.go.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/snaplink/audit-governance/internal/domain"
)

// RepublishFunc redelivers one recovered event. Implementations write the
// original accepted-topic message back (Producer.Republish) or ingest it
// through the audit API (outbox.HTTPDeliverer).
type RepublishFunc func(ctx context.Context, key, value []byte) error

// defaultDrainTimeout bounds one RunOnce round: Kafka readers block on
// FetchMessage, so "drain the topic" is expressed as "fetch until no
// message arrives within this window".
const defaultDrainTimeout = 5 * time.Second

// maxScanWindows bounds one scanAccepted round under sustained ingest: a
// topic that never goes quiet must not hold the round open. Two windows
// give a slow topic at least one full quiet-window chance to signal
// "end of retained topic" before the round is cut off.
const maxScanWindows = 2

// Replayer redelivers dead-lettered events. DLQ records carry only failure
// metadata (event_id, error_code, error_message), so the original event is
// recovered from the accepted topic by key and re-published (or re-ingested
// through the API). The accepted topic is always scanned from the first
// retained offset: every round recreates the accepted reader with
// StartOffset: kafka.FirstOffset (kafka-go honors StartOffset only at
// reader creation and rejects SetOffset for group readers), so no round
// reuses the previous round's end position. The DLQ topic is always drained
// from the group's committed offset: every round also recreates the DLQ
// reader, because kafka-go never re-delivers a fetched record within one
// group session (the fetch position advances past every FetchMessage,
// committed or not) — recreation is the only mechanism that lets a
// transiently-failed record be retried by a later round. The per-partition
// commit barrier in commitResolved guarantees the group's committed offset
// never crosses a still-pending record, so a recreated session always
// re-delivers the pending records. Replayed IDs in the state file make the
// re-scan idempotent, and committing the accepted scan position would make
// new DLQ records miss older original messages.
type Replayer struct {
	dlqReader   messageReader
	dlqNew      func() messageReader // per-round DLQ reader recreation (F1)
	accepted    messageReader
	acceptedNew func() messageReader // per-round accepted reader recreation (REQ-1)
	republish   RepublishFunc
	state       *ReplayState
	// tenantAware is true for production replayers. The legacy test seam is
	// deliberately kept false so old byte-compatible event-ID fixtures remain
	// useful; production Kafka and API modes both use canonical tenant state.
	tenantAware       bool
	logger            *log.Logger
	malformedOverflow []dlqRecord // identity-only overflow record merged before commit
	drainTimeout      time.Duration
	dlqRecords        atomic.Uint64
	malformed         atomic.Uint64
	acceptedSeen      atomic.Uint64
	// replayed counts only successful re-publishes (resolution path a).
	// Every other first-resolution path has its own counter below, so the
	// metric never conflates a replay with a convergence (REQ-1).
	replayed atomic.Uint64
	// permanent counts permanent closures — durable first resolutions where
	// a record is closed WITHOUT replay, under two policies: (1) the one-shot
	// closure of an error_code=permanent_error record whose single republish
	// attempt failed for a known non-uncertain reason (transient or permanent;
	// timeout/cancellation remains pending), and (2) the legacy live-rejection closure of any other record whose
	// republish was rejected with DeliveryError.Permanent. Both converge
	// the round and are alertable via republishFail, but never a replay
	// (REQ-PERM-2).
	permanent atomic.Uint64
	// attemptsExhausted counts collected DLQ records with
	// error_code=attempts_exhausted: per-round collection-time increments
	// mirroring dlqRecords semantics — a traffic counter, not a resolution
	// counter (re-delivered records count again per round; REQ-PERM-3).
	attemptsExhausted atomic.Uint64
	// unresolvable counts drained-unresolvable events (path d): the original
	// is absent from the accepted topic (retention expiry or recreation),
	// the DLQ offset is committed, and the event is dropped — a permanent
	// loss signal surfaced by the AuditDLQUnresolvableDrop alert.
	unresolvable atomic.Uint64
	// unparsableMarks counts anti-loop marks for key-matched accepted
	// messages whose value carries no canonical event_id (path b): neither
	// a replay nor a loss, but a first resolution that must stay observable.
	unparsableMarks atomic.Uint64
	republishFail   atomic.Uint64
	pending         atomic.Uint64
	// authBlocked counts error_code=unauthorized DLQ records held blocked
	// (per-round increments, mirroring dlqRecords semantics): a counter that
	// never falls, exported as audit_dlq_auth_blocked_total.
	authBlocked atomic.Uint64
	// authBlockedPending is the gauge of blocked records collected by the
	// LATEST round (audit_dlq_auth_blocked): it reflects the current blocked
	// backlog, falls to 0 once a round collects none (drained or flag
	// re-admitted them), and is the only signal for a STATIC backlog —
	// blocked records are excluded from the wanted set, so audit_dlq_pending
	// stays 0 in blocked-only rounds.
	authBlockedPending atomic.Uint64
	// replayAuthBlocked gates the block: default off (= the operator has not
	// acknowledged the credential is fixed) holds unauthorized records
	// pending; on, they are re-admitted to the wanted set and replay normally.
	replayAuthBlocked atomic.Bool
	// authBlockedDetailBudget is the per-round budget for the per-record
	// auth-blocked detail log lines (H1: the full backlog must never flood
	// the log); the round summary line always reports the total.
	authBlockedDetailBudget atomic.Uint64
	// tenantScopeMismatches counts exact API tenant_mismatch responses and
	// resolver/configuration scope failures. tenantScopePending is the latest
	// round's tenant-safety backlog gauge; neither path marks or commits.
	tenantScopeMismatches atomic.Uint64
	tenantScopePending    atomic.Uint64
}

// NewReplayer creates the DLQ and accepted-topic readers. The DLQ reader
// commits offsets only for durable decisions (committed once resolved, with
// the commitResolved barrier); the accepted reader never commits and always
// starts at the first retained offset — because StartOffset is honored only
// at reader creation, RunOnce recreates the accepted reader at the start of
// every round (resetAcceptedToFirstOffset), including the first round (the
// eagerly-created reader is replaced before its first fetch; kafka-go joins
// the group lazily, so this costs nothing). RunOnce recreates the DLQ reader
// as well (resetDLQToCommittedOffset), so each round drains from the group's
// committed offset and transiently-failed records are re-delivered.
func NewReplayer(brokers []string, dlqTopic, acceptedTopic, groupID string, state *ReplayState, republish RepublishFunc, logger *log.Logger) *Replayer {
	if dlqTopic == "" {
		dlqTopic = TopicDLQ
	}
	if acceptedTopic == "" {
		acceptedTopic = TopicAccepted
	}
	if groupID == "" {
		groupID = "audit-dlq-replay"
	}
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	dlqConfig := kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          dlqTopic,
		GroupID:        groupID + "-dlq",
		MinBytes:       1,
		MaxBytes:       1 << 20,
		CommitInterval: 0, // manual commits only
		StartOffset:    kafka.FirstOffset,
	}
	dlqReader := kafka.NewReader(dlqConfig)
	acceptedConfig := kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          acceptedTopic,
		GroupID:        groupID + "-accepted",
		MinBytes:       1,
		MaxBytes:       1 << 20,
		CommitInterval: 0, // never commit: every round re-scans from the start
		StartOffset:    kafka.FirstOffset,
	}
	acceptedReader := kafka.NewReader(acceptedConfig)
	return &Replayer{dlqReader: dlqReader, dlqNew: func() messageReader { return kafka.NewReader(dlqConfig) }, accepted: acceptedReader, acceptedNew: func() messageReader { return kafka.NewReader(acceptedConfig) }, republish: republish, state: state, tenantAware: true, logger: logger, drainTimeout: defaultDrainTimeout}
}

// newReplayerWithReaders is the test seam: the same messageReader contract
// as Consumer, so replay behavior is exercised without a broker.
func newReplayerWithReaders(dlq, accepted messageReader, state *ReplayState, republish RepublishFunc) *Replayer {
	return newReplayerWithReadersAndLogger(dlq, accepted, state, republish, log.New(io.Discard, "", 0))
}

// newReplayerWithReadersAndLogger is the test seam for acceptance tests that
// capture the durable log lines (e.g. the unresolvable mark) in addition to
// reader-level behavior.
func newReplayerWithReadersAndLogger(dlq, accepted messageReader, state *ReplayState, republish RepublishFunc, logger *log.Logger) *Replayer {
	// dlqNew/acceptedNew return the same injected fakes: rounds keep the
	// fakes' positions unless a test replaces them (existing retry tests
	// reset dlq.index/accepted.index manually to simulate redelivery).
	return &Replayer{dlqReader: dlq, dlqNew: func() messageReader { return dlq }, accepted: accepted, acceptedNew: func() messageReader { return accepted }, republish: republish, state: state, logger: logger, drainTimeout: defaultDrainTimeout}
}

// newReplayerWithAcceptedFactory is the REQ-1 test seam: a factory that
// returns a FRESH accepted reader per round models the production per-round
// recreation (a fresh reader joins with no committed offset and starts at the
// first retained offset), which is the mechanism under test. The initial
// reader is created eagerly, mirroring NewReplayer.
func newReplayerWithAcceptedFactory(dlq messageReader, acceptedNew func() messageReader, state *ReplayState, republish RepublishFunc, logger *log.Logger) *Replayer {
	return &Replayer{dlqReader: dlq, dlqNew: func() messageReader { return dlq }, accepted: acceptedNew(), acceptedNew: acceptedNew, republish: republish, state: state, logger: logger, drainTimeout: defaultDrainTimeout}
}

// newReplayerWithFactories is the F1 test seam: factories that hand out a
// FRESH reader per round for BOTH the DLQ and accepted sides, modeling the
// production per-round recreation. The initial readers are created eagerly,
// mirroring NewReplayer.
func newReplayerWithFactories(dlqNew, acceptedNew func() messageReader, state *ReplayState, republish RepublishFunc, logger *log.Logger) *Replayer {
	return &Replayer{dlqReader: dlqNew(), dlqNew: dlqNew, accepted: acceptedNew(), acceptedNew: acceptedNew, republish: republish, state: state, logger: logger, drainTimeout: defaultDrainTimeout}
}

// maxAuthBlockedLogDetails bounds the per-round per-record auth-blocked log
// detail lines. The blocked backlog can reach millions of records (the exact
// incident scenario: a rotated token dead-letters the whole accepted stream);
// logging every record every round would flood the log at ~3k lines/s. The
// first records of each round get a detail line, and the round summary
// always reports the total (H1).
const maxAuthBlockedLogDetails = 10

// SetReplayAuthBlocked re-admits error_code=unauthorized DLQ records to the
// replay set. Call only after the ingest credential is verified working;
// while the credential is still broken each round re-publishes once and the
// consumer re-dead-letters (visible via AuditDLQTraffic and
// AuditDLQAuthBlocked).
func (r *Replayer) SetReplayAuthBlocked(enabled bool) {
	r.replayAuthBlocked.Store(enabled)
}

// authBlockedCount reports how many collected records are held auth-blocked.
func authBlockedCount(collected []dlqRecord) uint64 {
	var blocked uint64
	for _, record := range collected {
		if record.authBlocked {
			blocked++
		}
	}
	return blocked
}

// ReplayerMetrics is the /metrics snapshot of the replay counters. Every DLQ
// event that reaches a durable first resolution increments exactly one of
// Replayed / UnparsableMarks / Permanent / Unresolvable; a transient
// republish failure of a non-permanent-code record increments none of them
// (the record stays pending for the next round). Permanent counts closures
// under two policies: (1) the one-shot — a wanted error_code=permanent_error
// record whose single republish attempt failed for a known non-uncertain
// reason (REQ-PERM-1); timeout/cancellation remains pending; (2) the legacy
// live rejection — a non-permanent-code record whose
// republish was rejected with DeliveryError.Permanent. Both are durable
// closures, never a replay. AttemptsExhausted is observational
// (collection-time, per round) and explicitly excluded from the resolution
// invariant: re-delivered records count again per round (REQ-PERM-3).
// AuthBlocked increments per round a blocked record is re-collected;
// AuthBlockedPending is the latest-round blocked backlog gauge.
type ReplayerMetrics struct {
	DLQRecords            uint64 // DLQ Failure records collected
	Malformed             uint64 // malformed DLQ records collected
	AcceptedScanned       uint64 // accepted-topic messages scanned
	Replayed              uint64 // successful re-publishes only
	Permanent             uint64 // permanent closures (one-shot + legacy live rejection)
	Unresolvable          uint64 // drained-unresolvable drops (permanent loss)
	UnparsableMarks       uint64 // anti-loop marks for key-matched unparsable messages
	AttemptsExhausted     uint64 // error_code=attempts_exhausted records collected (per-round traffic, NOT a resolution)
	RepublishFailures     uint64 // transient + permanent republish failures (inclusive)
	Pending               uint64 // wanted events pending in the current round
	AuthBlocked           uint64 // error_code=unauthorized records held blocked (per-round increments)
	AuthBlockedPending    uint64 // blocked records collected by the latest round (gauge)
	TenantScopeMismatches uint64 // resolver/API scope failures (counter)
	TenantScopePending    uint64 // tenant-safety backlog in the latest round (gauge)
}

// Metrics returns the replay resolution counters for the /metrics endpoint.
func (r *Replayer) Metrics() ReplayerMetrics {
	return ReplayerMetrics{
		DLQRecords:            r.dlqRecords.Load(),
		Malformed:             r.malformed.Load(),
		AcceptedScanned:       r.acceptedSeen.Load(),
		Replayed:              r.replayed.Load(),
		Permanent:             r.permanent.Load(),
		Unresolvable:          r.unresolvable.Load(),
		UnparsableMarks:       r.unparsableMarks.Load(),
		AttemptsExhausted:     r.attemptsExhausted.Load(),
		RepublishFailures:     r.republishFail.Load(),
		Pending:               r.pending.Load(),
		AuthBlocked:           r.authBlocked.Load(),
		AuthBlockedPending:    r.authBlockedPending.Load(),
		TenantScopeMismatches: r.tenantScopeMismatches.Load(),
		TenantScopePending:    r.tenantScopePending.Load(),
	}
}

// RunOnce performs one replay round: drain the currently available DLQ
// failures, recover the matching original events from the accepted topic and
// re-publish them. Draining is time-bounded (drainTimeout of quiet topic
// time); returns the number of events replayed this round. The DLQ reader is
// recreated at the start of every round (resetDLQToCommittedOffset) so the
// round starts at the broker's committed offset and transiently-failed
// records are re-delivered; the accepted reader is recreated before every
// scan (resetAcceptedToFirstOffset) so the round always begins at the first
// retained offset, never at the previous round's end position.
//
// DLQ offsets are committed per record, only for events that reached a
// durable decision (replayed, permanently rejected, unparsable, or already
// replayed in an earlier round), and only below the first still-pending
// record in each partition (the commitResolved barrier). A transient
// republish failure (or a crash mid-round) leaves that record uncommitted
// and never leapfrogged, so the next round's recreated DLQ session re-reads
// it and retries. Replayed IDs in the state file keep re-reads idempotent.
func (r *Replayer) RunOnce(ctx context.Context) (int, error) {
	// A scope backlog gauge describes the latest completed round, not the
	// previous round's stale value. The tenant-aware scan repopulates it after
	// a complete accepted-topic pass.
	r.tenantScopePending.Store(0)
	// F1: recreate the DLQ reader so this round drains from the broker's
	// committed offset, not from the previous round's session position.
	// kafka-go never re-delivers a fetched record within one group session
	// (the fetch position advances past every FetchMessage, committed or
	// not), so without recreation a transiently-failed record left
	// uncommitted by round N could never be retried by round N+1. The fresh
	// session joins the group and resumes at the committed offset, and the
	// commitResolved barrier guarantees that offset never crossed a pending
	// record — so every pending record IS re-delivered. A close failure
	// aborts the round: nothing is collected, marked, or committed.
	if err := r.resetDLQToCommittedOffset(); err != nil {
		return 0, err
	}
	// Each phase gets its own drain window: a shared window would let
	// collectFailures burn the whole budget waiting for the DLQ to go quiet,
	// leaving scanAccepted with an already-expired context (real kafka-go
	// returns ctx.Err() before any queued data, so the scan would replay
	// nothing).
	r.malformedOverflow = nil
	collected, err := r.collectFailures(ctx)
	if err != nil && !isDrained(ctx, err) {
		return 0, err
	}
	// Include any identity-only budget overflow in quarantine/barrier handling.
	collected = append(collected, r.malformedOverflow...)
	r.malformedOverflow = nil
	// Auth-blocked records (error_code=unauthorized, block enabled) are
	// excluded from the wanted set below but stay pending as commit
	// barriers. Surface them as the latest-round backlog gauge and one
	// bounded summary line per round (per-record detail lines are budgeted
	// in collectFailures, H1). The gauge is set even on the early path so a
	// blocked-only round is observable via audit_dlq_auth_blocked even
	// though audit_dlq_pending stays 0.
	r.authBlockedPending.Store(authBlockedCount(collected))
	if blocked := r.authBlockedPending.Load(); blocked > 0 {
		r.logger.Printf("dlq round: %d auth-blocked records pending (fix AUDIT_OUTBOX_TOKEN before replay)", blocked)
	}
	wanted := wantedEvents(collected)
	if len(wanted.replay) == 0 {
		// Every collected record is already replayed (or unparsable):
		// consume them so the next round does not re-read the same offsets.
		if commitErr := r.commitResolved(ctx, collected, nil); commitErr != nil {
			return 0, commitErr
		}
		return 0, nil
	}
	// REQ-1: every round re-seeks the accepted scan to the first retained
	// offset. kafka-go honors StartOffset only at reader creation and
	// rejects SetOffset for group readers, so the accepted reader is
	// recreated per round; without this, round N+1 starts where round N
	// ended and older originals are silently marked unresolvable and dropped.
	if err := r.resetAcceptedToFirstOffset(); err != nil {
		return 0, err
	}
	// pending is set only while a scan is actually in flight, so an abort on
	// either reset path never leaks a stale audit_dlq_pending reading.
	r.pending.Store(uint64(len(wanted.replay)))
	replayed, resolved, scanErr := r.scanAccepted(ctx, wanted)
	r.pending.Store(0)
	if scanErr != nil && !isDrained(ctx, scanErr) {
		// Transport failure: the round did not complete. Leave every
		// collected record uncommitted so the next round retries.
		return replayed, scanErr
	}
	// Replay work is done (window expired or transport returned): commit
	// only the records that reached a durable decision; transient failures
	// stay pending for the next round. A commit failure is fatal: the
	// records stay pending and the next round re-reads them (state file
	// keeps replay idempotent).
	if commitErr := r.commitResolved(ctx, collected, resolved); commitErr != nil {
		return replayed, commitErr
	}
	return replayed, nil
}

// resetDLQToCommittedOffset replaces the DLQ reader so this round's
// collectFailures starts at the broker's committed offset for the group.
// Within one group session kafka-go never re-delivers a fetched message
// (the fetch position advances past every FetchMessage, committed or not),
// so a transiently-failed record left uncommitted at round N cannot be
// re-read by round N+1 unless the reader is recreated: the fresh session
// joins the group and resumes at the committed offset. The commitResolved
// barrier guarantees the committed offset never crossed a pending record,
// so every pending record is re-delivered. The accepted reader is
// untouched: its StartOffset: FirstOffset re-scan semantics are governed by
// resetAcceptedToFirstOffset. A close failure aborts the round so nothing
// is collected, marked, or committed (records stay pending for the next
// round).
func (r *Replayer) resetDLQToCommittedOffset() error {
	if r.dlqReader != nil {
		if err := r.dlqReader.Close(); err != nil {
			return fmt.Errorf("reset dlq reader: close: %w", err)
		}
	}
	r.dlqReader = r.dlqNew()
	return nil
}

// resetAcceptedToFirstOffset replaces the accepted reader with a fresh one
// so this round's scan starts at the first retained offset. kafka-go honors
// StartOffset only at reader creation ("when it finds a partition without a
// committed offset") and SetOffset returns errNotAvailableWithGroup for
// group readers, so recreation is the only seek mechanism for the
// group-bound accepted reader (groupID + "-accepted"). The fresh reader
// joins with no committed offset (the accepted reader never commits) and
// begins at the first retained offset. The DLQ reader is untouched: its
// group membership and manual commits keep "failures consumed once"
// semantics. A close failure aborts the round so nothing is marked or
// committed (records stay pending for the next round).
func (r *Replayer) resetAcceptedToFirstOffset() error {
	if r.accepted != nil {
		if err := r.accepted.Close(); err != nil {
			return fmt.Errorf("reset accepted reader: close: %w", err)
		}
	}
	r.accepted = r.acceptedNew()
	return nil
}

// drainWindow returns the configured drain timeout with the default fallback.
func (r *Replayer) drainWindow() time.Duration {
	if r.drainTimeout <= 0 {
		return defaultDrainTimeout
	}
	return r.drainTimeout
}

// ReplayScheduler loops RunOnce with backoff until ctx is cancelled. A
// bounded scan pause lets the topic catch up between rounds.
func (r *Replayer) ReplayScheduler(ctx context.Context, interval time.Duration) error {
	for {
		if _, err := r.RunOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.logger.Printf("replay round failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// Close releases both readers.
func (r *Replayer) Close() error {
	dlqErr := r.dlqReader.Close()
	acceptedErr := r.accepted.Close()
	if dlqErr != nil {
		return dlqErr
	}
	return acceptedErr
}

// EventFromCanonical decodes a recovered accepted-topic message for
// API-based re-ingestion. Numbers stay exact so the derived digest is
// identical to the original producer's.
func EventFromCanonical(value []byte) (domain.Event, error) {
	var event domain.Event
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	if err := decoder.Decode(&event); err != nil {
		return domain.Event{}, fmt.Errorf("decode canonical event: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			err = errors.New("canonical event contains multiple JSON values")
		}
		return domain.Event{}, fmt.Errorf("decode canonical event: %w", err)
	}
	return event, nil
}
