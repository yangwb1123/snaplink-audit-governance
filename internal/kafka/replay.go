package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/fsutil"
	"github.com/snaplink/audit-governance/internal/outbox"
)

// RepublishFunc redelivers one recovered event. Implementations write the
// original accepted-topic message back (Producer.Republish) or ingest it
// through the audit API (outbox.HTTPDeliverer).
type RepublishFunc func(ctx context.Context, key, value []byte) error

// ReplayState is the persisted set of dead-letter event IDs that have
// already been replayed, so a re-scan of the accepted topic never replays an
// event twice. The file is rewritten atomically on each append.
type ReplayState struct {
	Replayed map[string]bool `json:"replayed"`
	path     string
	// syncFile/syncDir are the durability hooks used by Mark (defaults:
	// fsutil.SyncFile / fsutil.SyncDir). They are unexported and never
	// marshaled, so the persisted bytes stay byte-identical; same-package
	// tests replace them to record the ordered sync set or fault-inject a
	// step (mirrors archive.FileStore.syncDir / store.fileBackend.syncDir).
	syncFile func(path string) error
	syncDir  func(path string) error
}

// LoadReplayState reads the state file; a missing or empty file starts an
// empty set.
func LoadReplayState(path string) (*ReplayState, error) {
	state := &ReplayState{Replayed: map[string]bool{}, path: path}
	if path == "" {
		return state, nil
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return state, nil
		}
		return nil, fmt.Errorf("load replay state: %w", err)
	}
	if len(bytes.TrimSpace(encoded)) == 0 {
		return state, nil
	}
	if err := json.Unmarshal(encoded, state); err != nil {
		return nil, fmt.Errorf("decode replay state: %w", err)
	}
	if state.Replayed == nil {
		state.Replayed = map[string]bool{}
	}
	return state, nil
}

// Mark appends one event ID to the replayed set and persists the file
// atomically (temp file + rename). A failed persist is returned so the
// caller can keep the event pending instead of losing the record.
func (s *ReplayState) Mark(eventID string) error {
	s.Replayed[eventID] = true
	if s.path == "" {
		return nil
	}
	encoded, err := json.Marshal(s)
	if err != nil {
		return err
	}
	temp := s.path + ".tmp"
	if err := os.WriteFile(temp, encoded, 0o600); err != nil {
		_ = os.Remove(temp)
		return fmt.Errorf("write replay state: %w", err)
	}
	// fsync the temp contents BEFORE the rename: the replay-marks file is
	// the idempotency ledger for DLQ recovery, so the rename must never
	// expose a temp file whose bytes were not flushed (a crash could
	// otherwise leave a zeroed or torn target, which makes LoadReplayState
	// fail and cmd/audit-kafka-dlq-replay exit at boot). Any failure before
	// the rename removes the temp best-effort: the previously persisted
	// state stays authoritative, decodable, and free of stale .tmp files.
	syncFile := s.syncFile
	if syncFile == nil {
		syncFile = fsutil.SyncFile
	}
	if err := syncFile(temp); err != nil {
		_ = os.Remove(temp)
		return fmt.Errorf("sync replay state: %w", err)
	}
	if err := os.Rename(temp, s.path); err != nil {
		_ = os.Remove(temp)
		return fmt.Errorf("rename replay state: %w", err)
	}
	// The renamed directory entry must be durable before Mark reports
	// success (the same crash window the archive and store packages closed):
	// a power failure after rename can otherwise revert the entry, silently
	// un-marking committed replay decisions and re-triggering re-replays on
	// the next accepted-topic scan. The state file is flat in one
	// pre-existing directory, so the parent-dir sync is exactly one
	// directory. A dir-sync failure returns an error but deliberately KEEPS
	// the new target content: the in-memory map already holds the mark and
	// both old and new content decode, so the next Mark retries the
	// durability step (removing the target would lose committed marks).
	syncDir := s.syncDir
	if syncDir == nil {
		syncDir = fsutil.SyncDir
	}
	if err := syncDir(filepath.Dir(s.path)); err != nil {
		return fmt.Errorf("sync replay state directory: %w", err)
	}
	return nil
}

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
	dlqReader     messageReader
	dlqNew        func() messageReader // per-round DLQ reader recreation (F1)
	accepted      messageReader
	acceptedNew   func() messageReader // per-round accepted reader recreation (REQ-1)
	republish     RepublishFunc
	state         *ReplayState
	logger        *log.Logger
	drainTimeout  time.Duration
	dlqRecords    atomic.Uint64
	acceptedSeen  atomic.Uint64
	replayed      atomic.Uint64
	republishFail atomic.Uint64
	pending       atomic.Uint64
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
	return &Replayer{dlqReader: dlqReader, dlqNew: func() messageReader { return kafka.NewReader(dlqConfig) }, accepted: acceptedReader, acceptedNew: func() messageReader { return kafka.NewReader(acceptedConfig) }, republish: republish, state: state, logger: logger, drainTimeout: defaultDrainTimeout}
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

// Metrics returns (dlqRecords, acceptedScanned, replayed, republishFailures,
// pending) for the /metrics endpoint.
func (r *Replayer) Metrics() (dlqRecords, acceptedScanned, replayed, republishFailures, pending uint64) {
	return r.dlqRecords.Load(), r.acceptedSeen.Load(), r.replayed.Load(), r.republishFail.Load(), r.pending.Load()
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
	collected, err := r.collectFailures(ctx)
	if err != nil && !isDrained(ctx, err) {
		return 0, err
	}
	wanted := wantedEvents(collected)
	if len(wanted) == 0 {
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
	r.pending.Store(uint64(len(wanted)))
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

// dlqRecord is one collected DLQ failure with its parsed event ID.
type dlqRecord struct {
	message kafka.Message
	eventID string
	wanted  bool
}

// wantedEvents returns the set of collected event IDs not yet marked
// replayed.
func wantedEvents(collected []dlqRecord) map[string]bool {
	wanted := map[string]bool{}
	for _, record := range collected {
		if record.wanted {
			wanted[record.eventID] = true
		}
	}
	return wanted
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
func (r *Replayer) commitResolved(ctx context.Context, collected []dlqRecord, resolved map[string]bool) error {
	type partitionKey struct {
		topic     string
		partition int
	}
	// firstPending[partition] = lowest offset of a record that must stay
	// pending; no commit may cross it.
	firstPending := map[partitionKey]int64{}
	for _, record := range collected {
		if record.wanted && !resolved[record.eventID] {
			key := partitionKey{topic: record.message.Topic, partition: record.message.Partition}
			if offset, ok := firstPending[key]; !ok || record.message.Offset < offset {
				firstPending[key] = record.message.Offset
			}
		}
	}
	for _, record := range collected {
		if record.wanted && !resolved[record.eventID] {
			continue // transient failure: keep the record pending
		}
		key := partitionKey{topic: record.message.Topic, partition: record.message.Partition}
		if pending, ok := firstPending[key]; ok && record.message.Offset >= pending {
			continue // resolved but at/above the barrier: committing would leapfrog the pending record
		}
		if err := r.dlqReader.CommitMessages(ctx, record.message); err != nil {
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
	drainCtx, cancel := context.WithTimeout(ctx, r.drainWindow())
	defer cancel()
	var collected []dlqRecord
	for {
		message, err := r.dlqReader.FetchMessage(drainCtx)
		if err != nil {
			return collected, err
		}
		record := dlqRecord{message: message, eventID: string(message.Key)}
		var failure Failure
		decoder := json.NewDecoder(bytes.NewReader(message.Value))
		decoder.UseNumber()
		decodeErr := decoder.Decode(&failure)
		if decodeErr == nil {
			r.dlqRecords.Add(1)
			record.eventID = failure.EventID
			record.wanted = !r.state.Replayed[failure.EventID]
		} else {
			r.logger.Printf("dlq record unparsable topic=%s partition=%d offset=%d error=%v", r.dlqReader.Config().Topic, message.Partition, message.Offset, decodeErr)
		}
		collected = append(collected, record)
	}
}

// scanAccepted walks the accepted topic from its first retained offset and
// re-publishes every message whose payload event_id (authoritative) or key
// (fallback for unparsable values) matches a wanted event ID. Replay is
// idempotent: events already in the state file are skipped. A key-matched
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
// leaves everything pending. Returns the number of events replayed and the
// set of event IDs that reached a durable decision this round (replayed,
// permanently rejected, unparsable, or converged-as-unresolvable — NOT
// transiently failed).
func (r *Replayer) scanAccepted(ctx context.Context, wanted map[string]bool) (int, map[string]bool, error) {
	window := r.drainWindow()
	scanCtx, cancel := context.WithTimeout(ctx, maxScanWindows*window)
	defer cancel()
	replayed := 0
	resolved := map[string]bool{}
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
		if payloadID != "" && wanted[payloadID] && !r.state.Replayed[payloadID] {
			eventID = payloadID // payload event_id is authoritative
		} else if keyID := string(message.Key); wanted[keyID] && !r.state.Replayed[keyID] {
			eventID = keyID // fallback: unparsable values whose only signal is the key
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
			resolved[eventID] = true
			r.replayed.Add(1)
			replayed++
			continue
		}
		if err := r.republish(ctx, message.Key, message.Value); err != nil {
			var deliveryErr *outbox.DeliveryError
			if errors.As(err, &deliveryErr) && deliveryErr.Permanent {
				// API-rejected events will never succeed on retry: mark them
				// replayed so the round converges; the operator investigates
				// via the DLQ record.
				r.logger.Printf("permanent republish failure event_id=%s error=%v; marking replayed", eventID, err)
				if markErr := r.state.Mark(eventID); markErr != nil {
					return replayed, resolved, markErr
				}
				resolved[eventID] = true
				r.republishFail.Add(1)
				r.replayed.Add(1)
				replayed++
				continue
			}
			r.republishFail.Add(1)
			r.logger.Printf("republish failed event_id=%s error=%v; will retry next round", eventID, err)
			continue // found => stays pending, never marked unresolvable
		}
		if err := r.state.Mark(eventID); err != nil {
			return replayed, resolved, err
		}
		resolved[eventID] = true
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
		for eventID := range wanted {
			if resolved[eventID] || found[eventID] {
				continue
			}
			r.logger.Printf("unresolvable event_id=%s reason=original-not-found-in-accepted-topic round=%s", eventID, time.Now().Format(time.RFC3339))
			if err := r.state.Mark(eventID); err != nil {
				return replayed, resolved, err
			}
			resolved[eventID] = true
			r.replayed.Add(1)
			replayed++
		}
	}
	return replayed, resolved, scanErr
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
	return event, nil
}
