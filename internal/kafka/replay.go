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
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/snaplink/audit-governance/internal/domain"
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
		return fmt.Errorf("write replay state: %w", err)
	}
	if err := os.Rename(temp, s.path); err != nil {
		return fmt.Errorf("rename replay state: %w", err)
	}
	return nil
}

// defaultDrainTimeout bounds one RunOnce round: Kafka readers block on
// FetchMessage, so "drain the topic" is expressed as "fetch until no
// message arrives within this window".
const defaultDrainTimeout = 5 * time.Second

// Replayer redelivers dead-lettered events. DLQ records carry only failure
// metadata (event_id, error_code, error_message), so the original event is
// recovered from the accepted topic by key and re-published (or re-ingested
// through the API). The accepted topic is always scanned from the first
// retained offset: replayed IDs in the state file make the re-scan
// idempotent, and committing the scan position would make new DLQ records
// miss older original messages.
type Replayer struct {
	dlqReader     messageReader
	accepted      messageReader
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
// commits its offsets (failures are consumed once); the accepted reader
// never commits and always starts at the first retained offset.
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
	dlqReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          dlqTopic,
		GroupID:        groupID + "-dlq",
		MinBytes:       1,
		MaxBytes:       1 << 20,
		CommitInterval: 0, // manual commits only
		StartOffset:    kafka.FirstOffset,
	})
	acceptedReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          acceptedTopic,
		GroupID:        groupID + "-accepted",
		MinBytes:       1,
		MaxBytes:       1 << 20,
		CommitInterval: 0, // never commit: every round re-scans from the start
		StartOffset:    kafka.FirstOffset,
	})
	return &Replayer{dlqReader: dlqReader, accepted: acceptedReader, republish: republish, state: state, logger: logger, drainTimeout: defaultDrainTimeout}
}

// newReplayerWithReaders is the test seam: the same messageReader contract
// as Consumer, so replay behavior is exercised without a broker.
func newReplayerWithReaders(dlq, accepted messageReader, state *ReplayState, republish RepublishFunc) *Replayer {
	return &Replayer{dlqReader: dlq, accepted: accepted, republish: republish, state: state, logger: log.New(io.Discard, "", 0), drainTimeout: defaultDrainTimeout}
}

// Metrics returns (dlqRecords, acceptedScanned, replayed, republishFailures,
// pending) for the /metrics endpoint.
func (r *Replayer) Metrics() (dlqRecords, acceptedScanned, replayed, republishFailures, pending uint64) {
	return r.dlqRecords.Load(), r.acceptedSeen.Load(), r.replayed.Load(), r.republishFail.Load(), r.pending.Load()
}

// RunOnce performs one replay round: drain the currently available DLQ
// failures, recover the matching original events from the accepted topic and
// re-publish them. Draining is time-bounded (drainTimeout of quiet topic
// time); returns the number of events replayed this round.
//
// DLQ offsets are committed per record and only for events that reached a
// durable decision (replayed, permanently rejected, unparsable, or already
// replayed in an earlier round). A transient republish failure (or a crash
// mid-round) leaves that record uncommitted, so the next round re-reads it
// and retries. Replayed IDs in the state file keep re-reads idempotent.
func (r *Replayer) RunOnce(ctx context.Context) (int, error) {
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
// event reached a durable decision: already replayed at collect time
// (record.wanted false, including unparsable records), or resolved this
// round (present in resolved). Records with pending events stay uncommitted.
func (r *Replayer) commitResolved(ctx context.Context, collected []dlqRecord, resolved map[string]bool) error {
	for _, record := range collected {
		if record.wanted && !resolved[record.eventID] {
			continue // transient failure: keep the record pending
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
// re-publishes every message whose key matches a wanted event ID. Replay is
// idempotent: events already in the state file are skipped. An unparsable
// accepted message is skipped with a log line, mirroring the consumer. The
// scan ends when the drain window expires. Returns the number of events
// replayed and the set of event IDs that reached a durable decision this
// round (replayed, permanently rejected, or unparsable — NOT transiently
// failed).
func (r *Replayer) scanAccepted(ctx context.Context, wanted map[string]bool) (int, map[string]bool, error) {
	drainCtx, cancel := context.WithTimeout(ctx, r.drainWindow())
	defer cancel()
	replayed := 0
	resolved := map[string]bool{}
	for {
		message, err := r.accepted.FetchMessage(drainCtx)
		if err != nil {
			return replayed, resolved, err
		}
		r.acceptedSeen.Add(1)
		eventID := string(message.Key)
		if !wanted[eventID] || r.state.Replayed[eventID] {
			continue
		}
		// A message whose key is wanted but whose value is not a canonical
		// event cannot be replayed: log and mark it replayed to avoid an
		// infinite loop across rounds.
		if !looksLikeCanonicalEvent(message.Value) {
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
			continue
		}
		if err := r.state.Mark(eventID); err != nil {
			return replayed, resolved, err
		}
		resolved[eventID] = true
		r.replayed.Add(1)
		replayed++
		r.logger.Printf("replayed event_id=%s", eventID)
	}
}

// looksLikeCanonicalEvent cheaply verifies a recovered accepted message is a
// JSON object carrying an event_id before it is re-published.
func looksLikeCanonicalEvent(value []byte) bool {
	var probe struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(value, &probe); err != nil {
		return false
	}
	return probe.EventID != ""
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
