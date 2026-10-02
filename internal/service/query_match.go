package service

// Query evaluation: reading a tenant's events and deciding which of them a
// filter selects, including the cross-format payload-digest comparison and
// the Legal Hold overlap test that blocks an export. Kept apart from the
// ingest path so "what matches" and "what is admitted" stay separate.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

func (s *Service) eventsFor(ctx context.Context, tenantID string, predicate func(domain.Event) bool) ([]domain.Event, error) {
	var events []domain.Event
	err := s.Store.Read(ctx, func(data *store.Snapshot) error {
		var resolveErr error
		events, resolveErr = s.eventsFromSnapshot(ctx, data, tenantID, predicate)
		return resolveErr
	})
	return events, err
}

func (s *Service) matches(event domain.Event, query domain.Query, schemas map[string]domain.EventSchema) bool {
	if event.OccurredAt.Before(query.From) || !event.OccurredAt.Before(query.To) {
		return false
	}
	if query.EventType != "" && event.EventType != query.EventType {
		return false
	}
	if query.SourceSystem != "" && event.SourceSystem != query.SourceSystem {
		return false
	}
	if query.ActorID != "" && event.Actor.ID != query.ActorID {
		return false
	}
	if query.TargetID != "" && !hasTarget(event, query.TargetID) {
		return false
	}
	if query.AggregateType != "" && event.AggregateType != query.AggregateType {
		return false
	}
	if query.AggregateID != "" && event.AggregateID != query.AggregateID {
		return false
	}
	if query.OperationID != "" && event.OperationID != query.OperationID {
		return false
	}
	if query.CausationID != "" && event.CausationID != query.CausationID {
		return false
	}
	if query.CorrelationID != "" && event.CorrelationID != query.CorrelationID {
		return false
	}
	if query.TraceID != "" && event.TraceID != query.TraceID {
		return false
	}
	if query.Outcome != "" && event.Outcome != query.Outcome {
		return false
	}
	if query.PayloadField != "" {
		if !s.digestMatches(event, query, schemas) {
			return false
		}
	}
	if query.StreamID != "" && event.StreamID != query.StreamID {
		return false
	}
	return true
}

// digestMatches reports whether the stored event satisfies the query's
// (payload_field, payload_digest) filter. Both digest formats are accepted:
//   - same format (v1 unbound or v2 bound on both sides) → direct equality;
//   - mixed formats → the query-format digest is re-derived from the stored
//     plaintext and compared. The fallback fails closed when the plaintext
//     is unavailable (encrypted field, missing value, unknown schema
//     version), so a cross-format query can never match through a
//     ciphertext or an absent value.
//
// The fallback keeps old v1 clients able to search events ingested under
// the new bound format and new v2 clients able to search legacy events.
func (s *Service) digestMatches(event domain.Event, query domain.Query, schemas map[string]domain.EventSchema) bool {
	stored, ok := event.Payload[query.PayloadField+security.SearchDigestSuffix].(string)
	if !ok {
		return false
	}
	if security.IsBoundSearchDigest(stored) == security.IsBoundSearchDigest(query.PayloadDigest) {
		return stored == query.PayloadDigest
	}
	schema, ok := schemas[store.SchemaKey(event.TenantID, event.SchemaID, event.SchemaVersion)]
	if !ok || containsString(schema.EncryptedFields, query.PayloadField) {
		return false
	}
	value, ok := event.Payload[query.PayloadField]
	if !ok {
		return false
	}
	var derived string
	var err error
	if security.IsBoundSearchDigest(query.PayloadDigest) {
		derived, err = security.SearchDigestBound(value, s.Config.EncryptionKey, event.TenantID, query.PayloadField)
	} else {
		derived, err = security.SearchDigest(value, s.Config.EncryptionKey)
	}
	if err != nil {
		return false
	}
	return derived == query.PayloadDigest
}

func (s *Service) holdMatchesEvent(hold domain.LegalHold, event domain.Event, schemas map[string]domain.EventSchema) bool {
	filter := hold.Filter
	if filter.From.IsZero() && filter.To.IsZero() {
		return true
	}
	if filter.From.IsZero() || filter.To.IsZero() {
		return false
	}
	return s.matches(event, filter, schemas)
}

// holdCanSelectAnySelected is a conservative pre-filter the export gate runs
// before it materializes any event. It answers only "must this hold be
// evaluated?" — never "is this export blocked?". A true result says nothing
// about the outcome; the gate still decides with holdMatchesEvent.
//
// Soundness: eventsFromSnapshot materializes the set E of events with
// Q.From <= e.OccurredAt < Q.To that also satisfy the other query predicates,
// and for a fully bound filter F, holdMatchesEvent requires
// F.From <= e.OccurredAt < F.To.
// So h can match an element of E only if [F.From, F.To) intersects
// [Q.From, Q.To). An empty intersection rejects every element of E, so dropping
// h removes only iterations guaranteed to fail; every surviving hold is
// evaluated by the same predicate over the same event set in the same ID order,
// so the first match — and therefore the reported hold ID, the 403 body, the
// export.blocked fact and job.Error — are unchanged.
//
// The half-open convention is matches' own (occurred_at == To is rejected), so
// touching windows F.To == Q.From and F.From == Q.To are disjoint. The rule is
// derived from that convention; no tolerance, slack or threshold is introduced.
// A degenerate query window never permits a skip: with QueryEvents' own bound
// validation such a job cannot be created, and a hand-edited or legacy job row
// must not be able to use it to drop a hold.
func holdCanSelectAnySelected(hold domain.LegalHold, query domain.Query) bool {
	filter := hold.Filter
	if filter.From.IsZero() && filter.To.IsZero() {
		return true // unbound: holdMatchesEvent matches every event (fail-closed)
	}
	if filter.From.IsZero() || filter.To.IsZero() {
		return false // one-sided: holdMatchesEvent returns false for every event
	}
	if query.From.IsZero() || query.To.IsZero() || !query.From.Before(query.To) {
		return true // degenerate export window: never skip
	}
	if !filter.From.Before(filter.To) {
		// Empty hold window (inverted or zero-span). matches would reject
		// every event, so keeping it is conservative rather than exact. The
		// shape is only reachable from a hand-edited or legacy snapshot row —
		// CreateLegalHold rejects it — so keeping it costs one materialization
		// in practice never observed, while dropping it would let row-level
		// data that no production writer can produce decide a hold's fate.
		return true
	}
	return filter.From.Before(query.To) && query.From.Before(filter.To)
}

func appendUnique(values []string, wanted string) []string {
	for _, value := range values {
		if value == wanted {
			return values
		}
	}
	return append(values, wanted)
}

// compareEvents reports whether a precedes b in the chronological total
// order (OccurredAt, Sequence, EventID). event_id is unique per tenant
// (store.EventKey dedupe), so the tuple never ties: the order is strict.
func compareEvents(a, b domain.Event) bool {
	if a.OccurredAt.Equal(b.OccurredAt) {
		if a.Sequence == b.Sequence {
			return a.EventID < b.EventID
		}
		return a.Sequence < b.Sequence
	}
	return a.OccurredAt.Before(b.OccurredAt)
}

func sortEvents(events []domain.Event) {
	sort.Slice(events, func(i, j int) bool { return compareEvents(events[i], events[j]) })
}

func replay(events []domain.Event, tenantID, operationID, aggregateID string) domain.ReplayResult {
	state := map[string]any{}
	var last int64
	for _, event := range events {
		for field, change := range event.ChangedFields {
			if change.After == nil {
				delete(state, field)
			} else {
				state[field] = change.After
			}
		}
		last = event.Sequence
	}
	return domain.ReplayResult{TenantID: tenantID, OperationID: operationID, AggregateID: aggregateID, EventCount: len(events), State: state, LastSequence: last}
}

func hasField(event domain.Event, field string) bool {
	switch field {
	case "event_id":
		return event.EventID != ""
	case "source_system":
		return event.SourceSystem != ""
	case "event_type":
		return event.EventType != ""
	case "operation_id":
		return event.OperationID != ""
	case "aggregate_id":
		return event.AggregateID != ""
	case "actor.id":
		return event.Actor.ID != ""
	default:
		_, ok := event.Payload[field]
		return ok
	}
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func hasTarget(event domain.Event, targetID string) bool {
	for _, target := range event.Targets {
		if target.ID == targetID {
			return true
		}
	}
	return false
}

func rejectSensitive(value any) error {
	var scan func(any, string) error
	scan = func(current any, path string) error {
		switch v := current.(type) {
		case map[string]any:
			for key, child := range v {
				lower := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
				for _, forbidden := range []string{"password", "token", "private_key", "device_credential", "card_number", "secret"} {
					if strings.Contains(lower, forbidden) {
						return fmt.Errorf("%w: sensitive field %s is forbidden", domain.ErrInvalid, path+key)
					}
				}
				if err := scan(child, path+key+"."); err != nil {
					return err
				}
			}
		case []any:
			for i, child := range v {
				if err := scan(child, fmt.Sprintf("%s%d.", path, i)); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return scan(value, "")
}

func merkleRoot(hashes []string) string {
	if len(hashes) == 0 {
		return domain.HashBytes(nil)
	}
	level := append([]string(nil), hashes...)
	for len(level) > 1 {
		next := make([]string, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			left, right := level[i], level[i]
			if i+1 < len(level) {
				right = level[i+1]
			}
			next = append(next, domain.HashBytes([]byte(left+right)))
		}
		level = next
	}
	return level[0]
}

func newID(prefix string) string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(buf)
}
