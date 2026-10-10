package projection

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// SelectQueryReferences returns bounded, logically deduplicated candidates.
// It never returns payload or decrypts. The API rechecks these coordinates and
// fetches evidence from Ledger/WORM; projection presence is not audit truth.
func (s *Store) SelectQueryReferences(ctx context.Context, tenantID string, q domain.Query) ([]store.EventReference, error) {
	statement, args, err := projectionQuery(tenantID, q)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("projection query unavailable: %w", err)
	}
	defer rows.Close()
	result := []store.EventReference{}
	scanned := 0
	for rows.Next() {
		var reference store.EventReference
		var variants uint64
		var occurrence string
		if err := rows.Scan(&reference.EventID, &reference.StreamID, &reference.Sequence, &reference.Hash, &variants, &occurrence); err != nil {
			return nil, err
		}
		if variants != 1 {
			return nil, ErrProjectionConflict
		}
		reference.OccurredAt, err = time.Parse(time.RFC3339Nano, occurrence)
		if err != nil {
			return nil, ErrProjectionConflict
		}
		scanned++
		if scanned > store.MaxQuerySnapshotEvents {
			return nil, fmt.Errorf("%w: query candidate budget exceeded; narrow filters", domain.ErrQuotaExceeded)
		}
		if !reference.OccurredAt.Before(q.From) && reference.OccurredAt.Before(q.To) {
			result = append(result, reference)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func projectionQuery(tenantID string, q domain.Query) (string, []any, error) {
	if err := projectionScope(tenantID, ""); err != nil {
		return "", nil, err
	}
	if q.From.IsZero() || q.To.IsZero() || !q.From.Before(q.To) || q.Cursor != "" {
		return "", nil, domain.ErrInvalid
	}
	// Search digests intentionally do not exist in this table. Never put them
	// in ClickHouse SQL, parameters or diagnostics: use the ledger read adapter.
	if q.PayloadField != "" || q.PayloadDigest != "" {
		return "", nil, fmt.Errorf("%w: digest filter requires ledger query", domain.ErrInvalid)
	}
	// Table occurrence has millisecond precision, while the envelope retains
	// nanoseconds. Widen the physical prefilter; exact envelope times are tested
	// in Go before membership, so boundary rows are neither lost nor included.
	// Bind RFC3339 strings, not time.Time (native parameter formatting can
	// otherwise truncate a DateTime64 boundary to whole seconds).
	from := q.From.UTC().Truncate(time.Millisecond)
	to := q.To.UTC().Truncate(time.Millisecond)
	if to.Before(q.To) {
		to = to.Add(time.Millisecond)
	}
	// Older native time.Time inserts could saturate the physical DateTime
	// parameter at 2106. The protected envelope retains the exact occurrence;
	// never silently omit those legacy rows by using the physical column.
	predicates := []string{}
	args := []any{tenantID}
	timestamp := "parseDateTime64BestEffort(JSONExtractString(chosen_envelope, 'occurred_at'), 3, 'UTC')"
	if q.From.After(domain.MaxOccurredAt) || !q.To.After(domain.MinOccurredAt) {
		predicates = append(predicates, "0")
	} else {
		if !q.From.Before(domain.MinOccurredAt) {
			predicates = append(predicates, timestamp+" >= parseDateTime64BestEffort(?, 3, 'UTC')")
			args = append(args, from.Format(time.RFC3339Nano))
		}
		// Never pass 2300 (an exclusive upper bound just above our horizon)
		// to a parser that saturates at 2299 and silently truncates the day.
		if !q.To.After(domain.MaxOccurredAt) {
			predicates = append(predicates, timestamp+" < parseDateTime64BestEffort(?, 3, 'UTC')")
			args = append(args, to.Format(time.RFC3339Nano))
		}
	}
	if len(predicates) == 0 {
		predicates = append(predicates, "1")
	}
	for _, filter := range []struct{ key, value string }{
		{"event_type", q.EventType}, {"source_system", q.SourceSystem}, {"operation_id", q.OperationID}, {"causation_id", q.CausationID}, {"correlation_id", q.CorrelationID}, {"aggregate_type", q.AggregateType}, {"aggregate_id", q.AggregateID}, {"trace_id", q.TraceID}, {"outcome", q.Outcome},
	} {
		if filter.value != "" {
			predicates = append(predicates, "JSONExtractString(chosen_envelope, '"+filter.key+"') = ?")
			args = append(args, filter.value)
		}
	}
	if q.StreamID != "" {
		predicates = append(predicates, "chosen_stream_id = ?")
		args = append(args, q.StreamID)
	}
	if q.ActorID != "" {
		predicates = append(predicates, "JSONExtractString(chosen_envelope, 'actor', 'id') = ?")
		args = append(args, q.ActorID)
	}
	if q.TargetID != "" {
		predicates = append(predicates, "arrayExists(t -> JSONExtractString(t, 'id') = ?, JSONExtractArrayRaw(chosen_envelope, 'targets'))")
		args = append(args, q.TargetID)
	}
	// Conflicting variants cannot hide behind a predicate or background merge:
	// detect every ambiguous tenant identity before permitting matching rows.
	sql := `SELECT event_id, chosen_stream_id, chosen_sequence, chosen_hash, variants, JSONExtractString(chosen_envelope, 'occurred_at') FROM
(SELECT event_id, any(stream_id) AS chosen_stream_id, any(sequence) AS chosen_sequence,
any(event_hash) AS chosen_hash, any(occurred_at) AS chosen_occurred_at, any(envelope) AS chosen_envelope,
uniqExact(tuple(stream_id, sequence, event_hash, envelope_digest)) AS variants
FROM audit_projection_events WHERE tenant_id = ? GROUP BY event_id)
WHERE variants != 1 OR (` + strings.Join(predicates, " AND ") + `)
ORDER BY variants DESC, parseDateTime64BestEffort(JSONExtractString(chosen_envelope, 'occurred_at'), 3, 'UTC'), chosen_sequence, event_id
LIMIT 10001`
	return sql, args, nil
}

// CheckQueryStorage is a read-only dependency probe for API startup/readiness.
// Schema creation and fsync settings remain the projector/operator's job.
func (s *Store) CheckQueryStorage(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var engine, key string
	if err := s.db.QueryRowContext(ctx, `SELECT engine, sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'audit_projection_events'`).Scan(&engine, &key); err != nil {
		return err
	}
	if engine != "ReplacingMergeTree" || strings.ReplaceAll(key, " ", "") != "tenant_id,stream_id,sequence,event_id,event_hash,envelope_digest" {
		return fmt.Errorf("incompatible query projection schema")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT tenant_id, stream_id, sequence, event_id, event_hash, envelope_digest, envelope, occurred_at FROM audit_projection_events LIMIT 0`)
	if err != nil {
		return err
	}
	return rows.Close()
}
