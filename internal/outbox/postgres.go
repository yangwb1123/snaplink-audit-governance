package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// PostgresStore maps the audit_outbox table to Relay.Store. The optimistic
// condition status = 'pending' prevents concurrent Relay instances from
// completing the same record twice; duplicate deliveries are absorbed by
// the audit API event_id idempotency.
type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

const listPendingQuery = `
SELECT id, event_id, tenant_id, idempotency_key, payload, occurred_at,
       status, attempts, next_attempt_at, last_error, created_at
FROM audit_outbox
WHERE status = 'pending' AND next_attempt_at <= now()
ORDER BY next_attempt_at, id
LIMIT $1`

// ListPending returns due pending records ordered by retry time, plus a
// report of scanned rows whose payloads failed to decode into domain.Event.
// Decode failures never abort the batch: the corrupt row is excluded from
// records and reported (status stays pending; quarantine is the relay's
// job, REQ-1). Query-level and row-scan errors still abort.
func (p *PostgresStore) ListPending(ctx context.Context, limit int) ([]Record, []CorruptRecord, error) {
	rows, err := p.db.QueryContext(ctx, listPendingQuery, limit)
	if err != nil {
		return nil, nil, fmt.Errorf("list pending outbox: %w", err)
	}
	defer rows.Close()
	var records []Record
	var corrupt []CorruptRecord
	for rows.Next() {
		var record Record
		var payload []byte
		var lastError sql.NullString
		if err := rows.Scan(&record.ID, &record.Event.EventID, &record.Event.TenantID,
			&record.Event.IdempotencyKey, &payload, &record.Event.OccurredAt,
			&record.Status, &record.Attempts, &record.NextAttemptAt, &lastError,
			&record.CreatedAt); err != nil {
			return nil, nil, fmt.Errorf("scan outbox record: %w", err)
		}
		record.LastError = lastError.String
		// decodeEvent (sdk.go) preserves number literals via UseNumber so
		// the re-ingested digest matches the original ingest digest; the
		// decode path is shared with the sdk read-back, never duplicated.
		event, err := decodeEvent(payload)
		if err != nil {
			corrupt = append(corrupt, CorruptRecord{
				ID:       record.ID,
				Attempts: record.Attempts,
				Err:      fmt.Errorf("decode outbox payload id=%d: %w", record.ID, err),
			})
			continue
		}
		record.Event = event
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return records, corrupt, nil
}

const updateQuery = `
UPDATE audit_outbox
SET status = $1, attempts = $2, next_attempt_at = $3, last_error = $4,
    delivered_at = $5, delivered_event_id = $6, api_status = $7
WHERE id = $8 AND status = 'pending'`

// Update applies one delivery outcome to a record still pending.
func (p *PostgresStore) Update(ctx context.Context, id int64, patch Update) (bool, error) {
	result, err := p.db.ExecContext(ctx, updateQuery,
		patch.Status, patch.Attempts, patch.NextAttemptAt.UTC(),
		nullableString(patch.LastError), nullableTime(patch.DeliveredAt),
		nullableString(patch.DeliveredEventID), nullableString(patch.APIStatus), id)
	if err != nil {
		return false, fmt.Errorf("update outbox record %d: %w", id, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected == 1, nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

var _ Store = (*PostgresStore)(nil)
