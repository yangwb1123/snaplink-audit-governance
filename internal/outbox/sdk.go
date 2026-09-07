package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

type Execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// rowScanner is the minimal scan surface used by conflict classification.
// *sql.Row (returned by *sql.Tx.QueryRowContext) satisfies it; test doubles
// implement it without needing database/sql internals.
type rowScanner interface {
	Scan(dest ...any) error
}

// rowQueryer is the scripted-double surface: it returns rowScanner.
type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) rowScanner
}

// concreteRowQueryer is the exact method set of *sql.Tx: QueryRowContext
// returns the concrete *sql.Row. Go method-set matching requires identical
// return types, so a real transaction satisfies this interface (and only
// this one), never rowQueryer.
type concreteRowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// compile-time pin: a real database/sql transaction must keep satisfying the
// concrete classification surface (regression guard for the interface-shape
// defect where *sql.Tx could never satisfy rowQueryer).
var _ concreteRowQueryer = (*sql.Tx)(nil)

// queryRow resolves one classification read against either a real *sql.Tx
// (concrete *sql.Row) or a scripted test double (rowScanner). ok=false means
// the transaction surface cannot classify zero-row outcomes, which fails
// closed with ErrConflict instead of guessing.
func queryRow(ctx context.Context, tx Execer, query string, args ...any) (rowScanner, bool) {
	switch q := tx.(type) {
	case concreteRowQueryer:
		return q.QueryRowContext(ctx, query, args...), true
	case rowQueryer:
		return q.QueryRowContext(ctx, query, args...), true
	}
	return nil, false
}

// Insert writes an audit event into a caller-owned business transaction. The
// caller must commit its domain mutation and this insert together; on any
// error the caller must roll back (nothing is persisted partially).
//
// Return contract:
//   - nil: the event is durably recorded, or is an exact duplicate of an
//     already-recorded pending or delivered event. nil never means the event
//     reached the audit API; delivery is the relay's job.
//   - errors.Is(err, domain.ErrConflict): a deterministic conflict. The same
//     event_id was recorded with different canonical content, the tenant's
//     idempotency_key belongs to another event, the identical row was
//     already dead-lettered (status failed), or the zero-row outcome could
//     not be classified. Roll back and surface as 409; do NOT retry.
//   - any other error: durable acceptance is unknown (the statement or a
//     classification read failed). Roll back and retry with bounded backoff
//     plus jitter.
//
// Idempotent re-inserts never reset attempts/next_attempt_at of the existing
// row, and a dead-lettered row is never silently resurrected.
func Insert(ctx context.Context, tx Execer, event domain.Event) error {
	if event.TenantID == "" {
		return fmt.Errorf("%w: tenant_id is required for outbox", domain.ErrInvalid)
	}
	if err := event.ValidateBasic(); err != nil {
		return err
	}
	// FR-1/FR-3 (FM-1): reject before any SQL — matches the ingest cap
	// (service.go:975, itself defaulting to domain.MaxEventBytes at 123-124).
	// Measure the caller's original full event encoding before storage
	// canonicalization, so precision normalization cannot turn a one-byte
	// over-limit event into an accepted one.
	encoded, err := json.Marshal(event)
	if err != nil {
		return err
	}
	// The measure is the full event encoding (what the relay is asked to
	// deliver), so the outbox bound is at-least-as-strict as the payload-only
	// ingest cap (NFR-3); the exported constant keeps the two caps from
	// drifting.
	if len(encoded) > domain.MaxEventBytes {
		return fmt.Errorf("%w: payload exceeds %d bytes", domain.ErrInvalid, domain.MaxEventBytes)
	}
	// PostgreSQL timestamptz stores microseconds. Normalize the duplicated
	// column and the persisted payload to that same precision; otherwise an
	// event produced by time.Now (which commonly has nanoseconds) would be
	// quarantined as soon as the relay compared the two copies.
	storedEvent := event
	storedEvent.OccurredAt = normalizePostgresTimestamp(event.OccurredAt)
	encoded, err = json.Marshal(storedEvent)
	if err != nil {
		return err
	}
	if len(encoded) > domain.MaxEventBytes {
		return fmt.Errorf("%w: payload exceeds %d bytes", domain.ErrInvalid, domain.MaxEventBytes)
	}
	const query = `INSERT INTO audit_outbox (event_id, tenant_id, idempotency_key, payload, occurred_at, status, attempts, next_attempt_at)
VALUES ($1, $2, $3, $4, $5, 'pending', 0, now())
ON CONFLICT DO NOTHING`
	// The payload parameter is text, not []byte: pgx maps []byte to bytea,
	// which has no cast to jsonb and would fail on a real PostgreSQL.
	result, err := tx.ExecContext(ctx, query, storedEvent.EventID, storedEvent.TenantID, storedEvent.IdempotencyKey, string(encoded), storedEvent.OccurredAt)
	if err != nil {
		return fmt.Errorf("insert audit outbox: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("insert audit outbox: rows affected: %w", err)
	}
	if affected > 0 {
		return nil
	}
	return classifyZeroRows(ctx, tx, storedEvent, encoded)
}

// normalizePostgresTimestamp mirrors pgx's timestamptz encoding: finite
// timestamps are sent as whole microseconds, with sub-microsecond precision
// truncated. Keeping this normalization in the JSON payload as well as the
// SQL column makes an SDK-written row self-consistent when it is read back.
func normalizePostgresTimestamp(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

const (
	classifyEventIDQuery = `SELECT status, payload = $2::jsonb FROM audit_outbox WHERE event_id = $1`
	classifyPayloadQuery = `SELECT payload FROM audit_outbox WHERE event_id = $1`
	classifyIdemKeyQuery = `SELECT 1 FROM audit_outbox WHERE tenant_id = $1 AND idempotency_key = $2`
)

const unclassifiableMessage = "unable to classify outbox insert outcome"

// classifyZeroRows runs only when the insert absorbed a unique-index
// conflict (RowsAffected == 0). Both uniqueness constraints (event_id, and
// (tenant_id, idempotency_key) per tenant) absorb into this path; the
// classification reads run on the caller's transaction and only classify,
// never mutate. Every outcome is defined by the Insert contract: nil for a
// genuinely identical duplicate that is not dead-lettered, wrapped
// domain.ErrConflict for every conflicting or unclassifiable case, and a
// wrapped non-ErrConflict error when a classification read itself failed.
func classifyZeroRows(ctx context.Context, tx Execer, event domain.Event, encoded []byte) error {
	queryer, ok := queryRow(ctx, tx, classifyEventIDQuery, event.EventID, string(encoded))
	if !ok {
		return fmt.Errorf("%w: %s", domain.ErrConflict, unclassifiableMessage)
	}
	var status string
	var identical bool
	err := queryer.Scan(&status, &identical)
	if err == nil {
		if identical {
			return duplicateOutcome(status)
		}
		return classifyContentConflict(ctx, tx, event, status, encoded)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("insert audit outbox: classify event_id: %w", err)
	}
	// No event_id row: the conflict, if any, is the tenant-scoped
	// idempotency key.
	queryer, ok = queryRow(ctx, tx, classifyIdemKeyQuery, event.TenantID, event.IdempotencyKey)
	if !ok {
		return fmt.Errorf("%w: %s", domain.ErrConflict, unclassifiableMessage)
	}
	var exists int
	err = queryer.Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", domain.ErrConflict, unclassifiableMessage)
	}
	if err != nil {
		return fmt.Errorf("insert audit outbox: classify idempotency_key: %w", err)
	}
	return fmt.Errorf("%w: idempotency_key is already associated with another event", domain.ErrConflict)
}

// duplicateOutcome decides the idempotent-duplicate outcome from the stored
// row's delivery state: a dead-lettered row must surface as a conflict (the
// caller rolls back and uses a redelivery path) instead of returning nil for
// a row that will never be delivered.
func duplicateOutcome(status string) error {
	if status == StatusFailed {
		return fmt.Errorf("%w: event already dead-lettered", domain.ErrConflict)
	}
	return nil
}

// classifyContentConflict re-checks a jsonb-unequal event_id row against the
// canonical content digest. jsonb = is representation-coarse: json.Marshal
// preserves time zones (the same instant in two zones differs as text), so
// byte-identity implies jsonb equality but not the reverse. EventContentDigest
// is representation-canonical (UTC-normalized times, 'f'-format numbers), so
// agreeing digests mean the re-insert is the same logical event and only the
// encoding differs.
func classifyContentConflict(ctx context.Context, tx Execer, event domain.Event, status string, encoded []byte) error {
	queryer, ok := queryRow(ctx, tx, classifyPayloadQuery, event.EventID)
	if !ok {
		return fmt.Errorf("%w: %s", domain.ErrConflict, unclassifiableMessage)
	}
	var stored []byte
	err := queryer.Scan(&stored)
	if errors.Is(err, sql.ErrNoRows) {
		// Not reachable on the real store (append-only table, row visible to
		// the same transaction); fail closed rather than guess.
		return fmt.Errorf("%w: %s", domain.ErrConflict, unclassifiableMessage)
	}
	if err != nil {
		return fmt.Errorf("insert audit outbox: classify payload: %w", err)
	}
	storedEvent, err := decodeEvent(stored)
	if err != nil {
		return fmt.Errorf("insert audit outbox: classify payload: %w", err)
	}
	candidateDigest, err := domain.EventContentDigest(event)
	if err != nil {
		return fmt.Errorf("insert audit outbox: classify digest: %w", err)
	}
	storedDigest, err := domain.EventContentDigest(storedEvent)
	if err != nil {
		return fmt.Errorf("insert audit outbox: classify digest: %w", err)
	}
	if candidateDigest != storedDigest {
		return fmt.Errorf("%w: event_id already exists with different canonical content", domain.ErrConflict)
	}
	return duplicateOutcome(status)
}

// decodeEvent decodes a stored outbox payload preserving number literals, so
// the derived digest matches the original ingest digest (used by both the
// sdk read-back and PostgresStore.ListPending).
func decodeEvent(payload []byte) (domain.Event, error) {
	var event domain.Event
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&event); err != nil {
		return event, err
	}
	return event, nil
}

type Record struct {
	ID            int64        `json:"id"`
	Event         domain.Event `json:"event"`
	Status        string       `json:"status"`
	Attempts      int          `json:"attempts"`
	NextAttemptAt time.Time    `json:"next_attempt_at"`
	LastError     string       `json:"last_error,omitempty"`
	CreatedAt     time.Time    `json:"created_at"`
}
