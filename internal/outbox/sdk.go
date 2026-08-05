package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

type Execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// Insert writes an audit event into a caller-owned business transaction. The
// caller must commit its domain mutation and this insert together.
func Insert(ctx context.Context, tx Execer, event domain.Event) error {
	if event.TenantID == "" {
		return fmt.Errorf("%w: tenant_id is required for outbox", domain.ErrInvalid)
	}
	if err := event.ValidateBasic(); err != nil {
		return err
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return err
	}
	const query = `INSERT INTO audit_outbox (event_id, tenant_id, idempotency_key, payload, occurred_at, status, attempts, next_attempt_at)
VALUES ($1, $2, $3, $4, $5, 'pending', 0, now())
ON CONFLICT DO NOTHING`
	if _, err := tx.ExecContext(ctx, query, event.EventID, event.TenantID, event.IdempotencyKey, encoded, event.OccurredAt.UTC()); err != nil {
		return fmt.Errorf("insert audit outbox: %w", err)
	}
	return nil
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
