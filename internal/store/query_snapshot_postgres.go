package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func (s *Store) savePostgresQuerySnapshot(ctx context.Context, q QuerySnapshot, raw []byte, now time.Time) error {
	tx, err := s.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Admission and expiry cleanup are tenant-scoped and atomic across replicas.
	// Hash collisions serialize unrelated tenants but can never mix their data.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 814312))`, q.TenantID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM audit_query_snapshot WHERE tenant_id = $1 AND expires_at <= $2`, q.TenantID, now); err != nil {
		return err
	}
	var active int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM audit_query_snapshot WHERE tenant_id = $1`, q.TenantID).Scan(&active); err != nil {
		return err
	}
	if active >= MaxActiveQuerySnapshots {
		return domain.ErrQuotaExceeded
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO audit_query_snapshot (tenant_id, snapshot_id, expires_at, metadata)
 VALUES ($1, $2, $3, $4::jsonb) ON CONFLICT DO NOTHING`, q.TenantID, q.ID, q.ExpiresAt, string(raw))
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return domain.ErrConflict
	}
	return tx.Commit()
}

func (s *Store) readPostgresQuerySnapshot(ctx context.Context, tenantID, id string, now time.Time) ([]byte, error) {
	var raw []byte
	err := s.pool.QueryRowContext(ctx, `SELECT metadata FROM audit_query_snapshot WHERE tenant_id = $1 AND snapshot_id = $2 AND expires_at > $3`, tenantID, id, now).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrQuerySnapshotUnavailable
	}
	if err != nil {
		return nil, err
	}
	if len(raw) > MaxQuerySnapshotBytes {
		return nil, ErrQuerySnapshotUnavailable
	}
	return raw, nil
}
