package store

// The explicit v1 to v2 cutover for the PostgreSQL backend. The catalog
// contract is the evidence that the expand migration produced a complete,
// compatible target — relation names alone are not, so the query checks
// presence, kind, columns and the absence of a legacy table. MigratePostgresSnapshot
// then performs the conversion under a transaction-scoped database lock (and
// the snapshot row lock when the row exists) so two fresh migrators cannot
// both convert. Production startup refuses to reinterpret a legacy row when
// the 006 tables exist but the cutover has not run.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// postgresHotColdCatalogQuery returns one of absent, incomplete, incompatible,
// or complete. It deliberately validates the catalog contract used by both
// the cutover and the split-store SQL; relation names alone are not evidence
// that the layout is safe to use.
const postgresHotColdCatalogQuery = `
WITH target AS (
    SELECT 'tenant' AS name, to_regclass('audit_tenant') AS oid
    UNION ALL
    SELECT 'ledger' AS name, to_regclass('audit_ledger') AS oid
),
tenant_relation AS (
    SELECT oid FROM target WHERE name = 'tenant'
),
ledger_relation AS (
    SELECT oid FROM target WHERE name = 'ledger'
),
validity AS (
    SELECT
        t.oid IS NOT NULL AS tenant_present,
        l.oid IS NOT NULL AS ledger_present,
        (
            EXISTS (
                SELECT 1 FROM pg_class c
                WHERE c.oid = t.oid AND c.relkind IN ('r', 'p')
            )
            AND EXISTS (
                SELECT 1 FROM pg_attribute a
                WHERE a.attrelid = t.oid AND a.attname = 'tenant_id'
                  AND NOT a.attisdropped AND a.atttypid = 'text'::regtype
                  AND a.attnotnull
            )
            AND EXISTS (
                SELECT 1 FROM pg_attribute a
                WHERE a.attrelid = t.oid AND a.attname = 'snapshot'
                  AND NOT a.attisdropped AND a.atttypid = 'jsonb'::regtype
                  AND a.attnotnull
            )
            AND EXISTS (
                SELECT 1 FROM pg_attribute a
                WHERE a.attrelid = t.oid AND a.attname = 'version'
                  AND NOT a.attisdropped AND a.atttypid = 'int8'::regtype
                  AND a.attnotnull AND a.atthasdef
                  AND EXISTS (
                      SELECT 1 FROM pg_attrdef d
                      WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum
                        AND regexp_replace(lower(pg_get_expr(d.adbin, d.adrelid)), '[[:space:]()]', '', 'g')
                            IN ('1', '1::bigint', '1::integer', '1::numeric')
                  )
            )
            AND EXISTS (
                SELECT 1 FROM pg_attribute a
                WHERE a.attrelid = t.oid AND a.attname = 'updated_at'
                  AND NOT a.attisdropped AND a.atttypid = 'timestamptz'::regtype
                  AND a.attnotnull AND a.atthasdef
                  AND EXISTS (
                      SELECT 1 FROM pg_attrdef d
                      WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum
                        AND lower(pg_get_expr(d.adbin, d.adrelid)) NOT LIKE 'null%'
                  )
            )
            AND EXISTS (
                SELECT 1
                FROM pg_index i
                JOIN pg_class ic ON ic.oid = i.indexrelid
                JOIN pg_am am ON am.oid = ic.relam
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attname = 'tenant_id'
                WHERE i.indrelid = t.oid AND i.indisunique AND i.indisvalid
                  AND i.indpred IS NULL AND i.indnkeyatts = 1
                  AND i.indkey[0] = a.attnum AND am.amname = 'btree'
            )
        ) AS tenant_valid,
        (
            EXISTS (
                SELECT 1 FROM pg_class c
                WHERE c.oid = l.oid AND c.relkind IN ('r', 'p')
            )
            AND EXISTS (
                SELECT 1 FROM pg_attribute a
                WHERE a.attrelid = l.oid AND a.attname = 'id'
                  AND NOT a.attisdropped AND a.atttypid = 'int8'::regtype
                  AND a.attnotnull
                  AND (
                      a.attidentity IN ('a', 'd')
                      OR EXISTS (
                          SELECT 1 FROM pg_attrdef d
                          WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum
                            AND lower(pg_get_expr(d.adbin, d.adrelid)) LIKE 'nextval(%'
                      )
                  )
            )
            AND EXISTS (
                SELECT 1 FROM pg_attribute a
                WHERE a.attrelid = l.oid AND a.attname = 'tenant_id'
                  AND NOT a.attisdropped AND a.atttypid = 'text'::regtype
                  AND a.attnotnull
            )
            AND EXISTS (
                SELECT 1 FROM pg_attribute a
                WHERE a.attrelid = l.oid AND a.attname = 'record_type'
                  AND NOT a.attisdropped AND a.atttypid = 'text'::regtype
                  AND a.attnotnull
            )
            AND EXISTS (
                SELECT 1 FROM pg_attribute a
                WHERE a.attrelid = l.oid AND a.attname = 'key'
                  AND NOT a.attisdropped AND a.atttypid = 'text'::regtype
                  AND a.attnotnull
            )
            AND EXISTS (
                SELECT 1 FROM pg_attribute a
                WHERE a.attrelid = l.oid AND a.attname = 'version'
                  AND NOT a.attisdropped AND a.atttypid = 'int4'::regtype
                  AND a.attnotnull
            )
            AND EXISTS (
                SELECT 1 FROM pg_attribute a
                WHERE a.attrelid = l.oid AND a.attname = 'record'
                  AND NOT a.attisdropped AND a.atttypid = 'jsonb'::regtype
                  AND a.attnotnull
            )
            AND EXISTS (
                SELECT 1 FROM pg_attribute a
                WHERE a.attrelid = l.oid AND a.attname = 'written_at'
                  AND NOT a.attisdropped AND a.atttypid = 'timestamptz'::regtype
                  AND a.attnotnull AND a.atthasdef
                  AND EXISTS (
                      SELECT 1 FROM pg_attrdef d
                      WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum
                        AND lower(pg_get_expr(d.adbin, d.adrelid)) NOT LIKE 'null%'
                  )
            )
            AND EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_attribute a ON a.attrelid = l.oid AND a.attname = 'id'
                WHERE c.conrelid = l.oid AND c.contype = 'p'
                  AND c.conkey = ARRAY[a.attnum]::smallint[]
            )
            AND EXISTS (
                SELECT 1 FROM pg_constraint c
                WHERE c.conrelid = l.oid AND c.contype = 'c'
                  AND lower(regexp_replace(pg_get_constraintdef(c.oid), '[[:space:]]', '', 'g'))
                      = 'check((record_type=any(array[''receipt''::text,''segment''::text,''checkpoint''::text])))'
            )
            AND EXISTS (
                SELECT 1 FROM pg_constraint c
                WHERE c.conrelid = l.oid AND c.contype = 'c'
                  AND lower(regexp_replace(pg_get_constraintdef(c.oid), '[[:space:]]', '', 'g'))
                      = 'check((version>0))'
            )
            AND EXISTS (
                SELECT 1
                FROM pg_index i
                JOIN pg_class ic ON ic.oid = i.indexrelid
                JOIN pg_am am ON am.oid = ic.relam
                JOIN pg_attribute a1 ON a1.attrelid = l.oid AND a1.attname = 'tenant_id'
                JOIN pg_attribute a2 ON a2.attrelid = l.oid AND a2.attname = 'record_type'
                JOIN pg_attribute a3 ON a3.attrelid = l.oid AND a3.attname = 'key'
                JOIN pg_attribute a4 ON a4.attrelid = l.oid AND a4.attname = 'version'
                WHERE i.indrelid = l.oid AND i.indisunique AND i.indisvalid
                  AND i.indpred IS NULL AND i.indnkeyatts = 4
                  AND i.indkey[0] = a1.attnum AND i.indkey[1] = a2.attnum
                  AND i.indkey[2] = a3.attnum AND i.indkey[3] = a4.attnum
                  AND am.amname = 'btree'
            )
        ) AS ledger_valid
    FROM tenant_relation t CROSS JOIN ledger_relation l
)
SELECT CASE
    WHEN NOT tenant_present AND NOT ledger_present THEN 'absent'
    WHEN NOT tenant_present OR NOT ledger_present THEN 'incomplete'
    WHEN tenant_valid AND ledger_valid THEN 'complete'
    ELSE 'incompatible'
END
FROM validity`

const postgresHotColdCatalogStatusAbsent = "absent"
const postgresHotColdCatalogStatusIncomplete = "incomplete"
const postgresHotColdCatalogStatusIncompatible = "incompatible"
const postgresHotColdCatalogStatusComplete = "complete"

func postgresHotColdCatalogStatus(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (string, error) {
	var status string
	if err := queryer.QueryRowContext(ctx, postgresHotColdCatalogQuery).Scan(&status); err != nil {
		return "", err
	}
	return status, nil
}

func postgresHotColdCatalogError(status string) error {
	switch status {
	case postgresHotColdCatalogStatusAbsent:
		return fmt.Errorf("hot/cold schema is absent; apply migration 006_hot_cold_split.sql or provide an equivalent compatible schema")
	case postgresHotColdCatalogStatusIncomplete:
		return fmt.Errorf("hot/cold schema is incomplete; apply migration 006_hot_cold_split.sql or provide an equivalent compatible schema")
	case postgresHotColdCatalogStatusIncompatible:
		return fmt.Errorf("hot/cold schema is incompatible with migration 006; repair it or provide an equivalent compatible schema")
	case postgresHotColdCatalogStatusComplete:
		return nil
	default:
		return fmt.Errorf("hot/cold schema validation returned unknown status %q", status)
	}
}

func validatePostgresHotColdCatalog(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) error {
	status, err := postgresHotColdCatalogStatus(ctx, queryer)
	if err != nil {
		return fmt.Errorf("hot/cold catalog validation: %w", err)
	}
	return postgresHotColdCatalogError(status)
}

const postgresTenantLoadQuery = `SELECT snapshot, version FROM audit_tenant WHERE tenant_id = $1`

const postgresTenantSaveQuery = `INSERT INTO audit_tenant (tenant_id, snapshot, version)
VALUES ($1, $2::jsonb, 1)
ON CONFLICT (tenant_id) DO UPDATE
SET snapshot = EXCLUDED.snapshot, version = audit_tenant.version + 1, updated_at = now()
WHERE audit_tenant.version = $3`

const postgresLedgerAppendQuery = `INSERT INTO audit_ledger (tenant_id, record_type, key, version, record, written_at)
VALUES ($1, $2, $3, $4, $5::jsonb, $6)
ON CONFLICT (tenant_id, record_type, key, version) DO UPDATE
SET record = audit_ledger.record
WHERE audit_ledger.record = EXCLUDED.record`

// postgresSplitStore is selected only when migration 006's complete compatible
// target contract is present. The legacy postgresBackend remains the fallback
// for old deployments, which lets operators apply the expand migration before
// switching binaries.
const postgresHotColdMigrationLockKey int64 = 6006

// MigratePostgresSnapshot performs the v1 single-row to v2 hot/cold cutover
// while holding a transaction-scoped database lock and, when present, the
// snapshot row lock. The database lock closes the no-row gap where two fresh
// migrators cannot lock the absent snapshot row. It is intentionally an
// explicit operator action: production startup refuses to reinterpret a
// legacy row when migration 006 tables are present but the cutover has not
// run.
func MigratePostgresSnapshot(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, postgresHotColdMigrationLockKey); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("acquire hot/cold migration lock: %w", err)
	}
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	var encoded []byte
	var version int64
	// This is the first database inspection after BEGIN. In particular, do
	// not create the marker or backup until the complete target contract has
	// passed; a relation-name check is not sufficient for a safe cutover.
	if err := validatePostgresHotColdCatalog(ctx, tx); err != nil {
		return err
	}
	err = tx.QueryRowContext(ctx, `SELECT snapshot, version FROM audit_state_snapshot WHERE id = 1 FOR UPDATE`).Scan(&encoded, &version)
	if errors.Is(err, sql.ErrNoRows) {
		// A source-less database is a valid fresh initialization only when the
		// expand tables are also empty. Never adopt rows that predate this
		// marker as an unproven zero-data migration.
		if err := verifyPostgresHotColdTargetsEmpty(ctx, tx); err != nil {
			return err
		}
		// A v2 marker without a durable baseline is not verifiable. Establish
		// the explicit zero-data baseline and marker together so a later
		// idempotent call and readiness probe have an evidence trail.
		if err := writePostgresHotColdZeroBaseline(ctx, tx); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO audit_state_snapshot (id, snapshot, version) VALUES (1, $1::jsonb, 1)`, `{"layout_version":2}`); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
		return nil
	}
	if err != nil {
		return err
	}
	var data Snapshot
	if err := decodeSnapshot(encoded, &data); err != nil {
		return err
	}
	if data.LayoutVersion >= hotColdLayoutVersion {
		baseline, err := loadPostgresHotColdBaseline(ctx, tx)
		if err != nil {
			return err
		}
		if err := verifyPostgresHotColdBaseline(ctx, tx, baseline, false); err != nil {
			return err
		}
		// This is deliberately read-only. Do not rewrite the marker, target,
		// or backup while making an idempotent call.
		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
		return nil
	}
	var tenantCount, ledgerCount int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM audit_tenant`).Scan(&tenantCount); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM audit_ledger`).Scan(&ledgerCount); err != nil {
		return err
	}
	if tenantCount != 0 || ledgerCount != 0 {
		return hotColdInconsistency("pre-cutover targets expected audit_tenant=0 and audit_ledger=0, actual audit_tenant=%d audit_ledger=%d", tenantCount, ledgerCount)
	}
	prepared, err := splitLegacySnapshot(&data)
	if err != nil {
		return err
	}
	if err := createPostgresHotColdBaselineTable(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM audit_state_snapshot_v1_backup WHERE id = 1`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO audit_state_snapshot_v1_backup (id, snapshot, version, updated_at) SELECT id, snapshot, version, updated_at FROM audit_state_snapshot WHERE id = 1`); err != nil {
		return err
	}
	for tenantID, state := range prepared.tenant {
		encodedHot, err := json.Marshal(tenantHotSnapshot{TenantID: tenantID, Version: state.version, Events: state.hot.Events, Streams: state.hot.Streams})
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO audit_tenant (tenant_id, snapshot, version) VALUES ($1, $2::jsonb, $3)`, tenantID, string(encodedHot), state.version); err != nil {
			return err
		}
		for _, record := range state.ledger.committedRecords() {
			encodedRecord, err := json.Marshal(record)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO audit_ledger (tenant_id, record_type, key, version, record, written_at) VALUES ($1, $2, $3, $4, $5::jsonb, $6)`, record.TenantID, record.RecordType, record.Key, record.Version, string(encodedRecord), record.WrittenAt); err != nil {
				return err
			}
		}
	}
	if err := verifyPostgresHotColdBaseline(ctx, tx, prepared, true); err != nil {
		return err
	}
	encodedControl, err := json.Marshal(prepared.control)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE audit_state_snapshot SET snapshot = $1::jsonb, version = version + 1, updated_at = now() WHERE id = 1 AND version = $2`, string(encodedControl), version)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrSnapshotConflict
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

// enabled reports whether migration 006's complete, compatible target
// contract is present. The verdict is memoized because it is a catalog
// invariant, but a probe aborted by the caller's operation context is NOT:
// memoizing it would let one transient deadline on the first call poison every
// store operation in the process for the rest of its life. Schema verdicts and
// genuine (non-context) errors stay sticky, exactly as before.
func (p *postgresSplitStore) enabled(ctx context.Context) (bool, error) {
	p.probeMu.Lock()
	defer p.probeMu.Unlock()
	if !p.probed {
		status, err := postgresHotColdCatalogStatus(ctx, p.backend.db)
		if err == nil {
			p.catalogState = status
			if status != postgresHotColdCatalogStatusAbsent && status != postgresHotColdCatalogStatusComplete {
				p.catalogErr = postgresHotColdCatalogError(status)
			}
		} else if ctx.Err() == nil {
			// A real schema/transport error: sticky, so every operation keeps
			// reporting it instead of re-probing on every request.
			p.catalogErr = fmt.Errorf("hot/cold catalog detection: %w", err)
		}
		if p.catalogErr == nil || ctx.Err() == nil {
			p.probed = true
		}
	}
	if p.catalogErr != nil {
		return false, p.catalogErr
	}
	return p.catalogState == postgresHotColdCatalogStatusComplete, nil
}
