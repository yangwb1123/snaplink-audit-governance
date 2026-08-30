package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// postgresBackend persists the control-plane state snapshot in the
// single-row audit_state_snapshot table. Load/Save pairs are serialized by
// Store.mu within one process; across processes the version column provides
// an optimistic lock so a stale writer fails with ErrSnapshotConflict
// instead of overwriting a concurrent commit.
//
// lastVersion is the optimistic-lock baseline. It is written ONLY on the
// write path: LoadForUpdate records it and Save advances it, both of which
// Store.Update serializes with the exclusive lock. Load is side-effect-free
// and therefore safe under concurrent RLock readers.
type postgresBackend struct {
	db          *sql.DB
	lastVersion int64

	// Read self-audit trail state (trail.go). trailMu serializes trail
	// writes within the process; across replicas BIGSERIAL seq provides
	// total order. The trail is deliberately independent of the snapshot row
	// (no version bump, no updated_at, no row rewrite), so reads never
	// contend with ingests on the snapshot write path (F-2).
	trailMu      sync.Mutex
	trailCount   int64
	trailEnsured bool
}

const (
	snapshotLoadQuery = `SELECT snapshot, version FROM audit_state_snapshot WHERE id = 1`
	// Ensures the row exists before the version-checked update. A concurrent
	// first writer wins; the loser's stale version still conflicts below.
	snapshotInsertQuery = `INSERT INTO audit_state_snapshot (id, snapshot, version)
VALUES (1, '{}'::jsonb, 1) ON CONFLICT (id) DO NOTHING`
	snapshotUpdateQuery = `UPDATE audit_state_snapshot
SET snapshot = $1::jsonb, version = version + 1, updated_at = now()
WHERE id = 1 AND version = $2`
)

func (p *postgresBackend) Load() (*Snapshot, error) {
	data, _, err := p.load()
	if errors.Is(err, sql.ErrNoRows) {
		return data, nil
	}
	return data, err
}

func (p *postgresBackend) LoadForUpdate() (*Snapshot, error) {
	data, version, err := p.load()
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// No row: keep the previous baseline, exactly like the pre-split
			// Load. A backend primed at an older version whose row was
			// deleted externally still conflicts loudly on Save instead of
			// silently recreating an empty row.
			return data, nil
		}
		return nil, err
	}
	p.lastVersion = version // write-path baseline; serialized by Store.mu
	return data, nil
}

// load reads the snapshot row without touching any backend state. A missing
// row yields an empty snapshot plus sql.ErrNoRows so each caller decides
// whether the optimistic-lock baseline changes.
func (p *postgresBackend) load() (*Snapshot, int64, error) {
	data := NewSnapshot()
	var encoded []byte
	var version int64
	err := p.db.QueryRow(snapshotLoadQuery).Scan(&encoded, &version)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return data, 0, err
		}
		return nil, 0, fmt.Errorf("load state snapshot: %w", err)
	}
	if err := decodeSnapshot(encoded, data); err != nil {
		return nil, 0, fmt.Errorf("decode state snapshot: %w", err)
	}
	data.normalize()
	return data, version, nil
}

func (p *postgresBackend) Close() error { return p.db.Close() }

// Ready pings the control-plane database so /readyz can fail fast when the
// snapshot backend is unreachable, and verifies migration 005 landed: the
// read self-audit trail table must exist before the trail path can serve
// reads (F-5). File-backed stores have no probe.
func (p *postgresBackend) Ready(ctx context.Context) error {
	if err := p.db.PingContext(ctx); err != nil {
		return fmt.Errorf("state snapshot store: %w", err)
	}
	var trailExists bool
	if err := p.db.QueryRowContext(ctx, `SELECT to_regclass('admin_action_trail') IS NOT NULL`).Scan(&trailExists); err != nil {
		return fmt.Errorf("admin action trail catalog: %w", err)
	}
	if !trailExists {
		return fmt.Errorf("admin action trail table missing: apply migration 005_admin_action_trail.sql")
	}
	catalogStatus, err := postgresHotColdCatalogStatus(ctx, p.db)
	if err != nil {
		return fmt.Errorf("hot/cold catalog: %w", err)
	}
	switch catalogStatus {
	case postgresHotColdCatalogStatusAbsent:
		var layout string
		err := p.db.QueryRowContext(ctx, `SELECT COALESCE(snapshot->>'layout_version', '0') FROM audit_state_snapshot WHERE id = 1`).Scan(&layout)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("hot/cold layout marker: %w", err)
		}
		if layout == fmt.Sprint(hotColdLayoutVersion) {
			return fmt.Errorf("hot/cold layout marker is present but migration 006 tables are missing: apply 006_hot_cold_split.sql")
		}
	case postgresHotColdCatalogStatusIncomplete, postgresHotColdCatalogStatusIncompatible:
		return postgresHotColdCatalogError(catalogStatus)
	case postgresHotColdCatalogStatusComplete:
		// Applying 006 is an expand step; an existing v1 row must not be
		// served through the split store until the explicit cutover has moved
		// its ledger data. Keep this check in readiness so operators see the
		// migration action before requests start failing at the data path.
		var layout string
		var legacyLedger bool
		err := p.db.QueryRowContext(ctx, `
SELECT COALESCE(snapshot->>'layout_version', '0'),
       COALESCE(snapshot->'events', '{}'::jsonb) <> '{}'::jsonb OR
       COALESCE(snapshot->'receipts', '{}'::jsonb) <> '{}'::jsonb OR
       COALESCE(snapshot->'streams', '{}'::jsonb) <> '{}'::jsonb OR
       COALESCE(snapshot->'segments', '{}'::jsonb) <> '{}'::jsonb OR
       COALESCE(snapshot->'checkpoints', '{}'::jsonb) <> '{}'::jsonb
FROM audit_state_snapshot WHERE id = 1`).Scan(&layout, &legacyLedger)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("hot/cold layout marker: %w", err)
		}
		if layout != fmt.Sprint(hotColdLayoutVersion) && legacyLedger {
			return fmt.Errorf("hot/cold migration pending: audit_state_snapshot still contains v1 ledger data; run audit-pg-migrate after applying 006_hot_cold_split.sql")
		}
	default:
		return postgresHotColdCatalogError(catalogStatus)
	}
	return nil
}

func (p *postgresBackend) Save(data *Snapshot) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if p.lastVersion == 0 {
		// Row does not exist yet from our point of view. Creating it is only
		// safe if we win the insert; a concurrent initializer means our
		// empty-state snapshot must not overwrite its data.
		result, err := p.db.Exec(snapshotInsertQuery)
		if err != nil {
			return fmt.Errorf("ensure state snapshot row: %w", err)
		}
		inserted, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if inserted == 0 {
			return ErrSnapshotConflict
		}
		p.lastVersion = 1
	}
	result, err := p.db.Exec(snapshotUpdateQuery, string(encoded), p.lastVersion)
	if err != nil {
		return fmt.Errorf("save state snapshot: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrSnapshotConflict
	}
	p.lastVersion++
	return nil
}

var _ Backend = (*postgresBackend)(nil)
