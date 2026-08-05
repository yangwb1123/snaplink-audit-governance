package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// postgresBackend persists the control-plane state snapshot in the
// single-row audit_state_snapshot table. Load/Save pairs are serialized by
// Store.mu within one process; across processes the version column provides
// an optimistic lock so a stale writer fails with ErrSnapshotConflict
// instead of overwriting a concurrent commit.
type postgresBackend struct {
	db          *sql.DB
	lastVersion int64
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
	data := NewSnapshot()
	var encoded []byte
	var version int64
	err := p.db.QueryRow(snapshotLoadQuery).Scan(&encoded, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return data, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load state snapshot: %w", err)
	}
	if err := json.Unmarshal(encoded, data); err != nil {
		return nil, fmt.Errorf("decode state snapshot: %w", err)
	}
	data.normalize()
	p.lastVersion = version
	return data, nil
}

func (p *postgresBackend) Close() error { return p.db.Close() }

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
