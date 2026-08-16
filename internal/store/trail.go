package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/fsutil"
)

// AdminTrail is the optional append-only persistence for read-path admin
// facts. Backends that implement it absorb read facts without rewriting the
// snapshot document; backends that do not fall back to the legacy
// full-snapshot Save in Store.AppendAdminFact, preserving the fail-closed
// and optimistic-lock semantics of capability-less test seams.
type AdminTrail interface {
	// AppendAdminFact durably records one read-path fact. trailCap bounds
	// the trail (0 = unbounded, operator-explicit): a positive value
	// compacts the trail to its newest trailCap facts once the bound is
	// exceeded. An error means the fact is not persisted (fail closed).
	AppendAdminFact(fact domain.AdminAction, trailCap int) error
	// ReadAdminTrail returns trail facts newest-first (reverse append
	// order); an empty result when the backend has no trail.
	ReadAdminTrail() ([]domain.AdminAction, error)
}

// AppendAdminFact records one read self-audit fact (audit.event.read and
// the download legs: audit.event.export, export.download_rejected,
// export.blocked on the download leg). Trail-capable backends persist it in
// the append-only trail WITHOUT holding Store.mu — appends are append-only
// with no snapshot coupling, and the backend serializes its own trail
// writes, so reads stop blocking ingests (F-2, the direction's core defect).
// Capability-less backends fall back to the legacy full-snapshot Save under
// the exclusive lock, with identical conflict-retry and error semantics to
// Store.Update (F-1): the fact is appended to Snapshot.AdminActions and
// snapshotCap bounds that slice (drop-oldest) so even the fallback cannot
// regrow it unboundedly.
func (s *Store) AppendAdminFact(fact domain.AdminAction, snapshotCap, trailCap int) error {
	if trail, ok := s.backend.(AdminTrail); ok {
		return trail.AppendAdminFact(fact, trailCap)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updateLocked(func(data *Snapshot) error {
		data.AdminActions = append(data.AdminActions, fact)
		if snapshotCap > 0 && len(data.AdminActions) > snapshotCap {
			data.AdminActions = data.AdminActions[len(data.AdminActions)-snapshotCap:]
		}
		return nil
	})
}

// ReadAdminTrail returns the read self-audit trail newest-first for the
// merged ListAdminActions view. The trail is independent of the snapshot, so
// this never takes Store.mu (F-2); backends without a trail report nil.
func (s *Store) ReadAdminTrail() ([]domain.AdminAction, error) {
	if trail, ok := s.backend.(AdminTrail); ok {
		return trail.ReadAdminTrail()
	}
	return nil, nil
}

// ---- fileBackend trail -------------------------------------------------

// trailPathSuffix is appended to the state file path for the read
// self-audit trail, e.g. ./data/state.json -> ./data/state.json.admin-trail.jsonl.
const trailPathSuffix = ".admin-trail.jsonl"

// AppendAdminFact appends one fact to <state>.admin-trail.jsonl as a JSONL
// line, fsynced per append (durability parity with Save). The persistent
// O_APPEND fd is lazily opened (parent 0o750, file 0o640, dir-chain fsync
// on first creation). trailCount is a lazy baseline count so the cap check
// never re-reads the trail (F-4). Past the cap the file is compacted
// crash-atomically (tmp + fsync + rename + dir-chain sync + fd reopen,
// F-7): the trailing fact that overflowed the bound is dropped with the
// oldest facts.
func (f *fileBackend) AppendAdminFact(fact domain.AdminAction, trailCap int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.path == "" {
		// In-memory mode: a plain bounded slice under f.mu.
		f.trail = append(f.trail, fact)
		if trailCap > 0 && len(f.trail) > trailCap {
			// Copy the tail so the dropped prefix's backing array can be
			// garbage-collected.
			f.trail = append([]domain.AdminAction(nil), f.trail[len(f.trail)-trailCap:]...)
		}
		return nil
	}
	if err := f.ensureTrailFile(); err != nil {
		return err
	}
	encoded, err := json.Marshal(fact)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	if _, err := f.trailFile.Write(encoded); err != nil {
		return err
	}
	if err := f.trailFile.Sync(); err != nil {
		return err
	}
	f.trailCount++
	if trailCap > 0 && f.trailCount > int64(trailCap) {
		if err := f.compactTrail(trailCap); err != nil {
			return err
		}
	}
	return nil
}

// ReadAdminTrail reads the JSONL trail newest-first. A malformed non-final
// line fails closed (the governance surface must not silently truncate
// evidence); a truncated FINAL line (crash mid-append) is tolerated and
// dropped (FM-6). An empty result when the trail file does not exist yet.
func (f *fileBackend) ReadAdminTrail() ([]domain.AdminAction, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.path == "" {
		out := make([]domain.AdminAction, 0, len(f.trail))
		for i := len(f.trail) - 1; i >= 0; i-- {
			out = append(out, f.trail[i])
		}
		return out, nil
	}
	facts, err := f.readTrailFileLocked()
	if err != nil {
		return nil, err
	}
	out := make([]domain.AdminAction, 0, len(facts))
	for i := len(facts) - 1; i >= 0; i-- {
		out = append(out, facts[i])
	}
	return out, nil
}

// ensureTrailFile lazily opens the persistent O_APPEND trail fd and seeds
// trailCount with the existing complete lines (a restarted process must not
// re-trim what is already bounded). Caller holds f.mu exclusively.
func (f *fileBackend) ensureTrailFile() error {
	if f.trailFile != nil {
		return nil
	}
	if f.trailPath == "" {
		f.trailPath = f.path + trailPathSuffix
	}
	parent := filepath.Dir(f.trailPath)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return err
	}
	if !f.trailCounted {
		if contents, err := os.ReadFile(f.trailPath); err == nil {
			f.trailCount = countJSONLLines(contents)
		} else if !os.IsNotExist(err) {
			return err
		}
		f.trailCounted = true
	}
	_, statErr := os.Stat(f.trailPath)
	created := os.IsNotExist(statErr)
	if statErr != nil && !created {
		return statErr
	}
	fd, err := os.OpenFile(f.trailPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	if created {
		// First creation: the trail's directory entry must survive power
		// failure (same discipline as Save's post-rename chain sync).
		syncDir := f.syncDir
		if syncDir == nil {
			syncDir = fsutil.SyncDir
		}
		if err := fsutil.SyncDirChain(fsutil.DeepestExistingAncestor(parent), parent, syncDir); err != nil {
			_ = fd.Close()
			return err
		}
	}
	f.trailFile = fd
	return nil
}

// compactTrail rewrites the trail keeping the newest trailCap facts.
// Crash-atomic: write tmp -> fsync -> rename -> dir-chain sync -> reopen the
// persistent fd (the old fd points at the pre-rename inode). On any error
// before the rename the pre-compaction trail is intact (FM-5); the caller
// fails the append closed and the next append retries the compaction once
// the trigger condition persists. Caller holds f.mu exclusively.
func (f *fileBackend) compactTrail(trailCap int) error {
	facts, err := f.readTrailFileLocked()
	if err != nil {
		return err
	}
	if len(facts) <= trailCap {
		f.trailCount = int64(len(facts))
		return nil
	}
	keep := facts[len(facts)-trailCap:] // newest trailCap, oldest-first order
	tmp := f.trailPath + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	for _, fact := range keep {
		encoded, err := json.Marshal(fact)
		if err != nil {
			_ = file.Close()
			_ = os.Remove(tmp)
			return err
		}
		if _, err := file.Write(append(encoded, '\n')); err != nil {
			_ = file.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, f.trailPath); err != nil {
		return err
	}
	if f.trailFile != nil {
		_ = f.trailFile.Close()
		f.trailFile = nil
	}
	syncDir := f.syncDir
	if syncDir == nil {
		syncDir = fsutil.SyncDir
	}
	if err := fsutil.SyncDirChain(fsutil.DeepestExistingAncestor(filepath.Dir(f.trailPath)), filepath.Dir(f.trailPath), syncDir); err != nil {
		// The rename already replaced the trail; the next append reopens the
		// fd. Report the error (fail closed, FM-5).
		return err
	}
	fd, err := os.OpenFile(f.trailPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	f.trailFile = fd
	f.trailCount = int64(len(keep))
	return nil
}

// readTrailFileLocked decodes the JSONL trail oldest-first. Only complete
// lines (each ending in '\n') count as facts; a truncated final line is a
// crash mid-append and is dropped. Caller holds f.mu (at least RLock).
func (f *fileBackend) readTrailFileLocked() ([]domain.AdminAction, error) {
	if f.trailPath == "" {
		f.trailPath = f.path + trailPathSuffix
	}
	contents, err := os.ReadFile(f.trailPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var facts []domain.AdminAction
	for len(contents) > 0 {
		line, rest, complete := bytes.Cut(contents, []byte{'\n'})
		contents = rest
		if !complete {
			break // truncated final line: crash mid-append, tolerated
		}
		if len(line) == 0 {
			continue
		}
		var fact domain.AdminAction
		if err := json.Unmarshal(line, &fact); err != nil {
			return nil, fmt.Errorf("corrupt admin trail line: %w", err)
		}
		facts = append(facts, fact)
	}
	return facts, nil
}

// countJSONLLines counts the complete lines (terminated by '\n') in a JSONL
// trail file. A trailing partial line has no terminator and does not count.
func countJSONLLines(contents []byte) int64 {
	var count int64
	for _, b := range contents {
		if b == '\n' {
			count++
		}
	}
	return count
}

// ---- postgresBackend trail ----------------------------------------------

const (
	// ensureTrailTableQuery mirrors migration 005's table DDL so a late-
	// applied migration self-heals on the first trail append (F-5). The
	// tenant index stays migration-owned; this process only ever reads
	// order-by-seq.
	ensureTrailTableQuery = `CREATE TABLE IF NOT EXISTS admin_action_trail (
    seq         BIGSERIAL PRIMARY KEY,
    id          TEXT NOT NULL,
    tenant_id   TEXT NOT NULL,
    actor       TEXT NOT NULL,
    action      TEXT NOT NULL,
    target_type TEXT NOT NULL DEFAULT '',
    target_id   TEXT NOT NULL DEFAULT '',
    detail      TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL
)`
	trailInsertQuery = `INSERT INTO admin_action_trail
    (id, tenant_id, actor, action, target_type, target_id, detail, created_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
	// trailCompactQuery keeps the newest $1 rows via a seq watermark so a
	// concurrent replica's INSERT (allocated a newer BIGSERIAL seq after the
	// DELETE's statement snapshot) can never be deleted (F-4). With the
	// per-replica soft cap the table briefly exceeds the bound; this is
	// self-correcting (FM-7).
	trailCompactQuery = `DELETE FROM admin_action_trail WHERE seq < (
    SELECT seq FROM admin_action_trail ORDER BY seq DESC OFFSET $1 LIMIT 1)`
	trailReadQuery = `SELECT id, tenant_id, actor, action, target_type, target_id, detail, created_at
    FROM admin_action_trail ORDER BY seq DESC`
	trailCountQuery = `SELECT count(*) FROM admin_action_trail`
)

// AppendAdminFact inserts one fact into admin_action_trail — a single
// autocommit INSERT with no version bump, no updated_at and no row rewrite
// of audit_state_snapshot. trailMu serializes trail writes within the
// process; across replicas BIGSERIAL seq provides total order. The table is
// lazily ensured once per process (F-5); trailCount is seeded with a lazy
// count so the soft-cap check is O(1) per append (F-4).
func (p *postgresBackend) AppendAdminFact(fact domain.AdminAction, trailCap int) error {
	p.trailMu.Lock()
	defer p.trailMu.Unlock()
	ctx := context.Background()
	if !p.trailEnsured {
		if _, err := p.db.ExecContext(ctx, ensureTrailTableQuery); err != nil {
			return fmt.Errorf("ensure admin action trail: %w", err)
		}
		var count int64
		if err := p.db.QueryRowContext(ctx, trailCountQuery).Scan(&count); err != nil {
			return fmt.Errorf("count admin action trail: %w", err)
		}
		p.trailCount = count
		p.trailEnsured = true
	}
	if _, err := p.db.ExecContext(ctx, trailInsertQuery,
		fact.ID, fact.TenantID, fact.Actor, fact.Action, fact.TargetType, fact.TargetID, fact.Detail, fact.CreatedAt); err != nil {
		return fmt.Errorf("append admin action trail: %w", err)
	}
	p.trailCount++
	if trailCap > 0 && p.trailCount > int64(trailCap) {
		if _, err := p.db.ExecContext(ctx, trailCompactQuery, trailCap); err != nil {
			return fmt.Errorf("compact admin action trail: %w", err)
		}
	}
	return nil
}

// ReadAdminTrail returns trail facts newest-first (seq DESC, i.e. reverse
// append order). No trailMu is needed on the read side: readers see only
// committed rows, and the merged governance view is eventually consistent
// between the two sources by design. A missing table (migration 005 not
// applied) fails closed — the runbook requires 005 before rollout and readyz
// surfaces the missing table fast (F-5).
func (p *postgresBackend) ReadAdminTrail() ([]domain.AdminAction, error) {
	rows, err := p.db.QueryContext(context.Background(), trailReadQuery)
	if err != nil {
		return nil, fmt.Errorf("read admin action trail: %w", err)
	}
	defer rows.Close()
	var facts []domain.AdminAction
	for rows.Next() {
		var fact domain.AdminAction
		if err := rows.Scan(&fact.ID, &fact.TenantID, &fact.Actor, &fact.Action, &fact.TargetType, &fact.TargetID, &fact.Detail, &fact.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan admin action trail: %w", err)
		}
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read admin action trail: %w", err)
	}
	return facts, nil
}

var _ AdminTrail = (*fileBackend)(nil)
var _ AdminTrail = (*postgresBackend)(nil)
