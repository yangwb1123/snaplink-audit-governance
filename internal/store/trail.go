package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	// compacts the trail to the newest trailWatermark(trailCap) facts once
	// the bound is exceeded. An error means the fact is not persisted (fail
	// closed).
	AppendAdminFact(ctx context.Context, fact domain.AdminAction, trailCap int) error
	// ReadAdminTrail returns trail facts newest-first (reverse append
	// order); an empty result when the backend has no trail.
	ReadAdminTrail(ctx context.Context) ([]domain.AdminAction, error)
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
func (s *Store) AppendAdminFact(ctx context.Context, fact domain.AdminAction, snapshotCap, trailCap int) (err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	return s.appendAdminFact(op.ctx, fact, snapshotCap, trailCap)
}

// appendAdminFact is AppendAdminFact's body; see Store.read for the nesting
// rule.
func (s *Store) appendAdminFact(ctx context.Context, fact domain.AdminAction, snapshotCap, trailCap int) error {
	if trail, ok := s.backend.(AdminTrail); ok {
		return trail.AppendAdminFact(ctx, fact, trailCap)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updateLocked(ctx, func(data *Snapshot) error {
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
func (s *Store) ReadAdminTrail(ctx context.Context) (_ []domain.AdminAction, err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	if trail, ok := s.backend.(AdminTrail); ok {
		return trail.ReadAdminTrail(op.ctx)
	}
	return nil, nil
}

// ---- fileBackend trail -------------------------------------------------

// trailPathSuffix is appended to the state file path for the read
// self-audit trail, e.g. ./data/state.json -> ./data/state.json.admin-trail.jsonl.
const trailPathSuffix = ".admin-trail.jsonl"

// trailWatermark is how many of the newest facts a compaction retains for an
// operator cap of trailCap: half the cap, at least one. Compacting to the
// watermark instead of to the cap is what makes compaction amortized: the
// trail oscillates in [W, trailCap] and the O(trailCap) rewrite runs once per
// headroom+1 appends instead of once per append. For trailCap == 3 the trail
// settles at the same steady state it had when compaction trimmed to the cap
// (2 + ((n-2) mod 2) facts), which is why the existing exact-length
// compaction expectations are unchanged. 0 for an unbounded cap, where the
// compaction trigger is unreachable and nothing is ever trimmed.
func trailWatermark(trailCap int) int {
	if trailCap <= 0 {
		return 0
	}
	headroom := trailCap / 2
	if headroom < 1 {
		headroom = 1
	}
	if watermark := trailCap - headroom; watermark >= 1 {
		return watermark
	}
	return 1
}

// trailHeadroom is how many further appends fit before the next compaction
// (trailCap - trailWatermark(trailCap)); the trigger itself fires on the
// (headroom+1)-th one. 0 for an unbounded cap and for the degenerate
// trailCap == 1, which keeps compact-per-append behaviour because no positive
// headroom exists under a one-fact cap. Doc-and-test helper: the production
// schedule is driven by trailCount and the trailCap trigger, so no caller
// schedules work from this value.
func trailHeadroom(trailCap int) int {
	if trailCap <= 0 {
		return 0
	}
	return trailCap - trailWatermark(trailCap)
}

// AppendAdminFact appends one fact to <state>.admin-trail.jsonl as a JSONL
// line, fsynced per append (durability parity with Save). The persistent
// O_APPEND fd is lazily opened (new parent 0700, new file 0600, dir-chain fsync
// on first creation). trailCount is a lazy baseline count so the cap check
// never re-reads the trail (F-4). Past the cap the file is compacted
// crash-atomically (tmp + fsync + rename + dir-chain sync + fd reopen,
// F-7): the trailing fact that overflowed the bound is dropped with the
// oldest facts.
func (f *fileBackend) AppendAdminFact(ctx context.Context, fact domain.AdminAction, trailCap int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return os.ErrClosed
	}
	if f.path == "" {
		// In-memory mode: a plain bounded slice under f.mu.
		f.trail = append(f.trail, fact)
		if trailCap > 0 && len(f.trail) > trailCap {
			// Trim to the same watermark the file backend uses, so all three
			// retention paths agree on depth. Copy the tail so the dropped
			// prefix's backing array can be garbage-collected.
			keep := trailWatermark(trailCap)
			f.trail = append([]domain.AdminAction(nil), f.trail[len(f.trail)-keep:]...)
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
		// Compact to the watermark, not to the cap: resetting trailCount to
		// keep (rather than to trailCap) is what buys trailHeadroom(trailCap)
		// further appends before the next O(trailCap) rewrite.
		if err := f.compactTrail(trailWatermark(trailCap)); err != nil {
			return err
		}
	}
	return nil
}

// ReadAdminTrail reads the JSONL trail newest-first. A malformed non-final
// line fails closed (the governance surface must not silently truncate
// evidence); a truncated FINAL line (crash mid-append) is tolerated and
// dropped (FM-6). An empty result when the trail file does not exist yet.
func (f *fileBackend) ReadAdminTrail(ctx context.Context) ([]domain.AdminAction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.closed {
		return nil, os.ErrClosed
	}
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
	if err := fsutil.MakeDataDir(parent); err != nil {
		return err
	}
	if !f.trailCounted {
		if contents, err := fsutil.ReadDataFile(f.trailPath); err == nil {
			f.trailCount = countJSONLLines(contents)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		f.trailCounted = true
	}
	_, statErr := fsutil.StatDataFile(f.trailPath)
	created := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !created {
		return statErr
	}
	fd, err := fsutil.OpenDataFile(f.trailPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, fsutil.PrivateFileMode)
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

// compactTrail rewrites the trail keeping the newest keepCount facts (keepCount
// is the watermark trailWatermark(trailCap), not the cap itself, so the rewrite
// is amortized across trailHeadroom(trailCap) further appends). Crash-atomic:
// write tmp -> fsync -> rename -> dir-chain sync -> reopen the persistent fd
// (the old fd points at the pre-rename inode). On any error before the rename
// the pre-compaction trail is intact (FM-5); the caller fails the append
// closed and the next append retries the compaction once the trigger
// condition persists. Caller holds f.mu exclusively.
func (f *fileBackend) compactTrail(keepCount int) error {
	facts, err := f.readTrailFileLocked()
	if err != nil {
		return err
	}
	if len(facts) <= keepCount {
		f.trailCount = int64(len(facts))
		return nil
	}
	keep := facts[len(facts)-keepCount:] // newest keepCount, oldest-first order
	var content bytes.Buffer
	encoder := json.NewEncoder(&content)
	for _, fact := range keep {
		if err := encoder.Encode(fact); err != nil {
			return err
		}
	}
	err = fsutil.AtomicDataFile(f.trailPath, content.Bytes(), fsutil.PrivateFileMode, f.syncDir)
	if err != nil && !errors.Is(err, fsutil.ErrDataDirectorySync) {
		return err // no rename: previous trail/fd remain authoritative
	}
	f.trailCompactions++
	if f.trailFile != nil {
		err = errors.Join(err, f.trailFile.Close())
		f.trailFile = nil
	}
	if err != nil {
		return err // renamed; next append reopens, never claim a durable read
	}
	fd, err := fsutil.OpenDataFile(f.trailPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, fsutil.PrivateFileMode)
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
	contents, err := fsutil.ReadDataFile(f.trailPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
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
	// DELETE's statement snapshot) can never be deleted (F-4). $1 is the
	// watermark trailWatermark(trailCap), never the cap itself: trimming to
	// the watermark is what makes compaction amortized. With the per-replica
	// soft cap the table briefly exceeds the bound; the overshoot is bounded
	// by watermark + (headroom+1) rows per sharing replica and self-corrects
	// (FM-7). The DELETE — not the local counter — is authoritative, so an
	// over-count only costs a redundant no-op DELETE and an under-count only
	// defers one.
	trailCompactQuery = `DELETE FROM admin_action_trail WHERE seq < (
    SELECT seq FROM admin_action_trail ORDER BY seq DESC OFFSET ($1 - 1) LIMIT 1)`
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
//
// Compaction is amortized by trimming to trailWatermark(trailCap) and then
// resetting trailCount to that watermark, so the O(trailCap) DELETE runs once
// per trailHeadroom(trailCap)+1 appends instead of on every append past the
// cap — previously trailCount was never reset, so every audited read issued
// the offset scan while holding trailMu, serializing every other audited
// read's append behind it. The reset makes trailCount a lower bound on the
// global row count, which is the tightest bound that cannot over-delete
// (FM-7): a concurrent replica's rows are simply not counted, so this replica
// trims again after a bounded headroom instead of tracking them exactly.
// A compaction error leaves the counter high, so the next append retries.
func (p *postgresBackend) AppendAdminFact(ctx context.Context, fact domain.AdminAction, trailCap int) error {
	p.trailMu.Lock()
	defer p.trailMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
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
		keep := trailWatermark(trailCap)
		if _, err := p.db.ExecContext(ctx, trailCompactQuery, keep); err != nil {
			return fmt.Errorf("compact admin action trail: %w", err)
		}
		p.trailCount = int64(keep)
	}
	return nil
}

// ReadAdminTrail returns trail facts newest-first (seq DESC, i.e. reverse
// append order). No trailMu is needed on the read side: readers see only
// committed rows, and the merged governance view is eventually consistent
// between the two sources by design. A missing table (migration 005 not
// applied) fails closed — the runbook requires 005 before rollout and readyz
// surfaces the missing table fast (F-5).
func (p *postgresBackend) ReadAdminTrail(ctx context.Context) ([]domain.AdminAction, error) {
	rows, err := p.db.QueryContext(ctx, trailReadQuery)
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
