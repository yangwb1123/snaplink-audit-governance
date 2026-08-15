package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/fsutil"
)

// ErrSnapshotConflict is returned when a concurrent instance persisted the
// state snapshot after this instance loaded it. The optimistic version check
// prevents lost updates when multiple audit-api replicas share one
// PostgreSQL snapshot row.
var ErrSnapshotConflict = errors.New("state snapshot changed concurrently")

type StreamState struct {
	TenantID        string   `json:"tenant_id"`
	StreamID        string   `json:"stream_id"`
	NextSequence    int64    `json:"next_sequence"`
	HeadHash        string   `json:"head_hash"`
	PendingPrevHash string   `json:"pending_prev_hash,omitempty"`
	PendingHashes   []string `json:"pending_hashes,omitempty"`
	PendingEvents   []string `json:"pending_events,omitempty"`
}

type Snapshot struct {
	Tenants              map[string]domain.Tenant          `json:"tenants"`
	Sources              map[string]domain.SourceSystem    `json:"sources"`
	Schemas              map[string]domain.EventSchema     `json:"schemas"`
	Policies             map[string]domain.RetentionPolicy `json:"policies"`
	Events               map[string]domain.Event           `json:"events"`
	Receipts             map[string]domain.EventReceipt    `json:"receipts"`
	Streams              map[string]StreamState            `json:"streams"`
	Segments             map[string][]domain.Segment       `json:"segments"`
	Checkpoints          map[string][]domain.Checkpoint    `json:"checkpoints"`
	LegalHolds           map[string]domain.LegalHold       `json:"legal_holds"`
	Exports              map[string]domain.ExportJob       `json:"exports"`
	RestoreRuns          map[string]domain.RestoreRun      `json:"restore_runs"`
	AdminActions         []domain.AdminAction              `json:"admin_actions"`
	AggregateCheckpoints []domain.AggregateCheckpoint      `json:"aggregate_checkpoints"`
	// ArchiveConflictFailures counts consecutive archive passes aborted by
	// optimistic-lock exhaustion, per tenant. Reset to zero by the batch
	// receipt commit in ArchivePending; incremented best-effort by the
	// worker when a pass fails with ErrSnapshotConflict. It lives inside
	// the snapshot document, so old snapshots decode as nil and normalize
	// to an empty map — no migration is needed.
	ArchiveConflictFailures map[string]int `json:"archive_conflict_failures,omitempty"`
}

func NewSnapshot() *Snapshot {
	return &Snapshot{
		Tenants:                 map[string]domain.Tenant{},
		Sources:                 map[string]domain.SourceSystem{},
		Schemas:                 map[string]domain.EventSchema{},
		Policies:                map[string]domain.RetentionPolicy{},
		Events:                  map[string]domain.Event{},
		Receipts:                map[string]domain.EventReceipt{},
		Streams:                 map[string]StreamState{},
		Segments:                map[string][]domain.Segment{},
		Checkpoints:             map[string][]domain.Checkpoint{},
		LegalHolds:              map[string]domain.LegalHold{},
		Exports:                 map[string]domain.ExportJob{},
		RestoreRuns:             map[string]domain.RestoreRun{},
		AdminActions:            []domain.AdminAction{},
		AggregateCheckpoints:    []domain.AggregateCheckpoint{},
		ArchiveConflictFailures: map[string]int{},
	}
}

func (s *Snapshot) normalize() {
	if s.Tenants == nil {
		s.Tenants = map[string]domain.Tenant{}
	}
	if s.Sources == nil {
		s.Sources = map[string]domain.SourceSystem{}
	}
	if s.Schemas == nil {
		s.Schemas = map[string]domain.EventSchema{}
	}
	if s.Policies == nil {
		s.Policies = map[string]domain.RetentionPolicy{}
	}
	if s.Events == nil {
		s.Events = map[string]domain.Event{}
	}
	if s.Receipts == nil {
		s.Receipts = map[string]domain.EventReceipt{}
	}
	if s.Streams == nil {
		s.Streams = map[string]StreamState{}
	}
	if s.Segments == nil {
		s.Segments = map[string][]domain.Segment{}
	}
	if s.Checkpoints == nil {
		s.Checkpoints = map[string][]domain.Checkpoint{}
	}
	if s.LegalHolds == nil {
		s.LegalHolds = map[string]domain.LegalHold{}
	}
	if s.Exports == nil {
		s.Exports = map[string]domain.ExportJob{}
	}
	if s.RestoreRuns == nil {
		s.RestoreRuns = map[string]domain.RestoreRun{}
	}
	if s.AdminActions == nil {
		s.AdminActions = []domain.AdminAction{}
	}
	if s.AggregateCheckpoints == nil {
		s.AggregateCheckpoints = []domain.AggregateCheckpoint{}
	}
	if s.ArchiveConflictFailures == nil {
		s.ArchiveConflictFailures = map[string]int{}
	}
}

// Backend persists the control-plane state snapshot. Save must be atomic:
// on error the previously persisted snapshot stays authoritative. A Save
// returning nil additionally implies the renamed snapshot entry is durable
// against power failure: the parent-directory chain of the renamed entry is
// fsynced after the rename (M-12), so a crash immediately after Save cannot
// revert or lose the committed snapshot.
type Backend interface {
	// Load returns the current snapshot for read-only access. The returned
	// snapshot is owned by the backend and must not be retained. Load must
	// not mutate backend state: concurrent readers share one backend.
	Load() (*Snapshot, error)
	// LoadForUpdate returns a private copy for a read-modify-write cycle so
	// a failing closure can never corrupt the shared state; the copy is only
	// committed through Save. It also establishes the optimistic-lock
	// baseline for the subsequent Save; callers must not retain the
	// returned snapshot.
	LoadForUpdate() (*Snapshot, error)
	// Save atomically persists data. A nil return means the rename is
	// durable (parent-directory chain fsynced after rename).
	Save(data *Snapshot) error
}

type Store struct {
	mu      sync.RWMutex
	backend Backend
}

// Open returns a store backed by the local snapshot file. An empty path
// keeps all state in memory only.
func Open(path string) (*Store, error) {
	backend, err := openFileBackend(path)
	if err != nil {
		return nil, err
	}
	return &Store{backend: backend}, nil
}

// OpenPostgres returns a store whose snapshot lives in the single-row
// audit_state_snapshot table. Multiple replicas can share the row; the
// optimistic version check surfaces lost-update conflicts instead of
// silently overwriting each other.
func OpenPostgres(db *sql.DB) (*Store, error) {
	return &Store{backend: &postgresBackend{db: db}}, nil
}

// NewWithBackend builds a Store over an arbitrary Backend. It is a test
// seam so service-level tests can script optimistic-lock conflict
// sequences; production code uses Open or OpenPostgres.
func NewWithBackend(b Backend) *Store { return &Store{backend: b} }

func (s *Store) Read(fn func(*Snapshot) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := s.backend.Load()
	if err != nil {
		return err
	}
	return fn(data)
}

// snapshotRestorer is no longer needed: LoadForUpdate hands every Update
// closure a private copy, so failures cannot leak into shared state.

// snapshotConflictRetries bounds the optimistic-lock retry loop in Update.
// Postgres-backed stores share the single audit_state_snapshot row across
// replicas; a concurrent writer bumps the version between our LoadForUpdate
// and Save. The mutation closure is re-run on the fresh snapshot, so the
// retry is only sound because nothing was committed on a conflict (Save
// failed atomically).
const snapshotConflictRetries = 3

// snapshotConflictBackoff returns a bounded jittered delay for retry
// attempt n (0-based): base 5ms, doubling, capped at 25ms, plus up to 5ms
// of jitter so concurrent replicas do not retry in lockstep.
func snapshotConflictBackoff(attempt int) time.Duration {
	base := time.Duration(5*(1<<min(attempt, 3))) * time.Millisecond
	if base > 25*time.Millisecond {
		base = 25 * time.Millisecond
	}
	return base + time.Duration(rand.IntN(5_000_000))
}

// Update runs fn on a private snapshot copy and persists the result. When
// the backend reports ErrSnapshotConflict (concurrent writer won the
// optimistic version check), Update re-loads the fresh snapshot and re-runs
// fn up to snapshotConflictRetries times with bounded jitter. Other errors
// (including errors returned by fn itself) are returned immediately: they
// are not conflicts and must not be retried.
func (s *Store) Update(fn func(*Snapshot) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var lastErr error
	for attempt := 0; attempt <= snapshotConflictRetries; attempt++ {
		data, err := s.backend.LoadForUpdate()
		if err != nil {
			return err
		}
		if err := fn(data); err != nil {
			return err
		}
		if err := s.backend.Save(data); err != nil {
			if errors.Is(err, ErrSnapshotConflict) && attempt < snapshotConflictRetries {
				lastErr = err
				time.Sleep(snapshotConflictBackoff(attempt))
				continue
			}
			return err
		}
		return nil
	}
	return lastErr
}

// UpdateChecked is Update with explicit mutation reporting. The closure
// returns (mutated, error); the snapshot is persisted ONLY when mutated is
// true. Optimistic-lock semantics are identical to Update: on
// ErrSnapshotConflict the closure is re-run against the fresh snapshot (up
// to snapshotConflictRetries times, same jittered backoff), and the mutated
// flag of the run that commits decides persistence. Closures must return
// true exactly when they changed the snapshot — returning false after
// mutating silently discards the change. Intended for idempotent worker
// passes that can cheaply prove a no-op (no writes on an idle tick);
// ordinary mutations should keep using Update.
func (s *Store) UpdateChecked(fn func(*Snapshot) (bool, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var lastErr error
	for attempt := 0; attempt <= snapshotConflictRetries; attempt++ {
		data, err := s.backend.LoadForUpdate()
		if err != nil {
			return err
		}
		mutated, err := fn(data)
		if err != nil {
			return err
		}
		if !mutated {
			// No-op pass: no Save, no version bump, no updated_at, no byte
			// change. An idle worker tick must not rewrite the snapshot row.
			return nil
		}
		if err := s.backend.Save(data); err != nil {
			if errors.Is(err, ErrSnapshotConflict) && attempt < snapshotConflictRetries {
				lastErr = err
				time.Sleep(snapshotConflictBackoff(attempt))
				continue
			}
			return err
		}
		return nil
	}
	return lastErr
}

// Ready probes the persistence backend. File-backed stores are always ready;
// the Postgres backend pings its connection so /readyz can surface an
// unavailable control plane before requests start failing.
func (s *Store) Ready(ctx context.Context) error {
	if probe, ok := s.backend.(interface{ Ready(context.Context) error }); ok {
		return probe.Ready(ctx)
	}
	return nil
}

// Flush is kept for compatibility; every backend persists synchronously on
// Save, so there is nothing pending to flush.
func (s *Store) Flush() error { return nil }

// Close releases backend resources (for example the PostgreSQL connection
// pool, or the file backend's exclusive advisory lock on <path>.lock).
func (s *Store) Close() error {
	if closer, ok := s.backend.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// Snapshot returns a deep copy of the current persisted state.
func (s *Store) Snapshot() (*Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := s.backend.Load()
	if err != nil {
		return nil, err
	}
	return cloneSnapshot(data)
}

// CheckFileToPostgresMigrationHazard reports whether switching the given
// file-backed state to PostgreSQL would silently start an empty ledger. The
// PostgreSQL backend maps a missing row to an empty snapshot (a fresh
// deployment is legitimate), so an existing file ledger with events would
// silently "forget" every event on switch-over. Callers must refuse to
// start when this returns an error unless the operator explicitly opts in.
func CheckFileToPostgresMigrationHazard(statePath string) error {
	if statePath == "" {
		return nil
	}
	contents, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("check file state before postgres switch: %w", err)
	}
	if len(bytes.TrimSpace(contents)) == 0 {
		return nil
	}
	var data Snapshot
	if err := json.Unmarshal(contents, &data); err != nil {
		// Not our snapshot format: the file backend will report the real
		// error on Open; do not guess here.
		return nil
	}
	if len(data.Events) > 0 {
		return fmt.Errorf("file state %s holds %d events; switching to PostgreSQL would start an empty ledger — migrate the snapshot first (import tool or controlled cutover), or set AUDIT_ALLOW_PG_EMPTY_LEDGER=true to override", statePath, len(data.Events))
	}
	return nil
}

// fileBackend keeps the snapshot in memory and atomically writes it to
// path on Save.
type fileBackend struct {
	mu   sync.RWMutex
	data *Snapshot
	path string

	// lock is the exclusive advisory flock on <path>.lock, held for the
	// backend's lifetime and released by Close (the kernel also releases it
	// on process death — no stale-lock cleanup). Nil for in-memory backends
	// (path == "") and for direct test constructions.
	lock io.Closer

	// syncDir is the per-directory fsync hook used by Save's post-rename
	// durability step (mirrors archive.FileStore.syncDir). It defaults to
	// fsutil.SyncDir; tests replace it to record the ordered sync set or
	// fault-inject a specific path. Nil at production construction
	// (openFileBackend), so there is no exported surface change.
	syncDir func(path string) error
}

// openFileBackend builds a file-backed store. For a non-empty path it first
// creates the parent directory (0o750, the same mode Save uses) and then
// acquires the process-lifetime exclusive advisory flock on <path>.lock, so
// a second live writer on the same state path (another audit-api or
// audit-governance-worker) is refused at open with ErrStateFileLocked — the
// stale-load/clobber cycle is unreachable by construction. The lock is
// released on any post-acquisition error path so a failed Open never leaks
// it, and by Close (or process death) for a successful Open. In-memory mode
// (path == "") acquires no lock and touches no file.
func openFileBackend(path string) (*fileBackend, error) {
	f := &fileBackend{data: NewSnapshot(), path: path}
	if path == "" {
		return f, nil // pure in-memory mode: no lock, no directory, no file
	}
	// REQ-1: create the parent before lock acquisition so the lock file can
	// exist before the first Save (Save's own MkdirAll is unchanged).
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create state directory for %s: %w", path, err)
	}
	lock, err := acquireStateLock(path)
	if err != nil {
		return nil, err // ErrStateFileLocked-wrapped on contention
	}
	f.lock = lock
	contents, err := os.ReadFile(path)
	if err == nil && len(contents) > 0 {
		if err := decodeSnapshot(contents, f.data); err != nil {
			_ = lock.Close() // release before returning the decode error
			return nil, err
		}
		f.data.normalize()
	} else if err != nil && !os.IsNotExist(err) {
		_ = lock.Close()
		return nil, err
	}
	return f, nil
}

// Close releases the advisory flock and the lock file descriptor. It is safe
// on backends without a lock (in-memory mode, direct test constructions): a
// nil handle is tolerated. The kernel additionally releases the flock when
// the process dies, so a crashed holder never leaves a stale lock — no
// stale-lock detection or cleanup is needed.
func (f *fileBackend) Close() error {
	if f.lock == nil {
		return nil
	}
	return f.lock.Close()
}

func (f *fileBackend) Load() (*Snapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.data, nil
}

func (f *fileBackend) LoadForUpdate() (*Snapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return cloneSnapshot(f.data)
}

func (f *fileBackend) Save(data *Snapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.path == "" {
		f.data = data
		return nil
	}
	parent := filepath.Dir(f.path)
	// REQ-2: chain root = deepest ancestor that pre-exists this Save
	// (probed BEFORE MkdirAll so freshly created intermediates are included
	// in the post-rename sync chain). The snapshot has no configured root
	// directory (unlike archive's FileStore.Dir), so the chain terminates
	// at the deepest pre-existing directory: ancestors above it already
	// have durable dentries and must not be re-synced (fsutil_test.go pins
	// the root==dir collapse to exactly one sync).
	root := deepestExistingAncestor(parent)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	// fsync before rename: the snapshot is the control-plane source of
	// truth, so a crash after rename must not leave an empty or partial
	// state file behind. Any write/sync error removes the temp file
	// best-effort, keeping the invariant: Save returned an error ⇒ the
	// previously persisted snapshot stays authoritative (in memory, and —
	// best-effort — on disk); the target never holds a torn snapshot; no
	// .tmp remains (mirrors FileStore.Put).
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	if _, err := file.Write(encoded); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return err
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
	if err := os.Rename(tmp, f.path); err != nil {
		return err
	}
	// The renamed entry must be durable before Save reports success (M-12,
	// the same crash window the archive package closed): a power failure
	// after rename can otherwise revert or lose the snapshot's directory
	// entry, silently rolling back committed ledger state. Sync the
	// parent-directory chain leaf-to-root; f.data is updated only after the
	// chain sync succeeds.
	syncDir := f.syncDir
	if syncDir == nil {
		syncDir = fsutil.SyncDir
	}
	if err := fsutil.SyncDirChain(root, parent, syncDir); err != nil {
		// Chain sync failed: the rename already replaced the target, so
		// best-effort restore the previously persisted snapshot to the
		// target path ("no rename visible" — a fresh Open must load the
		// previously persisted snapshot). f.data still holds the previous
		// snapshot; it is only replaced below. Restore failures are
		// best-effort: the original chain-sync error is returned
		// regardless, and the restore's write+sync-before-rename guarantees
		// the target never holds a torn snapshot and no .tmp remains.
		f.restoreSnapshot()
		return fmt.Errorf("sync directory chain for %s: %w", f.path, err)
	}
	f.data = data
	return nil
}

// deepestExistingAncestor returns the deepest directory on the path to dir
// that already exists before this Save runs (walking upward with os.Stat;
// the filesystem root always exists and terminates the walk). Only these
// directories have durable dentries already; every directory between the
// returned root and dir is created by this Save and must itself be synced
// after the rename. A Stat error other than "confirmed present" is treated
// as not-existing: over-syncing a pre-existing ancestor is harmless (one
// extra fsync), while under-syncing a freshly created intermediate would
// reopen the M-12 window — the probe can therefore never compromise
// durability.
func deepestExistingAncestor(dir string) string {
	for d := filepath.Clean(dir); ; d = filepath.Dir(d) {
		if _, err := os.Stat(d); err == nil {
			return d
		}
		parent := filepath.Dir(d)
		if parent == d {
			return d // filesystem root always exists
		}
	}
}

// restoreSnapshot rewrites the previously persisted snapshot over the
// target path (best-effort "no rename visible" after a post-rename
// chain-sync failure in Save). It follows the same write → file.Sync →
// close → rename sequence as Save so the target never holds a torn
// snapshot, and finishes with a best-effort chain sync of its own. Every
// error is swallowed: the caller already returns the original chain-sync
// error, and the worst case is the target holding the new (complete,
// parseable) snapshot from the original rename — the in-memory authority
// (f.data) stays the previously persisted snapshot either way.
func (f *fileBackend) restoreSnapshot() {
	prev := f.data
	if prev == nil {
		prev = NewSnapshot()
	}
	encoded, err := json.MarshalIndent(prev, "", "  ")
	if err != nil {
		return
	}
	tmp := f.path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
	if err != nil {
		return
	}
	if _, err := file.Write(encoded); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmp)
		return
	}
	if err := os.Rename(tmp, f.path); err != nil {
		_ = os.Remove(tmp)
		return
	}
	syncDir := f.syncDir
	if syncDir == nil {
		syncDir = fsutil.SyncDir
	}
	_ = fsutil.SyncDirChain(deepestExistingAncestor(filepath.Dir(f.path)), filepath.Dir(f.path), syncDir)
}

func cloneSnapshot(data *Snapshot) (*Snapshot, error) {
	encoded, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	copyData := NewSnapshot()
	if err := decodeSnapshot(encoded, copyData); err != nil {
		return nil, err
	}
	copyData.normalize()
	return copyData, nil
}

// decodeSnapshot decodes a persisted snapshot with UseNumber so payload
// numbers survive the reload as json.Number instead of collapsing through
// float64. Digest re-derivation after a restart (VerifyIntegrity, dedupe)
// must see the exact digits the event was ingested with.
func decodeSnapshot(encoded []byte, data *Snapshot) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	return decoder.Decode(data)
}

// KeySeparator joins composite snapshot keys. It must not be NUL: Go JSON
// encoding escapes NUL as \u0000, which the PostgreSQL jsonb parser rejects.
// 0x1F (unit separator) round-trips through both JSON and jsonb.
const KeySeparator = "\x1f"

func TenantKey(tenantID string) string { return tenantID }

func SourceKey(tenantID, sourceID string) string { return tenantID + KeySeparator + sourceID }

func SchemaKey(tenantID, schemaID string, version int) string {
	return tenantID + KeySeparator + schemaID + KeySeparator + strconv.Itoa(version)
}

func EventKey(tenantID, eventID string) string { return tenantID + KeySeparator + eventID }

func StreamKey(tenantID, streamID string) string { return tenantID + KeySeparator + streamID }

// ValidTenantID validates a tenant identifier against the key-framing
// invariant. The critical rule is rejection of KeySeparator (0x1F): a tenant
// ID containing it makes composite keys ambiguous with another tenant's keys
// (StreamKey("a\x1fb","s") == StreamKey("a","b\x1fs")). Control
// characters, whitespace and path separators are rejected as hygiene (URLs,
// logs, archive paths). Delegates to domain.ValidKeyComponent so the charset
// rule has exactly one implementation across every untrusted API boundary.
// Returns a domain.ErrInvalid-wrapped error, or nil.
func ValidTenantID(id string) error {
	return domain.ValidKeyComponent("tenant id", id)
}

// SplitTenantKey splits a composite key into its tenant prefix and the
// remaining component. ok is false unless the key contains exactly one
// KeySeparator; keys with zero or multiple separators are attributed to no
// tenant (fail-closed). rest may be empty (StreamKey(t, "")).
func SplitTenantKey(key string) (tenantID, rest string, ok bool) {
	tenantID, rest, found := strings.Cut(key, KeySeparator)
	if !found || strings.Contains(rest, KeySeparator) {
		return "", "", false
	}
	return tenantID, rest, true
}
