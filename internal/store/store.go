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
	"sync/atomic"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/fsutil"
)

// ErrSnapshotConflict is returned when a concurrent instance persisted the
// state snapshot after this instance loaded it. The optimistic version check
// prevents lost updates when multiple audit-api replicas share one
// PostgreSQL snapshot row.
var ErrSnapshotConflict = errors.New("state snapshot changed concurrently")

// ErrOperationDeadline is returned when a store operation exhausted the
// deadline it was started with (Options.OperationTimeout, or the caller
// context's own earlier deadline). The error wraps context.DeadlineExceeded,
// so errors.Is matches both this sentinel and the standard context error.
//
// It is the "result unknown" class, not a fault class: a statement that was
// aborted mid-flight may or may not have committed, so callers must treat it
// exactly like ErrSnapshotConflict and let the idempotent retry converge. It
// is deliberately distinct from context.Canceled, which propagates unchanged
// because a cancelled caller has nothing to report.
var ErrOperationDeadline = errors.New("store operation deadline exceeded")

// DefaultOperationTimeout is the shipped per-operation bound when
// Options.OperationTimeout is zero. It is a starting default, not an
// operator-mandated value: every serving binary can override it
// (AUDIT_STORE_OPERATION_TIMEOUT).
//
// The bound is per store operation — one tenant's bounded write set, or one
// control-plane document — not per request, and it is deliberately far larger
// than a PostgreSQL round trip (BenchmarkIngest is ~1.3 ms in-process). It is
// also strictly below the 30s HTTP WriteTimeout of audit-api so that the
// retryable 503 classification can still be written to the client; the
// binary validates that relationship at startup.
const DefaultOperationTimeout = 10 * time.Second

// Options carries the operational knobs a Store needs at construction. The
// zero value is the shipped default, so Options{} keeps the pre-existing
// behaviour and no setter exists (see OpenWithControlWriteFault for the same
// constructor-parameter seam).
type Options struct {
	// OperationTimeout bounds every store operation. Zero selects
	// DefaultOperationTimeout; a negative value is rejected by the
	// constructors rather than silently disabling the bound.
	OperationTimeout time.Duration
}

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
	// LayoutVersion identifies the persistence layout. Version 0 is the
	// original single-document shape; version 2 keeps the control plane in
	// this snapshot while tenant hot data and immutable ledger metadata live
	// in the split tier. The field is additive so old snapshots remain
	// decodable and can be migrated on the next file-store open.
	LayoutVersion     int                               `json:"layout_version,omitempty"`
	Tenants           map[string]domain.Tenant          `json:"tenants"`
	Sources           map[string]domain.SourceSystem    `json:"sources"`
	Schemas           map[string]domain.EventSchema     `json:"schemas"`
	Policies          map[string]domain.RetentionPolicy `json:"policies"`
	Events            map[string]domain.Event           `json:"events"`
	Receipts          map[string]domain.EventReceipt    `json:"receipts"`
	Streams           map[string]StreamState            `json:"streams"`
	Segments          map[string][]domain.Segment       `json:"segments"`
	Checkpoints       map[string][]domain.Checkpoint    `json:"checkpoints"`
	LegalHolds        map[string]domain.LegalHold       `json:"legal_holds"`
	Exports           map[string]domain.ExportJob       `json:"exports"`
	ExportLeases      map[string]ExportLease            `json:"export_leases,omitempty"`
	ExportCheckpoints map[string]ExportCheckpoint       `json:"export_checkpoints,omitempty"`
	ExportPlans       map[string]ExportPlan             `json:"export_plans,omitempty"`
	RestoreRuns       map[string]domain.RestoreRun      `json:"restore_runs"`
	// LedgeredOutbox retains post-commit projection publications until a
	// broker acknowledges them. It is intentionally separate from Events: the
	// immutable event remains the source of truth while this queue is a
	// rebuildable delivery aid. Old snapshots decode nil and normalize it.
	LedgeredOutbox       map[string]domain.Event      `json:"ledgered_outbox,omitempty"`
	AdminActions         []domain.AdminAction         `json:"admin_actions"`
	AggregateCheckpoints []domain.AggregateCheckpoint `json:"aggregate_checkpoints"`
	// ArchiveConflictFailures counts consecutive archive passes aborted by
	// optimistic-lock exhaustion, per tenant. Reset to zero by the batch
	// receipt commit in ArchivePending; incremented best-effort by the
	// worker when a pass fails with ErrSnapshotConflict. It lives inside
	// the snapshot document, so old snapshots decode as nil and normalize
	// to an empty map — no migration is needed.
	ArchiveConflictFailures map[string]int `json:"archive_conflict_failures,omitempty"`
	// DeadLetters records per-tenant entries that will never archive (FM-1
	// over-limit keys, F-2 byte-conflicting objects), keyed by
	// EventKey(tenantID, eventID) for events and the synthetic segment id
	// ("seg:<stream>:<first>-<last>") for sealed segments, which have no
	// receipt. ArchivePending excludes them from the retry set. Old
	// snapshots decode as nil and normalize to an empty map — no migration.
	DeadLetters map[string]domain.DeadLetter `json:"dead_letters,omitempty"`
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
		ExportLeases:            map[string]ExportLease{},
		ExportCheckpoints:       map[string]ExportCheckpoint{},
		ExportPlans:             map[string]ExportPlan{},
		RestoreRuns:             map[string]domain.RestoreRun{},
		LedgeredOutbox:          map[string]domain.Event{},
		AdminActions:            []domain.AdminAction{},
		AggregateCheckpoints:    []domain.AggregateCheckpoint{},
		ArchiveConflictFailures: map[string]int{},
		DeadLetters:             map[string]domain.DeadLetter{},
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
	if s.ExportLeases == nil {
		s.ExportLeases = map[string]ExportLease{}
	}
	if s.ExportCheckpoints == nil {
		s.ExportCheckpoints = map[string]ExportCheckpoint{}
	}
	if s.ExportPlans == nil {
		s.ExportPlans = map[string]ExportPlan{}
	}
	if s.RestoreRuns == nil {
		s.RestoreRuns = map[string]domain.RestoreRun{}
	}
	if s.LedgeredOutbox == nil {
		s.LedgeredOutbox = map[string]domain.Event{}
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
	if s.DeadLetters == nil {
		s.DeadLetters = map[string]domain.DeadLetter{}
	}
}

// Backend persists the control-plane state snapshot. Save must be atomic:
// on error the previously persisted snapshot stays authoritative. A Save
// returning nil additionally implies the renamed snapshot entry is durable
// against power failure: the parent-directory chain of the renamed entry is
// fsynced after the rename (M-12), so a crash immediately after Save cannot
// revert or lose the committed snapshot.
type Backend interface {
	// Load returns an independent caller-owned snapshot for read-only access.
	// Load must not mutate backend state: concurrent readers share one
	// backend. ctx is the store operation context: a backend must abandon the
	// statement and return ctx.Err() once it ends.
	Load(ctx context.Context) (*Snapshot, error)
	// LoadForUpdate returns a private copy for a read-modify-write cycle so
	// a failing closure can never corrupt the shared state; the copy is only
	// committed through Save. It also establishes the optimistic-lock
	// baseline for the subsequent Save; callers must not retain the
	// returned snapshot.
	LoadForUpdate(ctx context.Context) (*Snapshot, error)
	// Save atomically persists a copy of data. A nil return means the rename
	// is durable (parent-directory chain fsynced after rename).
	Save(ctx context.Context, data *Snapshot) error
}

type Store struct {
	mu      sync.RWMutex
	backend Backend
	split   *splitStore
	pgSplit *postgresSplitStore
	// publications is used only by in-memory/test backends. Durable backends
	// use indexed rows or addressed files, never the shared control snapshot.
	publications map[string]publicationConfirmation
	// publicationSyncDir is nil in production. Tests can reproduce a rename
	// whose ancestor directory fsync fails without changing global hooks.
	publicationSyncDir func(string) error
	// Query artifacts have a separate lock and never rewrite ledger/control.
	queryMu        sync.Mutex
	querySnapshots map[string][]byte

	// opts holds the resolved operation timeout (see Options). pool is the
	// PostgreSQL handle for this store, or nil for file/in-memory stores;
	// PoolStats is the only reader and it never takes mu.
	opts Options
	pool *sql.DB
	// deadlineExhaustions is the unlabelled monotonic counter exported as
	// audit_store_operation_deadline_exceeded_total. Atomic so /metrics can
	// read it while an operation is in flight.
	deadlineExhaustions atomic.Uint64
}

// Open returns a store backed by the local snapshot file. An empty path
// keeps all state in memory only.
func Open(path string) (*Store, error) { return OpenWithOptions(path, Options{}) }

// OpenWithOptions is Open with an explicit Options value. A negative
// OperationTimeout is rejected: an operator misconfiguration must fail at
// startup, not silently remove the interruptible bound.
func OpenWithOptions(path string, opts Options) (*Store, error) {
	if opts.OperationTimeout < 0 {
		return nil, errors.New("store: operation timeout must not be negative")
	}
	backend, err := openFileBackend(path)
	if err != nil {
		return nil, err
	}
	split, err := newSplitStore(backend.data, path)
	if err != nil {
		_ = backend.Close()
		return nil, err
	}
	return &Store{backend: backend, split: split, opts: opts}, nil
}

// ControlWriteFault is consulted immediately before the v2 file store makes a
// durable control-plane write. It is a test seam in the same shape as Backend:
// the production constructors leave it nil and every write site pays one nil
// check. A hook is armed and disarmed by the test through the closure it
// installs, so there is no setter and no arm/disarm API.
type ControlWriteFault func() error

// OpenWithControlWriteFault opens the same v2 file split store as Open and
// installs fault on every durable control-plane write of that store: the
// control delta of a tenant commit and Store.UpdateControl. At a tenant commit
// the hot document and the cold ledger are already durable when the control
// document is written, so a fault here reproduces exactly the window a crash
// between those steps leaves behind — a committed event with no durable
// control-plane delta. A nil fault is rejected so a mis-wired seam fails loudly
// instead of being silently inert.
func OpenWithControlWriteFault(path string, fault ControlWriteFault) (*Store, error) {
	if fault == nil {
		return nil, errors.New("store: control write fault seam requires a non-nil hook")
	}
	backend, err := openFileBackend(path)
	if err != nil {
		return nil, err
	}
	split, err := newSplitStore(backend.data, path)
	if err != nil {
		_ = backend.Close()
		return nil, err
	}
	split.controlWriteFault = fault
	return &Store{backend: backend, split: split}, nil
}

// OpenWithLedgerWriteFault is an instance-local test seam at the HotFirst
// cold append boundary. Production constructors always leave the hook nil.
func OpenWithLedgerWriteFault(path string, fault func() error) (*Store, error) {
	if fault == nil {
		return nil, errors.New("store: ledger write fault requires a hook")
	}
	st, err := Open(path)
	if err != nil {
		return nil, err
	}
	st.split.ledgerWriteFault = fault
	return st, nil
}

// OpenPostgres returns a store whose snapshot lives in the single-row
// audit_state_snapshot table. Multiple replicas can share the row; the
// optimistic version check surfaces lost-update conflicts instead of
// silently overwriting each other.
func OpenPostgres(db *sql.DB) (*Store, error) { return OpenPostgresWithOptions(db, Options{}) }

// OpenPostgresWithOptions is OpenPostgres with an explicit Options value. The
// *sql.DB is retained so /metrics can publish database/sql pool statistics
// (PoolStats) without taking Store.mu or issuing a statement.
func OpenPostgresWithOptions(db *sql.DB, opts Options) (*Store, error) {
	if opts.OperationTimeout < 0 {
		return nil, errors.New("store: operation timeout must not be negative")
	}
	backend := &postgresBackend{db: db}
	return &Store{backend: backend, pgSplit: newPostgresSplitStore(backend), opts: opts, pool: db}, nil
}

// NewWithBackend builds a Store over an arbitrary Backend. It is a test
// seam so service-level tests can script optimistic-lock conflict
// sequences; production code uses Open or OpenPostgres.
func NewWithBackend(b Backend) *Store { return NewWithBackendOptions(b, Options{}) }

// NewWithBackendOptions is NewWithBackend with an explicit Options value.
func NewWithBackendOptions(b Backend, opts Options) *Store {
	return &Store{backend: b, opts: opts}
}

func (s *Store) Read(ctx context.Context, fn func(*Snapshot) error) (err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	return s.read(op.ctx, fn)
}

// read is Read's body. It is the shared entry point for every nested store
// call, so it must never re-derive the operation context: only the exported
// operation boundary (Read/Update/…) classifies the deadline.
func (s *Store) read(ctx context.Context, fn func(*Snapshot) error) error {
	if s.split != nil {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.split.read(ctx, fn)
	}
	if s.pgSplit != nil {
		enabled, err := s.pgSplit.enabled(ctx)
		if err != nil {
			return err
		}
		if enabled {
			s.mu.RLock()
			defer s.mu.RUnlock()
			return s.pgSplit.read(ctx, fn)
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := s.backend.Load(ctx)
	if err != nil {
		return err
	}
	copyData, err := cloneSnapshot(data)
	if err != nil {
		return err
	}
	return fn(copyData)
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
func (s *Store) Update(ctx context.Context, fn func(*Snapshot) error) (err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	return s.update(op.ctx, fn)
}

// update is Update's body; see read for the nesting rule.
func (s *Store) update(ctx context.Context, fn func(*Snapshot) error) error {
	if s.split != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.split.update(ctx, fn)
	}
	if s.pgSplit != nil {
		enabled, err := s.pgSplit.enabled(ctx)
		if err != nil {
			return err
		}
		if enabled {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.pgSplit.update(ctx, fn)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updateLocked(ctx, fn)
}

// updateLocked is Update's retry loop without the lock; callers must hold
// s.mu exclusively. It exists so AppendAdminFact's capability-less fallback
// can reuse the exact optimistic-lock semantics without re-entering Update
// and deadlocking on the same lock (F-1).
func (s *Store) updateLocked(ctx context.Context, fn func(*Snapshot) error) error {
	var lastErr error
	for attempt := 0; attempt <= snapshotConflictRetries; attempt++ {
		data, err := s.backend.LoadForUpdate(ctx)
		if err != nil {
			return err
		}
		if err := fn(data); err != nil {
			return err
		}
		if err := s.backend.Save(ctx, data); err != nil {
			if errors.Is(err, ErrSnapshotConflict) && attempt < snapshotConflictRetries {
				lastErr = err
				if err := sleepBackoff(ctx, attempt); err != nil {
					return err
				}
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
func (s *Store) UpdateChecked(ctx context.Context, fn func(*Snapshot) (bool, error)) (err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	return s.updateChecked(op.ctx, fn)
}

// updateChecked is UpdateChecked's body; see read for the nesting rule.
func (s *Store) updateChecked(ctx context.Context, fn func(*Snapshot) (bool, error)) error {
	if s.split != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.split.updateChecked(ctx, fn)
	}
	if s.pgSplit != nil {
		enabled, err := s.pgSplit.enabled(ctx)
		if err != nil {
			return err
		}
		if enabled {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.pgSplit.updateChecked(ctx, fn)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var lastErr error
	for attempt := 0; attempt <= snapshotConflictRetries; attempt++ {
		data, err := s.backend.LoadForUpdate(ctx)
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
		if err := s.backend.Save(ctx, data); err != nil {
			if errors.Is(err, ErrSnapshotConflict) && attempt < snapshotConflictRetries {
				lastErr = err
				if err := sleepBackoff(ctx, attempt); err != nil {
					return err
				}
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
func (s *Store) Snapshot(ctx context.Context) (_ *Snapshot, err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	return s.snapshot(op.ctx)
}

// snapshot is Snapshot's body; see read for the nesting rule.
func (s *Store) snapshot(ctx context.Context) (*Snapshot, error) {
	if s.split != nil {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.split.materialize(ctx)
	}
	if s.pgSplit != nil {
		enabled, err := s.pgSplit.enabled(ctx)
		if err != nil {
			return nil, err
		}
		if enabled {
			s.mu.RLock()
			defer s.mu.RUnlock()
			return s.pgSplit.materialize(ctx)
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := s.backend.Load(ctx)
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
	contents, err := fsutil.ReadDataFile(statePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
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
	if len(data.Events) > 0 || len(data.Receipts) > 0 {
		return fmt.Errorf("file state %s holds %d events and %d receipts; switching to PostgreSQL would start an empty ledger — migrate the snapshot first (import tool or controlled cutover), or set AUDIT_ALLOW_PG_EMPTY_LEDGER=true to override", statePath, len(data.Events), len(data.Receipts))
	}
	hotEvents, ledgerRecords, err := splitFileEvidence(filepath.Dir(statePath))
	if err != nil {
		return fmt.Errorf("inspect split file state before postgres switch: %w", err)
	}
	if hotEvents > 0 || ledgerRecords > 0 {
		return fmt.Errorf("file state %s holds %d hot events and %d cold ledger records in the split layout; switching to PostgreSQL would start an empty ledger — migrate the snapshot first (import tool or controlled cutover), or set AUDIT_ALLOW_PG_EMPTY_LEDGER=true to override", statePath, hotEvents, ledgerRecords)
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

	// Read self-audit trail state (trail.go). Append-only; serialized by f.mu
	// (never Store.mu — F-2), so reads never block ingests. File mode:
	// <path>.admin-trail.jsonl via a persistent O_APPEND fd. In-memory mode
	// (path == ""): a plain bounded slice.
	trailPath    string               // path + ".admin-trail.jsonl" (file mode)
	trailFile    *os.File             // persistent append fd; lazily opened
	trailCount   int64                // baseline-seeded append count (F-4)
	trailCounted bool                 // baseline scan done
	trail        []domain.AdminAction // in-memory mode trail

	// trailCompactions counts compaction rewrites that actually replaced the
	// trail file; it is incremented only after os.Rename succeeds, so a
	// no-op compaction and a pre-rename fault are not counted. Guarded by f.mu
	// (compactTrail holds it exclusively). Read only by same-package tests to
	// pin the amortized watermark schedule — not exported, not a metric.
	trailCompactions int64

	// Close serializes with trail IO under mu, closes the persistent trail
	// before releasing flock, and remembers both close outcomes. A closed
	// trail must never reopen after another file writer acquires that lock.
	closed   bool
	closeErr error
}

// openFileBackend builds a file-backed store. For a non-empty path it first
// creates a private parent directory (0700 for newly created components) and then
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
	if err := fsutil.MakeDataDir(filepath.Dir(path)); err != nil {
		return nil, fmt.Errorf("create state directory for %s: %w", path, err)
	}
	lock, err := acquireStateLock(path)
	if err != nil {
		return nil, err // ErrStateFileLocked-wrapped on contention
	}
	f.lock = lock
	contents, err := fsutil.ReadDataFile(path)
	if err == nil && len(contents) > 0 {
		if err := decodeSnapshot(contents, f.data); err != nil {
			_ = lock.Close() // release before returning the decode error
			return nil, err
		}
		f.data.normalize()
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = lock.Close()
		return nil, err
	}
	return f, nil
}

// Close waits for in-flight trail IO, closes its persistent descriptor, then
// releases the advisory flock even if trail close failed. Each descriptor is
// consumed once; repeated Close preserves the original combined outcome.
// Callers still drain service work before closing the store: this is not a
// replacement for API/worker shutdown or fencing of every split-store method.
func (f *fileBackend) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return f.closeErr
	}
	f.closed = true
	if f.trailFile != nil {
		f.closeErr = f.trailFile.Close()
		f.trailFile = nil
	}
	if f.lock != nil {
		f.closeErr = errors.Join(f.closeErr, f.lock.Close())
		f.lock = nil
	}
	return f.closeErr
}

func (f *fileBackend) Load(ctx context.Context) (*Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return cloneSnapshot(f.data)
}

func (f *fileBackend) LoadForUpdate(ctx context.Context) (*Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return cloneSnapshot(f.data)
}

func (f *fileBackend) Save(ctx context.Context, data *Snapshot) error {
	// The file backend honours ctx exactly once, before the durability chain
	// starts. Interrupting between write / fsync / rename / dir-chain fsync
	// would be strictly worse than finishing: the "Save returned an error ⇒
	// previous snapshot authoritative" invariant and the M-12 rename
	// durability both depend on running that chain to completion.
	if err := ctx.Err(); err != nil {
		return err
	}
	copyData, err := cloneSnapshot(data)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.path == "" {
		f.data = copyData
		return nil
	}
	encoded, err := json.MarshalIndent(copyData, "", "  ")
	if err != nil {
		return err
	}
	// Pin the parent for temp creation, rename and cleanup. An unknown old
	// .tmp is never truncated/deleted. Only update memory after file and full
	// directory-chain fsync; retain the prior post-rename restore contract.
	if err := fsutil.AtomicDataFile(f.path, encoded, fsutil.PrivateFileMode, f.syncDir); err != nil {
		if errors.Is(err, fsutil.ErrDataDirectorySync) {
			f.restoreSnapshot()
			return fmt.Errorf("sync directory chain for %s: %w", f.path, err)
		}
		return err
	}
	f.data = copyData
	return nil
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
	_ = fsutil.AtomicDataFile(f.path, encoded, fsutil.PrivateFileMode, f.syncDir)
}

type snapshotJSONCopy struct {
	LayoutVersion           int                               `json:"layout_version,omitempty"`
	Tenants                 map[string]domain.Tenant          `json:"tenants"`
	Sources                 map[string]domain.SourceSystem    `json:"sources"`
	Schemas                 map[string]domain.EventSchema     `json:"schemas"`
	Policies                map[string]domain.RetentionPolicy `json:"policies"`
	Receipts                map[string]domain.EventReceipt    `json:"receipts"`
	Streams                 map[string]StreamState            `json:"streams"`
	Segments                map[string][]domain.Segment       `json:"segments"`
	Checkpoints             map[string][]domain.Checkpoint    `json:"checkpoints"`
	LegalHolds              map[string]domain.LegalHold       `json:"legal_holds"`
	Exports                 map[string]domain.ExportJob       `json:"exports"`
	ExportLeases            map[string]ExportLease            `json:"export_leases,omitempty"`
	ExportCheckpoints       map[string]ExportCheckpoint       `json:"export_checkpoints,omitempty"`
	ExportPlans             map[string]ExportPlan             `json:"export_plans,omitempty"`
	RestoreRuns             map[string]domain.RestoreRun      `json:"restore_runs"`
	AdminActions            []domain.AdminAction              `json:"admin_actions"`
	AggregateCheckpoints    []domain.AggregateCheckpoint      `json:"aggregate_checkpoints"`
	ArchiveConflictFailures map[string]int                    `json:"archive_conflict_failures,omitempty"`
	DeadLetters             map[string]domain.DeadLetter      `json:"dead_letters,omitempty"`
}

func cloneEventMap(events map[string]domain.Event) (map[string]domain.Event, error) {
	if events == nil {
		return nil, nil
	}
	copyEvents := make(map[string]domain.Event, len(events))
	for key, event := range events {
		cloned, err := domain.CloneEvent(event)
		if err != nil {
			return nil, err
		}
		copyEvents[key] = cloned
	}
	return copyEvents, nil
}

func cloneSnapshot(data *Snapshot) (*Snapshot, error) {
	encoded, err := json.Marshal(snapshotJSONCopy{
		LayoutVersion:           data.LayoutVersion,
		Tenants:                 data.Tenants,
		Sources:                 data.Sources,
		Schemas:                 data.Schemas,
		Policies:                data.Policies,
		Receipts:                data.Receipts,
		Streams:                 data.Streams,
		Segments:                data.Segments,
		Checkpoints:             data.Checkpoints,
		LegalHolds:              data.LegalHolds,
		Exports:                 data.Exports,
		ExportLeases:            data.ExportLeases,
		ExportCheckpoints:       data.ExportCheckpoints,
		ExportPlans:             data.ExportPlans,
		RestoreRuns:             data.RestoreRuns,
		AdminActions:            data.AdminActions,
		AggregateCheckpoints:    data.AggregateCheckpoints,
		ArchiveConflictFailures: data.ArchiveConflictFailures,
		DeadLetters:             data.DeadLetters,
	})
	if err != nil {
		return nil, err
	}
	copyData := NewSnapshot()
	if err := decodeSnapshot(encoded, copyData); err != nil {
		return nil, err
	}
	copyData.Events, err = cloneEventMap(data.Events)
	if err != nil {
		return nil, err
	}
	copyData.LedgeredOutbox, err = cloneEventMap(data.LedgeredOutbox)
	if err != nil {
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
// logs, archive paths). ':' is additionally rejected: tenant IDs prefix
// every Event.Stream() frame and are the subject of ':'-delimited dev
// tokens, so an embedded ':' makes both ambiguous (dev-token rebinding
// defect). Delegates to domain.ValidTenantIDComponent so the charset rule
// has exactly one implementation across every untrusted API boundary.
// Returns a domain.ErrInvalid-wrapped error, or nil.
func ValidTenantID(id string) error {
	return domain.ValidTenantIDComponent("tenant id", id)
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
