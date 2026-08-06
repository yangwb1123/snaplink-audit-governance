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
	"unicode"

	"github.com/snaplink/audit-governance/internal/domain"
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
}

func NewSnapshot() *Snapshot {
	return &Snapshot{
		Tenants:              map[string]domain.Tenant{},
		Sources:              map[string]domain.SourceSystem{},
		Schemas:              map[string]domain.EventSchema{},
		Policies:             map[string]domain.RetentionPolicy{},
		Events:               map[string]domain.Event{},
		Receipts:             map[string]domain.EventReceipt{},
		Streams:              map[string]StreamState{},
		Segments:             map[string][]domain.Segment{},
		Checkpoints:          map[string][]domain.Checkpoint{},
		LegalHolds:           map[string]domain.LegalHold{},
		Exports:              map[string]domain.ExportJob{},
		RestoreRuns:          map[string]domain.RestoreRun{},
		AdminActions:         []domain.AdminAction{},
		AggregateCheckpoints: []domain.AggregateCheckpoint{},
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
}

// Backend persists the control-plane state snapshot. Save must be atomic:
// on error the previously persisted snapshot stays authoritative.
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
	// Save atomically persists data.
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
// pool). File-backed stores have nothing to release.
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

// fileBackend keeps the snapshot in memory and atomically writes it to
// path on Save.
type fileBackend struct {
	mu   sync.RWMutex
	data *Snapshot
	path string
}

func openFileBackend(path string) (*fileBackend, error) {
	data := NewSnapshot()
	if path != "" {
		contents, err := os.ReadFile(path)
		if err == nil && len(contents) > 0 {
			if err := decodeSnapshot(contents, data); err != nil {
				return nil, err
			}
			data.normalize()
		} else if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}
	return &fileBackend{data: data, path: path}, nil
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
	if err := os.MkdirAll(filepath.Dir(f.path), 0o750); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, encoded, 0o640); err != nil {
		return err
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return err
	}
	f.data = data
	return nil
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
// logs, archive paths). Returns a domain.ErrInvalid-wrapped error, or nil.
func ValidTenantID(id string) error {
	for _, r := range id {
		switch {
		case unicode.IsControl(r): // includes 0x1F, NUL, CR, LF, TAB, ...
			return fmt.Errorf("%w: tenant id must not contain control characters", domain.ErrInvalid)
		case unicode.IsSpace(r): // \t \n \v \f \r, ' ', U+0085, U+00A0
			return fmt.Errorf("%w: tenant id must not contain whitespace", domain.ErrInvalid)
		case r == '/' || r == '\\':
			return fmt.Errorf("%w: tenant id must not contain path separators", domain.ErrInvalid)
		}
	}
	return nil
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
