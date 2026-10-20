package store

// Short-lived query membership is an operational artifact, not ledger truth.
// It contains coordinates only, lives outside control, and is authenticated by
// the service. Losing it requires a fresh query, never a best-effort live resume.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/fsutil"
)

const (
	MaxQuerySnapshotEvents  = 10000
	MaxActiveQuerySnapshots = 16 // per tenant, shared across PostgreSQL replicas
	MaxQuerySnapshotBytes   = 4 * 1024 * 1024
	QuerySnapshotLifetime   = 15 * time.Minute
)

var ErrQuerySnapshotUnavailable = fmt.Errorf("%w: query snapshot expired or unavailable; restart query", domain.ErrInvalid)
var querySnapshotID = regexp.MustCompile(`^[0-9a-f]{32}$`)

type EventReference struct {
	OccurredAt time.Time `json:"occurred_at,omitempty"`
	EventID    string    `json:"event_id"`
	StreamID   string    `json:"stream_id"`
	Sequence   int64     `json:"sequence"`
	Hash       string    `json:"hash"`
}

type QuerySnapshot struct {
	ID        string           `json:"id"`
	TenantID  string           `json:"tenant_id"`
	Binding   string           `json:"binding"` // keyed scope fingerprint; never filters/digests
	Source    string           `json:"source"`
	CreatedAt time.Time        `json:"created_at"`
	ExpiresAt time.Time        `json:"expires_at"`
	Events    []EventReference `json:"events"`
	Signature string           `json:"signature"`
}

func (q QuerySnapshot) valid(now time.Time) error {
	if !querySnapshotID.MatchString(q.ID) || q.TenantID == "" || ValidTenantID(q.TenantID) != nil ||
		q.Binding == "" || q.Signature == "" || (q.Source != "ledger" && q.Source != "clickhouse") ||
		q.CreatedAt.IsZero() || !q.ExpiresAt.After(q.CreatedAt) || q.ExpiresAt.Sub(q.CreatedAt) > QuerySnapshotLifetime || !q.ExpiresAt.After(now) {
		return ErrQuerySnapshotUnavailable
	}
	if len(q.Events) > MaxQuerySnapshotEvents {
		return fmt.Errorf("%w: query snapshot event budget exceeded", domain.ErrQuotaExceeded)
	}
	seen := make(map[string]bool, len(q.Events))
	for _, e := range q.Events {
		if e.EventID == "" || e.StreamID == "" || e.Sequence <= 0 || e.Hash == "" || domain.ValidKeyComponent("event id", e.EventID) != nil || domain.ValidKeyComponent("stream id", e.StreamID) != nil || seen[e.EventID] {
			return ErrQuerySnapshotUnavailable
		}
		seen[e.EventID] = true
	}
	return nil
}

// SaveQuerySnapshot atomically admits one bounded membership artifact. It must
// not be called from a Store closure. queryMu is independent of Store.mu: file
// metadata IO never retains the ledger/control lock.
func (s *Store) SaveQuerySnapshot(ctx context.Context, q QuerySnapshot, now time.Time) (err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	if err := q.valid(now); err != nil {
		return err
	}
	raw, err := json.Marshal(q)
	if err != nil {
		return err
	}
	if len(raw) > MaxQuerySnapshotBytes {
		return fmt.Errorf("%w: query snapshot byte budget exceeded", domain.ErrQuotaExceeded)
	}
	if s.pool != nil {
		return s.savePostgresQuerySnapshot(op.ctx, q, raw, now)
	}
	s.queryMu.Lock()
	defer s.queryMu.Unlock()
	if err := op.ctx.Err(); err != nil {
		return err
	}
	if s.split != nil && s.split.path != "" {
		return s.saveFileQuerySnapshot(op.ctx, q, raw, now)
	}
	if s.querySnapshots == nil {
		s.querySnapshots = map[string][]byte{}
	}
	active := 0
	for key, encoded := range s.querySnapshots {
		var existing QuerySnapshot
		if json.Unmarshal(encoded, &existing) != nil {
			return ErrQuerySnapshotUnavailable
		}
		if !existing.ExpiresAt.After(now) {
			delete(s.querySnapshots, key)
			continue
		}
		if existing.TenantID == q.TenantID {
			active++
		}
	}
	key := EventKey(q.TenantID, q.ID)
	if _, exists := s.querySnapshots[key]; exists {
		return domain.ErrConflict
	}
	if active >= MaxActiveQuerySnapshots {
		return domain.ErrQuotaExceeded
	}
	s.querySnapshots[key] = append([]byte(nil), raw...)
	return nil
}

func (s *Store) ReadQuerySnapshot(ctx context.Context, tenantID, id string, now time.Time) (q QuerySnapshot, err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	if tenantID == "" || ValidTenantID(tenantID) != nil || !querySnapshotID.MatchString(id) {
		return q, ErrQuerySnapshotUnavailable
	}
	var raw []byte
	if s.pool != nil {
		raw, err = s.readPostgresQuerySnapshot(op.ctx, tenantID, id, now)
	} else {
		s.queryMu.Lock()
		defer s.queryMu.Unlock()
		if err := op.ctx.Err(); err != nil {
			return q, err
		}
		if s.split != nil && s.split.path != "" {
			path := s.querySnapshotPath(tenantID, id)
			file, openErr := fsutil.OpenDataFile(path, os.O_RDONLY, 0)
			if errors.Is(openErr, os.ErrNotExist) {
				return q, ErrQuerySnapshotUnavailable
			}
			if openErr != nil {
				return q, openErr
			}
			raw, err = readQuerySnapshotFile(file)
		} else {
			raw = append([]byte(nil), s.querySnapshots[EventKey(tenantID, id)]...)
		}
	}
	if err != nil {
		return q, err
	}
	if json.Unmarshal(raw, &q) != nil || q.ID != id || q.TenantID != tenantID {
		return QuerySnapshot{}, ErrQuerySnapshotUnavailable
	}
	if err := q.valid(now); err != nil {
		return QuerySnapshot{}, err
	}
	return q, nil
}

func (s *Store) querySnapshotPath(tenantID, id string) string {
	return filepath.Join(s.split.root, "query-snapshots", tenantFileName(tenantID), id+".json")
}

func (s *Store) saveFileQuerySnapshot(ctx context.Context, q QuerySnapshot, raw []byte, now time.Time) (err error) {
	path := s.querySnapshotPath(q.TenantID, q.ID)
	dir := filepath.Dir(path)
	parent, err := fsutil.OpenDataDir(dir, true)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, parent.Close()) }()
	entries, err := parent.ReadDir()
	if err != nil {
		return err
	}
	active := 0
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		// Only our validated artifact names may be garbage-collected; never WORM.
		id := entry.Name()[:len(entry.Name())-5]
		if !querySnapshotID.MatchString(id) {
			return ErrQuerySnapshotUnavailable
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() > MaxQuerySnapshotBytes {
			return ErrQuerySnapshotUnavailable
		}
		file, err := parent.OpenFile(entry.Name(), os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		encoded, err := readQuerySnapshotFile(file)
		if err != nil {
			return err
		}
		var existing QuerySnapshot
		if json.Unmarshal(encoded, &existing) != nil || existing.ID != id || existing.TenantID != q.TenantID {
			return ErrQuerySnapshotUnavailable
		}
		if !existing.ExpiresAt.After(now) {
			if err := parent.Remove(entry.Name()); err != nil {
				return err
			}
		} else {
			active++
		}
	}
	if active >= MaxActiveQuerySnapshots {
		return domain.ErrQuotaExceeded
	}
	if _, err := fsutil.StatDataFile(path); err == nil {
		return domain.ErrConflict
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := writeDurableFile(path, raw, fsutil.PrivateFileMode); err != nil {
		return err
	}
	return fsutil.SyncDirChain(s.split.root, dir, nil)
}

// Own and close this verified regular file; bound reads even if the inode's
// size changes between directory enumeration/Stat and the actual read.
func readQuerySnapshotFile(file *os.File) (raw []byte, err error) {
	defer func() { err = errors.Join(err, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() > MaxQuerySnapshotBytes {
		return nil, ErrQuerySnapshotUnavailable
	}
	raw, err = io.ReadAll(io.LimitReader(file, MaxQuerySnapshotBytes+1))
	if len(raw) > MaxQuerySnapshotBytes {
		return nil, ErrQuerySnapshotUnavailable
	}
	return raw, err
}

// Probe rather than create: migration 008 precedes API deployment on v1/v2 PG.
func (s *Store) CheckQuerySnapshotStorage(ctx context.Context) (err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	if s.pool == nil {
		return op.ctx.Err()
	}
	rows, err := s.pool.QueryContext(op.ctx, `SELECT tenant_id, snapshot_id, expires_at, metadata FROM audit_query_snapshot LIMIT 0`)
	if err != nil {
		return err
	}
	return rows.Close()
}
