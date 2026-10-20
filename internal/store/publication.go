package store

// Publication confirmations are delivery metadata, not audit facts or proof of
// ClickHouse indexing. They live outside the shared control document: indexed
// PostgreSQL rows or small tenant/event-addressed files. An interrupted cleanup
// can leave an outbox row AND a confirmation; that means cleanup is owed, not
// another broker delivery. Losing a confirmation permits redelivery, never loss.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/fsutil"
)

// ErrPublicationConflict fails closed when a confirmation belongs to different
// ledger coordinates under the same tenant/event identity.
var ErrPublicationConflict = errors.New("publication confirmation conflict")

// ErrPublicationCorrupt distinguishes malformed local delivery metadata from
// transient IO errors. Reconciliation skips it and reports an evidence failure.
var ErrPublicationCorrupt = errors.New("invalid publication confirmation")

type publicationConfirmation struct {
	TenantID    string    `json:"tenant_id"`
	EventID     string    `json:"event_id"`
	StreamID    string    `json:"stream_id"`
	Sequence    int64     `json:"sequence"`
	Hash        string    `json:"hash"`
	ConfirmedAt time.Time `json:"confirmed_at"`
}

func publicationFromReceipt(receipt domain.EventReceipt) publicationConfirmation {
	return publicationConfirmation{TenantID: receipt.TenantID, EventID: receipt.EventID,
		StreamID: receipt.StreamID, Sequence: receipt.Sequence, Hash: receipt.Hash}
}

func (p publicationConfirmation) valid() error {
	if p.TenantID == "" || p.EventID == "" || p.StreamID == "" || p.Sequence <= 0 || p.Hash == "" {
		return fmt.Errorf("%w: publication requires ledger coordinates", domain.ErrInvalid)
	}
	if err := ValidTenantID(p.TenantID); err != nil {
		return err
	}
	if err := domain.ValidKeyComponent("event id", p.EventID); err != nil {
		return err
	}
	return domain.ValidKeyComponent("stream id", p.StreamID)
}

func (p publicationConfirmation) matches(other publicationConfirmation) bool {
	return p.TenantID == other.TenantID && p.EventID == other.EventID &&
		p.StreamID == other.StreamID && p.Sequence == other.Sequence && p.Hash == other.Hash
}

// ConfirmPublication records a broker-acknowledged, ledger-linked event before
// its outbox row may be removed. Repeated confirmations preserve the first
// timestamp. A different hash/stream/sequence at the same identity is an error.
// Call only after broker success, never from inside a Store closure.
func (s *Store) ConfirmPublication(ctx context.Context, event domain.Event) (err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	confirmation := publicationFromReceipt(domain.EventReceipt{TenantID: event.TenantID, EventID: event.EventID,
		StreamID: event.StreamID, Sequence: event.Sequence, Hash: event.Hash})
	if err := confirmation.valid(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := op.ctx.Err(); err != nil {
		return err
	}
	confirmation.ConfirmedAt = time.Now().UTC()
	if s.pool != nil {
		return s.confirmPostgresPublication(op.ctx, confirmation)
	}
	existing, found, err := s.readPublication(op.ctx, confirmation)
	if err != nil {
		return err
	}
	if found {
		if !existing.matches(confirmation) {
			return ErrPublicationConflict
		}
		return nil
	}
	if s.split != nil && s.split.path != "" {
		encoded, err := json.Marshal(confirmation)
		if err != nil {
			return err
		}
		path := s.publicationPath(confirmation)
		if err := writeDurableFile(path, encoded, fsutil.PrivateFileMode); err != nil {
			return err
		}
		return s.syncPublicationDirectories(path)
	}
	if s.publications == nil {
		s.publications = map[string]publicationConfirmation{}
	}
	s.publications[EventKey(confirmation.TenantID, confirmation.EventID)] = confirmation
	return nil
}

// PublicationConfirmed is an identity-bound delivery check, not a receipt
// status check. Missing confirmation means "unknown/unconfirmed", including
// historical publications before confirmations existed; those may redeliver.
func (s *Store) PublicationConfirmed(ctx context.Context, receipt domain.EventReceipt) (confirmed bool, err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.publicationConfirmed(op.ctx, receipt)
}

func (s *Store) publicationConfirmed(ctx context.Context, receipt domain.EventReceipt) (bool, error) {
	want := publicationFromReceipt(receipt)
	if err := want.valid(); err != nil {
		return false, err
	}
	got, found, err := s.readPublication(ctx, want)
	if err != nil || !found {
		return false, err
	}
	if !got.matches(want) {
		return false, ErrPublicationConflict
	}
	return true, nil
}

// withPublicationReader binds a read-only lookup to the existing operation
// context and lock. The callback MUST NOT invoke another exported Store method:
// tenant closures already own Store.mu, which is not reentrant.
func (s *Store) withPublicationReader(ctx context.Context, tenantID string, fn func(*TenantView) error) func(*TenantView) error {
	return func(view *TenantView) error {
		view.PublicationConfirmed = func(receipt domain.EventReceipt) (bool, error) {
			if receipt.TenantID != tenantID {
				return false, fmt.Errorf("%w: publication tenant mismatch", domain.ErrInvalid)
			}
			return s.publicationConfirmed(ctx, receipt)
		}
		return fn(view)
	}
}

func (s *Store) publicationPath(p publicationConfirmation) string {
	digest := sha256.Sum256([]byte(p.EventID))
	name := hex.EncodeToString(digest[:])
	return filepath.Join(s.split.root, "publications", tenantFileName(p.TenantID), name[:2], name+".json")
}

func (s *Store) readPublication(ctx context.Context, want publicationConfirmation) (publicationConfirmation, bool, error) {
	if err := ctx.Err(); err != nil {
		return publicationConfirmation{}, false, err
	}
	if s.pool != nil {
		var p publicationConfirmation
		err := s.pool.QueryRowContext(ctx, `SELECT tenant_id, event_id, stream_id, sequence, event_hash, confirmed_at
FROM audit_publication_confirmation WHERE tenant_id = $1 AND event_id = $2`, want.TenantID, want.EventID).
			Scan(&p.TenantID, &p.EventID, &p.StreamID, &p.Sequence, &p.Hash, &p.ConfirmedAt)
		if errors.Is(err, sql.ErrNoRows) {
			return publicationConfirmation{}, false, nil
		}
		if err != nil {
			return publicationConfirmation{}, false, err
		}
		return checkedPublication(p)
	}
	if s.split != nil && s.split.path != "" {
		raw, err := fsutil.ReadDataFile(s.publicationPath(want))
		if errors.Is(err, os.ErrNotExist) {
			return publicationConfirmation{}, false, nil
		}
		if err != nil {
			return publicationConfirmation{}, false, err
		}
		var p publicationConfirmation
		if err := json.Unmarshal(raw, &p); err != nil {
			return publicationConfirmation{}, false, ErrPublicationCorrupt
		}
		if _, _, err := checkedPublication(p); err != nil {
			return publicationConfirmation{}, false, err
		}
		// A prior write may have renamed the complete file before a directory
		// sync failed. Repair that boundary before treating a visible file as
		// durable, including cleanup-only recovery after process restart.
		if err := s.syncPublicationDirectories(s.publicationPath(want)); err != nil {
			return publicationConfirmation{}, false, err
		}
		return p, true, nil
	}
	p, found := s.publications[EventKey(want.TenantID, want.EventID)]
	return p, found, nil
}

func (s *Store) syncPublicationDirectories(path string) error {
	return fsutil.SyncDirChain(s.split.root, filepath.Dir(path), s.publicationSyncDir)
}

func checkedPublication(p publicationConfirmation) (publicationConfirmation, bool, error) {
	if err := p.valid(); err != nil || p.ConfirmedAt.IsZero() {
		return publicationConfirmation{}, false, ErrPublicationCorrupt
	}
	return p, true, nil
}

// CheckPublicationStorage validates the optional delivery catalog without
// creating it. Call at startup/readiness when a LedgeredPublisher is enabled.
// -check-config remains network-free and cannot substitute for this probe.
func (s *Store) CheckPublicationStorage(ctx context.Context) (err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	if s.pool == nil {
		return op.ctx.Err()
	}
	rows, err := s.pool.QueryContext(op.ctx, `SELECT tenant_id, event_id, stream_id, sequence, event_hash, confirmed_at
FROM audit_publication_confirmation LIMIT 0`)
	if err != nil {
		return err
	}
	return rows.Close()
}

func (s *Store) confirmPostgresPublication(ctx context.Context, p publicationConfirmation) error {
	_, err := s.pool.ExecContext(ctx, `INSERT INTO audit_publication_confirmation
(tenant_id, event_id, stream_id, sequence, event_hash, confirmed_at) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, event_id) DO NOTHING`, p.TenantID, p.EventID, p.StreamID, p.Sequence, p.Hash, p.ConfirmedAt)
	if err != nil {
		return err
	}
	got, found, err := s.readPublication(ctx, p)
	if err != nil {
		return err
	}
	if !found || !got.matches(p) {
		return ErrPublicationConflict
	}
	return nil
}
