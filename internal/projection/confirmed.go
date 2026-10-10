package projection

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
)

// ErrProjectionConflict means that one fact identity or sequence has multiple
// projected variants. ReplacingMergeTree must not erase that diagnostic.
var ErrProjectionConflict = errors.New("projection: conflicting projected evidence")

const confirmedSchemaDDL = `CREATE TABLE IF NOT EXISTS audit_projection_events (
	tenant_id String,
	stream_id String,
	sequence UInt64,
	event_id String,
	event_hash String,
	prev_hash String,
	occurred_at DateTime64(3, 'UTC'),
	envelope_digest String,
	envelope String,
	projected_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (tenant_id, stream_id, sequence, event_id, event_hash, envelope_digest)
SETTINGS fsync_after_insert = 1, fsync_part_directory = 1`

func (s *Store) checkConfirmedSchema(ctx context.Context) error {
	var engine, key string
	if err := s.db.QueryRowContext(ctx, `SELECT engine, sorting_key FROM system.tables
WHERE database = currentDatabase() AND name = 'audit_projection_events'`).Scan(&engine, &key); err != nil {
		return fmt.Errorf("check confirmed projection schema: %w", err)
	}
	if engine != "ReplacingMergeTree" || strings.ReplaceAll(key, " ", "") != "tenant_id,stream_id,sequence,event_id,event_hash,envelope_digest" {
		return errors.New("incompatible confirmed projection schema; controlled migration required")
	}
	// IF NOT EXISTS alone cannot enforce the durability settings of a
	// preexisting table. Refuse startup if the operator role cannot apply them.
	_, err := s.db.ExecContext(ctx, `ALTER TABLE audit_projection_events MODIFY SETTING fsync_after_insert = 1, fsync_part_directory = 1`)
	return err
}

// projectionEnvelope preserves the full protected envelope for reconstruction,
// but never derived search digests. A deep copy protects the source ledger.
func projectionEnvelope(event domain.Event) ([]byte, string, error) {
	copy, err := domain.CloneEvent(event)
	if err != nil {
		return nil, "", err
	}
	copy.Payload, err = security.StripSearchDigests(copy.Payload)
	if err != nil {
		return nil, "", err
	}
	for key, change := range copy.ChangedFields {
		if strings.HasSuffix(key, security.SearchDigestSuffix) {
			delete(copy.ChangedFields, key)
			continue
		}
		// Changes can themselves contain nested objects; apply the same
		// boundary recursively, without interpreting or decrypting values.
		values, err := security.StripSearchDigests(map[string]any{"before": change.Before, "after": change.After})
		if err != nil {
			return nil, "", err
		}
		change.Before, change.After = values["before"], values["after"]
		copy.ChangedFields[key] = change
	}
	encoded, err := domain.CanonicalJSON(copy)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(encoded)
	return encoded, hex.EncodeToString(digest[:]), nil
}

func (s *Store) insertConfirmed(ctx context.Context, event domain.Event) error {
	envelope, digest, err := projectionEnvelope(event)
	if err != nil {
		return fmt.Errorf("encode projection envelope: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO audit_projection_events
(tenant_id, stream_id, sequence, event_id, event_hash, prev_hash, occurred_at, envelope_digest, envelope)
VALUES (?, ?, ?, ?, ?, ?, parseDateTime64BestEffort(?, 3, 'UTC'), ?, ?)`, event.TenantID, event.StreamID, uint64(event.Sequence),
		event.EventID, event.Hash, event.PrevHash, event.OccurredAt.UTC().Format(time.RFC3339Nano), digest, string(envelope))
	if err != nil {
		return fmt.Errorf("confirm projection write: %w", err)
	}
	return nil
}

func projectionScope(tenantID, streamID string) error {
	if tenantID == "" || domain.ValidTenantIDComponent("tenant_id", tenantID) != nil {
		return ErrTenantUnscoped
	}
	if streamID != "" {
		return domain.ValidKeyComponent("stream id", streamID)
	}
	return nil
}

// Confirmed checks the CURRENT destination, not a separate stale checkpoint.
// A dropped/replaced projection cannot remain confirmed based on broker state.
// Missing rows mean unknown. All variants are counted without FINAL: a merge
// preserves distinct coordinate/content variants because they lead the key.
func (s *Store) Confirmed(ctx context.Context, receipt domain.EventReceipt) (bool, error) {
	if err := projectionScope(receipt.TenantID, receipt.StreamID); err != nil {
		return false, err
	}
	if receipt.EventID == "" || receipt.StreamID == "" || receipt.Sequence <= 0 || receipt.Hash == "" {
		return false, ErrNotLedgered
	}
	if err := domain.ValidKeyComponent("event id", receipt.EventID); err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var variants, matching uint64
	err := s.db.QueryRowContext(ctx, `SELECT
uniqExact(tuple(stream_id, sequence, event_hash, envelope_digest)),
countIf(stream_id = ? AND sequence = ? AND event_hash = ?)
FROM audit_projection_events WHERE tenant_id = ? AND event_id = ?`,
		receipt.StreamID, uint64(receipt.Sequence), receipt.Hash, receipt.TenantID, receipt.EventID).Scan(&variants, &matching)
	if err != nil {
		return false, fmt.Errorf("read projection confirmation: %w", err)
	}
	if variants > 1 || (variants == 1 && matching == 0) {
		return false, ErrProjectionConflict
	}
	return variants == 1 && matching > 0, nil
}

// StreamWatermark is the observed contiguous hash-linked projected prefix of
// ONE tenant/stream. It is not a global Kafka offset, ledger head, immutable
// query snapshot, or proof that all tenant streams have arrived. A future
// conflicting row can invalidate a prefix, so watermarks are not cached truth.
type StreamWatermark struct {
	TenantID string `json:"tenant_id"`
	StreamID string `json:"stream_id"`
	Sequence uint64 `json:"sequence"`
	Hash     string `json:"hash,omitempty"`
	Gap      bool   `json:"gap"`
}

func (s *Store) Watermark(ctx context.Context, tenantID, streamID string) (StreamWatermark, error) {
	result := StreamWatermark{TenantID: tenantID, StreamID: streamID}
	if err := projectionScope(tenantID, streamID); err != nil {
		return result, err
	}
	if streamID == "" {
		return result, fmt.Errorf("%w: stream id required", domain.ErrInvalid)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// Ordered streaming aggregation: memory is independent of prefix length.
	// The shared deadline bounds historical IO; no partial result on timeout.
	rows, err := s.db.QueryContext(ctx, `SELECT sequence, any(event_hash), any(prev_hash),
uniqExact(tuple(event_id, event_hash, prev_hash, envelope_digest))
FROM audit_projection_events WHERE tenant_id = ? AND stream_id = ?
GROUP BY sequence ORDER BY sequence`, tenantID, streamID)
	if err != nil {
		return result, fmt.Errorf("read projection watermark: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var sequence, variants uint64
		var hash, prev string
		if err := rows.Scan(&sequence, &hash, &prev, &variants); err != nil {
			return StreamWatermark{}, err
		}
		if variants != 1 {
			return StreamWatermark{}, ErrProjectionConflict
		}
		if sequence != result.Sequence+1 {
			result.Gap = true
			return result, nil
		}
		if hash == "" || prev != result.Hash {
			return StreamWatermark{}, ErrProjectionConflict
		}
		result.Sequence, result.Hash = sequence, hash
	}
	if err := rows.Err(); err != nil {
		return StreamWatermark{}, err
	}
	return result, nil
}
