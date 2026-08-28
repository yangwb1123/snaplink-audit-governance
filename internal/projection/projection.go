// Package projection maintains the online query projection in ClickHouse
// (architecture plan section 11.2, ADR-0004). The projection is derived
// from ledger events on the ledgered topic, is eventually consistent and
// can be rebuilt from scratch; it is never the source of truth.
package projection

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
)

// ErrNotLedgered is returned when an event lacks complete ledger-assigned
// chain state; such an event must never materialize into the query surface.
var ErrNotLedgered = errors.New("projection: event lacks ledger-assigned chain state")

// ErrTenantUnscoped is returned when an event carries an empty or malformed
// tenant_id. The projection is a single shared multi-tenant table keyed first
// by tenant_id; such an event must never materialize into the query surface,
// mirroring ErrNotLedgered for missing ledger chain state. The format check
// uses domain.ValidTenantIDComponent, which rejects ':' and ids longer than
// MaxArchiveComponentBytes (85); the empty case is a separate explicit check
// because ValidTenantIDComponent("tenant_id", "") returns nil.
var ErrTenantUnscoped = errors.New("projection: event tenant_id is empty or malformed")

// Store writes the query projection table. tenant_id leads the sort key so
// tenant-scoped time-range scans stay efficient; ReplacingMergeTree keeps
// one row per (tenant_id, event_id) under at-least-once redelivery.
type Store struct {
	db *sql.DB
}

// Open connects to ClickHouse over the native protocol (default port 9000).
func Open(dsn string) (*Store, error) {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &Store{db: db}, nil
}

const schemaDDL = `CREATE TABLE IF NOT EXISTS audit_events (
	tenant_id String,
	occurred_at DateTime64(3, 'UTC'),
	event_id String,
	stream_id String,
	sequence UInt64,
	event_type String,
	source_system String,
	operation_id String,
	actor_id String,
	outcome String,
	payload String,
	event_hash String
) ENGINE = ReplacingMergeTree(occurred_at)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (tenant_id, occurred_at, event_id)`

// EnsureSchema creates the projection table when missing.
func (s *Store) EnsureSchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, schemaDDL); err != nil {
		return fmt.Errorf("ensure clickhouse schema: %w", err)
	}
	return nil
}

// projectionPayload removes derived search digests from a deep copy before
// the protected ledger payload is written to ClickHouse. Encrypted values are
// intentionally preserved as ciphertext: the projection is a query index,
// never a decryption boundary, and the ledger remains the source of truth.
func projectionPayload(event domain.Event) ([]byte, error) {
	stripped, err := security.StripSearchDigests(event.Payload)
	if err != nil {
		return nil, fmt.Errorf("strip search digests: %w", err)
	}
	payload, err := domain.CanonicalJSON(stripped)
	if err != nil {
		return nil, fmt.Errorf("encode payload: %w", err)
	}
	return payload, nil
}

// Insert writes one canonical event into the projection. ClickHouse applies
// the ReplacingMergeTree dedup asynchronously; queries must not assume
// immediate uniqueness. An event with an empty or malformed tenant_id (':'
// present, or > MaxArchiveComponentBytes) is rejected with ErrTenantUnscoped
// before any database access, mirroring ErrNotLedgered (missing chain state)
// and domain.ErrOccurredAtOutOfRange (out-of-range occurred_at); the
// projection is a shared multi-tenant table keyed first by tenant_id, so a
// tenant-scoping gap would write a phantom row invisible to scoped readers.
func (s *Store) Insert(ctx context.Context, event domain.Event) error {
	// Tenant-scoping guard FIRST (fail-closed): an event whose tenant_id is
	// empty or malformed must never enter the shared table. The check precedes
	// payload encoding and any database access, so a zero-value *Store (nil db)
	// returns ErrTenantUnscoped without panicking and no DB round-trip is
	// wasted. The empty case is explicit because ValidTenantIDComponent("") is
	// nil; the format guard covers ':' and the 85-byte archive-component cap.
	if event.TenantID == "" {
		return ErrTenantUnscoped
	}
	if err := domain.ValidTenantIDComponent("tenant_id", event.TenantID); err != nil {
		return ErrTenantUnscoped
	}
	// Presence guard: before payload encoding and before any database access,
	// so a zero-value *Store (nil db) returns ErrNotLedgered without panicking
	// and no DB round-trip is wasted. Presence, not authenticity: chain state
	// is assigned by the ledger upstream (the service strips client-supplied
	// stream_id and stamps StreamID/Sequence/Hash in the commit closure), so a
	// row lacking it is a non-fact.
	if event.StreamID == "" || event.Sequence <= 0 || event.Hash == "" {
		return ErrNotLedgered
	}
	// Ledger-horizon guard: defense in depth for events committed before the
	// domain gate (REQ-2) existed. ClickHouse DateTime64(3,'UTC') cannot index
	// an out-of-range occurred_at; rejecting here (before payload encoding and
	// before any DB access) lets the consumer classify the class as permanent
	// and dead-letter it on the first attempt instead of burning retries. The
	// nil-*Store probe proves this guard precedes DB access.
	if event.OccurredAt.Before(domain.MinOccurredAt) || event.OccurredAt.After(domain.MaxOccurredAt) {
		return fmt.Errorf("insert projection: %w", domain.ErrOccurredAtOutOfRange)
	}
	payload, err := projectionPayload(event)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO audit_events (tenant_id, occurred_at, event_id, stream_id, sequence, event_type, source_system, operation_id, actor_id, outcome, payload, event_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.TenantID, event.OccurredAt.UTC(), event.EventID, event.StreamID, event.Sequence,
		event.EventType, event.SourceSystem, event.OperationID, event.Actor.ID, event.Outcome,
		string(payload), event.Hash)
	if err != nil {
		return fmt.Errorf("insert projection: %w", err)
	}
	return nil
}

// CountTenant returns the projected row count for a tenant (after
// ReplacingMergeTree finalization is not guaranteed without FINAL).
func (s *Store) CountTenant(ctx context.Context, tenantID string) (uint64, error) {
	var count uint64
	if err := s.db.QueryRowContext(ctx, `SELECT count() FROM audit_events WHERE tenant_id = ?`, tenantID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count projection: %w", err)
	}
	return count, nil
}

// RebuildFrom scratch is supported by re-consuming the topic; no manual
// truncation is needed because ReplacingMergeTree converges on event_id.

func (s *Store) Close() error { return s.db.Close() }
