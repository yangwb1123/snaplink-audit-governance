package store

// The store operation context: one derived, interruptible bound per exported
// store operation, and the single place where deadline exhaustion is
// classified.
//
// Every statement on the PostgreSQL request path runs while the process-wide
// Store.mu is held, so before this change a single black-holed or
// lock-blocked connection froze ingest AND reads for every tenant in the
// process, with no signal able to end it. The bound is what makes the wait
// interruptible; the sentinel is what lets the transports classify the
// resulting "result unknown" outcome as retryable instead of internal error.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// operationKey marks a context as already being a store operation context.
// Nested store calls (the legacy tenant/control fallbacks call Read/Update
// inside ReadTenant/UpdateControl) must inherit the operation already in
// flight rather than start a second one: a second bound would double-count the
// deadline counter and a second classification would re-wrap the same error.
type operationKey struct{}

// operation is one store operation's derived context and its classification
// state. bounded is false for a nested pass-through, which is exactly how end
// knows it must not classify or count.
type operation struct {
	ctx     context.Context
	cancel  context.CancelFunc
	bounded bool
}

// operationTimeout resolves the effective bound: an explicit Options value
// wins, otherwise DefaultOperationTimeout. A zero Options value therefore
// keeps the shipped default rather than disabling the bound.
func (s *Store) operationTimeout() time.Duration {
	if s.opts.OperationTimeout > 0 {
		return s.opts.OperationTimeout
	}
	return DefaultOperationTimeout
}

// OperationTimeout reports the effective per-operation bound. It is a plain
// read of an immutable field, so it needs no lock and is safe to call from a
// metrics handler.
func (s *Store) OperationTimeout() time.Duration { return s.operationTimeout() }

// OperationDeadlineExhaustions is the monotonic count of store operations that
// ended with their operation context at DeadlineExceeded. It is unlabelled by
// construction (an atomic counter): a tenant, event or route label would make
// the metric cardinality unbounded.
func (s *Store) OperationDeadlineExhaustions() uint64 { return s.deadlineExhaustions.Load() }

// PoolStats returns a snapshot of the database/sql pool backing this store,
// or nil when the store is not PostgreSQL-backed (file and in-memory stores
// have no pool, and a zero-filled pool would be a misleading "0 connections").
//
// It reads database/sql's internally synchronized counters and never takes
// Store.mu nor issues a statement, so a /metrics scrape can never block on a
// store that is frozen inside a hung statement.
func (s *Store) PoolStats() *sql.DBStats {
	if s.pool == nil {
		return nil
	}
	stats := s.pool.Stats()
	return &stats
}

// begin derives ONE operation context per exported store operation, before
// Store.mu is taken. A nested call inside an existing operation receives that
// context unchanged (bounded == false), so exactly one deadline is counted
// and classified per outermost operation.
func (s *Store) begin(ctx context.Context) *operation {
	if _, nested := ctx.Value(operationKey{}).(struct{}); nested {
		return &operation{ctx: ctx, cancel: func() {}}
	}
	opCtx, cancel := context.WithTimeout(ctx, s.operationTimeout())
	return &operation{ctx: context.WithValue(opCtx, operationKey{}, struct{}{}), cancel: cancel, bounded: true}
}

// end is the single deadline-classification site for the store. It wraps both
// ErrOperationDeadline and context.DeadlineExceeded so errors.Is matches
// either, and counts the exhaustion exactly once.
//
// A nil err is returned unchanged even if the deadline expired in the same
// instant: reporting a committed write as failed would make the caller retry
// a durable commit, and the "never lie about a commit" rule outranks "never
// report success for an expired deadline". A context.Canceled propagates
// unchanged — the caller withdrew, so there is nothing to report.
func (s *Store) end(op *operation, err error) error {
	if err == nil || !op.bounded {
		return err
	}
	if errors.Is(op.ctx.Err(), context.DeadlineExceeded) {
		s.deadlineExhaustions.Add(1)
		return fmt.Errorf("%w: %w", ErrOperationDeadline, err)
	}
	return err
}

// sleepBackoff waits out the optimistic-lock retry backoff interruptibly.
// time.Sleep held Store.mu for the whole delay; here an ended operation
// context returns immediately, so a cancelled or expired caller cannot
// overrun its own deadline by retries × backoff.
//
// It uses time.NewTimer rather than time.After so the timer is released when
// the context wins the race instead of lingering until it fires.
func sleepBackoff(ctx context.Context, attempt int) error {
	timer := time.NewTimer(snapshotConflictBackoff(attempt))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
