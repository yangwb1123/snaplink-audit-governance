# Store operation deadline + observable, operator-configured control-plane pool

Direction: *Give every PostgreSQL request-path store operation a
context/deadline, and make the control-plane pool observable and
operator-configured.*

## What changed

**Every request-path statement now carries a context.** `internal/store` had
two conventions at once: `postgres.go`/`postgres_hotcold.go`/`postgres_cutover.go`
issued context-less `db.Query`/`db.QueryRow`/`db.Exec`/`db.Begin` (and
`saveInitialControl` blocked on `pg_advisory_xact_lock` under a hard-coded
`context.Background()`), while `trail.go` already used the `…Context` variants.
`Backend.Load/LoadForUpdate/Save`, the `postgresSplitStore` helpers, the
`postgresLedgerReader` and `MigratePostgresSnapshot` are now ctx-first, and the
optimistic-lock retry backoff sleeps on `ctx.Done()` instead of `time.Sleep`.
`internal/store/postgres_context_test.go` scans every non-test file of the
package with `go/parser` and fails on any reintroduction of a context-less
statement — and self-tests that scanner against a bad fixture so it cannot pass
vacuously.

**Store operations have a bounded, operator-tunable deadline.** `store.Options`
/`OpenWithOptions`/`OpenPostgresWithOptions` carry `OperationTimeout`
(default `store.DefaultOperationTimeout` = 10s, overridable with
`AUDIT_STORE_OPERATION_TIMEOUT`). One operation context is derived per exported
store operation, before `Store.mu`; exhaustion fails closed as the new sentinel
`store.ErrOperationDeadline`, which also matches `context.DeadlineExceeded`, and
increments an unlabelled monotonic counter exactly once per outermost operation.

**The new failure class is the existing retryable class.** HTTP maps it to
`503` with `code = store_timeout` next to `snapshot_conflict` (message still
redacted); gRPC maps it to `codes.Unavailable` with a fixed message next to
`store.ErrSnapshotConflict`. `api/openapi/openapi.yaml` (all 35
`x-error-mappings` blocks + the closed `Error` code list) and
`checks/route_contract.py`'s `ERROR_CODE_BY_SENTINEL` were updated together, so
`python3 cli.py check-routes` machine-enforces both directions.

**The pool is observable and operator-configured, with no invented number.**
`/metrics` gains five unlabelled `audit_store_pg_pool_*` lines from `db.Stats()`
(omitted, not zero-filled, when the store has no pool) plus
`audit_store_operation_deadline_exceeded_total`. The four pool variables and
`AUDIT_STORE_OPERATION_TIMEOUT` are resolved by one shared implementation
(`internal/runtimeconfig/postgres_pool.go`) used by `audit-api`,
`audit-governance-worker`, `audit-outbox-relay` and `audit-pg-migrate`.
**Unset means no setter call at all**, so the shipped pool is byte-for-byte what
`database/sql` did before; a set value is validated or the binary exits naming
the variable.

## Why

Not one statement executed under the process-wide `Store.mu` carried a context,
so a single black-holed or lock-blocked connection froze ingest *and* reads for
every tenant in the process with no signal able to end it. Every serving binary
also called bare `sql.Open`, so `database/sql` defaults applied (unlimited open
connections, 2 idle, no lifetime) with no way to observe or tune them.

## Operator impact

- **Deploy:** no migration. Ship with the five new variables unset — the pool is
  unchanged and the deadline is 10s.
- **Rollback:** revert the binary; the old build ignores all five variables and
  unsetting them restores the previous pool exactly.
- **New failure mode:** a non-zero `audit_store_operation_deadline_exceeded_total`
  means a statement was aborted with an unknown outcome. That is retryable by
  design (HTTP 503 `store_timeout`, gRPC `Unavailable`), and the idempotent
  retry converges it — the same half-completed window a crash already produces.
- **`audit-pg-migrate` behaviour changes (both deliberate):** the hard-coded
  `SetConnMaxLifetime(10m)` is replaced by
  `AUDIT_POSTGRES_CONN_MAX_LIFETIME` (rollback: `=10m`), and the unbounded
  `db.Ping()` became a 10s-bounded probe so a black-holed DSN fails the command
  instead of hanging it.

## Evidence-backed deviations from the approved design

1. **The ingest commit is detached at the service layer, not the store.** The
   existing regression tests `TestIngestCancelledContextAbortsPromptlyAndRecordsNothing`
   (cancellation *before* the commit must abort the ingest and record nothing)
   and `TestLedgeredPublishTimeoutRetainsOutbox` (cancellation *after* the commit
   must not lose the ledgered event) cannot both hold unless the two sides are
   distinguished. `Service.Ingest` therefore runs the ledger commit on the caller
   context and only the post-commit receipt transition on
   `context.WithoutCancel(ctx)`. The store itself never strips cancellation.
2. **`VerifyIntegrity` reports a caller cancellation as an interrupted
   verification, not an endpoint error.** `TestVerifyIntegrityCancelledCtxReturnsValidFalse`
   pins REQ-3 (fail-closed `Valid=false`, no signature-mismatch fact, no error).
   A store deadline is explicitly *excluded* from that treatment so the new
   retryable class still reaches the caller.
3. **`CreateExport` starts its background export with `context.WithoutCancel`.**
   The job must outlive the HTTP response that created it; otherwise it is killed
   the moment the response is written.
4. **F-D (design-raised blocker) is fixed here:** `postgresSplitStore.enabled`
   no longer memoizes a catalog probe that ended because the *operation context*
   ended, which would otherwise poison every store operation in the process
   permanently. Covered by `TestCatalogProbeContextFailureIsNotMemoized`.