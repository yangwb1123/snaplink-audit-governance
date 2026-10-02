# Bound WORM archive-fallback reads by the store operation deadline

Direction: *Propagate the store-operation / request context into the WORM
archive-fallback reads so the already-documented per-operation bound actually
bounds archive I/O, and the already-documented retryable `503 store_timeout`
can be written.*

## What changed

**Every WORM fallback read is now bounded by the store's own per-operation
deadline.** Reading a hot-evicted (archived) audit event used to re-arm a 30s
`archiveReadTimeout` from scratch **per archived receipt**, inside
`Store.mu`, with no relationship to the store operation that contained it. A
query over N archived receipts therefore cost `N × 30s` while holding the
process-wide store lock, and a single black-holed archive stalled every writer
in the process for up to 30s. `internal/service/archive_fallback.go` now derives
one bound per service operation — `min(archiveReadTimeout,
Store.OperationTimeout())`, as a child of the caller's context — and threads it
through every `Archive.Get` in that operation. The derivation is a plain read of
the store's existing immutable field, so there is **no new constant, Config
field, environment variable or default**: `AUDIT_STORE_OPERATION_TIMEOUT` is
still the only knob, and `archiveReadTimeout` survives as the upper ceiling and
as a test seam.

**The bound is short-circuited before any remote work.** Once it is exhausted,
the read returns that error immediately instead of issuing another `Archive.Get`,
so total remote work is bounded in time and stops early rather than continuing
to spend work on a doomed operation. An exhausted or cancelled bound still
fails closed: no partial event set is returned.

**Caller context is now threaded instead of dropped.** `GetEvent`,
`QueryEvents`, `eventsFor`, `consoleEvents`, `GetEventAcrossTenants` and the
legal-hold export gate (`holdBlockingExport`, which gained a `ctx` parameter)
all pass the request context into the fallback instead of
`context.Background()`. Caller cancellation therefore aborts the read
immediately and is never laundered into a store fault.

**The already-declared `503 store_timeout` is now reachable.** The defect was
*reachability*, not contract: `api/openapi/openapi.yaml` has declared
`store_timeout → 503` on every affected route since the store-deadline work, and
`/metrics` has published `audit_store_operation_deadline_exceeded_total` since
then, but `archiveReadTimeout` (30s) was not shorter than audit-api's 30s HTTP
`WriteTimeout`, so the retryable status could never be written — the client saw
an opaque transport error instead. The bound is derived **inside** the store
closure, strictly later than `Store.begin` armed `op.ctx`, so when the fallback
expires the operation context is already expired and the *existing*
`Store.end` classification deterministically yields `store.ErrOperationDeadline`
→ HTTP `503` / `code=store_timeout` / redacted `"internal server error"`, and
the counter moves. **No status, error code, metric or label was added**, and no
OpenAPI/AsyncAPI/proto file changed.

## Invariants deliberately preserved

- **F1 detachment is untouched.** `postEvent`/`postBatch` keep
  `context.WithoutCancel(r.Context())` verbatim, and `Service.Ingest` still
  commits the ledger on the caller context and the post-commit receipt
  transition on `commitCtx`. This change adds a *deadline at the fallback only*
  and adds no rollback path.
- **Idempotency / at-least-once.** An identical retry of an archived event
  still returns `Duplicate == true` with the archived receipt; a
  content-differing retry still returns `409 conflict`. The derivation adds no
  error path between the digest comparison and its return.
- **Single classification site.** The service layer never constructs or counts
  `store.ErrOperationDeadline`.
- **Fail-closed integrity.** Identity / receipt-hash / decode mismatches stay
  integrity errors, never timeouts; an unconfigured archive still fails with
  the same text.

## Operator impact

- **Deploy:** no migration, no config change, no downtime. Ship with the
  variables as they are.
- **Rollback:** revert the binary. No durable state is involved.
- **New failure mode (intended, already contract-declared):** a black-holed or
  very slow WORM archive on a read that needs a hot-evicted event now returns
  the retryable `503 store_timeout` (and, for ingest, discards the commit) at
  the store deadline instead of blocking for 30s per receipt. A full-tenant
  `VerifyIntegrity` scan over many archived events is bounded the same way;
  raise `AUDIT_STORE_OPERATION_TIMEOUT` (still strictly below 30s) or scan per
  tenant if that is too tight for a healthy-but-slow archive.
- **`audit_store_operation_deadline_exceeded_total` is now also moved by
  archive-fallback timeouts**, not only by database statements. It remains
  unlabelled. No new alert rule was added (`docs/SLO_ALERTS.md` is unchanged).

## Tests

`internal/service/archive_fallback_bound_test.go` (new),
`internal/httpapi/archive_fallback_deadline_test.go` (new),
`internal/service/idempotency_conflict_test.go` (extended). Verified that each
new assertion fails against the pre-change production code: the HTTP query took
**30.003s** before (the per-receipt ceiling) versus 0.23s after, and the
multi-receipt service pass lost `store.ErrOperationDeadline` classification
when only the per-operation derivation was reverted.

## Evidence-backed deviations from the approved design

1. **Three of the design's five derivation sites were dropped**
   (`eventFromSnapshot`, `checkExistingIngest`, `checkExistingTenantIngest`).
   Each is a *single*-receipt path, so the bound derived inside
   `archivedEventContext` already covers it with no `N × bound` amplification to
   fix; three extra derivation sites would have bought redundant timers and
   three more places to keep in sync. The two sites that matter for
   amplification — the multi-receipt loops in `eventsFromSnapshot` and
   `GetEventAcrossTenants` — do derive explicitly.
2. **`archivedEvent` (the `context.Background()` wrapper) was deleted rather
   than re-signatured.** A repository-wide grep found zero callers, production
   or test, after the threading landed; the design allowed deletion in that case.
3. **The redundant per-call `WithTimeout(ctx, archiveReadTimeout)` ceiling inside
   `archivedEventContext` was removed.** `archiveFallbackContext` already
   applies `min(archiveReadTimeout, OperationTimeout())`, so a second identical
   cap was provably a no-op child.
4. **`docs/SLO_ALERTS.md` was not touched.** It contains neither
   `store_timeout` nor the deadline counter, so the requirements' claim that it
   "already describes" them is not evidence-backed, and adding an alert rule
   would have introduced a new threshold (out of scope, and
   `checks/prometheus_rules` runs in the quality gate). The README WORM-eviction
   bullet and the `docs/BENCHMARKS.md` `AUDIT_STORE_OPERATION_TIMEOUT` row and
   saturation criteria item 3 carry the coverage instead.