# Bound and classify the export download's WORM read

Direction: *Bound and correctly classify the export download's WORM read: the
only `Archive.Get` outside the bounded service path.*

## What changed

**`GET /api/v1/exports/{jobID}/download` no longer touches the WORM archive
port.** It was the only route in `internal/httpapi` that called
`s.Service.Config.Archive.Get` itself, with the raw `*http.Request` context.
That read was unbounded (a black-holed or slow S3 endpoint pinned the handler
goroutine, the decrypted-export heap and the TCP connection until audit-api's
30s HTTP write timeout, not until the store's per-operation bound) and
unclassified (a non-sentinel archive error fell through `statusForError`'s
default arm to an opaque `500 internal_error`, and a caller deadline consumed
by that read arrived as a bare sentinel). Every other `Archive.Get`/`Put` in the
tree is bounded by `archiveReadBound()` and classified by `Store.end`; the
readiness probe in `server.go` is the only remaining direct port caller in
`httpapi`, and it is a deliberate dependency probe.

**The read moved behind one store operation in the service.** The new
`Service.ReadExportObject(ctx, tenantID, jobID)`
(`internal/service/export_read.go`) resolves the job tenant-scoped and reads
the sealed object inside a single `Store.Read` closure, deriving its context
from `s.archiveFallbackContext(ctx)` **inside** that closure — the placement
`archive_fallback.go` documents as load-bearing. Because `archiveReadBound()` is
`min(archiveReadTimeout, Store.OperationTimeout())` and audit-api validates the
store bound strictly below its 30s write timeout at startup
(`runtimeconfig.ValidateStoreOperationTimeoutBelow`, from `openStore` and
`prepareServer`), the operation deadline is the one that expires first and
`Store.end` — the store's single classification site — wraps the failure as
`store.ErrOperationDeadline`. The route therefore answers the **retryable
`503 store_timeout` it already declares** (`api/openapi/openapi.yaml`
`downloadExport` → `x-error-mappings: {code: store_timeout, status: 503}`),
with redacted `"internal server error"`, `Cache-Control: no-store`,
`audit_store_operation_deadline_exceeded_total` +1 and `audit_http_errors_total`
+1.

**No new surface was added.** No new constant, `Config` field, environment
variable, error sentinel, status code, error code, metric, label or OpenAPI /
AsyncAPI / proto change. `internal/service` never constructs
`store.ErrOperationDeadline` itself, and `internal/httpapi/authz.go` is
untouched — the classification reuses the arms every other bounded read already
declares. The only new operator requirement is the one the binary already
enforces: `AUDIT_STORE_OPERATION_TIMEOUT` unset (10s default) or below 30s.

**The missing-object class is translated in the service, not the transport.**
The handler no longer matches the archive adapter's error taxonomy
(`os.IsNotExist` cannot unwrap the `%w` wrapper the service adds, so keeping
that predicate would have silently converted a missing local object from `404
not_found` into `500 internal_error`). `ReadExportObject` maps `fs.ErrNotExist`
to `domain.ErrNotFound`, the transport drops the `os` import entirely, and the
404 is derived from the single `statusForError` table.

## Invariants deliberately preserved

- **Stage order and fail-closed.** `require → resolveScope → GetExport → 409
  completed-check → read → VerifyExportDownload → RecordExportDownload (R4
  legal-hold gate) → headers → Write`. Nothing is streamed before
  `RecordExportDownload` succeeds, and a failed read, verification or fact
  append still answers the JSON error envelope with no `x-ndjson`.
- **Cost.** A completed download with no active hold still issues exactly one
  `Archive.Get`, for an `exports/` key, and zero `events/` gets.
- **Tenant binding and existence masking.** An unknown job, another tenant's job
  and a tenant-less caller are refused before any remote call.
- **Read-only.** The new method appends no audit fact, no admin action and no
  job state; completed jobs stay immutable, so the pairing invariant that
  `VerifyExportDownload`'s digest check relies on is unchanged.
- **Caller cancellation.** `Store.end` propagates `context.Canceled` unchanged,
  so a client that disconnects mid-read still answers `499 client_cancelled`
  and is still kept out of the availability error budget.

## Behaviour change to be aware of

A caller deadline that expires **while the sealed object is being read** is now
classified by `Store.end` as `store.ErrOperationDeadline` → `503 store_timeout`
instead of `499 client_deadline_exceeded`. This is the intended consequence of
moving the read inside a store operation, and it makes this route behave like
every other bounded read in the tree (the 499 deadline class remains mapped,
declared and pinned as a mapping/counter class; it is simply no longer the
answer for a deadline consumed inside a store operation). The withdrawal
counters (`audit_http_request_cancelled_total{reason=...}`) do not move.

## Operator impact

- **Deploy:** no migration, no config change, no downtime; `api/openapi/**`,
  metrics and configuration are unchanged. Deployment is a plain binary swap.
- **Rollback:** revert the binary. The read is a pure read and no durable state
  is left half-written.
- **New (intended) signal:** a stalled WORM endpoint on this route now *raises*
  `audit_store_operation_deadline_exceeded_total` and the 503 error rate instead
  of producing a truncated response. That is the correct, retryable signal —
  watch it together with `readyz`'s `archive_unavailable`.
- **Known, unchanged trade-off:** the read happens while the store read lock is
  held, so a stalled archive pauses store writers for at most the operation
  bound. This matches the existing precedent (`eventsFromSnapshot` /
  `archivedEventContext` read N archived objects under the same lock under one
  bound); a single `DownloadExport` service method that collapses the four
  store operations of this route into one remains a follow-up.

## Tests

- `internal/service/export_read_test.go` (new): verbatim sealed bytes and cost
  oracle; the bound plus `store.ErrOperationDeadline` classification; the
  negative leg (a read bound tighter than the store bound is *not* classified,
  which is what makes the production 503 a property of the shipped relation);
  `archiveReadBound() == Store.OperationTimeout()` over `{2s, 10s, 25s}`;
  exhausted-bound short-circuit (0 remote calls, cancellation stays bare);
  tenant binding / completion-shape table with 0 archive calls; missing object
  → `domain.ErrNotFound` against a real `FileStore`, including the
  `os.IsNotExist`-cannot-unwrap-`%w` pin; unconfigured archive fails closed
  instead of panicking.
- `internal/httpapi/export_archive_read_test.go` (new): black-holed archive →
  bounded `503 store_timeout` with the counter split and the redacted body;
  the happy path still 200 with `Content-Length` agreement, exactly one
  `exports/` get, zero `events/` gets and exactly one `audit.event.export`; a
  missing object still `404 not_found` with no fact appended; a source scan and
  an AST scan proving `handlers_governance.go` holds no archive port and that
  `readyz` is the only `Service.Config.Archive` caller in `httpapi`.
- `internal/httpapi/caller_withdrawal_e2e_test.go`: the deadline half of the
  withdrawal e2e was rewritten to pin the new classification
  (`TestDownloadCallerDeadlineInsideStoreOperationIsStoreTimeout`) using a real
  declared caller deadline, which is what makes `Store.end`'s classification
  deterministic; the cancellation half is unchanged.
- `internal/service/archive_fallback_bound_test.go`: the multi-receipt bound
  test now pauses after its context ends, exactly as its sibling subtests
  already document. Without it that assertion was a coin flip between two timers
  due at the same instant and failed intermittently at HEAD (reproduced at
  ~2-4 runs in 15 on this worktree, independent of this change).

## Evidence-backed deviations from the approved design

1. **The missing-object translation lives in the service (the architect's
   "Option B"), not in the handler.** The approved design kept
   `errors.Is(err, fs.ErrNotExist)` in `handlers_governance.go`; the design
   gate recorded that either placement was acceptable and the architect review
   recommended translating in the service so the transport stays free of backend
   error taxonomy. This also removes the `os` import from `httpapi` entirely
   rather than replacing it with `io/fs`, and the 404 code/message are
   unchanged (`not_found` / `not found`).
2. **The concurrency-race acceptance test (design T5, "hold created during the
   archive read") is not included.** With the read inside the store closure, a
   hold created by a concurrent writer can only become visible through a
   `sync.RWMutex` writer-preference race, and a second `store.Store` over the
   same state file is not constructible in this package (the file backend takes
   an exclusive flock; `store.NewWithBackend` needs a `Backend` fixture). Rather
   than land a test whose result depends on scheduler order, the R4 gate is
   pinned where it is deterministic: an active hold still answers 403 with no
   archive read and no fact (`TestHTTPExportStatusStillBlocksOnIntersectingHold`,
   `TestHTTPExportHold403BodyLeaksNothingCrossTenant`), and a hold created after
   an export completed still blocks the download at the service level
   (`TestExportAccessBlockedByHoldCreatedAfterCompletion`).
3. **`docs/SLO_ALERTS.md` was not changed.** The counter already has no alert
   rule (its documented role is a decision criterion in `docs/BENCHMARKS.md`),
   and adding one would introduce a new threshold — outside this direction's
   "no new surface" constraint and checked by `checks/prometheus_rules` in the
   quality gate. The counter and error-rate interpretation is recorded here and
   in the export-download runbook path instead.
