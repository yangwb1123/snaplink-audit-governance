# Classify caller withdrawal as HTTP 499 / gRPC `Canceled` instead of a 500

Direction: *Stop mapping client cancellation / caller-deadline as an internal
500 (and a gRPC `codes.Internal`): keep `audit_http_errors_total` a true
availability signal.*

## What changed

**A withdrawn request is now its own non-5xx class.** `internal/httpapi/authz.go`
`statusForError` had no arm for the context sentinels, so a client that
disconnected mid-read — or whose own request deadline expired — fell through to
`500 internal_error` / `"internal server error"` and was counted into
`audit_http_errors_total`, the documented P1 availability and error-budget
source (`rate(audit_http_errors_total[5m]) / rate(audit_http_requests_total[5m])
> 0.01`, `AuditIngestErrorRate`). Scanners, load-balancer timeouts and client
churn therefore raised a "check ingest and storage dependencies" page while the
store and archive were healthy.

Two codes, one status, both **last** in every switch:

| Sentinel | HTTP | `code` | gRPC | `rejection_code` |
|---|---|---|---|---|
| `context.Canceled` | `499` | `client_cancelled` | `codes.Canceled` | `canceled` |
| `context.DeadlineExceeded` (caller-context, at the boundary) | `499` | `client_deadline_exceeded` | `codes.DeadlineExceeded` | `deadline_exceeded` |

`499` is declared once as `statusClientClosedRequest` (neither `net/http` nor
Python's `http.HTTPStatus` defines it) and resolved by
`checks/route_contract.py` through an explicit allow-list. `408` was rejected:
a withdrawal is not a server timeout and 408 invites a retry of a request the
caller already abandoned.

**The message is fixed, and that is mandatory, not cosmetic.** `errorBody`
redacts only `status >= 500`, so a status-only fix would have published the
wrapped cause of a `<500` body — the event id from
`archivedEventContext` (`fmt.Errorf("archived event %s: %w", …)`), the archive
key, the S3 endpoint, pgx detail. Both new codes carry a fixed string
(`"client cancelled the request"` / `"client request deadline exceeded"`), the
same two strings on both transports, and the 499 body is `Cache-Control:
no-store` like a 5xx.

**One counting funnel.** `(*Server).recordOutcome(status, err)` is now the only
place a rendered status is counted: `>= 500` moves `audit_http_errors_total`
(its definition is unchanged), `499` moves the new
`audit_http_request_cancelled_total{reason="cancelled"|"deadline_exceeded"}`.
The batch partial-failure path, which called `errorBody` + `writeJSON` directly
and hand-rolled its own `if partialStatus >= 500`, now funnels through it too, so
a 499 there is counted exactly once. Both series are rendered unconditionally
(including `0`) so the withdrawal alert can never fire on a missing series; the
only label is a closed two-value vocabulary — no tenant, event, route or URL.

**The cited read path is now reachable through the fix.**
`downloadExport` answered an archive-read failure with a literal
`http.StatusInternalServerError` that bypassed `statusForError` entirely; it now
calls `statusForError(err)`. A genuine archive failure (e.g. `ENOTDIR`) still
falls through the default arm to `500 internal_error` / `"internal server error"`,
so `TestHTTPDownloadExportRedacts500` stays green unchanged.

**Contract, gate, alert, docs.** All 35 OpenAPI operations declare the two codes
at 499 plus a `'499'` response (the batch operation keeps its inline
`oneOf: [Error, BatchPartialError]` shape); `components.schemas.Error`
documents the fixed message, the budget exclusion and the retry contract.
`checks/route_contract.py` extracts the two new arms **arm-bounded** (comment
lines blanked, arm body stopped at the next `case`/`default`), so an arm can
never adopt a later arm's return — the lazy cross-arm `.*?` was only invisible
while every arm returned an `http.Status*` literal. `deploy/prometheus-rules.verify.yml`
gains `AuditClientRequestWithdrawal` (P3) in `audit-slo`;
`AuditIngestErrorRate` is byte-identical. `docs/SLO_ALERTS.md` gains the P3 row,
the 17→18 / 7→8 counts, and a §3 bullet stating that withdrawn requests do
**not** consume the error budget.

## Invariants deliberately preserved

- **Ordering is load-bearing.** `Store.end` wraps an exhausted bound as
  `fmt.Errorf("%w: %w", ErrOperationDeadline, err)`, so that composite also
  satisfies `errors.Is(err, context.DeadlineExceeded)`. The withdrawal arms sit
  **after** `store.ErrOperationDeadline` in `statusForError`, `errorBody` and
  `toStatus`; both transports pin this with an ordering-proof test. A withdrawal
  arm placed earlier would silently convert the documented retryable
  `503 store_timeout` / `codes.Unavailable` into a caller withdrawal and stop
  clients retrying an unknown-outcome write.
- **Sentinel identity only, never error text.** A client-library timeout
  (`*url.Error`, `"Client.Timeout exceeded"`) is not
  `errors.Is(context.DeadlineExceeded)` and stays a fail-closed 500.
- **`audit_http_errors_total` keeps its definition.** Nothing about the
  availability metric changed; only which statuses reach it.
- **Ingest detachment (F1) is untouched.** `postEvent`/`postBatch` keep
  `context.WithoutCancel`, so 499 is unreachable on the write routes today. The
  contract declares it on every operation for uniformity and says so.
- **A deadline consumed inside a store operation is still `503 store_timeout`.**
  The boundary cannot distinguish a caller-inherited expiry from the store's own
  bound; disentangling that is a separate store-layer change. The caller-deadline
  class is observable where the sentinel reaches the boundary outside a store
  operation (`downloadExport`'s archive read, `archivedEventContext`'s
  exhausted-bound guard).
- **No new logging.** `statusError` deliberately does not log a withdrawal as an
  internal fault (a scanner can withdraw thousands of requests a minute).

## Operator impact

- **Deploy:** no migration, no config change, no downtime.
- **Rollback:** revert the binary.
- **New (intended) client-visible status:** `499` on read/query paths. Most
  intermediaries synthesise their own 499 or substitute a 504 rather than
  forwarding this one; the metric is authoritative. A client that retries
  "4xx ⇒ back off and retry" must be taught that 499 is terminal for that
  attempt (it produced no response).
- **Rolling deploy:** counters are per-process, so `AuditIngestErrorRate` mixes
  old and new pods until the rollout completes.
- **New P3 alert `AuditClientRequestWithdrawal`** at a **5% first-calibration
  ratio** (a placeholder: `> 0` would fire continuously under normal scanner
  traffic). Retune against real load before launch, as the rest of
  `docs/SLO_ALERTS.md` requires.
- **Watch item (not changed here):** `internal/outbox/http.go` classifies
  `400 <= code < 500` (except 401/429 and the structured `tenant_mismatch`) as a
  **permanent** delivery failure, so a 499 from this API would be dead-lettered
  rather than retried. It is unreachable today because the relay POSTs
  `/api/v1/events` and every write path detaches from the request context, so the
  relay's own cancellation never reaches a 499. Teaching the relay that a
  withdrawal is retryable is the follow-up if a write route ever becomes
  cancellable.

## Tests

`internal/httpapi/caller_withdrawal_mapping_test.go` (new): mapping table,
ordering proof against `store_timeout` / `snapshot_conflict`, the
timeout-shaped-but-not-sentinel control, classifier/status agreement, and the
counter split driven through `writeError`.
`internal/httpapi/caller_withdrawal_e2e_test.go` (new): the export-download and
event-read routes over the existing blocking-archive seam (recorder level for
the class and the redacted body), the socket-level client-disconnect test for
the counters (`audit_http_errors_total` flat, withdrawal counter up,
`audit_http_requests_total` up), and the `/metrics` cardinality test.
`internal/grpcapi/caller_withdrawal_status_test.go` (new): the `toStatus` /
`rejectionCode` table, the "never Internal for a withdrawal" claim, and the
no-internal-log assertion with an internal control.
`checks/test_route_contract.py` (extended): the withdrawal arms are extracted
and their absence from the spec fails the gate; an arm with no return fails
loudly (arm-bounded proof); a comment-mentioned sentinel is not extracted; an
unlisted local constant still fails by name.
`internal/httpapi/export_gate_cost_test.go` (extended additively):
`gateHTTPArchive` gained `blockUntilCancelled()` / `blocked()` and a
detail-carrying `ctx.Err()` wrapper — the constructor signature is unchanged, so
its three existing tests keep compiling.

## Evidence-backed deviations from the approved design

1. **`gateHTTPArchive` gained an `entered` channel field and the two methods,
   rather than the design's "opt-in mode whose `Get` signals `entered`".** Same
   seam, and still additive (no constructor change), but the channel is stored on
   the struct so the socket-level test can observe an entered read.
2. **The caller-deadline E2E test uses a deferred deadline context instead of an
   already-expired one.** An already-expired context is consumed by
   `GetExport`'s own store operation first, which deterministically yields the
   documented `503 store_timeout` — correct behaviour, but not the withdrawal
   class. `deferredDeadline` reports no `Deadline()` and expires only once the
   archive read is in flight, which makes the assertion deterministic without
   sleeping.
3. **The design's C1 rationale (arm-bleed into `store_timeout` → 500) was not
   reproducible** — the arm-bounded rewrite is kept as belt-and-braces, and the
   verified risk is the opposite one: an un-extracted arm making a declared code
   invisible. The alternation group is closed correctly and comment lines are
   stripped before extraction (both design-gate review notes).
4. **`docs/SLO_ALERTS.md` §2 preamble counts** were corrected to 18 rules / 8
   locally verifiable SLO rules, as the design's migration step 6 required.
5. **A pre-existing flaky test was made deterministic (unrelated package).**
   `internal/service/archive_fallback_bound_test.go`
   (`TestArchiveFallbackIsBoundedByStoreOperationTimeout`) failed ~40–70% of
   runs *on the pristine tree of this worktree* (verified with the change
   stashed), and `internal/httpapi/archive_fallback_deadline_test.go` carried
   the identical race. Cause: the archive fallback bound is
   `min(archiveReadTimeout, OperationTimeout())`, so when the ceiling is above
   the operation bound the two derived deadlines carry the **same** duration and
   differ only by the microseconds between their derivation — whether `Store.end`
   observes `op.ctx` as already expired (the retryable `store_timeout`
   classification) became a coin flip between two timers due at the same
   instant. The black-hole archives now pause briefly after their context ends,
   which is how a real S3/pgx read behaves. No production code changed and no
   assertion was weakened; `store_timeout` stays pinned at both the service and
   the HTTP boundary — which is exactly the regression this direction could have
   introduced.
