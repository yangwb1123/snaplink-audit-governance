# Legal-hold export gate: prove the blocker before materializing the events

`Service.holdBlockingExport` — the single gate behind `CreateExport`,
`GetExport`, `RecordExportDownload` and `runExport` — used to materialize the
tenant's whole event set (one `domain.CloneEvent` per hot event, plus one
verified WORM `Archive.Get` per archived receipt that has no hot body, each
under the 30 s `archiveReadTimeout`) **before** it enumerated the tenant's
active legal holds. For a tenant with no active hold the pass was pure waste: the
scan iterated an empty hold slice and returned "not blocked". All four call
sites pay this inside `store.Store.Read`, i.e. while the process-wide
`store.Store.mu` read lock is held, so every ingest (`Store.Update`) queues
behind it.

The gate now proves its candidate set first, from the in-memory control-plane
hold map alone, and only materializes when at least one hold can actually match.

## What changed

Two production functions, both unexported, both in `internal/service`:

- `query_match.go` — new `holdCanSelectAnySelected(hold, query)`: an exact,
  derived half-open interval-intersection test. It answers only "must this hold
  be evaluated?", never "is this export blocked?".
- `export.go` — new `holdBlockingCandidates(...)` (tenant-scoped, `ReleasedAt ==
  nil`, candidate-filtered, sorted by hold ID) and a reordered
  `holdBlockingExport` that returns `(zero, false, nil)` on an empty candidate
  set **before** the `data.Schemas` copy and before any event access.

No new export, no new `Service` field, no new package-level variable, no new
numeric constant: `archiveReadTimeout`, `exportPutTimeout`,
`exportTerminalRetryLimit`, `snapshotConflictRetries` and every
`engineering.yaml` threshold are untouched. `api/openapi/openapi.yaml`,
`api/asyncapi/asyncapi.yaml` and `api/proto/*` are byte-unchanged, and
`cli.py route-contract` Stage 4 passes untouched because both export routes
already declared the full `statusForError` mapping.

## What is guaranteed unchanged

- **The blocking decision is bit-identical.** A dropped hold is one
  `holdMatchesEvent` provably rejects for every event: a one-sided filter
  (branch 2 returns `false` unconditionally) or an empty intersection between
  `[F.From, F.To)` and the half-open `[Q.From, Q.To)` that `matches` already
  uses. The candidate list is therefore a *subsequence* of the old sorted list,
  so first-match-wins still reports the lowest-ID hold — the 403 body,
  `job.Error`, the `export.blocked` fact and the `audit.event.export` fact are
  all byte-identical.
- **The store-call sequence is unchanged.** The gate adds and removes no
  `Store.Read` / `Store.Update` / `Store.AppendAdminFact` call, which is what
  keeps the `armLoadFailure(ordinal)` / `armSaveFailure(ordinal)` fault-injection
  fixtures valid. `export_hold_test.go`, `export_terminal_persist_test.go` and
  `legal_hold_export_test.go` are byte-unchanged.
- **`holdMatchesEvent` remains the sole authority** on hold semantics. It is
  not modified, and the new predicate is deliberately **not** applied on the
  retention path (`EvaluateRetention`), which keeps its current exact behaviour.

Conservative-keep shapes, pinned by tests: an unbound (both-bounds-zero) filter
always blocks; a one-sided filter is never a candidate; a degenerate *export*
window (zero bound or `From >= To`) never drops a hold; and an inverted or
zero-span *hold* window is kept as a candidate rather than shortcut, because that
shape is only reachable from a hand-edited or legacy snapshot row.

## Recorded behaviour change (accepted, not a fail-open)

Measured against the **unmodified** gate (R-0 baseline) with an unreadable
`events/…` WORM object and zero active holds:

| Route | Before | After |
|---|---|---|
| `GET /api/v1/exports/{jobID}` | **500** | 200 + job body |
| `GET /api/v1/exports/{jobID}/download` | **500** | 200, digest-verified bytes |
| `POST /api/v1/exports` | fails closed | **fails closed, unchanged** (`QueryEvents` runs first) |
| `runExport` selection pass | job failed with the archive error | **job failed with the archive error, unchanged** |

An unreadable WORM event object can still **never be exported**: the create and
run legs both still read it and fail closed, and the sealed bytes remain gated by
`VerifyExportDownload` (job-bound AES-GCM + `domain.HashBytes` digest). What is
lost is *detection*, not prevention: `archivedEventContext` used to re-derive and
compare every archived event hash on every ordinary status/download request, and
`VerifyIntegrity` is operator-invoked with no scheduled sweep in-repo, so for a
tenant with no active hold a tampered or missing `events/…` object now produces
no signal until someone runs an integrity check or exports the affected window.
`audit_http_errors_total` will consequently under-count real archive degradation.
**Compensating control is owed and not in this change**: a scheduled per-tenant
archive integrity sweep, or a bounded counter incremented by
`archivedEventContext`. Owners: service + compliance.

## Measurement note

`audit_http_request_duration_seconds` is a single global histogram with no
route label, so the win cannot be isolated per route from `/metrics`. Use the
OTel span `http.route` on the two export routes (traces, not the histogram).

## Tests

- `internal/service/export_gate_cost_test.go` — the cost oracle is
  `archive.Get` (not wall clock): `eventsFromSnapshot` fetches before it applies
  the predicate, so `gets == 0` proves the gate cloned no event and read no WORM
  object. This is a statement about the *gate path only* — `Store.Read` still
  clones the snapshot's hot events before the closure runs, and that cost is
  unchanged. Covers zero holds, disjoint and touching windows, a 1 ns overlap in
  both directions, lowest-ID determinism with a dropped candidate, unbound /
  one-sided / released / foreign-tenant holds, degenerate hold windows, the
  brute-force predicate agreement over every window edge ±1 ns, and the run leg
  still failing closed on an unreadable object.
- `internal/httpapi/export_gate_cost_test.go` — the 500 → 200 flip plus two
  fail-closed controls (intersecting hold still 403 with the exact hold message;
  `POST /api/v1/exports` still fails non-2xx and persists no job).
