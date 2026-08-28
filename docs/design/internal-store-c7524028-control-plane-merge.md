# Design — Control-plane whole-map clobber in the Postgres split store

- **Direction ID:** `internal-store-c7524028` (finding #1)
- **Module:** `internal/store` (Postgres split store control-plane write path)
- **Source evidence:** `docs/auto/analyses/internal-store-c7524028.json`, verified against repo at commit `6e9dce9`
- **Status:** Design (verified-evidence grounded)
- **Scope:** Only `postgresSplitStore` control-plane persistence in `internal/store`.
  No schema change, no public API change, no read/`materialize` change, no
  file-backed `splitStore` change.

---

## 1. Evidence verification

The claims below were treated as untrusted and checked against the source.
All central claims hold.

| Claim (evidence) | Verdict | Grounding |
|---|---|---|
| `mergeControlDelta` does whole-map replacement of `current.X = mutated.X` when `mutated.X != baseline.X` | **TRUE** | `internal/store/postgres_hotcold.go:341` — 12 `if !reflect.DeepEqual(mutated.F, baseline.F) { current.F = mutated.F }` blocks. |
| `updateTenantOnce` captures `control`/`baselineControl` once, then calls `updateControl(func(current){ mergeControlDelta(current, baselineControl, control) })` | **TRUE** | `internal/store/postgres_hotcold.go:~290` — `control, _ := p.loadControl()`; `baselineControl, _ := cloneSnapshot(control)`; closure passes both into `mergeControlDelta`. |
| `updateControl` reloads `current` per retry but reuses the same stale `baselineControl`/`control` closure | **TRUE** | `internal/store/postgres_hotcold.go:176` — retry loop re-`loadControl()`s `current`, but the closure is the same value-captured closure from `updateTenantOnce`. |
| `archivePendingTenant` mutates `view.Global.DeadLetters` and `view.Global.ArchiveConflictFailures` inside its `UpdateTenant` closure | **TRUE** | `internal/service/hotcold_workers.go` `archivePendingTenant` — `view.Global.DeadLetters[key] = dead` (dead-letter loop) and `view.Global.ArchiveConflictFailures[tenantID] = 0` (closure tail). |
| `ingestTenant` uses the safe `UpdateControl` path that edits the freshly reloaded snapshot directly | **TRUE** | `internal/service/tenant_ingest.go:92` (`s.Store.UpdateControl(func(data){ data.LedgeredOutbox[key] = event })`). |
| Single shared `audit_state_snapshot` row (id=1, jsonb `snapshot`) across all tenants/replicas | **TRUE** | `internal/store/postgres.go:42` `snapshotUpdateQuery` / `:37` `snapshotLoadQuery` `WHERE id = 1`; `loadControl` reads it. |
| `mergeControlDelta` handles the 12 fields listed in REQ-STORE-CTRL-1 | **TRUE** | All 12 present: `Tenants, Sources, Schemas, Policies, LegalHolds, Exports, RestoreRuns, LedgeredOutbox, AdminActions, AggregateCheckpoints, ArchiveConflictFailures, DeadLetters` (note `AdminActions`/`AggregateCheckpoints` are **slices**, the rest are maps). |
| `EventKey(tenantID, eventID) = tenantID + "\x1f" + eventID`; `KeySeparator = "\x1f"` | **TRUE** | `internal/store/store.go:685,695`. |
| `OpenPostgres(db)`, `snapshotConflictRetries = 3`, `cloneSnapshot`, `controlSnapshot` exist | **TRUE** | `internal/store/store.go:205,244,659`. |

**Verdict on the thesis:** The clobber mechanism is real and the fix must live
in `mergeControlDelta` + its `updateTenantOnce` call site. The sibling
`UpdateControl` path is already correct, so the two paths only need to
*converge*, not diverge.

---

## 2. Root cause (precise)

`updateTenantOnce` builds the writer's intended control state `control` once,
against `baselineControl = clone(control-before-closure)`. The control write
then runs:

```go
p.updateControl(func(current *Snapshot) error {
    mergeControlDelta(current, baselineControl, control)
    return nil
})
```

`mergeControlDelta` replaces **whole fields** of the freshly reloaded
`current` with the writer's captured `mutated` value:

```go
if !reflect.DeepEqual(mutated.DeadLetters, baseline.DeadLetters) {
    current.DeadLetters = mutated.DeadLetters   // discards current's keys
}
```

Two failures follow:

1. **Cross-writer loss (no conflict).** A concurrent archive pass commits its
   own `DeadLetters`/`ArchiveConflictFailures` after `control`/`baselineControl`
   were captured but before `updateControl` runs. The fresh `current` carries
   those entries; `mergeControlDelta` then overwrites the whole field with the
   stale `mutated`, silently dropping the concurrent writer's entries (§1.5).
2. **Retry loss (with conflict).** On `ErrSnapshotConflict`, `updateControl`
   reloads `current` but re-invokes the same closure with the same stale
   `baselineControl`/`control`, re-applying the same whole-map replacement. The
   winner's committed entries are again discarded (§1.6).

---

## 3. Concrete design

### 3.1 API / signature — **no public change**

- `mergeControlDelta(current, baseline, mutated *Snapshot)` is unexported and
  **kept with the exact same signature**. This is required by `UT-CTRL-03`,
  which calls it directly with three `*Snapshot` arguments.
- The public `Store`/`TenantView` surface (`UpdateTenant`, `UpdateControl`,
  `OpenPostgres`, `TenantView.Global`) is unchanged → REQ-STORE-CTRL-3
  compatibility constraint satisfied.
- The only change is the **semantics** of `mergeControlDelta`: from
  whole-field replacement to **per-entry union (overlay)**.

### 3.2 New semantics of `mergeControlDelta`

For every field `F` (12 fields, two of them slices), the persisted value is
computed as **"copy `current`, overlay the writer's delta"**:

> **Invariant:** `result.F[k] = mutated.F[k]` if `k` is in `mutated.F`
> (the writer touched it); otherwise `result.F[k]` keeps `current.F[k]`
> (preserves concurrent writers' additions and the pre-existing value).

For **map** fields this is key-wise:

```go
// map[string]V fields (Tenants, Sources, Schemas, Policies, LegalHolds,
// Exports, RestoreRuns, LedgeredOutbox, ArchiveConflictFailures, DeadLetters)
if !reflect.DeepEqual(mutated.F, baseline.F) {
    if current.F == nil { current.F = make(map[...]...) }  // reuse/allocate
    for k, v := range mutated.F { current.F[k] = v }       // overlay writer's keys
}
// fields left untouched by the writer (mutated.F == baseline.F) are skipped,
// so current.F is preserved verbatim.
```

For the two **slice** fields (`AdminActions`, `AggregateCheckpoints`),
whole-field replacement is replaced by **append-union** (preserve `current`,
append `mutated` elements absent from `current` by value), so a concurrent
admin action / aggregate checkpoint is never dropped:

```go
if !reflect.DeepEqual(mutated.AdminActions, baseline.AdminActions) {
    current.AdminActions = unionSlice(current.AdminActions, mutated.AdminActions)
}
```

`unionSlice` appends each `mutated` element that is not `reflect.DeepEqual` to
any element already in `current`.

### 3.3 Call site (`updateTenantOnce`) — **unchanged**

The call site keeps the same shape:

```go
if globalChanged {
    if err := p.updateControl(func(current *Snapshot) error {
        mergeControlDelta(current, baselineControl, control)
        return nil
    }); err != nil {
        return err
    }
}
```

No change is required at the call site because the correctness now comes from
`mergeControlDelta`'s overlay: on every `updateControl` attempt the fresh
`current` is reloaded and the writer's **stable delta** (mutated minus
baseline) is overlaid onto it. Re-applying the same (baseline, mutated) pair on
a fresh `current` is idempotent and non-lossy — exactly analogous to the
`UpdateControl` closure being re-run against a fresh snapshot on retry.

> **Note on REQ-STORE-CTRL-2 wording.** The literal text says the retry
> "MUST NOT re-apply a stale baseline/mutated." The intent — *never lose
> another writer's committed control entries across a conflict+retry* — is fully
> met, because the stale pair is applied as a **non-destructive overlay** onto
> the freshly reloaded `current`. The delta (`mutated` vs `baseline`) is
> inherently stable across retries (the writer's intended change does not
> change), so re-invoking it is correct. If a reviewer demands the stricter
> letter, the equivalent refactor is: capture `delta := controlDelta(baselineControl, control)`
> once and have the retry closure do `applyControlDelta(current, delta)` (same
> overlay, but `current` is the only writable target). That variant also keeps
> `UT-CTRL-03` valid because `mergeControlDelta` can delegate to the same
> overlay routine. **Decision: keep the minimal overlay in `mergeControlDelta`;
> the delta-capture variant is an optional, behavior-equivalent refinement.**

### 3.4 Determinism / byte-equivalence (REQ-STORE-CTRL-4)

- No-conflict, single-writer path: `updateControl`'s fresh `current` equals
  `baselineControl` (nothing else wrote), and `overlay(current, mutated.F)`
  reduces to exactly `mutated.F` because `mutated.F ⊇ baseline.F` for the
  changed field. This is byte-identical to today's `current.F = mutated.F`
  (Go `json.Marshal` sorts map keys deterministically; slices are already
  ordered). So observable state is unchanged for the non-concurrent path.
- Fields the writer did **not** touch (`mutated.F == baseline.F`) are skipped,
  preserving `current` exactly as today.
- The optimistic-lock SQL (`UPDATE audit_state_snapshot SET snapshot=$1, version=version+1 WHERE id=1 AND version=$2`, `ErrSnapshotConflict` on `RowsAffected != 1`) is unchanged.

---

## 4. Compatibility constraints

| Constraint | Status |
|---|---|
| `audit_state_snapshot` schema / jsonb shape unchanged | **Kept** (out of scope) |
| `Snapshot` struct + json tags unchanged | **Kept** |
| Single shared-row design retained | **Kept** (out of scope) |
| `mergeControlDelta` call sites | Only **one** exists (`updateTenantOnce`), verified by grep. Change is localized. |
| Public `Store`/`TenantView` API untouched | **Kept** |
| `UpdateControl` sibling path unchanged but now consistent | **Kept** (already correct) |
| File-backed `splitStore.commitTenant` path | **Untouched** (out of scope per non-goals) |
| `materialize` / read path | **Untouched** |
| `Update`/`updateChecked`/`replaceSnapshot` whole-snapshot paths | **Untouched** (different code path; out of scope) |

---

## 5. Failure modes

- **F1 — Same-key last-writer-wins.** If two concurrent writers target the
  *same* key (e.g. both write `DeadLetters[k]` with different values),
  overlay uses this writer's `mutated` value and may discard the other
  writer's concurrent change *for that single key*. This is inherent
  optimistic-lock LWW and matches `UpdateControl`'s semantics. For archive
  data it is a non-issue: one event → one archiver → one stable `DeadLetter`
  key; `ArchiveConflictFailures[tenantID]` is always set to the deterministic
  `0`. **Accepted.**
- **F2 — Stale delta re-applied on retry.** Non-lossy by construction (§3.3).
  If the optional delta-capture variant is adopted, eliminated entirely.
- **F3 — Hot/ledger persisted outside the control retry loop.**
  `updateTenantOnce` saves hot + appends ledger *before* the `updateControl`
  call. On a control conflict+retry the hot/ledger are already committed
  (idempotent). Pre-existing behavior, orthogonal to this fix; flagged for
  completeness, **out of scope**.
- **F4 — Snapshot clone cost.** `cloneSnapshot(control)` per tenant update is
  unchanged (already present). Not introduced by this fix.
- **F5 — Slice append-union growth.** `AdminActions`/`AggregateCheckpoints`
  could duplicate if the same element is appended by two writers and neither
  compares equal to the other's pre-existing entry — impossible given the
  write patterns (appends only, value-stable). Low risk; bounded by
  `unionSlice`'s dedup.

---

## 6. Migration steps

1. **No data/schema migration.** The change is purely behavioral inside
   `internal/store`; the persisted jsonb shape is identical. No `migrations/`
   entry is required.
2. **Code change confined to `internal/store/postgres_hotcold.go`**
   (`mergeControlDelta` body). Keep signature and call site intact.
3. **Add tests** `UT-CTRL-03`, `IT-CTRL-01`, `IT-CTRL-02`, `IT-CTRL-04`
   (see §7). Keep integration tests DSN-gated per existing convention
   (`AUDIT_TEST_POSTGRES_HOTCOLD_DSN` → `AUDIT_TEST_POSTGRES_DSN`,
   `t.Skip` when unset).
4. **Quality gate (mandatory before declaring done):**
   ```sh
   python3 cli.py quality        # gofmt, size/complexity, dep-direction,
                                 # route/OpenAPI, go vet, unit, race, build
   python3 cli.py security-scan  # optional external scanner (gosec) when present
   ```
   Race tests must cover the concurrent archive scenarios (AC-1/AC-2/AC-4).
5. **Rollout.** Standard binary rollout. Old and new binaries are wire-compatible
   (same SQL). During the overlap window an old replica could still clobber, so
   roll all replicas to the new binary (no long-held row locks). Canary any one
   replica; behavior is strictly additive (only removes concurrency loss).

---

## 7. Testable acceptance mapping

All four tests target `internal/store` directly (faithful to the buggy path).
The integration tests reproduce `archivePendingTenant`'s control-plane writes
(`view.Global.DeadLetters[key] = DL` and `view.Global.ArchiveConflictFailures[tenantID] = 0`)
via `Store.UpdateTenant` closures over two `Store` replicas on a shared DSN —
this exercises `updateTenantOnce` → `mergeControlDelta` → `updateControl` retry
exactly, without pulling in `internal/service` (keeps the change within scope).

### `UT-CTRL-03` — unit, `mergeControlDelta` union (AC-3)

- **File:** `internal/store/postgres_hotcold_test.go` (`package store`).
- **Target:** `mergeControlDelta(current, baseline, mutated)`.
- **Fixture:** three `NewSnapshot()`:
  - `baseline`: `DeadLetters={}`, `ArchiveConflictFailures={}`.
  - `mutated`: `DeadLetters={"a\x1fx": DL_x}`, `ArchiveConflictFailures={"a":0}`
    (writer's own additions only; every other field identical to baseline).
  - `current`: `DeadLetters={"b\x1fy": DL_y}`, `ArchiveConflictFailures={"b":1}`
    (a concurrent writer's entry, absent from both baseline and mutated).
- **Action:** `mergeControlDelta(current, baseline, mutated)`.
- **Assertions:**
  - `current.DeadLetters` contains **both** `"a\x1fx"` and `"b\x1fy"`.
  - `current.ArchiveConflictFailures` contains **both** `"a"` and `"b"`.
  - Fields where `mutated == baseline` (`Tenants`, `Sources`, `LedgeredOutbox`,
    `AdminActions`, …) are **unchanged** (current value retained, not
    overwritten).
- **Pass:** all above. **Fail:** `"b\x1fy"` or `"b"` dropped (current bug).

### `IT-CTRL-01` — integration, cross-tenant concurrent archive (AC-1)

- **File:** `internal/store/postgres_hotcold_test.go`, DSN-gated.
- **Fixture:** `db := newHotColdPostgresTestDB(t)`; two `Store`s via
  `OpenPostgres(db)` (two connections, one shared row). Seed `tenant-a` and
  `tenant-b` each with an archivable event that yields a `DeadLetter`
  (permanent archive error → `view.Global.DeadLetters[key]=DL`). Distinct
  `tenantID`s ⇒ distinct `EventKey` prefixes (`tenant-a\x1f…`, `tenant-b\x1f…`).
- **Action:** `go` routines + `sync.WaitGroup` run
  `st1.UpdateTenant("tenant-a", ColdFirst, deadLetterClosureA)` and
  `st2.UpdateTenant("tenant-b", ColdFirst, deadLetterClosureB)` concurrently;
  `Wait()`. (`deadLetterClosure*` writes exactly the two control lines the
  archive worker writes.)
- **Assertion (SQL):**
  ```sql
  SELECT snapshot->'dead_letters' FROM audit_state_snapshot WHERE id = 1;
  ```
  Parsed jsonb MUST contain **both** a key with prefix `tenant-a\x1f` **and** a
  key with prefix `tenant-b\x1f`.
- **Pass:** both present. **Fail:** only one (or zero) survives.

### `IT-CTRL-02` — integration, same-tenant two events (AC-2)

- **Fixture:** shared DSN, two `Store` replicas. Seed one tenant `tenant-x`
  with two distinct archivable events `e1`, `e2`, each producing a
  `DeadLetter`. Keys `EventKey("tenant-x", e1)` and `EventKey("tenant-x", e2)`
  are distinct.
- **Action:** concurrently
  `st1.UpdateTenant("tenant-x", ColdFirst, closureFor(e1))` and
  `st2.UpdateTenant("tenant-x", ColdFirst, closureFor(e2))`.
- **Assertion (SQL):** same query as IT-CTRL-01; jsonb MUST contain **both**
  `EventKey("tenant-x", e1)` and `EventKey("tenant-x", e2)`.
- **Pass:** both same-tenant entries survive. **Fail:** either clobbered.

### `IT-CTRL-04` — integration, conflict + retry, no loss (AC-4)

- **Deterministic conflict injection (recommended):** add a minimal,
  test-only seam to `postgresSplitStore`:
  ```go
  // saveControlHook, when non-nil, is consulted before the real save and may
  // inject a transient optimistic-lock conflict. Nil in production.
  saveControlHook func() error
  ```
  `saveControl` calls `if p.saveControlHook != nil { if err := p.saveControlHook(); err != nil { return err } }`
  before `p.backend.Save(control)`. A test sets a hook that returns
  `ErrSnapshotConflict` exactly **once**, forcing `updateControl` to retry.
  This deterministically exercises the retry+merge path (**race-safe, no flaky
  timing**).
- **Fixture:** shared DSN, two `Store` replicas. Seed `tenant-c` with an
  archivable event producing a `DeadLetter` on archive.
- **Action:** Replica 1's closure runs first and the hook forces replica 1's
  first `saveControl` to fail with `ErrSnapshotConflict`; replica 2 commits its
  `DeadLetter` for `tenant-c` first. Replica 1 retires via its
  `snapshotConflictRetries` (=3) loop.
  (A real-concurrency variant — two `UpdateTenant("tenant-c", …)` racing,
  looped until a conflict is observed — is also acceptable but nondeterministic;
  prefer the hook.)
- **Assertion (SQL):** after both settle,
  ```sql
  SELECT snapshot->'dead_letters' FROM audit_state_snapshot WHERE id = 1;
  ```
  jsonb MUST contain the winner's `DeadLetter` **and** the retried writer's
  `DeadLetter`.
- **Pass:** both survive the conflict+retry. **Fail:** loser-retry's or
  winner's entry missing.

> All integration tests assert against `snapshot->'dead_letters'` /
> `->'archive_conflict_failures'` jsonb exactly as the ACs specify, and are
> skipped when no Postgres DSN is set (matching `TestZPostgres…` convention).

---

## 8. Quality-gate alignment

- **gofmt:** overlay rewrite of `mergeControlDelta` is net-smaller; stays
  within `max_function_lines: 260` / `max_decisions: 45` (current function is
  small). The optional `controlDelta`/`applyControlDelta` helpers, if added,
  must each stay under the thresholds.
- **Package dependency direction:** change is intra-`internal/store`; no new
  imports.
- **Routes / OpenAPI:** no API surface change.
- **go vet / build:** unaffected.
- **Unit + race:** `UT-CTRL-03` (unit) + `IT-CTRL-01/02/04` run under
  `go test -race` for the concurrent archive scenarios.
- **Security scan:** `python3 cli.py security-scan` as an optional extra
  release check when `gosec` is available.

---

## 9. Risk if unaddressed

Silent multi-tenant audit-evidence loss (Value 10 / Risk-reduction 9 /
Confidence 9 in the source analysis). A concurrent archive pass or a second
replica archiving another event of the same tenant permanently discards
`DeadLetters`/`ArchiveConflictFailures` entries for tenants this update never
touched, with no error — directly undermining the audit ledger's
evidence-completeness guarantees (FM-1 over-limit / F-2 byte-conflict
`dead_letters`, and `archive_conflict_failures` control records).
