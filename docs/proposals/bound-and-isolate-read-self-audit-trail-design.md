# Design — Bound and Isolate the Read Self-Audit Trail

Direction: "Bound and isolate the read self-audit trail so reads stop rewriting the whole
control-plane snapshot"
Module: `cmd/audit-api` (composes `internal/service`, `internal/store`, `internal/httpapi`,
and the Postgres migration surface)
Status: Proposed (evidence-verified; design for the design gate)
Gate: every landing increment must pass `python3 cli.py quality` (AGENTS.md).

---

## 1. Evidence verification ledger

The requirements spec (evidence) was treated as untrusted claims and re-verified against
HEAD. All substantive claims check out. Line numbers cited below are the verified ones.

| # | Cited claim | Verification result |
|---|---|---|
| 1 | `recordReadAction` at service.go:696–703; `Store.Update` closure appending to `data.AdminActions` at :701; empty-actor short-circuit | **Verified.** Function body exactly as cited (service.go:696–703). |
| 2 | Ten read call sites | **Verified.** `GetReceipt` service.go:664, `GetEvent` :686, `QueryEvents` :769, `Operation` :800, `OperationTimeline` :849, `AggregateTimeline` :860, `ReplayOperation` :872, `ReplayAggregate` :887, `VerifyIntegrity` governance.go:501, `RecordExportDownload` governance.go:121. |
| 3 | Two download legs bypass `recordReadAction`: `export.blocked` governance.go:113; `recordExportRejected` governance.go:186–191 (best-effort `_ =`) | **Verified.** |
| 4 | `Store.Update` store.go:219–248: exclusive `s.mu.Lock()`, clone via `LoadForUpdate`, `ErrSnapshotConflict` retry loop (`snapshotConflictRetries=3`, store.go:200; jittered backoff :204) | **Verified.** `cloneSnapshot` actually lives at store.go:571 (spec said :542; ~29-line drift, content correct). |
| 5 | `fileBackend.Save` store.go:394–460: `json.MarshalIndent` whole doc → tmp write → `file.Sync()` → rename → dir-chain sync | **Verified.** |
| 6 | `postgresBackend.Save` postgres.go:93–112: `json.Marshal` whole doc → version-gated `UPDATE audit_state_snapshot … version=version+1` (query consts :18–20) → `ErrSnapshotConflict` on `affected != 1` | **Verified.** |
| 7 | `Snapshot.AdminActions` store.go:52; `domain.AdminAction` models.go:385–394 (8 fields, no `Seq`) | **Verified.** |
| 8 | `ListAdminActions` governance.go:1099–1117: backward walk, `limit` clamped 1..100, filter-before-cap, never mutates the slice | **Verified.** |
| 9 | Exactly 17 non-test `AdminActions = append` sites, zero trim/rotation | **Verified by grep:** service.go:214,306,340,701; sources.go:33,61; governance.go:47,57,113,188,219,257,812,857,862,927,998. |
| 10 | `read_selfaudit_test.go` (597 lines) pins: one fact per actor-bearing read; empty actor ⇒ none; fail-closed on `saveErr`; `ErrSnapshotConflict` after bounded retries; FR-4 target encodings; newest-first + filter-before-cap; `TestVerifyIntegrityDoesNotAttestAdminTrail` | **Verified** (all named tests present; read_selfaudit_test.go = 597 lines). `readFacts` reads through `ListAdminActions` — trail facts must therefore be visible via the merged view. |
| 11 | HTTP pins: `TestHTTPReadSelfAuditVisible` server_test.go:2667; six-row `TestHTTPReadEndpointsAppendSelfAuditFacts` :3793; one-row `TestHTTPOperationSummaryAppendsExactlyOneFact` (~:3878); real file-backed store via `store.Open(t.TempDir()/state.json)` (server_test.go:56–59) | **Verified.** Minor drift on the one-row test line. |
| 12 | Seams: `scriptedConflictBackend` archive_batch_test.go:23–60; `holdGateBackend` governance_hold_test.go:27+; `conflictBackend` conflict_retry_test.go; `fileBackend.syncDir` hook store_test.go:343+ | **Verified.** All are capability-less single-`Backend` seams. |
| 13 | `TestRecordExportDownloadBlockFactUpdateFailure` governance_hold_test.go:208 (armed Save failure must abort the download, zero facts) | **Verified.** |
| 14 | Config pattern: `service.Config` service.go:25–67, `New` default resolution :115–148 (`<= 0 ⇒ default`), `DefaultAggregateCheckpointRetention = 1000` aggregate_checkpoint.go:15 | **Verified.** |
| 15 | `cmd/audit-api/main.go` flag/env pattern (`flag.Int` + `intEnv`) :57; `runCheckConfig` :372 validates via `service.New(nil, cfg)` | **Verified.** |
| 16 | `migrations/004_state_snapshot.sql` single-row table; migrations are operator-applied (no in-code runner) | **Verified.** |
| 17 | `CheckFileToPostgresMigrationHazard` store.go:325 inspects only `len(data.Events)` | **Verified.** |
| 18 | `Ready` optional-capability probe pattern (store.go:299) | **Verified.** |
| 19 | `operationTimelineNoAudit` shared un-audited core service.go:809 | **Verified.** |
| 20 | Sibling proposal carves out this exact concern: `docs/proposals/bound-control-plane-snapshot-design.md:459–460` ("Fixing the pre-existing unbounded growth of `AdminActions` … separate concern") | **Verified.** |
| 21 | Analysis source `docs/auto/analyses/cmd-audit-api-ffe949d3.json` item 1 | **Verified** (markdown-wrapped JSON; item 1 is this direction). |
| 22 | Quality-gate interaction | **Verified:** `internal/store` must not import service/httpapi/grpcapi (checks/architecture.py); file cap 1500 lines / 260 fn lines / 45 decisions (engineering.yaml); service.go is 1449/1500 today. |
| 23 | Mutation-fact count pins read through `ListAdminActions`: `TestAdminActionSelfAudit` service_test.go:1207 (set-membership, initial count 3), `schema_rollback_test.go:22/40` (before/after), `TestVerifyExportDownloadRejectsTamperedAndSwapped` service_test.go:1518/1558 (exactly one `export.download_rejected`), HTTP tampered-download pin server_test.go:1259/1298 | **Verified.** |
| 24 | `TestReadSelfAuditOnExport` read_selfaudit_test.go:122 expects reads==1 + exports==1 after `CreateExport` + `RecordExportDownload` (async `runExport` goroutine) | **Verified.** |
| 25 | `TestVerifyIntegrityDoesNotAttestAdminTrail` read_selfaudit_test.go:551 — tamper via `svc.Store.Update` on snapshot `AdminActions`, verdict must stay `Valid:true` | **Verified.** Under the design the read fact moves to the trail, so the tamper loop no-ops on an empty snapshot `AdminActions`; `VerifyIntegrity` never consults the trail — verdict unchanged, test green unmodified. |

### 1.1 Design gaps found in the spec (resolved in this design)

- **F-1 — `AppendAdminFact` fallback deadlock.** Spec REQ-3 says `AppendAdminFact` takes the
  exclusive lock and, on fallback, "identical to today's `recordReadAction` body" i.e. calls
  `s.Update` — which takes the same lock ⇒ deadlock. Resolution: extract the retry loop from
  `Update` into an unexported `updateLocked(fn)` (store.go:219–248 becomes `Lock` +
  `updateLocked`), and have the fallback path call `updateLocked` under the already-held
  lock. `Update`'s public behavior is byte-for-byte unchanged (pinned by conflict_retry_test.go).
- **F-2 — Spec REQ-3 keeps `Store.mu` exclusive per read.** Holding the store lock for every
  trail append leaves ingest blocked behind reads (exactly the consequence the direction
  names as the defect). Because the trail is append-only and consistency-independent of the
  snapshot (no read-modify-write), the trail fast path does **not** take `Store.mu`; the
  backend serializes its own trail writes (`fileBackend.mu` for the file, a new
  `postgresBackend.trailMu` for PG). Reads then never block ingests on the trail path —
  strictly better than today, and every pinned test is single-goroutine or count/set-based,
  so nothing observes the removed serialization. Fallback path keeps the lock.
- **F-3 — Mixed-version PG window (spec silent).** During a rolling deploy, an old replica
  still writes read facts into the snapshot `AdminActions`; a new replica's first capped
  append trims them before they ever reach the trail. Confined to read self-audit facts
  (never ledger/evidence data), bounded, self-healing; documented in §4 and §6. The file
  backend cannot mix versions (flock ⇒ single live writer, state_lock.go:64).
- **F-4 — O(1) append needs count bookkeeping.** Checking "trail length > cap" on every
  append must not re-read the whole trail. Both backends keep an in-memory count, seeded by
  a lazy baseline scan/count on first use (O(n) once per process, O(1) per append after).
  PG multi-replica note: the cap is a *soft* bound (each replica's in-memory count can lag);
  the compaction DELETE uses a `seq < threshold` watermark so a concurrent INSERT's newer
  `seq` can never be deleted (§3.5).
- **F-5 — PG rollout hazard.** If 005 is not applied before the new binary starts, every
  read fails closed. `postgresBackend.Ready` gains a trail-table catalog check (readyz fails
  fast) and the first trail append lazily runs `CREATE TABLE IF NOT EXISTS` (mirrors the
  snapshot-row lazy ensure at postgres.go:25–27), so a late-applied migration self-heals.
- **F-6 — `MaxAdminTrailActions == 0` vs `<= 0 ⇒ default` tension.** The two fields resolve
  differently: `MaxAdminActions <= 0 ⇒ DefaultMaxAdminActions`; `MaxAdminTrailActions < 0 ⇒
  DefaultMaxAdminTrailActions`, `== 0 ⇒ unbounded` (operator-explicit). Specified precisely
  in §3.7.
- **F-7 — Trail compaction must be crash-atomic.** The file compaction rewrites the JSONL;
  a crash mid-rewrite must not leave a torn trail. Compaction writes a temp file, fsyncs,
  renames, then reopens the persistent fd (same discipline as `fileBackend.Save`). A crash
  before rename leaves the pre-compaction trail intact; a crash after rename loses nothing
  newer than the cap (the cap was already exceeded; those facts were the oldest).
- **F-8 — `TestReadSelfAuditOnQueryAndGet`/`TestReadSelfAuditOnTimelineReplayReceiptVerify`
  are set/count-based** (verified); no in-repo pin observes cross-source merged ordering, so
  the D-4 tie-break (trail-first on equal `CreatedAt`) is unconstrained by tests and safe.

---

## 2. Target architecture

```
actor-bearing read (GetEvent/QueryEvents/…/RecordExportDownload/blocked-leg/recordExportRejected)
        │  service.recordReadAction / download legs
        ▼
Service.Store.AppendAdminFact(fact, s.Config.MaxAdminActions)
        │
        ├─ backend implements AdminTrail  ──►  append-only trail (NO Store.mu):
        │                                     • file backend:  <state>.admin-trail.jsonl (JSONL, O_APPEND fd, fsync/append)
        │                                     • PG backend:    INSERT INTO admin_action_trail (no version bump, no row rewrite)
        │                                     • in-memory:     slice
        └─ backend does NOT implement  ──►   legacy fallback under Store.mu:
                                            updateLocked(append fact + trimOldest(snapshotCap))  → full Save (exactly today's semantics)

mutation paths (14 append sites) ──► Service.appendAdminAction(data, action) inside the existing Update closure:
                                            append + trimOldest(MaxAdminActions)   (capped, drop-oldest, O(1) slice re-header)

ListAdminActions ──► Store.Read (snapshot AdminActions) + Store.ReadAdminTrail (trail, newest-first)
                     ├─ trail empty: verbatim current backward-walk loop (pins TestListAdminActionsPlatformTenantFilter)
                     └─ merge newest-first by CreatedAt desc; equal CreatedAt ⇒ trail first, then reverse-append within source;
                        tenant/platform filter BEFORE limit clamp (1..100); count == len(items) unchanged.
```

Read-path facts never enter the persisted snapshot; the snapshot holds only mutation-path
facts, capped. Each fact is written to exactly one source (no dedup needed).

---

## 3. API changes — complete list

All changes are internal Go API additions; **no** HTTP route, OpenAPI, proto, JSON shape, or
`domain.AdminAction` change.

### 3.1 New capability interface — `internal/store/trail.go` (new file)

```go
// AdminTrail is the optional append-only persistence for read-path admin facts.
// Backends that implement it absorb read facts without rewriting the snapshot
// document; backends that do not fall back to the legacy full-snapshot Save in
// Store.AppendAdminFact (preserving fail-closed and optimistic-lock semantics
// for capability-less test seams).
type AdminTrail interface {
    AppendAdminFact(fact domain.AdminAction) error // durable; error ⇒ fact not persisted
    ReadAdminTrail() ([]domain.AdminAction, error) // newest-first (reverse append order)
}
```

### 3.2 New `Store` methods — `internal/store/trail.go`

```go
// AppendAdminFact records one admin fact. Trail-capable backends persist it in
// the append-only trail WITHOUT holding Store.mu (append-only, no snapshot
// coupling; the backend serializes its own trail writes). Capability-less
// backends fall back to the legacy full-snapshot Save under the exclusive
// lock, with identical conflict-retry and error semantics to Store.Update.
func (s *Store) AppendAdminFact(fact domain.AdminAction, snapshotCap int) error

// ReadAdminTrail returns trail facts newest-first; an empty result when the
// backend has no trail.
func (s *Store) ReadAdminTrail() ([]domain.AdminAction, error)
```

### 3.3 `Store.Update` refactor (no behavioral change)

Extract the retry loop (store.go:219–248) into `func (s *Store) updateLocked(fn func(*Snapshot) error) error` (no lock). `Update` becomes `Lock(); defer Unlock(); return s.updateLocked(fn)`. Pinned by `conflict_retry_test.go`; the fallback path of `AppendAdminFact` calls `updateLocked` under the lock (F-1).

### 3.4 `fileBackend` — `internal/store/store.go` + `internal/store/trail.go`

- Implements `AdminTrail` when `path != ""` and in-memory mode (`path == ""`).
- Trail path: `f.path + ".admin-trail.jsonl"` (e.g. `./data/state.json` → `./data/state.json.admin-trail.jsonl`).
- Append (under `f.mu.Lock()`, never `Store.mu`): lazy `os.OpenFile(trailPath, O_APPEND|O_CREATE|O_WRONLY, 0o640)` with parent `MkdirAll(0o750)` + dir-chain sync (`f.syncDir` hook if set, else `fsutil.SyncDirChain`) on first creation; `json.Marshal(fact)` + `\n`; `file.Sync()` per append (durability parity with `Save`); increment in-memory `trailCount` (baseline-seeded by a lazy scan of the file on first use); if `cap > 0 && trailCount > cap` → compact (F-7): read all lines, keep newest cap, tmp+fsync+rename, reopen fd.
- Read (under `f.mu.RLock()`): read file, decode lines; malformed **non-final** line ⇒ error (fail closed — the governance surface must not silently truncate evidence); truncated **final** line (crash mid-append) ⇒ tolerated/dropped; return newest-first.
- `Save` and `restoreSnapshot` are untouched; they only ever touch `state.json`.

### 3.5 `postgresBackend` — `internal/store/postgres.go` + `internal/store/trail.go`

- New state: `trailMu sync.Mutex`, `trailCount int64`, `trailEnsured bool`.
- `AppendAdminFact` (under `trailMu`, never `Store.mu`): one-time lazy `CREATE TABLE IF NOT EXISTS admin_action_trail (…)` (F-5), then one `INSERT INTO admin_action_trail (id, tenant_id, actor, action, target_type, target_id, detail, created_at) VALUES ($1,…, $8)` — no `version` bump, no `updated_at` change, no `ErrSnapshotConflict` surface (append-only is replica-safe).
- Compaction when `trailCount > cap`: `DELETE FROM admin_action_trail WHERE seq < (SELECT seq FROM admin_action_trail ORDER BY seq DESC OFFSET $1 LIMIT 1)` — a `seq` watermark so concurrent INSERTs (newer `seq`, BIGSERIAL) are never deleted (F-4). Rare; amortized O(1) per append.
- `Ready` additionally checks `SELECT to_regclass('admin_action_trail') IS NOT NULL` — readyz fails fast when migration 005 is missing (F-5).
- Baseline `trailCount` seeded by `SELECT count(*) FROM admin_action_trail` on first use.

### 3.6 Service layer — `internal/service/admin_trail.go` (new file) + rewiring

```go
// MaxAdminActions caps Snapshot.AdminActions length (drop-oldest, newest
// retained). <= 0 selects DefaultMaxAdminActions. Enforced at every append
// site, so the persisted snapshot always satisfies the bound.
MaxAdminActions int
// MaxAdminTrailActions caps the separate read trail. 0 = unbounded append-only
// (operator-explicit); otherwise the trail is compacted to the newest
// MaxAdminTrailActions facts (amortized O(1) per append).
MaxAdminTrailActions int
```

- `const DefaultMaxAdminActions = 10000`; `const DefaultMaxAdminTrailActions = 100000` (in `admin_trail.go`, following the `DefaultAggregateCheckpointRetention` pattern).
- `func resolveAdminTrailConfig(cfg Config) (Config, error)` called from `service.New` (service.go:115–148) — resolution per F-6, plus validation (`MaxAdminActions >= 100`; `MaxAdminTrailActions == 0 || MaxAdminTrailActions >= MaxAdminActions`), returning `fmt.Errorf("%w: …", domain.ErrInvalid)` so `-check-config` and boot fail on misconfiguration.
- `func (s *Service) appendAdminAction(data *store.Snapshot, action domain.AdminAction)` — append + `if len > cap { data.AdminActions = data.AdminActions[len-cap:] }` (O(1) slice re-header, drop-oldest). All 14 mutation-path append statements are rewired to it (no output change for mutation facts).
- `recordReadAction` (service.go:696) body becomes `return s.Store.AppendAdminFact(s.adminAction(tenantID, actor, action, targetType, targetID, detail), s.Config.MaxAdminActions)` — empty-actor short-circuit unchanged.
- Download legs: governance.go:113 (`export.blocked`) and `recordExportRejected` (governance.go:186–191, keeping the best-effort `_ =`) call `Store.AppendAdminFact`. Create-time `export.blocked` (governance.go:47) and worker facts (`export.recovered`, `export.blocked` :927/:998) stay on the mutation path.

### 3.7 `cmd/audit-api/main.go`

- `-admin-actions-cap` / `AUDIT_ADMIN_ACTIONS_CAP` (default `service.DefaultMaxAdminActions`), `-admin-trail-cap` / `AUDIT_ADMIN_TRAIL_CAP` (default `service.DefaultMaxAdminTrailActions`; `0` = unbounded) — same `flag.Int` + `intEnv` pattern as `-segment-size` (main.go:57); wired into `service.Config{MaxAdminActions, MaxAdminTrailActions}` (main.go:80).
- `runCheckConfig` (:372) already calls `service.New(nil, cfg)` ⇒ REQ-1 validation surfaces there with exit code 1, no store opened.

### 3.8 Merged `ListAdminActions` (governance.go:1099 + helper in `admin_trail.go`)

Fast path (trail empty): the **verbatim** current loop. Merge path: snapshot facts (reverse slice order) ∪ trail facts (newest-first from `ReadAdminTrail`), merged newest-first by `CreatedAt` desc with ties → trail first, then reverse-append within source (D-4); then the existing tenant/platform filter before the `limit` clamp (1..100). `count == len(items)` and the HTTP handler (server.go:1066–1089) unchanged. Merge helper lives in `admin_trail.go` so `governance.go` stays under the 260-line/45-decision caps (1117/1500 lines today).

### 3.9 Migration — `migrations/005_admin_action_trail.sql` (new)

```sql
-- Append-only read self-audit trail. Read-path facts (audit.event.read,
-- audit.event.export, export.download_rejected, export.blocked on the
-- download leg) are INSERTed here instead of rewriting the single-row
-- audit_state_snapshot, so reads never force a full-row UPDATE.
CREATE TABLE IF NOT EXISTS admin_action_trail (
    seq         BIGSERIAL PRIMARY KEY,
    id          TEXT NOT NULL,
    tenant_id   TEXT NOT NULL,
    actor       TEXT NOT NULL,
    action      TEXT NOT NULL,
    target_type TEXT NOT NULL DEFAULT '',
    target_id   TEXT NOT NULL DEFAULT '',
    detail      TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS admin_action_trail_tenant_created
    ON admin_action_trail (tenant_id, created_at DESC, seq DESC);
```

---

## 4. Compatibility constraints

| # | Constraint | How it is satisfied |
|---|---|---|
| C-1 | `read_selfaudit_test.go` passes **unmodified** (597 lines, all 14 tests) | D-1 capability/fallback split: fail-closed (`saveErr`), conflict-exhaustion, and `holdGateBackend` tests run on capability-less seams ⇒ legacy fallback with today's exact semantics; count/encoding/order/filter tests run on the real file backend where read facts land in the trail and are visible via the merged `ListAdminActions` (they are set/count-based — verified F-8). |
| C-2 | `TestVerifyIntegrityDoesNotAttestAdminTrail` green | Read fact moves to the trail; the test's snapshot tamper loop no-ops (snapshot `AdminActions` holds no read facts); `VerifyIntegrity` never reads the trail (attestation stays out of scope, F4 decision (b)). |
| C-3 | `TestRecordExportDownloadBlockFactUpdateFailure` (governance_hold_test.go:208) green | `holdGateBackend` is capability-less ⇒ blocked-leg append falls back to `updateLocked` ⇒ armed Save failure surfaces raw and zero facts commit. |
| C-4 | HTTP boundary pins (`TestHTTPReadSelfAuditVisible`, six-row, one-row; tampered-download `export.download_rejected`) green | File-backed store ⇒ trail-capable; facts visible through `GET /api/v1/admin/actions` via the merge; `count == len(items)` unchanged. |
| C-5 | Mutation-fact counts (`TestAdminActionSelfAudit` service_test.go:1207, `schema_rollback_test.go:22/40`, `legal_hold_export_test.go`) green | Mutation facts stay in the snapshot; trail empty in those tests ⇒ fast path with verbatim loop. |
| C-6 | `Backend` interface unchanged; all test seams (scripted/holdGate/conflict) compile unchanged | Capability is a separate additive interface; seams simply don't implement it. |
| C-7 | `domain.AdminAction` JSON shape, canonical digests, `VerifyIntegrity` golden bytes unchanged | No schema change; trail is convention-only like `AdminActions` today. |
| C-8 | No OpenAPI/proto/HTTP surface change | Route-contract gate (checks/route_contract.py) scans handlers only; new store/service files register nothing. `checks/contract_fields.py` unaffected. |
| C-9 | Dependency direction: `internal/store` must not import service/httpapi/grpcapi (checks/architecture.py) | `trail.go` imports only `domain` + stdlib (same as store.go). |
| C-10 | File-size/function caps (engineering.yaml: 1500 lines, 260 fn lines, 45 decisions) | service.go grows ≤ ~15 lines (Config fields + resolution call; 1449 → ~1460); all new logic in `trail.go`/`admin_trail.go`; `ListAdminActions` merge in a helper. |
| C-11 | Existing configs/flags keep working | New flags default to bounded behavior; no existing flag changes. Existing state files/rows remain readable (`AdminActions` still decodes; trimmed to cap on first capped append). |
| C-12 | In-memory mode (`-state ""`) | In-memory trail slice under `f.mu`; behavior identical to today plus a bounded in-memory trail. |
| C-13 | Mixed-version rollout (PG) | Confined, bounded, self-healing loss of read self-audit facts only (F-3); never ledger/evidence data. Recommended rollout in §6. File backend: flock ⇒ no mixed versions. |
| C-14 | Snapshot cap changes behavior only when `AdminActions` would exceed 10 000 (previously unbounded) | Documented behavior change in `docs/release-notes.md`; the cap is enforced at append, so the persisted document never exceeds it (AC-1). |

---

## 5. Failure modes

| # | Failure | Behavior | Recovery / notes |
|---|---|---|---|
| FM-1 | File trail append error (ENOSPC, fsync failure, fd exhaustion at lazy open) | Read fails closed with the append error; the fact is not persisted (write failed ⇒ nothing committed) | Same observable semantics as today's fail-closed pin; snapshot unaffected; reads succeed again when space/fds free up. |
| FM-2 | PG trail INSERT error (DB down, row/table missing) | Read fails closed; nothing persisted (single-statement autocommit) | readyz (`Ready` catalog check) surfaces the missing-table case fast; migration 005. DB outage already fails all store ops. |
| FM-3 | Fallback path Save error (`saveErr` seams, PG conflict exhaustion) | Identical to today: `saveErr` surfaces immediately; `ErrSnapshotConflict` retried ≤ `snapshotConflictRetries` then surfaces; zero facts persisted | Pinned by `TestReadSelfAuditFailClosedTimelineReplayReceiptVerify` and `TestReadSelfAuditConflictExhaustionFailsReads`. |
| FM-4 | Trail file corruption (garbage/malformed non-final line) | `ReadAdminTrail` fails closed ⇒ `ListAdminActions` errors ⇒ HTTP 500; the trail is never silently truncated | Operator action: inspect/repair/rotate the JSONL. Truncated **final** line (crash mid-append) is tolerated and dropped. |
| FM-5 | Compaction failure (file rewrite ENOSPC; PG DELETE error) | The append that triggered compaction fails closed; pre-compaction trail intact (tmp+fsync+rename atomicity, F-7) | Next append retries compaction once the trigger condition persists. |
| FM-6 | Crash mid-trail-append | Partial final line ⇒ tolerated/dropped on next read; facts before the crash durable (per-append fsync) | AC-4 durability parity with `Save`. |
| FM-7 | Multi-replica PG soft cap | Table may briefly exceed the cap (per-replica in-memory counts); compaction DELETE uses a `seq` watermark ⇒ never deletes a concurrent insert (F-4) | Self-corrects; cap is a bound, not a hard invariant, on PG. |
| FM-8 | Unbounded trail (`MaxAdminTrailActions = 0`) | Append-only growth slope (one write per fact, O(n) total IO — the O(n²) rewrite amplification is gone); `ListAdminActions` read cost linear in trail size | Documented operator choice; default 100 000 bounds it. |
| FM-9 | PG `BIGSERIAL` overflow | `seq` is int64 ≈ 9.2×10¹⁸ facts — unreachable at any plausible read rate | Documented; no action. |
| FM-10 | Mixed-version PG window (old replica appends read facts to snapshot) | New replica's first capped append may trim those facts before they reach the trail | Confined to read self-audit facts (C-13); bounded by `MaxAdminActions`; self-heals post-rollout; raise the cap during rollout to shrink the window (§6). |
| FM-11 | `ListAdminActions` on a corrupt/huge trail stalls governance reads | Bounded by default cap (≤ 100 000 facts, low-frequency endpoint); fails closed on corruption (FM-4) | Operator can inspect the trail file independently of `state.json`. |
| FM-12 | Trail read and append interleave | Serialized by `fileBackend.mu` (readers RLock, appends Lock) — no torn lines; PG reads see only committed rows | Same lock family as `Save`; no deadlock (trail path never takes `Store.mu`; lock order `Store.mu → f.mu` preserved). |

---

## 6. Migration steps (operator runbook)

1. **File backend deployments (default).**
   - No data migration. Deploy the new binary and restart (single live writer via flock ⇒ no mixed-version window).
   - First actor-bearing read lazily creates `<state>.admin-trail.jsonl` (0o640, parent 0o750, dir-chain fsync).
   - Pre-existing `admin_actions` in `state.json` are trimmed to `MaxAdminActions` on the first capped append — no backfill, no rewrite of the file at boot.
   - Optional: set `-admin-actions-cap` / `AUDIT_ADMIN_ACTIONS_CAP` (default 10 000) and `-admin-trail-cap` / `AUDIT_ADMIN_TRAIL_CAP` (default 100 000; `0` = unbounded). Validate with `audit-api -check-config`.
2. **PostgreSQL deployments.**
   - Apply `migrations/005_admin_action_trail.sql` (idempotent, `CREATE TABLE IF NOT EXISTS`) **before or with** the rollout. `readyz` fails fast if it is missing; the first trail append also lazily self-heals (F-5).
   - No data backfill; the snapshot row's pre-existing `admin_actions` are trimmed by the first capped append.
   - Rollout ordering for multi-replica: apply 005, then roll the new binary. During the window an old replica's read facts land in the snapshot and may be trimmed by the new replica (FM-10); raising `AUDIT_ADMIN_ACTIONS_CAP` during rollout shrinks the window. Read self-audit facts only — never ledger/evidence data.
3. **Config validation.** Misconfigured caps (`MaxAdminActions < 100`, or a non-zero `MaxAdminTrailActions < MaxAdminActions`) fail at boot and at `-check-config` with an `ErrInvalid`-compatible error.
4. **Rollback.** Revert the binary. Old binaries ignore the trail file/table and resume appending read facts to the snapshot (unbounded again, but fully functional). The orphaned trail file/table is harmless; it can be deleted/truncated later. For PG, the orphan table can be dropped post-rollback.
5. **Release notes.** Add a `docs/release-notes.md` entry: behavior change — admin trail growth is now bounded (`AdminActions` capped at 10 000, drop-oldest); read facts persist in the sidecar trail (`<state>.admin-trail.jsonl` / `admin_action_trail`); reads no longer rewrite the snapshot.

---

## 7. Testable acceptance mapping

### AC-1 — Bounded snapshot after 10 000 actor-bearing reads

New test `internal/service/admin_trail_test.go: TestReadsDoNotRewriteSnapshotFile` (real file-backed store, `testService` pattern, explicit `MaxAdminActions=100`, `MaxAdminTrailActions=100` … caps; frozen clock):

1. Seed tenant + one event; record `S0 = os.Stat(state.json).Size()` and `M0 = ModTime()`.
2. Perform N = 10 000 actor-bearing reads (`svc.GetEvent("tenant-a", "auditor-1", eventID)`); every call must succeed.
3. Assert:
   - (a) `state.json` size == S0 and mtime == M0 — **zero snapshot writes during reads**;
   - (b) `svc.Store.Snapshot()` reports `len(AdminActions) <= MaxAdminActions`;
   - (c) `json.Marshal` of the loaded snapshot is ≤ the size with exactly `MaxAdminActions` facts + margin (bounded by cap, independent of N);
   - (d) trail file exists at `state.json + ".admin-trail.jsonl"` containing N facts (or ≤ trail cap when capped).
4. Companion store-level assertion (counting backend implementing `Backend` + `AdminTrail`, in `internal/store/trail_test.go`): after N reads, `saves == 0` and `appends == N`.

### AC-2 — Read-self-audit invariant holds (`read_selfaudit_test.go` stays green, unmodified)

No new test. Gate = the unmodified suite plus the regression list:

- `internal/service/read_selfaudit_test.go` (all 14 tests, incl. fail-closed + conflict-exhaustion via the fallback, count/encoding via the trail merge, `TestListAdminActionsPlatformTenantFilter` via the empty-trail fast path, `TestVerifyIntegrityDoesNotAttestAdminTrail`).
- `internal/httpapi/server_test.go`: `TestHTTPReadSelfAuditVisible` (:2667), `TestHTTPReadEndpointsAppendSelfAuditFacts` (:3793), `TestHTTPOperationSummaryAppendsExactlyOneFact`, tampered-download rejected-fact pin (:1259/:1298).
- `internal/service/governance_hold_test.go`: `TestRecordExportDownloadBlockFactUpdateFailure` (:208) and the block-replaces-export legs (:668–693).
- `internal/service/legal_hold_export_test.go`, `service_test.go` (`TestAdminActionSelfAudit`), `schema_rollback_test.go`, `archive_batch_test.go`, `conflict_retry_test.go` (Update refactor).

### AC-3 — `ListAdminActions` returns the newest facts up to its limit after the cap is hit

New test `internal/service/admin_trail_test.go: TestListAdminActionsNewestAfterCap` (real file-backed store, `MaxAdminActions=5`, unbounded trail; variant with `MaxAdminTrailActions=5`):

- Ingest one event; perform 20 actor-bearing reads of distinct event IDs.
- `ListAdminActions("tenant-a", false, 100)` returns ≤ 100 items and includes all 5 newest read targets (set equality with the last 5; frozen clock ⇒ "newest" = last-appended, deterministic via reverse trail order); the oldest 15 targets are absent.
- `ListAdminActions("tenant-a", false, 3)` returns exactly the 3 newest (cap respected; filter-before-cap preserved).
- `Snapshot().AdminActions` length ≤ 5 (AC-1 bound holds simultaneously).

### AC-4 — Instrumentation proves no full-snapshot write; a separate append-only trail absorbs read facts

New tests in `internal/store/trail_test.go` (+ optional PG integration behind `AUDIT_TEST_POSTGRES_DSN`, postgres_test.go:14–33 skip pattern):

- Branch (a): AC-1's mtime/size assertion is the proof; counting-backend asserts `saves == 0`, `appends == N` across N reads.
- Branch (b): JSONL line-per-fact with one `file.Sync()` per append and no `state.json` write (observable via the deterministic `<state>.admin-trail.jsonl` path and `os.Stat`); PG: read ⇒ `admin_action_trail` row count +1 and `audit_state_snapshot.version` unchanged (new table created; `TRUNCATE admin_action_trail` in setup).
- Fallback negative control: on a capability-less backend (scripted), N reads still produce N `Save` calls and the fail-closed tests of AC-2 pass — isolation is a production-backend property, not a weakening of the invariant.

### Store-level unit tests (new, `internal/store/trail_test.go`)

- `TestTrailAppendReadRoundTrip` — JSONL line-per-fact, newest-first read.
- `TestTrailCompactionKeepsNewest` — cap exceeded ⇒ file rewritten with newest cap; in-memory slice variant.
- `TestTrailReadFailsClosedOnCorruption` — malformed non-final line ⇒ error; truncated final line ⇒ tolerated.
- `TestTrailAppendFailClosesRead` — injected append error ⇒ returned, fact absent from trail.
- `TestAppendAdminFactFallbackPreservesConflictRetries` / `TestAppendAdminFactFallbackSurfacesSaveErr` — capability-less backend: conflict retry + error propagation identical to `Update` (F-1, FM-3).
- `TestAppendAdminFactDoesNotHoldStoreLock` — trail append does not block a concurrent `Update` (best-effort timing guard; documents F-2).
- `TestReadAdminTrailEmptyWhenCapabilityAbsent` — `nil`/empty for seams.

### Service-level unit tests (new, `internal/service/admin_trail_test.go`)

- AC-1/AC-3 tests above.
- `TestResolveAdminTrailConfigValidation` — defaults, `<= 0`/`0` semantics (F-6), invalid-caps rejection (`ErrInvalid`).
- `TestMutationAppendsTrimOldest` — `MaxAdminActions` low cap on the mutation path (e.g. 5 tenant creates ⇒ snapshot holds 5, oldest dropped, newest retained).

### Gate

`python3 cli.py quality` green after each increment (gofmt, file/function thresholds, dependency direction, invariants, route contract, vet, unit + race, builds). If `gosec` is available, `python3 cli.py security-scan` as an additional release check (AGENTS.md).

---

## 8. Landing increments

1. **Store layer** (self-contained, no service behavior change): `internal/store/trail.go` (capability + `AppendAdminFact` + `ReadAdminTrail`), `updateLocked` extraction, `fileBackend` trail, `postgresBackend` trail + `Ready` catalog check, `migrations/005_admin_action_trail.sql`, `internal/store/trail_test.go`. Existing service code untouched ⇒ all existing tests green. Gate: `python3 cli.py quality`.
2. **Service layer**: `internal/service/admin_trail.go` (defaults, `resolveAdminTrailConfig`, `appendAdminAction`, merge helper), `Config` fields + `New` hook, `recordReadAction` reroute, 14 mutation-site rewiring, download legs, `ListAdminActions` merge, `admin_trail_test.go`. Gate: `python3 cli.py quality` (+ race).
3. **CLI + docs**: `cmd/audit-api/main.go` flags/env + wiring, `runCheckConfig` coverage (automatic via `New`), `docs/release-notes.md` entry. Gate: `python3 cli.py quality`; manual smoke: file mode (`-state` tmpdir) run reads, assert `state.json` mtime unchanged and `.admin-trail.jsonl` grows; PG mode with 005 applied, assert `version` unchanged across reads; `-check-config` with invalid caps exits 1.

---

## 9. Non-goals (unchanged from the requirements spec)

- Hot/cold snapshot split of the ledger (`docs/proposals/bound-control-plane-snapshot-design.md` — separate direction).
- Cryptographic attestation/inclusion of the admin trail in `VerifyIntegrity` (F4 decision (b), pinned by `TestVerifyIntegrityDoesNotAttestAdminTrail`).
- Bounding `Exports` / `RestoreRuns` growth in the global doc (named separate concerns by the snapshot-bounding design's §11).
- Per-tenant `AdminActions` caps (global cap satisfies AC-1's "per-tenant or global"; per-tenant is a future knob).
- Group-commit/batched fsync for trail appends (would break synchronous fail-closed semantics); batching is the documented future optimization if profiling demands it.
- Any change to `ListAdminActions`' `limit` semantics (1..100), the HTTP handler, or OpenAPI/proto surfaces; any change to `domain.AdminAction`'s JSON shape or canonical digests.
- Replay/backfill of historical facts into the trail; pre-existing snapshot `admin_actions` are trimmed by the first capped append.
