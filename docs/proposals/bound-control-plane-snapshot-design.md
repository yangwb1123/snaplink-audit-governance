# Design — Bound the Control-Plane Snapshot

Direction: "Bound the control-plane snapshot: stop rewriting the entire immutable ledger on
every write and evict archived events"
Module: `internal/store` (+ read surfaces in `internal/service` that must not break)
Status: Increment 2 implemented in the reference tree; PostgreSQL cutover is explicit
Gate: every landing increment must pass `python3 cli.py quality` (AGENTS.md).

Implementation note: the reference tree archives and evicts hot event payloads,
serves verified archive fallback reads across event details, queries, timelines,
replay, export, legal-hold, and integrity surfaces, and now persists a v2
per-tenant hot document plus append-only cold ledger. File stores auto-migrate
v1 snapshots with a `.v1` backup. PostgreSQL enables the split only after
`006_hot_cold_split.sql` is applied and `store.MigratePostgresSnapshot` is run;
legacy single-row deployments remain supported during the expand/cutover window.

---

## 1. Evidence verification ledger

Every citation in the requirements spec was checked against HEAD (`18159c5`). All claims
verified; line numbers drift by ≤ 8 lines as already noted in the spec. Highlights:

| Citation | Verification |
|---|---|
| `migrations/004_state_snapshot.sql` | Single-row `audit_state_snapshot (id=1, snapshot jsonb, version bigint, updated_at)`. The only optimistic lock is `version`. |
| `Snapshot` struct, store.go:39 | `Events map[string]domain.Event`, `Receipts map[string]domain.EventReceipt` plus 13 more collections. `data.Events[key] = event` exists in exactly one non-test place: service.go:538. The other `data.Events` refs are reads (:475 idempotency, :672 GetEvent). |
| `LoadForUpdate` → `cloneSnapshot` | store.go:388 (file), :45 (PG); cloneSnapshot store.go:542 = full `json.Marshal` + `decodeSnapshot` (UseNumber) round trip. |
| `Update` / `UpdateChecked` | store.go:219 / :254. `snapshotConflictRetries = 3` (store.go:200), jittered backoff 5→25 ms + ≤5 ms (:204). Each retry re-clones. |
| `fileBackend.Save` | store.go:394; `json.MarshalIndent(data, "", "  ")` of the whole doc, tmp-write + `file.Sync()` + rename + dir-chain fsync; no version/CAS. |
| `postgresBackend.Save` | postgres.go:93; `json.Marshal(data)` whole doc, version-gated UPDATE; `ErrSnapshotConflict` on `affected != 1`. |
| Ingest CAS cycles | Ledger CAS service.go:468; `data.Events[key] = event` :538; status commits :577/:599/:613 — **errors are now propagated** (the `_ =` swallow pattern is gone; confirmed at service.go:577–623). 2 full CAS cycles per event. |
| `ArchivePending` | governance.go:541–613; collects non-archived events, verifies WORM writes, then one batch `Store.Update` marks receipts `StatusArchived` and evicts the corresponding hot payloads. |
| `conflictBackend` + retry pins | conflict_retry_test.go:15–37, tests at :72/:81/:82 pin `snapshotConflictRetries`. |
| No eviction path (historical baseline) | **Superseded by Increment 1.** `ArchivePending` and successful ingest now delete only the hot event payload after a verified archive write; receipts remain as the linkage anchor. |
| Read surfaces | **Updated by Increment 1.** `GetEvent`, `QueryEvents`, `eventsFor` (timeline/replay/restore), `VerifyIntegrity`, `holdBlockingExport`, and `runExport` use verified archive fallback for evicted events; `EvaluateRetention` keeps its documented hot-window policy. |
| Archive key | `events/{tenant}/{stream}/{seq:020d}-{eventID}.json` service.go:1154; `archive.Store.Get` exists (archive.go:178 FileStore, :358 S3Store); `StatusArchived` set only after a verified Put. |
| Retained metadata | `EventReceipt` keeps Status/StreamID/Sequence/Hash; `Segment` keeps FirstPrevHash/LastHash/EventCount/MerkleRoot/ManifestHash/Signature (domain/models.go). |
| Migration hazard | `CheckFileToPostgresMigrationHazard` store.go:325 uses `len(data.Events)`; test store_test.go:194. |
| Benchmarks/fixtures | `BenchmarkIngest` bench_test.go:37, `BenchmarkQueryLargeLedger` :87; `testService`/`crmPrincipal`/`testCtx`; PG harness skips without `AUDIT_TEST_POSTGRES_DSN` (postgres_test.go:14–33); `scriptedConflictBackend` in archive_batch_test.go. |
| Migrations | Operator-applied `.sql` files (no in-code migration runner; postgres tests assume 001+004 applied). |
| OpenAPI | `events/{eventId}` :49, `/receipt` :54, `/integrity/verify` :105, `/exports` :87, `/replay` :75, timelines :70/:80 — surface unchanged is a non-goal. |

### 1.1 Findings that shape the design (not in the spec)

- **F-1 — A1/A2 tension.** A1 accepts persisted growth ∝ retained receipts/segments (slope
  bound), while A2 requires per-write cost flat vs archived count K (≤ 3× vs K=0, marginal
  < 1×). With receipts/segments/checkpoints retained in the *rewritten* document (I-4), every
  `Update` clones + serializes ∝ K: 10,000 receipts ≈ 2 MB JSON vs ≈ 50 KB for the K=0
  control plane ⇒ clone ratio ≈ 40×, far above the 3× CI bound. **A2 therefore requires the
  retained cold metadata to leave the per-write rewritten set** (append-only cold tier), not
  just eviction of event payloads. A1 stays satisfiable: the *persisted* cold tier still grows
  ∝ retained metadata, which is exactly the accepted slope.
- **F-2 — A3.5/A2 mechanism tension.** A3.5's fault-injection assumes the archived-mark and
  the eviction land in ONE atomic Save (single doc). A2 requires receipts out of the hot doc,
  so the archived-mark (a cold append) and the eviction (a hot delete) can no longer share one
  Save. The design resolves this with an explicit commit ordering + idempotent-recovery
  argument and an adapted (documented) crash-atomicity test; see §5.2 and the acceptance map.
- **F-3 — Complexity budget.** `service.go` is 1448/1500 lines, `governance.go` 971;
  `engineering.yaml` caps functions at 260 lines / 45 decisions. All new service code must
  land in **new files** (`archive_fallback.go`, `ledger.go`, `eviction.go`), never inline in
  existing functions.
- **F-4 — `GetReceipt`/idempotency-key scan depend on cold metadata.** The idempotency-key
  conflict scan (service.go:503) iterates `data.Events`; after eviction it would silently stop
  covering archived events. `EventReceipt` does not carry `IdempotencyKey`, so the scan must
  retain the key in receipts (§5.3) — a small additive schema change, backward compatible.
- **F-5 — Legal-hold gates are correctness-critical `data.Events` consumers** not named in
  REQ-7's fallback list. `holdBlockingExport` (governance.go:812) and `runExport` selection
  (:857) must be archive-inclusive, otherwise a hold would fail to block export of archived
  events and the A3 export-digest equivalence would break. They join the fallback surface.
- **F-6 — `CheckFileToPostgresMigrationHazard` under-counts after eviction** (REQ-6): after
  the cold split it must also inspect the ledger directory / ledger table.

---

## 2. Target architecture: hot/cold split with per-tenant hot documents

Two storage tiers replace the single document. The Go API exposes both.

```
┌─────────────────────────────────────────────────────────────────────┐
│ Tier 1 — HOT control plane (cloned + serialized per write, bounded) │
│                                                                     │
│  global doc (one per store): Tenants, Sources, Schemas, Policies,   │
│    LegalHolds, Exports, RestoreRuns, AdminActions,                  │
│    AggregateCheckpoints, ArchiveConflictFailures                    │
│  per-tenant doc (one per tenant): Streams, Events                   │
│    Events holds ONLY hot (non-StatusArchived) events                │
├─────────────────────────────────────────────────────────────────────┤
│ Tier 2 — COLD ledger (append-only, per tenant, never rewritten)     │
│                                                                     │
│  receipts  (versioned records: v1 ledgered, v2 indexed, v3 archived)│
│  segments  (immutable after creation)                               │
│  checkpoints (immutable after creation)                             │
└─────────────────────────────────────────────────────────────────────┘
```

Why this shape:

- **Bounded rewrite (REQ-2):** per-write cost depends only on the tenant's hot doc
  (streams + in-flight events, bounded by `SegmentSize × streams × archive-latency window`).
  Cold tier is append-only: appends are O(1) in existing records, no clone, no re-serialize,
  no optimistic-lock CAS.
- **Conflict isolation (REQ-8/A4):** hot writes are per-tenant CAS ⇒ cross-tenant writes
  never contend on one version counter. Cold appends have no CAS at all.
- **Eviction (REQ-1):** an archived event is deleted from the tenant hot doc; its payload
  lives only in the archive (I-3: `StatusArchived` ⇒ verified object exists); its receipt,
  segment and checkpoint live in the cold ledger (I-4: hash chain retained).

Persistence mapping:

| Tier | File backend | PostgreSQL |
|---|---|---|
| global hot | `{dir}/control.json` (the existing `statePath` is reused as this file) | `audit_state_snapshot` (existing row; now holds control plane) |
| tenant hot | `{dir}/tenants/{tenantID}.json` | `audit_tenant (tenant_id PK, snapshot jsonb, version bigint)` |
| cold ledger | `{dir}/ledger/{tenantID}.jsonl` (append-only, compaction via tmp+rename+fsync) | `audit_ledger (id bigserial, tenant_id, rec_type, key, version, record jsonb, written_at, UNIQUE (tenant_id, rec_type, key, version))` |

In-memory store (`store.Open("")`) keeps all tiers in memory; cold appends update an
in-memory index built at open (file) or queried via SQL (PG).

---

## 3. Landing increments

Both increments land independently and keep the gate green at every commit.

### Increment 1 — eviction + read fallback + explicit policies (single doc, no layout change)
Satisfies REQ-1, REQ-3, REQ-4, REQ-5 (same-event), REQ-6, REQ-7 and acceptance A1, A3
(single-doc mechanism, A3.5 verbatim). REQ-2/REQ-8 and A2/A4 are **designed but not yet
satisfied** — they land in Increment 2; the A2 benchmark is landed here as a measurement
tool and documents the pre-split cost in `docs/BENCHMARKS.md`.

### Increment 2 — hot/cold split + per-tenant hot docs
Satisfies REQ-2, REQ-8 and acceptance A2, A4; adapts A3.5's crash-atomicity test to the
two-tier commit ordering (§5.2). Includes the file layout migration and PostgreSQL
schema/cutover helper (§7).

---

## 4. Increment 1 — eviction and archive fallback

### 4.1 Eviction inside `ArchivePending` (REQ-1)

`ArchivePending` (governance.go:541–613) keeps its current shape; the batch closure gains
the eviction delete, so the archived-mark and the eviction share **one atomic Save**:

```go
now := s.Now()
if err := s.Store.Update(func(data *store.Snapshot) error {
    for _, event := range archivedEvents {
        key := store.EventKey(tenantID, event.EventID)
        receipt, ok := data.Receipts[key]
        if !ok {
            return domain.ErrNotFound
        }
        receipt.Status = domain.StatusArchived
        receipt.IndexedAt = now
        receipt.ArchivedAt = now
        data.Receipts[key] = receipt
        delete(data.Events, key) // REQ-1: evict in the same atomic Save
    }
    data.ArchiveConflictFailures[tenantID] = 0
    return nil
}); err != nil {
    return 0, err
}
```

Guards:
- Only `archivedEvents` (receipts just marked `StatusArchived` after a **verified** Put) are
  deleted. Pending/unsealed events are never touched (I-4).
- The batch Save is atomic (file fsync/rename, PG version-gated UPDATE) ⇒ a crash cannot
  produce archived-without-evicted or evicted-without-archived within this pass (A3.5 verbatim
  at this increment, fault-injected via `scriptedConflictBackend.saveErr`).
- **Idempotent crash recovery:** an event whose receipt is already `StatusArchived` (from a
  crash between mark and evict — impossible at Increment 1 since both are one Save, but
  relevant after Increment 2) is handled by extending the collection predicate to also pick
  hot events whose receipt is already archived; see §5.2.

### 4.2 Archive read fallback (REQ-3)

New file `internal/service/archive_fallback.go` (keeps service.go under the 1500-line cap):

```go
// eventArchiveKey is the single source of truth for the archive object key,
// shared by archiveEvent and the fallback readers (currently inline at
// service.go:1154).
func eventArchiveKey(tenantID, streamID string, sequence int64, eventID string) string {
    return fmt.Sprintf("events/%s/%s/%020d-%s.json",
        safeName(tenantID), safeName(streamID), sequence, safeName(eventID))
}

// archivedEvent fetches an archived event and verifies it against the
// retained receipt hash chain linkage before returning it. Fail-closed:
// any mismatch (object missing, decode failure, hash mismatch, identity
// mismatch) returns an error, never a silent ErrNotFound.
func (s *Service) archivedEvent(receipt domain.EventReceipt) (domain.Event, error) {
    raw, err := s.Config.Archive.Get(context.Background(), eventArchiveKey(receipt.TenantID, receipt.StreamID, receipt.Sequence, receipt.EventID))
    if err != nil { return domain.Event{}, fmt.Errorf("archived event %s: %w", receipt.EventID, err) }
    var event domain.Event
    if err := decodeEventNumbered(raw, &event); err != nil { return domain.Event{}, err }
    // Byte-identity: the object is CanonicalJSON of the in-memory event
    // (service.go:1150), so decode round-trips to identical canonical bytes
    // (I-3). Cross-check the retained hash linkage:
    if event.EventID != receipt.EventID || event.TenantID != receipt.TenantID ||
       event.StreamID != receipt.StreamID || event.Sequence != receipt.Sequence {
        return domain.Event{}, fmt.Errorf("archived event %s: identity mismatch", receipt.EventID)
    }
    h, err := s.eventHash(event)
    if err != nil || h != receipt.Hash {
        return domain.Event{}, fmt.Errorf("archived event %s: hash does not match retained receipt", receipt.EventID)
    }
    return event, nil
}
```

`decodeEventNumbered` reuses the store `UseNumber` discipline (digit-preserving decode,
store.go:559) so large-int payloads survive the fallback exactly as they did in-snapshot.

Read-surface changes (all read paths keep their existing self-audit fact appends):

| Surface | Change |
|---|---|
| `GetEvent` (service.go:669) | Hot `data.Events` hit ⇒ unchanged. Miss ⇒ look up receipt; if `Status == StatusArchived`, `archivedEvent(receipt)`; else `ErrNotFound`. Verification failure surfaces as an error (integrity failure), not a silent 404. |
| `VerifyIntegrity` (governance.go:345) | Events = hot events + archived events (cold receipts → `archivedEvent`), merged per stream and ordered by sequence; `EventCount` = hot + archived. Chain/merkle/manifest/signature checks unchanged (they re-derive from events + segments). |
| `eventsFor` (service.go:1167) | Returns hot events + archived events (receipt-driven), same predicate. Serves operation/aggregate timeline, replay, restore preview identically pre/post eviction (A3). |
| `runExport` selection (governance.go:857) | Selection over hot + archived events, same `matches` filter; strip-digest → canonical JSONL transform unchanged ⇒ byte-identical export (A3). |
| `holdBlockingExport` (governance.go:812) | Scans hot + archived events so a legal hold continues to block exports containing archived events (F-5). |
| `QueryEvents` (service.go:705) | Archive-inclusive after verified fallback (REQ-7): archived events remain visible with the pre-eviction API semantics; the OpenAPI description documents the WORM read path. |
| `EvaluateRetention` (service.go:357) | **Hot-only, documented policy**: archived events are past the retention window by construction; pinned by test. |

### 4.3 Idempotency (REQ-5)

- Same-event re-ingest of an evicted ID: hot miss ⇒ consult the receipt; `StatusArchived`
  ⇒ fetch `archivedEvent`, derive `EventDigest(decoded)` + `reconstructAndDerive`, compare
  with the input digest ⇒ identical content → `Duplicate=true` (receipt unchanged, no second
  append); different content → `ErrConflict` with `event_id_content_conflict` (service.go:475
  branch, extended to the archive fallback).
- Cross-event idempotency-key scan (service.go:503): add `IdempotencyKey` to `EventReceipt`
  (set when writing receipt v1 in Ingest) so the scan covers archived events from retained
  receipts (F-4; O(K) scan over cold receipts — see Increment 2 §5.3 for the O(1) index).

### 4.4 Migration hazard stays fail-closed (REQ-6)

`CheckFileToPostgresMigrationHazard` (store.go:325) trips when the ledger is non-empty in any
retained form, not just hot events:

```go
if len(data.Events) > 0 || len(data.Receipts) > 0 {
    return fmt.Errorf("file state %s holds %d events / %d receipts; switching to PostgreSQL would start an empty ledger — ... AUDIT_ALLOW_PG_EMPTY_LEDGER=true ...", ...)
}
```

Increment 2 additionally inspects `{dir}/ledger/*.jsonl` (any non-empty log) and the tenant
docs. Extend `TestCheckFileToPostgresMigrationHazard` (store_test.go:194) with an
all-archived/evicted state (N receipts, 0 events) that must still trip.

### 4.5 Increment 1 acceptance

A1 (file slope + payload independence + baseline pin), A3 (end-to-end equivalence, restart,
re-ingest, crash-atomicity verbatim), REQ-6, REQ-7 tests. See §8 table.

---

## 5. Increment 2 — hot/cold split and per-tenant hot docs

### 5.1 Store API changes (internal/store)

New cold-ledger accessor and tenant-scoped primitives; existing `Read`/`Update`/
`UpdateChecked`/`Snapshot` keep their names and CAS semantics but now address the **global**
doc:

```go
// Cold, append-only tier for one tenant. Appends are buffered per pass and
// committed only after the pass's hot Save (HotFirst) or before it
// (ColdFirst); reads always consult the committed log, never the buffer.
type TenantLedger struct{ /* unexported */ }

func (l *TenantLedger) Receipt(key string) (domain.EventReceipt, bool)
func (l *TenantLedger) SetReceipt(r domain.EventReceipt) // appends next version; idempotent by (key, version)
func (l *TenantLedger) Segments(streamKey string) []domain.Segment
func (l *TenantLedger) AppendSegment(s domain.Segment)
func (l *TenantLedger) Checkpoints(streamKey string) []domain.Checkpoint
func (l *TenantLedger) AppendCheckpoint(c domain.Checkpoint)

// TenantView is what tenant closures receive: a private copy of the tenant
// hot doc (Streams, Events) plus the cold ledger accessor.
type TenantView struct {
    Hot    *Snapshot      // tenant-scoped: Streams + Events populated; global maps empty
    Ledger *TenantLedger
}

// Commit order: HotFirst commits the hot Save (per-tenant CAS, bounded
// retry) then the ledger buffer; ColdFirst commits the ledger buffer first
// (used by eviction so the archived-mark always precedes the delete, I-3).
type CommitOrder int
const (
    HotFirst CommitOrder = iota
    ColdFirst
)

func (s *Store) ReadTenant(tenantID string, fn func(*TenantView) error) error
func (s *Store) UpdateTenant(tenantID string, order CommitOrder, fn func(*TenantView) error) error

// Bulk cold read (integrity/export/hazard/backfill) without materializing
// the cold tier into a hot doc.
func (s *Store) LedgerScan(tenantID string, fn func(rec LedgerRecord) error) error
```

`Snapshot` gains an additive `LayoutVersion int json:"layout_version,omitempty"` field so a
v2 layout is self-describing and old files are detectable on open (I-1).

### 5.2 Commit ordering and crash-atomicity (resolves F-2)

| Operation | Order | Crash window | Recovery |
|---|---|---|---|
| Ingest cycle 1 (event+stream+receipt v1+segment+checkpoint) | HotFirst | hot committed, cold missing | Re-ingest duplicate path re-derives and appends the cold records; fail-closed error returned until then (matches today's 503-retry contract); `GetReceipt` derives from the hot event when the cold receipt is missing (documented). |
| Status commits (indexed/archived receipt) | HotFirst | hot untouched (no hot write), cold missing | Idempotent retry appends the same (key, version) — dedupe by unique constraint. |
| ArchivePending (receipt v2 archived + evict) | ColdFirst | receipt archived, event still hot | The transient "archived-with-events-retained" is today's pre-eviction behavior (safe, never lossy); the next pass's collection predicate includes hot events whose receipt is already `StatusArchived` ⇒ evicts and converges. "Evicted-without-archived" is impossible: the delete is the last step. |

A3.5 adaptation (documented deviation): the Increment-1 single-Save fault-injection test stays
as-is (still green). Increment 2 adds two fault-injection points (cold commit fails; hot Save
fails) and asserts the safety properties that matter: (i) eviction never precedes the
archived-mark; (ii) every terminal state converges to archived-and-evicted; (iii) no content
loss; (iv) the transient archived-with-events-retained is explicitly permitted and
self-healing. This preserves the acceptance's *intent* (no lossy or inconsistent states) under
the two-tier constraint that A2 forces.

### 5.3 Cold-ledger read index (bounded per-write cost, O(1) lookups)

- File backend: build per-tenant in-memory index at open by scanning the JSONL once (O(K) at
  startup); maintain on append; compaction (drop superseded receipt versions) is a
  governance-worker task (tmp+rename+fsync, rare, off the hot path).
- PG: latest-record lookup `SELECT record FROM audit_ledger WHERE tenant_id=$1 AND
  record_type=$2 AND key=$3 ORDER BY version DESC LIMIT 1` (indexed); idempotency-key index on
  `(tenant_id, (record->>'idempotency_key'))` expression index.
- `EventReceipt.IdempotencyKey` (additive, F-4): set at ingest receipt v1; old records decode
  with empty key and simply never match (same as pre-eviction absence).
- Ingest's idempotency-key scan moves from `data.Events` to the cold receipt index: O(1)
  per ingest (A2 flatness preserved).

### 5.4 Increment 2 acceptance

A2 (benchmark + CI-gate unit test), A4 (scripted per-tenant CAS + PG harness), the adapted
A3.5 crash tests, layout migration tests. See §8.

---

## 6. API changes — complete list

| # | Location | Change | Compat |
|---|---|---|---|
| 1 | `internal/store` | New `TenantLedger`, `TenantView`, `CommitOrder`, `ReadTenant`, `UpdateTenant`, `LedgerScan` | Additive; existing `Read/Update/UpdateChecked/Snapshot` keep signatures (now global-scoped) |
| 2 | `internal/store` | `Snapshot.LayoutVersion` field (omitempty) | Additive; old files decode with 0 ⇒ v1 detection |
| 3 | `internal/store` | `CheckFileToPostgresMigrationHazard` counts receipts + ledger dir in addition to events | Behavior: stricter (fail-closed) |
| 4 | `internal/domain` | `EventReceipt.IdempotencyKey` | Additive field |
| 5 | `internal/service` | New files `archive_fallback.go`, `eviction.go`, `ledger.go`; `eventArchiveKey` extracted from `archiveEvent` | No exported surface change |
| 6 | `internal/service` | `GetEvent`, `VerifyIntegrity`, `eventsFor` (timeline/replay/restore), `runExport`, `holdBlockingExport`: archive-inclusive | Behavior preserved (equivalence pinned by tests) |
| 7 | `internal/service` | `QueryEvents` archive-inclusive fallback; `EvaluateRetention` hot-window policy | Behavior and policy documented + pinned + release note |
| 8 | `api/openapi/openapi.yaml` | Description notes only (no schema/route change) | Compatible |
| 9 | `migrations/006_hot_cold_split.sql` (new) | `audit_tenant`, `audit_ledger` tables | New tables; existing `audit_state_snapshot` retained as control plane |
| 10 | `cmd/audit-pg-migrate` + `internal/store/postgres_hotcold.go` | `audit-pg-migrate -confirm MIGRATE` explicit PG cutover command and library helper | Additive |

HTTP/OpenAPI surface is otherwise unchanged (non-goal): `GET /events/{id}`, `/receipt`,
`POST /integrity/verify`, `/exports`, replay/timeline routes all keep their contracts.

---

## 7. Compatibility constraints

1. **Old snapshots load (I-1).** v1 file/row decodes via `decodeSnapshot` unchanged.
   Increment 1 writes v1 shape (smaller `Events`). Increment 2's open performs an explicit,
   idempotent split (v1 → global + tenant docs + cold logs), writes a `{statePath}.v1` backup,
   and fails closed if the target layout is partially present (marker = `layout_version`).
2. **Rollback.** File: restore `{statePath}.v1` and remove new siblings. PG: restore the
   backed-up snapshot row, drop new tables. Documented cutover order: Increment 1 ships and
   soaks first (fully reversible, no layout change); Increment 2 ships after.
3. **`StatusArchived ⇒ object present and verified` (I-3) is the eviction precondition** in
   both increments (already enforced by verified Puts).
4. **Reads fail closed.** Fallback verification failure is an error, never a silent 404;
   a missing archive object for an archived receipt is surfaced (archive-tier fault, out of
   this design's scope to repair).
5. **wait_for semantics and self-audit facts unchanged**; receipt `Status` transitions keep
   their current ordering (ledgered → indexed → archived).
6. **Durability floor unchanged.** Cold appends use the same write→fsync discipline as
   `fileBackend.Save`; compaction reuses tmp+rename+dir-chain-sync.
7. **Gate.** All new code lives in new files (service.go stays ≤ 1500 lines); functions
   ≤ 260 lines / ≤ 45 decisions; `gofmt`, `go vet`, unit, race, build, routes, OpenAPI checks
   green per AGENTS.md.

---

## 8. Failure modes

| # | Failure | Detection | Recovery / behavior |
|---|---|---|---|
| FM-1 | Archive object missing/corrupt for an archived receipt | `archive.Get` error or hash mismatch in `archivedEvent`/`VerifyIntegrity`/export | Fail closed with an integrity error (never silent 404). Archive is the WORM source of truth; fallback reads are bounded by `archiveReadTimeout`; restore from archive-tier redundancy/backup remains an external operation. Eviction never deletes archive objects. |
| FM-2 | Crash between cold commit and hot Save (ColdFirst, eviction) | Receipt archived, event still hot | Next `ArchivePending` pass evicts (collection predicate includes hot events with archived receipts); transient state is today's behavior — safe. |
| FM-3 | Crash between hot Save and cold commit (HotFirst, ingest) | Event hot, cold receipt missing | Idempotent re-ingest completes the cold records; `GetReceipt` derives from hot event in the window; fail-closed error until recovered (existing 503/retry contract). |
| FM-4 | Same-tenant hot CAS exhaustion (multi-replica) | `ErrSnapshotConflict` after `snapshotConflictRetries` | Existing retry + backoff + fail-loud mapping (503); per-tenant now, so blast radius shrinks. |
| FM-5 | Duplicate cold appends on retry | — | Dedupe by `(tenant_id, rec_type, key, version)` (PG unique constraint + `ON CONFLICT DO NOTHING`; file index skip). Closure re-run computes the same version from the committed index. |
| FM-6 | Compaction crash | — | tmp+rename+fsync; old log stays valid; compaction reruns. |
| FM-7 | Migration crash mid-split | Partial layout / `layout_version` marker | Idempotent split (only move what is not yet present; verify counts); `.v1` backup prevents loss; fail-closed on inconsistent layout. |
| FM-8 | Silent query/timeline/retention shrinkage | REQ-7 tests + release note | Policy is explicit and pinned; in-snapshot behavior unchanged. |

---

## 9. Migration steps (operator runbook)

**Increment 1** (no layout/schema change):
1. Deploy `audit-api` / `audit-governance-worker` binaries. Existing state files and the PG
   row load unchanged.
2. Run `python3 cli.py quality`; record `BenchmarkIngestWithArchivedLedger` in
   `docs/BENCHMARKS.md` (documents the pre-split K-dependence the direction removes).

**Increment 2**:
1. *File backend:* first open auto-migrates v1 → v2 (split into `control.json` + `tenants/*`
   + `ledger/*.jsonl`, `.v1` backup). Validate on a copied state file first; verify counts
   (events per tenant, receipts, segments) before and after.
2. *PostgreSQL:* apply `migrations/006_hot_cold_split.sql` (new `audit_tenant`,
   `audit_ledger`; `audit_state_snapshot` unchanged). Then run the cutover with the API
   stopped by running `go run ./cmd/audit-pg-migrate -confirm MIGRATE` (or a
   program that calls `store.MigratePostgresSnapshot(db)`): it locks the single row, backs it up in
   `audit_state_snapshot_v1_backup`, inserts per-tenant `audit_tenant` rows and
   `audit_ledger` records, then marks the control row as layout v2 in one transaction.
   Keep the backup table and a database dump until the cutover is validated.
3. *Rollback:* file — restore `.v1`, delete new siblings. PG — restore backup row, drop new
   tables, redeploy previous binaries.
4. *Cutover order:* Increment 1 first (independent, reversible); Increment 2 after soak.

---

## 10. Testable acceptance mapping

| Acceptance | Test (file) | Measurable assertion | Increment |
|---|---|---|---|
| A1 file | `internal/store/hotcold_capacity_test.go` (`TestFileHotColdCapacitySlope`) | retained receipt/segment/checkpoint metadata at N∈{100,1k,5k} grows linearly; 4 KB→8 KB evicted payload leaves control/tenant/ledger size within slack; WORM archive bytes are measured separately | 1 / 2 |
| A1 PG | `internal/store/postgres_hotcold_test.go` (`TestZPostgresHotColdCapacityEnvelope`) | DSN-gated workload via `OpenPostgres`; assert `pg_column_size` bounds on control row + tenant rows + ledger records; reset rows/tables first, no `t.Parallel()` | 2 |
| A2 bench | `internal/service/bench_test.go` (`BenchmarkIngestWithArchivedLedger`) | K∈{0,1k,10k,50k} preloaded archived+evicted; report ns/op + B/op per K; recorded in `docs/BENCHMARKS.md`; runnable via `go test -bench=BenchmarkIngestWithArchivedLedger -benchmem ./internal/service` | 1 (records pre-split cost) |
| A2 gate | `TestIngestCostIndependentOfArchivedEvents` (unit, in-memory store) | K=10k vs K=0: single `Ingest` ≤ 3× wall and ≤ 3× allocs (`AllocsPerRun`); `cost(10k)−cost(1k) < cost(1k)` (marginal flat ⇒ O(1) w.r.t. archived events) | 2 |
| A3 e2e | `internal/service/archive_eviction_test.go` (real archive dir, both backends, restart) | `TestArchivedEventEvictionKeepsReadSemantics` covers verified `GetEvent`, query, timeline, operation/aggregate replay and re-ingest conflict; `TestArchivedExportDigestMatchesHotExport` pins export count/digest equivalence; `TestArchivedEventEvictionSurvivesFileRestart` covers restart durability and integrity | 1 / 2 |
| A3.5 crash | `internal/store/hotcold_test.go` (`TestHotFirstColdFailureRetainsDurableHotState`, `TestColdFirstHotFailureLeavesRecoverableArchivedState`) | injected cold append failure never loses the durable hot event; injected hot save failure leaves archived receipt plus retained hot body; cleared fault converges to eviction | 2 |
| A4 | `internal/store/postgres_hotcold_test.go` (`TestTenantCASConflictPartition`, `TestZPostgresTenantCASNoCrossTenantConflicts`) | scripted R=4/M=200 partition has zero cross-tenant conflicts and non-zero same-tenant baseline; optional PG R=4 cross-tenant writes complete without `ErrSnapshotConflict` | 2 |
| REQ-6 | extend `TestCheckFileToPostgresMigrationHazard` (store_test.go:194) | state with N receipts / 0 events (archived+evicted) still trips; Increment 2: non-empty ledger dir trips; error names `AUDIT_ALLOW_PG_EMPTY_LEDGER` | 1 / 2 |
| REQ-7 | `TestArchivedEventEvictionKeepsReadSemantics`, `TestEvaluateRetentionHotWindowPolicy` (service) | verified archive fallback preserves event/query/timeline/replay/export/integrity behavior after eviction; retention remains a hot-window policy | 1 |
| REQ-5 | A3 re-ingest assertions + `TestReingestArchivedEventIdempotency` | Duplicate/Conflict/no duplicate append; digest comparison exact (UseNumber) | 1 |

Exit criteria: all assertions green on both backends; `python3 cli.py quality` green at every
commit; `docs/BENCHMARKS.md` records K-sensitivity before and after Increment 2.

---

## 11. Non-goals (unchanged from the requirements spec)

- Ingest status-error handling (already fixed; separate direction).
- Version CAS / optimistic locking inside the file snapshot JSON. (The file backend
  now enforces a single live writer instead: `store.Open` holds a process-lifetime
  exclusive advisory flock on the sidecar `<path>.lock` (`0o600`, `O_NOFOLLOW`,
  `LOCK_EX|LOCK_NB` with bounded retry; the `lockReplayState` pattern in
  `internal/kafka/replay.go`), released on `Close` or process death — no stale-lock
  cleanup. A second `audit-api`/`audit-governance-worker` on the same `-state` path
  fails at startup with `store.ErrStateFileLocked` naming the path and holder.
  flock is advisory and filesystem-dependent (NFS caveat): the file backend remains
  a single-node mode; the PostgreSQL backend (version CAS) remains the multi-replica
  path.)
- Export format/encryption, legal-hold logic, outbox relay, Kafka DLQ replay, OpenAPI surface.
- Archive write semantics (WORM verification, read-back) or object keys.
- Background GC/purge of receipts or segments — receipts + hash chain are retained by design.
- Fixing the pre-existing unbounded growth of `AdminActions` / `Exports` / `RestoreRuns` in
  the global doc (separate concern; unchanged by this direction).
