# Amortize the read self-audit trail compaction

Every audited read appends one fact to the read self-audit trail. Past the
operator cap (`AUDIT_ADMIN_TRAIL_CAP` / `-admin-trail-cap`, default `100000`)
**every** audited read used to pay an O(cap) compaction cost, and because
`recordReadAction` is fail-closed, that cost landed directly on the availability
of the read path the audit surface must keep serving:

- **file backend** — `compactTrail` reset `trailCount` to exactly the cap, so
  the next append tripped the trigger again: a full ~100k-line `os.ReadFile` +
  100k `json.Unmarshal` decode, 100k `json.Marshal` writes, two fsyncs, a
  rename, a directory-chain fsync and an fd reopen **per audited read**.
- **postgres backend** — `trailCount` was never reset after
  `trailCompactQuery`, so every append past the cap issued a
  `DELETE … OFFSET ($1-1)` index scan **while holding `trailMu`**, serializing
  every other audited read's append behind it.
- **in-memory file-mode twin** (`f.path == ""`) copied the newest `cap` facts on
  every overflow.

## Behaviour change

Compaction now trims to a **watermark derived from the operator's cap** and
resets the counter to that watermark, so the trail oscillates in `[cap/2, cap]`
and compaction runs once per half-cap of appends instead of once per append.

Two unexported pure helpers in `internal/store/trail.go`:

```
W(cap) = 0                       if cap <= 0   (unbounded: never compacted)
W(cap) = max(1, cap - max(1,cap/2))   if cap >= 1
H(cap) = cap - W(cap)                  if cap >= 1
```

At the shipped default: one ~100k-line rewrite per **50 001** audited reads
instead of one per read, and one `DELETE` per 50 001 appends instead of one per
append. `trailMu` stops being a per-append serialization point for compaction.

**Explicit, accepted delta:** immediately after a compaction the trail retains
`W(cap)` newest facts instead of `cap` (50 000 rather than 100 000 at the
default). The cap remains the operator-visible upper bound — the trail never
exceeds it. Operators who need `k` facts of *post-compaction* depth should set
`-admin-trail-cap = 2k`.

**Postgres table size (new bounded overshoot).** Resetting the counter makes it
a *lower* bound on the global row count, which is the tightest bound that cannot
over-delete. With `R` replicas sharing one table the row count is bounded by
`W(cap) + (H(cap)+1)·R` (≈150k at the default for `R=2`, ≈300k for `R=5`). Both
drift directions are safe: the `DELETE` — not the counter — is authoritative, so
an over-count only costs a redundant no-op `DELETE` and an under-count only
defers one.

## Compatibility

- `AdminTrail`, `Store.AppendAdminFact`, `Store.ReadAdminTrail`,
  `service.Config.MaxAdminTrailActions`, `resolveAdminTrailConfig`,
  `DefaultMaxAdminTrailActions`, the `-admin-trail-cap` flag and its help text:
  **unchanged**. No new flag, env var, `Config` field, constant, metric, alert,
  runbook threshold or SLO — the divisor is an internal identity of the
  amortization.
- `trailCompactQuery`'s **SQL text**, the table/index DDL and migration 005 are
  unchanged (only the bound parameter's value changes), so the `seq` watermark
  still guarantees a concurrent replica's newer `BIGSERIAL` insert is never
  deleted.
- The compaction chain is byte-for-byte unchanged: read → tmp write → `Sync` →
  `Close` → `os.Rename` → close old fd → `SyncDirChain` → reopen. A pre-rename
  fault still leaves the pre-compaction trail intact and fails the append
  closed; a failed `DELETE` leaves the counter high so the next append retries.
  The corruption policy (malformed non-final line fails closed, truncated final
  line dropped) is untouched.
- No migration and no data migration: the JSONL format, the trail path and the
  column set are unchanged. Rollback is a binary revert — a trail trimmed to `W`
  is read fine (no minimum-count assumption anywhere) and an older binary resumes
  compacting to `cap`.
- `internal/httpapi` production code, `api/openapi`, `api/asyncapi`,
  `api/proto` and the `/metrics` series are **unchanged**.
- **No existing test assertion was edited.** At `cap = 3` the watermark is 2 and
  the compaction interval is 2, so the steady state is `2 + ((n-2) mod 2)` — the
  exact value the old trim-to-the-cap produced. The watermark is pinned instead
  by new assertions.

## Verification

```sh
go test ./internal/store -run 'Trail' -count=1 -v
go test ./internal/httpapi -run 'TestAuditedReadsStayBoundedUnderTrailCap' -count=1 -v
AUDIT_TEST_POSTGRES_DSN=... go test ./internal/store -run 'Trail' -count=1 -v
go test -race -count=1 ./internal/store ./internal/httpapi
python3 cli.py check && python3 cli.py quality
```

New tests: `TestTrailWatermarkDerivedFromCap` (pure watermark/headroom table and
its invariants), `TestTrailUnboundedCapNeverCompacts`,
`TestTrailCompactionAmortizedOnFileBackend` (10 appends past a full cap ⇒ 1
rewrite, where the old code produced 10; reopen baseline recount does not
re-trim), `TestTrailCompactionWatermarkOscillation` (pins the watermark itself),
`TestTrailCompactionNeverExceedsCapForSmallCap` (file and in-memory),
`TestTrailCompactionFaultBeforeRenameLeavesTrailIntact` (pre-rename fault leaves
the trail and tmp path clean, append fails closed, retry compacts),
`TestPostgresTrailAppendAmortizesCompaction` (DELETE arg is the watermark, never
the cap; counter resets; a full headroom issues no `DELETE`),
`TestPostgresTrailCompactionFailureRetries`,
`TestPostgresTrailAmortizationMatchesFileBackend`, and
`TestAuditedReadsStayBoundedUnderTrailCap` (30 audited GETs against a cap of 8:
all `200`, trail never above the cap).

`TestPostgresTrailCompactionWatermark` (DSN-gated) keeps its exact-length and
newest-first assertions and gains the watermark evidence.