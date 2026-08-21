# Design — Project only ledgered events; reject pre-ledger rows at `Store.Insert`

Status: **Implemented in the reference tree** · Date: 2026-08-20 · Module: `cmd/audit-projector`,
`internal/projection` (+ `internal/kafka` constant already declared)
Related: `docs/proposals/ledgered-projection-pipeline.md`,
`docs/proposals/projection-field-protection.md` (sibling directions)

This design is produced from the requirements spec `requirements.md` (run
`project-only-ledgered-events-reject-pre-ledger-r-2f296b78`), which was
treated as **untrusted evidence and re-verified against the working tree**
at commit `b7aa627` (clean). Every claim used below was re-checked; the
verification ledger is in §2, including two material findings the spec does
not cover (§2.2).

Implementation note: the historical accepted-topic projector path is now
guarded by `projection.ErrNotLedgered`, the default source is
`kafka.TopicLedgered`, and the API's durable post-ledger publisher feeds that
topic. The verify compose/e2e path creates the topic and enables publication;
the sections below are retained as the migration and failure-mode reference.

---

## 1. Problem (one paragraph)

Historically, `cmd/audit-projector` defaulted to `kafka.TopicAccepted` and
wrote whatever arrived through `projection.Store.Insert`, which had no
chain-state presence guard. The accepted
topic is published **before** the ledger assigns `StreamID`/`Sequence`/
`PrevHash`/`Hash` (`cmd/audit-outbox-relay/main.go:59` → `service.go:431`
strip → `service.go:522–525` commit closure), so every accepted-topic row
enters the ClickHouse query surface with `stream_id=''`, `sequence=0`,
`event_hash=''` — including events the ledger later rejects (400/409/422).
Result: non-facts in the compliance query surface, hash-chain integrity
queries can never match a receipt, and ADR-0004 §4
("projector 消费 `audit.events.ledgered.v1` 构建投影") is violated even
though `kafka.TopicLedgered` (`kafka.go:34`) and the AsyncAPI `ledgered`
channel (`api/asyncapi/asyncapi.yaml:24`) already exist.

## 2. Evidence verification ledger

### 2.1 Claim-by-claim (all re-checked at b7aa627)

| Claim | Verified? | Where |
|---|---|---|
| E1 projector defaults to `TopicLedgered` | ✅ (implemented) | `cmd/audit-projector/main.go` |
| E2 projector ingests via `projection.Store.Insert` | ✅ | `main.go:42` — `kafka.IngestFunc(store.Insert)` |
| E3 `schemaDDL` has `stream_id`/`sequence`/`event_hash` | ✅ | `projection.go:44,45,52`; `ReplacingMergeTree(occurred_at) ORDER BY (tenant_id, occurred_at, event_id)` |
| E4 `Insert` rejects missing chain state | ✅ (implemented) | `projection.Store.Insert` returns `ErrNotLedgered` before payload encoding/DB access |
| E5 `TopicAccepted` at kafka.go:24 | ✅ | `kafka.go:24` |
| E6 `TopicLedgered` is wired | ✅ (implemented) | API durable publisher and projector default use `kafka.TopicLedgered` |
| E7 strip of client `stream_id` | ✅ (line drift confirmed) | `service.go:426–431`; `event.StreamID = ""` at :431 |
| E8 chain assignment in commit closure | ✅ (line drift confirmed) | `service.go:519–525`; stamps at :522–525, `stream.NextSequence++` :529 |
| E9 AsyncAPI `ledgered` channel | ✅ | `asyncapi.yaml:24–25` → address `audit.events.ledgered.v1` |
| E10 ARCHITECTURE_PLAN §11.2 rebuild mandate | ✅ | `docs/ARCHITECTURE_PLAN.md:424` |
| E11 sibling proposal exists | ✅ | `docs/proposals/ledgered-projection-pipeline.md` |
| F1 `domain.Event` chain-field types | ✅ | `models.go:79` `StreamID string`, :80 `Sequence int64`, :82 `Hash string` |
| F2 ADR-0004 §4 | ✅ | `docs/adr/ADR-0004.md:22` |
| F3 `Insert` sole non-test caller is the projector | ✅ | grep: only `main.go:42` (method value) + `projection_test.go:39,43` |
| F4 existing fixture is post-ledger-shaped | ✅ | `projection_test.go:46–48` — `StreamID="demo:source:demo"`, `Sequence=1`, `Hash="abc123"`; DSN-gated :16–19 |
| F5 config log printed after `Open`/`EnsureSchema` | ✅ | `main.go:41` after :27–29/:36–38; not observable when ClickHouse down |
| F6 `NewConsumer` fallback only when `topic==""` | ✅ | `kafka.go:228–229` |
| F7 binary-test precedent | ✅ | `cmd/audit-kafka-consumer/main_test.go:27`, `cmd/audit-governance-worker/main_test.go:704` |
| F8 `ErrNotLedgered` absent from tree | ✅ | repo-wide grep: no match |
| F9 `cmd/audit-projector/main_test.go` absent | ✅ | dir contains only `main.go` |
| F10 plain-error semantics, 8 attempts, commit+log DLQ | ✅ | `kafka.go:197` (`WithMaxAttempts` default 8); :355–370 dead-letter degrades to commit+log; projector attaches no `WithDLQ` |
| F11 relay publishes accepted pre-ledger | ✅ | `cmd/audit-outbox-relay/main.go:59` |
| F12 AsyncAPI↔code gate aligned | ✅ | `python3 -m checks.asyncapi_channels` → `PASS: asyncapi channels (5 send channels, 5 symbols aligned, undeclared topics: 0)` |
| F13 budgets / build list | ✅ | `engineering.yaml:10–12` (1500/260/45); `audit-projector` in build list :27–28; `cli.py quality` composition verified |
| Baseline gate | ✅ | `python3 cli.py quality` → `QUALITY PASS` at b7aa627 |

**Net:** every evidence claim the design relies on is confirmed. The two
line-drift corrections the spec itself documents (E6, E8) are correct.

### 2.2 Material findings the spec does NOT cover

- **N1 — `test/e2e/fullstack.sh:212–216` asserts the projection row exists**
  (`SELECT count() FROM audit.audit_events WHERE event_id='$EVENT_ID'` must
  be `1`) and `deploy/docker-compose.verify.yml:134–142` runs `audit-projector`
  with **no `AUDIT_KAFKA_TOPIC`** (only `AUDIT_KAFKA_BROKERS` +
  `AUDIT_CLICKHOUSE_DSN`). With the default flipped and REQ-1 active, this e2e
  cannot pass until the sibling **ledgered producer** lands (accepted-topic
  traffic is now rejected; ledgered traffic does not exist yet). The spec's
  out-of-scope list omits this surface. It is not part of `cli.py quality`
  (docker-based, manual), so AC-5 is unaffected — but the design must sequence
  it (see §6.3) or the e2e silently fails during the transition window.
- **N2 — sibling `docs/proposals/projection-field-protection.md` §3.2 makes the
  identical projector-side changes** (default → `TopicLedgered`, log-first
  reorder) plus `internal/service/ledgered.go` producer wiring. The projector
  changes are **idempotent across the two directions** (both land to the same
  final text), but landing order matters for merge churn and for who owns the
  e2e/compose updates. Coordinate to avoid one direction reverting the other.
- **N3 — env-clean in the new binary test must also strip
  `AUDIT_CLICKHOUSE_DSN`.** The kafka-consumer test pattern (F7) strips only
  `AUDIT_KAFKA_*`. Without stripping `AUDIT_CLICKHOUSE_DSN`, a developer with
  a live-Ch env var makes the binary reach `consumer.Run` and the exit
  path/timing become environment-dependent. AC-3.1 says "no ClickHouse
  override (unreachable default DSN)" — the harness must guarantee that.
- **N4 — `-topic=""` edge:** `kafka.NewConsumer` falls back to
  `TopicAccepted` when `topic == ""` (`kafka.go:228`). A deploy passing an
  empty override silently re-points the projector at the accepted topic; with
  REQ-1 active this yields `ErrNotLedgered` on every row (fail-loud, not
  garbage), and the first log line names the resolved topic. Documented as
  FM-7.

---

## 3. API changes (concrete)

### 3.1 `internal/projection/projection.go` — new exported sentinel + guard

Add stdlib `errors` import (currently absent) and the sentinel; guard as the
**first statement** of `Insert`, before payload encoding and before any
database access:

```go
// ErrNotLedgered is returned when an event lacks complete ledger-assigned
// chain state; such an event must never materialize into the query surface.
var ErrNotLedgered = errors.New("projection: event lacks ledger-assigned chain state")

func (s *Store) Insert(ctx context.Context, event domain.Event) error {
	// Presence guard FIRST: before payload encoding and before any database
	// access, so a zero-value *Store (nil db) returns ErrNotLedgered without
	// panicking (AC-1.1 regression probe) and no DB round-trip is wasted.
	if event.StreamID == "" || event.Sequence <= 0 || event.Hash == "" {
		return ErrNotLedgered
	}
	payload, err := domain.CanonicalJSON(event.Payload)
	// ... unchanged ...
}
```

Semantics:
- **Presence, not authenticity.** The projection never re-derives chain
  fields; authenticity is guaranteed upstream (ledger stamps at
  `service.go:522–525`; client values stripped/overwritten at :431).
- **Type-exact.** `StreamID string`, `Sequence int64`, `Hash string`
  (F1). `Sequence <= 0` also rejects negative forged values that would
  otherwise fail the `sequence UInt64` column.
- **No row is written** for unledgered events; `ErrNotLedgered` is returned
  bare (sentinel identity preserved for `errors.Is`).
- The existing post-ledger-shaped fixture (F4) passes unchanged.

### 3.2 `cmd/audit-projector/main.go` — default topic flip + log-first

```go
	topic := flag.String("topic", envOr("AUDIT_KAFKA_TOPIC", kafka.TopicLedgered), "source topic") // was TopicAccepted
```

and move the resolved-config line to be the **first stdout line**, after flag
parsing and broker/backoff validation, **before** `projection.Open` /
`EnsureSchema`:

```go
	flag.Parse()
	if *brokers == "" {
		log.Fatalf("brokers are required: pass -brokers or set AUDIT_KAFKA_BROKERS")
	}
	if *backoff <= 0 {
		log.Fatalf("backoff must be positive")
	}
	logger := log.New(os.Stdout, "audit-projector ", log.LstdFlags|log.Lmicroseconds)
	// FIRST LINE: resolved config, observable even when ClickHouse/Kafka is
	// down (REQ-3 / AC-3).
	logger.Printf("brokers=%s topic=%s group=%s clickhouse=%s", *brokers, *topic, *group, *dsn)
	store, err := projection.Open(*dsn) // :27-29 unchanged
	// ...
	// the old :41 log line is removed (moved above)
```

Unchanged: consumer group id `audit-projector`, `kafka.IngestFunc(store.Insert)`,
`FirstOffset` start (`kafka.go:235`) — a fresh group backfills the ledgered
topic from its beginning, giving the ADR-0004 §4 rebuild property.

### 3.3 `internal/kafka` — no change

`TopicLedgered` already exists (`kafka.go:34`); the AsyncAPI gate already
aligns it (F12). No new topic, channel, or constant.

### 3.4 API surface summary

| Item | Change | Kind |
|---|---|---|
| `projection.ErrNotLedgered` | new exported sentinel | additive |
| `Store.Insert` contract | returns `ErrNotLedgered` for unledgered rows before any DB work | additive rejection case; no signature change |
| `Store.Insert` return values | existing success/error paths unchanged | none |
| `cmd/audit-projector` flag default | `TopicAccepted` → `TopicLedgered` | behavioral default change |
| `kafka.IngestFunc`/consumer wiring | unchanged | none |
| `internal/kafka` | unchanged | none |
| AsyncAPI / OpenAPI / proto | unchanged | none |

## 4. Compatibility constraints

1. **Additive contract.** `Insert`'s only in-tree non-test caller is
   `cmd/audit-projector` (F3). No existing return value changes meaning;
   `ErrNotLedgered` is a new error identity, safe to `errors.Is` against.
2. **Accepted-topic contract untouched.** Relay, ledger-consumer, DLQ
   classification, and 400/409/422 semantics unchanged. Only the projector's
   **default** source moves; `-topic` / `AUDIT_KAFKA_TOPIC` overrides keep
   working for the migration window (now fail-loud, never garbage — §7).
3. **No new topic/channel.** `TopicLedgered` and the AsyncAPI `ledgered`
   channel already exist and are gate-aligned; `checks/asyncapi_channels.py`
   Rule A/B output stays byte-identical (verified at baseline: 5 send
   channels, 5 symbols aligned, undeclared topics: 0).
4. **Existing integration fixture (F4) passes the guard unchanged** —
   `StreamID="demo:source:demo"`, `Sequence=1`, `Hash="abc123"`; duplicate
   insert (at-least-once redelivery) still succeeds.
5. **Dependency direction.** `internal/projection` gains only stdlib
   `errors`; stays dependency-free of `internal/kafka`/`internal/service`.
   `checks/architecture.py` forbidden edges untouched.
6. **`-topic=""` edge (N4).** Explicit empty override falls back to
   `TopicAccepted` inside `NewConsumer` — fail-loud via the guard, never
   garbage. The first log line names the resolved topic.
7. **e2e/verify-compose (N1).** `fullstack.sh:212–216` and
   `deploy/docker-compose.verify.yml:134–142` assume accepted-topic traffic
   reaches the projection. Neither is part of `cli.py quality`; both must be
   sequenced with the sibling ledgered-producer direction (§6.3).
8. **Sibling overlap (N2).** `projection-field-protection.md` §3.2 makes the
   same projector edits. Landing order is coordinated in §6; the changes are
   textually idempotent, so the second direction to land must **not revert**
   the first and must own the doc/e2e drift.
9. **Docs drift (non-gated).** `README.md:60`, `docs/VALIDATION_PLAN.md:111`,
   and `docs/ARCHITECTURE_PLAN.md` §6.4 describe the projector consuming the
   accepted topic. Updating them is recommended in step 3 of §6.1 (non-gated;
   `checks/adr_compliance.py` only requires file presence).
10. **Budgets.** `projection.go` grows ~12 lines (~108 total) and `Insert`
    ~3 lines; `main.go` reorders ~5 lines — well under 1500 lines/file, 260
    lines/function, 45 decisions (`engineering.yaml:10–12`). No helper
    extraction needed.

## 5. Failure modes

| # | Failure | Detection | Impact | Handling |
|---|---|---|---|---|
| FM-1 | Pre-ledger/forged message reaches projector ingest | REQ-1 guard | No garbage row | `ErrNotLedgered`; no write; retried to the per-message cap (default 8) then dead-letter commit+log (F10). |
| FM-2 | Projector misconfigured to accepted topic | `ErrNotLedgered` on every row; first log line names the topic (REQ-3) | No projection writes; loud failure | Ops error; override honored for migration; fail-loud by design. |
| FM-3 | Rejected event (400/409/422) reaches projector ingest | REQ-1 guard | No row | Structural: rejected events never receive chain state; the guard rejects. |
| FM-4 | Duplicate redelivery of a ledgered message | — | Idempotent by `event_id` | At-least-once + `ReplacingMergeTree` dedup on `(tenant_id, occurred_at, event_id)` ⇒ convergent. |
| FM-5 | Guard placement regression (moved after DB access) | AC-1.1 nil-`db` probe | Nil panic instead of `ErrNotLedgered` | Unit test fails; guard must precede payload encoding and DB access. |
| FM-6 | Ledgered topic empty (producer not yet deployed) | Consumer lag ≈ 0 | Projection stays empty; no garbage | Correct fail-safe; sibling direction adds the producer; ADR-0004 §4 rebuild is the backstop. |
| FM-7 | `-topic=""` (N4) | `NewConsumer` fallback to accepted | Accepted-topic rows all rejected | Fail-loud guard evidence + first log line names resolved topic. |
| FM-8 | fullstack e2e run before ledgered producer (N1) | CH projection assertion fails at `fullstack.sh:216` | E2E red | Sequence: run e2e only after the sibling producer lands, or have that direction update the assertion. |
| FM-9 | Two sibling directions land conflicting projector edits (N2) | merge conflict / revert | Churn | §6 landing order; idempotent final text; owner for e2e/doc updates assigned. |

**Guarantee summary:** the projection never contains unledgered facts
(FM-1/2/3/7); forged chain values never materialize (guard + service
strip/overwrite); hash-chain integrity queries against the projection become
meaningful once ledgered events flow (AC-2.1); the gate never silently passes
contract drift (AC-4/AC-5).

## 6. Migration steps

### 6.1 Code landing order (each step `python3 cli.py quality`-green)

1. **REQ-3 first (log-first).** Move the resolved-config log line before
   `projection.Open`/`EnsureSchema`. Add `cmd/audit-projector/main_test.go`
   with the `TestMain` build + `runBinary` harness (env-clean strips
   `AUDIT_KAFKA_*` **and** `AUDIT_CLICKHOUSE_DSN`, N3) and a **baseline**
   `TestResolvedDefaultTopicIsLedgered` asserting the *current* default
   (`topic=audit.events.accepted.v1`) so the harness is exercised gate-green
   before the flip.
2. **REQ-1 (guard).** `ErrNotLedgered` + guard; add AC-1.1
   (`TestInsertRejectsUnledgered`, nil-`db`, table-driven, one subtest per
   case) and AC-1.3/AC-2.2 integration tests (DSN-gated). Confirm AC-1.2
   (existing fixture) still green.
3. **REQ-2 (default flip).** `TopicAccepted` → `TopicLedgered`; flip the
   AC-3.1 assertion to `topic=audit.events.ledgered.v1`; add the AC-3.2
   override sub-case (`-topic=audit.events.accepted.v1` and
   `AUDIT_KAFKA_TOPIC=...` env form). AC-4.2 grep satisfied. Recommended
   (non-gated): update `README.md:60`, `VALIDATION_PLAN.md:111`,
   `ARCHITECTURE_PLAN.md` §6.4.
4. **AC-2 integration tests** (`TestClickHouseRoundTripsChainColumns`,
   `TestClickHouseRejectsPreLedgerRow`) — DSN-gated, distinct `event_id`s to
   avoid cross-test interference with the existing `projection-it-1` fixture.
5. **Full gate** — `python3 cli.py quality` exit 0; optional
   `python3 cli.py security-scan` when gosec is available.

### 6.2 Deployment order (production)

1. **Deploy the guard first** (projector still on the accepted topic).
   Expected: projection stops receiving rows; `ErrNotLedgered` retry/dead-
   letter evidence in projector logs; first log line names
   `topic=audit.events.accepted.v1`. This is the deliberate fail-loud window
   (§7.2) — no garbage, no silent change.
2. **Land the sibling ledgered producer** (publication hook +
   `cmd/audit-api` wiring, per `ledgered-projection-pipeline.md` / the
   `internal/service/ledgered.go` producer of `projection-field-protection.md`).
   Ledgered traffic now flows.
3. **Deploy the default flip.** Verify the first log line reads
   `topic=audit.events.ledgered.v1`; the fresh consumer group backfills from
   `FirstOffset` (rebuild property).
4. **Ops: purge historical garbage rows** (`stream_id=''`/`sequence=0`/
   `event_hash=''`) from `audit_events` — table rebuild/truncate per ADR-0004
   §4 / ARCHITECTURE_PLAN §11.2. **Out of scope for code** (ops procedure).
5. **Rollback:** revert the flag default to `TopicAccepted` → with the guard
   active, fail-loud (never garbage); reverting the guard itself restores old
   behavior and is not recommended — the guard is the safety property.

### 6.3 e2e / compose sequencing (N1)

- Do **not** run `test/e2e/fullstack.sh` between step 1 and the sibling
  producer landing: its "verifying ClickHouse projection" assertion
  (`:212–216`) will be red by design.
- `deploy/docker-compose.verify.yml:134–142`: the projector service needs no
  `AUDIT_KAFKA_TOPIC` change — after the flip it resolves `TopicLedgered`
  automatically; the e2e assertion becomes meaningful only when the ledgered
  producer exists. The sibling direction should own any e2e assertion update
  (it already extends the pipeline to "accepted→ledger→projector", per
  `projection-field-protection.md:409`).

## 7. Testable acceptance mapping

| AC | Test (name) | File | Harness / gate | Assertion | Green at step |
|---|---|---|---|---|---|
| AC-1.1 | `TestInsertRejectsUnledgered` | `internal/projection/projection_test.go` | plain `go test`, **no ClickHouse**; zero-value `*Store` (nil `db`); table-driven, one subtest per case `{StreamID:""}`, `{Sequence:0}`, `{Hash:""}` | `errors.Is(err, ErrNotLedgered)`; nil-`db` probe proves no DB touch ⇒ no row written | 2 |
| AC-1.2 | `TestClickHouseProjectionIntegration` (unchanged) | same | `AUDIT_TEST_CLICKHOUSE_DSN` (skip otherwise) | insert ok, duplicate ok, `CountTenant("demo") != 0` | 2 (regression) |
| AC-1.3 | `TestClickHouseRejectsPreLedgerRow` | same | DSN-gated; **distinct** `event_id` | `ErrNotLedgered` + `SELECT count() ... WHERE event_id=?` = 0 | 2 |
| AC-2.1 | `TestClickHouseRoundTripsChainColumns` | same | DSN-gated; distinct `event_id`; post-ledger shape (`StreamID≠""`, `Sequence>0`, `Hash≠""`) | `SELECT stream_id, sequence, event_hash` equals input (all non-empty) | 4 |
| AC-2.2 | `TestClickHouseRejectsPreLedgerRow` (same as AC-1.3) | same | DSN-gated; pre-ledger shape (`StreamID=""`,`Sequence=0`,`Hash=""`) | `ErrNotLedgered` + count 0 (**zero rows**) | 2/4 |
| AC-3.1 | `TestResolvedDefaultTopicIsLedgered` | `cmd/audit-projector/main_test.go` (new, F7 pattern) | `go build` into `t.TempDir()` (TestMain), `exec.CommandContext` + timeout, env-clean strips `AUDIT_KAFKA_*`+`AUDIT_CLICKHOUSE_DSN` (N3), run with `-brokers=127.0.0.1:1` and no CH override | **first log line** contains `topic=audit.events.ledgered.v1` | baseline at 1 (asserts accepted), flipped at 3 |
| AC-3.2 | `TestTopicOverride` | same | `-topic=audit.events.accepted.v1` and `AUDIT_KAFKA_TOPIC=...` env form | first log line shows the override | 3 |
| AC-4.1 | grep `const TopicLedgered = "audit.events.ledgered.v1"` | `internal/kafka/kafka.go:34` | review/grep; value pinned by `checks/asyncapi_channels.py` Rule B | constant present, exact value | existing |
| AC-4.2 | grep `kafka.TopicLedgered` in `cmd/audit-projector` non-test Go | `main.go:19` flag default | review/grep | reference present (flipped from `TopicAccepted`) | 3 |
| AC-4.3 | `checks/asyncapi_channels.py` | gate | `python3 cli.py quality` | `PASS: asyncapi channels (5 send channels, 5 symbols aligned, undeclared topics: 0)` | all |
| AC-5 | `python3 cli.py quality` | gate | full gate (AGENTS.md) | exit 0: gofmt, file/function/decision budgets, dependency direction, root policy/security invariants, routes↔OpenAPI, asyncapi channels, `go vet`, python checks, unit tests, race tests, production builds (incl. `audit-projector`) | final |

Non-gated verification (N1): `test/e2e/fullstack.sh` CH-projection assertion
— expected red until the sibling producer lands; owned by that direction.

## 8. Out of scope (do not implement here)

Producing `audit.events.ledgered.v1` (publication hook in `internal/service`,
`cmd/audit-api` wiring — sibling directions); AsyncAPI/OpenAPI/proto contract
changes; DLQ wiring to the projector (`kafka.WithDLQ`, `-dlq-topic`,
`-max-attempts`); `TopicProjection`/`TopicArchive` producers; ClickHouse
engine/sort-key/partition/schema changes; accepted-topic contract changes;
HTTP status mapping changes; purging historical garbage rows (ops procedure,
§6.2 step 4).
