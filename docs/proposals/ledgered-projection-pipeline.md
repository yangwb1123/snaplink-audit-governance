# Design — Ledger-derived ClickHouse projection (AsyncAPI `ledgered`/`projection`/`archive` channels)

Status: Proposed · Date: 2026-08-11 · Module: `api/asyncapi` · Direction: "ClickHouse
projection is built from pre-ledger accepted events: chain fields are empty and
non-ledgered facts enter the compliance query surface"

This document treats the requirements specification (`docs/auto/runs/.../requirements.md`)
as **untrusted claims**. Section 1 re-verifies every claim against the repository.
Sections 2–9 give the concrete design: API changes, compatibility constraints,
failure modes, migration steps, and a testable acceptance mapping.

---

## 1. Evidence verification

All nine cited claims were checked against the working tree. **All confirmed.**
Minor discrepancies (line numbers drift by ≤2; E1's channel list omits `accepted`
and `dlq` from its summary) do not change any conclusion.

| # | Claim | Verification |
|---|---|---|
| E1 | `asyncapi.yaml` declares `ledgered`/`projection`/`archive` (and `accepted`, `dlq`) with `publishLedgered`/`publishProjection`/`publishArchive` (`action: send`); `stream_id` documented "Server-assigned; a client-supplied value is stripped on ingest." | **Confirmed.** Channels lines 9–28, operations lines 38–52, `stream_id` description at the `EventEnvelope` schema. |
| E2 | ADR-0004 §4: "projector 消费 `audit.events.ledgered.v1` 构建投影" | **Confirmed.** `docs/adr/ADR-0004.md` line 22. |
| E3 | `cmd/audit-projector/main.go` defaults to `kafka.TopicAccepted`; ingest is `store.Insert`; group `audit-projector` | **Confirmed.** main.go:19, :37, :20. |
| E4 | `internal/kafka/kafka.go` defines only `TopicAccepted`/`TopicDLQ`; repo-wide grep shows no `TopicLedgered`/`TopicProjection`/`TopicArchive` | **Confirmed.** kafka.go:24,28. Grep over `internal/`+`cmd/` finds no other topic constants; the only `archive` matches are file/S3 archive code, not the channel. |
| E5 | `projection.go` schema has `stream_id`/`sequence`/`event_hash`; `Insert` never validates ledger state | **Confirmed.** `schemaDDL` + `Insert` (projection.go:40,73–82) write `event.StreamID`/`Sequence`/`Hash` with no presence checks. |
| E6 | `service.go:414` strips client `stream_id`; `:505–508` assign `StreamID`/`Sequence`/`PrevHash`/`Hash` inside the commit closure | **Confirmed.** Exact lines in current tree: 414 (strip), 505–508 (assign). |
| E7 | `projection_test.go` sets chain fields on the fixture but asserts only `count != 0` | **Confirmed.** projection_test.go:46–48 set `StreamID`/`Sequence`/`Hash`; assertions at :70–73 check only count. |
| E8 | Permanent rejections (422/400/409) are dead-lettered by `cmd/audit-kafka-consumer`; `outbox/http.go` classifies 4xx≠429 permanent; server maps `ErrInvalid`→400, `ErrConflict`→409, `ErrTenantMismatch`→422 | **Confirmed.** http.go:51–54; server.go:1040 (`ErrInvalid`→400), :1048 (`ErrConflict`→409), :1053–1055 (`ErrTenantMismatch`→422); kafka.go `consume()` immediate dead-letter on `DeliveryError.Permanent`. |
| E9 | Gate never validates AsyncAPI channel↔code alignment: `cli.py:358` file-existence only; `contract_fields.py` regex-only; `route_contract.py` OpenAPI-only; no check parses `asyncapi.yaml` | **Confirmed.** cli.py:358 (`cmd_capabilities`), cli.py:407–420 (quality module list — no asyncapi check), contract_fields.py:44–66, route_contract.py (single `openapi` hit, zero `asyncapi`). |

**Verified current data flow** (matches the spec): outbox relay → `audit.events.accepted.v1`
(pre-ledger, client-supplied or empty chain fields) → two independent consumer
groups: `audit-ledger-consumer` (POSTs to API, ledger assigns chain fields) and
`audit-projector` (inserts **pre-ledger** rows into ClickHouse directly, with
`hash=""`, `sequence=0`, `stream_id=""`, including events the ledger later rejects).
`ledgered`/`projection`/`archive` have no producing code; ADR-0004 §4 is violated.

**Additional verified facts used by the design** (not in the spec):

- **F1.** `service.Ingest` returns a receipt carrying `StreamID`/`Sequence`/`Hash`
  after commit (service.go:523–526); duplicate ingests short-circuit at
  `receipt.Duplicate` before any re-publication point (service.go:540–543) — a
  natural idempotency hook for REQ-1.
- **F2.** `service.Config` has no Kafka/publisher field today; `cmd/audit-api`
  wires no brokers. A publisher must be injected (interface) with a no-op default.
- **F3.** The outbox machinery (`outbox.Relay`, `outbox.PostgresStore`, migrations
  `001`–`004`) is reusable: `PostgresStore` is table-bound (SQL embeds
  `audit_outbox`); a second table + store variant gives at-least-once delivery
  with `event_id` idempotency, mirroring the accepted-topic relay.
- **F4.** `kafka.Consumer` already dead-letters `outbox.DeliveryError{Permanent:true}`
  immediately (kafka.go `consume()`); kafka_test.go:142 pins this.
- **F5.** `checks/` convention: modules expose `run(root=None) -> int`; fixture
  tests follow `dev_auth_manifest`'s temp-tree pattern; `PyYAML 6.0.3` is installed.
- **F6.** `architecture.py` forbids only `domain/security/auth/store → service/httpapi/grpcapi`;
  `internal/projection → internal/outbox` is currently **allowed** by the gate.
- **F7.** `cmd/audit-governance-worker` performs per-tenant `ArchivePending` passes
  (main.go:100) — the REQ-7 hook point; it has no Kafka flags today.

---

## 2. Goals and scope

Goals (from the spec, unchanged): G1 post-ledger-only projection; G2 rejected
events never projected; G3 client-supplied chain fields never projected;
G4 gate fails on channel↔symbol drift.

In scope: `internal/kafka` constants + producer plumbing, ledgered publication
after commit, projector default topic + post-insert publish, `Insert` ledger-state
validation, worker archive-pass publish, new gate check, tests.

Out of scope (unchanged): accepted-topic contract, outbox retry state machine,
DLQ replay, ClickHouse engine/sort key/partitioning, HTTP status mapping, AsyncAPI
3.0 structural validation.

---

## 3. API changes

### 3.1 `internal/kafka/kafka.go` — topic constants

```go
const TopicLedgered    = "audit.events.ledgered.v1"    // post-ledger chain-linked events
const TopicProjection  = "audit.events.projection.v1"  // "indexed" signal, one per successful insert
const TopicArchive     = "audit.events.archive.v1"     // one per completed archival pass
```

No change to `Producer`/`Consumer` signatures: `NewProducer(brokers, topic)` and
`NewConsumer(brokers, topic, group, ingest, ...)` are topic-parameterized already.
`Deliver` serializes `domain.Event` canonical JSON — the exact shape the projector
and downstream subscribers need (chain fields included).

### 3.2 Ledgered publication (REQ-1) — service hook + outbox relay

**`internal/service/service.go`:**

```go
// LedgeredPublisher delivers a post-ledger event to audit.events.ledgered.v1.
// Implementations must be at-least-once and idempotent by EventID. A nil
// Config.LedgeredPublisher disables publication (logged once at startup).
type LedgeredPublisher interface {
    Publish(ctx context.Context, event domain.Event) error
}
// Config gains: LedgeredPublisher LedgeredPublisher  // optional
```

In `Ingest`, after the `if receipt.Duplicate { return receipt, nil }` guard
(service.go:548) and before the archive-status block:

```go
if s.Config.LedgeredPublisher != nil {
    if err := s.Config.LedgeredPublisher.Publish(ctx, event); err != nil {
        // Ledger commit already durable. Never fail ingest on publish failure.
        s.logf("ledgered publish failed event_id=%s error=%v", event.EventID, err)
    }
}
```

The `event` value at that point carries server-assigned `StreamID`/`Sequence`/
`PrevHash`/`Hash` (mutated inside the commit closure, E6) — the exact post-ledger
shape REQ-1 requires. Only committed events reach this point: `ErrTenantMismatch`
(422, service.go:407), `ErrInvalid`/`ErrSchemaNotFound` (400, validateEvent), and
`ErrConflict` (409, service.go:480/494) all return before the closure runs. The
`event` value carries the receipt's chain fields (assigned at service.go:522–526
`receipt.LedgeredAt`/`receipt.StreamID`/`receipt.Sequence`/`receipt.Hash`).

**`internal/outbox/` — at-least-once delivery (mirrors the existing relay):**

- New `outbox.LedgeredStore` (or `PostgresStore` generalized with a table name;
  see §4 compatibility) reading/writing table `ledgered_outbox` with the same
  column shape as `audit_outbox` (id, event_id, tenant_id, idempotency_key,
  payload, occurred_at, status, attempts, next_attempt_at, last_error, created_at).
- `cmd/audit-outbox-relay` gains a `-ledgered-drain` flag (env
  `AUDIT_OUTBOX_LEDGERED_DRAIN=true`): when set, drains `ledgered_outbox` and
  delivers via `kafka.NewProducer(brokers, kafka.TopicLedgered).Deliver` (brokers
  required in this mode). The relay's existing retry/backoff/max-attempts state
  machine is reused unchanged; `event_id` idempotency is preserved by the
  projector's `ReplacingMergeTree` dedup on `(tenant_id, event_id)`.
- `cmd/audit-api` writes the ledgered outbox record post-commit. To keep the
  audit-api free of Postgres-outbox knowledge, the record write is the
  implementation of a small `ledgeredoutbox.Publisher` (new package under
  `internal/` or directly in `internal/outbox`) injected into `service.Config`
  from `cmd/audit-api` when `AUDIT_LEDGERED_BROKERS` + a Postgres DSN are set.
  If Postgres is unavailable, the API logs a startup warning and runs with
  publication disabled (degraded, not broken — see §5).

Rationale: publishing from `service.Ingest` covers **all** ingest transports
(HTTP REST, gRPC, kafka-consumer→HTTP), whereas publishing from
`cmd/audit-kafka-consumer` would silently miss HTTP-delivered events.

### 3.3 Projector (REQ-2, REQ-6) — `cmd/audit-projector/main.go`

```go
topic := flag.String("topic", envOr("AUDIT_KAFKA_TOPIC", kafka.TopicLedgered), "source topic")
```

- Startup log moved **before** `projection.Open`/`EnsureSchema` so the resolved
  topic is observable without a live ClickHouse (enables AC-2.3):
  `logger.Printf("brokers=%s topic=%s group=%s clickhouse=%s", ...)`.
- Post-insert publish: when `-publish-projection` brokers are configured
  (`AUDIT_KAFKA_BROKERS` is already required), construct
  `kafka.NewProducer(brokers, kafka.TopicProjection)` and wrap the ingest func:

```go
ingest := kafka.IngestFunc(func(ctx context.Context, event domain.Event) error {
    if err := store.Insert(ctx, event); err != nil {
        return err // never publish a row that failed to insert
    }
    return projectionProducer.Deliver(ctx, event)
})
```

Publish failure returns an error → `kafka.Consumer` retries (transient) →
re-insert (idempotent, `ReplacingMergeTree`) → publish retried: at-least-once.
Consumer group stays `audit-projector`.

### 3.4 `internal/projection/projection.go` — REQ-3

```go
// ErrNotLedgered is returned when an event lacks complete ledger chain state;
// such an event can never be materialized into the query surface.
var ErrNotLedgered = errors.New("projection: event lacks ledger-assigned chain state")

func (s *Store) Insert(ctx context.Context, event domain.Event) error {
    if event.StreamID == "" || event.Sequence <= 0 || event.Hash == "" {
        return ErrNotLedgered // before any payload encoding or write
    }
    ...
}
```

No row is written for unledgered events. The `kafka.Consumer` treats plain errors
as transient (8 retries, then `attempts_exhausted` dead-letter) — acceptable for
a misconfigured consumer; optionally wrap as
`outbox.DeliveryError{Permanent: true}` to dead-letter immediately (§4.4).

### 3.5 Archive publish (REQ-7) — `cmd/audit-governance-worker/main.go`

- New optional flags: `-kafka-brokers` (`AUDIT_KAFKA_BROKERS`), and use
  `kafka.NewProducer(brokers, kafka.TopicArchive)`. When brokers are empty,
  archive publication is disabled (current behavior preserved).
- After a successful per-tenant `svc.ArchivePending(tenant.ID)` pass
  (main.go:100), publish one envelope:

```go
event := domain.Event{
    EventID: "archive-pass:" + tenant.ID + ":" + passKey(tenant), // deterministic → idempotent
    TenantID: tenant.ID, SourceSystem: "audit-governance-worker",
    EventType: "archive.pass.completed", SchemaID: "archive.pass", SchemaVersion: 1,
    OccurredAt: now, Actor: domain.Actor{ID: "governance-worker"},
    Action: "archive", Outcome: "success",
    DataClassification: "restricted", RetentionClass: "standard",
    IdempotencyKey: same deterministic pass key,
    Payload: map[string]any{"archived": archived, "conflict_failures": failures},
}
```

`passKey` must be deterministic per pass (e.g. sealed-segment count + checkpoint
timestamp truncated to a stable boundary) so a redelivered pass publishes the
same `event_id` — at-least-once, idempotent, matching the AsyncAPI
`AuditEvent`/`EventEnvelope` contract (all required fields present; chain fields
left empty by design — the archive channel is an operational signal, not a ledger
fact; REQ-3 does not apply to it since it never reaches `Insert`).

### 3.6 Gate check (REQ-8) — `checks/asyncapi_channels.py`

New module, wired into `cli.py` `cmd_quality()` between `contract_fields` and
`proto_sync`. Uses `yaml.safe_load` (PyYAML 6.0.3 installed; gate already
requires Python tooling).

Algorithm:

1. Parse `api/asyncapi/asyncapi.yaml`; collect operations with `action == "send"`,
   resolve each `channel.$ref` → channel → `address`.
2. Collect the topic literal `"audit.events.*.v1"` from every non-test `.go` file
   under `internal/` and `cmd/` (skip `_test.go`).
3. Collect `NewProducer(` call sites in the same file set; resolve the topic
   argument to a literal via a small resolver:
   - `kafka.TopicX` identifier → const value parsed from `internal/kafka/kafka.go`;
   - `"literal"` → itself;
   - `*flagVar` → the `flag.String("name", <default>, ...)` default, recursively;
   - `envOr("ENV", <fallback>)` → the fallback, recursively;
   - unresolved identifier → ignored (does not count as a producer).
4. A send-channel address **passes** iff it is the resolved topic of ≥1
   `NewProducer(` call in a non-test `.go` file (this covers `accepted`
   [relay/main.go:59], `dlq` [consumer main.go:29→`*dlqTopic`→`TopicDLQ`], and the
   new `ledgered`/`projection`/`archive`).
5. On any missing address, print `FAIL: asyncapi channels` naming each
   address without a producer (sorted), exit 1; else `PASS: asyncapi channels`.

Pre-fix state: `ledgered`/`projection`/`archive` fail; `accepted`/`dlq` pass.
Runs offline in ~milliseconds (three regex passes + YAML parse).

---

## 4. Compatibility constraints

1. **Accepted-topic contract untouched.** The outbox relay still publishes
   pre-ledger events to `audit.events.accepted.v1`; `audit-ledger-consumer`
   behavior, DLQ classification (4xx≠429 permanent), and 400/409/422 semantics
   are unchanged. Only the **projector's default source** moves, and only the
   **default** — `-topic`/`AUDIT_KAFKA_TOPIC` overrides keep working for ops
   that must point at the accepted topic during the migration window.
2. **Projector startup behavior.** With REQ-3 active, an operator who runs the
   projector against the accepted topic (old default) gets `ErrNotLedgered`
   retry/dead-letter noise instead of silent garbage rows — a deliberate
   fail-loud degradation. This is the intended safety property, not a regression.
3. **`projection.Insert` contract tightened.** Existing callers other than the
   projector: none in-tree (`Insert` is called only from `cmd/audit-projector`).
   The integration test fixture (E7) already supplies chain fields, so it
   continues to pass unchanged; new negative tests cover the tightened contract.
4. **Service API additive.** `Config.LedgeredPublisher` is a new optional field;
   nil keeps today's behavior (no publication, log warning). `Ingest` return
   values and receipts unchanged. Existing service tests (F1-adjacent,
   `TestIngestStripsClientStreamID` etc.) unaffected.
5. **Outbox table reuse.** If `PostgresStore` is generalized to take a table
   name, the existing `audit_outbox` behavior must be byte-identical (default
   table name `audit_outbox`); migration `005_ledgered_outbox.sql` creates the
   parallel table. Relay's `-ledgered-drain` mode must not alter the default
   drain path (flag default off).
6. **Gate check heuristic.** The producer-resolution rules are pinned by fixture
   tests (AC-1.2). Any legitimate future producer pattern that the resolver
   cannot resolve will **fail** the gate (fail-closed), forcing either a resolver
   extension or a channel removal — never a silent pass. `accepted`/`dlq` must
   keep passing with **existing** code (no test-fixture-only exemptions).
7. **File/function size budgets.** `engineering.yaml` caps: 1500 lines/file,
   260 lines/function, 45 decisions/function. The `Ingest` function is already
   large; the publication block must be extracted into a helper
   (`publishLedgered(ctx, event, receipt)`), keeping `Ingest` within budget.
8. **Dependency direction.** `internal/projection` may not import
   `internal/service` (not forbidden today, but avoid). The optional permanent
   error for `ErrNotLedgered` uses `internal/outbox` from `internal/projection`
   — allowed by the current gate (F6); if the maintainers prefer, define a
   `PermanentError` marker in `internal/domain` instead (§5 lists this as a
   decision point).
9. **No schema changes.** `api/asyncapi/asyncapi.yaml` and the ClickHouse DDL
   stay as-is; `ReplacingMergeTree(occurred_at) ORDER BY (tenant_id, occurred_at,
   event_id)` already dedups redelivered ledgered events.

---

## 5. Failure modes

| # | Failure | Detection | Impact | Handling |
|---|---|---|---|---|
| F1 | Ledgered outbox record write fails after commit (Postgres down) | API log; `ledgered_outbox` empty while `audit_events` rows accumulate in the projection's absence | Projection stale; **ledger intact** | No rollback (commit is durable). Warning log + metric. Event is missing from the ledgered topic; recovery via rebuild (§7.2). Bounded by design: the API remains functional; publication is best-effort. |
| F2 | Relay → `ledgered` publish fails repeatedly (broker down) | Relay retry/backoff, eventually `attempts_exhausted` dead-letter; outbox rows stay `pending` (delivered_at unset) | Projection stale; no data loss on broker recovery | Existing relay state machine; `event_id` idempotency makes redelivery safe; DLQ replay tool (`cmd/audit-kafka-dlq-replay`) recovers. |
| F3 | Ledgered publish succeeds but projector crashed before insert | Kafka retention + consumer group offset lag | Projection stale | Restart projector; group `audit-projector` re-reads from last commit. At-least-once + dedup ⇒ convergent. |
| F4 | Malicious/forged accepted-topic message with chain fields | REQ-3 gate at `Insert` | No garbage row | `ErrNotLedgered`; retried 8× then dead-lettered (`attempts_exhausted`). If wrapped permanent (decision point §4.8): immediate dead-letter. |
| F5 | Projector misconfigured back to accepted topic | `ErrNotLedgered` storm in projector log + DLQ | No projection writes; loud failure | Ops error; startup log + dead-letter evidence point at the misconfiguration. |
| F6 | Projection publish fails after successful insert | `Deliver` error → consumer retry → re-insert + re-publish | Duplicate `projection` messages (idempotent by `event_id`) | At-least-once semantics as designed; dedup at subscribers. |
| F7 | Archive publish fails (broker down) | Worker log; pass already recorded in store | Missing `archive` signal | Logged warning; next pass re-publishes (deterministic `event_id`). Archive pass itself never blocks on Kafka (publish after success, error not fatal). |
| F8 | Gate check resolver can't resolve a producer arg | Gate FAILS (fail-closed) | CI blocked | Extend resolver; fixture tests pin both directions. |
| F9 | Worker/API started with Kafka env pointing at wrong cluster | Wrong topics written | Cross-cluster pollution | Existing ops controls (broker flags/env per binary); out of scope. |
| F10 | Rejected event (400/409/422) somehow reaches projector ingest | REQ-3 validation | No row | `ErrNotLedgered`/permanent dead-letter. Structural guarantee: REQ-1 never publishes rejected events. |

**Guarantee summary:** the ledger commit is never rolled back by any publication
failure (F1–F3, F6, F7); the projection never contains unledgered facts (F4, F5,
F10); the gate never silently passes channel drift (F8).

---

## 6. Migration steps

Ordered, each step gate-green (`python3 cli.py quality`) before the next:

1. **Step 0 — baseline:** run `python3 cli.py quality`; record green state.
2. **Step 1 — gate check first (red):** add `checks/asyncapi_channels.py` +
   fixtures + wire into `cli.py` `cmd_quality()`. Gate now **fails** naming
   `audit.events.ledgered.v1`, `audit.events.projection.v1`, `audit.events.archive.v1`
   (AC-1.1 pre-fix pinned). Commit the red state deliberately (the check is the
   acceptance harness for everything that follows).
3. **Step 2 — kafka constants:** add `TopicLedgered`/`TopicProjection`/`TopicArchive`
   to `internal/kafka/kafka.go`; unit tests for `NewProducer` default unchanged.
   (Gate still red: no producer call sites.)
4. **Step 3 — REQ-3 validation:** `projection.Insert` ledger-state guard +
   `ErrNotLedgered` + negative unit tests (AC-3.2). Existing integration test
   still green (fixture has chain fields).
5. **Step 4 — projector:** default topic → `kafka.TopicLedgered`; move startup
   log before store open (AC-2.3); add projection producer + wrapped ingest
   (REQ-6) with fake-publisher unit test; integration test AC-2 against
   `AUDIT_TEST_CLICKHOUSE_DSN`. Gate green for `ledgered` + `projection`
   (producers exist in `cmd/audit-projector`).
6. **Step 5 — ledgered publication:** `LedgeredPublisher` in service + post-commit
   publish helper + service tests (AC-3.1 publish-never-called on 400/409/422;
   AC-4.1 forged fields never published); `outbox` ledgered store/table variant;
   migration `005_ledgered_outbox.sql`; relay `-ledgered-drain` mode; audit-api
   flag wiring (`AUDIT_LEDGERED_BROKERS`). Gate green for `ledgered` producer
   (relay) — note the gate resolves the `-ledgered-drain` conditional producer,
   so the `NewProducer(brokers, kafka.TopicLedgered)` call must be unconditional
   in the relay's construction path (flag-gated at runtime, present at compile
   time).
7. **Step 6 — archive publish:** worker `-kafka-brokers` + `TopicArchive`
   producer + publish after successful `ArchivePending` pass + unit test (REQ-7,
   AC-1 passes fully). Gate green.
8. **Step 7 — deployment sequencing (production):**
   a. Deploy relay + API (ledgered outbox starts filling; no consumer yet — rows
      accumulate in `ledgered_outbox`, bounded by normal retry semantics; this is
      the backfill window).
   b. Deploy projector with new default (consumes `ledgered`).
   c. Run one-time backfill: drain the pending `ledgered_outbox` rows (relay
      naturally drains them on startup); for events committed **before** step (a),
      rebuild the projection from the ledger snapshot via the existing
      rebuild path (re-consume or full rebuild per ADR-0004 §4) — rows missing
      from the ledgered topic are re-derived from the ledger itself (the only
      authoritative source), not from the accepted topic.
   d. Remove the old accepted-topic projection lag only after a query-surface
      diff shows no divergence for a full retention window.
9. **Step 8 — final:** full `python3 cli.py quality` + (if available)
   `python3 cli.py security-scan`; update release notes.

---

## 7. Testable acceptance mapping

The four ACs from the spec map to concrete tests; each entry states the file,
the harness, and the assertion.

### AC-1 — Gate fails when a declared channel has no producing symbol
- **AC-1.1 (manual/CI):** `python3 cli.py quality` → pre-fix output contains
  `FAIL: asyncapi channels` naming `audit.events.ledgered.v1`,
  `audit.events.projection.v1`, `audit.events.archive.v1`; post-fix exits 0.
- **AC-1.2 (unit, `checks/test_quality_checks.py` or new `checks/test_asyncapi_channels.py`):**
  temp-tree fixtures (dev_auth_manifest pattern, F5):
  (a) `asyncapi.yaml` with a send channel `ghost` at `audit.events.ghost.v1`, no
  Go producer → `run(temp)` returns 1 and names the address;
  (b) same fixture + `internal/kafka/ghost.go` containing
  `const TopicGhost = "audit.events.ghost.v1"` + `kafka.NewProducer(b, kafka.TopicGhost)`
  → returns 0;
  (c) fixture with only `accepted`/`dlq` channels and the real kafka.go
  constants/relay/consumer files → returns 0 (existing symbols keep passing).
- **AC-1.3:** the check is wired into `cmd_quality()` and the post-fix CI run is
  green (Step 8).

### AC-2 — Projector consuming post-ledger events yields populated chain fields
- **AC-2.1/AC-2.2 (integration, `internal/projection/projection_test.go`, gated
  by `AUDIT_TEST_CLICKHOUSE_DSN`):** build a post-ledger event the way the
  service does (or drive one through `service.Ingest` in a service test to get
  the receipt — receipt path is testable without ClickHouse; the projection
  assertion needs CH). Insert via the projector's wrapped ingest func (insert +
  fake projection publisher); then
  `SELECT stream_id, sequence, event_hash FROM audit_events WHERE event_id = ?`
  and assert `event_hash == receipt.Hash` (non-empty), `sequence == receipt.Sequence`
  (`> 0`), `stream_id == receipt.StreamID` (non-empty). Negative variant: a
  pre-ledger-shaped event (empty chain fields) for a distinct `event_id` yields
  **no** row (AC-2.2; `ErrNotLedgered` via REQ-3).
- **AC-2.3 (startup test, `cmd/audit-projector/main_test.go`):** run the binary
  with `-brokers=127.0.0.1:1` and unreachable CH DSN, capture stdout, assert the
  first log line contains `topic=audit.events.ledgered.v1` when `-topic` and
  `AUDIT_KAFKA_TOPIC` are unset (log moved before `projection.Open`, §3.3).
  Same test asserts the env override still wins when set.

### AC-3 — Permanently rejected events are never inserted
- **AC-3.1 (unit, `internal/service/service_test.go` + `internal/kafka/kafka_test.go`):**
  - service level: for each rejection class — forged tenant → `ErrTenantMismatch`,
    invalid schema → `ErrInvalid`, content conflict → `ErrConflict` — attach a
    recording fake `LedgeredPublisher`, call `Ingest`, assert error returned AND
    `Publish` never invoked (so the event can never reach the ledgered topic,
    hence never the projector).
  - consumer level: reuse the existing permanent-error dead-letter test
    (kafka_test.go:142); extend the projector's wrapped ingest with a fake
    publisher + a fake reader: when `Insert` returns `ErrNotLedgered` (wrapped
    permanent per §4.8), assert dead-letter occurs and `Publish` was never
    called.
- **AC-3.2 (unit, `internal/projection/projection_test.go`, no CH needed for the
  error assertion):** `store.Insert(ctx, event)` with `StreamID==""` /
  `Sequence==0` / `Hash==""` (each case separately) returns `ErrNotLedgered`
  and writes no row; with `AUDIT_TEST_CLICKHOUSE_DSN` set, verify
  `CountTenant` / `SELECT count() WHERE event_id = ?` returns 0.

### AC-4 — Client-supplied chain fields never propagate
- **AC-4.1 (unit, `internal/service/service_test.go`):** event with forged
  `StreamID="client:forged"`, `Sequence=999`, `Hash="forged-hash"` through
  `Ingest` with a recording fake publisher; assert the published event's chain
  fields equal the receipt's ledger-assigned values, and that
  `"forged-hash"`/`999` appear nowhere in the published canonical JSON (extends
  the existing `TestIngestStripsClientStreamID` pattern, service_test.go:409).
  Chain through to the projection assertion in AC-2.1 (receipt-derived values).
- **AC-4.2 (unit, `internal/projection/projection_test.go`):** `Insert` fed
  forged chain values with the REQ-3 guard present → either `ErrNotLedgered`
  (e.g. forged values failing guard: empty/zero cases) or, for guard-passing
  forged values, the row contains exactly the values passed — assert the row
  equals the input (no re-derivation at the projection layer; the guard's job is
  presence, the service's job is authenticity — document this division).

### REQ-9 — Gate green
- `python3 cli.py quality` exits 0 after all steps (§6), per `AGENTS.md`;
  optional `python3 cli.py security-scan`.

---

## 8. Effort and risk notes

- **Effort estimate (spec: 6):** Step 1 ~0.5d, Steps 2–4 ~0.75d, Step 5 ~1.5d
  (outbox variant + migration + relay mode), Step 6 ~0.5d, tests ~1d, deployment
  doc ~0.25d. Total ≈ 4.5–5d.
- **Key risk — REQ-1 placement:** publishing from the service is the only
  transport-complete option; the outbox gives at-least-once. The residual window
  (commit durable → outbox write lost, F1) is closed by rebuild-from-ledger
  (§7, step 7c), which ADR-0004 §4 already mandates.
- **Key risk — gate heuristic rot:** pinned by AC-1.2 fixtures in both
  directions; fail-closed by design.
- **Dependency risk:** `internal/projection → internal/outbox` import (if chosen)
  is gate-legal today but should be a conscious decision (§4.8); the
  `domain.PermanentError` alternative keeps projection dependency-free.
