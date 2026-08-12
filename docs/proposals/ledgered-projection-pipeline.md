# Design — Feed the ClickHouse projection from post-ledger events and reject unledgered rows

Status: Approved for implementation · Date: 2026-08-12 · Module: `api/asyncapi`
Direction: "Feed the ClickHouse projection from post-ledger events and reject unledgered rows"
Supersedes: the 2026-08-11 draft of this file (that draft predates the sibling
"Gate AsyncAPI channel declarations" work — the `TopicLedgered`/`TopicProjection`/
`TopicArchive` constants and `checks/asyncapi_channels.py` now exist — and its
scope decisions are superseded by the approved requirements spec, §5/§10).

This document treats the approved requirements spec
(`docs/auto/runs/feed-the-clickhouse-projection-from-post-ledger--525c6225/artifacts/requirements-48eaf625/requirements.md`)
as **untrusted claims**. Section 1 re-verifies every claim against the working
tree. Sections 2–9 give the concrete design: API changes, compatibility
constraints, failure modes, migration steps, and a testable acceptance mapping.

---

## 1. Evidence verification

Every claim (E1–E10, F1–F10) was re-checked against the working tree at
commit `fae5b4c`. **All confirmed**, with the corrections and additions noted.

| # | Claim | Verification |
|---|---|---|
| E1 | `cmd/audit-projector/main.go` defaults to `kafka.TopicAccepted` | **Confirmed.** main.go:19 — `flag.String("topic", envOr("AUDIT_KAFKA_TOPIC", kafka.TopicAccepted), ...)`. |
| E2 | Projector ingests via `projection.Store.Insert` | **Confirmed.** main.go:42 — `kafka.NewConsumer(..., kafka.IngestFunc(store.Insert), ...)`. |
| E3 | `projection.Insert`/`schemaDDL` write `StreamID`/`Sequence`/`Hash` with no presence validation | **Confirmed.** projection.go:33–45 (`schemaDDL`: `stream_id String`, `sequence UInt64`, `event_hash String`), projection.go:70–88 (`Insert`: payload encode then `ExecContext`, no guard). |
| E4 | `internal/kafka/kafka.go` defines `TopicAccepted`/`TopicDLQ`; grep shows zero producers/consumers using `TopicLedgered`/`TopicProjection`/`TopicArchive` | **Confirmed (as corrected).** kafka.go:20/24 (`TopicAccepted`/`TopicDLQ`) and kafka.go:28–42 (`TopicLedgered`/`TopicProjection`/`TopicArchive`, added by the sibling work). Grep over `cmd/`+`internal/` non-test call sites: only `TopicAccepted` is used (`cmd/audit-outbox-relay/main.go:59`, `cmd/audit-kafka-dlq-replay` via a flag, `cmd/audit-kafka-consumer` via flags); `NewConsumer`/`NewProducer` default to `TopicAccepted` (kafka.go:86, 230). |
| E5 | Ledger assigns chain fields only inside `service.Ingest`'s commit closure | **Confirmed.** service.go:422 strips client `StreamID`; service.go:513–516 assign `StreamID`/`Sequence`/`PrevHash`/`Hash` inside the `s.Store.Update` closure; service.go:531–533 copy into the receipt. |
| E6 | ADR-0004 §4: projector consumes `audit.events.ledgered.v1` | **Confirmed.** docs/adr/ADR-0004.md:22 — "projector 消费 `audit.events.ledgered.v1` 构建投影". |
| E7 | `asyncapi.yaml` declares `ledgered`/`projection`/`archive` channels and the `EventEnvelope` chain fields | **Confirmed.** asyncapi.yaml channels (lines 9–32), operations `publishAccepted`/`publishLedgered`/`publishProjection`/`publishArchive`/`publishFailure` all `action: send` (lines 36–53), `EventEnvelope` `stream_id` (documented "Server-assigned…"), `sequence`, `prev_hash`, `hash` (lines 66–114). **Additional finding:** `channels.ledgered` currently has **no `description`** (only `accepted` has one) — REQ-7.1 is a real delta, and it is parser-safe (see §3.5). |
| E8 | Ledger rejections map to 400/409/422 | **Confirmed.** `internal/httpapi/server.go:1204–1229` `statusForError`: `ErrInvalid`→400, `ErrConflict`→409, `ErrTenantMismatch`→422, `ErrSchemaNotFound`→422. Sentinels at `internal/domain/models.go:32,35,37,42`. |
| E9 | Accepted-topic events carry no chain fields; rejected events still get projected | **Confirmed.** `cmd/audit-outbox-relay/main.go:59` publishes the pre-ledger event to `TopicAccepted`; the ledger assigns chain fields later inside the API `Ingest` commit closure; the projector consumes `accepted` in parallel with the ledger consumer, so rows exist for events the ledger rejects. |
| E10 | AsyncAPI↔code alignment gate exists and currently passes | **Confirmed.** `checks/asyncapi_channels.py` (Rules A/B, constant alignment only) wired at cli.py:408/419; current run: `PASS: asyncapi channels (5 send channels, 5 symbols aligned, undeclared topics: 0)`, exit 0. `python3 cli.py quality` at baseline: **QUALITY PASS, exit 0**. |

**Additional verified facts:**

| # | Claim | Verification |
|---|---|---|
| F1 | `service.Ingest` is near the function-size budget; the publish hook must be a helper | **Confirmed, line numbers drift.** `Ingest` spans service.go:394–621 (228 lines) against the 260-line/45-decision cap (engineering.yaml). Extraction is still required because the publish block plus the call adds ~8 inline lines and any added decision counts toward the 45. |
| F2 | `service.Config` has no publisher or logger field; `cmd/audit-api` wires no Kafka | **Confirmed.** Config fields end at `AggregateCheckpointRetention` (service.go:51); `cmd/audit-api/main.go:74` builds `service.Config{...}` with no Kafka wiring. |
| F3 | `projection.Store.Insert` has no in-tree caller other than `cmd/audit-projector` | **Confirmed.** Only main.go:42 (non-test); `internal/projection/projection_test.go` uses it in tests. Tightening the contract breaks nothing else. |
| F4 | Existing integration fixture sets `StreamID="demo:source:demo"`, `Sequence=1`, `Hash="abc123"`; gated by `AUDIT_TEST_CLICKHOUSE_DSN` | **Confirmed.** projection_test.go:46–48 fixture; gate at projection_test.go:16–19; assertions check only `count != 0` (projection_test.go:70–73). The fixture satisfies the new guard unchanged (AC-1.2). |
| F5 | Service-test harness exists | **Confirmed.** `testService` (service_test.go:43), `testEvent` (service_test.go:70), `TestIngestStripsClientStreamID` (service_test.go:409), `TestIngestIdempotencyConflictAndIntegrity` (service_test.go:74, constructs `ErrConflict`). `crmPrincipal = domain.IngestPrincipal{ClientID: "crm"}` (service_test.go:21). |
| F6 | Binary-level test precedent exists | **Confirmed.** `cmd/audit-governance-worker/main_test.go:559–561` builds with `exec.Command("go", "build", "-o", bin, ".")`. |
| F7 | `kafka.Producer.Deliver` is topic-parameterized and keys by `event_id` | **Confirmed.** kafka.go:101–107 — `Deliver(ctx, event)` writes canonical JSON with `Key: []byte(event.EventID)`; `NewProducer(brokers, topic)` (kafka.go:86). |
| F8 | `architecture.py` forbids only `domain/security/auth/store → service/httpapi/grpcapi` | **Confirmed.** FORBIDDEN dict — `service → kafka` not forbidden; the design still keeps transport out of `service` via interface injection. |
| F9 | Duplicate re-ingest short-circuits at the `receipt.Duplicate` guard | **Confirmed.** service.go:555–556 `if receipt.Duplicate { return receipt, nil }`; archive block follows at service.go:558. |
| F10 | ClickHouse table is `ReplacingMergeTree(occurred_at) ORDER BY (tenant_id, occurred_at, event_id)` | **Confirmed.** projection.go:33–45 schemaDDL. Redelivered ledgered events dedup on `(tenant_id, occurred_at, event_id)` — at-least-once publication converges. |

**New findings from this verification (not in the spec, load-bearing for the design):**

- **N1 — `Ingest` has no `ctx` parameter.** service.go:394 —
  `func (s *Service) Ingest(tenantID string, principal domain.IngestPrincipal, event domain.Event, waitFor string) (domain.EventReceipt, error)`.
  The spec's REQ-1 pseudo-code `publishLedgered(ctx, event)` cannot receive a
  transport context. The helper must construct a **bounded** context internally
  (`context.WithTimeout(context.Background(), ledgeredPublishTimeout)`). This
  both fits the no-ctx signature (no breaking change to the 4 HTTP/gRPC call
  sites — httpapi/server.go:265,309; grpcapi/server.go:50,72,102) and bounds
  ingest latency when the broker hangs (see §5 FM-7).
- **N2 — A fresh consumer group starts at the topic's beginning.**
  kafka.go:235 — `StartOffset: kafka.FirstOffset`. A newly rolled
  `audit-projector` group consuming `audit.events.ledgered.v1` will backfill
  the whole topic from its creation — the migration property that makes the
  "enable publication before flipping the projector" sequence lossless (§6).
- **N3 — The projector consumer has no DLQ attached.** main.go:42 passes no
  `WithDLQ`. FM-4's "retry/dead-letter evidence" is therefore: backoff-retry
  logs, then the attempts-exhausted path which **commits + logs** (kafka.go
  `deadLetter()` degrades to commit+log when no DLQ is attached). Wiring a DLQ
  producer into the projector is a cheap optional hardening step (one
  `WithDLQ(...)` call), out of the acceptance scope, recommended in §5 FM-4.
- **N4 — Gate anchors are content-based.** `checks/stream_consistency.py`
  matches substrings (`ErrTenantMismatch`, `event.StreamID = ""`,
  `event.Stream()`, `event.StreamID = streamID`) and their relative order, not
  absolute line numbers. Inserting the publish call after service.go:555 does
  not disturb it, provided the helper (a) never contains `event.StreamID = ""`
  and (b) never contains `event.Stream()` before the existing derivation —
  both avoided by construction (§3.1).
- **N5 — The strict YAML-subset parser accepts the REQ-7 doc shapes.**
  `checks/asyncapi_channels.py` already parses the `>-` block scalar on
  `channels.accepted` (block-scalar support: `_split_key_value`/`_skip_block_scalar`)
  and flow-map descriptions (`stream_id`'s "Server-assigned; …" inside
  `{ type: string, description: … }` via `_check_flow`). Adding the same shapes
  to `ledgered`/`sequence`/`prev_hash`/`hash` is parser-safe; keep the
  description text free of flow-container-breaking characters (quotes/braces).

**Verified current data flow** (matches the spec): outbox relay →
`audit.events.accepted.v1` (pre-ledger; chain fields empty/absent) → two
independent consumer groups: `audit-kafka-consumer` (POST → API `Ingest`;
ledger assigns `StreamID`/`Sequence`/`PrevHash`/`Hash` in the commit closure;
receipt carries them) and `audit-projector` (inserts **pre-ledger** rows into
ClickHouse with `hash=""`, `sequence=0`, `stream_id=""`, including events the
ledger later rejects). `ledgered`/`projection`/`archive` have no producing
code; ADR-0004 §4 is violated.

---

## 2. Target data flow

```
business service ──► outbox relay ──► audit.events.accepted.v1 (pre-ledger)
                                          │
                                          ▼
                              audit-kafka-consumer ──► POST ──► api/grpc Ingest
                                                                   │
                              ledger commit closure assigns         │
                              StreamID/Sequence/PrevHash/Hash ─────┤ (durable, once per commit)
                                                                   ▼
                                                    publishLedgered(event)  [REQ-1]
                                                                   │
                                                                   ▼
                                              audit.events.ledgered.v1  (canonical JSON, key=event_id)
                                                                   │
                                                                   ▼
                                          audit-projector (group audit-projector,
                                          StartOffset FirstOffset)
                                                                   │  store.Insert [REQ-4 guard]
                                                                   ▼
                                                        ClickHouse audit_events
                                                    (ReplacingMergeTree dedup)
```

- Rejected events (400/409/422) return before the commit closure → never reach
  the publish point → never reach the ledgered topic (REQ-5, structural).
- Client-supplied chain fields are stripped/overwritten before the publish
  point → the published event carries only server-assigned values (REQ-6).
- `projection.Insert` refuses any event lacking complete chain state, so
  accepted-topic rows (or any forged row) cannot materialize (REQ-4).

---

## 3. API changes

### 3.1 `internal/service` — post-ledger publication hook (REQ-1)

**New file `internal/service/ledgered.go`** (~60 lines). Rationale: service.go
is 1388/1500 lines; the interface + helper + logging shim belong in their own
file, keeping both files well under the cap and `Ingest` untouched in size
beyond a one-line call.

```go
package service

import (
	"context"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

// ledgeredPublishTimeout bounds a best-effort publish so a hung broker can
// never stall the ingest response path beyond this window (FM-1/FM-7). The
// ledger commit is already durable; a timeout only costs the ledgered-stream
// lag, which the rebuild-from-ledger path recovers (ADR-0004 §4).
const ledgeredPublishTimeout = 5 * time.Second

// LedgeredPublisher delivers a post-ledger event to audit.events.ledgered.v1.
// Implementations must be at-least-once and idempotent by EventID.
type LedgeredPublisher interface {
	Publish(ctx context.Context, event domain.Event) error
}

// logf is the nil-safe logging shim for Config.Logf.
func (s *Service) logf(format string, args ...any) {
	if s.Config.Logf != nil {
		s.Config.Logf(format, args...)
	}
}

// publishLedgered best-effort publishes the post-commit event exactly once per
// ledgered commit. It never fails ingest: the ledger commit is durable and a
// publish error (broker down, timeout, encode failure) is logged and swallowed.
// A nil Config.LedgeredPublisher preserves today's behavior exactly (no-op).
func (s *Service) publishLedgered(event domain.Event) {
	if s.Config.LedgeredPublisher == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), ledgeredPublishTimeout)
	defer cancel()
	if err := s.Config.LedgeredPublisher.Publish(ctx, event); err != nil {
		s.logf("ledgered publish failed event_id=%s sequence=%d error=%v", event.EventID, event.Sequence, err)
		return
	}
	s.logf("ledgered published event_id=%s stream_id=%s sequence=%d hash=%s", event.EventID, event.StreamID, event.Sequence, event.Hash)
}
```

**`service.Config` additions** (service.go, additive — nil preserves behavior):

```go
type Config struct {
	// ... existing fields unchanged ...
	// LedgeredPublisher optionally publishes the post-commit event to
	// audit.events.ledgered.v1. Nil disables publication (today's behavior).
	LedgeredPublisher LedgeredPublisher
	// Logf is the nil-safe logging hook used by best-effort ledgered
	// publication. Nil is a silent no-op (AC-3.3 asserts it is invoked when set).
	Logf func(format string, args ...any)
}
```

**`Ingest` call site** (service.go, between the duplicate guard and the archive
block — REQ-1.3):

```go
	if receipt.Duplicate {
		return receipt, nil
	}
	s.publishLedgered(event) // NEW — post-commit, non-duplicate, non-conflict
	if archive.Configured(s.Config.Archive) {
		...
	}
```

Invariant of the call site: at this point `event` carries the closure-mutated
server values (`StreamID`/`Sequence`/`PrevHash`/`Hash` assigned at
service.go:513–516) because `Store.Update` re-invokes the closure on CAS
conflict and the final invocation's mutations are what survive; the receipt
copied the same values at service.go:531–533. The helper satisfies N4 by
construction: it contains neither `event.StreamID = ""` nor `event.Stream()`.

Semantics audit against REQ-1:

- **Exactly once per ledgered commit:** the publish point is after
  `receipt.Duplicate` (F9) and after a successful `Store.Update` (any closure
  error returns before). Duplicate re-ingest returns at the guard — no
  re-publish. Conflict re-ingest returns an error from the closure — no
  publish. Only a fresh ledgered commit reaches the point.
- **Never fail ingest:** the helper returns no error; `Ingest`'s signature and
  control flow are unchanged beyond the call.
- **Covers all transports:** HTTP (httpapi/server.go:265,309) and gRPC
  (grpcapi/server.go:50,72,102) both funnel through `Service.Ingest`; the
  kafka-consumer→HTTP path is the HTTP one. One hook, all transports.

### 3.2 `cmd/audit-api` — production publisher wiring (REQ-2)

`internal/kafka` is untouched (no signature changes, F7). `cmd/audit-api` may
import `internal/kafka` (cmd is the adapter layer; F8 permits `service → kafka`
anyway, but we keep `service` transport-free).

```go
// cmd/audit-api/main.go
ledgeredBrokers := flag.String("ledgered-brokers", os.Getenv("AUDIT_LEDGERED_BROKERS"),
	"comma-separated Kafka brokers for post-ledger events (audit.events.ledgered.v1); empty disables ledgered publication")
```

Wiring (after `flag.Parse()` and logger creation, before `service.New`):

```go
type ledgeredPublisher struct{ producer *kafka.Producer }

func (a ledgeredPublisher) Publish(ctx context.Context, event domain.Event) error {
	return a.producer.Deliver(ctx, event) // canonical JSON, key = event_id (F7)
}

var ledgeredPub service.LedgeredPublisher
if *ledgeredBrokers != "" {
	producer := kafka.NewProducer(strings.Split(*ledgeredBrokers, ","), kafka.TopicLedgered)
	defer producer.Close() // closed on shutdown (REQ-2)
	ledgeredPub = ledgeredPublisher{producer: producer}
	logger.Printf("ledgered_publish=enabled brokers=%s topic=%s", *ledgeredBrokers, kafka.TopicLedgered)
} else {
	logger.Printf("ledgered_publish=disabled (set -ledgered-brokers or AUDIT_LEDGERED_BROKERS to enable)")
}
// ...inside cfg construction...
cfg := service.Config{ /* existing fields */ , LedgeredPublisher: ledgeredPub, Logf: logger.Printf}
```

Design notes:

- The thin `ledgeredPublisher` adapter satisfies `LedgeredPublisher.Publish` via
  `Producer.Deliver` without widening `internal/kafka`'s API surface (REQ-2.1).
- `defer producer.Close()` sits in `main`, so shutdown ordering matches the
  existing `defer st.Close()` pattern.
- `Logf: logger.Printf` — `*log.Logger.Printf` matches `func(string, ...any)`
  exactly; no wrapper needed.
- **At-least-once/idempotency:** sync acks=all writes, keyed by `event_id`
  (hash balancer, F7); downstream `ReplacingMergeTree` dedup on
  `(tenant_id, occurred_at, event_id)` (F10) makes redelivery convergent.

### 3.3 `cmd/audit-projector` — post-ledger consumption + log-first (REQ-3)

```go
topic := flag.String("topic", envOr("AUDIT_KAFKA_TOPIC", kafka.TopicLedgered), "source topic")
```

Startup reorder (REQ-3.2): move the resolved-configuration log line to be the
**first stdout line** — after flag parsing and broker/backoff validation,
**before** `projection.Open`/`EnsureSchema`:

```go
	flag.Parse()
	if *brokers == "" {
		log.Fatalf("brokers are required: pass -brokers or set AUDIT_KAFKA_BROKERS")
	}
	if *backoff <= 0 {
		log.Fatalf("backoff must be positive")
	}
	logger := log.New(os.Stdout, "audit-projector ", log.LstdFlags|log.Lmicroseconds)
	// FIRST LINE: resolved config, observable even when ClickHouse/Kafka is down (AC-2.1).
	logger.Printf("brokers=%s topic=%s group=%s clickhouse=%s", *brokers, *topic, *group, *dsn)
	store, err := projection.Open(*dsn)
	...
```

Consumer group `audit-projector` and `kafka.IngestFunc(store.Insert)` unchanged
(REQ-3.3). The fresh group starts at `FirstOffset` (N2) → the ledgered topic is
backfilled on first run (§6 step 4).

### 3.4 `internal/projection` — unledgered-row guard (REQ-4)

projection.go (96 lines today; ~+12 after):

```go
// ErrNotLedgered is returned when an event lacks complete ledger-assigned chain
// state; such an event must never materialize into the query surface.
var ErrNotLedgered = errors.New("projection: event lacks ledger-assigned chain state")

func (s *Store) Insert(ctx context.Context, event domain.Event) error {
	// Presence guard FIRST: before payload encoding and before any database
	// access, so a zero-value *Store (nil db) returns ErrNotLedgered without
	// panicking (AC-1.1 regression probe) and no DB round-trip is wasted.
	if event.StreamID == "" || event.Sequence <= 0 || event.Hash == "" {
		return ErrNotLedgered
	}
	payload, err := domain.CanonicalJSON(event.Payload)
	...
}
```

- `errors` import added to projection.go (currently absent).
- `event.Sequence` is `int64` (domain/models.go:80), `Hash` is `string` (:82) —
  the guard types are exact.
- The guard is **presence**, not authenticity: the projection layer never
  re-derives chain fields; authenticity is guaranteed upstream (REQ-5/REQ-6).
- No row is written for unledgered events — including events the ledger
  rejected (FM-3/FM-4/FM-6) and forged chain values (REQ-6 + REQ-4).

### 3.5 `api/asyncapi/asyncapi.yaml` — contract documentation (REQ-7)

No schema/channel/operation/required-set changes. Two documentation-only
deltas, both parser-safe per N5:

1. `channels.ledgered` gains a `description` block scalar, same `>-` style as
   `channels.accepted`:

```yaml
  ledgered:
    address: audit.events.ledgered.v1
    description: >-
      Post-ledger, chain-linked audit events. Published exactly once per
      ledgered commit by the audit API after the immutable ledger commit;
      stream_id, sequence, prev_hash and hash are server-assigned and always
      present (sequence >= 1, non-empty hash). The Kafka message key MUST
      equal the payload's event_id (parity with accepted).
    messages:
      auditEvent:
        $ref: '#/components/messages/AuditEvent'
```

2. `EventEnvelope` property descriptions for `sequence`, `prev_hash`, `hash`,
   mirroring the existing `stream_id` description (flow-map `description:`
   inside `{...}`, exactly the `stream_id` shape):

```yaml
        stream_id: { type: string, description: Server-assigned; a client-supplied value is stripped on ingest. }
        sequence: { type: integer, description: Server-assigned sequence within the ledger stream; always >= 1 on the ledgered channel. }
        prev_hash: { type: string, description: Server-assigned predecessor hash in the ledger chain; empty only for the first event of a stream. }
        hash: { type: string, description: Server-assigned chain hash; always non-empty on the ledgered channel. }
```

The `required` list stays unchanged — chain fields remain optional in the
shared envelope so the accepted channel stays conformant (accepted events omit
them via `omitempty`).

### 3.6 Engineering-budget compliance

| Artifact | Budget | After change | Verdict |
|---|---|---|---|
| `internal/service/service.go` | 1500 lines/file | 1390 (one call line) | OK |
| `internal/service/ledgered.go` (new) | 1500 | ~60 | OK |
| `service.Ingest` | 260 lines / 45 decisions | 229 lines, +1 call, 0 new decisions | OK |
| `internal/projection/projection.go` | 1500 | ~108 | OK |
| `internal/kafka/kafka.go` | 1500 | unchanged | OK |
| `cmd/audit-projector/main.go` | 1500 | ~70 | OK |
| Dependency direction | `service` must not import `kafka` | interface in `service`, adapter in `cmd/audit-api` | OK |
| `checks/stream_consistency.py` | content anchors | unaffected (N4) | OK |
| `checks/asyncapi_channels.py` | Rules A/B | no address/constant change; doc shapes parse (N5) | OK |

---

## 4. Compatibility constraints

1. **Accepted-topic contract untouched.** Relay, ledger-consumer, DLQ
   classification and 400/409/422 semantics unchanged (E8/E9 verified). Only
   the projector's **default** source moves (REQ-3.1).
2. **Projector override preserved.** `-topic`/`AUDIT_KAFKA_TOPIC` still select
   the accepted topic for operators mid-migration. With REQ-4 active that
   configuration now yields `ErrNotLedgered` evidence instead of garbage rows —
   a deliberate fail-loud safety property (§8.4 of the spec, FM-4).
3. **`projection.Insert` contract tightened.** Only in-tree caller is
   `cmd/audit-projector` (F3); the existing integration fixture satisfies the
   guard unchanged (F4/AC-1.2). `ErrNotLedgered` is additive (new exported
   sentinel, no existing return value changes meaning).
4. **`service.Config` additive.** `LedgeredPublisher` and `Logf` default to nil
   → today's behavior exactly (no publication, no logging). All existing
   service tests construct `Config{...}` without the new fields — unaffected.
5. **`Ingest` signature unchanged** (N1) — no HTTP/gRPC call-site churn.
6. **AsyncAPI additive-documentation only.** No schema/required-set change;
   shared `EventEnvelope` keeps chain fields optional so the accepted channel
   stays conformant; gate Rule A/B unaffected (E10).
7. **`internal/kafka` API surface unchanged** (REQ-2.1) — the adapter lives in
   `cmd/audit-api`.
8. **Residual risk (accepted):** a crash between the durable ledger commit and
   the in-process publish loses that event from the ledgered stream; the
   projection is eventually consistent and rebuildable from the ledger
   (ADR-0004 §4 rebuild path is the backstop; durable outbox delivery of
   ledgered events is explicitly out of scope, §9).

---

## 5. Failure modes

| # | Failure | Detection | Impact | Handling |
|---|---|---|---|---|
| FM-1 | Publish fails after commit (broker down / timeout) | `Config.Logf` "ledgered publish failed" | Ledgered-stream lag; **ledger intact** | Best-effort; never fails ingest; bounded by `ledgeredPublishTimeout` (5 s); rebuild-from-ledger recovers (ADR-0004 §4). |
| FM-2 | Projector restarted mid-stream | Consumer-group offset lag | Projection stale | At-least-once + `ReplacingMergeTree` dedup on `(tenant_id, occurred_at, event_id)` ⇒ convergent (F10). |
| FM-3 | Forged/malicious accepted-topic message reaches projector ingest | REQ-4 guard | No garbage row | `ErrNotLedgered`; no write; retry then commit+log. |
| FM-4 | Projector misconfigured to accepted topic | `ErrNotLedgered` on every row; backoff-retry logs; attempts-exhausted commit+log (N3 — no DLQ attached today) | No projection writes; loud failure | Ops error; first log line names the resolved topic (REQ-3.2). Optional hardening (out of acceptance scope): attach `kafka.WithDLQ(...)` so evidence is also an `attempts_exhausted` Failure record. |
| FM-5 | Duplicate re-delivery of a ledgered message | — | Idempotent by `event_id` | Dedup at table level (F10); publish-once-per-commit (REQ-1.3). |
| FM-6 | Rejected event (400/409/422) reaches projector ingest | REQ-4 guard + structural (REQ-5) | No row | Guard rejects; structural guarantee: rejected events never reach the publish point. |
| FM-7 | Publish blocks (broker hang, not refuse) | Timeout via bounded ctx | Ingest latency capped at 5 s; ledgered lag | Context deadline aborts; logged; ingest proceeds. |
| FM-8 | `Config.Logf` nil (operator didn't wire logger) | — | Publish failures silently dropped | By design nil-safe no-op (spec REQ-1.3); `cmd/audit-api` always wires `logger.Printf`, so production deployments are observable. |
| FM-9 | Client-forged chain fields in the accepted topic | Existing strip/overwrite (E5) | None | Strip at service.go:422; unconditional overwrite in closure; publish uses the post-commit value (REQ-6). |

**Guarantee summary:** the ledger commit is never rolled back by publication
failure (FM-1/FM-7); the projection never contains unledgered facts
(FM-3/FM-4/FM-6); forged chain values never reach the ledgered stream or the
projection (REQ-6 + REQ-4); the gate never silently passes contract drift (AC-4).

---

## 6. Migration steps

Code lands as one release (all REQs; gate green — §8). Deployment order:

1. **Pre-flight (operator).** Create `audit.events.ledgered.v1` (and ensure the
   API's producer and the projector's consumer have ACLs). Existing accepted
   topic, relay, DLQ untouched.
2. **Roll `audit-api` with publication disabled.** Deploy the new API binary
   first *without* `-ledgered-brokers`/`AUDIT_LEDGERED_BROKERS`: behavior is
   byte-identical to today (nil `LedgeredPublisher`), startup log notes
   `ledgered_publish=disabled`. This decouples code rollout from traffic
   change; rollback is a plain binary revert.
3. **Enable ledgered publication.** Set `AUDIT_LEDGERED_BROKERS`, restart the
   API. Verify: startup log `ledgered_publish=enabled`; a consumer of the
   ledgered topic (kcat etc.) shows canonical events with key=`event_id` and
   non-empty `stream_id`/`sequence`/`prev_hash`/`hash`. Ingest stays green —
   publish failure only logs (FM-1). Events published between step 3 and step 4
   are retained on the topic; the projector's fresh group will consume them
   from `FirstOffset` (N2) — nothing is lost.
4. **Roll `audit-projector` to the new binary.** The default flips to
   `audit.events.ledgered.v1`; the startup log's first line is
   `topic=audit.events.ledgered.v1` (REQ-3.2). The new consumer group
   (same `audit-projector` id, `StartOffset FirstOffset`) backfills the ledgered
   topic from its beginning; `ReplacingMergeTree` converges on
   `(tenant_id, occurred_at, event_id)` (F10).
   - *Rollback:* redeploy the previous projector binary (default accepted) —
     pre-existing garbage-row behavior returns; or keep an explicit
     `-topic=audit.events.accepted.v1` override in place, accepting loud
     `ErrNotLedgered` evidence (FM-4) — never silent garbage.
5. **Purge historical garbage rows (optional, operator decision).** Rows
   projected before this change (including rows for events the ledger rejected,
   which have no ledgered twin) are not auto-removed. Per ADR-0004 §4, the
   supported clean-history path is a full rebuild from the ledgered topic
   (re-consume; table-level dedup replaces same-key rows; rejected-event rows
   have no twin and need a table rebuild/truncate to vanish). Not a code
   change; a documented ops procedure.
6. **Verify.** Run the AC-2.2-style query against ClickHouse: for a sampled
   receipt, `SELECT stream_id, sequence, event_hash WHERE event_id = ?` matches
   `receipt.StreamID`/`Sequence`/`Hash`. Confirm `python3 cli.py quality` exit 0
   on the release tree.

Migration window semantics (spec §8.2): `-topic`/`AUDIT_KAFKA_TOPIC` remain
honored; with REQ-4 active that configuration is fail-loud by design (FM-4),
never silent garbage.

---

## 7. Testable acceptance mapping

The four supplied acceptance checks are decomposed into concrete tests. All run
under `go test ./...` (wired into `cli.py quality`); ClickHouse-gated tests
skip unless `AUDIT_TEST_CLICKHOUSE_DSN` is set.

| AC | Test (file) | Harness | Assertion |
|---|---|---|---|
| **AC-1.1** | `TestInsertRejectsUnledgered` — new, `internal/projection/projection_test.go`, **no ClickHouse** | zero-value `*Store` (nil `db`); valid fixture (all chain fields set) mutated per case: `{StreamID:""}`, `{Sequence:0}`, `{Sequence:-1}`, `{Hash:""}`; table-driven, one subtest per case | `errors.Is(err, ErrNotLedgered)`; no nil-panic (guard precedes the DB access — the nil-`db` probe) |
| **AC-1.2** | `TestClickHouseProjectionIntegration` — existing, unchanged | fixture `StreamID="demo:source:demo"`, `Sequence=1`, `Hash="abc123"` (F4) | insert OK; duplicate insert OK; `CountTenant("demo") != 0` |
| **AC-1.3** | `TestInsertUnledgeredWritesNoRow` — new, gated by `AUDIT_TEST_CLICKHOUSE_DSN` | `Insert` an unledgered event with a **distinct** `event_id` (chain fields empty) | returns `ErrNotLedgered`; `SELECT count() FROM audit_events WHERE event_id = ?` == 0 |
| **AC-2.1** | `TestStartupLogsResolvedTopic` — new, `cmd/audit-projector/main_test.go` (new file), `go build`+exec pattern per `cmd/audit-governance-worker/main_test.go:559` | build binary into `t.TempDir()`; run with `-brokers=127.0.0.1:1` (unreachable), **no** topic flags/env; `exec.CommandContext` with timeout; capture stdout (binary exits non-zero once Open/consume fails — only stdout matters) | first stdout line contains `topic=audit.events.ledgered.v1` (requires REQ-3.1 + REQ-3.2); sub-case with `AUDIT_KAFKA_TOPIC=audit.events.accepted.v1` (or `-topic=…`) ⇒ first line shows the override |
| **AC-2.2** | `TestProjectionRoundTripsReceiptChainFields` — new, gated by `AUDIT_TEST_CLICKHOUSE_DSN` | source chain fields from a real `service.Ingest` receipt (in-test: construct service via `service.New` + `Ingest`, or an equivalent fixture with `StreamID`/`Sequence`/`Hash` populated); `Insert` the post-ledger event | `SELECT stream_id, sequence, event_hash WHERE event_id = ?` equals `receipt.StreamID` (non-empty), `receipt.Sequence` (> 0), `receipt.Hash` (non-empty) |
| **AC-3.1** | `TestIngestRejectionsNeverPublish` — new, `internal/service/service_test.go` | recording fake `LedgeredPublisher` (records events + call count) injected via `svc.Config.LedgeredPublisher` after `testService` (service_test.go:43); three rejection classes: (a) `ErrTenantMismatch` — envelope `TenantID` ≠ resolved tenant; (b) `ErrInvalid` — drop `resource` from `testEvent` payload (schema-required); (c) `ErrConflict` — re-ingest same `event_id` with different payload (pattern of service_test.go:74) | `Ingest` returns the error **and** publish count == 0 |
| **AC-3.2** | `TestIngestPublishesLedgeredExactlyOnce` — new, same file | recording fake; successful `Ingest` with `testEvent`; then duplicate re-ingest (identical content); then a fresh event with forged inputs `StreamID='client:forged'`, `Sequence=999`, `Hash='forged-hash'` | publish count == 1 after first ingest; published event's `StreamID`/`Sequence`/`Hash` == receipt's; forged re-ingest: published event carries **server-assigned** values and `domain.CanonicalJSON(published)` contains none of `client:forged`, `forged-hash`, `999`; duplicate re-ingest returns `Duplicate==true` and publish count stays 1 |
| **AC-3.3** | `TestPublishFailureDoesNotFailIngest` — new, same file | fake whose `Publish` returns an error; `Config.Logf` captures messages | `Ingest` returns the receipt with nil error; captured log contains the failed `event_id`; ingest proceeds (receipt status is Ledgered/Archived as usual) |
| **AC-4.1** | gate | `python3 cli.py quality` | exit 0: gofmt, filesize/complexity budgets, dependency direction, root invariants, route↔OpenAPI, `checks/asyncapi_channels.py` (Rules A/B), `go vet`, unit tests, race tests, production binary builds |
| **AC-4.2** | gate regression | `python3 cli.py check` output | `PASS: asyncapi channels (5 send channels, 5 symbols aligned, undeclared topics: 0)` — no new channel addresses or topic constants |

Test-harness notes (verified against the tree):

- `recordingPublisher` must guard with a mutex only if publish is ever called
  concurrently; in these tests it is called synchronously inside `Ingest`, so a
  plain slice suffices. `svc.Config` is an exported struct field — inject
  `pub`/`logf` after `testService` returns, no harness signature change.
- AC-2.1 uses `exec.CommandContext` with a timeout (e.g. 30 s) and
  `cmd.Output()`: stdout is returned even when the command exits non-zero
  (`*exec.ExitError`), and stderr is discarded by default — exactly the first
  line of stdout is the assertion target.
- AC-1.1's nil-`db` probe is the regression guard for guard placement: if the
  guard were moved after `s.db.ExecContext`, the zero-value store would
  nil-panic instead of returning `ErrNotLedgered`.

---

## 8. Implementation order (each step gate-green)

1. **REQ-3.2 first:** move the projector startup log before `Open`/`EnsureSchema`
   — makes the default-topic behavior observable and unlocks AC-2.1. Add
   `cmd/audit-projector/main_test.go` (AC-2.1).
2. **REQ-4:** `ErrNotLedgered` + guard + AC-1.1/AC-1.3 tests; confirm AC-1.2
   (existing fixture) still green.
3. **REQ-3.1:** default topic → `kafka.TopicLedgered`; AC-2.1 override
   sub-case.
4. **REQ-1 + REQ-2:** `internal/service/ledgered.go` (interface, `logf`,
   `publishLedgered`), `Config` fields, `Ingest` call site, `cmd/audit-api`
   flag/env/adapter wiring; AC-3.1/AC-3.2/AC-3.3 tests.
5. **REQ-7:** asyncapi.yaml `ledgered` description + chain-field property docs
   (parser-safe shapes, N5).
6. **AC-4:** full `python3 cli.py quality` (baseline verified green at
   `fae5b4c`); optional `python3 cli.py security-scan` when gosec is available.

Gate requirement (AGENTS.md): run `python3 cli.py quality` after every step;
final state must exit 0.

---

## 9. Out of scope (do not implement here)

- Producers for `TopicProjection`/`TopicArchive`; archive-pass completion
  signals from `cmd/audit-governance-worker`.
- Durable outbox delivery of ledgered events (`ledgered_outbox` table, relay
  `-ledgered-drain` mode, migration).
- Producer-call-site enforcement in `checks/asyncapi_channels.py` (constant
  alignment only, per the sibling direction's scope).
- ClickHouse engine/sort-key/partitioning changes; accepted-topic contract
  changes; DLQ replay changes; HTTP status mapping changes; AsyncAPI 3.0
  structural validation.
- Optional hardening, explicitly not acceptance-required: DLQ attachment to the
  projector consumer (FM-4), and a table-rebuild ops procedure for historical
  garbage rows (§6 step 5).
