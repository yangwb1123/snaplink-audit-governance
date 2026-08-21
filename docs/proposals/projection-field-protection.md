# Design — Projection derives from ledger-protected payloads; strips digests; refuses unledgered rows

Status: Implemented in the reference tree · Date: 2026-08-20 · Module: `cmd/audit-api` (+ `cmd/audit-projector`, `internal/projection`, `internal/service`, `internal/kafka`)
Direction: "ClickHouse projection persists plaintext copies of schema-encrypted (and unvalidated) fields, bypassing ledger-side field protection"
Companion design: `docs/proposals/ledgered-projection-pipeline.md` (the post-ledger-stream mechanism; this design adopts its publisher shape with the three fixes its review gate required — BatchTimeout, verify-stack wiring, guard-guarantee wording).

This document treats the approved requirements spec
(`docs/auto/runs/clickhouse-projection-persists-plaintext-copies--3d1890d1/artifacts/requirements-10762e10/requirements.md`)
as **untrusted claims**. Section 1 re-verifies every claim against the working
tree at HEAD `99c0325`. Sections 2–9 give the concrete design: API changes,
compatibility constraints, failure modes, migration steps, and a testable
acceptance mapping.

Implementation note: the post-ledger publisher/outbox and projector topic gate
are wired in `internal/service/ledgered.go`, `cmd/audit-api`, and
`cmd/audit-projector`; `internal/projection.projectionPayload` now strips
`__search_digest` keys recursively while preserving encrypted ciphertext. The
behavior is covered by `TestProjectionPayloadStripsSearchDigests`; ClickHouse
integration checks remain environment-gated.

---

## 1. Evidence verification

Every cited file/symbol (E1–E14, F1–F6) was re-checked against the working
tree. **All confirmed**, with the corrections and additions noted.

| # | Claim | Verification |
|---|---|---|
| E1 | Baseline `projection.Store.Insert` stored `CanonicalJSON(event.Payload)` with no stripping | **Confirmed as historical baseline; fixed.** `projectionPayload` now deep-copies through `security.StripSearchDigests` before canonical encoding; it does not decrypt or rewrite ledger ciphertext. |
| E2 | `schemaDDL` has `payload String`, no encryption | **Confirmed.** projection.go:40–57; `payload String` at :51. |
| E3 | Historical projector default was `kafka.TopicAccepted` | **Superseded by implementation.** `cmd/audit-projector` now defaults to `kafka.TopicLedgered` and still wires `kafka.IngestFunc(store.Insert)`. |
| E4 | Only producer of `TopicAccepted` is the outbox relay, raw events; `Deliver` = `CanonicalJSON(event)` | **Confirmed.** kafka.go:24 (`TopicAccepted`); `Deliver` :101–107 writes canonical JSON keyed by `event_id`. Non-test `NewProducer` call sites: `cmd/audit-outbox-relay/main.go:59` (→ accepted), `cmd/audit-kafka-consumer/main.go:42` (→ DLQ), `cmd/audit-kafka-dlq-replay/main.go:58` (→ accepted, `Republish` byte-for-byte only). |
| E5 | Outbox relay delivers raw events, no validation | **Confirmed.** relay.go:127 — `r.Deliver(ctx, record.Event)` passes the stored event straight through. |
| E6 | Encryption/digest protection applied only inside `service.Ingest` | **Confirmed.** `Ingest` :394; `validateEvent` :896; `protectSensitiveFields` :955, called :446; chain fields (`StreamID`/`Sequence`/`PrevHash`/`Hash`) assigned only inside the commit closure :513–516 (client `StreamID` stripped at :422); receipt copies at :531–533. |
| E7 | `cmd/audit-api` has no Kafka publisher in the ingest path | **Confirmed.** grep: no `kafka` import in cmd/audit-api/main.go; ingest is httpapi/grpcapi → `service.Ingest`. |
| E8 | Every query surface strips `*__search_digest`; projection does not | **Confirmed.** `security.StripSearchDigests` (fieldcrypto.go:137, deep-copy, any depth) used at httpapi/server.go:352,404,452,500 and service/governance.go:635. projection.go has no strip. |
| E9 | Encrypted values are self-describing `enc:v1:` strings | **Confirmed.** fieldcrypto.go:16 (`encryptedPrefix`), :46. |
| E10 | `rejectSensitive` → `ErrInvalid`; tenant mismatch → `ErrTenantMismatch` | **Confirmed.** rejectSensitive service.go:1299 (forbidden normalized substrings: password/token/private_key/device_credential/card_number/secret); `ErrTenantMismatch` at :415. |
| E11 | `projection.Store.Insert`'s only in-tree caller is `cmd/audit-projector` | **Confirmed.** Only main.go:42 (non-test); tests use it directly. |
| E12 | ClickHouse fixture exists, gated, carries chain fields | **Confirmed.** projection_test.go: fixture `StreamID="demo:source:demo"`, `Sequence=1`, `Hash="abc123"`; asserts `count != 0`; gate `AUDIT_TEST_CLICKHOUSE_DSN`. Satisfies guard+strip unchanged. |
| E13 | Service test harness exists | **Confirmed.** `testService` (service_test.go:43), `testEvent` (:70), `crmPrincipal` (:21). |
| E14 | ADR-0004 §4 mandates projector consume `audit.events.ledgered.v1` | **Implemented.** ADR-0004, the API durable publisher, and the projector default now agree on the ledgered topic. |

**Additional verified facts:**

| # | Claim | Verification |
|---|---|---|
| F1 | Ledger-stored payloads carry `enc:v1:` ciphertext **plus** bound `sd2:` digests added by `protectSensitiveFields` | **Confirmed.** service.go:985-ish `event.Payload[field] = encoded`; :1002-ish `event.Payload[field+"__search_digest"] = digest` (`SearchDigestBound`, `sd2:` prefix). Projection must strip digests **after** protection, or AC-2's "no `field__search_digest`" fails even on the post-ledger stream. |
| F2 | `rejectSensitive` forbids literal key `card_number` | **Confirmed.** service.go:1302–1304 forbidden list includes `card_number` (substring of normalized key). Fixtures must use e.g. `account_number`. |
| F3 | `service.GetEvent` returns the ledger-stored protected event; actor `""` skips read self-audit | **Confirmed.** GetEvent :646; `recordReadAction` returns nil for actor `""`. |
| F4 | `domain.CanonicalJSON` gives byte-deterministic comparison | **Confirmed.** canonical.go:22; maps sorted recursively; representation-independent numbers. |
| F5 | Budgets: 1500 lines/file, 260 lines/function, 45 decisions | **Confirmed.** engineering.yaml. projection.go is 96 lines; guard+strip fit. No protection change to `service.Ingest` needed. |
| F6 | Dependency direction permits `internal/projection → internal/security` | **Confirmed.** checks/architecture.py FORBIDDEN = `{domain,security,auth,store} → {service,httpapi,grpcapi}`; `internal/projection` is not a forbidden source; projection already imports `internal/domain`. |

**New findings from this verification (load-bearing for the design):**

- **N1 — kafka-go's synchronous producer defaults to a 1 s batch timeout.**
  `internal/kafka/kafka.go:90` builds `&kafka.Writer{..., Async: false}` with no
  `BatchTimeout`; kafka-go v0.4.51 `writer.go:826–828` falls back to
  `1 * time.Second`. A synchronous post-ledger publish on the ingest hot path
  (the sanctioned mechanism, §3.3) would add up to ~1 s steady-state latency
  per ingest. The companion design's review gate failed on exactly this. Fix:
  set `BatchTimeout` in `NewProducer` (§3.5). This also lowers the outbox
  relay's existing per-delivery latency (a strict improvement, no semantics
  change).
- **N2 — The repo's verify stack/E2E will deterministically break on the
  default flip unless wired.** `deploy/docker-compose.verify.yml:134–146`
  (audit-projector) sets only `AUDIT_KAFKA_BROKERS`/`AUDIT_CLICKHOUSE_DSN`;
  `test/e2e/fullstack.sh:174–177` creates only `accepted`+`dlq` topics and
  asserts `CH_ROW=1` for a projected row (:212–215). With the default flipped
  to `TopicLedgered`, and with the guard refusing accepted-topic rows, the E2E
  projection check fails unless (a) `audit.events.ledgered.v1` is created,
  (b) the audit-api publishes to it (`AUDIT_LEDGERED_BROKERS` in the compose
  api service), and (c) the projection assertion tolerates the extra hop.
  E2E is **not** part of `cli.py quality` (verified: cmd_quality runs fmt,
  filesize, complexity, architecture, directory_fanout, root_files,
  root_business_code, invariants, exemptions, adr_compliance, make_help,
  route_contract, contract_fields, asyncapi_channels, proto_sync,
  dev_auth_manifest, tenant_consistency, stream_consistency,
  sensitive_logging, vet, python checks, unit, race, build) — but the
  adversarial review treats repo consistency as blocking (the companion
  design's gate failed on this).
- **N3 — `projection.Insert` guard placement probe is sound.** A zero-value
  `*Store` has nil `db`; a guard placed after the DB access would nil-panic on
  the AC-1.2 mutation cases; the presence guard placed first returns
  `ErrNotLedgered` without touching the store.
- **N4 — Gate interactions are benign.** `checks/stream_consistency.py` is
  content-anchored on service.go (`ErrTenantMismatch`, `event.StreamID = ""`,
  `event.Stream()`, `event.StreamID = streamID`, relative order); the
  `publishLedgered` helper lives in a new file and contains none of those
  strings, so the one-line Ingest insertion cannot disturb it.
  `checks/asyncapi_channels.py` enforces constant↔address alignment only —
  its docstring states "no traffic or consumer enforcement" — so a
  `TopicLedgered` producer call site in `cmd/audit-api` is safe. The
  architecture checker globs `*.go` only under `internal/{domain,security,
  auth,store}`; a **test-only** import of `internal/service` from
  `internal/projection/projection_test.go` (the AC-1.1 harness) is gate-safe,
  and `internal/service`'s own test import of `internal/projection` is
  likewise (service is never a scanned source). No import cycle arises.
- **N5 — Guard is presence-only; the guarantee must be worded precisely.**
  REQ-3.3 says the projection never re-derives chain fields. Non-empty forged
  chain fields would pass the guard **only** if an operator points the
  projector at a non-sanctioned topic (accepted-topic override / rollback /
  misconfiguration). On the sanctioned path this is structurally excluded:
  `Ingest` strips client chain fields (:422) and the publish uses the
  post-commit server-assigned values. The design states this limit (§5
  FM-9) instead of overclaiming "no garbage rows ever".
- **N6 — `CanonicalJSON` is representation-independent**, so the
  `StripSearchDigests` JSON round-trip (which yields `json.Number`) does not
  change stored bytes for digest-free payloads: projected payloads that
  contain no `*__search_digest` key are byte-identical to today (compat
  constraint §4.3; the existing fixture and E2E event have no digest keys).

---

## 2. Scope interpretation (resolves spec §5/§10 tension)

The spec's in-scope item (1) sanctions the **post-ledger stream as the default
mechanism** (REQ-1.2, ADR-0004 §4) and AC-3.2 pins it (`topic=…ledgered.v1`
first log line). E14 confirms the topic has zero producers today, so the
sanctioned mechanism requires a best-effort post-ledger publisher. The spec's
§10 "any change to service.Ingest" is scoped by its own parenthetical —
"protection already exists there" — i.e. **no change to protection
semantics**, not a ban on a post-commit publish call (the durable-outbox
variant is what §10 explicitly excludes). This design therefore includes the
minimal publisher (adopted from the companion design) as mechanism wiring,
plus the projection-side enforcement that is this direction's core.

Consequence: the acceptance criteria (AC-1.1, AC-2.1, AC-3.1) are satisfiable
**in-process without Kafka** (Ingest → GetEvent → Insert → SELECT); the
publisher is required for the production mechanism and for the repo's own E2E
to stay green after the default flip.

---

## 3. API changes

### 3.1 `internal/projection/projection.go` — guard + digest strip (REQ-2, REQ-3)

~96 → ~125 lines. Two additions and one reordering inside `Insert`.

```go
package projection

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
)

// ErrNotLedgered is returned when an event lacks complete ledger-assigned
// chain state; such an event must never materialize into the query surface.
var ErrNotLedgered = errors.New("projection: event lacks ledger-assigned chain state")

// projectionPayload prepares the stored payload: every key ending in
// "__search_digest" (any nesting depth — nested maps and array elements,
// unbound v1 and bound v2 digests alike) is removed from a deep copy before
// canonical encoding. The caller's event.Payload map is never mutated:
// store-shared maps are referenced by the ledger snapshot.
func projectionPayload(event domain.Event) ([]byte, error) {
	stripped, err := security.StripSearchDigests(event.Payload)
	if err != nil {
		return nil, fmt.Errorf("strip search digests: %w", err)
	}
	payload, err := domain.CanonicalJSON(stripped)
	if err != nil {
		return nil, fmt.Errorf("encode payload: %w", err)
	}
	return payload, nil
}

// Insert writes one canonical event into the projection. ClickHouse applies
// the ReplacingMergeTree dedup asynchronously; queries must not assume
// immediate uniqueness.
//
// The presence guard runs first — before payload encoding and before any
// database access — so an event that never completed the ledger commit
// (missing ledger-assigned StreamID/Sequence/Hash) is refused with
// ErrNotLedgered and no row is written. The guard is presence, not
// authenticity: the projection layer never re-derives chain fields;
// authenticity is the ledger's job.
func (s *Store) Insert(ctx context.Context, event domain.Event) error {
	if event.StreamID == "" || event.Sequence <= 0 || event.Hash == "" {
		return ErrNotLedgered
	}
	payload, err := projectionPayload(event)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO audit_events (tenant_id, occurred_at, event_id, stream_id, sequence, event_type, source_system, operation_id, actor_id, outcome, payload, event_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.TenantID, event.OccurredAt.UTC(), event.EventID, event.StreamID, event.Sequence,
		event.EventType, event.SourceSystem, event.OperationID, event.Actor.ID, event.Outcome,
		string(payload), event.Hash)
	if err != nil {
		return fmt.Errorf("insert projection: %w", err)
	}
	return nil
}
```

Notes:
- The guard reads `Sequence` (int64, domain/models.go:80) and `Hash` (string,
  :82) — exact types.
- `projectionPayload` is package-private; REQ-6.1 is satisfied because the
  unit tests live in `package projection` (same package, no export needed).
- The strip round-trip preserves byte shape for digest-free payloads (N6).

### 3.2 `cmd/audit-projector/main.go` — post-ledger default + log-first (REQ-1.2, REQ-1.4)

Two changes:

```go
	topic := flag.String("topic", envOr("AUDIT_KAFKA_TOPIC", kafka.TopicLedgered), "source topic")
```

and startup reorder — the resolved-config line becomes the **first stdout
line**, after flag validation, before `projection.Open`/`EnsureSchema`:

```go
	flag.Parse()
	if *brokers == "" {
		log.Fatalf("brokers are required: pass -brokers or set AUDIT_KAFKA_BROKERS")
	}
	if *backoff <= 0 {
		log.Fatalf("backoff must be positive")
	}
	logger := log.New(os.Stdout, "audit-projector ", log.LstdFlags|log.Lmicroseconds)
	// FIRST LINE: the resolved source topic is observable even when
	// ClickHouse/Kafka are unreachable (REQ-1.4, AC-3.2).
	logger.Printf("brokers=%s topic=%s group=%s clickhouse=%s", *brokers, *topic, *group, *dsn)
	store, err := projection.Open(*dsn)
	// ... unchanged from here (the old :41 log line is removed) ...
```

`kafka.IngestFunc(store.Insert)` and the consumer group stay unchanged.

### 3.3 `internal/service/ledgered.go` (new, ~60 lines) — sanctioned mechanism publisher (REQ-1.2)

Adopted from the companion design (approved proposal
`docs/proposals/ledgered-projection-pipeline.md` §3.1) with no changes to its
shape; this is the only service-layer change and it is **post-commit,
best-effort, protection-neutral**.

```go
package service

import (
	"context"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

// ledgeredPublishTimeout bounds the best-effort post-ledger publish so a
// hung broker can never stall the ingest response path beyond this window.
// The ledger commit is already durable; a timeout only costs ledgered-stream
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

// publishLedgered best-effort publishes the post-commit event exactly once
// per ledgered commit. It never fails ingest: the ledger commit is durable
// and a publish error (broker down, timeout, encode failure) is logged and
// swallowed. A nil Config.LedgeredPublisher preserves today's behavior (no-op).
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

**`service.Config` additions** (additive; nil preserves today's behavior):

```go
	// LedgeredPublisher optionally publishes the post-commit event to
	// audit.events.ledgered.v1 (the sanctioned projection source per
	// ADR-0004 §4). Nil disables publication (today's behavior).
	LedgeredPublisher LedgeredPublisher
	// Logf is the nil-safe logging hook used by best-effort ledgered
	// publication. Nil is a silent no-op.
	Logf func(format string, args ...any)
```

**`Ingest` call site** — one line between the duplicate guard and the archive
block (service.go:555–558):

```go
	if receipt.Duplicate {
		return receipt, nil
	}
	s.publishLedgered(event) // NEW — post-commit, non-duplicate, non-conflict
	if archive.Configured(s.Config.Archive) {
```

Invariants of the call site: at this point `event` carries the closure-mutated
server values (`StreamID`/`Sequence`/`Hash` assigned at :513–516); the receipt
copied the same values (:531–533). Exactly-once-per-commit: duplicate re-ingest
returns at the guard; conflict re-ingest returns from the closure; only a fresh
ledgered commit reaches the publish point. `Ingest`'s signature and
protection semantics are unchanged (no HTTP/gRPC churn — the 4 transport call
sites at httpapi/server.go:265,309 and grpcapi/server.go:50,72,102 are
untouched). The helper contains neither `event.StreamID = ""` nor
`event.Stream()`, so `checks/stream_consistency.py` anchors are undisturbed
(N4).

### 3.4 `cmd/audit-api/main.go` — publisher wiring (REQ-1.2)

`internal/kafka` is not imported by service; the adapter lives in the cmd
layer. `cmd/audit-api` may import `internal/kafka` (it is the adapter layer).

```go
	ledgeredBrokers := flag.String("ledgered-brokers", os.Getenv("AUDIT_LEDGERED_BROKERS"),
		"comma-separated Kafka brokers for post-ledger events (audit.events.ledgered.v1); empty disables ledgered publication")
```

After `flag.Parse()`/logger creation, before building `cfg`:

```go
type ledgeredPublisher struct{ producer *kafka.Producer }

func (a ledgeredPublisher) Publish(ctx context.Context, event domain.Event) error {
	return a.producer.Deliver(ctx, event) // canonical JSON, key = event_id
}

	// in main():
	var ledgeredPub service.LedgeredPublisher
	if *ledgeredBrokers != "" {
		producer := kafka.NewProducer(strings.Split(*ledgeredBrokers, ","), kafka.TopicLedgered)
		defer producer.Close()
		ledgeredPub = ledgeredPublisher{producer: producer}
		logger.Printf("ledgered_publish=enabled brokers=%s topic=%s", *ledgeredBrokers, kafka.TopicLedgered)
	} else {
		logger.Printf("ledgered_publish=disabled (set -ledgered-brokers or AUDIT_LEDGERED_BROKERS to enable)")
	}
	cfg := service.Config{ /* existing fields */ , LedgeredPublisher: ledgeredPub, Logf: logger.Printf}
```

Notes:
- The `-check-config` path (runCheckConfig) validates secrets only; the new
  flag does not participate and does not break it.
- `defer producer.Close()` mirrors the existing `defer st.Close()` pattern.
- At-least-once/idempotency: sync acks=all writes keyed by `event_id`;
  `ReplacingMergeTree` dedup on `(tenant_id, occurred_at, event_id)` makes
  redelivery convergent.

### 3.5 `internal/kafka/kafka.go` — BatchTimeout fix (N1)

No signature change; one constant and one writer field:

```go
// producerBatchTimeout caps how long a synchronous Producer.Deliver waits to
// fill a batch before writing. kafka-go's default is 1s, which would add up
// to a second of steady-state latency to every event delivered through a
// synchronous producer (the audit-api ledgered publish and the outbox relay
// hot paths). Audit volumes rarely fill kafka-go's default 100-message batch
// within a deadline, so a small timeout trades batching for latency.
const producerBatchTimeout = 10 * time.Millisecond
```

```go
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchTimeout: producerBatchTimeout,
	}
```

Compatibility: all existing `NewProducer` callers (outbox relay, DLQ replay,
kafka-consumer DLQ) keep their signatures; their per-message delivery latency
drops from ≤1 s to ≤10 ms (strictly an improvement; write semantics unchanged).
The healthy-broker ingest latency budget of the ledgered publish becomes
~10 ms + RTT, bounded at 5 s on broker hang (FM-7).

### 3.6 Verify stack / E2E (N2) — `deploy/docker-compose.verify.yml` + `test/e2e/fullstack.sh`

- `docker-compose.verify.yml` audit-api service environment gains
  `AUDIT_LEDGERED_BROKERS: redpanda:9092` so the verify stack exercises the
  sanctioned path.
- `fullstack.sh` topic creation gains the ledgered topic:
  ```sh
  $COMPOSE exec -T redpanda rpk topic create audit.events.ledgered.v1 \
    --partitions 1 --replicas 1 >/dev/null 2>&1 || true
  ```
- The projection assertion (currently a fixed `sleep 5` then `CH_ROW=1`)
  becomes a bounded retry loop (e.g. up to 30 × 1 s) to absorb the extra
  accepted→ledger→projector hop, and additionally asserts the projected row
  carries chain fields:
  `SELECT count() ... WHERE event_id='$EVENT_ID' AND stream_id != '' AND sequence > 0 AND event_hash != ''`.
  The E2E event (`{"note":"fullstack e2e"}`) has no encrypted/searchable
  fields, so digest/plaintext assertions remain the job of the gated
  integration tests (AC-1.1/AC-2.2/AC-3.1), not the E2E.

### 3.7 Engineering-budget compliance (REQ-5.1)

| Artifact | Budget | After change | Verdict |
|---|---|---|---|
| `internal/projection/projection.go` | 1500 lines | ~125 | OK |
| `internal/service/service.go` | 1500 | 1389 (+1 call line) | OK |
| `internal/service/ledgered.go` (new) | 1500 | ~60 | OK |
| `service.Ingest` | 260 lines / 45 decisions | ~229 lines, +1 call, 0 new decisions | OK |
| `cmd/audit-api/main.go` | 1500 | ~+30 | OK |
| `cmd/audit-projector/main.go` | 1500 | ~70 | OK |
| `internal/kafka/kafka.go` | 1500 | ~400 (+const +field) | OK |
| Dependency direction | — | `projection → security` permitted (F6); service stays transport-free (interface in service, adapter in cmd) | OK |
| `checks/stream_consistency.py` | content anchors | unaffected (N4) | OK |
| `checks/asyncapi_channels.py` | Rules A/B | no constant/address change; new producer call site safe (N4) | OK |

---

## 4. Compatibility constraints

1. **Accepted-topic contract untouched.** Relay, ledger-consumer, DLQ
   classification and 400/409/422 semantics unchanged. Only the projector's
   **default** source moves; `-topic`/`AUDIT_KAFKA_TOPIC` overrides still work.
2. **Projector override preserved (migration window).** Pointing the projector
   at the accepted topic now yields `ErrNotLedgered` evidence (retry logs,
   attempts-exhausted commit+log) instead of plaintext/garbage rows —
   deliberate fail-loud behavior (REQ-5.4), never silent divergence.
3. **`projection.Insert` contract tightened, additive.** Only in-tree caller
   is `cmd/audit-projector` (E11); the existing integration fixture (E12)
   satisfies guard+strip unchanged; `ErrNotLedgered` is a new exported
   sentinel. **Byte-compat:** digest-free payloads store byte-identical bytes
   to today (N6).
4. **No protection-semantics change.** `service.Ingest`/`protectSensitiveFields`
   are untouched; the single added call is post-commit best-effort
   publication. `Ingest` signature unchanged — no HTTP/gRPC churn.
5. **`service.Config` additive.** `LedgeredPublisher`/`Logf` default nil →
   today's behavior exactly; existing service tests construct `Config{...}`
   without the new fields — unaffected.
6. **`internal/kafka` behavior-only change.** `NewProducer` gains
   `BatchTimeout: 10ms` (no signature change); relay/DLQ-replay inherit lower
   latency.
7. **Test-only cross-package imports are gate-safe.** `projection_test.go`
   imports `internal/service` (AC-1.1 harness) and `service_test.go` imports
   `internal/projection` (AC-2.1 unprojectability) — neither package is a
   scanned FORBIDDEN source (N4); no import cycle.
8. **Verify stack updated in the same release** (§3.6), so the E2E stays green
   after the default flip (N2).
9. **Residual risk (accepted).** A crash between the durable ledger commit and
   the in-process publish loses that event from the ledgered feed; the
   projection is eventually consistent and rebuildable from the ledger per
   ADR-0004 §4 (durable outbox delivery is explicitly out of scope, §9).
10. **Historical rows.** Rows already projected (plaintext, digest-carrying,
    or for rejected events with no ledgered twin) are not auto-removed; the
    supported clean-history path is a full rebuild from the ledgered stream
    (ReplacingMergeTree converges on same-key rows; rejected-event rows need
    a table rebuild/truncate) — a documented ops procedure (§6 step 6).

---

## 5. Failure modes

| # | Failure | Detection | Impact | Handling |
|---|---|---|---|---|
| FM-1 | Projector still pointed at accepted topic (misconfig/override) | `ErrNotLedgered` on every row; backoff-retry logs; attempts-exhausted commit+log | No projection writes; **no plaintext rows**; loud failure | Ops error; first log line names the resolved topic (REQ-1.4, AC-3.2). |
| FM-2 | Client smuggles `*__search_digest` keys (v1 unbound or v2 bound) in payload | AC-2.2 assertion | Correlation risk if persisted | Stripped in `Insert` (REQ-2) regardless of source. |
| FM-3 | Ledger rejects an event (sensitive-field, tenant mismatch, schema, conflict) | Service error before commit closure | Would previously project a non-fact | Structural: never reaches the ledgered stream; guard (REQ-3) refuses any unledgered row. |
| FM-4 | Post-ledger publish fails / lags (broker down) | `Config.Logf` "ledgered publish failed"; projection staleness | Ledger intact; projection eventually consistent | Best-effort; never fails ingest; bounded by `ledgeredPublishTimeout` (5 s); rebuild-from-ledger recovers (ADR-0004 §4). |
| FM-5 | Duplicate redelivery of a ledgered message | — | Idempotent | `ReplacingMergeTree` dedup on `(tenant_id, occurred_at, event_id)`; publish-once-per-commit (REQ-1.3). |
| FM-6 | Encrypted-field ciphertext corrupted/missing in a ledgered event | Guard passes (chain fields present); payload carries `enc:v1:` string | Broken ciphertext stored | Accepted: projection stores ledger payloads verbatim (minus digests); authenticity is the ledger's job (REQ-3.3). |
| FM-7 | Publish blocks (broker hang) | Timeout via bounded ctx | Ingest latency capped at 5 s hung / ~10 ms healthy; ledgered lag | `ledgeredPublishTimeout` aborts; `BatchTimeout` fix (N1); logged; ingest proceeds. |
| FM-8 | `Config.Logf` nil (unwired logger) | — | Publish failures silently dropped | By design nil-safe no-op; `cmd/audit-api` always wires `logger.Printf`. |
| FM-9 | Forged non-empty chain fields on a **non-sanctioned** topic | Guard passes (presence-only) | Forged row if operator overrides topic | Documented limit — no overclaim: structurally excluded on the sanctioned path (Ingest strips client chain fields :422; publish uses post-commit server values); log-first topic line makes the misconfiguration visible; authenticity is the ledger's job (REQ-3.3). |
| FM-10 | Verify stack/E2E regression from the default flip | `fullstack.sh` projection check fails | E2E red | Wired in the same release (§3.6): ledgered topic creation, `AUDIT_LEDGERED_BROKERS`, retry loop. |
| FM-11 | Projector lags behind the ledgered stream (restart, broker outage) | Consumer-group offset lag | Projection stale | At-least-once + dedup ⇒ convergent; rebuild-from-ledger recovers (ADR-0004 §4). |

**Guarantee summary (precise):** on the sanctioned (post-ledger) path the
projection never contains plaintext of schema-encrypted fields (FM-1/FM-6,
AC-1); never contains search-digest material at any depth (FM-2, AC-2.2);
never contains unledgered facts (FM-3, AC-2.1); and the projected payload
equals the ledger-stored protected payload minus derived digests by
construction (AC-3.1). Under a non-sanctioned topic override, the presence
guard refuses every unledgered (chain-less) event loudly (FM-1); the
presence-only limit on forged chain values is FM-9.

---

## 6. Migration steps

Code lands as one release (all REQs; gate green — §8). Deployment order:

1. **Pre-flight (operator).** Create `audit.events.ledgered.v1`; grant ACLs to
   the audit-api producer and the projector consumer. Existing accepted topic,
   relay, DLQ untouched.
2. **Roll `audit-api` with the publisher.** Deploy the new API binary with
   `AUDIT_LEDGERED_BROKERS` set. Startup log: `ledgered_publish=enabled`.
   Behavior for operators who leave it unset is byte-identical to today
   (nil `LedgeredPublisher`; log: `ledgered_publish=disabled`). Verify with a
   topic consumer (kcat etc.): canonical events, key=`event_id`, non-empty
   `stream_id`/`sequence`/`prev_hash`/`hash`. Ingest stays green — publish
   failure only logs (FM-4/FM-7).
3. **Roll `audit-projector`.** Default flips to `audit.events.ledgered.v1`;
   the first stdout line names the resolved topic (AC-3.2). A fresh consumer
   group (same `audit-projector` id) starts at `FirstOffset`
   (kafka.go:235) and backfills the ledgered topic from its creation —
   events published in step 2 are not lost. `ReplacingMergeTree` converges on
   `(tenant_id, occurred_at, event_id)`.
   - *Rollback:* redeploy the previous projector binary (default accepted) —
     pre-existing garbage-row behavior returns; or keep an explicit
     `-topic=audit.events.accepted.v1` override, accepting loud
     `ErrNotLedgered` evidence (FM-1) — never silent garbage.
4. **Override window semantics.** `-topic`/`AUDIT_KAFKA_TOPIC` remain honored
   for operators mid-migration; with the guard active that configuration is
   fail-loud (FM-1), never silent.
5. **Verify stack.** The updated `docker-compose.verify.yml`/`fullstack.sh`
   (§3.6) ship in the same release so the E2E projection check passes on the
   sanctioned path.
6. **Purge historical plaintext rows (optional, operator decision).** Rows
   projected before this change (including rows for events the ledger
   rejected, which have no ledgered twin) are not auto-removed. Per
   ADR-0004 §4, the supported clean-history path is a full rebuild from the
   ledgered stream (re-consume; table-level dedup replaces same-key rows;
   rejected-event rows need a table rebuild/truncate to vanish). Not a code
   change; a documented ops procedure.
7. **Verify.** Sample receipts against ClickHouse:
   `SELECT stream_id, sequence, event_hash WHERE event_id = ?` matches
   `receipt.StreamID`/`Sequence`/`Hash`; `python3 cli.py quality` exits 0 on
   the release tree.

---

## 7. Testable acceptance mapping

All tests run under `go test ./...` (wired into `cli.py quality`);
ClickHouse-gated tests skip unless `AUDIT_TEST_CLICKHOUSE_DSN` is set.

| AC | Test (file) | Harness | Assertion |
|---|---|---|---|
| **AC-1.1** (negative path) | `TestClickHouseProjectionPlaintextAndDigestInvariant` — gated | in-test service with an encrypted/searchable schema and a raw event with no chain fields | `projection.Insert` returns `ErrNotLedgered`; no row is written |
| **AC-1.1** (protected path) | same test, continued | `svc.Ingest("tenant-a", domain.IngestPrincipal{ClientID:"crm"}, E_raw, domain.StatusLedgered)` must succeed; `ledgerEvent, _ := svc.GetEvent("tenant-a", "", E_raw.EventID)` | ledger payload protected: `account_number` is a string with `enc:v1:` prefix ≠ `"4111111111111111"`; `email__search_digest` has `sd2:` prefix; `projection.Insert(ctx, ledgerEvent)` returns nil |
| **AC-1.1** (row scan) | same test, continued | `SELECT payload FROM audit_events WHERE event_id = ?`; parse (UseNumber) | (a) no string value anywhere equals `"4111111111111111"` and the substring does not appear in the raw column value; (b) `account_number` equals the ledger ciphertext (`enc:v1:` prefix); (c) no key at any depth ends with `__search_digest`; (d) all non-encrypted values equal the ledger values |
| **AC-1.2** | `TestInsertRejectsUnledgered` — new, `internal/projection/projection_test.go`, **no ClickHouse** | zero-value `*Store` (nil `db`); valid fixture mutated per case `{StreamID:""}`, `{Sequence:0}`, `{Sequence:-1}`, `{Hash:""}`; table-driven | `errors.Is(err, ErrNotLedgered)`; no nil-panic (guard precedes the DB access — the placement probe, N3) |
| **AC-1.2** (regression) | `TestClickHouseProjectionIntegration` — existing, unchanged | fixture `StreamID="demo:source:demo"`, `Sequence=1`, `Hash="abc123"` | insert OK; duplicate insert OK; `CountTenant("demo") != 0` — fixture satisfies guard+strip unchanged |
| **AC-2.1** (rejections) | `TestIngestRejectionsAreNotProjectable` — new, `internal/service/service_test.go` | `testService(t, false)`; (a) payload with `"password":"hunter2"` → `errors.Is(err, domain.ErrInvalid)`; (b) envelope `event.TenantID = "tenant-b"` while resolved tenant is `"tenant-a"` → `errors.Is(err, domain.ErrTenantMismatch)` | `Ingest` returns the error and a **zero receipt**; the rejected event has no chain fields |
| **AC-2.1** (unprojectable) | `TestInsertRejectsUnledgered` reuse + gated `TestClickHouseRejectedEventsUnprojectable` — `internal/projection/projection_test.go` | rejected event (no chain fields) inserted on zero-value store; gated: distinct `event_id`, `Insert` then `SELECT count() WHERE event_id = ?` | `ErrNotLedgered`; gated count == 0 |
| **AC-2.2** | `TestProjectionPayloadStripsSearchDigests` — new, `internal/projection/projection_test.go`, **no ClickHouse** | payload with, at any depth: v1 unbound digest key (`"email__search_digest":"legacy-no-sd2-prefix"`), v2 bound digest key (`"email__search_digest":"sd2:…"` in a nested map), digest key inside an array element, an `enc:v1:` ciphertext string, non-digest keys | prepared bytes contain no key ending `__search_digest` (at any depth); caller's map unmutated (deep-copy semantics — original still carries the digest keys); non-digest keys intact; ciphertext string passes through untouched |
| **AC-3.1** | same gated test as AC-1.1 (final block) | `rowPayload` from `SELECT payload` (UseNumber decode) | `bytes.Equal(domain.CanonicalJSON(rowPayload), domain.CanonicalJSON(security.StripSearchDigests(ledgerEvent.Payload)))` — byte-for-byte, modulo derived digests |
| **AC-3.2** | `TestStartupLogsResolvedTopic` — new, `cmd/audit-projector/main_test.go`, `go build`+exec per `cmd/audit-governance-worker/main_test.go:559` pattern | build binary into `t.TempDir()`; run with `-brokers=127.0.0.1:1` (unreachable), **no** topic flags/env (`t.Setenv("AUDIT_KAFKA_TOPIC","")`); `exec.CommandContext` timeout ~30 s; capture stdout (binary exits non-zero at `Open` — only stdout matters) | **first stdout line** contains `topic=audit.events.ledgered.v1` (requires REQ-1.2 + REQ-1.4); sub-case `AUDIT_KAFKA_TOPIC=audit.events.accepted.v1` (or `-topic=…`) ⇒ first line shows the override |
| **AC-4.1** | gate | `python3 cli.py quality` | exit 0: gofmt, filesize/complexity budgets, dependency direction, root invariants, route↔OpenAPI, asyncapi_channels (Rules A/B), `go vet`, unit tests, race tests, production binary builds |
| **AC-4.2** | gate regression | existing ClickHouse fixture + full `go test ./...` | all green; optional `python3 cli.py security-scan` when gosec is available |
| Mechanism (supporting REQ-1.2) | `TestIngestPublishesLedgeredExactlyOnce` / `TestIngestRejectionsNeverPublish` / `TestPublishFailureDoesNotFailIngest` — new, `internal/service/service_test.go` | recording fake `LedgeredPublisher` + `Config.Logf` capture injected after `testService` | successful ingest publishes exactly once with server-assigned chain fields; duplicate re-ingest does not re-publish; rejected events never publish; publish error does not fail ingest (receipt returned, log contains event_id) |
| Mechanism (supporting REQ-1.2) | `TestProducerBatchTimeout` — new, `internal/kafka/kafka_test.go` | `kafka.NewProducer([]string{"x"}, kafka.TopicLedgered)` in-package | `producer.writer.BatchTimeout == 10ms` (guards N1 regression) |
| E2E (supporting REQ-1.2/N2) | `test/e2e/fullstack.sh` (updated) | compose verify stack | ledgered topic created; `AUDIT_LEDGERED_BROKERS` set; projection retry loop passes with chain-field assertion |

Test-harness notes (verified against the tree):

- AC-1.1's service harness is built **inside** `projection_test.go` (the
  shared `testService` in package service is not importable across packages);
  the spec's file placement is honored and the test-only `internal/service`
  import is gate-safe (N4).
- The rejected events in AC-2.1 structurally lack chain fields (they never
  reach the commit closure), so the projection-side assertion is the generic
  guard test plus a gated count==0 — no duplication of rejection logic.
- AC-3.2 uses `exec.CommandContext` + `cmd.Output()`: stdout is returned even
  on non-zero exit (`*exec.ExitError`); stderr is discarded by default.
- AC-1.1's nil-`db`/mutation probes double as the guard-placement regression
  guard (N3).
- E2E changes are part of the release because the default flip is
  observably breaking for the verify stack otherwise (N2, FM-10).

---

## 8. Implementation order (each step gate-green)

1. **REQ-3 first:** `ErrNotLedgered` + presence guard before payload
   encoding/DB access in `Insert`; AC-1.2 guard tests (nil-`db` table);
   confirm the existing ClickHouse fixture still passes.
2. **REQ-2:** `projectionPayload` helper (`StripSearchDigests` →
   `CanonicalJSON`); AC-2.2 strip unit tests; wire into `Insert`.
3. **REQ-1 projector side:** default topic → `kafka.TopicLedgered`; move the
   resolved-config log line to first output; add `cmd/audit-projector/main_test.go`
   (AC-3.2).
4. **REQ-1 mechanism:** `internal/service/ledgered.go`, `Config` additions,
   one-line `Ingest` call site, `cmd/audit-api` flag/env/adapter wiring,
   `internal/kafka` `BatchTimeout` fix (N1), mechanism tests.
5. **AC-1/AC-3 integration tests** (ClickHouse-gated) driving the full
   in-process pipeline with encrypted+searchable schema fields (AC-1.1,
   AC-3.1); AC-2.1 rejection tests; `internal/kafka` BatchTimeout test.
6. **Verify stack** (§3.6): compose env, fullstack.sh ledgered topic +
   retry-loop projection assertion.
7. **AC-4:** full `python3 cli.py quality` (baseline verified green at HEAD
   `99c0325` before changes); optional `python3 cli.py security-scan` when
   gosec is available.

Gate requirement (AGENTS.md): no code/contract change is complete while
`python3 cli.py quality` fails; run it after every step; the final state must
exit 0.

---

## 9. Out of scope (do not implement here)

- Durable outbox delivery of post-ledger events (`ledgered_outbox` table,
  relay drain mode, migration) — the sanctioned mechanism is best-effort
  in-process publish (§3.3), with the ADR-0004 rebuild path as backstop.
- Producers for `TopicProjection`/`TopicArchive`; archive-pass completion
  signals from `cmd/audit-governance-worker`.
- Any change to `service.Ingest` **protection semantics**,
  `protectSensitiveFields`, or the ledger commit path (protection already
  exists there; the single added line is post-commit publication).
- ClickHouse engine/sort-key/partitioning changes; accepted-topic contract
  changes; DLQ replay changes; HTTP/gRPC status mapping changes; AsyncAPI
  structural changes (constant alignment only — the companion gate work).
- Automatic purge/rebuild of historical plaintext rows (documented ops
  procedure per ADR-0004 §4, §6 step 6).
- Optional hardening, explicitly not acceptance-required: DLQ attachment to
  the projector consumer (FM-1 evidence stays retry+commit+log), and
  classification of `ErrNotLedgered` as a permanent `outbox.DeliveryError`
  for immediate dead-lettering.
