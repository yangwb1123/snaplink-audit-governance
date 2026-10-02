# Isolate the projector's dead-letter topic from the ingest DLQ replay tool

The projector and the ingest consumer dead-lettered into the **same** topic,
`audit.events.dlq.v1`, while the only recovery tool that drains it
(`audit-kafka-dlq-replay`) recovers originals **from the accepted topic**. A
ClickHouse outage or an `ErrNotLedgered` / `ErrTenantUnscoped` rejection was
therefore published as a `Failure` whose original lives on
`audit.events.ledgered.v1`, and the next replay round closed it one of two
ways: re-published as a redundant accepted re-ingest (never a ClickHouse
re-insert) or marked unresolvable and its DLQ offset committed — a permanent
close of projection evidence, firing `AuditDLQUnresolvableDrop`
("permanent loss of ledger evidence") for events that were never at risk. The
projection pipeline had no recoverable DLQ of its own.

## What changed

`audit.events.projection.dlq.v1` is now the projector's dead-letter topic, and
both pipelines fail closed if pointed at the other's:

| Change | Where |
|---|---|
| `kafka.TopicProjectionDLQ` (new exported const) | `internal/kafka/kafka.go` |
| `kafka.ValidateProjectionDLQ(sourceTopic, dlqTopic)`, `kafka.ValidateIngestDLQ(dlqTopic)` (new, exported) | `internal/kafka/topology.go` |
| `ValidateReplayTopology` additionally rejects the projection DLQ | `internal/kafka/topology.go` |
| projector default `-dlq-topic` / `AUDIT_KAFKA_DLQ_TOPIC` | `cmd/audit-projector/config.go` |
| `resolveReplayConfig(args, getenv)` resolution seam (validation now runs inside it) | `cmd/audit-kafka-dlq-replay/config.go` (new) + `main.go` |
| ingest consumer rejects the projection DLQ | `cmd/audit-kafka-consumer/main.go` |
| `projectionDlq` channel + `publishProjectionFailure` operation, `Failure` payload **reused unchanged** | `api/asyncapi/asyncapi.yaml` |
| `KNOWN_ABBREVIATIONS` is now a dotted **longest-suffix-first** address table | `checks/asyncapi_channels.py` |

The new rules are directional and live in the two new validators, so
`ValidateTopicTopology` is byte-unchanged and its 8-case table still holds: a
projector-shaped source with the projection DLQ must stay acceptable to the
shared rule, which is exactly why the projection/ingest rules cannot live
there.

`derive_symbol` now matches the address segments left after dropping a `v<N>`
suffix against a dotted-suffix table, longest first. That is what makes
`audit.events.projection.dlq.v1` derive `TopicProjectionDLQ` while
`audit.events.dlq.v1` still derives `TopicDLQ` and `audit.events.projection.v1`
still derives `TopicProjection` (the address `docs/adr/ADR-0001.md` reserves for
the future projection channel — a collision the naive derivation would have
created). No exemption allowlist was added; the existing
`test_no_exemption_allowlist_static` guard still forbids one.

## Operator-visible break (previously accepted configurations, now fail closed)

All three rejections happen **before** any ClickHouse, producer, consumer,
replay-state or broker construction, with a non-zero exit:

- projector with `-dlq-topic audit.events.dlq.v1` → `invalid projector topology: …`
- `audit-kafka-dlq-replay` with `-dlq-topic audit.events.projection.dlq.v1` →
  `replay topology: …`
- `audit-kafka-consumer` with `-dlq-topic audit.events.projection.dlq.v1` →
  `consumer topology: …`

Any other explicit address still works (e.g. a per-environment
`audit.events.projection.dlq.staging.v1`); only the two stock addresses are
rejected.

## Migration (ordered, non-lossy)

1. **Create the topic first** (plus an ACL for the projector's service
   principal): `rpk topic create audit.events.projection.dlq.v1 --partitions 1
   --replicas 1`. Deploying the code first is a **stall, never a loss** —
   `Consumer.deadLetter` returns the publish error *before* `CommitMessages`, so
   the ledgered offset stays uncommitted — but a stalled projection pipeline is
   still an outage.
2. Ship the binary/contract change. Nothing consumes the new topic yet.
3. Roll `audit-projector`; confirm `dlq_topic="audit.events.projection.dlq.v1"`
   in its startup log.
4. Leave `audit-kafka-dlq-replay` on the ingest DLQ — no reconfiguration, no
   re-scan, no state-file change. `audit_dlq_*` semantics are unchanged.
5. **Rollback:** revert the code in one release. The topic and the AsyncAPI
   channel are additive; if you keep the channel, revert the Go constant
   together with it, otherwise Rule B reports an undeclared topic and the gate
   goes red. Records already in the projection DLQ stay there until retention
   expires.

## Explicitly unchanged

- **No-drop invariants.** `Consumer.deadLetter` publish-before-commit, the
  `RunOnce` per-round reader recreation and accepted-reset-before-scan ordering
  (`checks/replay_round_reset.py`), `commitResolved`'s per-partition barrier,
  `ReplayState.Mark`/`MarkTenant` durability, the ledgered publish path, the
  ledger's at-least-once guarantee and the projector retry/`max-attempts` policy
  are all untouched.
- **No metric or alert drift.** `deploy/prometheus-rules.verify.yml` and
  `deploy/prometheus.verify.yml` are unmodified and the three pinned alerts keep
  their exact `expr`/`severity`/annotations. No line was added to
  `internal/httpapi`'s `/metrics`, to the consumer's `metricsText` or to the
  replayer's `metricsText` (both golden-pinned).
- **No transport coupling.** `internal/httpapi` gains no topic knowledge, no
  route, no metric line and no import: `go list -deps ./internal/httpapi` still
  contains no `internal/kafka`, `internal/outbox` or `internal/projection`.

## Accepted residual risk

- **No projection-DLQ observability.** `audit-projector` exposes no `/metrics`
  and is not scraped, so `AuditDLQTraffic` / `AuditDLQUnresolvableDrop` cover the
  ingest side only: a filling projection DLQ is currently operator-invisible.
  Deliberately out of scope (a sibling direction owns the projection pipeline's
  metric/scrape/alert) and pinned by leaving `deploy/prometheus*.yml` untouched.
- **Historical mixed records.** Pre-change projection failures already in
  `audit.events.dlq.v1` are not retroactively classifiable; the ingest replayer
  keeps draining them (redundant accepted re-ingest, or unresolvable close)
  until retention expires. No data migration is proposed.
- **`v<N>` derivation trap.** `audit.events.projection.dlq.v2` would also derive
  `TopicProjectionDLQ`, so Rule A fails until a matching channel exists — the
  pre-existing convention, now visible for this topic.

## Tests

- `internal/kafka/topology_projection_dlq_test.go` — broker-free regression: a
  ledgered event whose projection fails transiently dead-letters to a publisher
  bound to `TopicProjectionDLQ` (with the commit ordered after publication); an
  ingest replayer over `TopicDLQ` leaves **every** `ReplayerMetrics` field at
  zero, its DLQ reader is on the ingest topic, and the projection event id is
  absent from the `ReplayState` and from the DLQ commits. A deliberate **control**
  case feeds the same `Failure` into the ingest DLQ and asserts it *is* closed
  as unresolvable, so the isolation assertion cannot pass vacuously. Plus the
  topology-invariant table (address separation, both directional guards, the
  unchanged `ValidateTopicTopology`, and the custom-address escape hatch).
- `cmd/audit-projector/config_test.go` — default is `TopicProjectionDLQ` and
  differs from both the ingest DLQ and the source topic; new collision cases
  (ingest DLQ via env and via flag, projection DLQ as its own source) expect the
  `invalid projector topology` prefix; a new
  `TestRunProjectorDeadLettersToProjectionDLQ` pins the producer's topic; the
  startup-ordering test gains an ingest-DLQ case asserting `called == false`.
  Flag-over-env-over-default precedence stays green.
- `cmd/audit-kafka-dlq-replay/main_test.go` — new `TestValidateReplayTopology`
  row; `TestResolveReplayConfigKeepsIngestDefaults`; and
  `TestResolveReplayConfigRejectsProjectionDLQWithoutBroker`, which asserts the
  returned config is the **zero value** so no producer/replayer can be built,
  plus an exec-level `TestReplayBinaryExitsBeforeAnyBrokerConnection` (real
  binary, `-brokers 127.0.0.1:1`) asserting a non-zero exit with
  `replay topology:` and no `replay round` / `replayed=` / `dlq_topic=` line.
- `cmd/audit-kafka-consumer/main_test.go` — `TestConsumerTopologyRejectsProjectionDLQ`,
  including that an empty DLQ keeps its existing no-publisher diagnostic.
- `checks/test_asyncapi_channels.py` — `test_derive_symbol_table` gains
  `audit.events.projection.dlq.v1 → TopicProjectionDLQ`; all ten existing rows
  keep their values.
