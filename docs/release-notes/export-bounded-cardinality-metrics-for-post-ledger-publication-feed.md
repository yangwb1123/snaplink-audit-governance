# Export bounded-cardinality metrics for post-ledger publication failures and the durable outbox backlog

`audit-api` now exports the post-ledger publication feed on `GET /metrics`. That
path is the only channel feeding the `audit.projector` / ClickHouse projection,
and it was the one failure mode in the product with **no** machine-readable
signal: a broker refusal was `logf`-only, a flush-round failure was `log`-only,
and the durable backlog grew in `Snapshot.LedgeredOutbox` with no counter, no
gauge and no alert. A broker outage therefore stalled the projection indefinitely
while the dashboard stayed fully green — every other failure mode
(`audit_ingest_conflicts_total`, `audit_ingest_quota_exceeded_total`,
`audit_store_operation_deadline_exceeded_total`,
`audit_consumer_dlq_publish_failures_total`, …) already had a series.

Three new series, all **unlabelled** (a tenant, event, route, host, DSN or reason
label would make the endpoint grow with traffic, the same rule as
`audit_ledgered_intent_repaired_total`):

| Series | Type | Meaning |
|---|---|---|
| `audit_ledgered_publish_failures_total` | counter | one publication attempt this process could not complete: broker refusal, event clone failure, or a delivery whose durable acknowledgement failed. Counts both the ingest request path and the background flush round. |
| `audit_ledgered_publish_success_total` | counter | one publication delivered **and** durably acknowledged (the intent left `Snapshot.LedgeredOutbox`). Counts both paths. |
| `audit_ledgered_outbox_pending` | gauge | `len(Snapshot.LedgeredOutbox)` observed by the most recent completed flush round, sampled **before** batch truncation, so it is the backlog and not this round's batch size. |

Operator-visible changes:

- **New alert** `AuditLedgeredPublishFailures` in the `audit-slo` group of
  `deploy/prometheus-rules.verify.yml`:
  `increase(audit_ledgered_publish_failures_total[15m]) > 0`, `severity: warning`.
  It mirrors `AuditDLQPublishFailures` exactly — no `for:`, no rate or count
  threshold; the only numeric literals are the mirrored `[15m]` window and `> 0`.
  `warning`, not `page`: the immutable ledger is authoritative and the intent is
  durable, so the impact is **projection staleness, not ledger loss** — the same
  class as `AuditConsumerLag` and `AuditDLQBacklog`. The ledgered publisher never
  blocks ingest, so a 1s retry loop would page continuously. The rules file now
  carries 19 rules (audit-dlq 8 + audit-slo 9 + audit-production 2) and
  `docs/SLO_ALERTS.md` §2 is updated to match.
- **The three series are omitted, not zero-filled, unless a `LedgeredPublisher`
  is configured**, and the gauge is omitted again until one flush round has
  sampled it. A process that cannot publish anything must not report "0 failures
  since boot" as if it were healthy. The reference deployment starts with no
  broker, so its `/metrics` is unchanged; with a publisher wired the series
  appear and `audit_ledgered_outbox_pending` returns to 0 within one flush
  interval. Fallback signal on a publisher-less deployment is the existing
  `ledgered publish failed` log line.
- **`audit_ledgered_outbox_pending` is a last sample, not a live read.** It is
  refreshed by the flush round (`cmd/audit-api/ledgered.go`, one second), so it
  reads a drained value on the next tick, and a frozen store or a contended round
  leaves the previous value in place rather than reporting a fabricated number.
  There is deliberately no alert on the gauge: it would need a `for:` duration or
  a pending-count threshold, and neither is a number this change is entitled to
  invent. Store degradation is already covered by
  `audit_store_operation_deadline_exceeded_total` and `readyz`.
- **A `/metrics` scrape cannot block on the store.** All three series are atomic
  loads taken in the handler; the backlog is sampled out of band by the flush
  round, never under `store.Store.mu` and never issuing a statement (the same
  rule `Store.PoolStats` already follows).
- **The three causes are not labelled.** They have three distinct fixes but
  already three distinct log lines (`ledgered publish failed`,
  `ledgered publish clone failed`, `ledgered outbox acknowledgement failed`),
  which is how an operator tells them apart. Counters are per-process, as
  `audit_http_errors_total` already is: aggregate with `sum(...)`/`increase(...)`
  across `job="audit-api"`; the gauge is a per-process snapshot.

No behaviour change: at-least-once is unchanged (a failure still leaves the
durable intent for the next round, and acknowledgements stay in the same
`Store.Update`), the per-key publication gate, batching, the 1s cadence, backoff
and the ingest answer are untouched, no log line was added or removed, and
`cmd/audit-api/ledgered.go` needed no edit — the existing loop became the
sampler. Rollback is a pure code revert; **revert the alert rule before the
code**, otherwise a rolled-back process publishes no series while the rule is
live, which is the same silent state as a publisher-less deployment.