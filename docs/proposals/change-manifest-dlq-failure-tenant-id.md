# Change Manifest — DLQ `Failure.tenant_id` + consumer tenant scope

Status: Implemented in the reference tree · Date: 2026-08-20 · Module: `api/asyncapi` + `cmd/audit-kafka-consumer` + `internal/kafka`
Direction: "Add tenant-scoped validation to the Kafka consumer — foreign-tenant events on the shared accepted topic are permanently dead-lettered and dropped from the ledger"

Platform-contract change discipline: this is a **cross-contract change** (the
AsyncAPI `Failure` payload gains an optional property). Per the platform
contract's change discipline, this manifest records the contract, the change,
the compatibility analysis, the producer/consumer inventory, and the rollback
path so the field cannot drift or orphan. The companion design is
`docs/auto/runs/add-tenant-scoped-validation-to-the-kafka-consum-31d05198/artifacts/design-472034f754eb71d0/design.md`.

## 1. Contract

- **Spec:** `api/asyncapi/asyncapi.yaml`, `components.messages.Failure` payload.
- **Channel:** `audit.events.dlq.v1` — address, `publishFailure` operation, and
  the channel↔Go-symbol alignment (`checks/asyncapi_channels.py` Rule A/B) are
  **unchanged**.
- **Required set (unchanged):** `[event_id, error_code, error_message]`.

## 2. Change

Additive optional property `tenant_id: string` to the `Failure` payload, with
`required` unchanged. Go side: `internal/kafka/kafka.go` `Failure` struct gains
`TenantID string \`json:"tenant_id,omitempty"\``; populated in
`Consumer.deadLetter` (the decoded-event paths: permanent, attempts-exhausted,
unauthorized) and empty in `Consumer.deadLetterUnparsable` (the payload was not
decodable, so no tenant can be recovered).

**Semantics (identity-protocol F-1):** `tenant_id` is the tenant **claimed by
the event envelope at dead-letter time** — an untrusted topic value, **never
server-verified**. The ingest API's server-resolved tenant (DS-08,
`internal/service/service.go`) is the only authorization authority. Any future
consumer (the replayer is the intended one) must keep API re-enforcement as
its sole authorization gate and use `tenant_id` for display/filtering only;
it must never scope, route, or attribute by this field without API
re-enforcement.

Companion change in the same change set: the consumer gains an optional tenant
scope (`-tenant` / `AUDIT_KAFKA_TENANT`, `Consumer.WithTenant`): foreign-tenant
events are skipped + committed before ingest (never dead-lettered), counted in
the disjoint `audit_consumer_foreign_tenant_skipped_total` metric, and logged
with both `tenant=` (envelope) and `consumer_tenant=` (configured) for FM-3
diagnostics. The two changes are shipped together because the scope makes the
`tenant_mismatch` → `permanent_error` → REQ-PERM-1 one-shot closure path
unreachable for correctly-scoped instances, and the DLQ field makes the
remaining dead-letter paths self-describing.

## 3. Compatibility analysis

| Direction | Behavior | Evidence |
|---|---|---|
| New producer → old consumer | Replayer `collectFailures` decodes with `json.Decoder`; unknown fields ignored → 4-key records decode fine, `tenant_id` unused | `internal/kafka/replay.go` decode path; verified at HEAD `4021404` |
| Old producer → new consumer | 3-key records decode with empty `TenantID`; `omitempty` keeps the unparsable path's serialization byte-identical to today | `internal/kafka/kafka_test.go` `TestFailurePayloadMatchesAsyncAPISchema` (extended in this change set) |
| Mixed-version DLQ records | Both record shapes coexist on `audit.events.dlq.v1`; both decode in both directions; DLQ key (`event_id`) unchanged → replay routing untouched | — |

`required` unchanged means the contract stays additive-safe both directions.
The contract-change test moves with the schema in the same change set
(`TestFailurePayloadMatchesAsyncAPISchema` pins the exact serialized key set)
and `checks/contract_fields.py` now pins the YAML-side key set/required
(FM-6 hardening), so a schema edit without the struct update (or vice versa)
fails the gate.

## 4. Producer/consumer inventory

- **Producers** (exactly two publish sites, verified): `Consumer.deadLetter`
  (populates `tenant_id` from the decoded envelope), `Consumer.deadLetterUnparsable`
  (leaves it empty). `PublishFailure` and the DLQ key selection are unchanged.
- **Consumers:** the replayer `collectFailures` (decode-only, **unchanged** —
  it ignores the new field). Intended future consumer: replayer `dlqRecord`
  capture of `tenant_id` for round-summary attribution (**deferred by design**,
  out of this change set; this manifest records it so the field does not orphan).
- **Registered in** `docs/evolution/state.jsonl` as a `change_manifest` record
  (local trace; the durable record is this file — `docs/evolution/` is
  gitignored).

## 5. Rollback

- Revert schema + struct + population together (one change set); old
  3-key-only binaries interoperate with both record shapes in both directions;
  no replayer change is required either way.
- The consumer scope rolls back with the binary + compose env together
  (FM-7): an old binary ignores `AUDIT_KAFKA_TENANT` and foreign-tenant events
  resume the pre-fix dead-letter/REQ-PERM-1 loss chain — the recognizable
  regression spike is the `audit_consumer_dlq_published_total` /
  `permanent_error` volume resuming.

## 6. Operator hazard (identity-protocol F-2)

A non-fatal startup warning fires when a dev token's tenant differs from
`AUDIT_KAFKA_TENANT` (`warning: tenant scope mismatch tenant_claim=… consumer_tenant=…`):
a misaligned instance would otherwise silently skip every own-tenant event.
JWT-configured instances (claims not verified at consumer startup) are covered
by the skip counter + the skip log's `tenant=`/`consumer_tenant=` pair, and by
the `AuditConsumerForeignTenantSkips` Prometheus alert added in this change
set.
