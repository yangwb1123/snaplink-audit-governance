# Release Notes

## 2026-08-06 — tenant/field-scoped search digests; digests stripped from API and export responses

**Behavior change (security, threat-model boundary D):** search digests are
now bound to the tenant ID and field name. `security.SearchDigestBound`
derives `sd2:`-prefixed digests whose HMAC input is the canonical JSON value
plus the tenant ID and field name, so the same plaintext in two tenants (or
under two field names) yields different digests — an actor holding read
access to several tenants can no longer correlate records across tenants by
digest equality. The stored key name (`<field>__search_digest`) and the
wire/OpenAPI contract are unchanged.

- Search compatibility: `QueryEvents`, legal-hold filtering and export
  filtering accept both formats. Same-format digests compare directly;
  mixed formats re-derive the query-format digest from the stored plaintext
  (fail-closed when the plaintext is unavailable — encrypted+searchable
  fields are same-format only). Old v1 clients can search events ingested
  under the new format and new clients can search legacy events; event
  idempotency and `VerifyIntegrity` are unaffected (`SourceDigest` is
  computed pre-protection and `reconstructAndDerive` deletes digest keys by
  schema name).
- Response redaction: `GET /api/v1/events/{id}` and `GET /api/v1/events`
  now strip every `*__search_digest` key from the payload recursively, on
  deep copies only (the store keeps the digest for search; a strip failure
  returns 500 rather than leaking). Decrypted export JSONL is stripped the
  same way.
- Migration: none — digest values are derived at ingest; existing events
  keep their v1 digests and keep matching through the fallback. Plain
  deploy/revert; no backfill (WORM immutability).
- Scope guard: archive stripping, key rotation/HKDF separation, and
  timeline/replay endpoints remain explicitly out of scope (sibling
  findings).

## 2026-08-06 — outbox.Insert reports duplicate/conflict outcomes instead of silently dropping events

**Behavior change (data integrity):** `outbox.Insert` no longer discards the
`ExecContext` result of its targetless `ON CONFLICT DO NOTHING` insert. When
`RowsAffected() == 0` (one of the two uniqueness constraints — `event_id` or
`(tenant_id, idempotency_key)` — absorbed the row), `Insert` now classifies
the outcome inside the caller's transaction and returns a defined error
instead of nil, so a business transaction can no longer commit its domain
mutation believing the audit event was queued when no outbox row exists and
the relay will never deliver anything. Return contract (also on the `Insert`
doc comment): `nil` = durably recorded, or an exact duplicate of an existing
pending/delivered row (idempotent re-inserts never reset
`attempts`/`next_attempt_at`); `errors.Is(err, domain.ErrConflict)` =
deterministic conflict (same `event_id` with different canonical content,
idempotency-key reuse for another event, an identical row already
dead-lettered, or an unclassifiable zero-row outcome) — roll back, surface
409, **do not retry**; any other error = acceptance unknown (statement or
classification read failed) — roll back and retry with bounded backoff plus
jitter. Identical content is decided by jsonb equality first with an
`EventContentDigest` cross-check when jsonb differs only in representation
(same instant in another time zone, `1.0` vs `1`) — matching the ingest
path's canonicalization; a dead-lettered row is never silently resurrected.

- Public API unchanged: `Insert(ctx, tx Execer, event) error` and `Execer`
  are byte-identical; only unexported seams were added (`rowScanner`,
  `rowQueryer`, `classifyZeroRows`). No schema change, no new sentinel, no
  route/OpenAPI change.
- Callers: there are no production callers of `outbox.Insert` yet (the e2e
  suite writes `audit_outbox` directly); the change defines the contract
  future business-transaction writers rely on. Callers must roll back on
  any error and must not retry `ErrConflict`.
- Ops: none — the SQL shape is unchanged (still targetless `DO NOTHING`),
  so the relay's `ListPending` polling behavior is untouched. The payload
  parameter is now sent as text instead of `[]byte` (bytea has no cast to
  jsonb under the simple protocol), which is required for the insert to
  work on a real PostgreSQL.
- Known limits (accepted): under REPEATABLE READ/SERIALIZABLE a concurrent
  duplicate whose winner committed after this transaction's snapshot
  degrades fail-closed (raw unique violation, non-`ErrConflict` wrapped
  error) instead of classifying as idempotent; callers must keep the
  default READ COMMITTED. DSN-gated integration scenarios (S1–S8) cover
  this and the concurrent-duplicate determinism; they run only when
  `AUDIT_TEST_POSTGRES_DSN` is set.

## 2026-08-06 — Kafka consumer dead-letters permanently failing messages

**Behavior change (availability):** `audit-kafka-consumer` no longer retries
poison events forever. Ingest failures classified as permanent
(`outbox.DeliveryError{Permanent}` — the audit API's 4xx except 429) are
dead-lettered immediately; transient failures are retried **in place** — the
message is held in-process and re-delivered to the ingest callback up to
`-max-attempts` (default 8, env `AUDIT_KAFKA_MAX_ATTEMPTS`), and the next
message is fetched only after the held one is resolved (success, permanent
dead-letter, or cap exhaustion). This is load-bearing: kafka-go's reader
advances its fetch position past every message it returns, committed or not
(`reader.go`: `r.offset = msg.Offset + 1`), so re-fetching after a failure
would silently skip the failed message once a later message commits. The
pre-change loop had exactly that defect for transient failures; retrying the
held message closes it. A dead-letter publishes a `Failure` record
(`event_id`, `error_code`, `error_message` — the AsyncAPI
`audit.events.dlq.v1` contract, keyed by the failing event ID) via
`-dlq-topic` (default `audit.events.dlq.v1`, env `AUDIT_KAFKA_DLQ_TOPIC`),
commits the message, and continues. A DLQ publish failure or a consumer
without a publisher degrades to commit + log and never blocks partition
progress. New `error_code` values: `permanent_error`, `attempts_exhausted`.

- Wire-compatible: `audit-projector` keeps its existing flags and inherits
  the capped-retry behavior with commit+log degradation (no DLQ publisher);
  projection gaps after 8 failed inserts are rebuildable via `RebuildFrom`.
- Ops: pre-create `audit.events.dlq.v1` with ≥30d retention before deploy
  and verify the created topic's retention — broker auto-create (if
  enabled) would create it with default retention instead. Poison events
  are expected to clear consumer lag within one backoff cycle instead of
  stalling the partition.
- Known limits (accepted, tracked as follow-ups): (1) the cap also applies
  to outage-class errors — at defaults (30s HTTP timeout + 2s backoff × 8
  attempts) a dependency outage longer than ~4.3 min drains the partition
  into the DLQ as `attempts_exhausted`; the DLQ has no replay consumer yet,
  so recovery is manual (DLQ consumer + traffic alert are the follow-up).
  (2) Sustained 429 quota throttling beyond the same window dead-letters
  valid events. (3) There is no per-message ingest deadline (spec
  non-goal): a hung ingest (e.g. ClickHouse) can still stall a partition;
  the ledger path is bounded by the HTTP client `-timeout`. (4) A static
  `AUDIT_OUTBOX_TOKEN` that expires mid-run turns every event into a
  `permanent_error` DLQ record — rotate tokens before expiry.

## 2026-08-06 — Separation of duties enforced in restore approval

**Behavior change (security):** approving or rejecting a restore run now
requires a decision actor different from the actor who created the run. The
same principal can no longer create and decide a restore (previously the
happy path). The 403 response was already part of the documented contract
(`openapi.yaml` decide endpoints), so this is a wire-compatible tightening:

- The guard runs inside the atomic `Store.Update` closure in
  `transitionRestore` after the existing 404/409 checks, with precedence
  pinned **NotFound → Conflict → Forbidden** (404 masks cross-tenant
  existence; 409 dominates for decided runs). A refusal commits nothing:
  no version bump, no admin-action record.
- **Auth hardening (separately revertible):** the JWT `sub` claim is now
  parsed with the same strict rule as `client_id`/`azp` (`strictIdentityClaim`)
  — whitespace-padded or non-string subjects are rejected instead of
  slipping past the empty-string check. Without this, a padded `sub` could
  canonicalize the same principal into a different-looking actor string and
  bypass the exact-string same-actor guard.
- Dev-auth deployments become effectively single-principal per tenant
  (`sub == tenant ID == creator`), so every run is unapprovable via dev
  tokens. Documented escape hatch: a platform token
  (`audit:platform:cross_tenant`) naming the tenant via the `?tenant_id`
  query parameter — but even the platform cannot self-approve its own run.
- **Mixed-version window:** during a rolling deploy, the old binary can
  still approve runs the new binary refuses. Both write well-formed state
  (the old one simply lacks the check); no data migration or backfill is
  needed.

Migration: none (no data/config change). Rollback = revert the guard commit
(refusals write nothing, so reverting restores the old behavior with no
state repair). New tests: strict-sub rejection, same-actor 403 at HTTP and
service level, refusal atomicity (no admin action), concurrent distinct-actor
(1 winner / 1 conflict) and same-actor (all refused) races, platform escape
hatch.

## 2026-08-06 — Internal error details redacted from 5xx responses

**Behavior change (security):** HTTP error responses with status ≥ 500 no
longer carry internal error text. Previously `writeError` and the batch
partial-receipt path serialized `err.Error()` verbatim, so filesystem paths,
store paths, errno strings and crypto details (e.g. `open …/state.json.tmp:
is a directory`, archive `ENOTDIR`/`EISDIR` text) leaked to API clients on
download and ingest failures. `GET /api/v1/exports/{jobID}` also surfaced the
raw export-failure diagnostic via the `error` field.

Details:

- `errorBody(status, err, r)` is now status-keyed: every ≥ 500 response
  collapses to the fixed `internal server error` message / `internal_error`
  code (the code keeps the domain mapping, which by construction never maps
  to ≥ 500, so the two cannot contradict). The confirmed-dead
  `strings.Contains(message, "internal server error")` branch and the no-op
  `if status >= 400 { _ = r }` block were removed.
- The `Cache-Control: no-store` header on 5xx, the panic-recovery path, all
  4xx messages, the `os.IsNotExist`→404 download mapping and the envelope
  (`code`/`message`/`request_id`) are unchanged; OpenAPI `Error.message` is an
  unconstrained string, so the contract is schema-compatible with no spec
  edit.
- `getExport` masks a failed job's `error` field to the fixed
  `"export failed"` message at the API boundary on a value copy. Raw
  diagnostics stay in the operator-only snapshot (`state.json`) —
  persist-time sanitization is a documented deferral.

Migration: none (internal response-body change; no data/config migration).
Rollback = revert the commit. No new logging was added.

## 2026-08-06 — Dev authentication flips to fail-closed defaults

**Behavior change (security):** `-allow-dev-auth` now defaults to `false`, so a
bare `./audit-api` run with no JWT trust source fails startup with the
preserved string `no JWT verification trust source is configured` instead of
silently accepting unsigned `dev:<tenant>:<role>` tokens. Previously any
default deployment let an unauthenticated remote caller mint platform-admin
dev tokens (full cross-tenant read/write/governance access); malformed env
values even failed *open* (`boolEnv` fell back to the default `true`).

Details:

- **Default flip:** `flag.Bool("allow-dev-auth", strictBoolEnv("AUDIT_ALLOW_DEV_AUTH", false), …)`. Runtime semantics of an explicit `AUDIT_ALLOW_DEV_AUTH=true` / `-allow-dev-auth=true` are unchanged; `deploy/docker-compose.verify.yml` already sets the env (zero compose changes).
- **Strict env parsing:** `boolEnv` → `strictBoolEnv` for all four audit-api boolean vars (`AUDIT_ALLOW_DEV_AUTH`, `AUDIT_ALLOW_LOCAL_HS256`, `AUDIT_ALLOW_INSECURE_JWKS_LOOPBACK`, `AUDIT_ALLOW_DEV_SECRETS`). A malformed value exits 1 naming the variable *before* `flag.Parse` (so even `-h` exits 1); `strconv.ParseBool` literals (`1`/`TRUE`/`t`) behave like `true`; present-but-empty falls back to the default. The worker binary keeps its own lenient `boolEnv` (documented non-goal — its fallback is `false`, i.e. fail-closed).
- **Preflight gate:** `-check-config` now validates the authentication configuration with the same rules as startup (zero-auth configs fail preflight) and applies an **environment-only** dev-auth allowlist: `-allow-dev-auth` alone can never satisfy the gate (flag-only invocations fail with the auditable marker `check_config=fail auth=dev_auth_flag_not_allowlisted`); `AUDIT_ALLOW_DEV_AUTH=true` passes. Success output `check_config=ok …` is byte-identical.
- **Flag-beats-env precedence is unchanged** at runtime: `-allow-dev-auth=false` + env `true` keeps dev auth off; `-allow-dev-auth=true` + env `false` keeps it on — but the preflight fails in the latter cell, so CI cannot bless a config the runtime enables without the env allowlist.

Migration: set `AUDIT_ALLOW_DEV_AUTH` explicitly in the **runtime** environment (`true` only for dev; `false`/absent for prod), scrub any malformed values (they now hard-fail naming the variable), and remove `-allow-dev-auth` from CI `-check-config` invocations. Rollback = redeploy the previous binary; no data migration. Dev stacks should bind the published port to loopback (`127.0.0.1:19089:8089`).

## 2026-08-06 — Archive enablement keyed on the configured Store

**Behavior change (fix):** archiving is now enabled based on the configured
`archive.Store`, not the `ArchiveDir` string. In S3-only deployments (an S3
store injected via `external.Archive()` while `Config.ArchiveDir` stays empty)
the ingest gate now archives events and `ArchivePending` retries pending
receipts. Previously both treated the destination as unconfigured: ingest
skipped archiving and receipts stalled at `StatusIndexed` forever.

Details:

- New `archive.Configured(Store)` predicate (nil-safe; empty-dir `FileStore` →
  unconfigured, S3 and injected stores → configured), shared by the ingest
  gate, `ArchivePending`, and the readyz probe so the call sites cannot
  re-diverge.
- `FileStore.Put` now rejects an empty `Dir` before any filesystem access
  (same message as `Ready`) — previously it silently wrote into the process
  CWD.
- readyz now skips unconfigured stores instead of panicking on a nil store.

Only behavior flips: the S3-only bug itself, and the broken split-brain
configuration `&FileStore{Dir:""}` + `ArchiveDir:"/x"` (silent CWD writes →
loud `ErrInvalid`). Local file-mode behavior is unchanged. No migration
needed; rollback = redeploy the previous binary.

## 2026-08-06 — FileStore.Put fails loudly on corruption instead of reporting archived

**Behavior change (fix):** `archive.FileStore.Put` no longer treats *any*
pre-existing path as "already archived". Previously a write/sync failure
mid-Put left a truncated partial object at the final key, every later retry
hit EEXIST and returned nil, and the receipt was marked `StatusArchived` —
a permanently corrupt object in a WORM compliance archive with no repair
path. The same EEXIST branch silently accepted symlinks and directories.

New `FileStore.Put` contract (no signature/API/route changes):

- A pre-existing path that is not a regular file (symlink, directory,
  device) is an error.
- An existing regular file is only "already archived" when it is
  byte-identical to the new payload; a mismatch — or a file that cannot be
  read, and therefore cannot be verified — is an error, and the pre-existing
  object is never modified or removed.
- Any Write/Sync/Close error after the object was created removes it
  best-effort, keeping the invariant "Put returned an error ⇒ no object at
  the key". O_EXCL/`0o440` and the idempotent byte-identical retry behavior
  are unchanged.

**Impact:** identical retries (the normal replay path) still return nil;
only previously-silent corruption becomes loud. Ingest degrades such
failures to `StatusIndexed` (unchanged service behavior), so receipts stay
retryable via `ArchivePending` — operators must clear the blocking object
first. S3Store is unaffected. No data/config migration; rolling deploy with
a transient window where old and new binaries disagree on mismatch verdicts
(old: nil, new: error); rollback = redeploy the previous binary.

## Operator runbook — archive mismatch errors

**Symptoms:** `ArchivePending` returns `archive path ... already exists with
different content` (or `... cannot be verified`); affected receipts remain
`StatusIndexed` and are retried on every `ArchivePending` run.

**Diagnosis:** for the failing key (see `archiveEvent`/`archiveSegment` key
formats in `internal/service/service.go`), compare the object on disk with
the expected canonical JSON:

```sh
ls -la <archive-dir>/<key>
# regenerate the expected payload, e.g. for an event:
#   jq -S . <(git show HEAD:internal/service/service_test.go)  # reference only
od -c <archive-dir>/<key>   # truncated/corrupt content = crash-leftover partial
```

**Cause classification:**

1. Crash- or error-leftover partial object from a failed Put under the old
   binary (pre-deploy). The object is *not* the true event payload → safe
   to remove, then re-run `ArchivePending`.
2. Tampered object (an external process wrote the path). Removing it is a
   deliberate WORM exception — confirm with the tenant/compliance owner
   first; the re-archived payload is re-verified by `VerifyIntegrity`.
3. Genuine key collision (different payload, same key). Do **not** remove:
   investigate the producer; the receipt correctly refuses to mark it
   archived.

**Repair:** after removing a confirmed-partial object, re-run
`ArchivePending <tenant>` (or `audit-governance-worker`'s pending-archive
job); the receipt converges to `StatusArchived` only after a byte-identical
write. If `os.Remove` itself fails (read-only filesystem, permissions),
fix the destination and retry — the receipt stays `StatusIndexed` and is
never falsely reported archived.

**Verification:** `python3 cli.py check`; archive unit tests
`go test ./internal/archive/` cover the mismatch, unreadable, non-regular,
and write-fault (EFBIG/read-only) legs.
