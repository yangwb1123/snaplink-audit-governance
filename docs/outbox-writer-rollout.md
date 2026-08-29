# Outbox writer inventory and rollout verification

This note is the release evidence for the `SourceDigest`-independent outbox
classifier. It covers the actual writer surfaces visible from this checkout and
the sibling source trees available during verification. The target is this
repository's PostgreSQL `audit_outbox` schema (`migrations/001_control_plane.sql`
and `migrations/003_outbox_relay.sql`) and the Go SDK
`internal/outbox.Insert`.

## Inventory result

The source inventory was run on 2026-08-29 with the following searches:

```sh
git grep -n -E 'outbox\.Insert|outbox\.Insert\(|INSERT INTO audit_outbox' \
  -- ':!docs/auto/**' ':!logs/**'
rg -n --hidden -g '*.go' -g '*.sql' -g '*.yaml' -g '*.yml' \
  'github.com/snaplink/audit-governance/internal/outbox|outbox\.Insert|audit_outbox' \
  /home/u1/workspace/demo /home/u1/aero-id /home/u1/aero-im \
  /home/u1/aero-vault /home/u1/snaplink-sso /home/u1/snaplink-ai
```

Results:

| Source | Verified writer surface | Target SDK/schema? | Rollout action |
|---|---|---:|---|
| This repository | `internal/outbox/sdk.go::Insert` is the only production SDK implementation; its callers in `internal/outbox/*_test.go` are tests. `cmd/audit-outbox-relay` reads and updates rows but does not insert them. | Yes | Ship this classifier change. |
| This repository's `test/e2e/fullstack.sh` | Direct SQL inserts seed the verification database. They are test fixtures, not a production writer or SDK consumer. | No | No binary upgrade. |
| `snaplink` at source revision `fc79ad70` | `infrastructure/auditoutbox/sqlite.go::InsertEventTx` writes the sibling service's SQLite `audit_outbox`; `infrastructure/postgres/tenantcommerce/outbox.go::insertOutboxEventTx` writes `tenant_commerce_outbox`. Neither imports this module or uses `internal/outbox.Insert`. | No | No change for this patch. |
| `snaplink` HTTP integration | `infrastructure/auditgovernance/http_client.go::governanceEvent` sends events directly to the governance API. Its wire type has no `source_digest`; the API's normal ingest path derives the server digest. | No | Existing source revision is compatible; verify its normal API integration separately. |
| `aero-vault` at source revision `b6e85c8` | `internal/repository/audit_governance_write.go::insertAuditGovernanceResult` writes `audit_governance_outbox`, and `internal/auditgovernance/http.go::governanceWire` sends a separately owned redacted wire event with no `source_digest`. | No | No change for this patch. |
| `aero-im` | `crates/aero-audit-connector/src/outbox.rs` and its `audit_governance_outbox` migrations are Rust-owned; `src/client.rs` posts claimed payloads directly and has no target SDK dependency or `source_digest` field. | No | No change for this patch. |
| `aero-id` | `internal/connector/auditgovernance/publisher.go` posts its separately owned audit facts directly; no reference to this repository's module path or `internal/outbox.Insert` was found. | No | No change for this patch. |
| `snaplink-sso`, `snaplink-ai` | No reference to this repository's module path or `internal/outbox.Insert` was found in the available source. Any local outbox packages are separately owned. | No | No change for this patch. |

The target repository therefore has **no in-repository production caller**, and
no external consumer of this repository's `internal/outbox` SDK was found in
the available sibling source trees. The sibling `snaplink` outbox rows must not
be mistaken for the target PostgreSQL rows: they have different schemas and are
drained by a different client path.

## Release decision and guard

The updated `internal/outbox` package is safe to roll out for the inventoried
writer set. There is no database migration or relay rollout required. The
classifier change must be included in every future binary that imports this
SDK; updating only `audit-outbox-relay` cannot repair an old writer, because the
classification happens inside the caller's transaction.

A deployment artifact not represented by the inventory above is a release
blocker until its source or binary is checked for `outbox.Insert` and the AC-1
scenario is run against its disposable outbox database. Each writer must:

1. call `Insert` in the same transaction as its business mutation;
2. roll back on every returned error;
3. treat `errors.Is(err, domain.ErrConflict)` as a non-retryable conflict; and
4. never replace the existing row after a conflict.

The regression evidence for this contract is:

- `internal/domain/canonical_test.go::TestEventContentDigestExclusionMatrix`;
- `internal/domain/canonical_test.go::TestEventDigestPreservesNonEmptySourceDigest`;
- `internal/outbox/sdk_test.go::TestInsertConflictWithSameSourceDigest`; and
- the DSN-gated AC-1 scenario in
  `internal/outbox/postgres_test.go::TestPostgresStoreIntegration`.

For a newly discovered writer, run the focused test and then the full repository
quality gate before promoting the writer. Historical false duplicates cannot be
reconstructed from the existing outbox row; this rollout prevents new ones.
