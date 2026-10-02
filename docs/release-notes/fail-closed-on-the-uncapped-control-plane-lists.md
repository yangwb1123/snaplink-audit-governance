# Fail closed on the uncapped control-plane authorization and schema field lists

The control-plane write routes now reject an oversized list instead of storing it verbatim.

`POST /api/v1/sources`, `PUT /api/v1/sources/{sourceID}` and `POST /api/v1/schemas`
previously applied **no resource cap** to `allowed_client_ids` or to
`required_fields` / `allowed_fields` / `encrypted_fields` / `searchable_fields`.
The only bound was the 512 KiB request-body cap, so a single registration could
persist a list that every later ingest re-scanned (`allowed_client_ids` via
`SourceSystem.AllowsClient`, the four schema lists via `validateEvent`,
`protectSensitiveFields` and `reconstructEventForDigest`).

## Behaviour change

- `allowed_client_ids`: at most **256** entries, each at most **8192 UTF-8 bytes**.
- each of the four schema field lists: at most **256** entries, each at most
  **8192 UTF-8 bytes**.
- A violation is `400` / `invalid_request` with the shared envelope-cap text,
  e.g. `invalid request: envelope size cap exceeded: allowed_client_ids count 257, max 256`.
  Exactly at the cap is still accepted (`201` create, `200` update).

The rejection happens in the service write boundary (`normalizeSource`,
`RegisterSchema`), before any `Store.Update`: no source row, no schema key, no
admin action and no read-trail row is written. No new error code, sentinel,
status, route or permission was introduced, and the ingest caps, the gRPC alias
table and the AsyncAPI/proto contracts are untouched.

## Operator notes

- `api/openapi/openapi.yaml` declares `maxItems` / item `maxLength` on the three
  **input** schemas only. The response schemas describe the cap but declare no
  maximum: reads are deliberately not gated, so a row registered before this
  change is still served verbatim.
- **Existing over-cap rows keep working.** `GET /api/v1/sources` and
  `GET /api/v1/schemas` return them unchanged and ingest keeps authorizing them.
  Only the *first rewrite* of such a row (`PUT /api/v1/sources/{id}`, or a new
  version of that schema) fails until the list is trimmed to the cap.
- No migration is required and no snapshot layout changed.
- The cap values are provisional and derived by the recorded recipe in
  `docs/BENCHMARKS.md` ("Control-plane list cap decision gate"); that section
  states that no production snapshot was measured in this run and that the
  distribution must be re-measured before any constant is widened.
- Inventory an existing state directory before upgrading:

  ```sh
  cp -r /var/lib/audit-governance ./state-copy   # the file backend flocks <path>.lock
  AUDIT_CAPS_STATE=./state-copy/state.json go test ./internal/service -run TestControlPlaneListDistributionReport -v
  ```

## Verification

```sh
go test ./internal/domain -run 'TestControlPlaneCapsAreFixed|TestValidateSourceCaps|TestValidateSchemaCaps' -v
go test ./internal/domain -run 'TestOpenAPI' -v
go test ./internal/httpapi -run 'TestHTTPControlPlane' -v
go test ./internal/service -run 'TestNormalizeSourceRejectsOverCap|TestAddSourceOverCap|TestUpdateSourceOverCap|TestRegisterSchemaOverCap|TestBootstrapInProcessCaller|TestEmptyAllowedClientIDs|TestSchemaVersionSupersetAndRollback|TestSchemaListsAtCap|TestLegacyOverCapSourceRow|TestIngestValidationUnchangedByCaps' -v
go test ./internal/service -run 'TestSourceAuthorizationCost|TestControlPlaneListDistributionReport' -v
```