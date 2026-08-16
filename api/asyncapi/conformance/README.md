# Producer conformance fixtures — EventEnvelope rejection matrix

`producer_minimal.json` is the executable-backed documentation of what the
ingest API does with envelopes that satisfy the fixed `EventEnvelope`
contract surface (12-token `required`, nested `actor.required: [id]`, and the
`anyOf` payload|payload_ref at-least-one disjunction) but violate a
validation rule. Each record carries:

| Key | Meaning |
|---|---|
| `name` | case id; also the Go sub-test name |
| `envelope` | a full EventEnvelope JSON body (unknown wrapper keys are benign — the Go decoder ignores them) |
| `expected_status` | HTTP status the runtime must return (machine-checked) |
| `expected_code` | `error.code` (machine-checked) |
| `documented_rejection` | human prose for the rejection; used by the fixture itself so a stale claim is visible in review, but the status/code are authoritative |

The test that reads this file
(`internal/httpapi/asyncapi_contract_test.go`) additionally derives the
expected status from the mapping boundary
(`domain.Event.ValidateBasic` → `statusForError`) before the HTTP
round-trip for every case where validation rejects the envelope, so a
fixture that re-claims a status the runtime cannot produce fails on first
read even if the handler someday changes.

## Matrix

| Case | Envelope delta vs spec-conforming | Runtime path | Status | `error.code` |
|---|---|---|---|---|
| `producer_minimal_payloadless` | all 12 required + `actor.id` present; **no** payload/payload_ref, no tenant_id | `ValidateBasic` → `ErrInvalid` (`payload or payload_ref is required`) | **400** | `invalid_request` |
| `tenant_mismatch_contrast` | validated-conforming shape (payload present) + `tenant_id: "other"` (bearer resolves tenant-a) | `Ingest` (after `ValidateBasic` passes) → `ErrTenantMismatch` (DS-08) | 422 | `tenant_mismatch` |
| `occurred_at_out_of_range_contrast` | same shape, `occurred_at` = year 2300 | `ValidateBasic` → `ErrOccurredAtOutOfRange` (sentinel matched before `ErrInvalid`) | 422 | `occurred_at_out_of_range` |

## Why the payload-less case is 400, not 422

The 422 class is reserved for the three sentinel errors the hint text
documents: `schema_not_found`, `tenant_mismatch`, `occurred_at_out_of_range`
(`server.go statusForError` matches `ErrOccurredAtOutOfRange` before
`ErrInvalid`). A missing payload/payload_ref (or missing `actor.id`) is an
ordinary contract violation → `ErrInvalid` → **400 `invalid_request`**. A
producer that built to a spec where payload|payload_ref were optional was
already rejected 400 by the runtime; the fixed asyncapi.yaml merely documents
that enforcement (`anyOf`).

## Spec layout invariants the fixture depends on

- `actor` stays on one line (`actor: { type: object, required: [id], ... }`):
  the `asyncapi_envelope_props` regex in `checks/contract_fields.py` captures
  property names with `^\s+(\w+): \{`, so a multi-line reflow would drop the
  `actor` property from the coverage check and red the gate.
- `anyOf` stays a single-line flow at `EventEnvelope` level so the strict
  YAML-subset parser (`checks/asyncapi_channels.parse_spec`) keeps treating
  it as an opaque balanced flow and never leaks it into the property set.