# Design — Idempotency-key conflict must not corrupt the owner receipt

**Module:** `internal/service`
**Direction:** `idempotency-key-conflict-permanently-corrupts-th-6365419a`
**Status:** Verified → Implemented → Quality gate PASS

This document treats the supplied requirements as *untrusted claims*, verifies
each against the working tree, then specifies the concrete fix, its API
surface, compatibility constraints, failure modes, migration steps, and the
testable acceptance mapping. The fix is already implemented and proven by the
quality gate (`python3 cli.py quality` → `QUALITY PASS`).

---

## 1. Claim verification (evidence vs. tree)

| Claim | Verdict | Evidence in tree |
|-------|---------|------------------|
| E1 — legacy path mutates + returns owner receipt | **Confirmed** | `internal/service/service.go` ingest `Store.Update` closure: `receipt = data.Receipts[existingKey]` → sets `Conflict/ErrorCode/ErrorMessage` → `data.Receipts[existingKey] = receipt` → returns the mutated owner receipt. |
| E2 — hot/cold path mutates + persists + returns owner receipt | **Confirmed** | `internal/service/tenant_ingest.go` `ingestTenant` `Store.UpdateTenant` closure: `receipt = existingReceipt` → sets conflict fields → `view.Ledger.SetReceipt(receipt)` → returns it. |
| E3 — `idempotencyConflictKey` returns owner **key** | **Confirmed** | `internal/service/archive_fallback.go` returns `(key, true)` for the owner event/receipt. |
| E4 — `tenantIdempotencyConflict` returns owner **key+receipt** | **Confirmed** | `internal/service/tenant_ingest.go` returns `(key, receipt, true)`. |
| E5 — `GetReceipt` reads the stored receipt directly | **Confirmed** | `internal/service/service.go` `GetReceipt` reads `data.Receipts[store.EventKey(tenantID, eventID)]` and surfaces it. |
| E6 — legacy regression test never re-reads owner | **Confirmed** | `service_test.go:TestIdempotencyKeyCannotBeReusedAcrossEvents` asserts only the *returned* receipt. |
| E7 — archived-flow test never re-reads owner | **Confirmed** | `archive_eviction_test.go:TestArchivedReceiptProtectsIdempotencyKey` asserts only the *returned* receipt. |
| `domain.ErrConflict` sentinel | **Confirmed** | `internal/domain/models.go:42` `ErrConflict = errors.New("conflict")`. |

**Conclusion:** the bug is real and the diagnosis (owner receipt mutated/persisted
and returned to the colliding caller) is accurate. The supplied behavior
requirements R1–R4 and acceptance criteria AC1–AC3 are correct and complete.

---

## 2. Root cause

Both ingest paths locate the **pre-existing owner event A** that owns the
colliding idempotency key and then (a) mutate A's receipt in place and
(b) *persist that mutation* before returning it as the collider B's result.
Because the closure's local `receipt` is written back into the store snapshot
(`data.Receipts[existingKey] = receipt` / `view.Ledger.SetReceipt(receipt)`),
A's genuinely-successful receipt is permanently corrupted, and `GetReceipt(A)`
— which reads the stored receipt directly — surfaces the corruption.

The helpers `idempotencyConflictKey` / `tenantIdempotencyConflict` *hand back*
the owner's key/receipt, which structurally invites the misuse: the natural
next line is "set fields on this receipt and write it back."

---

## 3. API changes

### 3.1 Helper predicates no longer leak the owner identity

The helpers are renamed and narrowed so they **cannot** hand the caller the
owner's key/receipt — they now answer only "is this idempotency key already
bound to a different event?"

```go
// before
func idempotencyConflictKey(data *store.Snapshot, tenantID, eventKey, idempotencyKey string) (string, bool)
func tenantIdempotencyConflict(view *store.TenantView, tenantID, eventKey, idempotencyKey string) (string, domain.EventReceipt, bool)

// after
func idempotencyKeyReused(data *store.Snapshot, tenantID, eventKey, idempotencyKey string) bool
func idempotencyKeyReusedTenant(view *store.TenantView, tenantID, eventKey, idempotencyKey string) bool
```

Detection logic is **unchanged** (scan hot events + retained cold receipts /
ledger idempotency index); only the return shape changed. Each function has a
single caller, so the blast radius is two call sites. No callers in tests or
other packages reference the old names (verified by `grep`).

### 3.2 New shared constructor for the conflict receipt

```go
// conflictReceipt builds a self-describing conflict receipt for the colliding
// (rejected) ingest. It deliberately does NOT reference the pre-existing owner
// event's stored receipt.
func conflictReceipt(tenantID string, event domain.Event) domain.EventReceipt {
    return domain.EventReceipt{
        EventID:        event.EventID,
        TenantID:       tenantID,
        IdempotencyKey: event.IdempotencyKey,
        Conflict:       true,
        ErrorCode:      "idempotency_key_conflict",
        ErrorMessage:   "idempotency_key is already associated with another event",
    }
}
```

This is the only place the conflict fields live, removing the duplicated
literal strings from both ingest paths (DRY + guaranteed identical contract).

### 3.3 Ingest path transforms

**Legacy (`service.go`, `Store.Update` closure):**
```go
// before: read owner receipt, mutate it, write it back, return it
if existingKey, ok := idempotencyConflictKey(...); ok {
    receipt = data.Receipts[existingKey]
    receipt.Conflict = true
    receipt.ErrorCode = "idempotency_key_conflict"
    receipt.ErrorMessage = "..."
    data.Receipts[existingKey] = receipt
    return fmt.Errorf("%w: ...", domain.ErrConflict)
}

// after: build a fresh conflict receipt for the collider, touch nothing else
if idempotencyKeyReused(data, tenantID, key, event.IdempotencyKey) {
    receipt = conflictReceipt(tenantID, event)
    return fmt.Errorf("%w: ...", domain.ErrConflict)
}
```

**Hot/cold (`tenant_ingest.go`, `ingestTenant` `Store.UpdateTenant` closure):**
```go
// before: read owner receipt, mutate it, persist it, return it
if _, existingReceipt, ok := tenantIdempotencyConflict(...); ok {
    receipt = existingReceipt
    receipt.Conflict = true
    receipt.ErrorCode = "idempotency_key_conflict"
    receipt.ErrorMessage = "..."
    view.Ledger.SetReceipt(receipt)
    return fmt.Errorf("%w: ...", domain.ErrConflict)
}

// after: build a fresh conflict receipt for the collider, persist nothing
if idempotencyKeyReusedTenant(view, tenantID, key, event.IdempotencyKey) {
    receipt = conflictReceipt(tenantID, event)
    return fmt.Errorf("%w: ...", domain.ErrConflict)
}
```

`Service.Ingest`'s post-closure handling is **unchanged**: on
`errors.Is(err, domain.ErrConflict)` it returns `receipt, err`, so the collider
now receives the freshly-built conflict receipt while the store closure made
**zero** writes to the owner receipt.

---

## 4. Compatibility constraints

- **Error contract (R4):** `Ingest` still returns a non-nil error wrapping
  `domain.ErrConflict`. Existing `errors.Is(err, domain.ErrConflict)` callers
  and the `ErrorCode == "idempotency_key_conflict"` checks in
  `TestIdempotencyKeyCannotBeReusedAcrossEvents` /
  `TestArchivedReceiptProtectsIdempotencyKey` continue to pass (verified).
- **Idempotency semantics preserved:** tenant-scoped, non-reusable idempotency
  keys remain the behavior; only the *surfaced* receipt for the collider changed
  (owner receipt → fresh conflict receipt). Uniqueness enforcement is unchanged.
- **No new persisted state for the collider:** B's conflict receipt is *not*
  written to the store (per R2/R3). `GetReceipt(tenant-a, _, B.EventID)` returns
  `ErrNotFound` both before and after the fix (B was never committed), so no
  observable regression in read paths.
- **No API/transport change:** HTTP/gRPC/AsyncAPI contracts are untouched; the
  `EventReceipt` JSON shape is identical (same fields, different values only on
  the rejected-collider path).
- **Retry consistency:** a retry of B (same EventID + same key K) re-hits
  `checkExistingIngest`/`checkExistingTenantIngest` (B still not persisted) →
  falls through to the idempotency predicate → returns the same conflict. The
  rejection is stable and idempotent.
- **Size/complexity thresholds:** `conflictReceipt` is a tiny leaf; the two
  ingest closures shrink. All within `engineering.yaml` (`max_function_lines:
  260`, `max_decisions: 45`).

---

## 5. Failure modes & mitigations

| Failure mode | Why it can't happen now | Residual risk / note |
|--------------|------------------------|----------------------|
| Owner receipt corrupted by collision (original bug) | Closure no longer reads or writes the owner receipt; predicate returns only `bool`. | None. |
| Collider receives owner's receipt | `conflictReceipt` is built from B's own identity, never from A's stored copy. | AC1/AC3 assert `receipt.EventID == B.EventID`. |
| Spurious persistence of owner mutation | Removed `data.Receipts[existingKey] = receipt` and `view.Ledger.SetReceipt(receipt)` from the conflict branches. | AC3 asserts the persisted snapshot/ledger shows A clean. |
| Future misuse via helper | Helpers renamed + narrowed to `bool` so they cannot return the owner receipt to be mutated. | Code review / lint: no new `SetReceipt` in conflict branches. |
| CAS retry re-applies mutation | Conflict branch performs no store writes, so retry/replay is a no-op w.r.t. A. | Store commit/rollback semantics unchanged. |
| Hot/cold vs legacy divergence | Same `conflictReceipt` constructor used by both paths; predicates share detection semantics. | Both covered by parametrized test (`hotcold=false/true`). |
| Archived-owner collision | Owner receipt retains `IdempotencyKey` after archival (by design), so the cold-ledger index still detects the conflict; owner stays clean. | Covered by bonus test. |

---

## 6. Migration steps

This is a behavior correction with no schema, no migration, and no config flag.

1. **Apply code change** (done): rename predicates, add `conflictReceipt`, edit
   both ingest closures. No external callers affected (internal-only helpers).
2. **Run the gate** (done): `python3 cli.py quality` → `QUALITY PASS`
   (gofmt, filesize/complexity, architecture, root/invariants, route
   contract, `go vet`, unit + race tests, production build).
3. **Optional release hardener** (done-if-available): `python3 cli.py
   security-scan` (gosec) as an additional release check.
4. **Deploy:** no data migration required. In-flight colliding ingests simply
   begin observing a clean owner receipt + a self-describing conflict receipt on
   the next deploy; stored data was never structurally altered.
5. **Rollback:** single-commit revert is safe; the only behavioral delta is the
   *content* of the rejected-collider receipt and the now-clean owner receipt.

---

## 7. Testable acceptance mapping

New tests live in `internal/service/idempotency_conflict_test.go` and reuse the
existing `testService`, `testEvent`, `crmPrincipal`, `testCtx` fixtures.

| Acceptance criterion | Where enforced | Assertion |
|---------------------|----------------|-----------|
| **AC1** — returned receipt for collider is a conflict | `TestIdempotencyConflictKeepsOwnerReceiptClean` (both `hotcold=false` and `hotcold=true`) | `errors.Is(err, domain.ErrConflict)`; `receipt.Conflict == true`; `receipt.ErrorCode == "idempotency_key_conflict"`. |
| **AC2** — owner receipt untouched in snapshot | same test, after the collision | `svc.GetReceipt("tenant-a", "", A.EventID)` → `Conflict==false`, `ErrorCode==""`, `ErrorMessage==""`. |
| **AC3** — collider receipt fresh + no owner mutation persisted | same test, via `assertOwnerReceiptClean` | returned receipt `EventID == B.EventID` (not A's) and not equal to the stored owner; persisted `data.Receipts[key]` (legacy) / `view.Ledger.Receipt(key)` (hot/cold) shows `Conflict==false`, `ErrorCode==""`, `ErrorMessage==""`. |
| **AC1–AC3 bonus** — archived owner | `TestIdempotencyConflictKeepsArchivedOwnerReceiptClean` (`testService(t, true)`, owner `StatusArchived`) | same AC1/AC2/AC3 checks after re-reading the archived owner. |
| **Regression guard (R4)** | existing `TestIdempotencyKeyCannotBeReusedAcrossEvents` + `TestArchivedReceiptProtectsIdempotencyKey` | unchanged assertions still satisfy the `ErrConflict`/error-code contract. |

Test run result (targeted):
```
--- PASS: TestIdempotencyConflictKeepsOwnerReceiptClean (hotcold=false)
--- PASS: TestIdempotencyConflictKeepsOwnerReceiptClean (hotcold=true)
--- PASS: TestIdempotencyConflictKeepsArchivedOwnerReceiptClean
--- PASS: TestIdempotencyKeyCannotBeReusedAcrossEvents
--- PASS: TestArchivedReceiptProtectsIdempotencyKey
```

---

## 8. Verification

- `python3 cli.py quality` → **QUALITY PASS** (full suite incl. `-race`,
  `go vet`, route/OpenAPI/AsyncAPI/Proto contract checks, filesize &
  complexity thresholds, dependency direction).
- New AC1–AC3 tests pass for both `testService(t, false)` (legacy) and
  `testService(t, true)` (hot/cold), plus the archived-owner bonus.
- Out of scope (per requirements §7) explicitly **not** changed: quota
  ordering/charging, `waitFor=archived` downgrade, idempotency uniqueness
  semantics, and `GetReceipt` audit-logging/read path beyond observing the
  now-clean owner receipt.
