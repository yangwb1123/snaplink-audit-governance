# Design — Vault Transit signer: key-name binding (REQ-2), context propagation (REQ-3), comment accuracy (REQ-4)

Source of truth for verification: `docs/auto/analyses/internal-security-2cd39427.json`
(direction "Vault Transit signer sends X-Vault-Token over plaintext HTTP and cannot be
cancelled or bound to the expected key") and the requirements spec produced from it
(`docs/auto/runs/vault-transit-signer-sends-x-vault-token-over-pl-45ac8d79/artifacts/requirements-10762e10/requirements.md`).
Every claim below was re-verified symbol-by-symbol against the working tree at commit `9724bee`.

---

## 0. Verification outcome (untrusted claims vs. tree)

| Evidence claim | Verified against tree | Verdict |
|---|---|---|
| Transport hardening (REQ-TLS-4/5) already landed; `http://` non-loopback fails closed at `Signer()`/`-check-config`/startup | `resolveVaultTransport()` (`internal/runtimeconfig/runtimeconfig.go:157`) fails closed; enforced in `Signer()` (`:66`), `Transport()` (`:194`), `wireExternal` (`cmd/audit-api/main.go:169`); release note `docs/release-notes.md:42-70` (2026-08-13); tests `TestSignerValidatesVaultAddr` (`runtimeconfig_test.go:247`), `TestRunCheckConfigVaultFailFast` (`main_test.go:71`), `TestCheckConfigTransportLine` (`:111`), `TestStartupVaultHTTPFatal` (`:255`) — all **passing** on this tree | **Confirmed** — analysis claim "no scheme enforcement" is stale |
| `call()` uses `context.Background()`; shutdown cannot cancel in-flight Vault requests | `vaultsigner.go:69` still `http.NewRequestWithContext(context.Background(), …)`; `service.Signer` (`internal/service/service.go:60-67`) carries no context | **Confirmed — live gap** (REQ-3) |
| `Sign()` accepts any non-empty signature without key binding | `vaultsigner.go:38-46`: `decoded["signature"].(string)`, empty-string check only, returned verbatim | **Confirmed — live gap** (REQ-2) |
| `vault-addr` flag accepts any scheme from env | `cmd/audit-api/main.go:59` (env default, no parse-time scheme check); mitigated downstream by `Signer()`/`Transport()` | **Confirmed** — flag-level true, downstream fail-closed |
| Doc comment "Uses plain HTTP…" | `vaultsigner.go:17-18` still present, now misleading | **Confirmed — stale** (REQ-4) |
| REQ-3 consequence: "server shutdown (main.go:131-137) cancels in-flight Vault requests" | **Correction:** `http.Server.Shutdown` (Go 1.26 stdlib `net/http/server.go:3150-3186`) closes listeners/idle conns and waits within its context budget; it does **not** cancel request contexts. Request contexts are cancelled on client-connection close, HTTP/2 stream reset, or `ServeHTTP` return. Genuine cancellation sources after REQ-3: client disconnect, gRPC client cancel, worker signal, test-driven cancel. The shutdown-path benefit is indirect (handlers stay bounded by the Vault client's 10 s timeout; main's 10 s Shutdown budget bounds the drain). AC-3 as written does **not** depend on Shutdown semantics and stays fully testable | **Partial** — motivation overstated, AC-3 unaffected |
| Line citations (analysis §4) | `sanitizeAddr` at `runtimeconfig.go:229` (cited :213), `loopbackHost` at `:250` (cited :222), shutdown block `main.go:139-145` (cited :131-137) | Symbols verified; minor line drift |

The four acceptance checks are preserved verbatim and remain testable (mapping in §6).

---

## 1. Scope

**In scope**

- **REQ-2** — `VaultTransitSigner.Sign` binds the returned signature to the configured
  transit key name; rejects foreign-key and malformed signatures at signing time.
- **REQ-3** — caller-supplied `context.Context` through the Vault request path
  (entry point → `call` → `http.NewRequestWithContext`); no `context.Background()`
  in the signer's request path; context threaded from HTTP/gRPC/worker entry points
  into the four sign/verify call sites.
- **REQ-4** — reword the stale constructor comment (`vaultsigner.go:17-18`).
- **REQ-1** — regression lock: transport fail-closed behavior stays green (AC-1 tests
  are the gate; no code change).

**Non-goals (unchanged from evidence)**

- `Verify` semantics: Vault validates server-side against the configured key; cross-key
  signatures already verify `valid=false`. No change.
- No scheme enforcement inside `NewVaultTransitSigner` (would break the documented
  loopback opt-in and `TestVaultTransitUnreachable`, which constructs the signer
  directly with `http://127.0.0.1:1`). Policy stays centralized in `SigningArchive`.
- No key rotation/versioning, no S3-leg changes, no changes to `hmacSigner` output
  (it ignores the context — deterministic local computation).
- No new dependencies; the existing `net/http` client is retained.

---

## 2. API changes

### 2.1 `internal/security/vaultsigner.go`

```go
// Sign(ctx, data) — signature change:
func (v *VaultTransitSigner) Sign(ctx context.Context, data []byte) (string, error)
func (v *VaultTransitSigner) Verify(ctx context.Context, data []byte, signature string) (bool, error)
func (v *VaultTransitSigner) call(ctx context.Context, operation string, body map[string]any, extract func(map[string]any) (string, error)) (string, error)
```

`NewVaultTransitSigner(addr, token, key string) *VaultTransitSigner`, `Algorithm()`,
struct fields, and the 10 s client timeout are **unchanged**.

**REQ-2 — key binding** inside `Sign`'s extract closure, after the existing
empty-string check:

```go
// checkSignatureKey verifies the returned Vault Transit signature is bound to the
// configured key. Vault signatures have the shape vault:v<ver>:<key-name>:<payload>.
// Key names are not secrets; the payload is never echoed.
func (v *VaultTransitSigner) checkSignatureKey(signature string) error {
    parts := strings.Split(signature, ":")
    if len(parts) < 4 || parts[0] != "vault" || !vaultVersionSegment(parts[1]) || parts[2] == "" {
        return fmt.Errorf("vault transit sign: signature has malformed prefix: expected vault:v<version>:<key>:<payload>")
    }
    if observed := parts[2]; observed != v.key {
        return fmt.Errorf("vault transit sign: signature bound to transit key %q, want %q: refusing to record evidence under a different key", observed, v.key)
    }
    return nil
}
```

- `vaultVersionSegment(s)` is `len(s) > 1 && s[0] == 'v' && s[1:]` all digits
  (Vault versions are `v1`, `v2`, …; the version is informational only — key-name
  binding is the requirement, per evidence AC-2).
- On success the original signature string is returned **verbatim** (no
  reconstruction), so stored evidence keeps the exact `vault:v1:<key>:…` shape and
  `Verify` round-trips (AC-4).
- Error text names both keys and never echoes the signature payload (repo F4
  discipline: credentials/data are never echoed; key names are explicitly
  non-secret).

**REQ-3 — context**: `call` passes `ctx` to `http.NewRequestWithContext`; the
`context.Background()` call at `vaultsigner.go:69` is deleted. No other
`context.Background()` remains in the request path (entry point → `call` →
round trip). The `X-Vault-Token` header, URL construction, status handling, and
1 MiB body cap are unchanged.

**REQ-4 — comment** (non-functional): replace "Uses plain HTTP against the Vault
API to avoid a heavyweight SDK dependency." with text stating the signer performs
no transport security itself and plaintext `http` is reachable only via the
explicit loopback opt-in (`AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK`) enforced upstream
by `SigningArchive.resolveVaultTransport`.

### 2.2 `internal/service/service.go` — `Signer` interface

```go
// Signer creates and verifies checkpoint signatures. Implementations must be
// deterministic per input so Verify can re-check archived manifests. The context
// cancels in-flight external (Vault) requests; local signers ignore it.
type Signer interface {
    Sign(ctx context.Context, data []byte) (string, error)
    Verify(ctx context.Context, data []byte, signature string) (bool, error)
    Algorithm() string
}
```

- `hmacSigner.Sign(ctx, data)` / `hmacSigner.Verify(ctx, data, sig)`: ctx unused
  (deterministic local HMAC); output bytes identical.

### 2.3 Service methods — context threading

```go
func (s *Service) Ingest(ctx context.Context, tenantID string, principal domain.IngestPrincipal, event domain.Event, waitFor string) (domain.EventReceipt, error)                       // service.go:403
func (s *Service) CreateAggregateCheckpoint(ctx context.Context, tenantID string) error                                                                                          // governance.go:274
func (s *Service) VerifyIntegrity(ctx context.Context, tenantID, actor, streamID string) (IntegrityResult, error)                                                                // governance.go:336
func (s *Service) SealPendingSegments(ctx context.Context, tenantID string) error                                                                                                // governance.go:485
func (s *Service) sealSegment(ctx context.Context, stream store.StreamState, now time.Time) (domain.Segment, domain.Checkpoint, error)                                            // service.go:1096
```

Sign/verify call sites become context-carrying:

| Call site | Change |
|---|---|
| `governance.go:298` (`CreateAggregateCheckpoint`) | `Sign(ctx, []byte(root))` |
| `governance.go:392` (`VerifyIntegrity`) | `Verify(ctx, []byte(expected), aggregate.Signature)` |
| `governance.go:464` (`VerifyIntegrity`) | `Verify(ctx, []byte(manifestHash), segment.Signature)` |
| `service.go:1101` (`sealSegment`) | `Sign(ctx, []byte(manifestHash))` |
| `service.go:545` (`Ingest` closure) | `sealSegment(ctx, stream, now)` |
| `governance.go:493` (`SealPendingSegments` closure) | `sealSegment(ctx, stream, now)` |

`ctx` flows into the `Store.Update`/`UpdateChecked` closures by capture (the store
operations themselves remain local/atomic and are not cancellable — out of scope).

### 2.4 Entry points

| Caller | Change |
|---|---|
| `internal/httpapi/server.go:265, 309` (`Ingest`) | `s.Service.Ingest(r.Context(), …)` |
| `internal/httpapi/server.go:629` (`verifyIntegrity`) | `s.Service.VerifyIntegrity(r.Context(), tenantID, claims.Subject, request.StreamID)` |
| `internal/grpcapi/server.go:50, 72, 102` (`Ingest`) | pass the RPC `ctx` (unary handlers) / `stream.Context()` (`WriteStream`) |
| `cmd/audit-governance-worker/main.go` | replace `signal.Notify` with `ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)`; `runEvaluatePass(ctx, logger, svc)`; loop select on `<-ctx.Done()`; `SealPendingSegments(ctx, …)`, `CreateAggregateCheckpoint(ctx, …)` |

The worker's immediate first `evaluate()` before listener registration is kept
(existing behavior; the context is live from creation).

---

## 3. Compatibility constraints

1. **`service.Signer` is module-internal.** Both implementors are in-repo
   (`hmacSigner`, `VaultTransitSigner`); Go's `internal/` rule prevents any external
   module from implementing the interface. The platform contract ("core code only
   depends on ports, not sibling project class names") is unaffected — no sibling
   project imports this module's internals. A breaking interface change is therefore
   compile-safe.
2. **Binary lockstep.** `audit-api` and `audit-governance-worker` are built from the
   same tree and must ship together (existing release-note discipline). REQ-2 takes
   effect in both binaries' signing paths simultaneously; there is no mixed-version
   window.
3. **Evidence/archive format unchanged.** Signatures are stored verbatim; `Verify`
   is untouched; existing archives remain verifiable (AC-4). REQ-2 adds a read-time
   check on the *response*, not on stored data.
4. **HTTP routes, OpenAPI, CLI flags, env vars, check-config output: unchanged.**
   The route-contract / contract-fields / asyncapi checks in `python3 cli.py quality`
   are unaffected (handler internals only).
5. **REQ-2 is zero-behavior-change for correctly configured deployments.** Vault's
   Transit engine always embeds the URL-path key in the returned signature
   (`vault:v1:<key>:<payload>`); the repository's own fixture
   (`vaultsigner_test.go:29`) proves the positive path. Only a misconfigured or
   compromised Vault (signature under a different key) or a format-changing proxy
   triggers the new error.
6. **HMAC default signer**: output bytes and behavior identical; context ignored.
7. **No new dependencies**, no new env vars/flags, no storage migration.

---

## 4. Failure modes

| Failure | Behavior after change | Fail-closed? |
|---|---|---|
| Vault returns signature bound to a different key (misconfig/compromise) | `Sign` returns error naming configured + observed key; `Store.Update`/`UpdateChecked` closure fails → no checkpoint/segment recorded, no archive write (archive happens only after a successful update). HTTP: `statusForError` default → 500 | Yes — evidence is never recorded under an unbound key; the mismatch surfaces at signing time instead of a later `VerifyIntegrity` run |
| Malformed/truncated prefix (`garbage`, `vault:v1:`, missing payload) | Same fail-closed `Sign` error | Yes |
| Non-standard-but-future Vault version (`vault:v2:<key>:…`) | Accepted (version segment is informational; binding is on key name). Deliberately lenient — key rotation/versioning is a separate direction | n/a |
| Caller context cancelled before/during sign | `client.Do` returns the context error promptly (`errors.Is(err, context.Canceled)` holds through the signer's `%w` wrap); Ingest fails closed (500). `VerifyIntegrity` follows its existing semantics: `verifyErr != nil` → `result.Valid=false` + a "root/signature mismatch" entry (no endpoint failure) | Yes |
| Client disconnects mid-request | net/http cancels `r.Context()` → Vault round trip aborted promptly instead of blocking on the 10 s client timeout | n/a |
| Worker SIGTERM mid-pass | `signal.NotifyContext` cancels the pass context; sign calls fail fast per tenant (logged `checkpoint_error=`); the loop exits on the next select; `st.Flush` persists committed state (store updates are atomic per tenant) | n/a |
| `http.Server.Shutdown` with in-flight Vault call | **Documented limitation (verified):** `Shutdown` does not cancel request contexts; the drain is bounded by the Vault client's 10 s timeout per handler and main's 10 s Shutdown budget, after which process exit kills stragglers. Prompt abort on shutdown would require `Server.Close()`-style semantics or a cancellable `BaseContext` — **out of scope**, noted as follow-up. AC-3 does not depend on this path | n/a |
| Transport enforcement regresses (REQ-1) | AC-1 keep-green tests (`TestSignerValidatesVaultAddr`, `TestRunCheckConfigVaultFailFast`, `TestCheckConfigTransportLine`, `TestStartupVaultHTTPFatal`) fail the quality gate | n/a |
| Signature payload leaked in error text | Prevented by design: errors echo key names only, never the payload (F4 discipline) | n/a |

---

## 5. Migration steps

1. **Baseline gate.** Run `python3 cli.py quality` and record green — this is the
   AC-1/AC-4 keep-green proof before any change.
2. **REQ-4 (comment-only, independent).** Reword `vaultsigner.go:17-18`. No
   functional impact; no tests.
3. **REQ-2 (independent of REQ-3).**
   a. Add `checkSignatureKey` + `vaultVersionSegment`; wire into `Sign`'s extract
      closure.
   b. Add AC-2 tests (see §6) to `internal/security/vaultsigner_test.go`.
   c. `go test ./internal/security/...`.
4. **REQ-3 (mechanical, one commit).**
   a. Change `service.Signer` interface; update `hmacSigner`, `VaultTransitSigner`,
      and the test wrappers `countingSigner`/`erroringSigner`
      (`internal/service/aggregate_checkpoint_test.go:16-31`).
   b. Thread ctx through the four service methods + `sealSegment` and the six
      sign/verify call sites (§2.3).
   c. Update entry points (§2.4): httpapi (3 sites), grpcapi (3 sites), worker
      (`signal.NotifyContext` + `runEvaluatePass`).
   d. Mechanical test updates: ~165 single-line call sites across the 14 service
      test files (verified: zero multiline `Ingest(`/`VerifyIntegrity(` calls) +
      7 `runEvaluatePass(` calls in `cmd/audit-governance-worker/main_test.go`.
      Use a per-file `testCtx := context.Background()` or a package helper.
   e. Add AC-3 tests (§6) to `vaultsigner_test.go`.
5. **Full gate.** `python3 cli.py quality` (gofmt, size/complexity thresholds,
   package dependency direction, root invariants, OpenAPI route contract,
   `go vet`, unit + race tests, production binary builds). Optional:
   `python3 cli.py security-scan`.
6. **Deploy.** Ship both binaries in the same batch; run `-check-config` per
   environment first (assert `transport_vault=tls`); then roll. No storage
   migration, no env/flag changes.

---

## 6. Testable acceptance mapping

Each acceptance check from the direction is preserved verbatim (bolded) with its
test implementation.

**AC-1 — "Test that NewVaultTransitSigner with an http:// address fails in non-dev
mode or emits a hard startup warning (e.g., check-config exit nonzero)."**
Already implemented; the direction's "or" clause is satisfied by the
check-config/startup branch the tree chose. **Keep green** (REQ-1):

| Test | Asserts |
|---|---|
| `TestSignerValidatesVaultAddr` (`internal/runtimeconfig/runtimeconfig_test.go:247`) | table: `http://` non-loopback with/without opt-in → error containing `non-loopback`, signer nil; `http://127.0.0.1:8200` without opt-in → error naming `AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK`; loopback+opt-in → signer built, transport `http`; `https://` → transport `tls`; schemeless/whitespace/malformed/`ftp://` → error |
| `TestRunCheckConfigVaultFailFast` (`cmd/audit-api/main_test.go:71`) | `-check-config` exits 1, never prints `check_config=ok`, names the variable; `https://` exits 0 with `transport_vault=tls` |
| `TestCheckConfigTransportLine` (`main_test.go:111`) | ok-line carries `transport_vault=tls|http|local` |
| `TestStartupVaultHTTPFatal` (`main_test.go:255`) | startup with `http://` non-loopback aborts (non-zero exit, actionable text, no `listen=` line) |

**AC-2 — "Test that a Vault response whose signature carries a transit key name
different from the configured key is rejected by Sign."** New tests (httptest, no
network):

| Test (new, `vaultsigner_test.go`) | Setup → assertion |
|---|---|
| `TestVaultTransitSignRejectsForeignKey` | server returns `{"data":{"signature":"vault:v1:other-key:fake-signature"}}`; signer key `test-key` → `Sign` errors, error text contains `test-key` (and the observed key), no signature returned |
| `TestVaultTransitSignRejectsMalformed` | server returns `garbage`, `vault:v1:`, `vault:x:test-key:x`, `vault:v1:test-key` (no payload) → `Sign` errors (no longer accepts any non-empty string) |
| Positive control (existing `TestVaultTransitSignAndVerify`) | `vault:v1:test-key:fake-signature` → `Sign` succeeds; fixture key name stays `test-key` |

**AC-3 — "Test that Verify/Sign propagate a caller-supplied context so shutdown
cancels in-flight requests (no context.Background in request path)."** New tests:

| Test (new, `vaultsigner_test.go`) | Assertion |
|---|---|
| `TestVaultTransitSignVerifyCancelledContext` | pre-cancelled ctx on both `Sign` and `Verify` → returns promptly, `errors.Is(err, context.Canceled)` |
| `TestVaultTransitInFlightCancellation` | httptest handler blocks on `<-r.Context().Done()` then writes; start the call, cancel mid-flight → call returns promptly with the context error; handler observes `r.Context().Err() == context.Canceled` |
| Static invariant | no `context.Background()` in the Vault request path of `vaultsigner.go` (code-review invariant; optionally a comment-guarded grep in the quality gate) |

**AC-4 — "Existing httptest-based sign/verify/403 tests (vaultsigner_test.go)
remain green."** Keep green:

| Test | After change |
|---|---|
| `TestVaultTransitSignAndVerify` (`:42`) | positive sign/verify round-trip, base64 input check — unchanged behavior, ctx arg added |
| `TestVaultTransitUnauthorized` (`:85`) | wrong token → 403 — unchanged, ctx arg added |
| `TestVaultTransitUnreachable` (`:96`) | `http://127.0.0.1:1` with `context.Background()` → connection refused still fails (ctx arg added; the explicit-loopback opt-in still permits direct construction) |

---

## 7. Traceability

| Requirement | Boundary | Acceptance |
|---|---|---|
| REQ-1 (transport fail-closed regression) | `resolveVaultTransport` → `Signer()`/`Transport()` → check-config + startup (both binaries) | AC-1 (keep green) |
| REQ-2 (key-name binding) | `security.VaultTransitSigner.Sign` + `checkSignatureKey` | AC-2 (new tests) |
| REQ-3 (context propagation) | `security.VaultTransitSigner.call` + `service.Signer` + 4 service methods + `sealSegment` + httpapi/grpcapi/worker entry points | AC-3 (new tests) |
| REQ-4 (comment accuracy) | `vaultsigner.go:17-18` | non-functional |
| Preservation | `vaultsigner_test.go` httptest suites | AC-4 (keep green) |

## 8. Engineering gate

Per `AGENTS.md`, the implementation must pass `python3 cli.py quality` before being
reported complete (gofmt, file/function size and complexity thresholds, package
dependency direction, repository root policy/invariants, OpenAPI route contract,
`go vet`, unit tests, race tests, production binary builds); optional
`python3 cli.py security-scan` if gosec is available.
