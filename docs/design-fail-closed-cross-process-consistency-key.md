# Design — Fail-Closed Cross-Process Consistency Key for Signer + Archive

**Module:** `internal/runtimeconfig`
**Direction:** Add a fail-closed cross-process consistency key for signer + archive
**Status:** Implemented (evidence verified; see ADR-0010 and release notes)

---

## 0. Evidence verification (untrusted claims → verified)

I read every cited file and symbol. The substance of every evidence claim (E1–E7)
is **confirmed**, but several line numbers are approximate and **two technical
claims inside FR-1 are inaccurate and must be corrected before implementation**.
Corrections are flagged inline below.

| ID | Claim | Verification |
|----|-------|--------------|
| E1 | Package doc, lines 1–6, cites shared purpose | ✅ Verbatim present in `runtimeconfig.go`. |
| E2 | `wireExternal` calls `external.Signer()` / `external.Archive()` and logs `signer=`/`archive=` only (API `main.go` ~272–302) | ✅ Confirmed. Error path returns `fmt.Errorf("signer: %w", …)` / `fmt.Errorf("archive: %w", …)` — reusable for fail-closed key. |
| E3 | Worker `runCheckConfig` + `newArchiveStore` (line 45) → `external.Archive()` | ✅ Confirmed. |
| E4 | API `runCheckConfigWithMTLS` logs `signer`/`archive`/`transport_s3`/`transport_vault`/`transport_grpc`, never compares across processes (line 476) | ✅ Confirmed. |
| E5 | `service.New` defaults `cfg.Signer = hmacSigner{secret: cfg.SigningSecret}`; `Signer` interface exposes `Algorithm() string`; `hmacSigner.Algorithm()` returns `"HMAC-SHA256(dev-compatible)"` | ✅ Confirmed (`Signer()` at start of `New`; interface has `Algorithm()`; string exact). Line numbers differ from the cited 83–87/108. |
| E6 | `SigningArchive` carries `VaultTransitKey`, `S3Bucket`, `ArchiveDir` | ✅ Confirmed (plus more fields). |
| E7 | Existing tests `TestSignerSelection`, `TestArchiveSelection`, `TestArchiveRequiresPositiveRetentionDays`, `TestEnvNameConstants`, `TestArchiveResolvesS3TransportUseSSL` exist, same package | ✅ Confirmed. Helpers `fullS3`/`fullVault` already exist — reuse them. |

### Correction 1 — FR-1 step-3 predicate is NOT identical to `Archive()`

The requirement states `s3Configured` is "identical predicate to `Archive()`
(lines ~160–171)". The actual `Archive()` uses an **OR** to compute
`configured` and then an AND-check:

```go
// runtimeconfig.go Archive()
configured := s.S3Endpoint != "" || s.S3Bucket != "" || s.S3AccessKey != "" || s.S3SecretKey != ""
if configured && (s.S3Endpoint == "" || s.S3Bucket == "" || s.S3AccessKey == "" || s.S3SecretKey == "") {
    return nil, fmt.Errorf("s3 archive requires endpoint, bucket, access key and secret key together")
}
if s.S3Endpoint == "" {            // <-- effective S3-vs-file branch
    return &archive.FileStore{Dir: s.ArchiveDir}, nil
}
```

The **AND** form in FR-1 (`… != "" && … != "" && … != "" && … != ""`) would
produce a file identity for a partial S3 configuration. That is not fail-closed
and is not the same branch as `Archive()`. **Implementation correction:** the
method now mirrors `Archive()`'s OR-then-partial-error check and then branches on
`S3Endpoint == ""`; a direct caller therefore receives the same partial-S3
error instead of a misleading file key. Both forms converge on valid
configurations, but the exact `Archive()` branch is the safer contract.

### Correction 2 — vault `Algorithm()` already embeds the transit *key name*

`security.VaultTransitSigner.Algorithm()` returns `"vault-transit:" + v.key`
(e.g. `"vault-transit:audit-checkpoints"`). Therefore the transit key **is**
part of the consistency key via the `algorithm` component (satisfying I3), and
a different `VaultTransitKey` changes the key. This must be stated explicitly:
the key embeds the transit **key name** (an identity already logged today as
`signer=vault-transit key=audit-checkpoints`), **not** the `VaultToken` (secret).
I4 (non-secret) holds.

### Design note — canonical HMAC token vs runtime `Algorithm()`

FR-1 fixes the HMAC-default algorithm token as `"HMAC-SHA256"`, but the runtime
`hmacSigner.Algorithm()` returns `"HMAC-SHA256(dev-compatible)"`. The key uses
the **canonical** token on purpose (stable, free of dev-compat prose). Consequence:
the consistency key's HMAC token (`HMAC-SHA256`) differs from both the runtime
`Algorithm()` string and the `check_config=ok` `signer=hmac-sha256` field. That
is acceptable — the key is compared *across processes*, not against
`Signer.Algorithm()`. Document this so operators do not treat the token mismatch
as a bug.

---

## 1. API changes

### FR-1 — `SigningArchive.ConsistencyKey() (string, error)`  (new method)

Signature (concrete):

```go
// ConsistencyKey derives a single deterministic, fail-closed key from the
// resolved signer algorithm and the archive destination identity, so the
// audit-api and audit-governance-worker produce a byte-identical value from
// the same deployment configuration and a different value when the scheme or
// destination diverges. No I/O: it resolves the signer via Signer() (which
// validates without network) and derives the archive identity from the same
// selection branch Archive() uses. The key contains only non-secret identity
// (algorithm name; s3:<bucket>/file:<dir>) — never signing/encryption secret,
// Vault token, or S3 credentials (I4). An invalid signing configuration fails
// closed with an error and no key (I2).
func (s SigningArchive) ConsistencyKey() (string, error) {
	signer, err := s.Signer()
	if err != nil {
		return "", fmt.Errorf("consistency key: signer: %w", err)
	}
	var algorithm string
	if signer == nil { // HMAC default: Signer() returns nil,nil iff VaultAddr==""
		algorithm = "HMAC-SHA256"
	} else {
		algorithm = signer.Algorithm() // vault: "vault-transit:<keyname>"
	}
	configured := s.S3Endpoint != "" || s.S3Bucket != "" || s.S3AccessKey != "" || s.S3SecretKey != ""
	if configured && (s.S3Endpoint == "" || s.S3Bucket == "" || s.S3AccessKey == "" || s.S3SecretKey == "") {
		return "", fmt.Errorf("consistency key: archive: s3 archive requires endpoint, bucket, access key and secret key together")
	}
	archiveID := "file:" + s.ArchiveDir
	if s.S3Endpoint != "" {
		archiveID = "s3:" + s.S3Bucket
	}
	return fmt.Sprintf("%s|%s", algorithm, archiveID), nil
}
```

Notes:
- `Signer()` only **parses** (via `resolveVaultTransport`) and constructs a
  signer object; it performs **no network call** → `ConsistencyKey` is safe to
  call from `-check-config` and at boot (C1 satisfied).
- `runtimeconfig` already imports `fmt` and `service`; no new import edge is
  introduced (C2 satisfied).

### FR-2 — emit `consistency_key=<value>` in both binaries

**Audit-api startup** — `cmd/audit-api/main.go`, in `wireExternal`
(lines 272–302), after the existing `signer=`/`archive=` logs and before
`return nil`:

```go
if key, keyErr := external.ConsistencyKey(); keyErr != nil {
	return fmt.Errorf("consistency_key: %w", keyErr)
} else {
	logger.Printf("consistency_key=%s", key)
}
```

**Audit-api `-check-config`** — `cmd/audit-api/main.go`, `runCheckConfigWithMTLS`
(line 476). Compute the key **before** the final `Printf`; fail-closed on error:

```go
consistencyKey, keyErr := external.ConsistencyKey()
if keyErr != nil {
	logger.Printf("consistency_key: %v", keyErr)
	return 1
}
logger.Printf("check_config=ok signing_secret_length=%d encryption_key_length=%d jwt_secret_length=%d signer=%s archive=%s transport_s3=%s transport_vault=%s transport_grpc=%s consistency_key=%s", len(svc.Config.SigningSecret), len(svc.Config.EncryptionKey), len(authenticator.JWTSecret), signerName, archiveName, s3Transport, vaultTransport, grpcTransport, consistencyKey)
```

**Worker startup** — `cmd/audit-governance-worker/main.go` (lines 119/127),
after the `signer=`/`archive=s3` logs:

```go
if ckey, ckErr := external.ConsistencyKey(); ckErr != nil {
	logger.Fatalf("consistency_key: %v", ckErr)
} else {
	logger.Printf("consistency_key=%s", ckey)
}
```

**Worker `-check-config`** — `cmd/audit-governance-worker/main.go`, `runCheckConfig`
(line 395). Append the **same trailing field** as the API line:

```go
consistencyKey, keyErr := external.ConsistencyKey()
if keyErr != nil {
	logger.Printf("consistency_key: %v", keyErr)
	return 1
}
logger.Printf("check_config=ok signing_secret_length=%d encryption_key_length=%d signer=%s archive=%s transport_s3=%s transport_vault=%s transport_grpc=%s consistency_key=%s", len(svc.Config.SigningSecret), len(svc.Config.EncryptionKey), signerName, archiveName, s3Transport, vaultTransport, "disabled", consistencyKey)
```

Constraint **C4**: `consistency_key` is the **last** field on both `check_config=ok`
lines, appended after `transport_grpc`. The pre-existing field set/order is
unchanged, so the API↔worker line comparison (C6) stays valid.

### FR-3 — cross-process deployment assertion (fail-closed)

`checks/consistency_key.py` and the `cli.py consistency-check` command run the
**built** `audit-api` and `audit-governance-worker` binaries with their pure
`-consistency-key` mode against one copied deployment environment. The mode
performs no state-store open, authentication preflight, gRPC validation, S3
client construction, archive readiness probe, or network call. The check fails
closed when a binary is missing, exits non-zero, emits no key, emits ambiguous
keys, or emits different keys. It prints both values on a mismatch.

The command is intentionally separate from `quality`: deployment automation
runs `python3 cli.py build` followed by `python3 cli.py consistency-check` (or
`python3 cli.py predeploy`). The in-repository CI workflow performs that same
sequence with an explicitly development-only local fallback; production jobs
must pass the merged deployment environment and retain the normal full
`-check-config`/readiness gates as a separate check.

### FR-4 — unit tests (append to `internal/runtimeconfig/runtimeconfig_test.go`)

```go
func TestConsistencyKeyStableForIdenticalConfig(t *testing.T) {
	// (a) HMAC-default shape: VaultAddr=="", ArchiveDir set.
	h1 := SigningArchive{SigningSecret: "secret", ArchiveDir: t.TempDir()}
	h2 := h1
	k1, err := h1.ConsistencyKey(); if err != nil { t.Fatal(err) }
	k2, err := h2.ConsistencyKey(); if err != nil { t.Fatal(err) }
	if k1 != k2 { t.Fatalf("hmac key not stable: %q vs %q", k1, k2) }
	if k1 != "HMAC-SHA256|file:"+h1.ArchiveDir { t.Fatalf("hmac key=%q", k1) }
	// stability across repeated calls
	if k3, _ := h1.ConsistencyKey(); k3 != k1 { t.Fatalf("hmac key not idempotent") }

	// (b) Vault+S3 shape: full vault + full s3 (non-default secrets).
	v := fullVault(t, "https://vault.example.com:8200", false)
	a := fullS3(t, "https://s3.example.com", true)
	base := SigningArchive{
		SigningSecret: v.SigningSecret, VaultAddr: v.VaultAddr, VaultToken: v.VaultToken,
		VaultTransitKey: v.VaultTransitKey,
		S3Endpoint: a.S3Endpoint, S3Bucket: a.S3Bucket, S3AccessKey: a.S3AccessKey,
		S3SecretKey: a.S3SecretKey, EncryptionKey: a.EncryptionKey, ArchiveRetentionDays: 365,
	}
	b := base
	vk1, err := base.ConsistencyKey(); if err != nil { t.Fatal(err) }
	vk2, err := b.ConsistencyKey();   if err != nil { t.Fatal(err) }
	if vk1 != vk2 { t.Fatalf("vault key not stable: %q vs %q", vk1, vk2) }
	if vk1 != "vault-transit:audit-checkpoints|s3:worm" { t.Fatalf("vault key=%q", vk1) }
}

func TestConsistencyKeyChangesWhenTransitKeyChanges(t *testing.T) {
	v := fullVault(t, "https://vault.example.com:8200", false)
	a := fullS3(t, "https://s3.example.com", true)
	base := func(key string) SigningArchive {
		return SigningArchive{
			SigningSecret: v.SigningSecret, VaultAddr: v.VaultAddr, VaultToken: v.VaultToken,
			VaultTransitKey: key,
			S3Endpoint: a.S3Endpoint, S3Bucket: a.S3Bucket, S3AccessKey: a.S3AccessKey,
			S3SecretKey: a.S3SecretKey, EncryptionKey: a.EncryptionKey, ArchiveRetentionDays: 365,
		}
	}
	k1, err := base("audit-checkpoints").ConsistencyKey(); if err != nil { t.Fatal(err) }
	k2, err := base("audit-checkpoints-2").ConsistencyKey(); if err != nil { t.Fatal(err) }
	if k1 == k2 { t.Fatalf("transit key change must change key: %q", k1) }

	// control: changing S3 bucket changes the key
	withBucket := base("audit-checkpoints"); withBucket.S3Bucket = "worm-2"
	kb, err := withBucket.ConsistencyKey(); if err != nil { t.Fatal(err) }
	if kb == k1 { t.Fatalf("bucket change must change key: %q", k1) }

	// control: changing ArchiveDir (file path) changes the key
	dir := base("audit-checkpoints"); dir.S3Endpoint, dir.S3Bucket, dir.S3AccessKey, dir.S3SecretKey = "", "", "", ""
	dir.ArchiveDir = t.TempDir()
	kd, err := dir.ConsistencyKey(); if err != nil { t.Fatal(err) }
	if kd == k1 { t.Fatalf("archive dir change must change key: %q", k1) }

	// different scheme families yield different keys
	if k1 == kd { t.Fatalf("vault vs hmac keys must differ") }
}
```

Run with `go test ./internal/runtimeconfig/...` and via the `python cli.py quality`
gate (which runs `cmd_test`/`cmd_race` and the python checks).

---

## 2. Compatibility constraints

| ID | Constraint | How satisfied |
|----|-----------|---------------|
| C1 | `ConsistencyKey` pure / non-I/O | Calls only `Signer()` (URL-parse + object construct, no network) and a local predicate. Safe at boot and in `-check-config`. |
| C2 | No new dependency edge | Method lives on `SigningArchive` in `runtimeconfig`, reuses existing `fmt`/`service` imports. `runtimeconfig` must still not import `cmd/...` (architecture gate stays green). |
| C3 | No secret material in key or logs | Key = algorithm name + `s3:<bucket>` / `file:<dir>`. The vault component embeds the transit **key name** only (already logged today), never `VaultToken`/`S3SecretKey`/`S3AccessKey`/`SigningSecret`/`EncryptionKey`. |
| C4 | `consistency_key` is the last field on both `check_config=ok` lines | Both lines append it after `transport_grpc`; pre-existing fields/order preserved (C6). |
| BC1 | Additive log field | Appending a trailing `key=value` is non-breaking for `key=value` parsers (the existing convention). **Risk:** a parser that asserts a fixed field *count* would break — CI already keys on the `check_config=ok` prefix + field names, so this is low risk; call it out in the changelog. |
| BC2 | No behavior change to `Signer()`/`Archive()`/transport logic | Out of scope per §8; the method adds no branch to those functions. |

---

## 3. Failure modes

| # | Mode | Trigger | Behavior | Mitigation |
|---|------|---------|----------|------------|
| F1 | **Helm/env drift between Deployments** | API and worker get different `AUDIT_VAULT_TRANSIT_KEY` / `AUDIT_S3_BUCKET` | Keys differ → FR-3 (`consistency-check`) fails in CI/pre-deploy, non-zero. **This is the primary catch.** | Run `cli.py consistency-check` against the merged values before traffic. |
| F2 | **Partial external config in one process** | e.g. worker missing `AUDIT_VAULT_TOKEN` | That process's `-check-config` already fails at `Signer()`/`Archive()` (existing fail-closed) → no `consistency_key` emitted → FR-3 reports a missing key on one side → fails. | None needed; existing guard covers it. |
| F3 | **Archive identity omits endpoint host** *(limitation)* | Same `S3Bucket` name on two different `S3Endpoint`s (cross-account/region) | Key is identical (`s3:<bucket>`) despite pointing at different destinations. | By-design per FR-1 (`archiveID := "s3:" + s.S3Bucket`). If stronger guarantee is needed later, include `S3Endpoint` host — out of scope here. |
| F4 | **Retention-days drift not detected** *(limitation)* | `AUDIT_ARCHIVE_RETENTION_DAYS` differs between processes | Key is identical (retention is a per-object Put parameter, not a destination identity). | Out of scope; both processes read the same Helm value. Document that the key pins *destination identity*, not every Put parameter. |
| F5 | **ConsistencyKey on partially-configured S3 archive** | Direct call with e.g. only `S3Endpoint` set | The method mirrors `Archive()` and returns the partial-S3 error; no file key is produced. | Direct unit test plus both normal preflight paths remain fail-closed. |
| F6 | **Both processes wrong-but-identical** | e.g. Vault env missing in *both* → both fall back to HMAC | Keys match → gate passes. The key is a *consistency* oracle, not a correctness oracle. | Separate concern (config validity) already enforced by secret/transport gates. Document. |
| F7 | **Canonical HMAC token ≠ runtime `Algorithm()`** | HMAC default | Key shows `HMAC-SHA256`; runtime signer reports `HMAC-SHA256(dev-compatible)`; `check_config` `signer=` shows `hmac-sha256`. | Documented design choice (§0 Correction 2). |

---

## Review disposition and implementation evidence

- **F-A / architect F-1:** resolved with `-consistency-key` in both binaries.
  The mode resolves secrets and pure endpoint syntax only; it does not run the
  worker's S3 readiness probe or the API's gRPC preflight. The checker invokes
  the built binaries and has mocked comparison tests.
- **F2 / security F-2:** resolved by `checks/consistency_key.py`, which uses
  `ROOT/bin/*`, copies one environment to both processes, handles missing or
  failed binaries, rejects ambiguous output, and is wired through
  `cli.py consistency-check`.
- **F-B (first design correction):** resolved by mirroring `Archive()`'s
  OR-configured/partial-error/`S3Endpoint == ""` branch in `ConsistencyKey`;
  partial S3 input cannot produce a file key.
- **F-C (second design correction):** resolved and documented: Vault's
  `vault-transit:<keyname>` algorithm includes the Transit key name, never the
  Vault token. The canonical HMAC token is intentionally `HMAC-SHA256`.
- **DevOps F3 (no in-repository CI wiring):** resolved for this repository by
  `.github/workflows/quality.yml` and the `predeploy`/`consistency-check`
  commands. The workflow uses a development-only fallback solely to exercise
  wiring; a release job must inject the merged deployment environment.
- **DevOps F0 (no production IaC):** explicitly rejected as outside this
  direction, not silently marked complete. This repository remains a runnable
  reference implementation; `deploy/*.verify.*` is not a production manifest,
  and neither this feature nor its CI workflow makes a production-readiness
  claim. A production IaC/secret-delivery direction must supply its own values
  and invoke the documented pre-deploy command.
- **Security F-3 (endpoint/transport/HMAC-secret drift):** explicitly rejected
  as a widening of the fixed v1 contract. FR-1/C3/I4 require a non-secret,
  name-level key with the exact `algorithm|s3:<bucket>` or
  `algorithm|file:<dir>` shape; encoding a secret fingerprint would violate
  that boundary. Endpoint and transport correctness remains fail-closed in
  `Signer`/`Archive`/`Transport` and the full `-check-config` path. The
  limitation is tested and documented rather than presented as backend
  identity assurance.
- **F7/F8:** the canonical HMAC token and the additive trailing field are
  documented in the release notes and runbook; the API-only
  `jwt_secret_length` difference remains intentional and the consistency
  checker reads only the dedicated key output.

## 4. Migration steps

1. **Add `ConsistencyKey()`** to `internal/runtimeconfig/runtimeconfig.go`
   (after `Archive()`), implementing §1 FR-1 with the exact `Archive()` branch
   and partial-S3 rejection (Correction 1). No new imports.
2. **Append the two unit tests** (`TestConsistencyKeyStableForIdenticalConfig`,
   `TestConsistencyKeyChangesWhenTransitKeyChanges`) to
   `internal/runtimeconfig/runtimeconfig_test.go`, reusing `fullS3`/`fullVault`.
3. `go test ./internal/runtimeconfig/...` → green; existing suite
   (`TestSignerSelection`, …) unchanged.
4. **Emit `consistency_key`** in all four sites (API `wireExternal` + API
   `runCheckConfigWithMTLS`; worker startup + worker `runCheckConfig`) per §1
   FR-2, at the tail (C4).
5. `gofmt` + `go vet ./...` + `go build ./...`.
6. **Add `checks/consistency_key.py`** and a `cli.py` `consistency-check`
   command using the dedicated network-free `-consistency-key` mode; document
   it as a pre-deploy/CI stage run **after** build.
7. `python3 cli.py quality` → QUALITY PASS (architecture/filesize/complexity
   gates remain green; no new dependency edge).
8. **Docs/changelog:** note the new `consistency_key` field (last on
   `check_config=ok` and in boot logs), the canonical `HMAC-SHA256` token
   (F7), and the limitation that the key pins destination *identity* not
   endpoint host or retention (F3/F4). Update the deployment runbook to run
   `cli.py consistency-check`.

---

## 5. Testable acceptance mapping

| Acceptance | Requirement | Testable artifact | Pass condition |
|-----------|-------------|------------------|----------------|
| **AC-1** | `ConsistencyKey() (string, error)` deterministic from algorithm + archive identity | Code review + `go test` | Method exists; returns `A|B`; equals `Signer().Algorithm()` joined with `s3:<bucket>`/`file:<dir>`. I1–I4 hold. |
| **AC-2** | Both binaries emit `consistency_key` at `-check-config` and startup; byte-identical for identical env | `TestConsistencyKeyStableForIdenticalConfig` (I1) + manual `-check-config` of both binaries with same env | `grep consistency_key` on API and worker output; values equal; field is last. |
| **AC-3** | Cross-process check fails when keys differ | `checks/consistency_key.py` + `cli.py consistency-check` | Non-zero exit + both values printed when env diverges; zero when identical. **Add a unit test in `checks/test_*.py`** driving a synthetic mismatch. |
| **AC-4** | `TestConsistencyKeyStableForIdenticalConfig` + `TestConsistencyKeyChangesWhenTransitKeyChanges` pass | `go test ./internal/runtimeconfig/...` | Both green; included in `python3 cli.py quality`. |

| Invariant | Maps to | How verified |
|-----------|---------|--------------|
| I1 pure/deterministic | AC-2, AC-4 | `TestConsistencyKeyStableForIdenticalConfig` (equal values; repeated calls equal; no clock/nonce/hostname). |
| I2 fail-closed | AC-1 | Partial Vault config → `Signer()` errors → `ConsistencyKey` returns `("", err)`; startup/`-check-config` fail. |
| I3 scheme-sensitive | AC-4 | `TestConsistencyKeyChangesWhenTransitKeyChanges` (transit key, bucket, dir controls). |
| I4 non-secret | AC-1, C3 | Key string contains only algorithm name + `s3:<bucket>`/`file:<dir>`; asserted in tests; no secret pattern. |

**Gate wiring:** `python3 cli.py quality` must stay GREEN (covers C1/C2 via
architecture + filesize + `go vet`/`test`/`race`/`build`). The cross-process
assertion (AC-3) is run as a **separate** `consistency-check` CI/pre-deploy
stage after build, not inside `cmd_quality`, to avoid requiring a full
deployment env during the local unit gate.

**Pass condition (full):** AC-1–AC-4 all green, existing `runtimeconfig_test.go`
suite continues unchanged, `python3 cli.py quality` → QUALITY PASS, and
`consistency-check` passes against the merged deployment values.
