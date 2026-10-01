# Design: evidence-backed `WriteStream` capacity decision gate

## Status and decision

This is a measurement and decision-gate design. It does **not** select a
capacity, add an admission limiter, add an overload status contract, or add a
runtime setting. Earlier review identified process-wide stream capacity as a
risk but did not establish an approved value, production resource budget, or
traffic profile. This phase records observations without inferring a limit or
default; runtime enforcement requires a separate evidence-backed decision.

The only implementation in this phase is an opt-in, build-tagged real gRPC
load test with a test-only observational interceptor. The observations in
`docs/BENCHMARKS.md` are workload results, not a safe limit, default, SLO, or
client contract.

## Verification of the supplied evidence

| Claim | Verification | Design consequence |
| --- | --- | --- |
| `Server.WriteStream` has no process-wide active-handler accounting | **Verified** in `internal/grpcapi/server.go`; the handler authenticates, receives, ingests, sends, and loops without admission state. | Measure before deciding; do not infer a bound from the handler code. |
| Expiry-aware receive creates a goroutine around `Recv` | **Verified** in `receiveWriteStreamRequest`; non-expiring credentials call `Recv` directly. | Count expiry-aware receive workers separately and verify cleanup. |
| Production gRPC options have no stream-capacity setting | **Verified** in `cmd/audit-api/main.go`; options cover keepalive, receive size, recovery, and credentials. | Keep transport settings separate from RPC admission. |
| Keepalive is transport-level only | **Verified** in `internal/grpcapi/keepalive.go`. | Do not use idle/keepalive values as capacity evidence. |
| Idle JWT expiry is an existing oracle | **Verified** by `TestGRPCWriteStreamJWTExpiryWhileIdle`; it expects `Unauthenticated` and no persisted event. | The load test must not alter expiry or persistence semantics. |
| Existing benchmarks contain no real `WriteStream` resource measurements | **Verified** in `docs/BENCHMARKS.md`. | Existing ingest/query numbers cannot become a handler limit. |
| An approved process-wide admission contract exists | **Not established:** no approved capacity, production resource ceiling, or production traffic profile was found. | Keep measurements observational; approve capacity and overload semantics in a separate decision before runtime enforcement. |

## Current-phase API and implementation changes

### Wire and public API

There are no protobuf, generated-code, RPC-shape, receipt, persistence, or
production configuration changes. Existing clients continue to use the same
bidirectional `WriteStream` method and the same authentication and expiry
rules.

The load test is `internal/grpcapi/writestream_capacity_load_test.go` and is
compiled only with the `writestream_load` test build tag. Its command-line
build tag is test tooling, not a server flag or runtime capacity setting.
The test-only stream interceptor counts handler lifetime and wraps `RecvMsg` to
count the expiry-aware receive call. `Server.Register` and `Server` itself are
unchanged; the wrapper has no admission method and cannot accept, reject, or
otherwise control a stream.

### Conditional follow-on API, only after capacity approval

If production-relevant measurements and a separate operational decision
approve process-wide admission, the gate should be implemented at the gRPC
stream-interceptor boundary, not at the beginning of `Server.WriteStream`:

```go
type WriteStreamAdmission interface {
    TryReserve() (release func(), admitted bool)
}

func WriteStreamAdmissionInterceptor(
    admission WriteStreamAdmission,
) grpc.StreamServerInterceptor
```

The interceptor would match only
`/snaplink.audit.v1.Ingest/WriteStream`, call `TryReserve` before invoking the
application handler, and return `codes.ResourceExhausted` immediately when no
reservation exists. Thus the rejected call enters neither `Server.WriteStream`
nor application `Recv`. The actual constructor/wiring and any selected
capacity remain intentionally unspecified until that approval.

The reservation must be a lifecycle lease, not a bare `defer` at handler entry:
its handler reference and any expiry-aware `Recv` worker reference are released
independently, and the gate releases only after both references are gone. This
prevents a timer-returned handler from admitting a replacement while its
transport receive goroutine is still unwinding.

## Compatibility constraints

* Preserve `Write`, `WriteBatch`, and `WriteStream` protobuf messages and
  generated APIs.
* Preserve initial and per-message authentication, effective JWT expiry
  (`exp + ClockSkew` with the existing integer-second boundary), idle expiry
  `Unauthenticated`, post-expiry rejection, source/tenant binding, receipt
  ordering, detached durable ingest, wire framing, and persistence layout.
* Keep transport receive-size rejection and the existing per-stream over-cap
  skip budget distinct from any future admission result. Both already use
  `ResourceExhausted` in specific, unrelated cases.
* Keep keepalive as a connection/transport policy, not an RPC-capacity signal.
* A future overload status would be additive for callers below the selected
  limit but a behavioral change for callers above it; clients would need
  bounded retry/backoff and must reconcile event IDs from receipts rather than
  treating `Send` success as durable acceptance.
* The future gate is process-local. A multi-process or multi-replica global
  limit would require a separately designed coordinator and is not implied by
  this decision gate.

## Measurement design

The build-tagged test starts a real TCP listener with the registered
`Ingest.WriteStream` service, production-like keepalive/receive-size/recovery
options, a JWT authenticator with a non-zero effective expiry, and two
independently created `grpc.ClientConn` values to that listener. It runs fixed
observational workloads at offered levels 4, 12, and 24. Each level mixes idle
expiry-aware streams, normal message-producing streams, and streams whose
receipt readers wait 700 ms before reading. These numbers are test workload
levels only.

The test records:

* offered, opened, and active streams, including per-connection stream mix;
* active handler and expiry-aware receive-worker peaks from the test-only
  observational interceptor;
* goroutine count, Go `HeapAlloc`, `/proc/self/status` RSS when available, and
  `/proc/self/fd` count when available;
* stream-open, message send, receipt, and terminal latency distributions in
  milliseconds, plus an explicit empty admission-latency observation because
  no admission path exists;
* status/error distributions and slow-reader behavior; and
* a controlled `service.Config.LedgeredPublisher` degradation during the
  highest workload, including injected condition, observed duration, errors,
  and post-fault successful publications.

The test has no performance assertion and cannot turn a measurement into an
approved capacity. Its process resource readings include the in-process test
clients, so production sizing requires a separately isolated server process
and a production-like environment.

Run it with:

```sh
go test -tags=writestream_load ./internal/grpcapi \
  -run '^TestGRPCWriteStreamCapacityLoad$' -count=1 -timeout=60s -v
```

## Failure modes and handling

| Condition | Current behavior/observation | Future handling after approval |
| --- | --- | --- |
| Too many long-lived idle streams | No admission; handlers, expiry workers, goroutines, memory, and descriptors are observed until transport/client cleanup. | Interceptor rejects the next stream immediately; no application handler/`Recv` entry. |
| Slow receipt readers | Client receipt latency and server handler/send pressure are recorded; no new status is invented. | Remains an application-flow-control effect, not a capacity release signal. Lease releases only at actual stream/worker cleanup. |
| JWT expiry while idle | `Unauthenticated`; no event persistence, pinned by the existing expiry test. | Same result; expiry releases the lifecycle lease after worker cleanup. |
| EOF or client cancellation | Existing successful EOF or transport cancellation semantics. | Lease release is asserted for both paths. |
| Authentication/permission failure | Existing `Unauthenticated`/`PermissionDenied`, before ingest. | Reservation is released even though no message is processed. |
| Recovered handler panic | Recovery interceptor returns redacted `Internal`. | Reservation release is in a panic-safe interceptor/lease defer. |
| Dependency degradation | The load test records publisher failures and recovery but does not assert durable outbox state; existing ingest/outbox tests remain the durability oracle. | No admission decision is derived from one fault result; any future capacity decision must include dependency-fault observations. |
| Resource saturation or process termination | Record the first failed operation, status distribution, and resource peak; do not label the last passing workload safe. | Require a separate decision record, production evidence, and contract tests before rollout. |

## Migration and rollout

### Measurement phase

1. Review the verification table and preserve the capacity-approval gate.
2. Run the tagged test in an isolated reference environment and in a
   production-relevant environment with at least two client connections.
3. Append raw/tabulated output and environment metadata to
   `docs/BENCHMARKS.md`; retain the reference-baseline and non-production-SLO
   disclaimers.
4. Record a separate operational decision to approve an evidence-backed
   capacity/overload contract or to keep the current behavior with mitigations.
   A benchmark result alone does not approve a capacity value.

No schema, token, protobuf, client, or deployment migration is needed for this
phase.

### Conditional admission phase

Only after separate capacity and overload-contract approval:

1. Add the interceptor/lease implementation and a selected deployment
   configuration through a separate reviewed change. Do not silently reuse a
   benchmark level as the selected value.
2. Add contract tests for immediate rejection, no handler/`Recv` entry, release
   on EOF/auth failure/cancellation/expiry/panic, and worker cleanup.
3. Canary the new status behavior with client retry/backoff and event-ID
   reconciliation enabled; observe handler/worker gauges and dependency/error
   distributions.
4. Roll out per process/replica, then document the chosen contract and
   rollback configuration. A rollback is a binary/config rollback; no data
   migration is expected.

## Testable acceptance mapping

| Acceptance | Evidence and test |
| --- | --- |
| Real listener and two independent connections | `TestGRPCWriteStreamCapacityLoad` uses `net.Listen` and two `grpc.NewClient` connections; the tagged command above is the reproducible invocation. |
| Candidate workload observations, not a default | The three report records contain offered/opened/active streams and explicitly set `admission_control_present=false`; the benchmark section says observations are not a limit. |
| Handler/worker/goroutine/memory/fd/latency/status coverage | The test-only interceptor and runtime/`/proc` samplers populate each `resources`, latency, and status field per level. |
| Normal, idle-expiry, and slow-reader coverage | The workload matrix and report record all three stream classes; JWTs carry a non-zero expiry. |
| Dependency degradation and recovery | The highest-level report records the injected publisher fault, observed duration, error count, and recovery successes. |
| Existing behavior unchanged | Existing gRPC regression suite, including `TestGRPCWriteStreamJWTExpiryWhileIdle`, remains in `go test ./...`; the load test does not alter wire or persistence code. |
| Runtime behavior unchanged in the measurement phase | No production admission interceptor, global limiter, overload contract, capacity config, or numeric default is added; the measurement interceptor exists only under the test build tag and is observational. |
| Conditional follow-on proof | Not enabled in this phase. After capacity approval, add the interceptor contract tests listed above and rerun `python3 cli.py check` and `python3 cli.py quality`. |
