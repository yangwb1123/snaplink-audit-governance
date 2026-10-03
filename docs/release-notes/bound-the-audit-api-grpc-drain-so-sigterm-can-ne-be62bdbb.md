# Bound the audit-api gRPC drain so SIGTERM can never hang on an idle WriteStream

Direction: *Bound the audit-api gRPC drain so SIGTERM can never hang on an idle
`WriteStream`.*

## What changed

**`SIGTERM` can no longer hang `audit-api` on an idle gRPC ingest stream.** The
shutdown path ended in a bare `grpcServer.GracefulStop()`, which has no deadline
and no force fallback. An authenticated-but-idle `Ingest/WriteStream` parks in
`receiveWriteStreamRequest` — for a development token the effective expiry is
zero and `Recv` is called with no timer at all, and for a long-lived signed token
it parks on a ~70-year expiry timer — so one open stream turned `SIGTERM` into an
indefinite hang. The process was only ended by an external `SIGKILL`, and the
line after the drain (`_ = st.Close()`) was never reached, so the file
backend's exclusive flock on `<state>.lock` and the PostgreSQL pool were never
released by the process. Nothing in the log distinguished a clean drain from an
indefinite block, because the force path did not exist.

**The exit sequence is now bounded and ordered.** `cmd/audit-api/main.go` gained
`shutdownTimeout` (10s, the pre-existing HTTP drain budget under a name),
`shutdownDeps`, `shutdown`, `grpcDrainBound`, `tracerShutdowner`, `shutdownTracer`
and `drainGRPC`:

1. HTTP connection drain under `shutdownTimeout`;
2. tracer flush under **its own** fresh `shutdownTimeout` context;
3. `healthServer.Shutdown()` (probes observe `NOT_SERVING`, FR-2.3);
4. `drainGRPC` — `GracefulStop()` in a goroutine, `grpcServer.Stop()` at the
   bound;
5. `_ = st.Close()`, always reached.

Worst-case budget is `2·shutdownTimeout + 2·op` (the PostgreSQL `db.Close()`
waits for in-flight statements), i.e. **40s at shipped defaults** and **< 80s** at
the startup-enforced maximum; the file backend is `2·shutdownTimeout + op`. Both
are finite, so releasing the state lock no longer requires an operator `SIGKILL`.
The documented budget, the two log values and the rollback path are in
`docs/DEPLOY_RUNBOOK.md` 「SIGTERM 下的有界退出序列（gRPC drain）」.

**The drain window is derived, never invented.** `grpcDrainBound(st)` is exactly
one store operation (`st.OperationTimeout()`), falling back to
`shutdownTimeout` only for the nil-store configuration-only tests. A detached
in-flight ingest commit therefore has had the same bound available on the request
path before the forced stop arrives, and
`runtimeconfig.ValidateStoreOperationTimeoutBelow` — already enforced in
`openStore` **and** `prepareServer` — keeps it below the HTTP socket budget.
`AUDIT_STORE_OPERATION_TIMEOUT` stays the single knob; **no new environment
variable, flag, route, metric or contract change**, and `api/openapi`,
`api/asyncapi` and `api/proto` are untouched.

**One bounded operator line per shutdown.** `grpc_drain=drained` or
`grpc_drain=force_stopped` — a closed two-value vocabulary, no tenant, client,
event, route or URL label. No gRPC listener configured means no line at all
(there was no drain to report). The tracer no longer discards its shutdown error
against the shared HTTP drain context (`_ = tracer.Shutdown(ctx)` would have
silently degraded to a `ctx.Err()` no-op once `server.Shutdown` spent its
budget); a failure logs `tracing_shutdown=failed` with no error text, because an
OTLP error can embed the collector endpoint URL that startup already logs.

**`drainGRPC` returns on the bound unconditionally.** `grpc.Server.Stop()` has
no documented prompt-return contract, and grpc-go parks it behind a concurrent
`GracefulStop()` whenever a handler ignores its cancelled context, so the forced
`Stop()` runs off the caller's goroutine too. Without that, the helper could
park inside `Stop` and reintroduce exactly the hang it exists to remove.

## Why it matters

A stuck `SIGTERM` means an indefinite rollout, a state lock that only the kernel
reclaims at process death, an unclosed PostgreSQL pool, and no operator signal
distinguishing "drained cleanly" from "wedged".

Forced stop is safe because the ingest commits are already detached from the RPC
context (`context.WithoutCancel` in `Write`, `WriteBatch` and `WriteStream`),
`fileBackend.Save` is atomic, and an interrupted PostgreSQL statement lands in
the documented `store.ErrOperationDeadline` "result unknown, retry idempotently"
class: a client that sees `Unavailable`/`Canceled` retries by `event_id` and
converges on exactly one receipt.

## How it was verified

- `TestSIGTERMDrainsIdleWriteStreamAndReleasesStateLock` (`cmd/audit-api/
  sigterm_drain_process_test.go`) starts the **real binary** with an open idle
  `WriteStream` on loopback, waits for `grpc_listen=`, proves the state lock is
  held, sends `SIGTERM` and asserts the process exits cleanly inside the drain
  bound, prints exactly one `grpc_drain=force_stopped`, leaves the lock
  releasable, and keeps the pre-drain event's single durable receipt. It is
  table-driven over **both** park shapes (dev token with a zero effective expiry,
  and a far-future HS256 token) plus a paired control sub-test with no stream in
  flight, which must log `grpc_drain=drained` — the non-vacuity oracle.
  Re-running it against an unbounded drain makes both idle-stream cells hang and
  the control pass.
- `TestForcedStopDuringIngestCommitLeavesExactlyOneReceipt` holds the store write
  lock so the ingest commit is genuinely in flight, forces the stop mid-commit,
  and pins the durable invariant plus idempotent convergence to one receipt, with
  a negative control that commits the same sequence without a forced stop.
- `TestGRPCDrainBoundDerivedFromStoreOperationTimeout` pins the derivation
  (shipped default = `store.DefaultOperationTimeout`, an operator override, the
  nil-store fallback) and asserts
  `runtimeconfig.ValidateStoreOperationBelow(grpcDrainBound(st), httpWriteTimeout)`
  is satisfied in every cell.
- `TestDrainEmitsExactlyOneOperatorLine`, `TestShutdownTracerUsesItsOwnContext`,
  `TestShutdownTracerNilIsNoOp` (including the typed-nil `*telemetry.Tracer`
  production actually passes), `TestTracerShutdownFailureIsNotSilentlyDiscarded`,
  `TestMainGoDoesNotDiscardTracerShutdown` and `TestMainGoShutdownLiteralOrdering`
  pin the operator contract and the two static regressions.
- `internal/grpcapi/wiring_test.go` (`TestProductionWiringStatic`) is
  byte-identical and still passes: `healthServer.Shutdown()` still precedes
  `grpcServer.GracefulStop()` in `cmd/audit-api/main.go`. `internal/httpapi` and
  `internal/grpcapi` production sources are unchanged.

## No behaviour change on the happy path

With nothing in flight the sequence is still health drain → graceful stop →
`st.Close()`, the HTTP drain budget is byte-identical (10s), and the new line
reads `grpc_drain=drained`.