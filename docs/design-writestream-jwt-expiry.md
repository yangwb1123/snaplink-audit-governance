# Design: JWT expiry for long-lived `WriteStream` sessions

## Verification of the supplied evidence

The cited symbols were checked against the repository rather than assumed:

| Claim | Result | Consequence |
| --- | --- | --- |
| `internal/grpcapi/server.go` authenticates `WriteStream` once and reuses the claims | **Verified** | The loop must reauthenticate before each message and must not retain the initial principal for ingest. |
| `internal/auth/auth.go` requires `exp` and rejects `now >= int64(exp) + int64(ClockSkew.Seconds())` | **Verified** | The stream monitor must consume this verified boundary, including truncation and exact-boundary rejection. |
| `Claims` contains server-derived client/tenant/permission data | **Verified** | Envelope fields remain data only; they cannot replace claims. |
| `Service.Ingest` resolves tenant/source access from the principal and performs tenant consistency checks | **Verified** | No new per-message tenant lookup or persistence change is needed. |
| `EventEnvelope` has no authentication-claim fields and `WriteStream` is bidirectional | **Verified** | The wire contract remains unchanged. |

The defect is therefore specifically the missing expiry wake-up while blocked in
`Recv` and the reuse of the initial claims after a receive. The current handler
also deliberately uses `context.WithoutCancel` for an accepted ingest; that
behavior is retained.

## Concrete design and API changes

1. `auth.Authenticator` gains an optional `Now func() time.Time` clock seam.
   A nil value retains the production `time.Now` behavior. JWT `exp`/`nbf`
   validation uses this clock.
2. Verified JWT `auth.Claims` records the effective expiry internally and
   exposes it through `EffectiveExpiry() (time.Time, bool)`. The value is
   produced only after signature and claim verification and already includes
   the authenticator's exact integer-second clock-skew calculation. Development
   tokens return `false` and retain their existing non-expiring behavior.
3. `Server.WriteStream` authenticates initially as today, then calls the same
   `authenticate(..., "audit:event:write")` path after each successful
   `Recv`, before examining, converting, skipping, or ingesting the envelope.
   The freshly returned claims create the principal and tenant argument for
   that message.
4. After initial JWT authentication, the handler monitors the verified
   effective expiry while blocked in `Recv`. A buffered receive goroutine and
   timer are used because the gRPC `Recv` API has no per-call deadline. Timer
   expiry returns the fixed redacted `codes.Unauthenticated` status. A receive
   result racing the timer is checked against the authenticator clock and is
   revalidated before processing.
5. There is no protobuf or client token-refresh change. `Write` and
   `WriteBatch` remain unchanged.

The expiry timer never parses authorization metadata or an unverified JWT
claim. It uses only `Claims.EffectiveExpiry()` from the initial verified
authentication and the same authenticator clock used by revalidation.

## Compatibility and invariants

- Wire messages, receipt shape/order, skip behavior for over-cap envelopes,
  source binding, tenant resolution, and durable outbox behavior are unchanged.
- `EffectiveExpiry` is an additive method. `Authenticator.Now` is an additive
  test/clock seam on an internal exported struct; new and changed literals
  should remain keyed (unkeyed literals would be source-fragile when fields are
  added). A nil `Now` preserves the production wall clock.
- A message that passes reauthentication before the effective boundary follows
  the existing conversion and ingest path. The detached
  `context.WithoutCancel` is deliberately retained after acceptance.
- A message received at or after the effective boundary cannot reach
  `Service.Ingest`; a client-side `Send` success is not an acceptance signal.
- Missing/invalid/expired credentials remain `Unauthenticated`; valid claims
  without `audit:event:write` remain `PermissionDenied`. Authentication and
  timer errors expose no verifier detail or expiry timestamp.
- Normal client EOF before expiry remains a successful stream close. Development
  tokens have no expiry wake-up, but are still revalidated before each message.
- Positive and negative `ClockSkew` values, including the exact boundary, use
  the existing `int64` semantics. No token-refresh protocol or data migration
  is introduced.

## Failure modes and handling

| Condition | Client result | Persistence | Handling |
| --- | --- | --- | --- |
| Initial metadata missing/ambiguous or JWT invalid | `Unauthenticated` | none | Existing initial gate; details logged only server-side. |
| Initial valid token lacks write permission | `PermissionDenied` | none | Existing permission gate. |
| Revalidation fails before a message is processed | `Unauthenticated` or `PermissionDenied` | none for that message | Terminate before conversion/ingest. |
| Effective expiry while idle in `Recv` | `Unauthenticated` | none | Timer wins and closes the stream. |
| Token expires while an already-authorized ingest is running | Existing ingest result/receipt semantics | May commit | Accepted work remains detached and durable as before. |
| Client closes normally before expiry | successful EOF | prior accepted work unchanged | Existing close behavior. |
| Transport/client cancellation while receiving | transport cancellation status | no new message is ingested | Existing transport behavior; expiry status takes precedence when the expiry wake-up wins. |

## Migration and rollout

1. Deploy the server implementation; no schema, protobuf, or outbox migration is
   required.
2. Keep existing clients unchanged. Clients should treat
   `Unauthenticated` from `WriteStream.Recv` as a reconnect/re-authenticate
   signal and reconcile accepted event IDs from receipts, not `Send` return
   values. A stream cannot refresh its token in place.
3. Roll out with existing `ClockSkew` configuration. Verify nominally expired
   tokens within positive skew and reject at `exp + skew`; keep zero skew for
   strict deployments.
4. Observe unauthenticated stream termination and reconnect rates. Rollback is
   a binary rollback only; there is no persistent migration or compatibility
   shim to remove.

## Testable acceptance mapping

| Acceptance | Test evidence |
| --- | --- |
| Idle stream ends with `Unauthenticated` and no persistence | `TestGRPCWriteStreamJWTExpiryWhileIdle` blocks `Recv` and checks Events, Receipts, and LedgeredOutbox. |
| Prefix succeeds; post-expiry message is not accepted | `TestGRPCWriteStreamRejectsPostExpiryMessage` checks the prefix persistence witness and the rejected ID across all persistence maps. |
| Positive skew is honored and exact effective boundary rejects | `TestGRPCWriteStreamHonorsClockSkew` sends after nominal `exp`, then checks rejection at `exp + ClockSkew`; `internal/auth/clock_skew_test.go` retains unit boundary coverage. |
| Revalidation precedes event validation and ingest | The stream handler performs authentication immediately after receive and before nil-envelope, conversion, skip, or `Service.Ingest` branches; rejected-ID persistence assertions cover the observable result. |
| Identity and permission remain server-derived | `TestGRPCWriteStreamClaimsAreNotExtendedByEventFields` submits payload/actor fields naming another tenant/client or permission, asserts the persisted event/receipt stays under the verified scope, and verifies a no-write token is denied before conversion. Existing `assertGRPCSourceBinding`, `TestGRPCRejectsKeyFramingTenantClaimUnauthenticated`, and service tenant/client tests cover the source and tenant portions. |
| Wire and durable-commit compatibility | Existing `Write`, `WriteBatch`, receipt, stream-skip, EOF, and detached-ingest tests remain unchanged and run under the repository quality gate. |
