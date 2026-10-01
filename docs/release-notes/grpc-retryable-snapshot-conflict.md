# Classify an exhausted snapshot conflict as retryable-transient on gRPC ingest

`store.ErrSnapshotConflict` — what `Store.Update` returns after the
optimistic-lock retry loop is exhausted, with nothing committed — now maps to
gRPC `codes.Unavailable` instead of `codes.Internal` on `Write`, `WriteStream`
and `WriteBatch`. The message is the fixed constant
`state snapshot changed concurrently; retry the request`; backend detail
(state path, errno, pgx/Vault text) is logged server-side under
`grpc snapshot conflict:` instead of `grpc internal error:` and never crosses
the boundary.

Behaviour change for producers: an at-least-once client that honours the
standard gRPC retry policy now retries this condition (ingest is idempotent,
so a retry resolves to the original receipt), where `codes.Internal` told it to
give up and drop the event. `WriteBatchOutcome.rejection_code` for an affected
member changes from `internal` to the already-published `unavailable` token;
consumers must stop treating these members as internal faults. No
persistence, retry, backoff or SLO number changed, and the HTTP surface keeps
its existing `503` + `snapshot_conflict` mapping, which this change mirrors.

Validation: `internal/grpcapi` covers the mapping, the message redaction, the
log taxonomy, the rejection-code vocabulary and the end-to-end `WriteBatch`
committed-prefix / rejected-member / `NOT_ATTEMPTED`-suffix property;
`internal/httpapi` pins the unchanged 503 mapping.