# Stop the background ledgered-outbox flusher from head-of-line blocking the synchronous post-commit publish

The post-commit ledgered publish gate is now scoped to a single pending event
instead of the whole process, and the ingest request path never waits for it.

**Behavior change (no migration, no configuration change).** Before this change
`Service.ledgeredPublishMu` was one process-wide mutex taken by both the
synchronous post-commit publisher and `FlushLedgeredOutbox`, and the flusher
held it for a *whole round*: one snapshot read plus up to 100 sequential
publishes (5s each) plus up to 100 exclusive whole-snapshot acknowledgements.
`Service.Ingest` then waited on that mutex with no bound of its own — and the
HTTP/gRPC ingest context is deliberately detached from client cancellation
(`context.WithoutCancel`), so a client disconnect did not shorten the wait
either — even though the ledger commit had already answered the caller. A slow
or stopped broker therefore delayed `POST /api/v1/events`, every member of
`POST /api/v1/events:batch` (the handler ingests members in a loop), and the
gRPC `Write`/`WriteBatch` paths by an amount bounded by nothing in the request
path — above the API's own 30s `WriteTimeout`.

Now:

* the gate is a per-`store.EventKey` refcounted table (`internal/service/ledgered_gate.go`),
  acquired with `TryLock` and therefore never waited on. At most one publisher
  per pending event — the property the old comment documented — and unrelated
  events never block each other;
* a contended caller **defers**: it leaves the durable intent in
  `Snapshot.LedgeredOutbox` (written by the ingest transaction, not by the
  publish step) and returns. Nothing is lost; the publisher that owns the key,
  or the next flush round, delivers it. The ingest answer is unchanged;
* `FlushLedgeredOutbox` takes a round-level `TryLock` and holds one key gate at
  a time, so a long round no longer delays an ingest and two overlapping rounds
  cannot publish the same key from stale snapshots. A round that skips every key
  is a no-op (returns `0, nil`), not a failure;
* the flusher's staleness revalidation is hoisted to at most **one** extra store
  read per round, taken lazily and failing **open** — `Store.Read` materialises
  the whole control snapshot, so a per-key check would have turned one read per
  tick into up to 100 under exactly the load that makes revalidation necessary.

**New metric.** `GET /metrics` now renders
`audit_ledgered_publish_deferred_total`, unlabelled and monotonic, next to
`audit_ledgered_intent_repaired_total`, read through a single atomic load so a
scrape never waits on the publication gate or the store. With a consistent
durable outbox the request path does not contend, so this counter should stay at
`0`; a non-zero value is direct evidence that the one-publisher-per-pending-event
invariant was breached (two commits mapping onto one outbox key, or a lost
intent re-created while a flush was publishing it) and that at-least-once
delivery absorbed it. No Prometheus alert or SLO row was added: the existing
`audit_http_request_duration_seconds` histogram is the instrument that observes
this fix.

**Known limitation (unchanged, out of scope).** The flush loop still retries on
a fixed 1s tick with no backoff or jitter, so N replicas retry a dead broker in
lockstep. The retry cadence belongs to `cmd/audit-api/ledgered.go`, not to this
gate.

**Verification.** `internal/service/ledgered_gate_test.go` and
`internal/httpapi/ledgered_metrics_test.go` cover the bounded wait, the
same-key invariant (one in-flight publish per pending event), durable-row
survival across a deferral and its drain, the round mutex, one-read
revalidation with its fail-open branch, the refcount table not leaking, the
unlabelled `/metrics` line, and the unchanged HTTP ingest contract for both the
single and batch routes. Each concurrency test fails in seconds against the
pre-change tree. `python3 cli.py check` and `python3 cli.py quality` are green
(`gofmt`, `filesize`, `complexity`, `architecture`, `route-contract`,
`openapi-contract`, `sensitive-logging`, `go vet`, `go test`, `go test -race`,
build); `gosec` is not installed in this environment, so
`python3 cli.py security-scan` was not run.
