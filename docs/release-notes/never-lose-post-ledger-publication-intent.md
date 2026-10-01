# Never lose the post-ledger publication intent on tenant ingest

Ingest on the tenant (hot/cold) path now creates the durable publication intent
for `audit.events.ledgered.v1` in the **same** store operation that commits the
event and its cold receipt, instead of in a second store operation afterwards.
A crash, SIGKILL or failed control write between the two commits could previously
leave a committed ledger record and a WORM archive object with no intent at all,
which made the event permanently absent from the projection feed — no DLQ record,
no replay path, no metric.

Behaviour changes:

- `Store.UpdateControl` is no longer called by the tenant ingest path. A control
  write is one durability boundary per ingest instead of two, so ingest no
  longer performs a second control-document write after the ledger commit.
- When a control write still fails after the hot document and the cold ledger are
  durable (the residual window both backends leave open), `Ingest` reports the
  failure and an **idempotent retry re-creates the missing intent** from the
  stored event. The retry returns the stored receipt byte-for-byte unchanged and
  does not publish inline; the outbox flush loop publishes as before.
- The retry repair only fires for an ingest that is still `ledgered` with no
  pending intent row — an ingest that completed (receipt already `indexed` or
  `archived`), an event-id content conflict, and an archived event with no hot
  payload are never re-queued. Each event key is repaired at most once per
  process, so the counter is a one-shot signal rather than a retry amplifier.
- New metric on `GET /metrics`: `audit_ledgered_intent_repaired_total`, unlabelled
  and monotonic. Read it as "a tenant commit died between its durable cold append
  and its control-plane write". It must stay `0`; a rising value alongside a flat
  `audit_ingest_duplicates_total` is how you learn a lost intent is still owed
  (a repair only happens when a client retries).

Compatibility: no snapshot field, no database migration, no wire-contract change.
The published payload is still the unchanged `domain.Event` envelope, and the
repaired publication carries the **stored** stream/sequence/hash, so a
client-supplied chain can never reach the feed.

Known limitation: the repair needs a client retry. A fire-and-forget producer
that never retries leaves the intent owed — hence the metric. A boot-time
reconciler scanning receipts is deliberately deferred.
