-- Operational delivery metadata, deliberately outside the shared control
-- snapshot and immutable audit ledger. Apply before enabling ledgered Kafka
-- publication on either the v1 or v2 PostgreSQL layout. A missing row is
-- unconfirmed, never evidence that the event was not previously delivered.
CREATE TABLE IF NOT EXISTS audit_publication_confirmation (
    tenant_id    TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    stream_id    TEXT NOT NULL CHECK (stream_id <> ''),
    sequence     BIGINT NOT NULL CHECK (sequence > 0),
    event_hash   TEXT NOT NULL CHECK (event_hash <> ''),
    confirmed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, event_id)
);
