-- Outbox relay delivery bookkeeping. Business transactions insert into
-- audit_outbox (see internal/outbox SDK); the audit-outbox-relay consumes
-- pending rows, posts them to the audit API and records the outcome here.
ALTER TABLE audit_outbox
    ADD COLUMN IF NOT EXISTS delivered_at timestamptz,
    ADD COLUMN IF NOT EXISTS delivered_event_id text,
    ADD COLUMN IF NOT EXISTS api_status text;

COMMENT ON COLUMN audit_outbox.delivered_at IS
    'When the audit API accepted the event (status delivered).';
COMMENT ON COLUMN audit_outbox.delivered_event_id IS
    'event_id reported by the audit API receipt, identical to the payload event_id.';
COMMENT ON COLUMN audit_outbox.api_status IS
    'API receipt status observed at delivery time (accepted/ledgered/...).';

CREATE INDEX IF NOT EXISTS idx_audit_outbox_failed
    ON audit_outbox (next_attempt_at)
    WHERE status = 'failed';
