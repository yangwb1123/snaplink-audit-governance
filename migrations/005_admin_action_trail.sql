-- Append-only read self-audit trail. Read-path facts (audit.event.read,
-- audit.event.export, export.download_rejected, export.blocked on the
-- download leg) are INSERTed here instead of rewriting the single-row
-- audit_state_snapshot, so reads never force a full-row UPDATE with a
-- version bump. The snapshot keeps only mutation-path facts, capped by
-- MaxAdminActions (drop-oldest).
--
-- seq is the compaction watermark: compaction deletes rows with
-- seq < (the seq of the cap-th newest row), so a concurrent replica's
-- INSERT (allocated a newer BIGSERIAL seq) is never deleted.
CREATE TABLE IF NOT EXISTS admin_action_trail (
    seq         BIGSERIAL PRIMARY KEY,
    id          TEXT NOT NULL,
    tenant_id   TEXT NOT NULL,
    actor       TEXT NOT NULL,
    action      TEXT NOT NULL,
    target_type TEXT NOT NULL DEFAULT '',
    target_id   TEXT NOT NULL DEFAULT '',
    detail      TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS admin_action_trail_tenant_created
    ON admin_action_trail (tenant_id, created_at DESC, seq DESC);
