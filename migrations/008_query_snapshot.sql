-- Ephemeral, tenant-scoped query membership; neither ledger nor evidence.
-- Authentication/scope MAC and exact event coordinates are carried in metadata.
CREATE TABLE IF NOT EXISTS audit_query_snapshot (
    tenant_id text NOT NULL,
    snapshot_id text NOT NULL,
    expires_at timestamptz NOT NULL,
    metadata jsonb NOT NULL,
    PRIMARY KEY (tenant_id, snapshot_id),
    CHECK (octet_length(metadata::text) <= 4194304)
);
CREATE INDEX IF NOT EXISTS audit_query_snapshot_expiry ON audit_query_snapshot (tenant_id, expires_at);
