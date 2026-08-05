-- PostgreSQL control-plane state snapshot. The reference audit-api stores
-- its whole control-plane snapshot in this single row when started with
-- AUDIT_POSTGRES_DSN; the version column gives replicas an optimistic lock
-- against lost updates (see internal/store postgresBackend).
CREATE TABLE IF NOT EXISTS audit_state_snapshot (
    id integer PRIMARY KEY CHECK (id = 1),
    snapshot jsonb NOT NULL,
    version bigint NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT now()
);
