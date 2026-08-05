CREATE TABLE IF NOT EXISTS tenants (
    id text PRIMARY KEY,
    name text NOT NULL,
    home_region text NOT NULL DEFAULT 'local',
    data_region text NOT NULL DEFAULT 'local',
    active boolean NOT NULL DEFAULT true,
    events_per_second integer NOT NULL DEFAULT 0,
    burst integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sources (
    tenant_id text NOT NULL REFERENCES tenants(id),
    id text NOT NULL,
    name text NOT NULL,
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS event_schemas (
    tenant_id text NOT NULL REFERENCES tenants(id),
    schema_id text NOT NULL,
    version integer NOT NULL,
    event_type text NOT NULL,
    required_fields jsonb NOT NULL DEFAULT '[]',
    allowed_fields jsonb NOT NULL DEFAULT '[]',
    encrypted_fields jsonb NOT NULL DEFAULT '[]',
    searchable_fields jsonb NOT NULL DEFAULT '[]',
    classification text NOT NULL,
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, schema_id, version)
);

CREATE TABLE IF NOT EXISTS retention_policies (
    tenant_id text PRIMARY KEY REFERENCES tenants(id),
    hot_days integer NOT NULL DEFAULT 30,
    warm_days integer NOT NULL DEFAULT 180,
    archive_days integer NOT NULL DEFAULT 365,
    retention_class text NOT NULL DEFAULT 'standard'
);

CREATE TABLE IF NOT EXISTS ledger_streams (
    tenant_id text NOT NULL,
    stream_id text NOT NULL,
    next_sequence bigint NOT NULL DEFAULT 1,
    head_hash text NOT NULL DEFAULT '',
    PRIMARY KEY (tenant_id, stream_id)
);

CREATE TABLE IF NOT EXISTS ledger_events (
    tenant_id text NOT NULL REFERENCES tenants(id),
    event_id text NOT NULL,
    idempotency_key text NOT NULL,
    event_type text NOT NULL,
    schema_id text NOT NULL,
    schema_version integer NOT NULL,
    occurred_at timestamptz NOT NULL,
    stream_id text NOT NULL,
    sequence bigint NOT NULL,
    prev_hash text NOT NULL DEFAULT '',
    event_hash text NOT NULL,
    canonical_event jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, event_id),
    UNIQUE (tenant_id, idempotency_key),
    UNIQUE (tenant_id, stream_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_ledger_events_tenant_occurred
    ON ledger_events (tenant_id, occurred_at, event_id);

CREATE INDEX IF NOT EXISTS idx_ledger_events_operation
    ON ledger_events (tenant_id, ((canonical_event->>'operation_id')))
    WHERE canonical_event ? 'operation_id';

CREATE TABLE IF NOT EXISTS event_receipts (
    tenant_id text NOT NULL,
    event_id text NOT NULL,
    idempotency_key text NOT NULL,
    status text NOT NULL,
    event_hash text NOT NULL,
    stream_id text NOT NULL,
    sequence bigint NOT NULL,
    accepted_at timestamptz NOT NULL,
    ledgered_at timestamptz,
    indexed_at timestamptz,
    archived_at timestamptz,
    canonical_event jsonb NOT NULL,
    PRIMARY KEY (tenant_id, event_id),
    UNIQUE (tenant_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS ledger_segments (
    tenant_id text NOT NULL,
    stream_id text NOT NULL,
    first_sequence bigint NOT NULL,
    last_sequence bigint NOT NULL,
    first_prev_hash text NOT NULL,
    last_hash text NOT NULL,
    event_count integer NOT NULL,
    merkle_root text NOT NULL,
    manifest_hash text NOT NULL,
    signature text NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, stream_id, first_sequence)
);

CREATE TABLE IF NOT EXISTS signed_checkpoints (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    stream_id text NOT NULL,
    sequence bigint NOT NULL,
    merkle_root text NOT NULL,
    signature text NOT NULL,
    algorithm text NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS legal_holds (
    id text PRIMARY KEY,
    tenant_id text NOT NULL REFERENCES tenants(id),
    name text NOT NULL,
    reason text NOT NULL,
    filter jsonb NOT NULL,
    created_by text NOT NULL,
    created_at timestamptz NOT NULL,
    released_at timestamptz,
    released_by text
);

CREATE TABLE IF NOT EXISTS export_jobs (
    id text PRIMARY KEY,
    tenant_id text NOT NULL REFERENCES tenants(id),
    requested_by text NOT NULL,
    query jsonb NOT NULL,
    status text NOT NULL,
    object_path text,
    digest text,
    event_count integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL,
    finished_at timestamptz,
    error text
);

CREATE TABLE IF NOT EXISTS audit_outbox (
    id bigserial PRIMARY KEY,
    event_id text NOT NULL UNIQUE,
    tenant_id text NOT NULL,
    idempotency_key text NOT NULL,
    payload jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_outbox_pending ON audit_outbox (status, next_attempt_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_audit_outbox_tenant_idempotency
    ON audit_outbox (tenant_id, idempotency_key);

CREATE TABLE IF NOT EXISTS restore_runs (
    id text PRIMARY KEY,
    tenant_id text NOT NULL REFERENCES tenants(id),
    operation_id text NOT NULL,
    status text NOT NULL,
    reason text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_restore_runs_tenant_created
    ON restore_runs (tenant_id, created_at DESC);

ALTER TABLE event_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE ledger_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE legal_holds ENABLE ROW LEVEL SECURITY;
ALTER TABLE restore_runs ENABLE ROW LEVEL SECURITY;

-- The application must set this transaction-local value after verifying the
-- token. It is deliberately not derived from a request header.
DROP POLICY IF EXISTS event_receipts_tenant_isolation ON event_receipts;
CREATE POLICY event_receipts_tenant_isolation ON event_receipts
    USING (tenant_id = current_setting('app.tenant_id', true));

DROP POLICY IF EXISTS ledger_events_tenant_isolation ON ledger_events;
CREATE POLICY ledger_events_tenant_isolation ON ledger_events
    USING (tenant_id = current_setting('app.tenant_id', true));

DROP POLICY IF EXISTS legal_holds_tenant_isolation ON legal_holds;
CREATE POLICY legal_holds_tenant_isolation ON legal_holds
    USING (tenant_id = current_setting('app.tenant_id', true));

DROP POLICY IF EXISTS restore_runs_tenant_isolation ON restore_runs;
CREATE POLICY restore_runs_tenant_isolation ON restore_runs
    USING (tenant_id = current_setting('app.tenant_id', true));
