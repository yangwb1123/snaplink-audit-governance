-- Per-tenant hot documents and append-only cold ledger for the bounded
-- control-plane layout. The existing audit_state_snapshot row remains the
-- global control plane; migration 006 is additive so the old backend can be
-- rolled back before the cutover/import is completed.
CREATE TABLE IF NOT EXISTS audit_tenant (
    tenant_id  TEXT PRIMARY KEY,
    snapshot   JSONB NOT NULL,
    version    BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_ledger (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   TEXT NOT NULL,
    record_type TEXT NOT NULL CHECK (record_type IN ('receipt', 'segment', 'checkpoint')),
    key         TEXT NOT NULL,
    version     INTEGER NOT NULL CHECK (version > 0),
    record      JSONB NOT NULL,
    written_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, record_type, key, version)
);

CREATE INDEX IF NOT EXISTS audit_ledger_tenant_id_idx
    ON audit_ledger (tenant_id, id);

CREATE INDEX IF NOT EXISTS audit_ledger_receipt_key_idx
    ON audit_ledger (tenant_id, record_type, key, version DESC)
    WHERE record_type = 'receipt';

CREATE INDEX IF NOT EXISTS audit_ledger_receipt_idempotency_idx
    ON audit_ledger (tenant_id, ((record->'receipt'->>'idempotency_key')))
    WHERE record_type = 'receipt' AND (record->'receipt'->>'idempotency_key') <> '';
