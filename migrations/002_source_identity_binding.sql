ALTER TABLE sources
    ADD COLUMN IF NOT EXISTS allowed_client_ids jsonb NOT NULL DEFAULT '[]'::jsonb;

COMMENT ON COLUMN sources.allowed_client_ids IS
    'Exact OAuth client_id values allowed to ingest as this source. An empty array permits only client_id = sources.id.';

CREATE INDEX IF NOT EXISTS idx_sources_active_source_id
    ON sources (id)
    WHERE active;

CREATE INDEX IF NOT EXISTS idx_sources_allowed_client_ids
    ON sources USING gin (allowed_client_ids)
    WHERE active;
