ALTER TABLE provider_connections
    DROP CONSTRAINT IF EXISTS provider_connections_provider_check;

ALTER TABLE provider_connections
    ADD CONSTRAINT provider_connections_provider_check
    CHECK (provider IN ('ollama', 'openai_compatible', 'extension'));

ALTER TABLE provider_connections
    ADD COLUMN IF NOT EXISTS adapter_id TEXT;

ALTER TABLE provider_connections
    ADD COLUMN IF NOT EXISTS credential_envs_json JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE provider_connections
    ADD COLUMN IF NOT EXISTS extension_config_json JSONB NOT NULL DEFAULT '{}'::jsonb;
