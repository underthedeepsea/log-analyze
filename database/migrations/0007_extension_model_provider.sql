PRAGMA foreign_keys = OFF;

CREATE TABLE provider_connections_v7 (
    connection_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    provider TEXT NOT NULL CHECK (provider IN ('ollama', 'openai_compatible', 'extension')),
    base_url TEXT NOT NULL,
    api_key_env TEXT,
    adapter_id TEXT,
    credential_envs_json TEXT NOT NULL DEFAULT '{}',
    extension_config_json TEXT NOT NULL DEFAULT '{}',
    timeout_seconds REAL NOT NULL DEFAULT 120 CHECK (timeout_seconds > 0),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    is_default INTEGER NOT NULL DEFAULT 0 CHECK (is_default IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

INSERT INTO provider_connections_v7(
    connection_id, display_name, provider, base_url, api_key_env, adapter_id,
    credential_envs_json, extension_config_json, timeout_seconds, enabled,
    is_default, created_at, updated_at
)
SELECT
    connection_id, display_name, provider, base_url, api_key_env, NULL,
    '{}', '{}', timeout_seconds, enabled, is_default, created_at, updated_at
FROM provider_connections;

DROP TABLE provider_connections;
ALTER TABLE provider_connections_v7 RENAME TO provider_connections;

PRAGMA foreign_keys = ON;
