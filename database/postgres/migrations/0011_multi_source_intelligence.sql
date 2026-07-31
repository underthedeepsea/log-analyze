CREATE TABLE IF NOT EXISTS multi_source_rules (
    rule_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL,
    definition_json JSONB NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    schema_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS multi_source_observations (
    observation_id TEXT PRIMARY KEY,
    source_job_id TEXT,
    cluster TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_family TEXT NOT NULL,
    component TEXT NOT NULL,
    severity TEXT,
    template_hash TEXT NOT NULL,
    template TEXT NOT NULL,
    occurrence_count BIGINT NOT NULL CHECK (occurrence_count > 0),
    risk_score DOUBLE PRECISION NOT NULL,
    risk_level TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    entity_keys_json JSONB NOT NULL,
    relations_json JSONB NOT NULL,
    schema_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_multi_source_observation_time
    ON multi_source_observations(cluster, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_multi_source_observation_source
    ON multi_source_observations(source_family, window_start DESC);

CREATE TABLE IF NOT EXISTS multi_source_correlations (
    correlation_id TEXT PRIMARY KEY,
    rule_id TEXT NOT NULL REFERENCES multi_source_rules(rule_id),
    rule_version BIGINT NOT NULL,
    cluster TEXT NOT NULL,
    primary_entity_key TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    confidence DOUBLE PRECISION NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    risk_score DOUBLE PRECISION NOT NULL,
    source_families_json JSONB NOT NULL,
    schema_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_multi_source_correlation_entity
    ON multi_source_correlations(primary_entity_key, window_start DESC);

CREATE TABLE IF NOT EXISTS multi_source_correlation_items (
    correlation_id TEXT NOT NULL REFERENCES multi_source_correlations(correlation_id) ON DELETE CASCADE,
    observation_id TEXT NOT NULL REFERENCES multi_source_observations(observation_id) ON DELETE CASCADE,
    position BIGINT NOT NULL,
    PRIMARY KEY (correlation_id, observation_id)
);

CREATE TABLE IF NOT EXISTS multi_source_audit_events (
    audit_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT,
    actor TEXT,
    request_id TEXT NOT NULL,
    attributes_json JSONB NOT NULL,
    schema_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_multi_source_audit_created
    ON multi_source_audit_events(created_at DESC, audit_id DESC);
