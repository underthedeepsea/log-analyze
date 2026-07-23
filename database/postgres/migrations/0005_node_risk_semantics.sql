CREATE TABLE IF NOT EXISTS risk_semantic_rules (
    rule_id TEXT PRIMARY KEY, source TEXT NOT NULL CHECK (source IN ('builtin', 'user', 'imported', 'ai_candidate')),
    override_of TEXT REFERENCES risk_semantic_rules(rule_id), status TEXT NOT NULL CHECK (status IN ('draft', 'published', 'disabled', 'deprecated')),
    enabled BOOLEAN NOT NULL, current_version INTEGER NOT NULL CHECK (current_version > 0), content_json JSONB NOT NULL,
    content_hash TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'risk_semantic_rule_v1'
);

CREATE TABLE IF NOT EXISTS risk_semantic_rule_versions (
    rule_id TEXT NOT NULL REFERENCES risk_semantic_rules(rule_id) ON DELETE CASCADE, version INTEGER NOT NULL CHECK (version > 0),
    content_json JSONB NOT NULL, content_hash TEXT NOT NULL, changed_by TEXT NOT NULL, change_reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL, schema_version TEXT NOT NULL DEFAULT 'risk_semantic_rule_version_v1',
    PRIMARY KEY (rule_id, version)
);

CREATE TABLE IF NOT EXISTS risk_semantic_events (
    event_id TEXT PRIMARY KEY, rule_id TEXT REFERENCES risk_semantic_rules(rule_id), event_type TEXT NOT NULL,
    from_version INTEGER, to_version INTEGER, before_json JSONB, after_json JSONB, operator TEXT NOT NULL, reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL, schema_version TEXT NOT NULL DEFAULT 'risk_semantic_event_v1'
);

CREATE TABLE IF NOT EXISTS risk_semantic_validations (
    rule_id TEXT NOT NULL REFERENCES risk_semantic_rules(rule_id) ON DELETE CASCADE, version INTEGER NOT NULL,
    valid BOOLEAN NOT NULL, validation_json JSONB NOT NULL, content_hash TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'risk_semantic_validation_v1', PRIMARY KEY (rule_id, version)
);

CREATE TABLE IF NOT EXISTS risk_semantic_unclassified (
    candidate_id TEXT PRIMARY KEY, component TEXT, template_hash TEXT, typed_message TEXT NOT NULL,
    occurrence_count INTEGER NOT NULL DEFAULT 1, first_seen TIMESTAMPTZ NOT NULL, last_seen TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'open', candidate_json JSONB NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'risk_semantic_candidate_v1'
);

CREATE TABLE IF NOT EXISTS node_risk_events (
    event_id TEXT PRIMARY KEY, cluster TEXT NOT NULL, node_id TEXT NOT NULL, risk_domain TEXT NOT NULL,
    risk_category TEXT NOT NULL, risk_type TEXT NOT NULL, risk_subtype TEXT, severity TEXT NOT NULL,
    base_score DOUBLE PRECISION NOT NULL, confidence DOUBLE PRECISION NOT NULL, semantic_rule_id TEXT NOT NULL,
    semantic_rule_version INTEGER NOT NULL, dedup_key TEXT NOT NULL, occurrence_count INTEGER NOT NULL CHECK (occurrence_count > 0),
    first_seen TIMESTAMPTZ NOT NULL, last_seen TIMESTAMPTZ NOT NULL, window_start TIMESTAMPTZ NOT NULL, window_end TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'acknowledged', 'recovered')), recovered_at TIMESTAMPTZ,
    acknowledged_at TIMESTAMPTZ, acknowledged_by TEXT, semantic_fields_json JSONB NOT NULL, evidence_refs_json JSONB NOT NULL,
    source_job_id TEXT, source_trace_id TEXT, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'node_risk_event_v1', UNIQUE (dedup_key, window_start)
);

CREATE TABLE IF NOT EXISTS node_risk_ingestions (
    source_event_fingerprint TEXT PRIMARY KEY, event_id TEXT NOT NULL REFERENCES node_risk_events(event_id) ON DELETE CASCADE,
    source_job_id TEXT, occurrence_count INTEGER NOT NULL, ingested_at TIMESTAMPTZ NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'node_risk_ingestion_v1'
);

CREATE TABLE IF NOT EXISTS node_risk_daily (
    cluster TEXT NOT NULL, node_id TEXT NOT NULL, date DATE NOT NULL, event_count INTEGER NOT NULL,
    occurrence_count INTEGER NOT NULL, distinct_risk_types INTEGER NOT NULL, critical_count INTEGER NOT NULL,
    high_count INTEGER NOT NULL, medium_count INTEGER NOT NULL, low_count INTEGER NOT NULL, active_count INTEGER NOT NULL,
    recovered_count INTEGER NOT NULL, max_event_score DOUBLE PRECISION NOT NULL, max_overall_score DOUBLE PRECISION NOT NULL,
    latest_risk_at TIMESTAMPTZ, domain_distribution_json JSONB NOT NULL, type_distribution_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL, schema_version TEXT NOT NULL DEFAULT 'node_risk_daily_v1', PRIMARY KEY (cluster, node_id, date)
);

CREATE TABLE IF NOT EXISTS node_risk_snapshots (
    cluster TEXT NOT NULL, node_id TEXT NOT NULL, overall_score DOUBLE PRECISION NOT NULL, overall_level TEXT NOT NULL,
    confidence DOUBLE PRECISION NOT NULL, trend TEXT NOT NULL, active_event_count INTEGER NOT NULL, event_count_24h INTEGER NOT NULL,
    event_count_7d INTEGER NOT NULL, event_count_30d INTEGER NOT NULL, occurrence_count_24h INTEGER NOT NULL,
    distinct_risk_types_7d INTEGER NOT NULL, primary_risks_json JSONB NOT NULL, assessment_reasons_json JSONB NOT NULL,
    score_breakdown_json JSONB NOT NULL, latest_risk_at TIMESTAMPTZ, calculated_at TIMESTAMPTZ NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'node_risk_snapshot_v1', PRIMARY KEY (cluster, node_id)
);

CREATE TABLE IF NOT EXISTS node_risk_audit_events (
    audit_id TEXT PRIMARY KEY, event_id TEXT REFERENCES node_risk_events(event_id), event_type TEXT NOT NULL,
    event_json JSONB NOT NULL, operator TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'node_risk_audit_v1'
);

CREATE INDEX IF NOT EXISTS idx_risk_semantic_effective ON risk_semantic_rules(status, enabled, source);
CREATE INDEX IF NOT EXISTS idx_risk_semantic_override ON risk_semantic_rules(override_of);
CREATE INDEX IF NOT EXISTS idx_node_risk_node_time ON node_risk_events(cluster, node_id, last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_node_risk_type_time ON node_risk_events(risk_type, last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_node_risk_status_time ON node_risk_events(status, last_seen DESC);
