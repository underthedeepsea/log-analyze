CREATE TABLE IF NOT EXISTS app_settings (
    setting_key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS legacy_imports (
    source_path TEXT PRIMARY KEY,
    source_sha256 TEXT NOT NULL,
    records_imported INTEGER NOT NULL DEFAULT 0,
    imported_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS provider_connections (
    connection_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    provider TEXT NOT NULL CHECK (provider IN ('ollama', 'openai_compatible')),
    base_url TEXT NOT NULL,
    api_key_env TEXT,
    timeout_seconds REAL NOT NULL DEFAULT 120 CHECK (timeout_seconds > 0),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    is_default INTEGER NOT NULL DEFAULT 0 CHECK (is_default IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS model_profiles (
    profile_id TEXT PRIMARY KEY,
    connection_id TEXT NOT NULL REFERENCES provider_connections(connection_id),
    model TEXT NOT NULL,
    display_name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    structured_output_mode TEXT NOT NULL DEFAULT 'json_schema'
        CHECK (structured_output_mode IN ('json_schema', 'json_object', 'prompt_only')),
    profile_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_model_profiles_connection ON model_profiles(connection_id);

CREATE TABLE IF NOT EXISTS prompt_templates (
    prompt_id TEXT PRIMARY KEY,
    analysis_type TEXT NOT NULL,
    display_name TEXT,
    description TEXT,
    status TEXT NOT NULL,
    is_default INTEGER NOT NULL DEFAULT 0,
    current_version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS prompt_versions (
    prompt_id TEXT NOT NULL REFERENCES prompt_templates(prompt_id),
    version INTEGER NOT NULL,
    content TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    note TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (prompt_id, version)
);

CREATE TABLE IF NOT EXISTS feature_jobs (
    job_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    model_profile_id TEXT REFERENCES model_profiles(profile_id),
    connection_snapshot_json TEXT,
    profile_snapshot_json TEXT,
    job_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS feature_job_entities (
    job_id TEXT NOT NULL REFERENCES feature_jobs(job_id) ON DELETE CASCADE,
    entity_id TEXT NOT NULL,
    status TEXT NOT NULL,
    risk_score REAL,
    entity_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job_id, entity_id)
);

CREATE TABLE IF NOT EXISTS feature_candidates (
    candidate_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES feature_jobs(job_id) ON DELETE CASCADE,
    entity_id TEXT,
    status TEXT,
    candidate_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS feature_job_events (
    job_id TEXT NOT NULL REFERENCES feature_jobs(job_id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    event_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (job_id, sequence)
);

CREATE TABLE IF NOT EXISTS approved_rules (
    rule_id TEXT PRIMARY KEY,
    signature TEXT NOT NULL UNIQUE,
    feature_type TEXT NOT NULL,
    rule_json TEXT NOT NULL,
    approved_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rule_reuse_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id TEXT NOT NULL REFERENCES approved_rules(rule_id),
    job_id TEXT,
    entity_id TEXT,
    reused_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ai_traces (
    trace_id TEXT PRIMARY KEY,
    job_id TEXT,
    provider TEXT,
    model TEXT,
    status TEXT,
    prompt_id TEXT,
    prompt_hash TEXT,
    latency_ms INTEGER,
    trace_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ai_traces_created ON ai_traces(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ai_traces_job ON ai_traces(job_id);

CREATE TABLE IF NOT EXISTS ai_cache_entries (
    signature TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS processing_metrics_daily (
    metric_date TEXT PRIMARY KEY,
    llm_logs INTEGER NOT NULL DEFAULT 0 CHECK (llm_logs >= 0),
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS upload_sessions (
    upload_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    filename TEXT NOT NULL,
    source_path TEXT,
    size_bytes INTEGER NOT NULL,
    sha256 TEXT,
    manifest_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS input_jobs (
    input_job_id TEXT PRIMARY KEY,
    upload_id TEXT REFERENCES upload_sessions(upload_id),
    status TEXT NOT NULL,
    stage TEXT NOT NULL,
    job_json TEXT NOT NULL,
    progress_json TEXT NOT NULL,
    result_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id TEXT PRIMARY KEY,
    owner_type TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    path TEXT NOT NULL,
    size_bytes INTEGER,
    sha256 TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS drain_templates (
    template_hash TEXT PRIMARY KEY,
    component TEXT,
    status TEXT,
    template_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS drain_template_versions (
    template_hash TEXT NOT NULL REFERENCES drain_templates(template_hash),
    version INTEGER NOT NULL,
    template_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (template_hash, version)
);

CREATE TABLE IF NOT EXISTS drain_template_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    template_hash TEXT,
    event_type TEXT NOT NULL,
    event_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS drain_config_versions (
    config_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    status TEXT NOT NULL,
    content TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    config_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (config_id, version)
);

CREATE TABLE IF NOT EXISTS drain_config_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    config_id TEXT,
    version INTEGER,
    event_type TEXT NOT NULL,
    event_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS drain_datasets (
    dataset_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    version TEXT,
    dataset_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS drain_annotations (
    annotation_id TEXT PRIMARY KEY,
    dataset_id TEXT,
    annotation_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS drain_reviews (
    review_id TEXT PRIMARY KEY,
    annotation_id TEXT,
    review_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS drain_eval_runs (
    evaluation_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    evaluation_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS drain_tune_runs (
    tune_run_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    tune_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS semantic_dictionaries (
    dictionary_id TEXT PRIMARY KEY,
    display_name TEXT,
    active_version INTEGER NOT NULL,
    dictionary_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS semantic_dictionary_versions (
    dictionary_id TEXT NOT NULL REFERENCES semantic_dictionaries(dictionary_id),
    version INTEGER NOT NULL,
    status TEXT NOT NULL,
    dictionary_json TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (dictionary_id, version)
);

CREATE TABLE IF NOT EXISTS semantic_validation_runs (
    dictionary_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    validation_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (dictionary_id, version)
);

CREATE TABLE IF NOT EXISTS semantic_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    dictionary_id TEXT,
    version INTEGER,
    event_type TEXT NOT NULL,
    event_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
