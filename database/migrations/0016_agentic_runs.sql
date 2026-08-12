CREATE TABLE IF NOT EXISTS agent_runs (
    run_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    parent_run_id TEXT REFERENCES agent_runs(run_id),
    source_job_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    model_profile_id TEXT NOT NULL,
    prompt_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'planning', 'running', 'paused', 'awaiting_human', 'completed', 'failed', 'cancelled')),
    max_steps INTEGER NOT NULL CHECK (max_steps > 0),
    max_tool_calls INTEGER NOT NULL CHECK (max_tool_calls > 0),
    used_tool_calls INTEGER NOT NULL DEFAULT 0 CHECK (used_tool_calls >= 0),
    timeout_seconds REAL NOT NULL CHECK (timeout_seconds > 0),
    allowed_tools_json TEXT NOT NULL,
    locked_snapshot_json TEXT NOT NULL,
    goal TEXT,
    error_code TEXT,
    error_summary TEXT,
    idempotency_key TEXT NOT NULL,
    actor TEXT NOT NULL,
    roles_json TEXT NOT NULL,
    request_id TEXT NOT NULL,
    state_version INTEGER NOT NULL DEFAULT 1 CHECK (state_version >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    UNIQUE(actor, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_agent_runs_status_created ON agent_runs(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_runs_source_entity ON agent_runs(source_job_id, entity_id, created_at DESC);

CREATE TABLE IF NOT EXISTS agent_run_steps (
    run_id TEXT NOT NULL REFERENCES agent_runs(run_id) ON DELETE CASCADE,
    step_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    tool_name TEXT NOT NULL,
    arguments_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    result_summary_json TEXT NOT NULL,
    error_code TEXT,
    error_summary TEXT,
    started_at TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    schema_version TEXT NOT NULL DEFAULT '1.0',
    PRIMARY KEY (run_id, step_id),
    UNIQUE (run_id, sequence)
);
CREATE INDEX IF NOT EXISTS idx_agent_run_steps_run_sequence ON agent_run_steps(run_id, sequence);

CREATE TABLE IF NOT EXISTS agent_tool_calls (
    tool_call_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES agent_runs(run_id) ON DELETE CASCADE,
    step_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    arguments_summary_json TEXT NOT NULL,
    result_summary_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    cost_units INTEGER NOT NULL DEFAULT 1 CHECK (cost_units > 0),
    latency_ms INTEGER,
    error_code TEXT,
    error_summary TEXT,
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    schema_version TEXT NOT NULL DEFAULT '1.0',
    UNIQUE (run_id, idempotency_key),
    FOREIGN KEY (run_id, step_id) REFERENCES agent_run_steps(run_id, step_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_agent_tool_calls_run_created ON agent_tool_calls(run_id, created_at);

CREATE TABLE IF NOT EXISTS agent_artifacts (
    artifact_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES agent_runs(run_id) ON DELETE CASCADE,
    step_id TEXT,
    artifact_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    fingerprint TEXT,
    created_at TEXT NOT NULL,
    schema_version TEXT NOT NULL DEFAULT '1.0',
    FOREIGN KEY (run_id, step_id) REFERENCES agent_run_steps(run_id, step_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_agent_artifacts_run_type ON agent_artifacts(run_id, artifact_type, created_at);

CREATE TABLE IF NOT EXISTS agent_run_events (
    event_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES agent_runs(run_id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    event_type TEXT NOT NULL,
    attributes_json TEXT NOT NULL,
    idempotency_key TEXT,
    created_at TEXT NOT NULL,
    schema_version TEXT NOT NULL DEFAULT '1.0',
    UNIQUE (run_id, sequence),
    UNIQUE (run_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_agent_run_events_run_sequence ON agent_run_events(run_id, sequence);
