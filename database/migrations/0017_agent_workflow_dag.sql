CREATE TABLE IF NOT EXISTS agent_workflows (
    workflow_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL DEFAULT '1.0',
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    definition_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    actor TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(actor, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_agent_workflows_status_created ON agent_workflows(status, created_at DESC);

CREATE TABLE IF NOT EXISTS agent_workflow_runs (
    workflow_run_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL DEFAULT '1.0',
    workflow_id TEXT NOT NULL REFERENCES agent_workflows(workflow_id),
    parent_run_id TEXT REFERENCES agent_workflow_runs(workflow_run_id),
    source_job_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    model_profile_id TEXT NOT NULL,
    prompt_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued','running','paused','awaiting_human','completed','failed','cancelled')),
    max_concurrency INTEGER NOT NULL CHECK (max_concurrency > 0),
    max_tool_calls INTEGER NOT NULL CHECK (max_tool_calls > 0),
    used_tool_calls INTEGER NOT NULL DEFAULT 0 CHECK (used_tool_calls >= 0),
    timeout_seconds REAL NOT NULL CHECK (timeout_seconds > 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
    locked_snapshot_json TEXT NOT NULL,
    error_code TEXT,
    error_summary TEXT,
    actor TEXT NOT NULL,
    roles_json TEXT NOT NULL,
    request_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    state_version INTEGER NOT NULL DEFAULT 1 CHECK (state_version >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    UNIQUE(actor, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_agent_workflow_runs_status_created ON agent_workflow_runs(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_workflow_runs_source_entity ON agent_workflow_runs(source_job_id, entity_id, created_at DESC);

CREATE TABLE IF NOT EXISTS agent_workflow_nodes (
    workflow_run_id TEXT NOT NULL REFERENCES agent_workflow_runs(workflow_run_id) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    schema_version TEXT NOT NULL DEFAULT '1.0',
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    role_id TEXT NOT NULL,
    dependencies_json TEXT NOT NULL,
    allowed_tools_json TEXT NOT NULL,
    max_steps INTEGER NOT NULL CHECK (max_steps > 0),
    max_tool_calls INTEGER NOT NULL CHECK (max_tool_calls > 0),
    timeout_seconds REAL NOT NULL CHECK (timeout_seconds > 0),
    status TEXT NOT NULL CHECK (status IN ('pending','running','completed','failed','cancelled','skipped')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    child_agent_run_id TEXT REFERENCES agent_runs(run_id) ON DELETE SET NULL,
    used_tool_calls INTEGER NOT NULL DEFAULT 0 CHECK (used_tool_calls >= 0),
    result_summary_json TEXT NOT NULL,
    error_code TEXT,
    error_summary TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    PRIMARY KEY(workflow_run_id, node_id),
    UNIQUE(workflow_run_id, sequence)
);
CREATE INDEX IF NOT EXISTS idx_agent_workflow_nodes_run_status ON agent_workflow_nodes(workflow_run_id, status, sequence);

CREATE TABLE IF NOT EXISTS agent_workflow_events (
    event_id TEXT PRIMARY KEY,
    workflow_run_id TEXT NOT NULL REFERENCES agent_workflow_runs(workflow_run_id) ON DELETE CASCADE,
    schema_version TEXT NOT NULL DEFAULT '1.0',
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    event_type TEXT NOT NULL,
    node_id TEXT,
    attributes_json TEXT NOT NULL,
    idempotency_key TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(workflow_run_id, sequence),
    UNIQUE(workflow_run_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_agent_workflow_events_run_sequence ON agent_workflow_events(workflow_run_id, sequence);
