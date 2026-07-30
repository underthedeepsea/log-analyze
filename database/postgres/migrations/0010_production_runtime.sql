CREATE TABLE IF NOT EXISTS runtime_policies (
    policy_id TEXT PRIMARY KEY,
    policy_json JSONB NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    schema_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime_maintenance_runs (
    run_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('dry_run', 'execute')),
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    summary_json JSONB NOT NULL,
    error_code TEXT,
    error_message TEXT,
    actor TEXT,
    request_id TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_runtime_maintenance_status_updated
    ON runtime_maintenance_runs(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS runtime_quota_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    usage_json JSONB NOT NULL,
    schema_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_runtime_quota_created
    ON runtime_quota_snapshots(created_at DESC);

CREATE TABLE IF NOT EXISTS runtime_audit_events (
    audit_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT,
    actor TEXT,
    roles_json JSONB NOT NULL,
    request_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    attributes_json JSONB NOT NULL,
    schema_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_runtime_audit_created
    ON runtime_audit_events(created_at DESC, audit_id DESC);
CREATE INDEX IF NOT EXISTS idx_runtime_audit_resource
    ON runtime_audit_events(resource_type, resource_id, created_at DESC);
