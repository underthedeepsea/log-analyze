CREATE TABLE IF NOT EXISTS orchestration_runs (
    orchestration_run_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES feature_jobs(job_id) ON DELETE CASCADE,
    orchestrator TEXT NOT NULL CHECK (orchestrator IN ('airflow')),
    external_dag_id TEXT,
    external_run_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending_dispatch', 'dispatched', 'running', 'cancel_requested', 'completed', 'failed', 'cancelled', 'dispatch_failed')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    state_version INTEGER NOT NULL DEFAULT 1 CHECK (state_version >= 1),
    request_id TEXT NOT NULL,
    actor TEXT NOT NULL,
    roles_json JSONB NOT NULL,
    last_heartbeat_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_code TEXT,
    error_summary TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (job_id, orchestrator)
);

CREATE INDEX IF NOT EXISTS idx_orchestration_runs_reconcile
    ON orchestration_runs(status, updated_at, orchestration_run_id);
