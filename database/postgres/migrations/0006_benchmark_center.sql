CREATE TABLE IF NOT EXISTS benchmark_suites (
    suite_id TEXT PRIMARY KEY, name TEXT NOT NULL, source_type TEXT NOT NULL CHECK (source_type IN ('canonical', 'trace', 'custom')),
    case_count INTEGER NOT NULL DEFAULT 0 CHECK (case_count >= 0), suite_json JSONB NOT NULL,
    version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0), created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'benchmark_suite_v1'
);

CREATE TABLE IF NOT EXISTS benchmark_runs (
    run_id TEXT PRIMARY KEY, suite_id TEXT NOT NULL REFERENCES benchmark_suites(suite_id) ON DELETE RESTRICT,
    mode TEXT NOT NULL CHECK (mode IN ('fake', 'history', 'real')),
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    idempotency_key TEXT NOT NULL UNIQUE, snapshot_json JSONB NOT NULL, metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    progress_completed INTEGER NOT NULL DEFAULT 0 CHECK (progress_completed >= 0), progress_total INTEGER NOT NULL DEFAULT 0 CHECK (progress_total >= 0),
    error TEXT, version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0), created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL, schema_version TEXT NOT NULL DEFAULT 'benchmark_run_v1'
);

CREATE TABLE IF NOT EXISTS benchmark_case_results (
    result_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES benchmark_runs(run_id) ON DELETE CASCADE,
    case_id TEXT NOT NULL, passed BOOLEAN NOT NULL, json_valid BOOLEAN NOT NULL, schema_valid BOOLEAN NOT NULL,
    template_reference_ok BOOLEAN NOT NULL, duration_ms DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (duration_ms >= 0),
    error_type TEXT, result_json JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'benchmark_case_result_v1', UNIQUE (run_id, case_id)
);

CREATE TABLE IF NOT EXISTS benchmark_gates (
    gate_id TEXT PRIMARY KEY, baseline_run_id TEXT NOT NULL REFERENCES benchmark_runs(run_id) ON DELETE RESTRICT,
    candidate_run_id TEXT NOT NULL REFERENCES benchmark_runs(run_id) ON DELETE RESTRICT,
    decision TEXT NOT NULL CHECK (decision IN ('passed', 'blocked', 'manual_review')), thresholds_json JSONB NOT NULL,
    deltas_json JSONB NOT NULL, reasons_json JSONB NOT NULL, operator TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'benchmark_gate_v1'
);

CREATE TABLE IF NOT EXISTS benchmark_artifacts (
    artifact_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES benchmark_runs(run_id) ON DELETE CASCADE,
    artifact_type TEXT NOT NULL, path TEXT NOT NULL, size_bytes BIGINT NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    sha256 TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, schema_version TEXT NOT NULL DEFAULT 'benchmark_artifact_v1'
);

CREATE TABLE IF NOT EXISTS benchmark_audit_events (
    event_id TEXT PRIMARY KEY, run_id TEXT REFERENCES benchmark_runs(run_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL, event_json JSONB NOT NULL, operator TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'benchmark_audit_event_v1'
);

CREATE INDEX IF NOT EXISTS idx_benchmark_suites_updated ON benchmark_suites(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_benchmark_runs_status_updated ON benchmark_runs(status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_benchmark_runs_suite_created ON benchmark_runs(suite_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_benchmark_case_run_passed ON benchmark_case_results(run_id, passed, case_id);
CREATE INDEX IF NOT EXISTS idx_benchmark_gates_candidate_created ON benchmark_gates(candidate_run_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_benchmark_audit_run_created ON benchmark_audit_events(run_id, created_at DESC);
