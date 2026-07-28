CREATE TABLE IF NOT EXISTS observability_runs (
    observation_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL UNIQUE,
    input_job_id TEXT,
    status TEXT NOT NULL,
    attributes_json JSONB NOT NULL,
    schema_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_observability_runs_status_updated
    ON observability_runs(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS observability_spans (
    span_id TEXT PRIMARY KEY,
    observation_id TEXT NOT NULL REFERENCES observability_runs(observation_id) ON DELETE CASCADE,
    trace_id TEXT,
    parent_span_id TEXT REFERENCES observability_spans(span_id),
    name TEXT NOT NULL,
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    duration_ms BIGINT,
    attributes_json JSONB NOT NULL,
    idempotency_key TEXT UNIQUE,
    schema_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_observability_spans_run_time
    ON observability_spans(observation_id, started_at);
CREATE INDEX IF NOT EXISTS idx_observability_spans_stage_status
    ON observability_spans(stage, status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_observability_spans_trace
    ON observability_spans(trace_id);

CREATE TABLE IF NOT EXISTS replay_runs (
    replay_id TEXT PRIMARY KEY,
    observation_id TEXT REFERENCES observability_runs(observation_id) ON DELETE SET NULL,
    source_trace_id TEXT NOT NULL REFERENCES ai_traces(trace_id),
    mode TEXT NOT NULL CHECK (mode IN ('historical', 'model')),
    status TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    snapshot_json JSONB NOT NULL,
    result_json JSONB NOT NULL,
    error_code TEXT,
    error_message TEXT,
    schema_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_replay_runs_source_created
    ON replay_runs(source_trace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_replay_runs_status_updated
    ON replay_runs(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS replay_events (
    replay_id TEXT NOT NULL REFERENCES replay_runs(replay_id) ON DELETE CASCADE,
    sequence BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    event_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (replay_id, sequence)
);
