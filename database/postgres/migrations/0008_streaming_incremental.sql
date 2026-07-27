CREATE TABLE IF NOT EXISTS streaming_tasks (
    task_id TEXT PRIMARY KEY,
    source_kind TEXT NOT NULL CHECK (source_kind IN ('file', 'kafka')),
    source_identity_json JSONB NOT NULL,
    config_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'failed', 'interrupted', 'completed', 'conflict')),
    stage TEXT NOT NULL CHECK (stage IN ('READING', 'SPOOLING', 'MINING', 'AGGREGATING', 'COMPLETED', 'FAILED', 'CONFLICT')),
    cursor_json JSONB NOT NULL,
    task_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_streaming_tasks_status_updated
    ON streaming_tasks(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS streaming_window_commits (
    task_id TEXT NOT NULL REFERENCES streaming_tasks(task_id) ON DELETE CASCADE,
    window_id TEXT NOT NULL,
    cursor_json JSONB NOT NULL,
    summary_json JSONB NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (task_id, window_id)
);

CREATE TABLE IF NOT EXISTS unknown_template_queue (
    task_id TEXT NOT NULL REFERENCES streaming_tasks(task_id) ON DELETE CASCADE,
    template_hash TEXT NOT NULL,
    component TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    config_hash TEXT NOT NULL,
    occurrence_count BIGINT NOT NULL CHECK (occurrence_count >= 0),
    template_json JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('open', 'reviewed', 'ignored')),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (task_id, template_hash, window_start)
);

CREATE INDEX IF NOT EXISTS idx_unknown_template_queue_status_updated
    ON unknown_template_queue(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS streaming_task_events (
    task_id TEXT NOT NULL REFERENCES streaming_tasks(task_id) ON DELETE CASCADE,
    sequence BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    event_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (task_id, sequence)
);
