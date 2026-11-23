CREATE TABLE kafka_stream_sources (
    source_id TEXT PRIMARY KEY,
    descriptor_json JSONB NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('bounded_high_water', 'continuous_bounded')),
    writer_key TEXT NOT NULL,
    assignment_key TEXT NOT NULL,
    writer_scope TEXT NOT NULL DEFAULT 'unverified',
    enabled INTEGER NOT NULL DEFAULT 0 CHECK (enabled IN (0, 1)),
    control_epoch BIGINT NOT NULL DEFAULT 0,
    initial_offset TEXT,
    active_task_id TEXT,
    completed_task_id TEXT,
    block_reason TEXT,
    block_stage TEXT,
    broker_identity TEXT,
    broker_writer_key TEXT UNIQUE,
    idle_at TIMESTAMPTZ,
    scheduler_token TEXT,
    scheduler_heartbeat_at TIMESTAMPTZ,
    scheduler_phase TEXT,
    scheduler_control_epoch BIGINT
);

CREATE UNIQUE INDEX idx_kafka_continuous_writer ON kafka_stream_sources(writer_key)
    WHERE mode = 'continuous_bounded';
CREATE INDEX idx_kafka_stream_writer ON kafka_stream_sources(writer_key);
CREATE INDEX idx_kafka_stream_assignment ON kafka_stream_sources(assignment_key)
    WHERE broker_writer_key IS NOT NULL;
ALTER TABLE streaming_tasks ADD COLUMN kafka_source_id TEXT REFERENCES kafka_stream_sources(source_id);
CREATE INDEX idx_streaming_tasks_kafka_source ON streaming_tasks(kafka_source_id, created_at, task_id);

INSERT INTO kafka_stream_sources(source_id, descriptor_json, mode, writer_key, assignment_key, active_task_id)
SELECT 'legacy_' || task_id, source_identity_json, 'bounded_high_water',
       COALESCE(source_identity_json->'configuration'->>'bootstrap_env', '') || '|' ||
       COALESCE(source_identity_json->'configuration'->>'topic', '') || '|' ||
       COALESCE(source_identity_json->'configuration'->>'consumer_group', ''),
       COALESCE(source_identity_json->'configuration'->>'topic', '') || '|' ||
       COALESCE(source_identity_json->'configuration'->>'consumer_group', ''), task_id
FROM streaming_tasks WHERE source_kind = 'kafka';
UPDATE streaming_tasks SET kafka_source_id = 'legacy_' || task_id WHERE source_kind = 'kafka';

CREATE TABLE kafka_observer_state (
    source_id TEXT PRIMARY KEY REFERENCES kafka_stream_sources(source_id) ON DELETE CASCADE,
    owner_token TEXT,
    latest_epoch TEXT,
    sampled_at TIMESTAMPTZ,
    status TEXT,
    expires_at TIMESTAMPTZ,
    next_due_at TIMESTAMPTZ,
    failed_samples INTEGER NOT NULL DEFAULT 0,
    recovering INTEGER NOT NULL DEFAULT 0,
    snapshot_json JSONB
);

CREATE TABLE kafka_worker_observations (
    task_id TEXT PRIMARY KEY REFERENCES streaming_tasks(task_id) ON DELETE CASCADE,
    worker_epoch TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    snapshot_json JSONB NOT NULL
);

ALTER TABLE streaming_window_commits ADD COLUMN commit_sequence BIGINT;
CREATE UNIQUE INDEX idx_streaming_commits_sequence
    ON streaming_window_commits(task_id, commit_sequence) WHERE commit_sequence IS NOT NULL;
CREATE INDEX idx_streaming_commits_time ON streaming_window_commits(task_id, committed_at);

ALTER TABLE streaming_task_events ADD COLUMN kafka_source_id TEXT REFERENCES kafka_stream_sources(source_id);
UPDATE streaming_task_events
SET kafka_source_id = (SELECT kafka_source_id FROM streaming_tasks t WHERE t.task_id = streaming_task_events.task_id)
WHERE EXISTS (SELECT 1 FROM streaming_tasks t WHERE t.task_id = streaming_task_events.task_id AND t.source_kind = 'kafka');
CREATE INDEX idx_streaming_events_kafka_source
    ON streaming_task_events(kafka_source_id, created_at DESC, task_id DESC, sequence DESC);
