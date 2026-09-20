CREATE TABLE operational_sources (
    source_id TEXT PRIMARY KEY,
    environment TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    identity_digest TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (environment, scope_key, source_kind, identity_digest)
);

CREATE TABLE operational_ingestion_batches (
    batch_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES operational_sources(source_id) ON DELETE RESTRICT,
    input_job_id TEXT NOT NULL,
    checkpoint_key TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    ranges_json TEXT NOT NULL,
    actual_count BIGINT,
    newly_ingested_count BIGINT,
    provenance TEXT NOT NULL CHECK (provenance IN ('verified', 'reported')),
    committed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (input_job_id, checkpoint_key)
);

CREATE TABLE operational_source_ranges (
    range_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES operational_sources(source_id) ON DELETE RESTRICT,
    kind TEXT NOT NULL CHECK (kind IN ('ingested', 'covered')),
    partition_key TEXT NOT NULL,
    start_position BIGINT NOT NULL CHECK (start_position >= 0),
    end_position BIGINT NOT NULL CHECK (end_position > start_position),
    first_event_at TIMESTAMPTZ,
    origin_id TEXT NOT NULL
);
CREATE INDEX idx_operational_source_ranges_lookup
    ON operational_source_ranges(source_id, kind, partition_key, start_position, end_position);
CREATE INDEX idx_operational_source_ranges_activity
    ON operational_source_ranges(kind, first_event_at);

CREATE TABLE operational_analysis_runs (
    analysis_run_id TEXT PRIMARY KEY,
    environment TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    request_key TEXT NOT NULL,
    input_job_id TEXT,
    source_id TEXT REFERENCES operational_sources(source_id) ON DELETE RESTRICT,
    parent_run_id TEXT REFERENCES operational_analysis_runs(analysis_run_id) ON DELETE RESTRICT,
    provenance TEXT NOT NULL CHECK (provenance IN ('verified', 'reported', 'unverified-input')),
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'partial', 'completed', 'failed', 'cancelled')),
    expected_members BIGINT NOT NULL CHECK (expected_members > 0),
    input_count BIGINT CHECK (input_count IS NULL OR input_count >= 0),
    reported_input_count BIGINT CHECK (reported_input_count IS NULL OR reported_input_count >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    settled_at TIMESTAMPTZ,
    UNIQUE (environment, request_key)
);
CREATE INDEX idx_operational_analysis_runs_completed
    ON operational_analysis_runs(environment, scope_key, completed_at, analysis_run_id);
CREATE INDEX idx_operational_analysis_runs_created
    ON operational_analysis_runs(environment, created_at, analysis_run_id);

CREATE TABLE operational_analysis_ranges (
    analysis_run_id TEXT NOT NULL REFERENCES operational_analysis_runs(analysis_run_id) ON DELETE RESTRICT,
    source_id TEXT NOT NULL REFERENCES operational_sources(source_id) ON DELETE RESTRICT,
    partition_key TEXT NOT NULL,
    start_position BIGINT NOT NULL CHECK (start_position >= 0),
    end_position BIGINT NOT NULL CHECK (end_position > start_position),
    PRIMARY KEY (analysis_run_id, source_id, partition_key, start_position, end_position)
);
CREATE INDEX idx_operational_analysis_ranges_lookup
    ON operational_analysis_ranges(source_id, partition_key, start_position, end_position);

CREATE TABLE operational_analysis_members (
    member_id TEXT PRIMARY KEY,
    analysis_run_id TEXT NOT NULL REFERENCES operational_analysis_runs(analysis_run_id) ON DELETE RESTRICT,
    feature_job_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'partial', 'completed', 'failed', 'cancelled')),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_operational_analysis_members_root_status
    ON operational_analysis_members(analysis_run_id, status);

CREATE TABLE operational_physical_calls (
    call_id TEXT PRIMARY KEY,
    analysis_run_id TEXT REFERENCES operational_analysis_runs(analysis_run_id) ON DELETE RESTRICT,
    logical_call_id TEXT NOT NULL,
    attempt_index BIGINT NOT NULL CHECK (attempt_index >= 0),
    environment TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    call_kind TEXT NOT NULL CHECK (call_kind IN ('provider', 'agent_tool')),
    provider TEXT,
    model TEXT,
    tool_name TEXT,
    caller_kind TEXT NOT NULL,
    caller_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('prepared', 'started', 'succeeded', 'failed', 'unknown')),
    prepared_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    input_tokens BIGINT CHECK (input_tokens IS NULL OR input_tokens >= 0),
    output_tokens BIGINT CHECK (output_tokens IS NULL OR output_tokens >= 0),
    total_tokens BIGINT CHECK (total_tokens IS NULL OR total_tokens >= 0),
    cached_input_tokens BIGINT CHECK (cached_input_tokens IS NULL OR cached_input_tokens >= 0),
    reasoning_tokens BIGINT CHECK (reasoning_tokens IS NULL OR reasoning_tokens >= 0),
    usage_quality TEXT NOT NULL CHECK (usage_quality IN ('known', 'partial', 'unknown', 'invalid')),
    invalid_usage BOOLEAN NOT NULL DEFAULT FALSE,
    error_code TEXT,
    provenance TEXT NOT NULL,
    metadata_contract TEXT NOT NULL,
    UNIQUE (environment, logical_call_id, attempt_index)
);
CREATE INDEX idx_operational_physical_calls_started
    ON operational_physical_calls(environment, scope_key, started_at, call_id);
CREATE INDEX idx_operational_physical_calls_root_kind
    ON operational_physical_calls(analysis_run_id, call_kind);
CREATE INDEX idx_operational_physical_calls_provider_model
    ON operational_physical_calls(provider, model, started_at);

CREATE TABLE operational_backfill_items (
    item_key TEXT PRIMARY KEY,
    source_digest TEXT,
    status TEXT NOT NULL,
    reason_code TEXT,
    updated_at TIMESTAMPTZ NOT NULL
);
