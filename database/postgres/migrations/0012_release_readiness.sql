CREATE TABLE IF NOT EXISTS release_validations (
    validation_id TEXT PRIMARY KEY,
    target_version TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('passed', 'warning', 'blocked')),
    summary_json JSONB NOT NULL,
    schema_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_release_validation_created
    ON release_validations(created_at DESC, validation_id DESC);

CREATE TABLE IF NOT EXISTS release_validation_checks (
    validation_id TEXT NOT NULL REFERENCES release_validations(validation_id) ON DELETE CASCADE,
    check_id TEXT NOT NULL,
    position BIGINT NOT NULL CHECK (position >= 0),
    title TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('passed', 'warning', 'blocked')),
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    evidence_json JSONB NOT NULL,
    schema_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (validation_id, check_id)
);

CREATE INDEX IF NOT EXISTS idx_release_validation_check_status
    ON release_validation_checks(validation_id, status, position);
