ALTER TABLE feature_candidates ADD COLUMN approval_key TEXT;
ALTER TABLE feature_candidates ADD COLUMN problem_code TEXT;
ALTER TABLE feature_candidates ADD COLUMN approval_group_id TEXT;
ALTER TABLE feature_candidates ADD COLUMN resolved_rule_id TEXT;
ALTER TABLE feature_candidates ADD COLUMN resolution_type TEXT;

ALTER TABLE approved_rules ADD COLUMN problem_code TEXT;
ALTER TABLE approved_rules ADD COLUMN approval_key TEXT;

CREATE TABLE approval_groups (
    approval_group_id TEXT PRIMARY KEY,
    approval_key TEXT NOT NULL UNIQUE,
    problem_code TEXT,
    feature_type TEXT NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    summary TEXT NOT NULL DEFAULT '',
    importance TEXT NOT NULL DEFAULT 'medium',
    status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'rejected', 'auto_resolved', 'superseded')),
    rule_id TEXT REFERENCES approved_rules(rule_id) ON DELETE SET NULL,
    first_seen TEXT,
    last_seen TEXT,
    occurrence_count BIGINT NOT NULL DEFAULT 0,
    affected_entity_count BIGINT NOT NULL DEFAULT 0,
    candidate_count BIGINT NOT NULL DEFAULT 0,
    group_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'approval_group_v1'
);
CREATE INDEX idx_approval_groups_status_updated ON approval_groups(status, updated_at DESC);
CREATE INDEX idx_approval_groups_problem_code ON approval_groups(problem_code);

CREATE TABLE approval_group_candidates (
    approval_group_id TEXT NOT NULL REFERENCES approval_groups(approval_group_id) ON DELETE CASCADE,
    candidate_id TEXT NOT NULL UNIQUE,
    job_id TEXT,
    entity_id TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (approval_group_id, candidate_id)
);
CREATE INDEX idx_approval_group_candidates_group ON approval_group_candidates(approval_group_id, created_at);

CREATE INDEX idx_feature_candidates_approval_key ON feature_candidates(approval_key);
CREATE INDEX idx_feature_candidates_problem_code ON feature_candidates(problem_code);
CREATE UNIQUE INDEX idx_approved_rules_approval_key ON approved_rules(approval_key) WHERE approval_key IS NOT NULL;
