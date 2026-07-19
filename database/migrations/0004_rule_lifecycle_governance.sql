ALTER TABLE approved_rules ADD COLUMN status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'disabled', 'under_review', 'deprecated', 'archived'));
ALTER TABLE approved_rules ADD COLUMN current_version INTEGER NOT NULL DEFAULT 1
    CHECK (current_version > 0);
ALTER TABLE approved_rules ADD COLUMN next_review_at TEXT;
ALTER TABLE approved_rules ADD COLUMN schema_version TEXT NOT NULL DEFAULT 'approved_rule_v2';

ALTER TABLE rule_reuse_events ADD COLUMN cluster TEXT;
ALTER TABLE rule_reuse_events ADD COLUMN schema_version TEXT NOT NULL DEFAULT 'rule_reuse_event_v2';

CREATE TABLE IF NOT EXISTS rule_versions (
    rule_id TEXT NOT NULL REFERENCES approved_rules(rule_id) ON DELETE CASCADE,
    version INTEGER NOT NULL CHECK (version > 0),
    rule_json TEXT NOT NULL,
    change_type TEXT NOT NULL,
    change_reason TEXT,
    operator TEXT NOT NULL,
    created_at TEXT NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'rule_version_v1',
    PRIMARY KEY (rule_id, version)
);

CREATE TABLE IF NOT EXISTS rule_feedback (
    feedback_id TEXT PRIMARY KEY,
    rule_id TEXT NOT NULL REFERENCES approved_rules(rule_id) ON DELETE CASCADE,
    outcome TEXT NOT NULL CHECK (outcome IN ('confirmed', 'false_positive')),
    cluster TEXT,
    job_id TEXT,
    entity_id TEXT,
    note TEXT,
    operator TEXT NOT NULL,
    created_at TEXT NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'rule_feedback_v1'
);

CREATE TABLE IF NOT EXISTS rule_audit_events (
    event_id TEXT PRIMARY KEY,
    rule_id TEXT NOT NULL REFERENCES approved_rules(rule_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    from_version INTEGER,
    to_version INTEGER NOT NULL,
    event_json TEXT NOT NULL,
    operator TEXT NOT NULL,
    created_at TEXT NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'rule_audit_event_v1'
);

CREATE INDEX IF NOT EXISTS idx_approved_rules_status ON approved_rules(status);
CREATE INDEX IF NOT EXISTS idx_rule_reuse_rule_time ON rule_reuse_events(rule_id, reused_at DESC);
CREATE INDEX IF NOT EXISTS idx_rule_feedback_rule_time ON rule_feedback(rule_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_rule_audit_rule_time ON rule_audit_events(rule_id, created_at DESC);

INSERT OR IGNORE INTO rule_versions(
    rule_id, version, rule_json, change_type, change_reason, operator, created_at, schema_version
)
SELECT
    rule_id, 1,
    json_set(
        rule_json,
        '$.schema_version', 'approved_rule_v2',
        '$.status', status,
        '$.current_version', current_version,
        '$.created_at', approved_at,
        '$.next_review_at', next_review_at
    ),
    'legacy_import', '由既有批准规则迁移', 'system-migration',
    updated_at, 'rule_version_v1'
FROM approved_rules;
