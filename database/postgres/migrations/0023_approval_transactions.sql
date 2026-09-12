CREATE TABLE approval_identity_locks (
    approval_key TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE approval_decisions (
    decision_id TEXT PRIMARY KEY,
    request_key TEXT NOT NULL,
    actor_scope TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    before_version TIMESTAMPTZ,
    result_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE(request_key, actor_scope)
);
CREATE INDEX idx_approval_decisions_candidate ON approval_decisions(candidate_id, created_at);
