CREATE TABLE approval_candidate_projection (
    candidate_id TEXT PRIMARY KEY REFERENCES feature_candidates(candidate_id) ON DELETE CASCADE,
    review_key TEXT NOT NULL,
    status TEXT NOT NULL,
    semantic_safe INTEGER NOT NULL,
    ambiguous INTEGER NOT NULL,
    importance_rank INTEGER NOT NULL,
    risk_score REAL NOT NULL,
    created_at TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    occurrence_count INTEGER NOT NULL,
    first_seen TEXT,
    last_seen TEXT,
    group_json TEXT NOT NULL
);
CREATE INDEX idx_approval_candidate_projection_page ON approval_candidate_projection(status,review_key,candidate_id);
CREATE TABLE approval_projection_state (
    singleton INTEGER PRIMARY KEY,
    generation TEXT NOT NULL
);
INSERT INTO approval_projection_state(singleton,generation) VALUES (1,'canonical-v1');
