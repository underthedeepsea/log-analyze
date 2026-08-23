BEGIN IMMEDIATE;

CREATE UNIQUE INDEX IF NOT EXISTS uq_feature_candidates_candidate_job
    ON feature_candidates(candidate_id, job_id);

DROP INDEX IF EXISTS idx_feature_candidate_feedback_candidate_time;
DROP INDEX IF EXISTS idx_feature_candidate_feedback_job_time;
ALTER TABLE feature_candidate_feedback RENAME TO feature_candidate_feedback_legacy;

CREATE TABLE feature_candidate_feedback (
    feedback_id TEXT PRIMARY KEY,
    candidate_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('approved', 'rejected')),
    reason_code TEXT NOT NULL CHECK (length(reason_code) BETWEEN 1 AND 120),
    note TEXT NOT NULL DEFAULT '' CHECK (length(note) <= 2000),
    actor TEXT NOT NULL CHECK (length(actor) BETWEEN 1 AND 255),
    request_id TEXT NOT NULL CHECK (length(request_id) BETWEEN 1 AND 255),
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 255),
    created_at TEXT NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'continuous_learning_feedback_v1',
    UNIQUE (candidate_id, idempotency_key),
    FOREIGN KEY (candidate_id, job_id)
        REFERENCES feature_candidates(candidate_id, job_id) ON DELETE RESTRICT
);

INSERT INTO feature_candidate_feedback(
    feedback_id, candidate_id, job_id, outcome, reason_code, note, actor,
    request_id, idempotency_key, created_at, schema_version
)
SELECT
    feedback_id, candidate_id, job_id, outcome, reason_code, note, actor,
    request_id, idempotency_key, created_at, schema_version
FROM feature_candidate_feedback_legacy;

DROP TABLE feature_candidate_feedback_legacy;

CREATE INDEX IF NOT EXISTS idx_feature_candidate_feedback_candidate_time
    ON feature_candidate_feedback(candidate_id, created_at DESC, feedback_id DESC);
CREATE INDEX IF NOT EXISTS idx_feature_candidate_feedback_job_time
    ON feature_candidate_feedback(job_id, created_at DESC, feedback_id DESC);

COMMIT;
