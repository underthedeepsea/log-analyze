CREATE TABLE IF NOT EXISTS feature_candidate_feedback (
    feedback_id TEXT PRIMARY KEY,
    candidate_id TEXT NOT NULL REFERENCES feature_candidates(candidate_id) ON DELETE RESTRICT,
    job_id TEXT NOT NULL REFERENCES feature_jobs(job_id) ON DELETE RESTRICT,
    outcome TEXT NOT NULL CHECK (outcome IN ('approved', 'rejected')),
    reason_code TEXT NOT NULL CHECK (length(reason_code) BETWEEN 1 AND 120),
    note TEXT NOT NULL DEFAULT '' CHECK (length(note) <= 2000),
    actor TEXT NOT NULL CHECK (length(actor) BETWEEN 1 AND 255),
    request_id TEXT NOT NULL CHECK (length(request_id) BETWEEN 1 AND 255),
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 255),
    created_at TEXT NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'continuous_learning_feedback_v1',
    UNIQUE (candidate_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_feature_candidate_feedback_candidate_time
    ON feature_candidate_feedback(candidate_id, created_at DESC, feedback_id DESC);
CREATE INDEX IF NOT EXISTS idx_feature_candidate_feedback_job_time
    ON feature_candidate_feedback(job_id, created_at DESC, feedback_id DESC);

ALTER TABLE drain_datasets ADD COLUMN dataset_family_id TEXT;
ALTER TABLE drain_datasets ADD COLUMN revision_number INTEGER;
ALTER TABLE drain_datasets ADD COLUMN content_sha256 TEXT;
ALTER TABLE drain_datasets ADD COLUMN parent_dataset_id TEXT REFERENCES drain_datasets(dataset_id);
ALTER TABLE drain_datasets ADD COLUMN lifecycle_status TEXT CHECK (lifecycle_status IS NULL OR lifecycle_status IN ('candidate', 'approved', 'retired'));
ALTER TABLE drain_datasets ADD COLUMN source_type TEXT;
ALTER TABLE drain_datasets ADD COLUMN source_id TEXT;
ALTER TABLE drain_datasets ADD COLUMN source_version TEXT;
ALTER TABLE drain_datasets ADD COLUMN description TEXT;
ALTER TABLE drain_datasets ADD COLUMN split TEXT;
ALTER TABLE drain_datasets ADD COLUMN record_count INTEGER;
ALTER TABLE drain_datasets ADD COLUMN actor TEXT;
ALTER TABLE drain_datasets ADD COLUMN request_id TEXT;
ALTER TABLE drain_datasets ADD COLUMN approved_by TEXT;
ALTER TABLE drain_datasets ADD COLUMN approved_at TEXT;
ALTER TABLE drain_datasets ADD COLUMN schema_version TEXT;

UPDATE drain_datasets
SET dataset_family_id = COALESCE(dataset_family_id, dataset_id),
    revision_number = COALESCE(revision_number, 1),
    lifecycle_status = COALESCE(lifecycle_status, 'approved'),
    source_type = COALESCE(source_type, 'legacy'),
    source_id = COALESCE(source_id, dataset_id),
    source_version = COALESCE(source_version, version),
    description = COALESCE(description, ''),
    split = COALESCE(split, 'validation'),
    record_count = COALESCE(
        record_count,
        CASE
            WHEN json_valid(dataset_json) AND json_type(dataset_json, '$.records') = 'array'
            THEN json_array_length(json_extract(dataset_json, '$.records'))
            ELSE 0
        END
    ),
    schema_version = COALESCE(schema_version, 'drain_dataset_revision_v1');

CREATE UNIQUE INDEX IF NOT EXISTS uq_drain_datasets_family_revision
    ON drain_datasets(dataset_family_id, revision_number);
CREATE INDEX IF NOT EXISTS idx_drain_datasets_family_revision
    ON drain_datasets(dataset_family_id, revision_number DESC);
CREATE INDEX IF NOT EXISTS idx_drain_datasets_status_updated
    ON drain_datasets(lifecycle_status, updated_at DESC, dataset_id DESC);

ALTER TABLE drain_annotations ADD COLUMN dataset_sha256 TEXT;
ALTER TABLE drain_annotations ADD COLUMN dataset_content_sha256 TEXT;
ALTER TABLE drain_reviews ADD COLUMN dataset_id TEXT;
ALTER TABLE drain_reviews ADD COLUMN dataset_sha256 TEXT;
ALTER TABLE drain_reviews ADD COLUMN dataset_content_sha256 TEXT;

CREATE INDEX IF NOT EXISTS idx_drain_annotations_dataset_revision
    ON drain_annotations(dataset_id, dataset_sha256);
CREATE INDEX IF NOT EXISTS idx_drain_reviews_dataset_revision
    ON drain_reviews(dataset_id, dataset_sha256);
