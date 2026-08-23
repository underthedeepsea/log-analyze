CREATE UNIQUE INDEX IF NOT EXISTS uq_feature_candidates_candidate_job
    ON feature_candidates(candidate_id, job_id);

ALTER TABLE feature_candidate_feedback
    DROP CONSTRAINT IF EXISTS feature_candidate_feedback_candidate_id_fkey,
    DROP CONSTRAINT IF EXISTS feature_candidate_feedback_job_id_fkey,
    DROP CONSTRAINT IF EXISTS feature_candidate_feedback_candidate_job_fkey,
    ADD CONSTRAINT feature_candidate_feedback_candidate_job_fkey
        FOREIGN KEY (candidate_id, job_id)
        REFERENCES feature_candidates(candidate_id, job_id) ON DELETE RESTRICT;
