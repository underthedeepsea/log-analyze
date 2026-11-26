-- Expression indexes persist the read projections alongside the source facts.
CREATE INDEX idx_streaming_windows_risk_time ON streaming_batch_windows(
    julianday(CASE WHEN json_valid(window_json) THEN json_extract(window_json, '$.last_seen') END),
    task_id, window_id, item_index
);
CREATE INDEX idx_streaming_windows_risk_node ON streaming_batch_windows(
    CASE WHEN json_valid(window_json) THEN json_extract(window_json, '$.entity_type') END,
    CASE WHEN json_valid(window_json) THEN json_extract(window_json, '$.cluster') END,
    CASE WHEN json_valid(window_json) THEN json_extract(window_json, '$.entity_id') END
);
CREATE INDEX idx_streaming_windows_risk_physical ON streaming_batch_windows(
    CASE WHEN json_valid(window_json) THEN json_array(
        json_extract(window_json, '$.source_namespace'),
        COALESCE(json_extract(window_json, '$.source_batch_id'), window_id),
        json_extract(window_json, '$.window_start'),
        COALESCE(json_extract(window_json, '$.source_item_id'),json_extract(window_json, '$.raw_log_id'),
            json_extract(window_json, '$.template_instance_hash'))
    ) END
);
CREATE INDEX idx_observations_node_time ON server_risk_observations(
    json_extract(sanitized_payload_json, '$.identity.cluster'),
    json_extract(sanitized_payload_json, '$.identity.node_id'),
    interval_end, subject_key
);
CREATE INDEX idx_observations_overview_time ON server_risk_observations(interval_end, interval_start)
    WHERE kind='adverse';
CREATE INDEX idx_observations_subject_time ON server_risk_observations(subject_key, interval_end, source_fact_key);
CREATE INDEX idx_correlation_candidates ON multi_source_observations(cluster, julianday(window_start), observation_id);
CREATE INDEX idx_node_risk_overview_time ON node_risk_events(julianday(last_seen), cluster, node_id)
    WHERE derivation_status='current';
CREATE INDEX idx_feature_jobs_runtime_order ON feature_jobs(updated_at DESC, job_id DESC);
CREATE INDEX idx_input_jobs_runtime_order ON input_jobs(updated_at DESC, input_job_id DESC);
CREATE INDEX idx_streaming_tasks_runtime_order ON streaming_tasks(updated_at DESC, task_id DESC);
CREATE INDEX idx_benchmark_runs_runtime_order ON benchmark_runs(updated_at DESC, run_id DESC);
CREATE INDEX idx_replay_runs_runtime_order ON replay_runs(updated_at DESC, replay_id DESC);

CREATE INDEX idx_observations_subject_replay ON server_risk_observations(
    subject_key,
    (CASE WHEN length(interval_start)=25 THEN substr(interval_start,1,19)||'.000000+00:00' ELSE interval_start END),
    json_extract(sanitized_payload_json,'$.source_sequence'), observation_id
);

ALTER TABLE server_risk_episodes ADD COLUMN cluster TEXT;
ALTER TABLE server_risk_episodes ADD COLUMN node_id TEXT;
ALTER TABLE server_risk_decisions ADD COLUMN cluster TEXT;
ALTER TABLE server_risk_decisions ADD COLUMN node_id TEXT;

UPDATE server_risk_episodes
SET cluster=CASE WHEN json_type((CASE WHEN json_valid(object_identity_json) THEN object_identity_json END),'$.cluster')='text' THEN json_extract((CASE WHEN json_valid(object_identity_json) THEN object_identity_json END),'$.cluster') END, node_id=CASE WHEN json_type((CASE WHEN json_valid(object_identity_json) THEN object_identity_json END),'$.node_id')='text' THEN json_extract((CASE WHEN json_valid(object_identity_json) THEN object_identity_json END),'$.node_id') END
WHERE CASE WHEN json_type((CASE WHEN json_valid(object_identity_json) THEN object_identity_json END),'$.cluster')='text' THEN json_extract((CASE WHEN json_valid(object_identity_json) THEN object_identity_json END),'$.cluster') END<>'' AND CASE WHEN json_type((CASE WHEN json_valid(object_identity_json) THEN object_identity_json END),'$.node_id')='text' THEN json_extract((CASE WHEN json_valid(object_identity_json) THEN object_identity_json END),'$.node_id') END<>'';

-- A dated decision is scoped only when every reference resolves to one
-- subject/node and agrees with any referenced episode. A bound episode
-- alone is enough only when the evidence array is valid and empty.
WITH evidence AS (
    SELECT d.decision_id,d.episode_id,d.subject_key,CASE WHEN json_valid(d.evidence_refs_json) THEN d.evidence_refs_json END AS refs
    FROM server_risk_decisions d
), proven AS (
    SELECT d.decision_id,
        CASE WHEN COUNT(r.key)=0 THEN e.cluster ELSE MIN(CASE WHEN json_type((o.sanitized_payload_json),'$.identity.cluster')='text' THEN json_extract((o.sanitized_payload_json),'$.identity.cluster') END) END AS cluster,
        CASE WHEN COUNT(r.key)=0 THEN e.node_id ELSE MIN(CASE WHEN json_type((o.sanitized_payload_json),'$.identity.node_id')='text' THEN json_extract((o.sanitized_payload_json),'$.identity.node_id') END) END AS node_id
    FROM evidence d
    LEFT JOIN server_risk_episodes e ON e.episode_id=d.episode_id AND e.subject_key=d.subject_key
    LEFT JOIN json_each(CASE WHEN json_type(d.refs)='array' THEN d.refs END) r ON json_type(d.refs)='array'
    LEFT JOIN server_risk_observations o ON o.observation_id=r.value
        AND o.subject_key=d.subject_key AND r.type='text'
    WHERE json_type(d.refs)='array'
    GROUP BY d.decision_id,d.episode_id,e.cluster,e.node_id
    HAVING (COUNT(r.key)=0 AND e.cluster IS NOT NULL AND e.node_id IS NOT NULL)
        OR (
            COUNT(r.key)>0
            AND COUNT(r.key)=COUNT(CASE WHEN json_type((o.sanitized_payload_json),'$.identity.cluster')='text' THEN json_extract((o.sanitized_payload_json),'$.identity.cluster') END)
            AND COUNT(r.key)=COUNT(CASE WHEN json_type((o.sanitized_payload_json),'$.identity.node_id')='text' THEN json_extract((o.sanitized_payload_json),'$.identity.node_id') END)
            AND MIN(CASE WHEN json_type((o.sanitized_payload_json),'$.identity.cluster')='text' THEN json_extract((o.sanitized_payload_json),'$.identity.cluster') END)=MAX(CASE WHEN json_type((o.sanitized_payload_json),'$.identity.cluster')='text' THEN json_extract((o.sanitized_payload_json),'$.identity.cluster') END)
            AND MIN(CASE WHEN json_type((o.sanitized_payload_json),'$.identity.node_id')='text' THEN json_extract((o.sanitized_payload_json),'$.identity.node_id') END)=MAX(CASE WHEN json_type((o.sanitized_payload_json),'$.identity.node_id')='text' THEN json_extract((o.sanitized_payload_json),'$.identity.node_id') END)
            AND MIN(CASE WHEN json_type((o.sanitized_payload_json),'$.identity.cluster')='text' THEN json_extract((o.sanitized_payload_json),'$.identity.cluster') END)<>''
            AND MIN(CASE WHEN json_type((o.sanitized_payload_json),'$.identity.node_id')='text' THEN json_extract((o.sanitized_payload_json),'$.identity.node_id') END)<>''
            AND (d.episode_id IS NULL OR (
                MIN(CASE WHEN json_type((o.sanitized_payload_json),'$.identity.cluster')='text' THEN json_extract((o.sanitized_payload_json),'$.identity.cluster') END)=e.cluster AND MIN(CASE WHEN json_type((o.sanitized_payload_json),'$.identity.node_id')='text' THEN json_extract((o.sanitized_payload_json),'$.identity.node_id') END)=e.node_id
            ))
        )
)
UPDATE server_risk_decisions SET cluster=proven.cluster,node_id=proven.node_id
FROM proven WHERE server_risk_decisions.decision_id=proven.decision_id;

DROP INDEX uq_server_risk_open_subject;
CREATE UNIQUE INDEX uq_server_risk_open_node_subject
    ON server_risk_episodes(cluster,node_id,subject_key)
    WHERE lifecycle='open' AND cluster IS NOT NULL AND node_id IS NOT NULL;
CREATE INDEX idx_server_risk_episode_scope ON server_risk_episodes(cluster,node_id,subject_key);
CREATE INDEX idx_server_risk_decision_replay
    ON server_risk_decisions(cluster,node_id,subject_key,effective_at,decision_id);
CREATE INDEX idx_server_risk_decision_node_time
    ON server_risk_decisions(cluster,node_id,effective_at,decision_id);

CREATE INDEX idx_streaming_windows_physical_lookup ON streaming_batch_windows(
    logrisk_window_physical(window_json,window_id),task_id,window_id,item_index
);

CREATE TABLE server_risk_current_checkpoints (
    cluster TEXT NOT NULL, node_id TEXT NOT NULL, subject_key TEXT NOT NULL,
    policy_digest TEXT NOT NULL, as_of TEXT NOT NULL,
    start_key TEXT NOT NULL, sequence_key BIGINT NOT NULL, observation_id TEXT NOT NULL,
    reducer_json TEXT NOT NULL, invalidated INTEGER NOT NULL DEFAULT 0 CHECK(invalidated IN (0,1)),
    PRIMARY KEY(cluster,node_id,subject_key)
);

CREATE INDEX idx_server_risk_observation_batch_digest ON server_risk_observations(batch_digest);

CREATE TRIGGER server_risk_invalidate_checkpoint_after_receipt_update
AFTER UPDATE OF input_revision,status ON server_risk_projection_state
WHEN OLD.input_namespace LIKE 'batch:%' AND OLD.status='complete'
    AND (NEW.status<>OLD.status OR NEW.input_revision<>OLD.input_revision)
BEGIN
    UPDATE server_risk_current_checkpoints SET invalidated=1
    WHERE EXISTS (
        SELECT 1 FROM server_risk_observations o WHERE o.batch_digest=OLD.input_revision
            AND o.subject_key=server_risk_current_checkpoints.subject_key
            AND json_extract(o.sanitized_payload_json,'$.identity.cluster')=server_risk_current_checkpoints.cluster
            AND json_extract(o.sanitized_payload_json,'$.identity.node_id')=server_risk_current_checkpoints.node_id
    );
END;

CREATE TRIGGER server_risk_invalidate_checkpoint_after_receipt_delete
AFTER DELETE ON server_risk_projection_state
WHEN OLD.input_namespace LIKE 'batch:%' AND OLD.status='complete'
BEGIN
    UPDATE server_risk_current_checkpoints SET invalidated=1
    WHERE EXISTS (
        SELECT 1 FROM server_risk_observations o WHERE o.batch_digest=OLD.input_revision
            AND o.subject_key=server_risk_current_checkpoints.subject_key
            AND json_extract(o.sanitized_payload_json,'$.identity.cluster')=server_risk_current_checkpoints.cluster
            AND json_extract(o.sanitized_payload_json,'$.identity.node_id')=server_risk_current_checkpoints.node_id
    );
END;

CREATE INDEX idx_server_risk_pending_commit_due
    ON server_risk_projection_state(evaluated_through,input_namespace)
    WHERE status='pending' AND input_namespace LIKE 'assessment-commit:%';
