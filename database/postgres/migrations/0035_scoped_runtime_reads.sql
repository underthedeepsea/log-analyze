-- Text timestamps in sanitized facts are ISO strings, independent of session timezone.
CREATE FUNCTION logrisk_iso_time(value TEXT) RETURNS TIMESTAMPTZ
LANGUAGE plpgsql IMMUTABLE STRICT
SET TimeZone = 'UTC'
SET DateStyle = 'ISO, YMD'
AS $$
BEGIN
    IF value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}' THEN
        RETURN NULL;
    END IF;
    RETURN value::timestamptz;
EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow OR invalid_parameter_value THEN
    RETURN NULL;
END;
$$;
CREATE INDEX idx_streaming_windows_risk_time ON streaming_batch_windows(
    logrisk_iso_time(window_json->>'last_seen'), task_id, window_id, item_index
);
CREATE INDEX idx_streaming_windows_risk_node ON streaming_batch_windows(
    (window_json->>'entity_type'), (window_json->>'cluster'), (window_json->>'entity_id')
);
CREATE INDEX idx_streaming_windows_risk_physical ON streaming_batch_windows((
    ARRAY[
        window_json->>'source_namespace',
        COALESCE(window_json->>'source_batch_id', window_id),
        window_json->>'window_start',
        COALESCE(window_json->>'source_item_id',window_json->>'raw_log_id',window_json->>'template_instance_hash')
    ]
));
CREATE INDEX idx_observations_node_time ON server_risk_observations(
    ((sanitized_payload_json::jsonb)->'identity'->>'cluster'),
    ((sanitized_payload_json::jsonb)->'identity'->>'node_id'),
    interval_end, subject_key
);
CREATE INDEX idx_observations_overview_time ON server_risk_observations(interval_end, interval_start)
    WHERE kind='adverse';
CREATE INDEX idx_observations_subject_time ON server_risk_observations(subject_key, interval_end, source_fact_key);
CREATE INDEX idx_correlation_candidates ON multi_source_observations(cluster, window_start, observation_id);
CREATE INDEX idx_node_risk_overview_time ON node_risk_events(last_seen, cluster, node_id)
    WHERE derivation_status='current';
CREATE INDEX idx_feature_jobs_runtime_order ON feature_jobs(updated_at DESC, job_id DESC);
CREATE INDEX idx_input_jobs_runtime_order ON input_jobs(updated_at DESC, input_job_id DESC);
CREATE INDEX idx_streaming_tasks_runtime_order ON streaming_tasks(updated_at DESC, task_id DESC);
CREATE INDEX idx_benchmark_runs_runtime_order ON benchmark_runs(updated_at DESC, run_id DESC);
CREATE INDEX idx_replay_runs_runtime_order ON replay_runs(updated_at DESC, replay_id DESC);

CREATE INDEX idx_observations_subject_replay ON server_risk_observations(
    subject_key,
    (CASE WHEN length(interval_start)=25 THEN substr(interval_start,1,19)||'.000000+00:00' ELSE interval_start END),
    ((sanitized_payload_json::jsonb->>'source_sequence')::bigint), observation_id
);

CREATE FUNCTION logrisk_migration_safe_json(value TEXT) RETURNS JSONB
LANGUAGE plpgsql IMMUTABLE STRICT AS $$
BEGIN
    RETURN value::jsonb;
EXCEPTION WHEN invalid_text_representation THEN
    RETURN NULL;
END;
$$;

ALTER TABLE server_risk_episodes ADD COLUMN cluster TEXT;
ALTER TABLE server_risk_episodes ADD COLUMN node_id TEXT;
ALTER TABLE server_risk_decisions ADD COLUMN cluster TEXT;
ALTER TABLE server_risk_decisions ADD COLUMN node_id TEXT;

UPDATE server_risk_episodes
SET cluster=CASE WHEN jsonb_typeof((logrisk_migration_safe_json(object_identity_json))->'cluster')='string' THEN (logrisk_migration_safe_json(object_identity_json))->>'cluster' END, node_id=CASE WHEN jsonb_typeof((logrisk_migration_safe_json(object_identity_json))->'node_id')='string' THEN (logrisk_migration_safe_json(object_identity_json))->>'node_id' END
WHERE CASE WHEN jsonb_typeof((logrisk_migration_safe_json(object_identity_json))->'cluster')='string' THEN (logrisk_migration_safe_json(object_identity_json))->>'cluster' END<>'' AND CASE WHEN jsonb_typeof((logrisk_migration_safe_json(object_identity_json))->'node_id')='string' THEN (logrisk_migration_safe_json(object_identity_json))->>'node_id' END<>'';

-- A dated decision is scoped only when every reference resolves to one
-- subject/node and agrees with any referenced episode. A bound episode
-- alone is enough only when the evidence array is valid and empty.
WITH evidence AS (
    SELECT d.decision_id,d.episode_id,d.subject_key,logrisk_migration_safe_json(d.evidence_refs_json) AS refs
    FROM server_risk_decisions d
), proven AS (
    SELECT d.decision_id,
        CASE WHEN COUNT(r.ordinality)=0 THEN e.cluster ELSE MIN(CASE WHEN jsonb_typeof((o.sanitized_payload_json::jsonb->'identity')->'cluster')='string' THEN (o.sanitized_payload_json::jsonb->'identity')->>'cluster' END) END AS cluster,
        CASE WHEN COUNT(r.ordinality)=0 THEN e.node_id ELSE MIN(CASE WHEN jsonb_typeof((o.sanitized_payload_json::jsonb->'identity')->'node_id')='string' THEN (o.sanitized_payload_json::jsonb->'identity')->>'node_id' END) END AS node_id
    FROM evidence d
    LEFT JOIN server_risk_episodes e ON e.episode_id=d.episode_id AND e.subject_key=d.subject_key
    LEFT JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(d.refs)='array' THEN d.refs END) WITH ORDINALITY AS r(value,ordinality) ON jsonb_typeof(d.refs)='array'
    LEFT JOIN server_risk_observations o ON o.observation_id=r.value#>>'{}'
        AND o.subject_key=d.subject_key AND CASE WHEN jsonb_typeof(r.value)='string' THEN 'text' END='text'
    WHERE jsonb_typeof(d.refs)='array'
    GROUP BY d.decision_id,d.episode_id,e.cluster,e.node_id
    HAVING (COUNT(r.ordinality)=0 AND e.cluster IS NOT NULL AND e.node_id IS NOT NULL)
        OR (
            COUNT(r.ordinality)>0
            AND COUNT(r.ordinality)=COUNT(CASE WHEN jsonb_typeof((o.sanitized_payload_json::jsonb->'identity')->'cluster')='string' THEN (o.sanitized_payload_json::jsonb->'identity')->>'cluster' END)
            AND COUNT(r.ordinality)=COUNT(CASE WHEN jsonb_typeof((o.sanitized_payload_json::jsonb->'identity')->'node_id')='string' THEN (o.sanitized_payload_json::jsonb->'identity')->>'node_id' END)
            AND MIN(CASE WHEN jsonb_typeof((o.sanitized_payload_json::jsonb->'identity')->'cluster')='string' THEN (o.sanitized_payload_json::jsonb->'identity')->>'cluster' END)=MAX(CASE WHEN jsonb_typeof((o.sanitized_payload_json::jsonb->'identity')->'cluster')='string' THEN (o.sanitized_payload_json::jsonb->'identity')->>'cluster' END)
            AND MIN(CASE WHEN jsonb_typeof((o.sanitized_payload_json::jsonb->'identity')->'node_id')='string' THEN (o.sanitized_payload_json::jsonb->'identity')->>'node_id' END)=MAX(CASE WHEN jsonb_typeof((o.sanitized_payload_json::jsonb->'identity')->'node_id')='string' THEN (o.sanitized_payload_json::jsonb->'identity')->>'node_id' END)
            AND MIN(CASE WHEN jsonb_typeof((o.sanitized_payload_json::jsonb->'identity')->'cluster')='string' THEN (o.sanitized_payload_json::jsonb->'identity')->>'cluster' END)<>''
            AND MIN(CASE WHEN jsonb_typeof((o.sanitized_payload_json::jsonb->'identity')->'node_id')='string' THEN (o.sanitized_payload_json::jsonb->'identity')->>'node_id' END)<>''
            AND (d.episode_id IS NULL OR (
                MIN(CASE WHEN jsonb_typeof((o.sanitized_payload_json::jsonb->'identity')->'cluster')='string' THEN (o.sanitized_payload_json::jsonb->'identity')->>'cluster' END)=e.cluster AND MIN(CASE WHEN jsonb_typeof((o.sanitized_payload_json::jsonb->'identity')->'node_id')='string' THEN (o.sanitized_payload_json::jsonb->'identity')->>'node_id' END)=e.node_id
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

DROP FUNCTION logrisk_migration_safe_json(TEXT);

CREATE FUNCTION logrisk_canonical_window_key(value JSONB) RETURNS TEXT
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT CASE jsonb_typeof(value)
        WHEN 'array' THEN '[' || COALESCE((
            SELECT string_agg(logrisk_canonical_window_key(item),',' ORDER BY position)
            FROM jsonb_array_elements(value) WITH ORDINALITY AS a(item,position)
        ),'') || ']'
        WHEN 'object' THEN '{' || COALESCE((
            SELECT string_agg(to_jsonb(key)::text || ':' || logrisk_canonical_window_key(item),',' ORDER BY key COLLATE "C")
            FROM jsonb_each(value) AS a(key,item)
        ),'') || '}'
        ELSE value::text
    END
$$;

CREATE FUNCTION logrisk_window_physical(value JSONB, commit_id TEXT) RETURNS TEXT
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE AS $$
    WITH keys AS (
        SELECT value->'source_namespace' AS ns, value->'window_start' AS stamp,
            CASE WHEN value->'source_batch_id' NOT IN ('null','""','false','0','[]','{}')
                 THEN value->'source_batch_id' ELSE to_jsonb(commit_id) END AS batch,
            COALESCE(
                CASE WHEN value->'source_item_id' NOT IN ('null','""','false','0','[]','{}') THEN value->'source_item_id' END,
                CASE WHEN value->'raw_log_id' NOT IN ('null','""','false','0','[]','{}') THEN value->'raw_log_id' END,
                NULLIF(value->'template_instance_hash','null')
            ) AS item
    )
    SELECT encode(sha256(convert_to(logrisk_canonical_window_key(jsonb_build_array(ns,batch,stamp,item)), 'UTF8')), 'hex')
    FROM keys WHERE item IS NOT NULL AND ns NOT IN ('null','""','false','0','[]','{}')
        AND stamp NOT IN ('null','""','false','0','[]','{}')
$$;

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

CREATE FUNCTION logrisk_invalidate_current_checkpoint() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.input_namespace NOT LIKE 'batch:%' OR OLD.status<>'complete' THEN
        RETURN NULL;
    END IF;
    IF TG_OP='UPDATE' AND NEW.status=OLD.status AND NEW.input_revision=OLD.input_revision THEN
        RETURN NULL;
    END IF;
    UPDATE server_risk_current_checkpoints c SET invalidated=1
    WHERE EXISTS (
        SELECT 1 FROM server_risk_observations o WHERE o.batch_digest=OLD.input_revision
            AND o.subject_key=c.subject_key
            AND o.sanitized_payload_json::jsonb->'identity'->>'cluster'=c.cluster
            AND o.sanitized_payload_json::jsonb->'identity'->>'node_id'=c.node_id
    );
    RETURN NULL;
END;
$$;

CREATE TRIGGER server_risk_invalidate_checkpoint_after_receipt_change
AFTER UPDATE OF input_revision,status OR DELETE ON server_risk_projection_state
FOR EACH ROW EXECUTE FUNCTION logrisk_invalidate_current_checkpoint();

CREATE INDEX idx_server_risk_pending_commit_due
    ON server_risk_projection_state(evaluated_through,input_namespace)
    WHERE status='pending' AND input_namespace LIKE 'assessment-commit:%';
