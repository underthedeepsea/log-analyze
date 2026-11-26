ALTER TABLE feature_job_entities ADD COLUMN entity_key TEXT;
UPDATE feature_job_entities SET entity_key =
    'e.' || encode(convert_to(entity_id, 'UTF8'), 'hex') || '.' ||
    encode(convert_to(COALESCE(entity_json->>'cluster', ''), 'UTF8'), 'hex') || '.' ||
    encode(convert_to(COALESCE(entity_json->>'entity_type', ''), 'UTF8'), 'hex');
ALTER TABLE feature_job_entities ALTER COLUMN entity_key SET NOT NULL;
ALTER TABLE feature_job_entities DROP CONSTRAINT feature_job_entities_pkey;
ALTER TABLE feature_job_entities ADD PRIMARY KEY (job_id, entity_key);
CREATE INDEX idx_feature_job_entities_id ON feature_job_entities(job_id, entity_id, entity_key);

ALTER TABLE streaming_result_entities ADD COLUMN identity_key TEXT NOT NULL DEFAULT '';
UPDATE streaming_result_entities SET identity_key =
    'e.' || encode(convert_to(entity_id, 'UTF8'), 'hex') || '.' ||
    encode(convert_to(COALESCE(CAST(entity_json AS jsonb)->>'cluster', ''), 'UTF8'), 'hex') || '.' ||
    encode(convert_to(COALESCE(CAST(entity_json AS jsonb)->>'entity_type', ''), 'UTF8'), 'hex');
CREATE INDEX idx_streaming_result_entities_scoped
    ON streaming_result_entities(task_id, generation, identity_key, entity_key);
CREATE TABLE streaming_scoped_feature_entities (
    task_id TEXT NOT NULL, generation TEXT NOT NULL,
    identity_key TEXT NOT NULL, source_json TEXT NOT NULL,
    PRIMARY KEY (task_id, generation, identity_key),
    FOREIGN KEY (task_id, generation) REFERENCES streaming_result_generations(task_id,generation) ON DELETE CASCADE
);

ALTER TABLE feature_job_entities ADD COLUMN scope_ambiguous BOOLEAN NOT NULL DEFAULT FALSE;
UPDATE feature_job_entities AS e SET scope_ambiguous=TRUE
WHERE EXISTS (
    SELECT 1 FROM jsonb_array_elements(
        CASE WHEN jsonb_typeof(e.entity_json #> '{source,top_templates}')='array'
        THEN e.entity_json #> '{source,top_templates}' ELSE '[]'::jsonb END
    ) AS template(value)
    WHERE template.value->>'cluster' <> COALESCE(e.entity_json->>'cluster', '')
    OR template.value->>'entity_type' <> COALESCE(e.entity_json->>'entity_type', '')
    OR template.value->>'entity_id' <> e.entity_id
) OR (
    e.entity_json #>> '{source,_source_ref,entity_key}' IS NULL
    AND EXISTS (
        SELECT 1 FROM streaming_result_entities AS source_entity
        WHERE source_entity.task_id=e.entity_json #>> '{source,_source_ref,task_id}'
        AND source_entity.generation=e.entity_json #>> '{source,_source_ref,generation}'
        AND source_entity.entity_id=e.entity_id
        GROUP BY source_entity.task_id, source_entity.generation, source_entity.entity_id
        HAVING COUNT(DISTINCT source_entity.identity_key) > 1
    )
);

ALTER TABLE feature_job_entities ADD COLUMN scope_verified BOOLEAN NOT NULL DEFAULT FALSE;
UPDATE feature_job_entities AS e SET scope_verified=TRUE
WHERE e.scope_ambiguous=FALSE AND EXISTS (
    SELECT 1 FROM streaming_result_entities AS source_entity
    JOIN streaming_result_generations AS generation
      ON generation.task_id=source_entity.task_id AND generation.generation=source_entity.generation
    WHERE source_entity.task_id=e.entity_json #>> '{source,_source_ref,task_id}'
    AND source_entity.generation=e.entity_json #>> '{source,_source_ref,generation}'
    AND source_entity.identity_key=e.entity_key
    AND source_entity.entity_id=e.entity_id
    AND generation.status='ready'
);

CREATE TABLE feature_job_candidate_entities (
    job_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    PRIMARY KEY (job_id, candidate_id, entity_key),
    FOREIGN KEY (job_id, entity_key) REFERENCES feature_job_entities(job_id, entity_key) ON DELETE CASCADE
);
CREATE INDEX idx_feature_job_candidate_entities_entity ON feature_job_candidate_entities(job_id, entity_key);
INSERT INTO feature_job_candidate_entities(job_id, candidate_id, entity_key)
SELECT DISTINCT e.job_id, member.value, e.entity_key
FROM feature_job_entities AS e, LATERAL jsonb_array_elements_text(
    CASE WHEN jsonb_typeof(e.entity_json->'feature_ids')='array'
    THEN e.entity_json->'feature_ids' ELSE '[]'::jsonb END
) AS member(value)
WHERE member.value<>'';
