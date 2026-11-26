CREATE TABLE feature_job_entities_scoped (
    job_id TEXT NOT NULL REFERENCES feature_jobs(job_id) ON DELETE CASCADE,
    entity_key TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    status TEXT NOT NULL,
    risk_score REAL,
    entity_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job_id, entity_key)
);
INSERT INTO feature_job_entities_scoped
SELECT job_id,
       'e.' || lower(hex(entity_id)) || '.' ||
       lower(hex(COALESCE(json_extract(entity_json, '$.cluster'), ''))) || '.' ||
       lower(hex(COALESCE(json_extract(entity_json, '$.entity_type'), ''))),
       entity_id, status, risk_score, entity_json, updated_at
FROM feature_job_entities;
DROP TABLE feature_job_entities;
ALTER TABLE feature_job_entities_scoped RENAME TO feature_job_entities;
CREATE INDEX idx_feature_job_entities_id ON feature_job_entities(job_id, entity_id, entity_key);

ALTER TABLE streaming_result_entities ADD COLUMN identity_key TEXT NOT NULL DEFAULT '';
UPDATE streaming_result_entities SET identity_key =
    'e.' || lower(hex(entity_id)) || '.' ||
    lower(hex(COALESCE(json_extract(entity_json, '$.cluster'), ''))) || '.' ||
    lower(hex(COALESCE(json_extract(entity_json, '$.entity_type'), '')));
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
    SELECT 1 FROM json_each(e.entity_json, '$.source.top_templates') AS template
    WHERE template.type='object' AND (
        json_extract(template.value, '$.cluster') <> COALESCE(json_extract(e.entity_json, '$.cluster'), '')
        OR json_extract(template.value, '$.entity_type') <> COALESCE(json_extract(e.entity_json, '$.entity_type'), '')
        OR json_extract(template.value, '$.entity_id') <> e.entity_id
    )
) OR (
    json_extract(e.entity_json, '$.source._source_ref.entity_key') IS NULL
    AND EXISTS (
        SELECT 1 FROM streaming_result_entities AS source_entity
        WHERE source_entity.task_id=json_extract(e.entity_json, '$.source._source_ref.task_id')
        AND source_entity.generation=json_extract(e.entity_json, '$.source._source_ref.generation')
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
    WHERE source_entity.task_id=json_extract(e.entity_json, '$.source._source_ref.task_id')
    AND source_entity.generation=json_extract(e.entity_json, '$.source._source_ref.generation')
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
FROM feature_job_entities AS e, json_each(e.entity_json, '$.feature_ids') AS member
WHERE member.type='text' AND member.value<>'';
