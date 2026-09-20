CREATE TABLE streaming_result_generations (
 task_id TEXT NOT NULL REFERENCES streaming_tasks(task_id) ON DELETE CASCADE,
 generation TEXT NOT NULL, status TEXT NOT NULL, rules_hash TEXT NOT NULL,
 frontier_json TEXT NOT NULL, after_window TEXT NOT NULL DEFAULT '', after_item INTEGER NOT NULL DEFAULT -1,
 summary_json TEXT NOT NULL DEFAULT '{}', PRIMARY KEY(task_id,generation)
);
CREATE TABLE streaming_result_windows (
 task_id TEXT NOT NULL, generation TEXT NOT NULL, window_key TEXT NOT NULL, canonical_key TEXT NOT NULL,
 entity_key TEXT NOT NULL, count INTEGER NOT NULL, score REAL NOT NULL, core_json TEXT NOT NULL,
 PRIMARY KEY(task_id,generation,window_key),
 FOREIGN KEY(task_id,generation) REFERENCES streaming_result_generations(task_id,generation) ON DELETE CASCADE
);
CREATE INDEX idx_streaming_result_windows_entity ON streaming_result_windows(task_id,generation,entity_key,score,window_key);
CREATE TABLE streaming_result_window_members (
 task_id TEXT NOT NULL,generation TEXT NOT NULL,window_key TEXT NOT NULL,kind TEXT NOT NULL,
 member_key TEXT NOT NULL,value_json TEXT NOT NULL,count INTEGER NOT NULL,
 PRIMARY KEY(task_id,generation,window_key,kind,member_key),
 FOREIGN KEY(task_id,generation,window_key) REFERENCES streaming_result_windows(task_id,generation,window_key) ON DELETE CASCADE
);
CREATE TABLE streaming_result_entities (
 task_id TEXT NOT NULL,generation TEXT NOT NULL,entity_key TEXT NOT NULL,score REAL NOT NULL,
 level TEXT NOT NULL,entity_id TEXT NOT NULL,entity_json TEXT NOT NULL,
 PRIMARY KEY(task_id,generation,entity_key),
 FOREIGN KEY(task_id,generation) REFERENCES streaming_result_generations(task_id,generation) ON DELETE CASCADE
);
CREATE INDEX idx_streaming_result_entities_page ON streaming_result_entities(task_id,generation,entity_key);
CREATE INDEX idx_streaming_result_entities_identity ON streaming_result_entities(task_id,generation,entity_id,entity_key);
CREATE TABLE streaming_feature_entities (
 task_id TEXT NOT NULL,generation TEXT NOT NULL,entity_id TEXT NOT NULL,source_json TEXT NOT NULL,
 PRIMARY KEY(task_id,generation,entity_id),
 FOREIGN KEY(task_id,generation) REFERENCES streaming_result_generations(task_id,generation) ON DELETE CASCADE
);
