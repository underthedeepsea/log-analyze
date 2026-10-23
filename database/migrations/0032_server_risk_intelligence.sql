CREATE TABLE server_risk_observations (
 observation_id TEXT PRIMARY KEY, source_fact_key TEXT NOT NULL, source_id TEXT NOT NULL,
 source_epoch TEXT NOT NULL, config_revision TEXT NOT NULL, semantic_revision TEXT NOT NULL,
 subject_key TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('adverse','recovery_candidate','state_snapshot','coverage')),
 interval_start TEXT NOT NULL, interval_end TEXT NOT NULL, received_at TEXT NOT NULL,
 mode TEXT NOT NULL CHECK(mode IN ('current','historical')), quality TEXT NOT NULL,
 sanitized_payload_json TEXT NOT NULL, payload_digest TEXT NOT NULL,
 batch_id TEXT NOT NULL, idempotency_key TEXT NOT NULL, batch_digest TEXT NOT NULL,
 UNIQUE(source_fact_key,semantic_revision)
);
CREATE INDEX idx_server_risk_observation_subject ON server_risk_observations(subject_key,interval_start);
CREATE INDEX idx_server_risk_observation_batch ON server_risk_observations(source_id,idempotency_key);
CREATE TABLE server_risk_projection_state (
 input_namespace TEXT PRIMARY KEY, input_revision TEXT NOT NULL, last_fact_ref TEXT,
 evaluated_through TEXT, status TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE server_risk_episodes (
 episode_id TEXT PRIMARY KEY, subject_key TEXT NOT NULL, risk_type TEXT NOT NULL,
 object_identity_json TEXT NOT NULL, first_adverse TEXT NOT NULL, last_adverse TEXT NOT NULL,
 candidate_start TEXT, resolved_at TEXT, previous_episode_id TEXT REFERENCES server_risk_episodes(episode_id),
 lifecycle TEXT NOT NULL CHECK(lifecycle IN ('open','resolved','superseded')), latest_revision TEXT NOT NULL
);
CREATE UNIQUE INDEX uq_server_risk_open_subject ON server_risk_episodes(subject_key) WHERE lifecycle='open';
CREATE TABLE server_risk_decisions (
 decision_id TEXT PRIMARY KEY, episode_id TEXT REFERENCES server_risk_episodes(episode_id), subject_key TEXT NOT NULL,
 revision TEXT NOT NULL, effective_at TEXT NOT NULL, decided_at TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('present','recovering','clear','unknown')), valid_until TEXT,
 reason_codes_json TEXT NOT NULL, evidence_refs_json TEXT NOT NULL, policy_version TEXT NOT NULL,
 superseded_by TEXT REFERENCES server_risk_decisions(decision_id), UNIQUE(subject_key,revision)
);
CREATE INDEX idx_server_risk_decision_subject ON server_risk_decisions(subject_key,effective_at);
CREATE TABLE server_risk_reports (
 report_id TEXT PRIMARY KEY, request_id TEXT NOT NULL, query_json TEXT NOT NULL, idempotency_scope TEXT NOT NULL,
 idempotency_key TEXT NOT NULL, request_digest TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('queued','running','completed','failed','cancelled')),
 cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK(cancel_requested IN (0,1)),
 claim TEXT, version INTEGER NOT NULL DEFAULT 0, deadline TEXT NOT NULL,
 evidence_snapshot_json TEXT NOT NULL, evidence_digest TEXT NOT NULL, profile_json TEXT NOT NULL,
 prompt_id TEXT NOT NULL, prompt_hash TEXT NOT NULL, prompt_content TEXT NOT NULL,
 renderer_version TEXT NOT NULL, artifact_path TEXT, artifact_digest TEXT, artifact_bytes INTEGER,
 error_code TEXT, usage_json TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, completed_at TEXT,
 model_calls INTEGER NOT NULL DEFAULT 0 CHECK(model_calls BETWEEN 0 AND 1),
 UNIQUE(idempotency_scope,idempotency_key)
);
CREATE INDEX idx_server_risk_report_status ON server_risk_reports(status,deadline);
INSERT OR IGNORE INTO model_profiles(profile_id,connection_id,model,display_name,enabled,structured_output_mode,profile_json,created_at,updated_at) SELECT 'ling_3_0_tiny_8b_q4km',connection_id,'maternion/ling-3.0-tiny:8b-Q4_K_M','Ling 3.0 Tiny 8B 风险日报',1,'json_schema',json_set('{"enabled": true, "provider": "ollama", "connection_id": "ollama-local", "structured_output_mode": "json_schema", "model": "maternion/ling-3.0-tiny:8b-Q4_K_M", "display_name": "Ling 3.0 Tiny 8B 风险日报", "parameter_size": "8b", "context_window_tokens": 32768, "recommended_input_tokens": 12000, "max_output_tokens": 2048, "default_prompt_id": "node_risk_report_v1", "json_reliability": "medium", "reasoning_capacity": "medium", "thinking": {"enabled": false, "provider_option_name": "think", "unsupported_behavior": "ignore"}, "evidence_budget": {"max_templates": 20, "max_template_chars": 320, "max_affected_entities": 100, "max_evidence_chars": 64000}, "options": {"temperature": 0}, "profile_id": "ling_3_0_tiny_8b_q4km"}','$.connection_id',connection_id),strftime('%Y-%m-%dT%H:%M:%fZ','now'),strftime('%Y-%m-%dT%H:%M:%fZ','now') FROM provider_connections WHERE provider='ollama' ORDER BY CASE WHEN connection_id='ollama-local' THEN 0 ELSE 1 END,is_default DESC,connection_id LIMIT 1;
