INSERT INTO model_profiles(
    profile_id, connection_id, model, display_name, enabled, structured_output_mode,
    profile_json, created_at, updated_at
)
SELECT
    'qwen3_5_9b_mlx', connection_id, 'qwen3.5:9b-mlx', 'Qwen3.5 9B MLX 高质量模式', TRUE, 'json_schema',
    jsonb_build_object(
        'profile_id', 'qwen3_5_9b_mlx', 'enabled', TRUE, 'provider', 'ollama', 'connection_id', connection_id,
        'structured_output_mode', 'json_schema', 'model', 'qwen3.5:9b-mlx',
        'display_name', 'Qwen3.5 9B MLX 高质量模式', 'parameter_size', '9b', 'context_window_tokens', 262144,
        'recommended_input_tokens', 12000, 'max_output_tokens', 2000,
        'default_prompt_id', 'feature_extract_v3_compact_strict_json_en', 'json_reliability', 'high',
        'reasoning_capacity', 'medium',
        'thinking', jsonb_build_object('enabled', FALSE, 'provider_option_name', 'think', 'unsupported_behavior', 'ignore'),
        'evidence_budget', jsonb_build_object('max_templates', 10, 'max_template_chars', 320, 'max_affected_entities', 45, 'max_evidence_chars', 16000),
        'options', jsonb_build_object('temperature', 0)
    ), NOW(), NOW()
FROM provider_connections
WHERE provider = 'ollama'
ORDER BY CASE WHEN connection_id = 'ollama-local' THEN 0 ELSE 1 END, is_default DESC, connection_id
LIMIT 1
ON CONFLICT (profile_id) DO NOTHING;
