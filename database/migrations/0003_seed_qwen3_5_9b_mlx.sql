INSERT OR IGNORE INTO model_profiles(
    profile_id,
    connection_id,
    model,
    display_name,
    enabled,
    structured_output_mode,
    profile_json,
    created_at,
    updated_at
)
SELECT
    'qwen3_5_9b_mlx',
    connection_id,
    'qwen3.5:9b-mlx',
    'Qwen3.5 9B MLX 高质量模式',
    1,
    'json_schema',
    json_object(
        'profile_id', 'qwen3_5_9b_mlx',
        'enabled', json('true'),
        'provider', 'ollama',
        'connection_id', connection_id,
        'structured_output_mode', 'json_schema',
        'model', 'qwen3.5:9b-mlx',
        'display_name', 'Qwen3.5 9B MLX 高质量模式',
        'parameter_size', '9b',
        'context_window_tokens', 262144,
        'recommended_input_tokens', 12000,
        'max_output_tokens', 2000,
        'default_prompt_id', 'feature_extract_v3_compact_strict_json_en',
        'json_reliability', 'high',
        'reasoning_capacity', 'medium',
        'thinking', json_object(
            'enabled', json('false'),
            'provider_option_name', 'think',
            'unsupported_behavior', 'ignore'
        ),
        'evidence_budget', json_object(
            'max_templates', 10,
            'max_template_chars', 320,
            'max_affected_entities', 45,
            'max_evidence_chars', 16000
        ),
        'options', json_object('temperature', 0)
    ),
    strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
FROM provider_connections
WHERE provider = 'ollama'
ORDER BY
    CASE WHEN connection_id = 'ollama-local' THEN 0 ELSE 1 END,
    is_default DESC,
    connection_id
LIMIT 1;
