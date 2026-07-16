UPDATE model_profiles
SET profile_json = json_remove(
        profile_json,
        '$.options.think',
        '$.options.num_predict',
        '$.options.structured_output_mode'
    ),
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE json_type(profile_json, '$.options') = 'object';

UPDATE model_profiles
SET profile_json = json_set(profile_json, '$.max_output_tokens', 1600),
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE profile_id = 'qwen3_5_4b_mlx'
  AND CAST(json_extract(profile_json, '$.max_output_tokens') AS INTEGER) = 900;
