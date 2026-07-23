UPDATE model_profiles
SET profile_json = profile_json #- '{options,think}' #- '{options,num_predict}' #- '{options,structured_output_mode}',
    updated_at = NOW()
WHERE jsonb_typeof(profile_json->'options') = 'object';

UPDATE model_profiles
SET profile_json = jsonb_set(profile_json, '{max_output_tokens}', '1600'::jsonb),
    updated_at = NOW()
WHERE profile_id = 'qwen3_5_4b_mlx'
  AND (profile_json->>'max_output_tokens')::INTEGER = 900;
