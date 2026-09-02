UPDATE approved_rules
SET schema_version = 'approved_rule_v1',
    rule_json = jsonb_set(rule_json, '{schema_version}', to_jsonb('approved_rule_v1'::text), true)
WHERE schema_version = 'approved_rule_v2'
  AND COALESCE(btrim(rule_json->>'schema_version'), '') = ''
  AND (approval_key IS NULL OR btrim(approval_key) = '')
  AND (problem_code IS NULL OR btrim(problem_code) = '')
  AND COALESCE(btrim(rule_json->>'approval_key'), '') = ''
  AND COALESCE(btrim(rule_json->>'canonical_approval_key'), '') = ''
  AND COALESCE(btrim(rule_json->>'problem_code'), '') = ''
  AND COALESCE(btrim(rule_json->>'problemCode'), '') = ''
  AND COALESCE(btrim(rule_json->>'match_mode'), '') = ''
  AND COALESCE(btrim(rule_json->>'risk_type'), '') = ''
  AND COALESCE(btrim(rule_json->>'cause'), '') = ''
  AND COALESCE(btrim(rule_json->>'template_storage_key'), '') = ''
  AND COALESCE(btrim(rule_json->>'strict_storage_key'), '') = ''
  AND NOT (rule_json ? 'anchor_signatures')
  AND NOT (rule_json ? 'supporting_signatures')
  AND NOT (rule_json ? 'component_scope')
  AND NOT (rule_json ? 'risk_semantic')
  AND NOT (rule_json ? 'semantic_fields');
