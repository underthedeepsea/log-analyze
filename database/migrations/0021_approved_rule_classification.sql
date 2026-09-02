UPDATE approved_rules
SET schema_version = 'approved_rule_v1',
    rule_json = json_set(rule_json, '$.schema_version', 'approved_rule_v1')
WHERE schema_version = 'approved_rule_v2'
  AND COALESCE(json_extract(rule_json, '$.schema_version'), '') = ''
  AND (approval_key IS NULL OR trim(approval_key) = '')
  AND (problem_code IS NULL OR trim(problem_code) = '')
  AND COALESCE(json_extract(rule_json, '$.approval_key'), '') = ''
  AND COALESCE(json_extract(rule_json, '$.canonical_approval_key'), '') = ''
  AND COALESCE(json_extract(rule_json, '$.problem_code'), '') = ''
  AND COALESCE(json_extract(rule_json, '$.problemCode'), '') = ''
  AND COALESCE(json_extract(rule_json, '$.match_mode'), '') = ''
  AND COALESCE(json_extract(rule_json, '$.risk_type'), '') = ''
  AND COALESCE(json_extract(rule_json, '$.cause'), '') = ''
  AND COALESCE(json_extract(rule_json, '$.template_storage_key'), '') = ''
  AND COALESCE(json_extract(rule_json, '$.strict_storage_key'), '') = ''
  AND json_type(rule_json, '$.anchor_signatures') IS NULL
  AND json_type(rule_json, '$.supporting_signatures') IS NULL
  AND json_type(rule_json, '$.component_scope') IS NULL
  AND json_type(rule_json, '$.risk_semantic') IS NULL
  AND json_type(rule_json, '$.semantic_fields') IS NULL;
