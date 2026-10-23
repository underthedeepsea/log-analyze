from __future__ import annotations
import copy
from .artifacts import canonical_fingerprint
from .evidence_context import evidence_facts
from .errors import AgenticError

def entity_analysis_view(manager, agent_service, job_id: str, entity_id: str, run_id: str | None = None) -> dict:
    evidence = manager.get_agent_evidence(job_id, entity_id)
    fact_hash = canonical_fingerprint(evidence)
    runs = []
    if agent_service is not None:
        repository = agent_service.repository
        if run_id:
            anchor = repository.get_run(run_id)
            if anchor['source_job_id'] != job_id or anchor['entity_id'] != entity_id:
                raise AgenticError('分析记录跨任务/实体', code='agent_scope_invalid')
        else:
            with repository.database.connect() as con:
                rows = con.execute('SELECT run_id FROM agent_runs WHERE source_job_id=? AND entity_id=? ORDER BY created_at DESC, run_id DESC LIMIT 1', (job_id, entity_id)).fetchall()
            anchor = repository.get_run(rows[0]['run_id']) if rows else None
        if anchor:
            workflow = anchor['locked_snapshot'].get('workflow_run_id')
            with repository.database.connect() as con:
                rows = con.execute('SELECT run_id FROM agent_runs WHERE source_job_id=? AND entity_id=? ORDER BY created_at DESC, run_id DESC', (job_id, entity_id)).fetchall()
            for row in rows:
                item = repository.get_run(row['run_id'])
                if (workflow and item['locked_snapshot'].get('workflow_run_id') == workflow) or item['run_id'] == anchor['run_id']:
                    runs.append(item)
    opinions = []
    for role in ['evidence_specialist', 'rule_specialist', 'feature_specialist']:
        item = next((run for run in runs if run['locked_snapshot'].get('workflow_role_id') == role), None)
        state = 'not_analyzed'; opinion = None; error = None
        if item:
            status = item['status']; error = item.get('error_code')
            artifacts = [a for a in item['artifacts'] if a['artifact_type'] == 'expert_assessment_v1']
            if status in {'failed', 'cancelled'}:state = 'analysis_failed'
            elif status in {'queued', 'planning', 'running', 'paused'}:state = 'running' if status != 'queued' else 'not_analyzed'
            elif artifacts:
                payload = artifacts[-1]['payload']; candidate = payload.get('assessment') or {}; response = payload.get('model_response') or {}
                scope = payload.get('scope') or {}
                if scope.get('source_job_id') == job_id and scope.get('entity_id') == entity_id and scope.get('evidence_hash') == fact_hash and response.get('assessment') == candidate and canonical_fingerprint(response) == payload.get('model_response_hash'):
                    opinion = candidate
                    state = 'analysis_available_pending_review' if candidate.get('schema_version') == 'expert_analysis_v3' else 'historical_analysis'
                else:state = 'analysis_failed';error = 'analysis_evidence_changed'
        opinions.append({'role_id':role,'state':state,'run_id':item['run_id'] if item else None,'error_code':error,'opinion':opinion,'origin':'model' if opinion else None})
    available = sum(x['state'] == 'analysis_available_pending_review' for x in opinions)
    state = 'analysis_failed' if any(x['state'] == 'analysis_failed' for x in opinions) else 'analysis_available_pending_review' if available == 3 else 'running' if any(x['state'] == 'running' for x in opinions) else 'partial' if available else 'not_analyzed'
    facts = evidence_facts(evidence)
    problems = [{'fact_ref':t['template_hash'],'description':t['template'],'component':t.get('component'),'severity':t.get('severity'),'log_occurrence_count':t.get('count'),'first_seen':t.get('first_seen'),'last_seen':t.get('last_seen'),'proven_object_count':next(x['proven_object_count'] for x in facts if x['template_ref']==t['template_hash'])} for t in evidence['templates']]
    return {'schema_version':'entity_analysis_view_v1','job_id':job_id,'entity_id':entity_id,'evidence_hash':fact_hash,'fact_origin':'immutable_source_evidence','facts':copy.deepcopy(evidence),'observed_problems':problems,'ai_state':state,'opinions':opinions,'semantic_acceptance':'manual_review_required','fallback_model_opinion':False}
