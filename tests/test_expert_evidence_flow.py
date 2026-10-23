from __future__ import annotations
import copy
import json
import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import pytest
from logrisk.agentic.models import AgentRunRequest
from logrisk.agentic.artifacts import canonical_fingerprint
from logrisk.agentic.compiler import WorkflowLimits
from logrisk.agentic.evidence_context import completed_upstream, rule_reference
from logrisk.agentic.errors import AgenticError
from logrisk.agentic.planner import ModelAgentPlanner
from logrisk.agentic.repository import AgentRepository
from logrisk.agentic.roles import build_role_registry
from logrisk.agentic.runtime import AgentRuntime
from logrisk.agentic.service import AgentService
from logrisk.agentic.tools import build_agent_tool_registry
from logrisk.agentic.workflow_repository import WorkflowRepository
from logrisk.agentic.workflow_worker import WorkflowWorker
from logrisk.agentic.workflow_scheduler import WorkflowScheduler
from logrisk.agentic.workflow_service import WorkflowService
from logrisk.database import SQLiteDatabase
from logrisk.feature_jobs import FeatureJobManager
from logrisk.sqlite_stores import SQLiteFeatureJobStore, SQLiteApprovalGroupStore, SQLiteApprovedRuleStore

TEMPLATES=[{'template_hash':'hash-oom','component':'kernel','template':'OOM killed process <NUM>','count':3,'severity':'ERROR','raw_sample':'raw-secret-never-provider'},
           {'template_hash':'hash-oom-two','component':'kernel','template':'Out of memory: Killed process <NUM>','count':2,'severity':'ERROR'}]
class Rules:
    def __init__(self,items):self.items=items;self.queries=[]
    def list_rules(self,**kw):
        self.queries.append(kw)
        return {'items':copy.deepcopy(self.items),'pagination':{'total':len(self.items)}}
class Packages:
    def list_packages(self):return []

def system(tmp_path,templates=None,rules=None,mutate=None):
    db=SQLiteDatabase(tmp_path/'agent.sqlite3')
    manager=FeatureJobManager(extractor=lambda *a,**k:[],auto_start=False,persistence=SQLiteFeatureJobStore(db),approval_group_store=SQLiteApprovalGroupStore(db),rule_store=SQLiteApprovedRuleStore(db))
    job_id=manager.create_job({'risk_entities':[{'entity_id':'node-a','entity_type':'node','cluster':'prod','risk_score':82,'risk_level':'high','samples':['raw-secret-never-provider'],'top_templates':copy.deepcopy(TEMPLATES if templates is None else templates)}]},model='fake-only')
    rule_source=Rules([] if rules is None else rules)
    registry=build_agent_tool_registry(manager,rule_source,Packages());repo=AgentRepository(db)
    s=SimpleNamespace(db=db,manager=manager,job_id=job_id,rules=rule_source,registry=registry,repo=repo,requests=[])
    class Model:
        def __init__(self,run):self.run=run
        def generate_json(self,messages,schema,**kw):
            payload=json.loads(messages[1]['content']);ctx=payload['evidence_summary']['expert_context'];role=ctx['role_id']
            calls=repo.get_run(self.run['run_id'])['tool_calls'];ids={x['tool_call_id'] for x in calls if x['status']=='completed'}
            assert {x['tool_call_id'] for x in ctx['read_receipts']}<=ids
            assert len(ctx['read_receipts'])==(1 if role=='evidence_specialist' else 2)
            assert all(x.get('template_hash') for x in ctx['evidence']['templates'])
            assert 'raw-secret-never-provider' not in json.dumps(payload)
            hashes=[x['template_hash'] for x in ctx['evidence']['templates']]
            feature={'feature_type':'node_memory_pressure','title':'内存压力异常','summary':'kernel 模板明确记录 OOM 异常。','importance':'high','template_hashes':hashes,'components':list({x['component'] for x in ctx['evidence']['templates']}),'tags':['内存','异常'],'selection_reason':'所选模板明确记录内存异常，实际根因与影响待确认。'}
            output={'goal':'根据本角色真实证据分析','assessment':{'role_id':role,'context_hash':ctx['context_hash'],'conclusion':'分析模板 '+ '|'.join(x['template'] for x in ctx['evidence']['templates']),'template_refs':hashes,'rule_refs':[rule_reference(x) for x in ctx['approved_rules']['items']],'upstream_refs':[x['artifact_id'] for x in ctx['upstream_assessments']],'limitations':['未执行根因分析，实际影响与恢复未知，候选等待人工审批。']},'steps':[] if role!='feature_specialist' else [{'step_id':'evaluate','tool_name':'evaluate_candidate','arguments':{'feature':feature}},{'step_id':'register','tool_name':'register_feature_candidate','arguments':{'feature':copy.deepcopy(feature)}}]}
            if ctx.get('schema_version')=='expert_context_v2':
                from logrisk.agentic.evidence_context import evidence_facts
                output['assessment'].update(observations=evidence_facts(ctx['evidence']),root_cause_status='unknown',recovery_status='unknown')
            if ctx.get('schema_version')=='expert_context_v3':
                output['assessment']={'schema_version':'expert_analysis_v3','role_id':role,'context_hash':ctx['context_hash'],'template_refs':hashes,'rule_refs':[rule_reference(x) for x in ctx['approved_rules']['items']],'upstream_refs':[x['artifact_id'] for x in ctx['upstream_assessments']],'analyses':[{'kind':'manual_guidance','status':'guidance','fact_refs':hashes,'text':'请按各模板分别核对对应对象和时段，不合并为同一根因。','missing_evidence':[],'manual_checks':['人工核对相应对象事件与影响，实际根因和恢复未知。']}],'limitations':['仅提出待核查解释；不是已确立RCA，也不自动批准。']}
            item={'role':role,'run_id':self.run['run_id'],'messages':copy.deepcopy(messages),'schema':copy.deepcopy(schema),'payload':payload,'response':copy.deepcopy(output)};s.requests.append(item)
            if mutate:mutate(s,role,output,ctx)
            item['response']=copy.deepcopy(output)
            return output
    def planner(run):return ModelAgentPlanner(Model(run),model='recording-fake',prompt_content='仅使用锁定脱敏证据，输出原schema，不RCA。',timeout=30)
    runtime=AgentRuntime(repo,planner,registry);child=AgentService(repo,runtime)
    wr=WorkflowRepository(db);workflows=WorkflowService(wr,WorkflowScheduler(wr,WorkflowWorker(wr,child)),build_role_registry(),WorkflowLimits(),evidence_locker=child.lock_evidence_summary)
    definition={'schema_version':'1.0','name':'固定三角色','description':'真实工具材料与模型意见分离','nodes':[{'node_id':'evidence','role_id':'evidence_specialist','depends_on':[]},{'node_id':'rules','role_id':'rule_specialist','depends_on':[]},{'node_id':'feature','role_id':'feature_specialist','depends_on':['evidence','rules']}],'budget':{'max_nodes':3,'max_concurrency':2,'max_tool_calls':20,'timeout_seconds':60},'retry_policy':{'max_attempts':1}}
    workflow=workflows.create_workflow(definition,actor='tester',idempotency_key='definition')
    s.child=child;s.workflows=workflows;s.workflow=workflow;s.runtime=runtime
    def create():return workflows.create_run(workflow['workflow_id'],source_job_id=job_id,entity_id='node-a',entity_type='node',model_profile_id='profile',prompt_id='agent_plan_v1',actor='tester',roles=('operator',),request_id='req',idempotency_key='workflow-run',evidence_summary={'entity':{'id':'node-a','cluster':'prod'},'template_count':len(TEMPLATES)},runtime_snapshot={'profile_snapshot':{'profile_id':'profile','connection_id':'local','enabled':True},'connection_snapshot':{'connection_id':'local','provider':'ollama','enabled':True},'prompt_id':'agent_plan_v1','prompt_sha256':'fixed'})
    s.create=create
    return s

def execute(s):
    run=s.create();result=s.workflows.execute_run(run['workflow_run_id']);return run,result

def test_recorded_three_roles_see_material_before_model_and_persist_real_pending(tmp_path):
    s=system(tmp_path);run,result=execute(s)
    assert result['status']=='awaiting_human',result
    assert len(s.requests)==3
    assert result['used_tool_calls']==7
    assert s.db
    with s.db.connect() as con:
        assert con.execute("SELECT COUNT(*) FROM approval_groups WHERE status='pending'").fetchone()[0]==1
        assert con.execute('SELECT COUNT(*) FROM approved_rules').fetchone()[0]==0
        assert con.execute('SELECT COUNT(*) FROM feature_candidates').fetchone()[0]==1
    feature=s.manager.get_job(s.job_id)['features'][0]
    assert feature['status']=='pending' and not feature.get('rule_id')
    assert set(feature['template_hashes'])=={'hash-oom','hash-oom-two'}
    from logrisk.approval_experts import ApprovalExpertReader
    cards=ApprovalExpertReader(s.db).read(s.job_id,feature['candidate_id'])['opinions']
    assert len(cards)==3 and all(card['opinion_origin']=='model' for card in cards)
    assert all('待核查解释' in card['conclusion'] for card in cards)

    for node in result['nodes']:
        child=s.repo.get_run(node['child_agent_run_id'])
        assert len([a for a in child['artifacts'] if a['artifact_type']=='expert_assessment_v1'])==1
        assert all(a['payload']['origin']=='deterministic_tool' for a in child['artifacts'] if a['artifact_type'] in ('evidence_assessment_v1','rule_match_assessment_v1'))
    ctx=next(x for x in s.requests if x['role']=='feature_specialist')['payload']['evidence_summary']['expert_context']
    assert {x['role_id'] for x in ctx['upstream_assessments']}=={'evidence_specialist','rule_specialist'}
    for opinion in ctx['upstream_assessments']:
        assert opinion['assessment']['schema_version']=='expert_analysis_v3' and opinion['assessment']['analyses']
    count=len(s.requests)
    assert s.workflows.execute_run(run['workflow_run_id'])['status']=='awaiting_human'
    assert len(s.requests)==count
    if capture:=os.getenv('LOGRISK_REPAIR_CAPTURE_DIR'):
        p=Path(capture);p.mkdir(parents=True,exist_ok=True)
        (p/'fake-recorded-three-role-input-output.json').write_text(json.dumps({'kind':'offline recording fake, not real Ling','requests':s.requests,'result':result},ensure_ascii=False,indent=2))
        import sqlite3
        with s.db.connect() as source:
            target=sqlite3.connect(p/'fake-three-role-final.sqlite3');source.backup(target);target.close()

def test_same_counts_changed_real_content_changes_model_input_and_bound_reference(tmp_path):
    a=system(tmp_path/'a');_,ra=execute(a)
    changed=copy.deepcopy(TEMPLATES)
    for t in changed:t['template_hash']+='-changed';t['template']='Memory cgroup OOM killed process <NUM>'
    b=system(tmp_path/'b',templates=changed);_,rb=execute(b)
    assert ra['status']==rb['status']=='awaiting_human'
    first=lambda s:next(x for x in s.requests if x['role']=='evidence_specialist')
    assert first(a)['response']['assessment']['context_hash']!=first(b)['response']['assessment']['context_hash']
    assert first(a)['response']['assessment']['template_refs']!=first(b)['response']['assessment']['template_refs']
    input_templates=lambda s:first(s)['payload']['evidence_summary']['expert_context']['evidence']['templates']
    assert [t['template'] for t in input_templates(a)]==[t['template'] for t in TEMPLATES]
    assert [t['template'] for t in input_templates(b)]==[t['template'] for t in changed]
    assert [t['count'] for t in input_templates(a)]==[t['count'] for t in input_templates(b)]
    assert first(a)['messages'][1]['content']!=first(b)['messages'][1]['content']
    assert 'conclusion' not in first(a)['response']['assessment']
    assert 'conclusion' not in first(b)['response']['assessment']

@pytest.mark.parametrize('bad',['missing_hash','wrong_hash_type','conflicting_identity','cross_cluster','cross_entity','cross_source','credential'])
def test_bad_locked_material_rejected_before_model(tmp_path,bad):
    s=system(tmp_path);original=s.manager.get_agent_evidence
    def tampered(*args):
        e=original(*args)
        if bad=='missing_hash':e['templates'][0]['template_hash']=None
        elif bad=='wrong_hash_type':e['templates'][0]['template_hash']=123
        elif bad=='conflicting_identity':e['templates'][1]['template_hash']=e['templates'][0]['template_hash']
        elif bad=='cross_cluster':e['templates'][0]['cluster']='foreign'
        elif bad=='cross_entity':e['templates'][0]['entity_id']='node-other'
        elif bad=='cross_source':e['templates'][0]['source_job_id']='foreign-job'
        else:e['templates'][0]['template']='authorization: Bearer real-secret-value'
        return e
    s.manager.get_agent_evidence=tampered
    with pytest.raises(AgenticError):s.create()
    assert not s.requests

@pytest.mark.parametrize('ref',['template','rule_version','upstream','input_context'])
def test_forged_model_references_rejected_without_candidate(tmp_path,ref):
    def mutate(s,role,output,ctx):
        if role!='feature_specialist':return
        if ref=='template':output['assessment']['template_refs']=['forged-template']
        elif ref=='rule_version':output['assessment']['rule_refs']=['missing-rule@999']
        elif ref=='upstream':output['assessment']['upstream_refs']=[]
        else:output['assessment']['context_hash']='other-context'
    s=system(tmp_path,mutate=mutate);_,r=execute(s)
    assert r['status']=='failed'
    assert s.manager.get_job(s.job_id)['features']==[]
    assert len(s.requests)==3

def test_active_persistent_rule_summary_is_versioned_reference_not_automatic_approval(tmp_path):
    rules=[{'rule_id':'active-different-cause','title':'另一原因仅供参考','status':'active','current_version':4,'problem_code':'network.cni.invalid','selection_reason':'明确CNI证据才适用','match_mode':'semantic','template_signatures':[{'template_hash':'hash-oom','component':'kernel'}]},
           {'rule_id':'revoked','title':'撤销规则','status':'disabled','current_version':7,'template_signatures':[{'template_hash':'hash-oom','component':'kernel'}]}]
    s=system(tmp_path,rules=rules);_,r=execute(s)
    assert r['status']=='awaiting_human'
    for request in s.requests:
        ctx=request['payload']['evidence_summary']['expert_context']
        if request['role']=='evidence_specialist':continue
        assert [x['rule_id'] for x in ctx['approved_rules']['items']]==['active-different-cause']
        assert ctx['approved_rules']['items'][0]['current_version']==4
        assert ctx['approved_rules']['items'][0]['selection_reason']=='明确CNI证据才适用'
        assert 'reference_only' in ctx['approved_rules']['items'][0]['applicability']
    candidate=s.manager.get_job(s.job_id)['features'][0]
    assert candidate['status']=='pending' and candidate.get('rule_id') is None
    # Two billed model-facing reads remain; persistence checks reread current authority.
    with s.db.connect() as con:
        assert con.execute("SELECT COUNT(*) FROM agent_tool_calls WHERE tool_name='find_approved_rules'").fetchone()[0]==2
    assert len(s.rules.queries)>2

def single(s,role='evidence_specialist',max_calls=10):
    allowed=build_role_registry().get(role).allowed_tools
    req=AgentRunRequest(s.job_id,'node-a','node','profile','agent_plan_v1',6,max_calls,120,allowed,'single','tester',('operator',),'req')
    return s.child.create_run(req,locked_snapshot={'workflow_role_id':role,'workflow_run_id':'single-workflow','workflow_node_id':role,'evidence_summary':{'entity':{'id':'node-a','cluster':'prod'}}})

def test_prefetch_budget_failure_precedes_model_and_counts_completed_read(tmp_path):
    s=system(tmp_path);run=single(s,'rule_specialist',1);r=s.child.execute_run(run['run_id'])
    assert r['status']=='failed' and r['error_code']=='agent_budget_exhausted'
    assert r['used_tool_calls']==1 and not s.requests

def test_cancel_during_model_discards_response_and_does_not_register(tmp_path):
    def cancel(s,role,output,ctx):s.child.cancel(s.requests[-1]['run_id'],idempotency_key='cancel')
    s=system(tmp_path,mutate=cancel);run=single(s);r=s.child.execute_run(run['run_id'])
    assert r['status']=='cancelled' and len(s.requests)==1
    assert not [a for a in r['artifacts'] if a['artifact_type']=='expert_assessment_v1']
    assert not s.manager.get_job(s.job_id)['features']

def test_timeout_in_prefetch_precedes_model(tmp_path):
    s=system(tmp_path);clock=[0.0];s.runtime.monotonic=lambda:clock[0]
    tool=s.registry.get('get_sanitized_evidence')
    def slow(args,ctx):
        output=tool.handler(args,ctx);clock[0]=121.0;return output
    s.registry._tools[tool.name]=replace(tool,handler=slow)
    run=single(s);r=s.child.execute_run(run['run_id'])
    assert r['status']=='failed' and r['error_code']=='agent_timeout'
    assert r['used_tool_calls']==1 and not s.requests

def test_restart_does_not_repeat_uncertain_model_or_candidate(tmp_path):
    def crash(s,role,output,ctx):raise SystemExit('simulated interruption after model received input')
    s=system(tmp_path,mutate=crash);run=single(s)
    with pytest.raises(SystemExit):s.child.execute_run(run['run_id'])
    assert len(s.requests)==1
    recovered=s.child.recover_run(run['run_id'])
    assert recovered['status']=='failed' and recovered['error_code']=='agent_plan_indeterminate'
    assert s.child.execute_run(run['run_id'])['status']=='failed'
    assert len(s.requests)==1 and not s.manager.get_job(s.job_id)['features']

def test_changed_evidence_after_workflow_lock_prevents_model(tmp_path):
    s=system(tmp_path);run=s.create();original=s.manager.get_agent_evidence
    def changed(*args):
        e=original(*args);e['templates'][0]['count']+=1;return e
    s.manager.get_agent_evidence=changed
    r=s.workflows.execute_run(run['workflow_run_id'])
    assert r['status']=='failed' and not s.requests

def test_completed_upstream_from_another_workflow_is_rejected(tmp_path):
    s=system(tmp_path);_,result=execute(s);feature=next(n for n in result['nodes'] if n['node_id']=='feature');run=s.repo.get_run(feature['child_agent_run_id'])
    bad=copy.deepcopy(run);bad['locked_snapshot']['workflow_run_id']='different-workflow'
    e=s.manager.get_agent_evidence(s.job_id,'node-a')
    with pytest.raises(AgenticError) as exc:completed_upstream(bad,s.repo,e,canonical_fingerprint(e))
    assert exc.value.code=='agent_upstream_scope_invalid'


def _http_planner(repository,requests):
    def planner(run):
        class Client:
            def generate_json(self,messages,schema,**kw):
                payload=json.loads(messages[1]['content']);ctx=payload['evidence_summary']['expert_context'];requests.append(ctx)
                current=repository.get_run(run['run_id'])
                assert len(current['tool_calls'])==(1 if ctx['role_id']=='evidence_specialist' else 2)
                hashes=[x['template_hash'] for x in ctx['evidence']['templates']]
                feature={'feature_type':'node_memory_pressure','title':'内存异常','summary':'kernel OOM异常','importance':'high','template_hashes':hashes,'components':['kernel'],'tags':['内存'],'selection_reason':'所选kernel OOM模板支持该待审批特征'}
                output={'goal':'分析锁定材料','assessment':{'role_id':ctx['role_id'],'context_hash':ctx['context_hash'],'conclusion':'基于真实模板形成HTTP入口意见','template_refs':hashes,'rule_refs':[rule_reference(x) for x in ctx['approved_rules']['items']],'upstream_refs':[x['artifact_id'] for x in ctx['upstream_assessments']],'limitations':['实际影响与根因未知，待人工审批']},'steps':[] if ctx['role_id']!='feature_specialist' else [{'step_id':'eval','tool_name':'evaluate_candidate','arguments':{'feature':feature}},{'step_id':'register','tool_name':'register_feature_candidate','arguments':{'feature':feature}}]}
                if ctx.get('schema_version')=='expert_context_v3':
                    output['assessment']={'schema_version':'expert_analysis_v3','role_id':ctx['role_id'],'context_hash':ctx['context_hash'],'template_refs':hashes,'rule_refs':[rule_reference(x) for x in ctx['approved_rules']['items']],'upstream_refs':[x['artifact_id'] for x in ctx['upstream_assessments']],'analyses':[{'kind':'manual_guidance','status':'guidance','fact_refs':hashes,'text':'逐模板核对不同异常的对象和时间，不推断同一根因。','missing_evidence':[],'manual_checks':['人工核对相应对象事件，影响与恢复未知。']}],'limitations':['离线fake只验证程序路径；根因和恢复未知，等待人工审批。']}
                return output
        return ModelAgentPlanner(Client(),model='recording-fake',prompt_content='三角色真实证据',timeout=30)
    return planner

def _three_role_definition():
    return {'schema_version':'1.0','name':'HTTP三角色','description':'两入口同合同','nodes':[{'node_id':'evidence','role_id':'evidence_specialist','depends_on':[]},{'node_id':'rules','role_id':'rule_specialist','depends_on':[]},{'node_id':'feature','role_id':'feature_specialist','depends_on':['evidence','rules']}],'budget':{'max_nodes':3,'max_concurrency':2,'max_tool_calls':20,'timeout_seconds':30},'retry_policy':{'max_attempts':1}}

def test_dashboard_http_entry_records_three_actual_model_contexts(tmp_path):
    import time
    from tests.test_agent_workflow_api import _serve,_request,_entity
    server,thread,manager,base=_serve(tmp_path,workflows=True);requests=[]
    server.agent_runs.runtime.planner=_http_planner(server.agent_runs.repository,requests)
    try:
        job=manager.create_job({'risk_entities':[_entity()]},model='fake',min_score=40)
        status,wf=_request(base+'/api/agent-workflows','POST',_three_role_definition(),{'Idempotency-Key':'definition-http'})
        assert status==201
        status,run=_request(base+f"/api/agent-workflows/{wf['workflow_id']}/runs",'POST',{'source_job_id':job,'entity_id':'node-a','model_profile_id':'qwen3_5_9b_mlx','prompt_id':'agent_plan_v1'},{'Idempotency-Key':'run-http'})
        assert status==202
        for _ in range(100):
            current=server.agent_workflows.get_run(run['workflow_run_id'])
            if current['status'] in ('awaiting_human','failed'):break
            time.sleep(.02)
        assert current['status']=='awaiting_human',current
        assert len(requests)==3 and current['used_tool_calls']==7
        assert all(x['scope']['source_job_id']==job for x in requests)
        assert manager.get_job(job)['features'][0]['status']=='pending'
    finally:
        server.shutdown();server.server_close();thread.join(timeout=2)

def test_django_http_entry_uses_same_shared_three_role_contract(tmp_path,monkeypatch):
    import os
    os.environ.setdefault('DJANGO_SETTINGS_MODULE','tests.django_test_project.settings')
    import django
    django.setup()
    from django.test import RequestFactory
    from dataclasses import replace
    from logrisk.application.container import ApplicationConfig,build_application_container
    from logrisk.runtime.identity import RequestIdentity
    from logrisk_django.views import agent_workflows as views
    from tests.test_agent_workflow_api import _entity
    config=replace(ApplicationConfig.for_test(project_root=Path(__file__).resolve().parents[1],state_root=tmp_path/'state'),output_root=tmp_path/'output',agentic_enabled=True,agent_workflows_enabled=True)
    container=build_application_container(config);container.feature_jobs.auto_start=False
    requests=[];container.agent_runs.runtime.planner=_http_planner(container.agent_runs.repository,requests)
    monkeypatch.setattr(views,'get_container',lambda:container)
    monkeypatch.setattr(views,'get_config',lambda:config)
    monkeypatch.setattr(views,'require_django_write_access',lambda request:RequestIdentity('test-operator',('operator',),'django-req',True,'test-fixture','127.0.0.1'))
    def trigger(run_id,request_id):
        container.agent_workflows.execute_run(run_id)
        return SimpleNamespace(external_run_id='offline-fake-trigger')
    monkeypatch.setattr(views,'get_agent_workflow_airflow_orchestrator',lambda:SimpleNamespace(dag_id='offline-fake',trigger_agent_workflow=trigger))
    job=container.feature_jobs.create_job({'risk_entities':[_entity()]},model='fake',min_score=40)
    wf=container.agent_workflows.create_workflow(_three_role_definition(),actor='test-operator',idempotency_key='definition-django')
    req=RequestFactory().post('/api/agent-workflows/'+wf['workflow_id']+'/runs',data=json.dumps({'source_job_id':job,'entity_id':'node-a','model_profile_id':'qwen3_5_9b_mlx','prompt_id':'agent_plan_v1'}),content_type='application/json',HTTP_IDEMPOTENCY_KEY='run-django')
    response=views.workflow_detail(req,wf['workflow_id'],view='runs')
    assert response.status_code==202,response.content
    run=json.loads(response.content);current=container.agent_workflows.get_run(run['workflow_run_id'])
    assert current['status']=='awaiting_human',current
    assert len(requests)==3 and current['used_tool_calls']==7
    assert all(x['scope']['source_job_id']==job for x in requests)
    assert container.feature_jobs.get_job(job)['features'][0]['status']=='pending'

def test_workflow_cancel_preserves_actual_prefetch_consumption(tmp_path):
    fired=[]
    def cancel(s,role,output,ctx):
        if not fired:
            fired.append(True)
            parent=s.repo.get_run(s.requests[-1]['run_id'])['locked_snapshot']['workflow_run_id']
            s.workflows.cancel(parent,idempotency_key='cancel-parent')
    s=system(tmp_path,mutate=cancel);_,result=execute(s)
    assert result['status']=='cancelled'
    actual=sum(s.repo.get_run(n['child_agent_run_id'])['used_tool_calls'] for n in result['nodes'] if n.get('child_agent_run_id'))
    assert result['used_tool_calls']==actual and actual>=1
    assert not s.manager.get_job(s.job_id)['features']

def test_workflow_recovery_never_creates_replacement_paid_child(tmp_path):
    def crash(s,role,output,ctx):
        if role=='evidence_specialist':raise SystemExit('uncertain model')
    s=system(tmp_path,mutate=crash);run=s.create()
    with s.db.transaction() as con:con.execute('UPDATE agent_workflow_runs SET max_attempts=2 WHERE workflow_run_id=?',(run['workflow_run_id'],))
    with pytest.raises(SystemExit):s.workflows.execute_run(run['workflow_run_id'])
    count=len(s.requests)
    s.child.recover_active_runs();s.workflows.recover_run(run['workflow_run_id'])
    result=s.workflows.execute_run(run['workflow_run_id'])
    assert result['status']=='failed'
    assert len(s.requests)==count
    assert not s.manager.get_job(s.job_id)['features']
