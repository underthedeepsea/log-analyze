from __future__ import annotations

import json
import threading
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from logrisk.agentic import AgentPlan, AgentStepPlan, FakeAgentPlanner
from logrisk.feature_jobs import FeatureJobManager
from pipeline.dashboard_server import build_server


def _request(url: str, method: str = "GET", payload: dict | None = None, headers: dict | None = None):
    request = Request(url, data=json.dumps(payload).encode() if payload is not None else None, method=method, headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urlopen(request, timeout=3) as response:
            return response.status, json.load(response)
    except HTTPError as exc:
        return exc.code, json.load(exc)


def _definition() -> dict:
    return {
        "schema_version": "1.0", "name": "证据工作流", "description": "受控单角色验收",
        "nodes": [{"node_id": "evidence", "role_id": "evidence_specialist", "depends_on": []}],
        "budget": {"max_nodes": 1, "max_concurrency": 1, "max_tool_calls": 4, "timeout_seconds": 120},
        "retry_policy": {"max_attempts": 2},
    }


def _serve(tmp_path, *, workflows: bool):
    frontend = tmp_path / "dist" / "index.html"; frontend.parent.mkdir(parents=True); frontend.write_text("<!doctype html>")
    manager = FeatureJobManager(auto_start=False)
    server = build_server("127.0.0.1", 0, manager=manager, frontend_path=frontend, database_path=tmp_path / "state.sqlite3", agentic_enabled=workflows, agent_workflows_enabled=workflows)
    if workflows:
        server.agent_runs.runtime.planner = FakeAgentPlanner(AgentPlan("读取", (AgentStepPlan("read", "get_sanitized_evidence", {"job_id": "job-1", "entity_id": "node-a"}),)))
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    return server, thread, manager, f"http://127.0.0.1:{server.server_address[1]}"


def _entity() -> dict:
    return {"window_start": "2026-08-13T00:00:00Z", "window_end": "2026-08-13T00:05:00Z", "cluster": "prod", "entity_type": "node", "entity_id": "node-a", "risk_score": 80, "risk_level": "high", "top_templates": [{"template_hash": "h1", "component": "kernel", "template": "OOM <NUM>", "count": 2}], "affected_entities": []}


def test_workflow_api_is_disabled_by_default(tmp_path):
    server, thread, _, base = _serve(tmp_path, workflows=False)
    try:
        status, body = _request(base + "/api/agent-workflows")
        assert status == 404 and body["code"] == "agent_workflows_disabled"
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=2)


def test_workflow_api_creates_definition_run_and_read_only_replay(tmp_path):
    server, thread, manager, base = _serve(tmp_path, workflows=True)
    try:
        status, workflow = _request(base + "/api/agent-workflows", "POST", _definition(), {"Idempotency-Key": "workflow-api-1"})
        assert status == 201
        status, listed = _request(base + "/api/agent-workflows")
        assert listed["items"][0]["workflow_id"] == workflow["workflow_id"]
        status, workflow_runs = _request(base + f"/api/agent-workflows/{workflow['workflow_id']}/runs")
        assert status == 200 and workflow_runs["items"] == []
        job_id = manager.create_job({"summary": {}, "risk_entities": [_entity()]}, model="qwen3:1.7b", min_score=40)
        status, run = _request(base + f"/api/agent-workflows/{workflow['workflow_id']}/runs", "POST", {"source_job_id": job_id, "entity_id": "node-a", "model_profile_id": "qwen3_5_9b_mlx", "prompt_id": "agent_plan_v1"}, {"Idempotency-Key": "workflow-run-api-1"})
        assert status == 202
        status, replay = _request(base + f"/api/agent-workflow-runs/{run['workflow_run_id']}/replay")
        assert status == 200 and replay["mode"] == "read_only"
        encoded = json.dumps(replay, ensure_ascii=False)
        assert "raw_sample" not in encoded and "samples" not in encoded
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=2)
