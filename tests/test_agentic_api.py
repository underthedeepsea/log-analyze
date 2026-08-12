from __future__ import annotations

import json
import threading
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from logrisk.agentic import AgentPlan, AgentStepPlan, FakeAgentPlanner
from logrisk.feature_jobs import FeatureJobManager
from pipeline.dashboard_server import build_server


def _request(url: str, method: str = "GET", payload: dict | None = None, headers: dict | None = None):
    request = Request(
        url,
        data=json.dumps(payload).encode() if payload is not None else None,
        method=method,
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    try:
        with urlopen(request, timeout=3) as response:
            return response.status, json.load(response)
    except HTTPError as exc:
        return exc.code, json.load(exc)


def _entity():
    return {
        "window_start": "2026-08-12T10:00:00+08:00",
        "window_end": "2026-08-12T10:05:00+08:00",
        "cluster": "prod-a",
        "entity_type": "node",
        "entity_id": "node-a",
        "risk_score": 80,
        "risk_level": "high",
        "top_templates": [{"template_hash": "hash-a", "component": "kernel", "template": "OOM <NUM>", "count": 2}],
        "affected_entities": [],
    }


def _serve(tmp_path, *, enabled: bool):
    frontend = tmp_path / "dist" / "index.html"
    frontend.parent.mkdir(parents=True)
    frontend.write_text("<!doctype html>", encoding="utf-8")
    manager = FeatureJobManager(auto_start=False)
    server = build_server(
        "127.0.0.1", 0, manager=manager, frontend_path=frontend,
        database_path=tmp_path / "state" / "logrisk.sqlite3", agentic_enabled=enabled,
    )
    if enabled:
        server.agent_runs.runtime.planner = FakeAgentPlanner(AgentPlan("读取证据", (  # type: ignore[attr-defined]
            AgentStepPlan("read", "get_sanitized_evidence", {}),
        )))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, manager, f"http://127.0.0.1:{server.server_address[1]}"


def test_agent_api_is_disabled_by_default(tmp_path):
    server, thread, _, base = _serve(tmp_path, enabled=False)
    try:
        status, body = _request(base + "/api/agent-runs")
        assert status == 404
        assert body["code"] == "agentic_disabled"
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=2)


def test_agent_api_creates_and_replays_run(tmp_path):
    server, thread, manager, base = _serve(tmp_path, enabled=True)
    try:
        job_id = manager.create_job({"summary": {}, "risk_entities": [_entity()]}, model="qwen3:1.7b", min_score=40)
        status, created = _request(
            base + "/api/agent-runs", "POST",
            {"source_job_id": job_id, "entity_id": "node-a", "model_profile_id": "qwen3_5_9b_mlx", "prompt_id": "agent_plan_v1"},
            {"Idempotency-Key": "api-run-1"},
        )
        assert status == 202
        run_id = created["run_id"]
        status, replay = _request(base + f"/api/agent-runs/{run_id}/replay")
        assert status == 200
        assert replay["mode"] == "read_only"
        assert replay["locked_snapshot"]["evidence_summary"]["template_count"] == 1
        encoded = json.dumps(replay, ensure_ascii=False)
        assert "raw_sample" not in encoded and "samples" not in encoded
        status, replay_post = _request(base + f"/api/agent-runs/{run_id}/actions/replay", "POST", {})
        assert status == 200
        assert replay_post["mode"] == "read_only"
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=2)
