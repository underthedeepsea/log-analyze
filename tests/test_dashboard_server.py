import json
import shutil
import threading
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from logrisk.ai_harness.prompt_registry import PromptRegistry
from logrisk.ai_harness.trace_logger import AITraceLogger
from logrisk.feature_jobs import FeatureJobManager
from pipeline.dashboard_server import build_server


def entity(entity_id="node-a", score=90):
    return {
        "window_start": "2026-06-22T10:00:00+08:00",
        "window_end": "2026-06-22T10:05:00+08:00",
        "cluster": "prod-a",
        "entity_type": "node",
        "entity_id": entity_id,
        "risk_score": score,
        "risk_level": "critical",
        "top_templates": [],
        "affected_entities": [],
    }


def candidate(source):
    return {
        "candidate_id": f"feature-{source['entity_id']}",
        "status": "pending",
        "reviewer_note": "",
        "approved_at": None,
        "cluster": source["cluster"],
        "entity": {"type": "node", "id": source["entity_id"]},
        "window_start": source["window_start"],
        "window_end": source["window_end"],
        "risk_score": source["risk_score"],
        "risk_level": source["risk_level"],
        "feature_type": "resource_pressure",
        "title": "内存压力",
        "summary": "OOM 特征",
        "importance": "critical",
        "template_hashes": ["hash"],
        "components": ["kernel"],
        "tags": ["oom"],
        "selection_reason": "高风险模板",
        "occurrence_count": 2,
        "time_range": {"first_seen": source["window_start"], "last_seen": source["window_end"]},
        "affected_entities": [],
        "source_templates": [{"template_hash": "hash", "template": "OOM", "count": 2}],
        "provider": "ollama",
        "model": "qwen3:1.7b",
        "trace_id": f"trace-{source['entity_id']}",
        "prompt_id": "feature_extract_v2_compact_en",
        "evaluator_result": {"passed": True, "errors": [], "warnings": [], "score": 1.0, "rule_results": []},
    }


@pytest.fixture
def dashboard(tmp_path):
    frontend = tmp_path / "dist" / "index.html"
    frontend.parent.mkdir()
    frontend.write_text("<!doctype html><title>Feature Dashboard</title>", encoding="utf-8")
    assets = frontend.parent / "assets"
    assets.mkdir()
    (assets / "app.js").write_text("console.log('app')", encoding="utf-8")
    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: [candidate(source)],
        auto_start=True,
    )
    server = build_server(
        "127.0.0.1",
        0,
        manager=manager,
        frontend_path=frontend,
        ollama_checker=lambda: {"online": True, "models": ["qwen3:1.7b"]},
        input_analyzer=lambda rows: {
            "summary": {"total_raw_logs": len(rows)},
            "risk_entities": [],
            "top_templates": [],
        },
    )
    prompt_dir = tmp_path / "prompts"
    shutil.copytree(Path("prompts"), prompt_dir)
    server.prompt_registry = PromptRegistry(  # type: ignore[attr-defined]
        prompt_dir,
        Path("configs") / "ai_harness.yaml",
        tmp_path / "state" / "prompt_versions.json",
    )
    server.trace_logger = AITraceLogger(tmp_path / "state" / "ai_traces.jsonl")  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    yield base_url, manager
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


def request_json(url, method="GET", payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    request = Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=3) as response:
        return response.status, json.load(response), response.headers


def test_serves_frontend_and_ollama_status(dashboard):
    base_url, _ = dashboard

    with urlopen(base_url + "/", timeout=3) as response:
        html = response.read().decode()
    status, payload, _ = request_json(base_url + "/api/ollama/status")

    assert "Feature Dashboard" in html
    assert status == 200
    assert payload == {"online": True, "models": ["qwen3:1.7b"]}


def test_create_job_snapshot_and_sse_events(dashboard):
    base_url, _ = dashboard
    result = {"summary": {"total_raw_logs": 10}, "risk_entities": [entity()]}

    status, created, _ = request_json(base_url + "/api/jobs", "POST", {
        "result": result,
        "model": "qwen3:1.7b",
        "min_score": 40,
    })
    job_id = created["job_id"]
    for _ in range(50):
        _, snapshot, _ = request_json(base_url + f"/api/jobs/{job_id}")
        if snapshot["status"].startswith("completed"):
            break

    with urlopen(base_url + f"/api/jobs/{job_id}/events?cursor=0", timeout=3) as response:
        events = response.read().decode()

    assert status == 202
    assert snapshot["features"][0]["title"] == "内存压力"
    assert "event: entity_completed" in events
    assert "event: job_completed" in events


def test_review_and_export_routes(dashboard):
    base_url, _ = dashboard
    _, created, _ = request_json(base_url + "/api/jobs", "POST", {
        "result": {"summary": {}, "risk_entities": [entity()]},
        "model": "qwen3:1.7b",
    })
    job_id = created["job_id"]
    for _ in range(50):
        _, snapshot, _ = request_json(base_url + f"/api/jobs/{job_id}")
        if snapshot["features"]:
            break

    status, feature, _ = request_json(
        base_url + f"/api/jobs/{job_id}/features/feature-node-a",
        "PATCH",
        {"status": "approved", "title": "人工确认特征", "reviewer_note": "checked"},
    )
    export_status, package, headers = request_json(base_url + f"/api/jobs/{job_id}/export", "POST", {})

    assert status == 200
    assert feature["status"] == "approved"
    assert export_status == 200
    assert package["approved_features"][0]["title"] == "人工确认特征"
    assert "attachment" in headers["Content-Disposition"]


def test_invalid_upload_and_unknown_route_return_json_errors(dashboard):
    base_url, _ = dashboard

    with pytest.raises(HTTPError) as invalid:
        request_json(base_url + "/api/jobs", "POST", {"result": {}, "model": "qwen3:1.7b"})
    with pytest.raises(HTTPError) as missing:
        request_json(base_url + "/api/missing")

    assert invalid.value.code == 400
    assert json.load(invalid.value)["error"]
    assert missing.value.code == 404


def test_plain_text_analysis_endpoint(dashboard):
    base_url, _ = dashboard

    status, payload, _ = request_json(
        base_url + "/api/inputs/analyze",
        "POST",
        {"filename": "events.log", "content": "error one\n\nerror two"},
    )

    assert status == 200
    assert payload["result"]["summary"]["total_raw_logs"] == 2


def test_rule_list_route(dashboard):
    base_url, _ = dashboard

    status, payload, _ = request_json(base_url + "/api/rules")

    assert status == 200
    assert payload == {"rules": []}


def test_system_metrics_route_returns_daily_llm_volume(dashboard):
    base_url, _ = dashboard

    status, payload, _ = request_json(base_url + "/api/metrics")

    assert status == 200
    assert payload == {"today_llm_logs": 0}


def test_observability_progress_exposes_evaluator_status(dashboard):
    base_url, _ = dashboard
    _, created, _ = request_json(base_url + "/api/jobs", "POST", {
        "result": {"summary": {}, "risk_entities": [entity()]},
        "model": "qwen3:1.7b",
    })
    job_id = created["job_id"]
    for _ in range(50):
        _, progress, _ = request_json(base_url + f"/api/ai-harness/jobs/{job_id}/progress")
        if progress["status"].startswith("completed"):
            break

    status, summary, _ = request_json(base_url + "/api/ai-harness/observability/summary")

    assert progress["summary"]["evaluator_total"] == 1
    assert progress["summary"]["evaluator_passed"] == 1
    assert progress["entities"][0]["evaluator_status"] == "passed"
    assert progress["entities"][0]["evaluator_result"]["score"] == 1.0
    assert status == 200
    assert summary["evaluator"]["passed"] == 1


def test_ai_harness_prompt_and_trace_routes(dashboard):
    base_url, _ = dashboard

    status, prompts, _ = request_json(base_url + "/api/ai-harness/prompts")
    status2, traces, _ = request_json(base_url + "/api/ai-harness/traces?limit=5")
    status3, harness, _ = request_json(base_url + "/api/ai-harness/status")

    assert status == 200
    assert prompts["current_prompt_id"] == "feature_extract_v2_compact_en"
    assert prompts["items"][0]["prompt_id"] == "feature_extract_v1"
    assert "prompt_hash" in prompts["items"][0]
    assert status2 == 200
    assert "items" in traces
    assert status3 == 200
    assert harness["trace_enabled"] is True


def test_ai_harness_prompt_update_route_records_history(dashboard):
    base_url, _ = dashboard

    detail_status, before, _ = request_json(base_url + "/api/ai-harness/prompts/feature_extract_v1")
    patch_status, updated, _ = request_json(
        base_url + "/api/ai-harness/prompts/feature_extract_v1",
        "PATCH",
        {"content": before["content"] + "\n# test edit", "note": "测试编辑"},
    )

    assert detail_status == 200
    assert patch_status == 200
    assert updated["content"].endswith("# test edit")
    assert updated["history"][0]["note"] == "测试编辑"


def test_create_job_route_forwards_prompt_id(dashboard):
    base_url, manager = dashboard

    _, created, _ = request_json(base_url + "/api/jobs", "POST", {
        "result": {"summary": {}, "risk_entities": [entity()]},
        "model": "qwen3:1.7b",
        "prompt_id": "feature_extract_v2_strict_en",
    })

    _, snapshot, _ = request_json(base_url + f"/api/jobs/{created['job_id']}")
    assert snapshot["prompt_id"] == "feature_extract_v2_strict_en"
    assert manager.get_job(created["job_id"])["prompt_id"] == "feature_extract_v2_strict_en"


def test_serves_frontend_for_ai_harness_routes(dashboard):
    base_url, _ = dashboard

    for path in ("/prompts", "/ai-traces", "/ai-observability"):
        with urlopen(base_url + path, timeout=3) as response:
            html = response.read().decode()
        assert "Feature Dashboard" in html


def test_ai_observability_routes_summarize_job_progress_and_events(dashboard):
    base_url, _ = dashboard

    _, created, _ = request_json(base_url + "/api/jobs", "POST", {
        "result": {"summary": {"total_raw_logs": 10, "total_template_windows": 6}, "risk_entities": [entity()]},
        "model": "qwen3:1.7b",
    })
    job_id = created["job_id"]
    for _ in range(50):
        _, snapshot, _ = request_json(base_url + f"/api/jobs/{job_id}")
        if snapshot["features"]:
            break

    status, summary, _ = request_json(base_url + "/api/ai-harness/observability/summary")
    progress_status, progress, _ = request_json(base_url + f"/api/ai-harness/jobs/{job_id}/progress")
    events_status, events, _ = request_json(base_url + f"/api/ai-harness/jobs/{job_id}/events")

    assert status == 200
    assert summary["current_job_id"] == job_id
    assert summary["candidate_feature_count"] == 1
    assert progress_status == 200
    assert progress["job_id"] == job_id
    assert progress["summary"]["risk_entities_total"] == 1
    assert progress["entities"][0]["status"] in {"candidate_generated", "waiting_review"}
    assert events_status == 200
    assert events["items"][0]["job_id"] == job_id
    assert "stage" in events["items"][0]


def test_serves_bundled_asset_with_correct_content_type(dashboard):
    base_url, _ = dashboard

    with urlopen(base_url + "/assets/app.js", timeout=3) as response:
        body = response.read().decode()

    assert "javascript" in response.headers["Content-Type"]
    assert "console.log" in body


def test_static_asset_path_cannot_escape_frontend_directory(dashboard):
    base_url, _ = dashboard

    with pytest.raises(HTTPError) as invalid:
        urlopen(base_url + "/assets/../index.html", timeout=3)

    assert invalid.value.code == 404
