import json
import threading
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

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
    }


@pytest.fixture
def dashboard(tmp_path):
    frontend = tmp_path / "index.html"
    frontend.write_text("<!doctype html><title>Feature Dashboard</title>", encoding="utf-8")
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
    )
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
