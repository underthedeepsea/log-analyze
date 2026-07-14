from __future__ import annotations

import json
import threading
from urllib.request import Request, urlopen

import pytest

from logrisk.feature_jobs import FeatureJobManager
from pipeline.dashboard_server import build_server


def request_json(url: str, method: str = "GET", payload: object | None = None):
    data = json.dumps(payload).encode() if payload is not None else None
    request = Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=3) as response:
        return response.status, json.load(response)


@pytest.fixture
def drain_api(tmp_path):
    frontend = tmp_path / "index.html"
    frontend.write_text("<!doctype html>", encoding="utf-8")
    server = build_server(
        "127.0.0.1",
        0,
        manager=FeatureJobManager(auto_start=False),
        frontend_path=frontend,
        drain_quality_root=tmp_path / "drain_quality",
        cors_origins=["http://127.0.0.1:3000"],
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


def test_dataset_annotation_and_eval_api_contract(drain_api):
    record = {
        "schema_version": "drain_gold_v1",
        "record_id": "log-1",
        "source_type": "system",
        "component": "kernel",
        "message_core": "error 5",
        "gold_group_id": "kernel-error",
        "gold_template": "error <NUM>",
        "semantic_fields": {},
        "protected_tokens": ["error"],
        "expected_risk_type": "kernel_error",
        "annotation_status": "approved",
    }
    status, dataset = request_json(drain_api + "/api/drain-quality/datasets", "POST", {"name": "gold", "records": [record]})
    _, listing = request_json(drain_api + "/api/drain-quality/datasets")
    _, annotation = request_json(drain_api + "/api/drain-quality/annotations", "POST", {"cluster_id": "c1", "action": "accept", "reviewer": "alice"})
    _, run = request_json(drain_api + "/api/drain-quality/eval-runs", "POST", {
        "dataset_id": dataset["dataset_id"],
        "profile_id": "legacy-default",
        "predictions": [{"record_id": "log-1", "predicted_group_id": "c1", "predicted_template": "error <NUM>", "predicted_semantic_fields": {}}],
    })

    assert status == 201
    assert listing["items"][0]["dataset_id"] == dataset["dataset_id"]
    assert annotation["schema_version"] == "drain_annotation_event_v1"
    assert run["status"] == "completed"
    assert run["metrics"]["labeled"]["pairwise_grouping_f1"] == 1.0


def test_health_cors_and_template_governance_api(drain_api):
    request = Request(drain_api + "/api/health", headers={"Origin": "http://127.0.0.1:3000"})
    with urlopen(request, timeout=3) as response:
        health = json.load(response)
        allowed_origin = response.headers.get("Access-Control-Allow-Origin")
    _, imported = request_json(drain_api + "/api/drain-quality/templates/import", "POST", {
        "templates": [{"template_hash": "h1", "template": "error <NUM>", "component": "kernel", "count": 2}],
    })
    item = imported["items"][0]
    _, edited = request_json(drain_api + "/api/drain-quality/templates/h1/changes", "POST", {
        "action": "edit", "template": "error <CODE>", "expected_version": item["version"], "confirmed": True,
    })
    _, listing = request_json(drain_api + "/api/drain-quality/templates")

    assert health["service"] == "logrisk-dashboard"
    assert allowed_origin == "http://127.0.0.1:3000"
    assert edited["original_template"] == "error <NUM>"
    assert listing["items"][0]["effective_template"] == "error <CODE>"


def test_profile_promotion_requires_confirmation(drain_api):
    _, profiles = request_json(drain_api + "/api/drain-quality/profiles")
    profile_id = profiles["items"][0]["profile_id"]

    with pytest.raises(Exception):
        request_json(drain_api + f"/api/drain-quality/profiles/{profile_id}/promote", "POST", {"confirmed": False})

    status, promoted = request_json(drain_api + f"/api/drain-quality/profiles/{profile_id}/promote", "POST", {"confirmed": True, "reviewer": "operator"})
    assert status == 200
    assert promoted["status"] == "promoted"
