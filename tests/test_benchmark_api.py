from __future__ import annotations

import json
import threading
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from logrisk.feature_jobs import FeatureJobManager
from pipeline.dashboard_server import build_server


def request_json(url: str, method: str = "GET", payload: object | None = None):
    data = json.dumps(payload).encode() if payload is not None else None
    request = Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=5) as response:
        return response.status, json.load(response)


@pytest.fixture
def benchmark_api(tmp_path):
    frontend = tmp_path / "index.html"
    frontend.write_text("<!doctype html>", encoding="utf-8")
    server = build_server(
        "127.0.0.1",
        0,
        manager=FeatureJobManager(auto_start=False),
        frontend_path=frontend,
        drain_quality_root=tmp_path / "drain_quality",
        semantic_root=tmp_path / "semantic",
        database_path=tmp_path / "state" / "logrisk.sqlite3",
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


def wait_for_run(base: str, run_id: str) -> dict[str, object]:
    for _ in range(100):
        _, detail = request_json(base + "/api/benchmark-center/runs/" + run_id)
        if detail["run"]["status"] in {"completed", "failed", "cancelled"}:
            return detail
        time.sleep(0.02)
    raise AssertionError("benchmark run did not finish")


def test_benchmark_api_creates_fake_run_and_exposes_unified_views(benchmark_api):
    _, suites = request_json(benchmark_api + "/api/benchmark-center/suites")
    status, created = request_json(benchmark_api + "/api/benchmark-center/runs", "POST", {
        "suite_id": suites["items"][0]["suite_id"],
        "mode": "fake",
        "prompt_id": "feature_extract_v3_compact_strict_json_en",
        "model_profile_id": "fake-model",
        "idempotency_key": "api-fake-run",
    })
    detail = wait_for_run(benchmark_api, created["run_id"])
    _, overview = request_json(benchmark_api + "/api/benchmark-center/overview")
    _, trends = request_json(benchmark_api + "/api/benchmark-center/trends")
    _, leaderboard = request_json(benchmark_api + "/api/benchmark-center/leaderboard")
    _, cases = request_json(benchmark_api + f"/api/benchmark-center/runs/{created['run_id']}/cases")
    _, artifacts = request_json(benchmark_api + f"/api/benchmark-center/runs/{created['run_id']}/artifacts")

    assert status == 202
    assert created["request_id"].startswith("request-")
    assert created["resource_id"] == created["run_id"]
    assert detail["run"]["status"] == "completed"
    assert overview["completed_run_count"] == 1
    assert overview["source_assets"]["canonical_eval_cases"] > 0
    assert "ai_traces" in overview["source_assets"]
    assert trends["items"][0]["run_id"] == created["run_id"]
    assert leaderboard["items"][0]["model_profile_id"] == "fake-model"
    assert cases["pagination"]["total"] > 0
    assert artifacts["items"] == []
    assert "raw_log" not in json.dumps(detail)


def test_real_run_without_confirmation_returns_422(benchmark_api):
    _, suites = request_json(benchmark_api + "/api/benchmark-center/suites")

    with pytest.raises(HTTPError) as error:
        request_json(benchmark_api + "/api/benchmark-center/runs", "POST", {
            "suite_id": suites["items"][0]["suite_id"],
            "mode": "real",
            "prompt_id": "prompt-a",
            "model_profile_id": "profile-a",
            "case_limit": 1,
            "timeout_seconds": 30,
            "retry_count": 0,
            "budget_units": 1000,
            "confirmed": False,
        })

    payload = json.load(error.value)
    assert error.value.code == 422
    assert payload["code"] == "confirmation_required"
    assert payload["request_id"].startswith("request-")


def test_comparison_and_gate_api_persist_human_decision_record(benchmark_api):
    _, suites = request_json(benchmark_api + "/api/benchmark-center/suites")
    run_ids = []
    for key in ("base", "candidate"):
        _, created = request_json(benchmark_api + "/api/benchmark-center/runs", "POST", {
            "suite_id": suites["items"][0]["suite_id"], "mode": "fake", "idempotency_key": key,
        })
        wait_for_run(benchmark_api, created["run_id"])
        run_ids.append(created["run_id"])
    payload = {"baseline_run_id": run_ids[0], "candidate_run_id": run_ids[1], "thresholds": {"min_pass_rate": 0.8}, "operator": "qa"}
    comparison_status, comparison = request_json(benchmark_api + "/api/benchmark-center/comparisons", "POST", payload)
    gate_status, gate = request_json(benchmark_api + "/api/benchmark-center/gates/evaluate", "POST", payload)

    assert comparison_status == 200
    assert comparison["decision"] == "passed"
    assert gate_status == 201
    assert gate["gate_id"].startswith("gate-")
    assert gate["operator"] == "qa"
