import json
import time
import shutil
import threading
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from logrisk.ai_harness.prompt_registry import PromptRegistry
from logrisk.ai_harness.model_profile import ModelProfileRegistry
from logrisk.ai_harness.trace_logger import AITraceLogger
from logrisk.feature_jobs import FeatureJobManager
from pipeline.dashboard_server import build_server, parse_args


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
        "prompt_id": "feature_extract_v3_compact_strict_json_en",
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
        database_path=tmp_path / "state" / "logrisk.sqlite3",
    )
    prompt_dir = tmp_path / "prompts"
    shutil.copytree(Path("prompts"), prompt_dir)
    server.prompt_registry = PromptRegistry(  # type: ignore[attr-defined]
        prompt_dir,
        Path("configs") / "ai_harness.yaml",
        tmp_path / "state" / "prompt_versions.json",
    )
    server.trace_logger = AITraceLogger(tmp_path / "state" / "ai_traces.jsonl")  # type: ignore[attr-defined]
    profile_config = tmp_path / "model_profiles.yaml"
    shutil.copyfile(Path("configs") / "model_profiles.yaml", profile_config)
    server.model_profiles = ModelProfileRegistry(profile_config)  # type: ignore[attr-defined]
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


def test_database_status_and_restart_candidate_configuration_are_available(dashboard):
    base_url, _ = dashboard

    status, current, _ = request_json(base_url + "/api/system/database")
    saved_status, saved, _ = request_json(base_url + "/api/system/database/config", "POST", {
        "provider": "postgres",
        "host": "postgres.internal",
        "port": 5432,
        "database": "logrisk",
        "user": "logrisk_app",
        "sslmode": "require",
        "password_env": "LOGRISK_POSTGRES_PASSWORD",
        "password": "never-store-this",
    })
    health_status, health, _ = request_json(base_url + "/api/health")

    assert status == saved_status == health_status == 200
    assert current["runtime"]["provider"] == "sqlite"
    assert saved["candidate"]["provider"] == "postgres"
    assert saved["candidate"]["password_configured"] is False
    assert saved["restart_required"] is True
    assert health["version"] == "1.34.0"
    assert health["storage"] == "sqlite"


def test_runtime_external_identity_configuration(tmp_path):
    frontend = tmp_path / "dist" / "index.html"
    frontend.parent.mkdir()
    frontend.write_text("<!doctype html><title>Runtime Identity</title>", encoding="utf-8")
    runtime_config = tmp_path / "runtime.yaml"
    runtime_config.write_text(
        """runtime:
  identity:
    enabled: true
    allow_loopback_bypass: false
    trusted_proxy_cidrs: [127.0.0.0/8]
    write_roles: [logrisk:operator]
""",
        encoding="utf-8",
    )
    server = build_server(
        "127.0.0.1",
        0,
        manager=FeatureJobManager(extractor=lambda *_args, **_kwargs: [], auto_start=True),
        frontend_path=frontend,
        database_path=tmp_path / "state" / "logrisk.sqlite3",
        state_root=tmp_path / "state",
        runtime_config_path=runtime_config,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        with pytest.raises(HTTPError) as rejected:
            request_json(base_url + "/api/runtime/retention/preview", "POST", {})
        allowed = Request(
            base_url + "/api/runtime/retention/preview",
            data=b"{}",
            method="POST",
            headers={
                "Content-Type": "application/json",
                "X-LOGRISK-Actor": "gateway-user",
                "X-LOGRISK-Roles": "logrisk:operator",
                "X-Request-ID": "runtime-config-test",
            },
        )
        with urlopen(allowed, timeout=3) as response:
            payload = json.load(response)

        assert rejected.value.code == 403
        assert json.load(rejected.value)["error_code"] == "runtime_identity_required"
        assert payload["request_id"] == "runtime-config-test"
        assert payload["maintenance"]["status"] == "completed"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_dashboard_cli_accepts_explicit_postgres_provider_and_url():
    args = parse_args([
        "--database-provider", "postgres",
        "--database-url", "postgresql://user:secret@db.example/logrisk",
    ])

    assert args.database_provider == "postgres"
    assert args.database_url == "postgresql://user:secret@db.example/logrisk"


def test_risk_semantic_and_node_risk_routes(dashboard):
    base_url, _ = dashboard

    semantic_status, semantics, _ = request_json(base_url + "/api/semantics")
    test_status, tested, _ = request_json(base_url + "/api/semantics/test", "POST", {
        "rule": next(item for item in semantics["items"] if item["id"] == "builtin.gpu.xid.79"),
        "positive_samples": ["NVRM: Xid (0000:65:00): 79, GPU has fallen off the bus"],
        "negative_samples": ["NVRM: Xid (0000:65:00): 35, Video processor exception"],
    })
    node_status, nodes, _ = request_json(base_url + "/api/node-risks")

    assert semantic_status == 200
    assert tested["valid"] is True
    assert tested["results"][0]["semantic_fields"] == {"pci_bdf": "0000:65:00", "xid_code": 79}
    assert test_status == node_status == 200
    assert nodes["items"] == []


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
    assert payload["drain_config"]["config_id"] == "baseline"
    assert payload["drain_config"]["content_hash"]


def test_large_upload_routes_create_input_job_and_result(dashboard):
    base_url, _ = dashboard
    content = b"error one\nerror two\n"

    status, session, _ = request_json(base_url + "/api/uploads", "POST", {
        "filename": "messages",
        "size_bytes": len(content),
        "chunk_size_bytes": 8,
    })
    upload_id = session["upload_id"]
    for index, start in enumerate(range(0, len(content), 8)):
        request = Request(
            base_url + f"/api/uploads/{upload_id}/chunks/{index}",
            data=content[start:start + 8],
            method="PUT",
            headers={"Content-Type": "application/octet-stream"},
        )
        with urlopen(request, timeout=3) as response:
            chunk = json.load(response)
        assert chunk["received"] is True
    complete_status, completed, _ = request_json(base_url + f"/api/uploads/{upload_id}/complete", "POST", {})
    job_status, job, _ = request_json(base_url + "/api/inputs/analyze-upload", "POST", {
        "upload_id": upload_id,
        "filename": "messages",
    })

    for _ in range(50):
        _, progress, _ = request_json(base_url + f"/api/input-jobs/{job['input_job_id']}")
        if progress["status"] == "completed":
            break
        time.sleep(0.02)
    result_status, result, _ = request_json(base_url + f"/api/input-jobs/{job['input_job_id']}/result")
    streaming_status, streaming, _ = request_json(base_url + "/api/streaming/tasks")

    assert status == 200
    assert complete_status == 200
    assert completed["status"] == "completed"
    assert job_status == 202
    assert job["drain_config_id"] == "baseline"
    assert job["drain_config_hash"]
    assert progress["drain_config_version"] == 1
    assert result_status == 200
    assert result["result"]["summary"]["total_raw_logs"] == 2
    assert streaming_status == 200
    assert streaming["tasks"][0]["input_job_id"] == job["input_job_id"]
    assert streaming["tasks"][0]["status"] == "completed"


def test_streaming_routes_expose_disabled_kafka_capability(dashboard):
    base_url, _ = dashboard

    tasks_status, tasks, _ = request_json(base_url + "/api/streaming/tasks")
    sources_status, sources, _ = request_json(base_url + "/api/streaming/sources")
    with pytest.raises(HTTPError) as unavailable:
        request_json(base_url + "/api/streaming/kafka/start", "POST", {})

    assert tasks_status == 200
    assert tasks == {"tasks": []}
    assert sources_status == 200
    assert sources["sources"]["kafka"]["enabled"] is False
    assert sources["sources"]["kafka"]["reason"] == "Kafka 消费适配器未注册"
    assert unavailable.value.code == 422
    assert json.load(unavailable.value)["code"] == "kafka_adapter_unavailable"


def test_rule_list_route(dashboard):
    base_url, _ = dashboard

    status, payload, _ = request_json(base_url + "/api/rules")

    assert status == 200
    assert payload == {"rules": []}


def test_rule_governance_api_versions_status_feedback_and_rollback(dashboard):
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
    request_json(
        base_url + f"/api/jobs/{job_id}/features/feature-node-a",
        "PATCH",
        {"status": "approved"},
    )

    list_status, listed, _ = request_json(base_url + "/api/rule-governance/rules")
    rule = listed["items"][0]
    status_code, changed, _ = request_json(
        base_url + f"/api/rule-governance/rules/{rule['rule_id']}/status",
        "POST",
        {"status": "disabled", "expected_version": 1, "operator": "reviewer-a", "reason": "复审停用"},
    )
    feedback_status, feedback, _ = request_json(
        base_url + f"/api/rule-governance/rules/{rule['rule_id']}/feedback",
        "POST",
        {"outcome": "false_positive", "operator": "reviewer-a", "note": "预期行为"},
    )
    detail_status, detail, _ = request_json(base_url + f"/api/rule-governance/rules/{rule['rule_id']}")
    queue_status, queue, _ = request_json(base_url + "/api/rule-governance/review-queue")
    rollback_status, rollback, _ = request_json(
        base_url + f"/api/rule-governance/rules/{rule['rule_id']}/rollback",
        "POST",
        {"target_version": 1, "expected_version": 2, "confirmed": True, "operator": "reviewer-a", "reason": "恢复首版"},
    )

    assert list_status == 200
    assert listed["schema_version"] == "rule_asset_list_v1"
    assert rule["status"] == "active"
    assert status_code == 200
    assert changed["request_id"].startswith("request-")
    assert changed["version"] == 2
    assert feedback_status == 201
    assert feedback["version"] == 2
    assert detail_status == 200
    assert [item["version"] for item in detail["versions"]] == [2, 1]
    assert queue_status == 200
    assert queue["items"][0]["rule_id"] == rule["rule_id"]
    assert rollback_status == 200
    assert rollback["version"] == 3


def test_rule_governance_api_returns_conflict_for_stale_version(dashboard):
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
    request_json(base_url + f"/api/jobs/{job_id}/features/feature-node-a", "PATCH", {"status": "approved"})
    _, listed, _ = request_json(base_url + "/api/rule-governance/rules")
    rule_id = listed["items"][0]["rule_id"]
    request_json(
        base_url + f"/api/rule-governance/rules/{rule_id}/status",
        "POST",
        {"status": "disabled", "expected_version": 1, "operator": "reviewer-a", "reason": "停用"},
    )

    with pytest.raises(HTTPError) as conflict:
        request_json(
            base_url + f"/api/rule-governance/rules/{rule_id}/status",
            "POST",
            {"status": "active", "expected_version": 1, "operator": "reviewer-b", "reason": "恢复"},
        )

    assert conflict.value.code == 409
    error = json.load(conflict.value)
    assert error["code"] == "version_conflict"
    assert error["request_id"].startswith("request-")


def test_system_metrics_route_returns_daily_llm_volume(dashboard):
    base_url, _ = dashboard

    status, payload, _ = request_json(base_url + "/api/metrics")

    assert status == 200
    assert payload == {"today_llm_logs": 0}


def test_config_route_exposes_ai_cache_flag(dashboard):
    base_url, _ = dashboard

    status, payload, _ = request_json(base_url + "/api/config")

    assert status == 200
    assert payload["ai_cache_enabled"] is True


def test_model_profiles_route_exposes_default_profile(dashboard):
    base_url, _ = dashboard

    status, payload, _ = request_json(base_url + "/api/ai-harness/model-profiles")

    assert status == 200
    assert payload["default_profile_id"] == "qwen3_1_7b_fast"
    assert payload["profiles"][0]["thinking_enabled"] is False
    assert payload["profiles"][0]["evidence_budget"]["max_templates"] == 6
    assert payload["profiles"][0]["connection_enabled"] is True
    assert payload["profiles"][0]["connection_ready"] is True


def test_model_profiles_route_creates_profile(dashboard):
    base_url, _ = dashboard

    status, payload, _ = request_json(base_url + "/api/ai-harness/model-profiles", "POST", {
        "profile_id": "custom_fast",
        "display_name": "Custom Fast",
        "provider": "ollama",
        "model": "qwen3.5:4b-mlx",
        "default_prompt_id": "feature_extract_v3_compact_strict_json_en",
        "thinking_enabled": False,
        "evidence_budget": {"max_templates": 4, "max_template_chars": 180, "max_affected_entities": 10, "max_evidence_chars": 6000},
    })
    _, listed, _ = request_json(base_url + "/api/ai-harness/model-profiles")

    assert status == 200
    assert payload["profile_id"] == "custom_fast"
    assert any(item["profile_id"] == "custom_fast" for item in listed["profiles"])


def test_model_connections_api_creates_lists_and_updates_remote_connection(dashboard, monkeypatch):
    base_url, _ = dashboard
    monkeypatch.setenv("REMOTE_LLM_KEY", "secret")

    status, saved, _ = request_json(base_url + "/api/ai-harness/connections", "POST", {
        "connection_id": "remote-main",
        "display_name": "远端主模型",
        "provider": "openai_compatible",
        "base_url": "https://llm.example/v1",
        "api_key_env": "REMOTE_LLM_KEY",
        "timeout_seconds": 60,
        "enabled": True,
    })
    _, listed, _ = request_json(base_url + "/api/ai-harness/connections")
    patch_status, updated, _ = request_json(base_url + "/api/ai-harness/connections/remote-main", "PATCH", {"enabled": False})

    assert status == 200
    assert saved["api_key_configured"] is True
    assert "secret" not in json.dumps(saved)
    assert any(item["connection_id"] == "remote-main" for item in listed["items"])
    assert patch_status == 200
    assert updated["enabled"] is False


def test_extension_adapter_api_lists_committed_template_without_secret_values(dashboard):
    base_url, _ = dashboard

    status, payload, _ = request_json(base_url + "/api/ai-harness/extensions")

    assert status == 200
    assert payload["items"][0]["adapter_id"] == "token_auth_template"
    assert payload["items"][0]["credential_fields"] == {"access_token": "访问 Token 环境变量"}
    assert "Token 实际值" in json.dumps(payload, ensure_ascii=False)


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
    assert prompts["current_prompt_id"] == "feature_extract_v3_compact_strict_json_en"
    assert {item["prompt_id"] for item in prompts["items"]} >= {"feature_extract_v1", "agent_plan_v1"}
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


def test_create_job_route_forwards_model_profile_id(dashboard):
    base_url, manager = dashboard

    _, created, _ = request_json(base_url + "/api/jobs", "POST", {
        "result": {"summary": {}, "risk_entities": [entity()]},
        "model": "qwen3:1.7b",
        "model_profile_id": "qwen3_1_7b_fast",
    })

    _, snapshot, _ = request_json(base_url + f"/api/jobs/{created['job_id']}")
    assert snapshot["model_profile_id"] == "qwen3_1_7b_fast"
    assert manager.get_job(created["job_id"])["model_profile_id"] == "qwen3_1_7b_fast"


def test_create_job_route_forwards_retry_count(dashboard):
    base_url, manager = dashboard

    _, created, _ = request_json(base_url + "/api/jobs", "POST", {
        "result": {"summary": {}, "risk_entities": [entity()]},
        "model": "qwen3:1.7b",
        "retry_count": 2,
    })

    _, snapshot, _ = request_json(base_url + f"/api/jobs/{created['job_id']}")
    assert snapshot["retry_count"] == 2
    assert manager.get_job(created["job_id"])["retry_count"] == 2


def test_serves_frontend_for_ai_harness_routes(dashboard):
    base_url, _ = dashboard

    for path in ("/prompts", "/ai-traces", "/ai-observability", "/model-profiles", "/rules", "/multi-source"):
        with urlopen(base_url + path, timeout=3) as response:
            html = response.read().decode()
        assert "Feature Dashboard" in html


def test_multi_source_routes_expose_sanitized_entity_timeline(dashboard):
    base_url, _ = dashboard
    status, summary, _ = request_json(base_url + "/api/multi-source/summary")
    entities_status, entities, _ = request_json(base_url + "/api/multi-source/entities")
    rules_status, rules, _ = request_json(base_url + "/api/multi-source/rules")

    assert status == entities_status == rules_status == 200
    assert summary["schema_version"] == "multi_source_summary_v1"
    assert entities["schema_version"] == "multi_source_entities_v1"
    assert rules["schema_version"] == "multi_source_rules_v1"
    assert rules["items"][0]["rule_id"] == "exact-entity-cross-source"


def test_multi_source_rule_route_persists_complete_editor_update(dashboard):
    base_url, _ = dashboard
    _, rules, _ = request_json(base_url + "/api/multi-source/rules")
    rule = rules["items"][0]

    status, updated, _ = request_json(
        base_url + "/api/multi-source/rules/exact-entity-cross-source",
        "PATCH",
        {
            "display_name": "节点运行时信号",
            "enabled": True,
            "source_pairs": [["kernel", "containerd"]],
            "max_gap_seconds": 180,
            "min_risk_score": 45,
            "min_count": 2,
            "confidence": 0.87,
            "expected_version": rule["version"],
        },
    )

    assert status == 200
    assert updated["display_name"] == "节点运行时信号"
    assert updated["source_pairs"] == [["kernel", "containerd"]]
    assert updated["max_gap_seconds"] == 180
    assert updated["min_risk_score"] == 45.0
    assert updated["min_count"] == 2
    assert updated["confidence"] == 0.87
    _, reloaded, _ = request_json(base_url + "/api/multi-source/rules")
    assert reloaded["items"][0]["version"] == rule["version"] + 1


def test_serves_branded_help_manual(dashboard):
    base_url, _ = dashboard

    with urlopen(base_url + "/help", timeout=3) as response:
        content_type = response.headers["Content-Type"]
        html = response.read().decode()

    assert content_type == "text/html; charset=utf-8"
    assert "/assets/logrisk-app-icon-orange-v2.png" in html
    assert "多来源关联" in html
    assert "source_pairs" in html
    assert 'id="manual-search"' in html
    assert 'id="manual-search-status"' in html
    assert "function filterManualSections" in html
    assert "#toc a[hidden]{display:none!important;}" in html
    assert "折叠分组导航" in html
    assert "AI 分析观测" in html
    assert "安全 Replay" in html

    with urlopen(base_url + "/manual-assets/01-overview.png", timeout=3) as response:
        assert response.headers["Content-Type"] == "image/png"
        assert response.read(8).startswith(b"\x89PNG")


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
    assert progress["model_profile"]["profile_id"] == "qwen3_1_7b_fast"
    assert progress["summary"]["risk_entities_total"] == 1
    assert progress["entities"][0]["status"] in {"candidate_generated", "waiting_review"}
    assert events_status == 200
    assert events["items"][0]["job_id"] == job_id
    assert "stage" in events["items"][0]

    observations_status, observations, _ = request_json(
        base_url + f"/api/observability-v2/observations?job_id={job_id}"
    )
    assert observations_status == 200
    assert observations["items"][0]["job_id"] == job_id
    observation_id = observations["items"][0]["observation_id"]

    timeline_status, timeline, _ = request_json(
        base_url + f"/api/observability-v2/observations/{observation_id}/timeline"
    )
    metrics_status, metrics, _ = request_json(base_url + "/api/observability-v2/metrics")
    assert timeline_status == 200
    assert timeline["items"]
    assert {
        "input",
        "normalize",
        "drain3",
        "aggregate",
        "evidence",
        "prompt",
        "model",
        "parse",
        "schema",
        "evaluator",
        "candidate",
    } <= {item["stage"] for item in timeline["items"]}
    assert metrics_status == 200
    assert metrics["observation_count"] >= 1
    assert metrics["span_count"] >= 1


def test_replay_api_returns_stable_error_code(dashboard):
    base_url, _ = dashboard

    with pytest.raises(HTTPError) as invalid:
        request_json(
            base_url + "/api/observability-v2/replays",
            "POST",
            {"source_trace_id": "missing", "mode": "historical", "confirmed": True},
        )

    payload = json.load(invalid.value)
    assert invalid.value.code == 400
    assert payload["code"] == payload["error_code"]
    assert payload["request_id"].startswith("request-")


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
