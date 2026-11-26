from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from urllib.error import URLError

from logrisk.agentic.models import AgentRunRequest
from logrisk.agentic.repository import AgentRepository
from logrisk.agentic.workflow_repository import WorkflowRepository
from logrisk.ai_harness.providers.ollama import OllamaModelClient
from logrisk.application import ApiFacade, ApplicationConfig, build_application_container
from logrisk.database import SQLiteDatabase
from logrisk.operational_ledgers import OperationalLedgerRepository, source_identity
from logrisk.stream_overview import DISPLAY_ZONE, StreamOverview
from pipeline.dashboard_server import build_server
from tests.server_risk_fixtures import request_http
from tests.test_agent_workflow_repository import _compiled, _runtime_snapshot
from tests.test_risk_overview import seed_event

BASE = "/api/stream-overview"
NOW = datetime(2026, 11, 23, 4, 30, tzinfo=timezone.utc)
SECRET = "sk-private-fixture-material"


def seed_overview(database, risk_service, *, now=None):
    """Seed a complete read-only overview test against an already isolated database."""
    now = now or datetime.now(timezone.utc)
    midnight = now.astimezone(DISPLAY_ZONE).replace(hour=0, minute=0, second=0, microsecond=0)
    today = min(now, midnight + timedelta(minutes=15)).astimezone(timezone.utc)
    yesterday = (midnight - timedelta(seconds=1)).astimezone(timezone.utc)
    seed_event(risk_service, "overview-prev", obj="previous", first=yesterday.isoformat())
    seed_event(risk_service, "overview-high", obj="first", first=today.isoformat())
    seed_event(risk_service, "overview-low", obj="second", level="low", first=today.isoformat())
    ledger = OperationalLedgerRepository(database)
    source = dict(environment="production", scope_key="default", source_kind="kafka", identity_digest="sample")
    source["source_id"] = source_identity(**source)
    for index, (at, start, end) in enumerate(((yesterday, 0, 10), (today, 5, 25))):
        ledger.record_ingestion_batch(
            batch_id=f"overview-batch-{index}", source=source, input_job_id="overview-source",
            checkpoint_key=f"overview-checkpoint-{index}", parser_version="test",
            ranges=[{"partition_key": "0", "start": start, "end": end}], actual_count=end-start, committed_at=at,
        )
    calls = (
        ("production", today, "workbench-model", {"input_tokens": 12, "output_tokens": 5}, "succeeded"),
        ("production", today, SECRET, {"input_tokens": 3}, "failed"),
        ("production", today, "unreceipted", None, "failed"),
        ("production", yesterday, "other-day", {"total_tokens": 200}, "succeeded"),
        ("local-test", today, "test-model", {"total_tokens": 200}, "succeeded"),
    )
    for index, (environment, at, model, usage, status) in enumerate(calls):
        call_id = f"overview-{index}"
        ledger.prepare_call(
            call_id=call_id, logical_call_id=call_id, attempt_index=0, environment=environment,
            scope_key="default", call_kind="provider", provider="ollama", model=model, caller_kind="feature_extractor",
        )
        ledger.start_call(call_id, started_at=at)
        ledger.finish_call(call_id, status=status, usage=usage, finished_at=at)
    workflows = WorkflowRepository(database)
    workflow = workflows.create_workflow(_compiled(), actor="fixture-actor", idempotency_key="overview-definition")
    run_id = workflows.create_run(
        workflow["workflow_id"], source_job_id="overview-job", entity_id="node-01", entity_type="node",
        model_profile_id="qwen", prompt_id="agent_plan_v1", actor="fixture-actor", roles=("operator",),
        request_id="overview-request", idempotency_key="overview-run", runtime_snapshot=_runtime_snapshot(),
    )["workflow_run_id"]
    workflows.transition_run(run_id, "running", allowed_from={"queued"})
    workflows.claim_node(run_id, "a")
    workflows.claim_node(run_id, "b")
    workflows.finish_node(run_id, "a", status="completed", used_tool_calls=2, result={"private_path": SECRET})
    workflows.finish_node(run_id, "b", status="failed", error_code=SECRET, error_summary=SECRET)
    workflows.transition_run(run_id, "failed", allowed_from={"running"}, error_code=SECRET)
    day = today.isoformat()
    with database.transaction() as conn:
        conn.execute("UPDATE agent_workflow_runs SET created_at=?,updated_at=?,started_at=?,completed_at=? WHERE workflow_run_id=?", (day, day, day, day, run_id))
        conn.execute("UPDATE agent_workflow_events SET created_at=? WHERE workflow_run_id=?", (day, run_id))
        conn.execute("UPDATE agent_workflow_nodes SET started_at=?,completed_at=? WHERE workflow_run_id=? AND attempt>0", (day, day, run_id))
        conn.execute(
            "INSERT INTO streaming_tasks(task_id,source_kind,source_identity_json,config_hash,status,stage,cursor_json,task_json,created_at,updated_at) "
            "VALUES ('overview-source','file','{}','test','completed','COMPLETED','{}','{}',?,?)",
            (day, day),
        )
        conn.execute(
            "INSERT INTO streaming_window_commits(task_id,window_id,cursor_json,summary_json,committed_at) VALUES (?,?,?,?,?)",
            ("overview-source", "window-1", "{}", '{"record_count":8,"template_count":2}', day),
        )
        conn.execute(
            "INSERT INTO unknown_template_queue(task_id,template_hash,component,window_start,config_hash,occurrence_count,template_json,status,created_at,updated_at) "
            "VALUES ('overview-source','template','test',?,'test',2,'{}','open',?,?)", (day, day, day),
        )
        conn.execute(
            "INSERT INTO feature_jobs(job_id,status,job_json,created_at,updated_at) VALUES ('overview-job','completed','{}',?,?)",
            (day, day),
        )
        for candidate_id in ("candidate-1", "candidate-2"):
            conn.execute(
                "INSERT INTO feature_candidates(candidate_id,job_id,entity_id,status,candidate_json,created_at,updated_at) "
                "VALUES (?,'overview-job','node-01','pending','{}',?,?)",
                (candidate_id, day, day),
            )
            conn.execute(
                "INSERT INTO approval_candidate_projection(candidate_id,review_key,status,semantic_safe,ambiguous,importance_rank,risk_score,created_at,entity_key,occurrence_count,group_json) "
                "VALUES (?,'shared-group','pending',1,0,0,0,?,'entity',1,'{}')",
                (candidate_id, day),
            )
        conn.execute(
            "INSERT INTO approved_rules(rule_id,signature,feature_type,rule_json,approved_at,updated_at) "
            "VALUES ('rule-1','signature','service','{}',?,?)", (day, day),
        )
        approved_at = (now - timedelta(minutes=5)).isoformat()
        decision = json.dumps({
            "status": "approved", "resolution_type": "manual", "approval_group_id": "closed-group", "approved_at": approved_at,
        })
        for index in range(2):
            conn.execute(
                "INSERT INTO approval_decisions(decision_id,request_key,actor_scope,payload_hash,candidate_id,result_json,created_at) "
                "VALUES (?,?,'operator','digest','closed-candidate',?,?)",
                (f"decision-{index}", f"request-{index}", decision, approved_at),
            )
    return run_id


def test_overview_aggregates_real_sources_without_exposing_payloads(tmp_path):
    server = build_server(
        "127.0.0.1", 0, state_root=tmp_path / "state", output_root=tmp_path / "output",
        database_path=tmp_path / "state/logrisk.sqlite3", database_provider="sqlite",
        ollama_checker=lambda: (_ for _ in ()).throw(AssertionError("Overview contacted model service")),
    )
    try:
        run_id = seed_overview(server.database, server.server_risk, now=NOW)
        overview = StreamOverview(server.database, server.server_risk, workflows_enabled=False, clock=NOW.isoformat)
        result = overview.read()
        assert result["timezone"] == "Asia/Shanghai"
        assert set(result) == {"as_of", "timezone", "risk", "usage", "workflows", "activity", "approvals"}
        risk = result["risk"]["data"]
        assert risk["today"]["hosts"] == risk["today"]["high_hosts"] == 1
        assert risk["today"]["groups"] == risk["today"]["recovery_groups"]["unknown"] == 2
        assert risk["today"]["known_occurrences"] == 14
        assert risk["trend"][-3:] == [
            {"date": "2026-11-21", "hosts": None, "high_hosts": None},
            {"date": "2026-11-22", "hosts": 1, "high_hosts": 1},
            {"date": "2026-11-23", "hosts": 1, "high_hosts": 1},
        ]
        usage = result["usage"]["data"]
        assert usage["known_tokens"] == 20 and usage["input_tokens"] == 15 and usage["output_tokens"] == 5
        assert usage["started_calls"] == 3 and usage["receipted_calls"] == 2 and usage["complete_calls"] == 1
        assert usage["total_tokens"] is None and usage["receipt_rate"] == 2 / 3
        assert usage["estimated_cost_usd"] is None
        assert usage["buckets"][0]["known_tokens"] == 20 and usage["buckets"][-1]["known_tokens"] is None
        assert usage["models"][1]["model"] is None
        activity = result["activity"]["data"]
        assert activity["today_records"] == 15 and activity["total_records"] == 25
        assert activity["seven_day_records"][-2]["records"] == 10 and activity["seven_day_records"][0]["records"] is None
        assert activity["committed_batch_records"]["today_records"] == 8
        assert activity["approved_rules"] == 1
        assert activity["drain"] == {"compression_rate": .75, "template_windows": 2, "unknown_templates": 1}
        approvals = result["approvals"]["data"]
        assert approvals["pending_groups"] == approvals["approved_last_hour"] == 1
        assert all(point["pending_groups"] is None for point in approvals["history"][:-1])
        assert approvals["history"][-1]["pending_groups"] == 1
        workflow = result["workflows"]["data"]
        assert workflow["enabled"] is False and workflow["queued"] is None
        assert workflow["runs"][0]["run_id"] == run_id
        detail = overview.workflows.overview_run(run_id)
        assert detail["status"] == "failed" and detail["candidate_count"] is None
        assert detail["nodes"][2]["depends_on"] == ["evidence", "rules"]
        assert detail["nodes"][2]["blocked_by"] == ["rules"]
        assert detail["nodes"][1]["result_code"] == "failed"
        assert SECRET not in repr((result, detail))
    finally:
        server.server_close()


def test_overview_http_and_django_gate_shared_summary_and_details(tmp_path, monkeypatch):
    import django
    from django.core.management import call_command
    from django.test import Client, override_settings

    monkeypatch.setenv("DJANGO_SETTINGS_MODULE", "tests.django_test_project.settings")
    django.setup()
    from logrisk_django import service_factory
    from tests.test_django_node_risk_api import config

    server = build_server(
        "127.0.0.1", 0, state_root=tmp_path / "standalone", output_root=tmp_path / "output",
        database_path=tmp_path / "standalone/logrisk.sqlite3", database_provider="sqlite",
        ollama_checker=lambda: (_ for _ in ()).throw(AssertionError("Overview contacted model service")),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        run_id = seed_overview(server.database, server.server_risk)
        base = f"http://127.0.0.1:{server.server_address[1]}"
        paths = (BASE, f"{BASE}/workflow-runs/{run_id}")
        status, summary, headers = request_http(base, paths[0])
        assert status == 200 and headers["Cache-Control"] == "no-store"
        assert summary["usage"]["data"]["known_tokens"] == 20
        assert request_http(base, paths[1])[1]["status"] == "failed"
        assert request_http(base, BASE + "?scope=anything")[0] == 422
        assert request_http(base, BASE + "/workflow-runs/" + SECRET)[0] == 404
        django_config = config(tmp_path / "django")
        with override_settings(LOGRISK=django_config):
            service_factory.clear_cached_container()
            call_command("logrisk_migrate", "--json")
            container = service_factory.get_container()
            django_run = seed_overview(container.database, container.server_risk)
            response = Client().get(BASE)
            assert response.status_code == 200 and response["Cache-Control"] == "no-store"
            assert response.json()["usage"]["data"]["known_tokens"] == 20
            assert Client().get(f"{BASE}/workflow-runs/{django_run}").json()["status"] == "failed"
            assert Client().post(BASE, data={}).status_code == 405
        denied = dict(django_config, identity_resolver="tests.django_test_project.resolver.AnonymousIdentityResolver")
        with override_settings(LOGRISK=denied):
            service_factory.clear_cached_container()
            for path in (BASE, f"{BASE}/workflow-runs/{django_run}"):
                response = Client().get(path, HTTP_X_PACAS_ACTOR="spoofed")
                assert response.status_code == 403 and response.json()["code"] == "forbidden"
    finally:
        service_factory.clear_cached_container()
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_default_feature_job_feed_counts_provider_attempts_without_counting_cache_hits(tmp_path, monkeypatch):
    from tests.test_operational_usage import _feature_entity, _feature_payload

    sent = []

    def opener(request, *, timeout):
        sent.append(request)
        if len(sent) == 1:
            raise URLError("fixture retry")
        return BytesIO(json.dumps(_feature_payload()).encode())

    def fake_client(_connection):
        return OllamaModelClient("http://127.0.0.1:1", opener=opener)

    monkeypatch.setattr("logrisk.application.container.create_model_client", fake_client)
    container = build_application_container(ApplicationConfig(
        project_root=Path(__file__).parents[1], state_root=tmp_path / "state", output_root=tmp_path / "output",
        database_path=tmp_path / "state/logrisk.sqlite3", database_provider="sqlite", feature_jobs_auto_start=False,
    ))
    jobs = container.feature_jobs
    payload = {"risk_entities": [_feature_entity()]}
    for _ in range(2):
        job_id = jobs.create_job(payload, model="qwen3:1.7b", retry_count=1)
        jobs.run_job(job_id)
        assert jobs.get_job(job_id)["status"] == "completed"
    assert len(sent) == 2
    overview = StreamOverview(container.database, container.server_risk, workflows_enabled=False)
    usage = overview.read()["usage"]["data"]
    assert usage["started_calls"] == 2 and usage["receipted_calls"] == 1
    assert usage["input_tokens"] == 10 and usage["output_tokens"] == 4
    assert usage["known_tokens"] == 14 and usage["total_tokens"] is None


def test_overview_workload_includes_batches_from_normal_input_job(tmp_path):
    container = build_application_container(ApplicationConfig(
        project_root=Path(__file__).parents[1], state_root=tmp_path / "state", output_root=tmp_path / "output",
        database_path=tmp_path / "state/logrisk.sqlite3", database_provider="sqlite", feature_jobs_auto_start=False,
    ))
    payload = b"fixture alpha\nfixture beta\nfixture gamma\n"
    upload = container.upload_store.create(filename="workload.log", size_bytes=len(payload))
    upload_id = upload["upload_id"]
    container.upload_store.append_chunk(upload_id=upload_id, index=0, data=payload)
    container.upload_store.complete(upload_id=upload_id)
    job = container.input_jobs.create(
        upload_id=upload_id, filename="workload.log", source_path=str(container.upload_store.source_path(upload_id)),
        drain_config=container.drain_quality.configs.active_snapshot(),
        semantic_snapshot=container.semantic_dictionaries.active_snapshot(),
    )
    container.run_input_job(job["input_job_id"])
    saved = container.input_jobs.get_job(job["input_job_id"])
    assert saved["status"] == "completed", saved.get("error")
    summary = container.streaming_state.committed_summary(saved["streaming_task_id"])
    assert summary["record_count"] == 3 and summary["template_count"] > 0
    response = ApiFacade(container, version="test").dispatch_read(BASE)
    assert response.status == 200
    activity = response.body["activity"]["data"]
    committed = activity["committed_batch_records"]
    assert committed["today_records"] == committed["total_records"] == 3
    assert committed["seven_day_records"][-1]["records"] == 3
    assert activity["drain"]["template_windows"] == summary["template_count"]
    assert activity["today_records"] == activity["total_records"] == 3


def test_workflow_detail_links_gate_evidence_only_to_the_selected_child(tmp_path):
    from tests.server_risk_fixtures import domain_services

    risk, *_ = domain_services(tmp_path)
    database = risk.repository.database
    workflow_id = seed_overview(database, risk, now=NOW)
    agents = AgentRepository(database)
    workflows = WorkflowRepository(database)

    def child(run_id):
        return agents.create_run(AgentRunRequest(
            source_job_id="overview-job", entity_id="node-01", entity_type="node",
            model_profile_id="qwen", prompt_id="agent_plan_v1", max_steps=5, max_tool_calls=4,
            timeout_seconds=60, allowed_tools=("evaluate_candidate",), idempotency_key=run_id,
            actor="fixture-actor", roles=(), request_id="private-request",
        ), locked_snapshot={"workflow_run_id": run_id, "workflow_node_id": "c"})["run_id"]

    attached = child(workflow_id)
    unrelated = child("different-workflow")
    evidence = {"passed": False, "evidence_hash": "locked", "evaluator_version": "fixture-evaluator", "feature": SECRET}
    agents.add_artifact(attached, "evaluation", evidence)
    agents.add_artifact(attached, "evaluation", {**evidence, "passed": "true"})
    agents.add_artifact(unrelated, "evaluation", {**evidence, "passed": True})
    workflows.transition_run(workflow_id, "running", allowed_from={"failed"})
    workflows.reset_node(workflow_id, "b")
    workflows.claim_node(workflow_id, "b")
    workflows.finish_node(workflow_id, "b", status="completed")
    workflows.claim_node(workflow_id, "c")
    workflows.attach_child_run(workflow_id, "c", unrelated)
    assert workflows.overview_run(workflow_id)["evaluator"] is None

    workflows.attach_child_run(workflow_id, "c", attached)
    with database.transaction() as conn:
        conn.execute(
            "UPDATE feature_candidates SET candidate_json=? WHERE candidate_id='candidate-1'",
            (json.dumps({"agent_run_id": attached, "origin": "agentic", "summary": SECRET}),),
        )
    result = workflows.overview_run(workflow_id)
    assert result["evaluator"] == {"passed": 0, "failed": 1}
    assert result["human"] == {"pending": 1, "approved": 0, "rejected": 0, "resolved": 0}
    assert result["candidate_count"] == 1 and SECRET not in repr(result)


def test_overview_handles_no_accounting_without_writes_or_false_zero(tmp_path, monkeypatch):
    database = SQLiteDatabase(tmp_path / "empty.sqlite3")

    def unexpected_write():
        raise AssertionError("read attempted a database write")

    monkeypatch.setattr(database, "transaction", unexpected_write)
    overview = StreamOverview(database, None, workflows_enabled=False, clock=NOW.isoformat).read()
    assert overview["risk"]["status"] == "unavailable"
    assert overview["usage"]["status"] == "unavailable"
    assert overview["usage"]["data"]["known_tokens"] is None
    assert "报告、回放和旧 Trace 缺少完整覆盖" in overview["usage"]["reason"]
    assert overview["activity"]["data"]["total_records"] is None
    assert overview["workflows"]["data"]["queued"] is None
    assert overview["approvals"]["data"]["history"][-1]["pending_groups"] == 0


def test_risk_daily_overview_reuses_one_commit_verification_and_no_cross_request_state(tmp_path, monkeypatch):
    from logrisk import streaming_state
    from tests.server_risk_fixtures import committed_window, domain_services, seed_committed

    risk, *_ = domain_services(tmp_path)
    database = risk.repository.database
    dates = ["2026-11-22", "2026-11-23"]
    seed_committed(database, "shared-commit", [
        committed_window(1, "2026-11-22T15:59:59+00:00", entity_id="node-a"),
        committed_window(2, "2026-11-22T16:00:00+00:00", entity_id="node-a"),
        committed_window(3, "2026-11-23T00:00:00+00:00", entity_id="node-b"),
    ])
    expected = [risk.overview(date, date) for date in dates]
    original_hash = streaming_state._payload_hash
    verified = []

    def hash_commit(cursor, windows, summary):
        verified.append(len(windows))
        return original_hash(cursor, windows, summary)

    monkeypatch.setattr(streaming_state, "_payload_hash", hash_commit)
    first = risk.overview_daily_summaries(dates, today_limit=1)
    assert [point["summary"] for point in first] == [point["summary"] for point in expected]
    assert [point["quality"] for point in first] == [point["quality"] for point in expected]
    assert [host["node_id"] for host in first[-1]["items"]] == [expected[-1]["items"][0]["node_id"]]
    assert verified == [3]
    with database.transaction() as conn:
        conn.execute("UPDATE streaming_window_commits SET payload_hash='different' WHERE window_id='shared-commit'")
    second = risk.overview_daily_summaries(dates, today_limit=1)
    assert "projection_pending" in second[-1]["quality"]["missing_reasons"]
    assert verified == [3, 3]


def test_risk_overview_scans_indexed_rows_once_and_preserves_physical_link_order(tmp_path, monkeypatch):
    from logrisk import streaming_state
    from logrisk.server_risk import repository as risk_repository
    from tests.server_risk_fixtures import committed_window, domain_services, seed_committed

    risk, *_ = domain_services(tmp_path)
    database = risk.repository.database
    old, new = "2026-11-22T15:59:59+00:00", "2026-11-23T01:00:00+00:00"
    first = committed_window(900, old, entity_id="other-namespace")
    duplicated = {**first, "first_seen": new, "last_seen": new}
    many = [committed_window(index, new, entity_id=f"node-{index // 4:03d}") for index in range(200)]
    seed_committed(database, "a-initial", [first])
    seed_committed(database, "z-duplicate", [duplicated, *many])
    seed_event(risk, "legacy-physical", node="linked-node", first=old)
    key = risk.repository._physical({"window_id": "irrelevant"}, first)
    with database.transaction() as conn:
        conn.execute(
            "INSERT INTO node_risk_ingestions(source_event_fingerprint,event_id,occurrence_count,ingested_at,physical_key) VALUES (?,?,?,?,?)",
            ("legacy-contribution", "legacy-physical", 7, old, key),
        )
    # A cold commit with unrelated physical positions never enters the hot read.
    cold = [committed_window(5000 + index, "2026-10-01T00:00:00+00:00", entity_id="cold-node") for index in range(1100)]
    seed_committed(database, "cold-history", cold)
    dates = ["2026-11-22", "2026-11-23"]
    expected = [risk.overview(date, date)["summary"] for date in dates]
    assert expected[0]["host_count"] == 1
    assert expected[1]["host_count"] == 51
    calls, read = [], []
    source = risk.repository._window_pages

    def count_scan(**kwargs):
        calls.append(True)
        assert kwargs.get("bounds") is not None
        for row in source(**kwargs):
            read.append(row["item_index"])
            yield row

    monkeypatch.setattr(risk.repository, "_window_pages", count_scan)
    summaries = risk.overview_daily_summaries(dates, today_limit=8)
    assert [row["summary"] for row in summaries] == expected
    assert calls == [True] and len(read) == 202
    assert "linked-node" in {row["node_id"] for row in summaries[1]["items"]}
    original_hash = streaming_state._stored_payload_hash
    hashed = []

    def count_full_hash(conn, task_id, commit_id, *args):
        checksum, count = original_hash(conn, task_id, commit_id, *args)
        hashed.append((commit_id, count))
        return checksum, count

    monkeypatch.setattr(streaming_state, "_stored_payload_hash", count_full_hash)
    monkeypatch.setattr(risk_repository, "MAX_OVERVIEW_WINDOWS", 100)
    fallback = risk.overview_daily_summaries(dates, today_limit=8)
    assert fallback == summaries
    assert 2 < len(calls) <= 12
    assert sorted(hashed) == [("a-initial", 1), ("z-duplicate", 201)]
    monkeypatch.setattr(risk.repository, "_window_pages", source)
    assert risk.overview_detail("prod-a", "linked-node", "2026-11-23", "2026-11-23")["groups"]


def test_overview_masks_plausible_credentials_inside_public_identifiers(tmp_path):
    from tests.server_risk_fixtures import domain_services

    risk, *_ = domain_services(tmp_path)
    database = risk.repository.database
    seed_overview(database, risk, now=NOW)
    masked_cluster = "team_ASIA0123456789ABCDEF"
    masked_node = "node-sk-fixture-token"
    masked_model = "model-AKIAABCDEFGHIJKLMNOP"
    masked_provider = "vendor_ghp_fixture"
    seed_event(risk, "credential-cluster", cluster=masked_cluster, first=NOW.isoformat())
    seed_event(risk, "credential-node", node=masked_node, first=NOW.isoformat())
    with database.transaction() as conn:
        conn.execute("UPDATE operational_physical_calls SET model='qwen3.5:4b-mlx' WHERE call_id='overview-0'")
        conn.execute("UPDATE operational_physical_calls SET provider=?,model=? WHERE call_id='overview-1'",
                     (masked_provider, masked_model))
    overview = StreamOverview(database, risk, workflows_enabled=False, clock=NOW.isoformat).read()
    assert overview["risk"]["data"]["today"]["hosts"] == 3
    assert overview["risk"]["data"]["hosts"][0]["node"] == "node-01"
    assert len(overview["risk"]["data"]["hosts"]) == 1
    models = overview["usage"]["data"]["models"]
    assert models[0]["model"] == "qwen3.5:4b-mlx" and models[0]["provider"] == "ollama"
    assert models[1]["model"] is None and models[1]["provider"] is None
    assert all(value not in repr(overview) for value in (masked_cluster, masked_node, masked_model, masked_provider))
