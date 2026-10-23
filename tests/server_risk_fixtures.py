from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from logrisk.server_risk.contracts import LING_MODEL, LING_PROFILE, digest, json_text
from pipeline.dashboard_server import build_server

FORBIDDEN = {"samples", "raw_sample", "message", "rawlogs", "token", "password", "secrets"}


def assert_sanitized(value):
    if isinstance(value, dict):
        assert not set(value).intersection(FORBIDDEN)
        for item in value.values():
            assert_sanitized(item)
    elif isinstance(value, list):
        for item in value:
            assert_sanitized(item)


class FakeReportModel:
    def __init__(self):
        self.calls = []
        self.invented = False
        self.last_metadata = {}

    def generate_json(self, messages, schema, *, model, timeout, options):
        assert model == LING_MODEL
        assert timeout <= 60
        assert options["num_predict"] <= 2048
        evidence = json.loads(messages[1]["content"])
        assert_sanitized(evidence)
        self.calls.append({"model": model, "timeout": timeout, "options": options, "evidence_digest": evidence["evidence_digest"]})
        return {"evidence_digest": evidence["evidence_digest"], "blocks": [{"claim_code": "risk_summary" if evidence["facts"] else "coverage_gap", "fact_refs": ["invented-ref"] if self.invented else [evidence["facts"][0]["fact_ref"]] if evidence["facts"] else [], "risk_refs": []}, {"claim_code": "coverage_gap", "fact_refs": [], "risk_refs": []}]}


def request_http(base, path, method="GET", payload=None, headers=None):
    body = json.dumps(payload).encode() if payload is not None else None
    request = Request(base + path, data=body, method=method, headers={"Content-Type": "application/json", **(headers or {})})
    try:
        response = urlopen(request, timeout=5)
    except HTTPError as exc:
        response = exc
    with response:
        data = response.read()
        content = json.loads(data) if "application/json" in response.headers.get("Content-Type", "") else data
        return response.code, content, dict(response.headers)


def fixture_server(tmp_path):
    fake = FakeReportModel()
    server = build_server("127.0.0.1", 0, database_path=tmp_path / "state" / "logrisk.sqlite3", state_root=tmp_path / "state", output_root=tmp_path / "output", node_risk_reports_enabled=True, ollama_checker=lambda: {"online": False, "models": []}, input_analyzer=lambda rows: {"risk_entities": [], "top_templates": []})
    service = server.server_risk
    service.reports.model_factory = lambda connection: fake
    # Fixture facts use explicit event intervals; no live analyzer or provider.
    with server.database.transaction() as conn:
        for index, cluster in enumerate(("prod-a", "prod-b")):
            conn.execute("INSERT INTO node_risk_events(event_id,cluster,node_id,risk_domain,risk_category,risk_type,severity,base_score,confidence,semantic_rule_id,semantic_rule_version,dedup_key,occurrence_count,first_seen,last_seen,window_start,window_end,status,semantic_fields_json,evidence_refs_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'active',?,?,?,?)", ("fixture-" + str(index), cluster, "node-01", "service", "service", "service.failed", "high", 60, .8, "fixture-rule", 1, "fixture-dedup-" + str(index), 7, "2026-08-31T01:00:00+00:00", "2026-08-31T01:01:00+00:00", "2026-08-31T01:00:00+00:00", "2026-08-31T01:05:00+00:00", json_text({"unit": "worker.service"}), "[]", "2026-08-31T02:00:00+00:00", "2026-08-31T02:00:00+00:00"))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, fake, "http://127.0.0.1:" + str(server.server_address[1]), thread


def report_request(key):
    return {"schema_version": "node_risk_report_request_v1", "cluster": "prod-a", "node_id": "node-01", "date": "2026-08-31", "timezone": "Asia/Shanghai", "model_profile_id": LING_PROFILE, "idempotency_key": key}


def wait_report(base, report_id):
    for _ in range(100):
        status, run, _ = request_http(base, "/api/node-risk-reports/" + report_id)
        assert status == 200
        if run["status"] in ("completed", "failed", "cancelled"):
            return run
        time.sleep(.01)
    raise AssertionError("Fake report did not finish")


def legacy_digest(database):
    with database.connect() as conn:
        rows = {table: [dict(row) for row in conn.execute("SELECT * FROM " + table).fetchall()] for table in ("node_risk_events", "node_risk_ingestions", "node_risk_score_samples")}
    return digest(rows)


def domain_services(tmp_path):
    import yaml
    from logrisk.database import SQLiteDatabase
    from logrisk.ai_harness.model_profile import ModelProfileRegistry
    from logrisk.ai_harness.prompt_registry import PromptRegistry
    from logrisk.server_risk.repository import ServerRiskRepository
    from logrisk.server_risk.reports import ReportService
    from logrisk.server_risk.policies import RiskPolicies
    from logrisk.server_risk.assessment import AssessmentService
    from logrisk.server_risk.observations import ObservationService
    from logrisk.server_risk.service import ServerRiskService
    database = SQLiteDatabase(tmp_path / "domain.sqlite3")
    now = {"value": "2026-08-31T12:00:00+00:00"}
    source = {"source_id": "fixture-source", "enabled": True, "source_epoch": "epoch-1", "config_revision": "config-1", "environment": "fixture", "cluster": "prod-a", "node_ids": ["node-01"], "capability": "authoritative_state", "objects": [{"object_kind": "unit", "object_id": "A"}, {"object_kind": "unit", "object_id": "B"}], "signals": {"failed": {"kind": "adverse", "risk_type": "service.failed"}, "unit_expected_state": {"kind": "state_snapshot", "risk_type": "service.failed", "field_types": {"ready": "boolean"}}, "candidate": {"kind": "recovery_candidate", "risk_type": "service.failed"}}, "permitted_actors": ["fixture-actor"], "freshness_seconds": 100, "max_gap_seconds": 10, "allowed_lateness_seconds": 864000}
    config = {"schema_version": "server_risk_config_v1", "sources": [source], "policies": {"service.failed": {"mode": "state", "recovery_signal": "unit_expected_state", "positive_field": "ready", "positive_value": True, "hold_seconds": 10}}}
    config_path = tmp_path / "server_risk.yaml"
    config_path.write_text(yaml.safe_dump(config))
    policies = RiskPolicies(config_path)
    repository = ServerRiskRepository(database)
    clock = lambda: now["value"]
    assessments = AssessmentService(repository, policies, clock)
    repository.assessment = assessments
    fake = FakeReportModel()
    class Connections:
        def get(self, _):
            return {"connection_id": "ollama-local", "provider": "ollama", "enabled": True, "timeout_seconds": 60, "base_url": "http://127.0.0.1:11434"}
    reports = ReportService(repository, ModelProfileRegistry(Path("configs/model_profiles.yaml")), Connections(), PromptRegistry(Path("prompts"), Path("configs/ai_harness.yaml")), tmp_path / "output", enabled=True, clock=clock, model_factory=lambda _: fake)
    service = ServerRiskService(repository, reports)
    service.assessments = assessments
    service.observations = ObservationService(repository, policies, assessments, clock)
    return service, now, config, policies, fake


def observation(seq, when, *, kind="adverse", obj="A", boot="boot-1", **extra):
    codes = {"adverse": "failed", "state_snapshot": "unit_expected_state", "recovery_candidate": "candidate", "coverage": "coverage"}
    result = {"identity": {"cluster": "prod-a", "node_id": "node-01", "object_kind": "unit", "object_id": obj, "boot_id": boot}, "kind": kind, "signal_code": codes[kind], "risk_type": "service.failed", "occurred_at": when, "source_sequence": seq, "source_position_ref": "position-" + str(seq), "typed_fields": {"severity": "high"} if kind == "adverse" else {"ready": True}}
    if kind == "adverse":
        result["count"] = 1
    result.update(extra)
    return result


def batch(key, *items, mode="current"):
    return {"schema_version": "server_risk_observation_batch_v1", "source_id": "fixture-source", "source_epoch": "epoch-1", "config_revision": "config-1", "batch_id": key, "idempotency_key": key, "mode": mode, "observations": list(items)}


def seed_browser_events(database, count=125):
    with database.transaction() as conn:
        row = dict(conn.execute("SELECT * FROM node_risk_events WHERE event_id='fixture-0'").fetchone())
        columns = list(row)
        for index in range(count):
            copy = dict(row, event_id="browser-event-" + str(index), dedup_key="browser-dedup-" + str(index), occurrence_count=1, semantic_fields_json=json_text({"unit": "worker-" + str(index) + ".service"}))
            conn.execute("INSERT INTO node_risk_events(" + ",".join(columns) + ") VALUES (" + ",".join("?" for _ in columns) + ")", [copy[key] for key in columns])


def commit_source(service, *, mode="current"):
    from logrisk.server_risk.committed_adapter import CommittedObservationAdapter
    source = service.assessments.policies.sources["fixture-source"]
    source["objects"] = [{"object_kind": "unit", "object_id": "A"}]
    source["commit_adapter"] = {"enabled": True, "actor": "fixture-actor", "source_namespace": "fixture-commits", "task_ids": ["fixture-task"], "mode": mode, "mappings": {"service.failed": {"kind": "adverse", "signal_code": "failed", "object_kind": "unit", "object_field": "unit"}, "fixture.healthy": {"kind": "state_snapshot", "signal_code": "unit_expected_state", "object_kind": "unit", "object_field": "unit"}}}
    stages = []
    adapter = CommittedObservationAdapter(service.repository, service.observations, service.assessments.policies, service.observations.clock, stage_hook=lambda stage, details: stages.append({"stage": stage, **details}))
    return adapter, stages


def committed_window(index, when, *, positive=False, **changes):
    window = {"cluster": "prod-a", "entity_type": "node", "entity_id": "node-01", "source_namespace": "fixture-commits", "source_batch_id": "physical-batch", "source_item_id": "item-" + str(index), "window_start": when, "first_seen": when, "last_seen": when, "count": 1, "risk_semantic": {"risk_type": "fixture.healthy" if positive else "service.failed", "severity": "high", "semantic_fields": {"unit": "A", "boot_id": "boot-1", "ready": True} if positive else {"unit": "A", "boot_id": "boot-1"}}}
    window.update(changes)
    return window


def seed_committed(database, commit_id, windows, *, summary=None, task_id="fixture-task"):
    from logrisk.streaming_state import _payload_hash
    from logrisk.database import utc_now
    cursor = {"kind": "file", "value": {"offset": 0}}
    summary = summary or {"template_count": len(windows)}
    with database.transaction() as conn:
        conn.execute("INSERT INTO streaming_tasks(task_id,source_kind,source_identity_json,config_hash,status,stage,cursor_json,task_json,created_at,updated_at) VALUES (?,'file','{}','fixture','completed','COMPLETED',?,'{}',?,?) ON CONFLICT(task_id) DO NOTHING", (task_id, json_text(cursor), utc_now(), utc_now()))
        conn.execute("INSERT INTO streaming_window_commits(task_id,window_id,cursor_json,summary_json,committed_at,payload_hash) VALUES (?,?,?,?,?,?)", (task_id, commit_id, json_text(cursor), json_text(summary), utc_now(), _payload_hash(cursor, windows, summary)))
        for index, window in enumerate(windows):
            conn.execute("INSERT INTO streaming_batch_windows(task_id,window_id,item_index,window_json) VALUES (?,?,?,?)", (task_id, commit_id, index, json_text(window)))
