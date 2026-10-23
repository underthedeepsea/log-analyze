from __future__ import annotations

import os
from pathlib import Path
import django
from django.core.management import call_command
from django.test import Client, override_settings

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_test_project.settings")
django.setup()

from tests.server_risk_fixtures import batch, observation, report_request, FakeReportModel


def config(tmp_path):
    return {"project_root": str(Path.cwd()), "state_root": str(tmp_path / "state"), "output_root": str(tmp_path / "output"), "database_provider": "sqlite", "shared_root": str(tmp_path / "shared"), "airflow_base_url": "http://127.0.0.1:18080", "identity_resolver": "tests.django_test_project.resolver.OperatorIdentityResolver", "write_roles": ["logrisk:operator"], "node_risk_reports_enabled": True}


def seed(database):
    with database.transaction() as conn:
        conn.execute("INSERT INTO node_risk_events(event_id,cluster,node_id,risk_domain,risk_category,risk_type,severity,base_score,confidence,semantic_rule_id,semantic_rule_version,dedup_key,occurrence_count,first_seen,last_seen,window_start,window_end,status,semantic_fields_json,evidence_refs_json,created_at,updated_at) VALUES ('event','prod-a','node-01','service','service','service.failed','high',60,.8,'rule',1,'dedup',7,'2026-08-31T01:00:00+00:00','2026-08-31T01:01:00+00:00','2026-08-31T01:00:00+00:00','2026-08-31T01:05:00+00:00','active','{}','[]','2026-08-31T01:00:00+00:00','2026-08-31T01:01:00+00:00')")


def test_django_persists_dispatches_ids_only_and_serves_html(tmp_path, monkeypatch):
    from logrisk_django import service_factory
    from logrisk.server_risk.airflow import execute_report
    calls = []
    class Dispatch:
        def trigger_node_risk_report(self, report_id, request_id):
            saved = service_factory.get_container().server_risk.reports.get(report_id)
            assert saved["status"] == "queued" and saved["request_id"] == request_id
            calls.append((report_id, request_id))
    monkeypatch.setattr(service_factory, "get_node_risk_report_airflow_orchestrator", lambda: Dispatch())
    with override_settings(LOGRISK=config(tmp_path)):
        service_factory.clear_cached_container(); call_command("logrisk_migrate", "--json")
        container = service_factory.get_container(); seed(container.database)
        fake = FakeReportModel(); container.server_risk.reports.model_factory = lambda _: fake
        client = Client()
        response = client.post("/api/node-risk-reports", data=report_request("django"), content_type="application/json")
        assert response.status_code == 202 and response.json()["status"] == "queued" and not fake.calls
        run = response.json()
        execute_report({"report_id": run["report_id"], "request_id": run["request_id"]}, container=container)
        detail = client.get("/api/node-risk-reports/" + run["report_id"])
        assert detail.json()["status"] == "completed" and len(fake.calls) == 1
        html = client.get(detail.json()["artifact"]["url"])
        assert html.status_code == 200 and html["Content-Type"].startswith("text/html") and html.content.startswith(b"<!doctype")
        assert client.get("/api/node-risks/prod-a/node-01/days/2026-08-31").json()["summary"]["known_occurrence_count"] == 7
        assert client.post("/api/node-risk-queries", data={"text": "8月31日这个服务器有什么风险", "cluster": "prod-a", "node_id": "node-01", "year": 2026}, content_type="application/json").json()["status"] == "resolved"
        service_factory.clear_cached_container()
    assert calls == [(run["report_id"], "request-django-test")]


def test_django_fail_closed_empty_roles_anonymous_and_dispatch_failure(tmp_path, monkeypatch):
    from logrisk_django import service_factory
    class Broken:
        def trigger_node_risk_report(self, *_): raise RuntimeError("secret model token raw")
    monkeypatch.setattr(service_factory, "get_node_risk_report_airflow_orchestrator", lambda: Broken())
    cfg = config(tmp_path)
    with override_settings(LOGRISK=cfg):
        service_factory.clear_cached_container(); call_command("logrisk_migrate", "--json"); seed(service_factory.get_container().database)
        response = Client().post("/api/node-risk-reports", data=report_request("broken"), content_type="application/json")
        assert response.status_code == 503 and response.json()["error"]["code"] == "report_dispatch_failed"
        assert b"secret" not in response.content
        service_factory.clear_cached_container()
    for changes in ({"write_roles": []}, {"identity_resolver": "tests.django_test_project.resolver.AnonymousIdentityResolver"}):
        with override_settings(LOGRISK=dict(cfg, **changes)):
            service_factory.clear_cached_container()
            assert Client().post("/api/node-risk-reports", data=report_request("denied"), content_type="application/json").status_code == 403
            service_factory.clear_cached_container()


def test_django_manual_processing_is_authenticated_and_never_clears_assessment(tmp_path):
    from logrisk_django import service_factory
    with override_settings(LOGRISK=config(tmp_path)):
        service_factory.clear_cached_container(); call_command("logrisk_migrate", "--json")
        container = service_factory.get_container(); seed(container.database)
        client = Client()
        response = client.post("/api/node-risks/events/event/acknowledge", data={"operator": "forged", "reason": "review"}, content_type="application/json")
        assert response.status_code == 200
        response = client.post("/api/node-risks/events/event/recover", data={"operator": "forged", "reason": "closed"}, content_type="application/json")
        assert response.status_code == 200
        assert client.get("/api/node-risks/prod-a/node-01/assessment").json()["status"] == "unknown"
        with container.database.connect() as conn:
            row = conn.execute("SELECT status,acknowledged_by FROM node_risk_events WHERE event_id='event'").fetchone()
            assert row["status"] == "recovered" and row["acknowledged_by"] == "pacas-alice"
        service_factory.clear_cached_container()


def test_django_read_actor_normalizes_query_without_write_role_and_denies_report(tmp_path, monkeypatch):
    from logrisk_django import service_factory
    calls = []
    monkeypatch.setattr(service_factory, "get_node_risk_report_airflow_orchestrator", lambda: calls.append("unexpected-dispatch"))
    cfg = dict(config(tmp_path), write_roles=["different-writer-role"])
    with override_settings(LOGRISK=cfg):
        service_factory.clear_cached_container()
        call_command("logrisk_migrate", "--json")
        container = service_factory.get_container()
        seed(container.database)
        client = Client()
        response = client.post("/api/node-risk-queries", data={"text": "8月31日这个服务器有什么风险", "cluster": "prod-a", "node_id": "node-01", "year": 2026}, content_type="application/json")
        assert response.status_code == 200 and response.json()["status"] == "resolved"
        assert client.get("/api/node-risks/prod-a/node-01/assessment").status_code == 200
        assert client.post("/api/node-risk-reports", data=report_request("read-actor"), content_type="application/json").status_code == 403
        assert client.post("/api/node-risk-reports/" + "0" * 32 + "/cancel", data={"idempotency_key": "read-cancel"}, content_type="application/json").status_code == 403
        assert client.post("/api/node-risk-observations", data={}, content_type="application/json").status_code == 403
        assert not calls
        with container.database.connect() as conn:
            assert conn.execute("SELECT COUNT(*) FROM server_risk_reports").fetchone()[0] == 0
        service_factory.clear_cached_container()


def test_django_forged_identity_headers_do_not_replace_host_identity_then_operator_succeeds(tmp_path, monkeypatch):
    from logrisk_django import service_factory
    dispatches = []
    class Dispatch:
        def trigger_node_risk_report(self, report_id, request_id):
            dispatches.append((report_id, request_id))
    monkeypatch.setattr(service_factory, "get_node_risk_report_airflow_orchestrator", lambda: Dispatch())
    cfg = config(tmp_path)
    with override_settings(LOGRISK=dict(cfg, identity_resolver="tests.django_test_project.resolver.AnonymousIdentityResolver")):
        service_factory.clear_cached_container()
        call_command("logrisk_migrate", "--json")
        container = service_factory.get_container()
        seed(container.database)
        fake = FakeReportModel()
        container.server_risk.reports.model_factory = lambda _: fake
        client = Client()
        forged = {"HTTP_X_LOGRISK_ACTOR": "pacas-alice", "HTTP_X_LOGRISK_ROLES": "logrisk:operator", "HTTP_X_FORWARDED_FOR": "127.0.0.1"}
        assert client.post("/api/node-risk-reports", data=report_request("forged"), content_type="application/json", **forged).status_code == 403
        assert client.get("/api/node-risks/prod-a/node-01/assessment", **forged).status_code == 403
        assert not dispatches and not fake.calls
        with container.database.connect() as conn:
            assert conn.execute("SELECT COUNT(*) FROM server_risk_reports").fetchone()[0] == 0
        service_factory.clear_cached_container()
    with override_settings(LOGRISK=cfg):
        service_factory.clear_cached_container()
        container = service_factory.get_container()
        fake = FakeReportModel()
        container.server_risk.reports.model_factory = lambda _: fake
        response = Client().post("/api/node-risk-reports", data=report_request("trusted-after-forgery"), content_type="application/json")
        assert response.status_code == 202 and len(dispatches) == 1 and not fake.calls
        run = response.json()
        container.server_risk.reports.execute(run["report_id"])
        assert container.server_risk.reports.get(run["report_id"])["status"] == "completed" and len(fake.calls) == 1
        service_factory.clear_cached_container()


def test_django_extreme_dates_have_shared_safe_error_and_paired_representable_days(tmp_path):
    from logrisk_django import service_factory
    with override_settings(LOGRISK=config(tmp_path)):
        service_factory.clear_cached_container()
        call_command("logrisk_migrate", "--json")
        container = service_factory.get_container()
        seed(container.database)
        hits = []
        container.server_risk.reports.stage_hook = lambda stage, report_id: hits.append(stage)
        client = Client()
        for day in ("9999-12-31", "0001-01-01", "2026-02-30"):
            response = client.get("/api/node-risks/prod-a/node-01/days/" + day, {"timezone": "Asia/Shanghai"})
            assert response.status_code == 422 and response.json()["code"] == "invalid_date_or_timezone"
            assert "query_validation" in hits
        for day in ("2026-08-31", "0001-01-02", "9999-12-30"):
            response = client.get("/api/node-risks/prod-a/node-01/days/" + day, {"timezone": "Asia/Shanghai"})
            assert response.status_code == 200 and response.json()["evaluation_mode"] == "historical"
        response = client.get("/api/node-risks/prod-a/node-01/assessment", {"mode": "historical", "as_of": "2026-08-31"})
        assert response.status_code == 422 and response.json()["code"] == "invalid_schema"
        assert client.get("/api/node-risks/prod-a/node-01/assessment").json()["evaluation_mode"] == "current"
        service_factory.clear_cached_container()
