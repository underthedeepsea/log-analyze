from __future__ import annotations

import os
from urllib.parse import quote, urlencode

import django
import pytest
from django.core.management import call_command
from django.test import Client, override_settings

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_test_project.settings")
django.setup()

from tests.server_risk_fixtures import fixture_server, request_http
from tests.test_django_node_risk_api import config
from pipeline.dashboard_server import build_server
from tests.test_revision005_kafka_profile import PROFILE, FakeAdapter


@pytest.fixture
def http_overview(tmp_path):
    server, fake, base, thread = fixture_server(tmp_path)
    try:
        yield server, fake, base
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def query(**changes):
    return urlencode({"from": "2026-08-31", "to": "2026-08-31", "timezone": "Asia/Shanghai", **changes})


def test_overview_http_full_host_pagination_detail_and_real_empty_result(http_overview):
    _, fake, base = http_overview
    status, body, headers = request_http(base, "/api/node-risks/overview?" + query(page=2, page_size=1))
    assert status == 200 and body["schema_version"] == "node_risk_overview_v1"
    assert body["summary"]["host_count"] == 2 and len(body["items"]) == 1
    assert body["pagination"]["total"] == 2
    assert headers["Cache-Control"] == "no-store" and not fake.calls
    status, body, _ = request_http(base, "/api/node-risks/prod-a/node-01/overview?" + query())
    assert status == 200 and body["schema_version"] == "node_risk_overview_detail_v1"
    assert body["host"]["cluster"] == "prod-a" and body["groups"][0]["known_occurrence_count"] == 7
    assert body["groups"][0]["analysis_status"] == "not_analyzed"
    status, body, _ = request_http(base, "/api/node-risks/overview?" + query(q="missing-host"))
    assert status == 200 and body["items"] == [] and body["summary"]["host_count"] == 0


@pytest.mark.parametrize("changes", [{"from": "9999-12-31", "to": "9999-12-31"}, {"from": "0001-01-01", "to": "0001-01-01"}, {"page_size": "101"}, {"page": "1.5"}, {"page": "-1"}, {"recovery": "active"}, {"timezone": "UTC"}, {"mode": "demo"}])
def test_overview_http_invalid_negative_reaches_query_validation(http_overview, changes):
    server, _, base = http_overview
    hits = []
    server.server_risk.reports.stage_hook = lambda stage, report_id: hits.append(stage)
    status, body, _ = request_http(base, "/api/node-risks/overview?" + query(**changes))
    assert status == 422 and body["code"]
    assert "query_validation" in hits
    assert request_http(base, "/api/node-risks/overview?" + query())[0] == 200


def test_overview_http_encoded_entity_and_cluster_are_independent(http_overview):
    server, _, base = http_overview
    with server.database.transaction() as conn:
        conn.execute("UPDATE node_risk_events SET cluster=?,node_id=? WHERE event_id='fixture-0'", ("集群 A", "node % 01"))
    path = "/api/node-risks/" + quote("集群 A", safe="") + "/" + quote("node % 01", safe="") + "/overview?" + query()
    status, body, _ = request_http(base, path)
    assert status == 200 and body["host"] == {"cluster": "集群 A", "node_id": "node % 01", "label": "node % 01"}
    assert request_http(base, "/api/node-risks/prod-b/" + quote("node % 01", safe="") + "/overview?" + query())[0] == 404


def test_overview_django_matches_standalone_and_preserves_trusted_identity(tmp_path, http_overview):
    server, _, base = http_overview
    from logrisk_django import service_factory
    with override_settings(LOGRISK=config(tmp_path / "django")):
        service_factory.clear_cached_container()
        call_command("logrisk_migrate", "--json")
        container = service_factory.get_container()
        with server.database.connect() as conn:
            rows = [dict(row) for row in conn.execute("SELECT * FROM node_risk_events").fetchall()]
        with container.database.transaction() as conn:
            for row in rows:
                keys = list(row)
                conn.execute("INSERT INTO node_risk_events(" + ",".join(keys) + ") VALUES (" + ",".join("?" for _ in keys) + ")", [row[key] for key in keys])
        for path in ("/api/node-risks/overview", "/api/node-risks/prod-a/node-01/overview"):
            response = Client().get(path + "?" + query())
            status, expected, _ = request_http(base, path + "?" + query())
            assert response.status_code == status == 200
            actual = response.json()
            for field in ("schema_version", "items", "groups", "summary", "pagination", "host", "analysis_status"):
                assert actual.get(field) == expected.get(field)
        service_factory.clear_cached_container()
    with override_settings(LOGRISK=dict(config(tmp_path / "django"), identity_resolver="tests.django_test_project.resolver.AnonymousIdentityResolver")):
        service_factory.clear_cached_container()
        assert Client().get("/api/node-risks/overview?" + query()).status_code == 403
        service_factory.clear_cached_container()


def test_overview_django_literal_percent_identity_is_not_decoded_twice(tmp_path):
    from logrisk_django import service_factory
    from tests.test_risk_overview import seed_event
    with override_settings(LOGRISK=config(tmp_path)):
        service_factory.clear_cached_container()
        call_command("logrisk_migrate", "--json")
        service = service_factory.get_container().server_risk
        for key, cluster, node, count in (("literal", "prod-a", "%41", 3), ("letter", "prod-a", "A", 8), ("literal-slash", "prod-a", "%2F", 11), ("cluster-literal", "%41", "node", 13), ("cluster-letter", "A", "node", 17)):
            seed_event(service, key, cluster=cluster, node=node, count=count)
        for cluster, node, count in (("prod-a", "%41", 3), ("prod-a", "A", 8), ("prod-a", "%2F", 11), ("%41", "node", 13), ("A", "node", 17)):
            path = "/api/node-risks/" + quote(cluster, safe="") + "/" + quote(node, safe="") + "/overview?" + query()
            response = Client().get(path)
            assert response.status_code == 200
            body = response.json()
            assert body["host"]["cluster"] == cluster and body["host"]["node_id"] == node
            assert body["groups"][0]["known_occurrence_count"] == count
        service_factory.clear_cached_container()


@pytest.fixture
def kafka_http(tmp_path):
    import threading
    server = build_server("127.0.0.1", 0, database_path=tmp_path / "state/logrisk.sqlite3", state_root=tmp_path / "state", output_root=tmp_path / "output", kafka_enabled=True, ollama_checker=lambda: {"online": False, "models": []}, input_analyzer=lambda _: {})
    adapter = FakeAdapter([])
    server.api_facade.container.kafka_adapters[adapter.adapter_id] = adapter
    captured = []
    server.run_kafka_task = lambda task_id, configuration: captured.append((task_id, configuration))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, "http://127.0.0.1:" + str(server.server_address[1]), captured
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def kafka_payload(**changes):
    return {"adapter_id": "profile-fixture", "topic": "isolated-fixture", "consumer_group": "isolated-fixture-group", "bootstrap_env": "FIXTURE_KAFKA_BOOTSTRAP", "source_profile": PROFILE, "cluster_id": "source-cluster", **changes}


def test_kafka_http_profile_and_cluster_reach_native_descriptor_and_worker(kafka_http):
    server, base, captured = kafka_http
    status, result, _ = request_http(base, "/api/streaming/kafka/start", "POST", kafka_payload())
    assert status == 202 and result["source"]["configuration"]["source_profile"] == PROFILE
    assert result["source"]["configuration"]["cluster_id"] == "source-cluster"
    assert captured and captured[0][1]["source_profile"] == PROFILE
    assert captured[0][1]["cluster_id"] == "source-cluster"
    assert server.streaming_state.get_task(result["task_id"])["source"]["configuration"] == result["source"]["configuration"]


@pytest.mark.parametrize("changes", [{"source_profile": "unknown"}, {"source_profile": {}}, {"source_profile": ""}, {"cluster_id": []}, {"cluster_id": False}, {"cluster_id": " "}])
def test_kafka_http_invalid_metadata_never_creates_or_starts_task(kafka_http, changes):
    server, base, captured = kafka_http
    status, _, _ = request_http(base, "/api/streaming/kafka/start", "POST", kafka_payload(**changes))
    assert status == 422 and not captured
    assert server.streaming_state.list_tasks() == []
    assert request_http(base, "/api/streaming/kafka/start", "POST", kafka_payload())[0] == 202
