from __future__ import annotations

import hashlib
import json
from pathlib import Path

from tests.server_risk_fixtures import fixture_server, legacy_digest, report_request, request_http, wait_report

STAGES = ["http_ingress", "query_validation", "evidence_snapshot", "report_run_persist", "model_invocation", "narrative_validation", "html_render", "artifact_publish", "http_download"]


def test_minimum_http_whole_path_and_seven_targeted_negatives(tmp_path):
    server, fake, base, thread = fixture_server(tmp_path)
    reports = server.server_risk.reports
    hits = []
    records = []
    cancel_publish = False
    def hook(stage, report_id):
        hits.append(stage)
        if cancel_publish and stage == "artifact_publish" and report_id:
            reports.cancel(report_id)
    reports.stage_hook = hook
    before = legacy_digest(server.database)
    def normal(key):
        hits.clear()
        status, day, _ = request_http(base, "/api/node-risks/prod-a/node-01/days/2026-08-31?timezone=Asia%2FShanghai")
        assert status == 200 and day["summary"]["known_occurrence_count"] == 7
        assert day["summary"]["occurrence_count"] is None and day["summary"]["coverage"] == "unknown"
        count = len(fake.calls)
        status, run, _ = request_http(base, "/api/node-risk-reports", "POST", report_request(key))
        assert status == 202
        run = wait_report(base, run["report_id"])
        assert run["status"] == "completed", run
        assert len(fake.calls) == count + 1
        status, html, headers = request_http(base, run["artifact"]["url"])
        assert status == 200 and isinstance(html, bytes)
        assert hashlib.sha256(html).hexdigest() == run["artifact"]["digest"]
        assert "text/html" in headers["Content-Type"] and "attachment" in headers["Content-Disposition"]
        assert "default-src 'none'" in headers["Content-Security-Policy"]
        assert "nosniff" == headers["X-Content-Type-Options"]
        assert all(stage in hits for stage in STAGES), hits
        records.append({"id": key, "kind": "positive", "stage_hits": list(hits), "observations": ["http_whole_path_completed"], "model_calls": 1, "artifact_digest": run["artifact"]["digest"]})
        return run, html
    def negative(name, stage, observation):
        assert stage in hits, (name, hits)
        records.append({"id": name, "kind": "negative", "stage_hits": list(hits), "observations": [observation]})
    try:
        p0, _ = normal("P0")
        calls = len(fake.calls)
        status, replayed, _ = request_http(base, "/api/node-risk-reports", "POST", report_request("P0"))
        assert status == 202 and replayed["report_id"] == p0["report_id"] and len(fake.calls) == calls
        status, error, _ = request_http(base, "/api/node-risk-reports", "POST", dict(report_request("P0"), timezone="UTC"))
        assert status == 409 and error["code"] == "report_idempotency_conflict" and len(fake.calls) == calls
        hits.clear()
        count = len(fake.calls)
        status, body, _ = request_http(base, "/api/node-risks/prod-a/node-01/days/2026-02-30?timezone=Asia%2FShanghai")
        assert status == 422 and body["code"] == "invalid_date_or_timezone"
        assert len(fake.calls) == count
        with server.database.connect() as conn:
            assert conn.execute("SELECT COUNT(*) FROM server_risk_reports").fetchone()[0] == 1
        negative("N1", "query_validation", "scope_or_date_rejected")
        normal("P1")

        original_facts = server.server_risk.repository.facts
        def dirty_facts(*args):
            return [dict(f, samples=["secret raw"], raw_sample="raw", message="untrusted assertion", typed_fields={**f["typed_fields"], "token": "secret", "message": "raw", "nested": {"password": "secret"}}) for f in original_facts(*args)]
        server.server_risk.repository.facts = dirty_facts
        run, _ = normal("N2-run")
        negative("N2", "evidence_snapshot", "provider_dto_sanitized")
        server.server_risk.repository.facts = original_facts
        normal("P2")

        hits.clear()
        count = len(fake.calls)
        payload = dict(report_request("N3"), model_profile_id="qwen3_1_7b_fast")
        status, error, _ = request_http(base, "/api/node-risk-reports", "POST", payload)
        assert status == 422 and error["code"] == "model_profile_mismatch" and len(fake.calls) == count
        negative("N3", "model_invocation", "model_profile_mismatch")
        normal("P3")

        hits.clear()
        fake.invented = True
        status, run, _ = request_http(base, "/api/node-risk-reports", "POST", report_request("N4"))
        assert status == 202
        run = wait_report(base, run["report_id"])
        assert run["status"] == "failed" and run["error"]["code"] == "report_evidence_validation_failed" and run["artifact"] is None
        assert request_http(base, "/api/node-risk-reports/" + run["report_id"] + "/html")[0] == 409
        negative("N4", "narrative_validation", "report_evidence_validation_failed")
        fake.invented = False
        normal("P4")

        def malicious_facts(*args):
            return [dict(f, object_id='</td><script>alert(1)</script><img src="https://evil" onerror="alert(1)">', template_text='<svg onload="alert(1)"></svg>') for f in original_facts(*args)]
        server.server_risk.repository.facts = malicious_facts
        _, html = normal("N5-run")
        assert b"<script" not in html and b"<img" not in html and b"<svg" not in html
        assert b"&lt;script&gt;alert(1)&lt;/script&gt;" in html
        negative("N5", "html_render", "escaped_text_no_script")
        server.server_risk.repository.facts = original_facts
        normal("P5")

        hits.clear()
        cancel_publish = True
        status, run, _ = request_http(base, "/api/node-risk-reports", "POST", report_request("N6"))
        assert status == 202
        run = wait_report(base, run["report_id"])
        assert run["status"] == "cancelled" and run["artifact"] is None
        assert not list((reports.root / run["report_id"]).glob("*.tmp"))
        assert not (reports.root / run["report_id"] / "report.html").exists()
        negative("N6", "artifact_publish", "cancelled_without_artifact")
        cancel_publish = False
        run, _ = normal("P6")

        hits.clear()
        path = reports.root / run["report_id"] / "report.html"
        path.write_bytes(b"tampered")
        status, error, _ = request_http(base, run["artifact"]["url"])
        assert status == 409 and error["code"] == "artifact_integrity_error"
        path.unlink()
        outside = tmp_path / "outside.html"
        outside.write_text("outside")
        path.symlink_to(outside)
        status, error, _ = request_http(base, run["artifact"]["url"])
        assert status == 409 and error["code"] == "artifact_path_error"
        negative("N7", "http_download", "artifact_integrity_or_path_error")
        normal("P7")
        # Strict new ingress: production loopback does not inherit old bypass.
        from logrisk.runtime.config import RuntimeConfig
        configured = RuntimeConfig.from_mapping({"identity": {"enabled": True, "allow_loopback_bypass": True, "trusted_proxy_cidrs": ["127.0.0.1/32"], "write_roles": ["risk-writer"]}})
        server.runtime_config = configured
        server.api_facade.container.runtime_config = configured
        calls_before = len(fake.calls)
        with server.database.connect() as conn:
            runs_before = conn.execute("SELECT COUNT(*) FROM server_risk_reports").fetchone()[0]
        auth_headers = {"X-LOGRISK-Actor": "tester", "X-LOGRISK-Roles": "risk-writer"}
        assert request_http(base, "/api/node-risk-reports", "POST", report_request("blocked-loopback"), headers=auth_headers)[0] == 403
        assert request_http(base, "/api/node-risks/prod-a/node-01/days/2026-08-31")[0] == 403
        empty_roles = RuntimeConfig.from_mapping({"identity": {"enabled": True, "allow_loopback_bypass": False, "trusted_proxy_cidrs": ["127.0.0.1/32"], "write_roles": []}})
        server.runtime_config = empty_roles
        server.api_facade.container.runtime_config = empty_roles
        assert request_http(base, "/api/node-risk-reports", "POST", report_request("blocked-empty-roles"), headers=auth_headers)[0] == 403
        assert len(fake.calls) == calls_before
        with server.database.connect() as conn:
            assert conn.execute("SELECT COUNT(*) FROM server_risk_reports").fetchone()[0] == runs_before
        strict = RuntimeConfig.from_mapping({"identity": {"enabled": True, "allow_loopback_bypass": False, "trusted_proxy_cidrs": ["127.0.0.1/32"], "write_roles": ["risk-writer"]}})
        server.runtime_config = strict
        server.api_facade.container.runtime_config = strict
        status, run, _ = request_http(base, "/api/node-risk-reports", "POST", report_request("strict-positive"), headers=auth_headers)
        assert status == 202
        import time
        for _ in range(100):
            status, run, _ = request_http(base, "/api/node-risk-reports/" + run["report_id"], headers=auth_headers)
            assert status == 200
            if run["status"] == "completed":
                break
            time.sleep(.01)
        assert run["status"] == "completed"
        assert request_http(base, run["artifact"]["url"], headers=auth_headers)[0] == 200
        records.append({"id": "C7m-strict", "kind": "shared_contract", "stage_hits": list(hits), "observations": ["production_loopback_denied", "empty_write_roles_denied", "trusted_actor_role_completed"], "model_calls": 1})
        assert legacy_digest(server.database) == before
        output = Path("output/server-risk-auto-recovery-continuation/author/minimum-http-evidence.json")
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps({"candidate_id": "auto-recovery-ultra-001", "command": ".venv/bin/python -m pytest tests/test_server_risk_report_path.py -q", "stages": STAGES, "records": records, "legacy_before_digest": before, "legacy_after_digest": legacy_digest(server.database), "fake_model_calls": fake.calls, "actual_model_identity": "fake", "runtime_model_attested": "unknown", "usage": None}, ensure_ascii=False, indent=2))
    finally:
        server.node_risk_report_worker.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_standalone_read_actor_can_normalize_query_but_cannot_write_reports_or_observations(tmp_path):
    from logrisk.runtime.config import RuntimeConfig
    server, fake, base, thread = fixture_server(tmp_path)
    strict = RuntimeConfig.from_mapping({"identity": {"enabled": True, "allow_loopback_bypass": False, "trusted_proxy_cidrs": ["127.0.0.1/32"], "write_roles": ["risk-writer"]}})
    server.runtime_config = strict
    server.api_facade.container.runtime_config = strict
    reader = {"X-LOGRISK-Actor": "reader", "X-LOGRISK-Roles": "risk-reader"}
    query = {"text": "8月31日这个服务器有什么风险", "cluster": "prod-a", "node_id": "node-01", "year": 2026}
    try:
        status, value, _ = request_http(base, "/api/node-risk-queries", "POST", query, reader)
        assert status == 200 and value["status"] == "resolved"
        assert value["query"]["cluster"] == "prod-a" and value["query"]["date"] == "2026-08-31"
        assert request_http(base, "/api/node-risks/prod-a/node-01/assessment", headers=reader)[0] == 200
        assert request_http(base, "/api/node-risk-reports", "POST", report_request("reader-denied"), reader)[0] == 403
        assert request_http(base, "/api/node-risk-reports/" + "0" * 32 + "/cancel", "POST", {"idempotency_key": "reader-cancel"}, reader)[0] == 403
        assert request_http(base, "/api/node-risk-observations", "POST", {}, reader)[0] == 403
        assert not fake.calls
        with server.database.connect() as conn:
            assert conn.execute("SELECT COUNT(*) FROM server_risk_reports").fetchone()[0] == 0
        writer = {"X-LOGRISK-Actor": "writer", "X-LOGRISK-Roles": "risk-writer"}
        status, run, _ = request_http(base, "/api/node-risk-reports", "POST", report_request("writer-positive"), writer)
        assert status == 202
        import time
        for _ in range(100):
            status, run, _ = request_http(base, "/api/node-risk-reports/" + run["report_id"], headers=writer)
            assert status == 200
            if run["status"] == "completed":
                break
            time.sleep(.01)
        assert run["status"] == "completed" and len(fake.calls) == 1
        assert request_http(base, run["artifact"]["url"], headers=writer)[0] == 200
    finally:
        server.node_risk_report_worker.close()
        server.shutdown()
        server.server_close()
        thread.join(2)


def test_direct_node_beyond_catalog_page_uses_explicit_identity_and_latest_risk_date(tmp_path):
    server, fake, base, thread = fixture_server(tmp_path)
    try:
        with server.database.transaction() as conn:
            row = dict(conn.execute("SELECT * FROM node_risk_events WHERE event_id='fixture-0'").fetchone())
            columns = list(row)
            query = "INSERT INTO node_risk_events(" + ",".join(columns) + ") VALUES (" + ",".join("?" for _ in columns) + ")"
            for index, node in enumerate(["page-node-" + str(i).zfill(3) for i in range(100)] + ["zz-node-101"]):
                item = dict(row, event_id="catalog-" + str(index), dedup_key="catalog-" + str(index), node_id=node)
                conn.execute(query, [item[key] for key in columns])
        status, catalog, _ = request_http(base, "/api/node-risks/search?limit=100")
        assert status == 200 and catalog["next_cursor"] and catalog["total"] == 103
        assert not any(item["node_id"] == "zz-node-101" for item in catalog["items"])
        status, value, _ = request_http(base, "/api/node-risks/prod-a/zz-node-101/assessment")
        assert status == 200 and value["subject"] == {"cluster": "prod-a", "node_id": "zz-node-101"}
        assert value["latest_risk_at"] == "2026-08-31T01:01:00+00:00" and value["status"] == "unknown"
        assert request_http(base, "/api/node-risks/prod-a/zz-node-101/days/2026-08-31")[1]["summary"]["known_occurrence_count"] == 7
        status, error, _ = request_http(base, "/api/node-risks/prod-a/missing-node/assessment")
        assert status == 404 and error["code"] == "node_not_found" and not fake.calls
    finally:
        server.node_risk_report_worker.close()
        server.shutdown()
        server.server_close()
        thread.join(2)
