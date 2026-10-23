from __future__ import annotations

import json
import pytest
from logrisk.server_risk.contracts import ServerRiskError
from tests.server_risk_fixtures import batch, domain_services, observation, report_request


def seed(service, now, count=1):
    service.observations.submit(batch("seed", *[observation(i, now["value"]) for i in range(count)]), "fixture-actor")


def test_report_locks_snapshot_single_call_complete_html_and_empty_day(tmp_path):
    service, now, _, _, fake = domain_services(tmp_path)
    seed(service, now, 126)
    run = service.reports.create(report_request("large"), "fixture-actor", request_id="stable-id")
    service.reports.execute(run["report_id"])
    result = service.reports.get(run["report_id"])
    assert result["status"] == "completed" and result["usage"] is None and result["request_id"] == "stable-id"
    body = service.reports.download(run["report_id"])
    assert body.count(b"<tr>") == 129 and b"126" in body
    service.reports.execute(run["report_id"])
    assert len(fake.calls) == 1
    assert service.reports.create(report_request("large"), "fixture-actor")["report_id"] == run["report_id"]
    empty = service.reports.create(dict(report_request("empty"), date="2026-09-01"), "fixture-actor")
    service.reports.execute(empty["report_id"])
    assert service.reports.get(empty["report_id"])["status"] == "completed"
    assert "采集覆盖证据不足".encode() in service.reports.download(empty["report_id"])


def test_report_cancel_before_call_after_call_and_profile_change(tmp_path):
    service, now, _, _, fake = domain_services(tmp_path)
    seed(service, now)
    run = service.reports.create(report_request("cancel"), "fixture-actor")
    service.reports.cancel(run["report_id"]); service.reports.execute(run["report_id"])
    assert service.reports.get(run["report_id"])["status"] == "cancelled" and not fake.calls
    run = service.reports.create(report_request("during"), "fixture-actor")
    generate = fake.generate_json
    def cancel(*args, **kwargs):
        result = generate(*args, **kwargs)
        service.reports.cancel(run["report_id"])
        return result
    fake.generate_json = cancel
    service.reports.execute(run["report_id"])
    assert service.reports.get(run["report_id"])["status"] == "cancelled" and len(fake.calls) == 1
    assert not (service.reports.root / run["report_id"] / "report.html").exists()
    changed = service.reports.create(report_request("changed"), "fixture-actor")
    with service.repository.database.transaction() as conn:
        row = conn.execute("SELECT profile_json FROM server_risk_reports WHERE report_id=?", (changed["report_id"],)).fetchone()
        profile = json.loads(row[0]); profile["max_output_tokens"] = 1
        conn.execute("UPDATE server_risk_reports SET profile_json=? WHERE report_id=?", (json.dumps(profile), changed["report_id"]))
    service.reports.execute(changed["report_id"])
    assert service.reports.get(changed["report_id"])["error"]["code"] == "report_profile_changed" and len(fake.calls) == 1


def test_report_get_entry_expires_interrupted_owned_execution_without_replay(tmp_path):
    service, now, _, _, fake = domain_services(tmp_path)
    seed(service, now)
    old = service.reports.create(report_request("crashed"), "fixture-actor")
    active = service.reports.create(report_request("active"), "fixture-actor")
    with service.repository.database.transaction() as conn:
        conn.execute("UPDATE server_risk_reports SET status='running',claim='dead-owner',model_calls=1,deadline='2026-08-31T11:59:00+00:00' WHERE report_id=?", (old["report_id"],))
        conn.execute("UPDATE server_risk_reports SET status='running',claim='other-live-owner',model_calls=1 WHERE report_id=?", (active["report_id"],))
    # GET is the actual shared API entry, not a manual recover method call.
    from logrisk.application.api import ApiFacade
    from types import SimpleNamespace
    from logrisk.runtime.config import RuntimeConfig
    facade = ApiFacade(SimpleNamespace(server_risk=service, runtime_config=RuntimeConfig.from_mapping({})), version="1.42.0")
    result = facade.dispatch_server_risk_read("/api/node-risk-reports/" + old["report_id"], {}, None)
    assert result.body["status"] == "failed" and result.body["error"]["code"] == "report_interrupted"
    assert service.reports.get(active["report_id"])["status"] == "running"
    service.reports.execute(old["report_id"])
    assert not fake.calls


def test_report_validation_and_integrity_fail_safe(tmp_path):
    service, now, _, _, fake = domain_services(tmp_path)
    seed(service, now)
    fake.invented = True
    run = service.reports.create(report_request("bad"), "fixture-actor")
    service.reports.execute(run["report_id"])
    result = service.reports.get(run["report_id"])
    assert result["error"]["code"] == "report_evidence_validation_failed" and result["artifact"] is None
    fake.invented = False
    run = service.reports.create(report_request("valid"), "fixture-actor")
    service.reports.execute(run["report_id"])
    path = service.reports.root / run["report_id"] / "report.html"
    path.write_bytes(b"tampered")
    with pytest.raises(ServerRiskError) as caught:
        service.reports.download(run["report_id"])
    assert caught.value.code == "artifact_integrity_error"


def test_deadline_provider_timeout_and_render_cancellation_are_terminal(tmp_path):
    service, now, _, _, fake = domain_services(tmp_path)
    seed(service, now)
    timeout = service.reports.create(report_request("timeout"), "fixture-actor")
    attempts = []
    def unavailable(*_, **__):
        attempts.append(1); raise TimeoutError("secret output")
    original = fake.generate_json; fake.generate_json = unavailable
    service.reports.execute(timeout["report_id"]); service.reports.execute(timeout["report_id"])
    assert service.reports.get(timeout["report_id"])["error"]["code"] == "report_model_unavailable" and len(attempts) == 1
    fake.generate_json = original
    for stage in ("html_render", "artifact_publish"):
        run = service.reports.create(report_request("cancel-" + stage), "fixture-actor")
        service.reports.stage_hook = lambda hit, report_id, expected=stage: service.reports.cancel(report_id) if report_id and hit == expected else None
        service.reports.execute(run["report_id"])
        assert service.reports.get(run["report_id"])["status"] == "cancelled"
        assert not list((service.reports.root / run["report_id"]).glob("*"))
    service.reports.stage_hook = None
    expired = service.reports.create(report_request("expired"), "fixture-actor")
    now["value"] = "2026-08-31T12:03:00+00:00"
    service.reports.execute(expired["report_id"])
    assert service.reports.get(expired["report_id"])["error"]["code"] == "report_deadline_exceeded"


def test_provider_transport_caps_bytes_before_parsing_and_budget_failures_are_safe(tmp_path):
    from logrisk.server_risk.reports import BoundedProviderResponse
    class Response:
        def read(self, size): assert size == 65537; return b"x" * size
        def close(self): pass
    with pytest.raises(ServerRiskError) as exc:
        with BoundedProviderResponse(Response()) as response: response.read()
    assert exc.value.code == "report_output_limit"
    service, now, _, _, fake = domain_services(tmp_path)
    seed(service, now)
    run = service.reports.create(report_request("too-large"), "fixture-actor")
    fake.generate_json = lambda *args, **kwargs: {"invented": "secret" * 6000}
    service.reports.execute(run["report_id"])
    result = service.reports.get(run["report_id"])
    assert result["error"]["code"] == "report_output_limit" and "secret" not in json.dumps(result)
    assert result["artifact"] is None


def test_two_independent_workers_claim_same_run_once(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    import threading
    from logrisk.server_risk.reports import ReportService
    service, now, _, _, fake = domain_services(tmp_path)
    seed(service, now)
    original = service.reports
    run = original.create(report_request("parallel"), "fixture-actor")
    second = ReportService(service.repository, original.profiles, original.connections, original.prompts, original.root.parent, enabled=True, clock=original.clock, model_factory=original.model_factory)
    barrier = threading.Barrier(2)
    def execute(worker): barrier.wait(3); worker.execute(run["report_id"])
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(execute, worker) for worker in (original, second)]
        for future in futures: future.result(5)
    assert original.get(run["report_id"])["status"] == "completed" and len(fake.calls) == 1
    with service.repository.database.connect() as conn:
        assert conn.execute("SELECT model_calls FROM server_risk_reports WHERE report_id=?", (run["report_id"],)).fetchone()[0] == 1


def test_claims_bind_subject_episode_and_preserve_valid_historical_recovery(tmp_path):
    from logrisk.server_risk.evidence import snapshot, validate_narrative
    service, now, _, policies, _ = domain_services(tmp_path)
    now["value"] = "2026-08-31T12:00:30+00:00"
    service.observations.submit(batch("subjects", observation(1, "2026-08-31T12:00:00+00:00"), observation(2, "2026-08-31T12:00:00+00:00", obj="B"), observation(3, "2026-08-31T12:00:10+00:00", kind="state_snapshot", obj="B"), observation(4, "2026-08-31T12:00:20+00:00", kind="state_snapshot", obj="B")), "fixture-actor")
    query = {k: report_request("claim")[k] for k in ("cluster", "node_id", "date", "timezone")}
    evidence = snapshot(service.repository, query, now["value"])
    a = next(f for f in evidence["facts"] if f["object_id"] == "A")
    b = next(f for f in evidence["facts"] if f["object_id"] == "B")
    def claim(code, fact, risk=None, value=evidence):
        return {"evidence_digest": value["evidence_digest"], "blocks": [{"claim_code": code, "fact_refs": [fact["fact_ref"]], "risk_refs": [risk or fact["risk_ref"]]}]}
    for invalid in (claim("observed_recovery", a), claim("risk_summary", a, b["risk_ref"]), claim("no_recovery_evidence", b)):
        with pytest.raises(ServerRiskError) as caught:
            validate_narrative(invalid, evidence)
        assert caught.value.code == "report_evidence_validation_failed"
    assert validate_narrative(claim("observed_recovery", b), evidence)
    assert validate_narrative(claim("risk_summary", a), evidence)
    now["value"] = "2026-09-01T12:00:20+00:00"
    service.observations.submit(batch("next-day", observation(5, "2026-09-01T12:00:00+00:00", kind="state_snapshot", obj="B"), observation(6, "2026-09-01T12:00:10+00:00", kind="state_snapshot", obj="B")), "fixture-actor")
    historical = snapshot(service.repository, query, now["value"])
    b = next(f for f in historical["facts"] if f["object_id"] == "B")
    assert validate_narrative(claim("observed_recovery", b, value=historical), historical)
    with pytest.raises(ServerRiskError):
        validate_narrative(claim("no_recovery_evidence", b, value=historical), historical)


def test_cross_day_unfinished_episode_is_referenced_rendered_without_new_occurrences(tmp_path):
    from logrisk.server_risk.evidence import snapshot, validate_narrative
    service, now, _, policies, fake = domain_services(tmp_path)
    source = policies.sources["fixture-source"]
    for signal in source["signals"].values():
        signal["risk_type"] = "gpu.hardware"
    source["signals"]["gpu_recovery_verified"] = {"kind": "state_snapshot", "risk_type": "gpu.hardware", "field_types": {"verified": "boolean"}}
    policies.rules = {"gpu.hardware": {"mode": "hardware", "recovery_signal": "gpu_recovery_verified", "positive_field": "verified", "positive_value": True, "hold_seconds": 10}}
    service.observations.submit(batch("gpu", observation(1, now["value"], risk_type="gpu.hardware")), "fixture-actor")
    now["value"] = "2026-09-01T12:00:00+00:00"
    request = dict(report_request("carry"), date="2026-09-01")
    query = {k: request[k] for k in ("cluster", "node_id", "date", "timezone")}
    evidence = snapshot(service.repository, query, now["value"])
    assert evidence["facts"] == [] and evidence["summary"]["known_occurrence_count"] == 0
    assert len(evidence["unfinished_episodes"]) == 1
    episode = evidence["unfinished_episodes"][0]
    narrative = {"evidence_digest": evidence["evidence_digest"], "blocks": [{"claim_code": "hardware_unresolved", "fact_refs": [episode["fact_ref"]], "risk_refs": [episode["risk_ref"]]}]}
    assert validate_narrative(narrative, evidence)
    fake.generate_json = lambda messages, *_args, **_kwargs: dict(narrative, evidence_digest=json.loads(messages[1]["content"])["evidence_digest"])
    run = service.reports.create(request, "fixture-actor")
    service.reports.execute(run["report_id"])
    body = service.reports.download(run["report_id"]).decode()
    assert "跨日仍未结束的风险" in body and episode["fact_ref"] in body and "gpu.hardware" in body
    now["value"] = "2026-09-01T12:00:20+00:00"
    service.observations.submit(batch("verify", observation(2, "2026-09-01T12:00:00+00:00", kind="state_snapshot", risk_type="gpu.hardware", signal_code="gpu_recovery_verified", typed_fields={"verified": True}), observation(3, "2026-09-01T12:00:10+00:00", kind="state_snapshot", risk_type="gpu.hardware", signal_code="gpu_recovery_verified", typed_fields={"verified": True})), "fixture-actor")
    resolved = snapshot(service.repository, query, now["value"])
    assert resolved["unfinished_episodes"] == []
    same_day = snapshot(service.repository, dict(query, date="2026-08-31"), now["value"])
    gpu = same_day["facts"][0]
    with pytest.raises(ServerRiskError):
        validate_narrative({"evidence_digest": resolved["evidence_digest"], "blocks": [{"claim_code": "hardware_unresolved", "fact_refs": [gpu["fact_ref"]], "risk_refs": [gpu["risk_ref"]]}]}, resolved)
    # A resolved GPU cannot borrow another object's unknown status.
    service.observations.submit(batch("unknown-B", observation(4, now["value"], obj="B", risk_type="gpu.hardware")), "fixture-actor")
    same_day = snapshot(service.repository, dict(query, date="2026-08-31"), now["value"])
    gpu = same_day["facts"][0]
    with pytest.raises(ServerRiskError):
        validate_narrative({"evidence_digest": same_day["evidence_digest"], "blocks": [{"claim_code": "hardware_unresolved", "fact_refs": [gpu["fact_ref"]], "risk_refs": [gpu["risk_ref"]]}]}, same_day)


def test_report_root_symlink_rejected_before_writes_with_normal_publish_control(tmp_path):
    service, now, *_ = domain_services(tmp_path)
    seed(service, now)
    outside = tmp_path / "outside"
    outside.mkdir()
    service.reports.root.parent.mkdir(exist_ok=True)
    service.reports.root.symlink_to(outside, target_is_directory=True)
    blocked = service.reports.create(report_request("root-link"), "fixture-actor")
    service.reports.execute(blocked["report_id"])
    final = service.reports.get(blocked["report_id"])
    assert final["status"] == "failed" and final["error"]["code"] == "artifact_path_error"
    assert not list(outside.iterdir()) and final["artifact"] is None
    service.reports.root.unlink()
    good = service.reports.create(report_request("normal-root"), "fixture-actor")
    service.reports.execute(good["report_id"])
    assert service.reports.get(good["report_id"])["status"] == "completed"
    assert service.reports.download(good["report_id"]).startswith(b"<!doctype")


@pytest.mark.parametrize("phase", ["headers", "body"])
def test_real_provider_absolute_deadline_closes_slow_drip_and_preserves_normal_call(tmp_path, phase):
    import threading
    import time
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from logrisk.server_risk.reports import create_report_model_client
    service, now, *_ = domain_services(tmp_path)
    seed(service, now)
    begun, closed = threading.Event(), threading.Event()
    calls = []
    mode = {"slow": True}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            evidence = json.loads(request["messages"][1]["content"])
            calls.append(request["model"])
            narrative = {"evidence_digest": evidence["evidence_digest"], "blocks": [{"claim_code": "risk_summary", "fact_refs": [evidence["facts"][0]["fact_ref"]], "risk_refs": [evidence["facts"][0]["risk_ref"]]}]}
            body = json.dumps({"message": {"content": json.dumps(narrative)}}).encode()
            header = ("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + str(len(body)) + "\r\nConnection: close\r\n\r\n").encode()
            begun.set()
            try:
                if mode["slow"]:
                    if phase == "body":
                        self.wfile.write(header)
                        self.wfile.flush()
                    for byte in header + body if phase == "headers" else body:
                        self.wfile.write(bytes([byte]))
                        self.wfile.flush()
                        time.sleep(.04)
                else:
                    self.wfile.write(header + body)
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                closed.set()
    provider = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=provider.serve_forever, daemon=True)
    thread.start()
    original_get = service.reports.connections.get
    service.reports.connections.get = lambda key: dict(original_get(key), base_url="http://127.0.0.1:" + str(provider.server_port), timeout_seconds=.25)
    service.reports.model_factory = create_report_model_client
    try:
        run = service.reports.create(report_request("drip-" + phase), "fixture-actor")
        started = time.monotonic()
        service.reports.execute(run["report_id"])
        elapsed = time.monotonic() - started
        assert begun.is_set() and elapsed < 1.0
        assert closed.wait(1.0), "deadline must close the provider socket"
        final = service.reports.get(run["report_id"])
        assert final["status"] == "failed" and final["error"]["code"] == "report_model_unavailable"
        assert final["artifact"] is None and len(calls) == 1
        service.reports.execute(run["report_id"])
        assert len(calls) == 1
        mode["slow"] = False
        normal = service.reports.create(report_request("normal-" + phase), "fixture-actor")
        service.reports.execute(normal["report_id"])
        assert service.reports.get(normal["report_id"])["status"] == "completed" and len(calls) == 2
        assert service.reports.download(normal["report_id"]).startswith(b"<!doctype")
    finally:
        provider.shutdown()
        provider.server_close()
        thread.join(2)


def test_real_provider_cancellation_closes_socket_and_releases_worker_for_normal_call(tmp_path):
    import threading
    import time
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from logrisk.server_risk.reports import create_report_model_client
    service, now, *_ = domain_services(tmp_path)
    seed(service, now)
    begun, closed = threading.Event(), threading.Event()
    calls = []
    mode = {"slow": True}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            evidence = json.loads(request["messages"][1]["content"])
            calls.append(request["model"])
            narrative = {"evidence_digest": evidence["evidence_digest"], "blocks": [{"claim_code": "risk_summary", "fact_refs": [evidence["facts"][0]["fact_ref"]], "risk_refs": []}]}
            body = json.dumps({"message": {"content": json.dumps(narrative)}}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            begun.set()
            try:
                if mode["slow"]:
                    for byte in body:
                        self.wfile.write(bytes([byte]))
                        self.wfile.flush()
                        time.sleep(.04)
                else:
                    self.wfile.write(body)
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                closed.set()
    provider = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=provider.serve_forever, daemon=True)
    thread.start()
    original_get = service.reports.connections.get
    service.reports.connections.get = lambda key: dict(original_get(key), base_url="http://127.0.0.1:" + str(provider.server_port), timeout_seconds=5)
    service.reports.model_factory = create_report_model_client
    try:
        run = service.reports.create(report_request("cancel-drip"), "fixture-actor")
        worker = threading.Thread(target=service.reports.execute, args=(run["report_id"],))
        worker.start()
        assert begun.wait(2)
        started = time.monotonic()
        service.reports.cancel(run["report_id"])
        worker.join(1)
        assert not worker.is_alive() and time.monotonic() - started < 1
        assert closed.wait(1)
        final = service.reports.get(run["report_id"])
        assert final["status"] == "cancelled" and final["artifact"] is None and len(calls) == 1
        mode["slow"] = False
        normal = service.reports.create(report_request("after-cancel"), "fixture-actor")
        service.reports.execute(normal["report_id"])
        assert service.reports.get(normal["report_id"])["status"] == "completed" and len(calls) == 2
    finally:
        provider.shutdown()
        provider.server_close()
        thread.join(2)


def test_qwen_disabled_connection_and_missing_profile_reject_without_runs_then_ling_succeeds(tmp_path):
    from dataclasses import replace
    service, now, _, _, fake = domain_services(tmp_path)
    seed(service, now)
    original_get = service.reports.connections.get
    cases = [("qwen3_1_7b_fast", True, "model_profile_mismatch"), ("ling_3_0_tiny_8b_q4km", False, "model_profile_unavailable"), ("missing-profile", True, "model_profile_unavailable")]
    for index, (profile, enabled, code) in enumerate(cases):
        service.reports.connections.get = lambda key, active=enabled: dict(original_get(key), enabled=active)
        with pytest.raises(ServerRiskError) as caught:
            service.reports.create(dict(report_request("rejected-" + str(index)), model_profile_id=profile), "fixture-actor")
        assert caught.value.code == code and not fake.calls
        with service.repository.database.connect() as conn:
            assert conn.execute("SELECT COUNT(*) FROM server_risk_reports").fetchone()[0] == 0
    service.reports.connections.get = original_get
    profile_id = "ling_3_0_tiny_8b_q4km"
    profile = service.reports.profiles.get(profile_id)
    service.reports.profiles._profiles[profile_id] = replace(profile, enabled=False)
    with pytest.raises(ServerRiskError) as caught:
        service.reports.create(report_request("disabled-profile"), "fixture-actor")
    assert caught.value.code == "model_profile_unavailable" and not fake.calls
    with service.repository.database.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM server_risk_reports").fetchone()[0] == 0
    service.reports.profiles._profiles[profile_id] = profile
    good = service.reports.create(report_request("valid-ling"), "fixture-actor")
    service.reports.execute(good["report_id"])
    assert service.reports.get(good["report_id"])["status"] == "completed" and len(fake.calls) == 1


def test_missing_registered_artifact_is_rejected_then_new_normal_run_downloads(tmp_path):
    service, now, _, _, fake = domain_services(tmp_path)
    seed(service, now)
    run = service.reports.create(report_request("missing-file"), "fixture-actor")
    service.reports.execute(run["report_id"])
    assert service.reports.download(run["report_id"]).startswith(b"<!doctype")
    (service.reports.root / run["report_id"] / "report.html").unlink()
    with pytest.raises(ServerRiskError) as caught:
        service.reports.download(run["report_id"])
    assert caught.value.code == "artifact_path_error"
    service.reports.execute(run["report_id"])
    assert len(fake.calls) == 1
    good = service.reports.create(report_request("after-missing"), "fixture-actor")
    service.reports.execute(good["report_id"])
    assert service.reports.get(good["report_id"])["status"] == "completed" and len(fake.calls) == 2
    assert service.reports.download(good["report_id"]).startswith(b"<!doctype")
