from __future__ import annotations

import json
import multiprocessing
import socket
import threading
import time
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest

from logrisk.server_risk.contracts import json_text, utc
from logrisk.server_risk.reports import create_report_model_client
from tests.server_risk_fixtures import batch, domain_services, observation, report_request


class BlockingTransportInitializer:
    """Runs exclusively in the helper: block the actual resolver/socket stage."""

    def __init__(self, stage, entered, release):
        self.stage, self.entered, self.release = stage, entered, release

    def __call__(self):
        if self.stage == "dns":
            original = socket.getaddrinfo
            def blocked(*args, **kwargs):
                self.entered.value = 1
                while not self.release.value:
                    time.sleep(.01)
                return original(*args, **kwargs)
            socket.getaddrinfo = blocked
        else:
            original = socket.socket
            entered, release = self.entered, self.release
            class BlockedSocket(original):
                def connect(self, address):
                    entered.value = 1
                    while not release.value:
                        time.sleep(.01)
                    return super().connect(address)
            socket.socket = BlockedSocket


def normal_provider():
    calls = []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass
        def do_POST(self):
            value = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            evidence = json.loads(value["messages"][1]["content"])
            calls.append(value["model"])
            body = json_text({"message": {"content": json_text({"evidence_digest": evidence["evidence_digest"], "blocks": [{"claim_code": "risk_summary", "fact_refs": [evidence["facts"][0]["fact_ref"]], "risk_refs": []}]})}, "done": True}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    return server, thread, calls


@pytest.mark.parametrize("stage", ["dns", "connect"])
@pytest.mark.parametrize("ending", ["provider_deadline", "overall_deadline", "cancel"])
def test_default_report_transport_reaps_blocked_stage_releases_slot_and_following_hostname_succeeds(tmp_path, stage, ending):
    service, now, _, _, _ = domain_services(tmp_path)
    service.observations.submit(batch("seed", observation(1, now["value"])), "fixture-actor")
    provider, provider_thread, calls = normal_provider()
    context = multiprocessing.get_context("spawn")
    entered, release = context.RawValue("i", 0), context.RawValue("i", 0)
    lifecycle = []
    reports = service.reports
    reports.model_factory = create_report_model_client
    reports.transport_initializer = BlockingTransportInitializer(stage, entered, release)
    reports.transport_hook = lambda event, pid: lifecycle.append({"event": event, "pid": pid, "monotonic": time.monotonic()})
    timeout = .9 if ending == "provider_deadline" else 60
    reports.connections = SimpleNamespace(get=lambda _: {"connection_id": "ollama-local", "provider": "ollama", "enabled": True, "timeout_seconds": timeout, "base_url": "http://localhost:" + str(provider.server_address[1])})
    run = reports.create(report_request(stage + ending), "fixture-actor")
    if ending == "overall_deadline":
        with service.repository.database.transaction() as conn:
            conn.execute("UPDATE server_risk_reports SET deadline=? WHERE report_id=?", ((utc(now["value"]) + timedelta(seconds=.9)).isoformat(), run["report_id"]))
    started = time.monotonic()
    execution = threading.Thread(target=reports.execute, args=(run["report_id"],))
    execution.start()
    try:
        enter_deadline = time.monotonic() + 3
        while not entered.value and time.monotonic() < enter_deadline:
            time.sleep(.01)
        assert entered.value == 1, "The target resolver/connect stage must really be entered"
        assert not release.value and not calls
        if ending == "cancel":
            cancel_at = time.monotonic()
            reports.cancel(run["report_id"])
            execution.join(.25)
            assert not execution.is_alive() and time.monotonic() - cancel_at <= .25
            expected = "cancelled"
        else:
            execution.join(2)
            assert not execution.is_alive() and time.monotonic() - started <= 1.15
            expected = "failed"
        assert not release.value, "Resource cleanup must finish while target remains blocked"
        assert [item["event"] for item in lifecycle] == ["transport_started", "transport_reaped"]
        assert reports.execution_lock.acquire(blocking=False)
        reports.execution_lock.release()
        terminal = reports.get(run["report_id"])
        assert terminal["status"] == expected and terminal["artifact"] is None
        assert not list((reports.root / run["report_id"]).glob("*"))
        with service.repository.database.connect() as conn:
            assert conn.execute("SELECT model_calls FROM server_risk_reports WHERE report_id=?", (run["report_id"],)).fetchone()[0] == 1
        assert not any(child.name == "node-risk-http-transport" for child in multiprocessing.active_children())
        reports.transport_initializer = None
        reports.connections = SimpleNamespace(get=lambda _: {"connection_id": "ollama-local", "provider": "ollama", "enabled": True, "timeout_seconds": 3, "base_url": "http://localhost:" + str(provider.server_address[1])})
        normal = reports.create(report_request("normal-" + stage + ending), "fixture-actor")
        reports.execute(normal["report_id"])
        assert reports.get(normal["report_id"])["status"] == "completed"
        assert reports.download(normal["report_id"]).startswith(b"<!doctype")
        assert len(calls) == 1
        release.value = 1
        time.sleep(.03)
        assert reports.get(run["report_id"])["status"] == expected
        assert not list((reports.root / run["report_id"]).glob("*"))
        evidence = {"stage_hit": stage, "ending": ending, "target_blocked_during_cleanup": True, "elapsed": time.monotonic() - started, "lifecycle": lifecycle, "normal_calls": len(calls), "terminal": expected, "slot_released": True, "no_late_artifact": True}
        from pathlib import Path
        out = Path("output/server-risk-auto-recovery-continuation/author/transport-evidence")
        out.mkdir(parents=True, exist_ok=True)
        (out / (stage + "-" + ending + ".json")).write_text(json.dumps(evidence, indent=2))
    finally:
        release.value = 1
        execution.join(2)
        provider.shutdown()
        provider.server_close()
        provider_thread.join(2)
