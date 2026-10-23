from __future__ import annotations

import json
import os
import selectors
import subprocess
import sys
import threading
from pathlib import Path
import pytest
from logrisk.mcp_server import api_base
from logrisk.mcp_tools import list_tools
from tests.server_risk_fixtures import fixture_server, report_request, request_http, wait_report


def rpc(process, message):
    process.stdin.write(json.dumps(message, ensure_ascii=False) + "\n"); process.stdin.flush()
    if "id" not in message: return None
    selector = selectors.DefaultSelector(); selector.register(process.stdout, selectors.EVENT_READ)
    assert selector.select(10), "stdio response deadline exceeded"
    line = process.stdout.readline(); selector.close()
    assert line and line.startswith("{"), line
    response = json.loads(line); assert response["jsonrpc"] == "2.0" and response["id"] == message["id"]
    return response


def start(base):
    return subprocess.Popen([sys.executable, "-m", "logrisk.mcp_server", "--api-base", base], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)


def initialize(process):
    response = rpc(process, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "fixture", "version": "1"}}})
    assert response["result"]["capabilities"] == {"tools": {}}
    rpc(process, {"jsonrpc": "2.0", "method": "notifications/initialized"})


def test_stdio_protocol_initialization_bounded_input_and_fixed_schema():
    process = start("http://127.0.0.1:1")
    try:
        assert rpc(process, {"jsonrpc": "2.0", "id": 0, "method": "tools/list"})["error"]["code"] == -32002
        initialize(process)
        tools = rpc(process, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"})["result"]["tools"]
        assert tools == list_tools() and len(tools) == 7
        for index, extra in enumerate(("sql", "path", "actor", "roles", "api_base"), 3):
            response = rpc(process, {"jsonrpc": "2.0", "id": index, "method": "tools/call", "params": {"name": "logrisk_get_report", "arguments": {"report_id": "a", extra: "anything"}}})
            assert response["error"]["code"] == -32602
        refused = rpc(process, {"jsonrpc": "2.0", "id": 8, "method": "tools/call", "params": {"name": "logrisk_find_nodes", "arguments": {}}})
        assert refused["result"]["isError"] and refused["result"]["structuredContent"]["code"] == "application_unavailable"
        process.stdin.write("x" * 1048577 + "\n"); process.stdin.flush()
        assert json.loads(process.stdout.readline())["error"]["code"] == -32600
        process.wait(timeout=5); assert not process.stderr.read()
    finally:
        if process.poll() is None: process.terminate(); process.wait(timeout=5)
    for value in ("http://example.com:80", "http://127.0.0.1", "https://127.0.0.1:1", "http://u:p@127.0.0.1:1", "http://127.0.0.1:1/path"):
        with pytest.raises(ValueError): api_base(value)


def test_real_stdio_seven_tools_use_http_and_persistent_report_lifecycle(tmp_path):
    server, fake, base, thread = fixture_server(tmp_path)
    process = start(base); request_id = 10
    records = []
    def call(name, arguments):
        nonlocal request_id
        request_id += 1
        response = rpc(process, {"jsonrpc": "2.0", "id": request_id, "method": "tools/call", "params": {"name": name, "arguments": arguments}})
        assert "result" in response, response
        result = response["result"]; records.append({"tool": name, "isError": result["isError"], "schema_version": result["structuredContent"].get("schema_version")})
        assert not result["isError"], result
        return result["structuredContent"]
    try:
        initialize(process)
        assert call("logrisk_find_nodes", {})["total"] == 2
        scope = {"cluster": "prod-a", "node_id": "node-01"}
        assert call("logrisk_daily_series", {**scope, "from": "2026-08-31", "to": "2026-09-01", "timezone": "Asia/Shanghai"})["items"][1]["occurrence_count"] is None
        assert call("logrisk_day_evidence", {**scope, "date": "2026-08-31", "timezone": "Asia/Shanghai"})["summary"]["known_occurrence_count"] == 7
        assert call("logrisk_current_assessment", scope)["status"] == "unknown"
        payload = report_request("mcp-report"); payload.pop("schema_version")
        run = call("logrisk_generate_day_report", payload)
        final = wait_report(base, run["report_id"])
        assert call("logrisk_get_report", {"report_id": run["report_id"]})["status"] == "completed"
        assert call("logrisk_cancel_report", {"report_id": run["report_id"], "idempotency_key": "cancel-finished"})["status"] == "completed"
        assert request_http(base, final["artifact"]["url"])[0] == 200 and len(fake.calls) == 1
        begun, release = threading.Event(), threading.Event()
        original = fake.generate_json
        def blocking(*args, **kwargs):
            begun.set(); assert release.wait(5); return original(*args, **kwargs)
        fake.generate_json = blocking
        blocked = call("logrisk_generate_day_report", dict(payload, idempotency_key="mcp-cancel"))
        assert begun.wait(3)
        cancelled = call("logrisk_cancel_report", {"report_id": blocked["report_id"], "idempotency_key": "cancel-inflight"})
        assert cancelled["cancel_requested"] and cancelled["status"] == "running"
        release.set()
        assert wait_report(base, blocked["report_id"])["status"] == "cancelled"
        output = Path("output/server-risk-auto-recovery-continuation/author/mcp-stdio-evidence.json")
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps({"records": records, "model_calls": len(fake.calls), "actual_model_identity": "fake", "usage": None}, indent=2))
    finally:
        process.terminate(); process.wait(timeout=5)
        assert not process.stderr.read()
        server.node_risk_report_worker.close(); server.shutdown(); server.server_close(); thread.join(2)


def test_real_stdio_malformed_cancellation_notifications_do_not_kill_process(tmp_path):
    server, _, base, thread = fixture_server(tmp_path)
    process = start(base)
    try:
        initialize(process)
        invalid = ([], {}, None, True, 1.5)
        for index, target in enumerate(invalid, 20):
            rpc(process, {"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": target}})
            response = rpc(process, {"jsonrpc": "2.0", "id": index, "method": "tools/list"})
            assert response["result"]["tools"] == list_tools() and process.poll() is None
        for params in ({"requestId": 1, "reason": []}, {"requestId": 1, "extra": "bad"}, {}):
            rpc(process, {"jsonrpc": "2.0", "method": "notifications/cancelled", "params": params})
        rpc(process, {"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": "not-active", "reason": "valid notification"}})
        response = rpc(process, {"jsonrpc": "2.0", "id": 30, "method": "tools/call", "params": {"name": "logrisk_find_nodes", "arguments": {}}})
        assert not response["result"]["isError"] and response["result"]["structuredContent"]["total"] == 2
    finally:
        process.terminate()
        process.wait(timeout=5)
        assert not process.stderr.read()
        server.node_risk_report_worker.close()
        server.shutdown()
        server.server_close()
        thread.join(2)


@pytest.mark.parametrize("phase", ["headers", "body"])
def test_mcp_http_absolute_deadline_closes_slow_drip_then_accepts_normal_response(tmp_path, monkeypatch, phase):
    import time
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    import logrisk.mcp_server as mcp
    closed = threading.Event()
    mode = {"slow": True}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            body = b'{"ok":true}'
            header = b'HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n'
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
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(mcp, "HTTP_TIMEOUT", .25)
    adapter = mcp.MCPServer("http://127.0.0.1:" + str(server.server_port))
    try:
        started = time.monotonic()
        value, error = adapter.http("GET", "/api/node-risks", None)
        assert time.monotonic() - started < 1 and error and value["code"] == "application_unavailable"
        assert closed.wait(1), "MCP deadline must close the HTTP socket"
        mode["slow"] = False
        assert adapter.http("GET", "/api/node-risks", None) == ({"ok": True}, False)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)


def test_real_stdio_cancellation_releases_http_and_admission_before_next_tool(tmp_path):
    import time
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    begun, closed = threading.Event(), threading.Event()
    mode = {"slow": True}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            body = b'{"schema_version":"node_risk_nodes_v1","items":[],"total":0,"next_cursor":null}'
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
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    process = start("http://127.0.0.1:" + str(server.server_port))
    try:
        initialize(process)
        process.stdin.write(json.dumps({"jsonrpc": "2.0", "id": 40, "method": "tools/call", "params": {"name": "logrisk_find_nodes", "arguments": {}}}) + "\n")
        process.stdin.flush()
        assert begun.wait(2)
        started = time.monotonic()
        rpc(process, {"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 40, "reason": "stop fixture"}})
        assert closed.wait(1) and time.monotonic() - started < 1
        mode["slow"] = False
        response = rpc(process, {"jsonrpc": "2.0", "id": 41, "method": "tools/call", "params": {"name": "logrisk_find_nodes", "arguments": {}}})
        assert not response["result"]["isError"] and response["result"]["structuredContent"]["total"] == 0
    finally:
        process.terminate()
        process.wait(timeout=5)
        assert not process.stderr.read()
        server.shutdown()
        server.server_close()
        thread.join(2)


def test_real_stdio_date_bounds_and_clock_injection_preserve_shared_error(tmp_path):
    server, _, base, thread = fixture_server(tmp_path)
    process = start(base)
    records = []
    try:
        initialize(process)
        scope = {"cluster": "prod-a", "node_id": "node-01", "timezone": "Asia/Shanghai"}
        for index, day in enumerate(("9999-12-31", "0001-01-01", "2026-02-30"), 501):
            result = rpc(process, {"jsonrpc": "2.0", "id": index, "method": "tools/call", "params": {"name": "logrisk_day_evidence", "arguments": {**scope, "date": day}}})
            assert result["error"]["code"] == -32602 and result["error"]["data"]["code"] == "invalid_date_or_timezone"
            records.append({"date": day, "error": result["error"]})
        for index, day in enumerate(("2026-08-31", "0001-01-02", "9999-12-30"), 510):
            result = rpc(process, {"jsonrpc": "2.0", "id": index, "method": "tools/call", "params": {"name": "logrisk_day_evidence", "arguments": {**scope, "date": day}}})
            assert not result["result"]["isError"] and result["result"]["structuredContent"]["evaluation_mode"] == "historical"
            records.append({"date": day, "status": "success"})
        result = rpc(process, {"jsonrpc": "2.0", "id": 520, "method": "tools/call", "params": {"name": "logrisk_current_assessment", "arguments": {"cluster": "prod-a", "node_id": "node-01", "mode": "historical", "as_of": "2026-08-31"}}})
        assert result["error"]["code"] == -32602
        result = rpc(process, {"jsonrpc": "2.0", "id": 521, "method": "tools/call", "params": {"name": "logrisk_current_assessment", "arguments": {"cluster": "prod-a", "node_id": "node-01"}}})
        assert result["result"]["structuredContent"]["evaluation_mode"] == "current"
        output = Path("output/server-risk-auto-recovery-continuation/author/date-stdio-evidence.json")
        output.write_text(json.dumps({"stage_hit": "shared_tool_date_validation", "records": records, "current_clock_override_rejected": True}, indent=2))
    finally:
        process.terminate()
        process.wait(timeout=5)
        assert not process.stderr.read()
        server.shutdown()
        server.server_close()
        thread.join(2)
