from __future__ import annotations

import json
import os
import subprocess
import threading
from pathlib import Path

import pytest

from logrisk import __version__
from logrisk.mcp_server import MCPServer


def _list(params):
    root = Path(__file__).resolve().parents[1]
    frames = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-11-25", "capabilities": {},
            "clientInfo": {"name": "compatibility-test", "version": "1"}}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": params},
    ]
    process = subprocess.run(
        [str(root / ".venv/bin/python"), "-B", "-m", "logrisk.mcp_server",
         "--api-base", "http://127.0.0.1:9"], cwd=root,
        env={"PATH": "/usr/bin:/bin", "PYTHONPATH": str(root / "src"),
             "PYTHONDONTWRITEBYTECODE": "1"},
        input="".join(json.dumps(frame) + "\n" for frame in frames),
        capture_output=True, text=True, timeout=10,
    )
    assert process.returncode == 0, process.stderr
    values = [json.loads(line) for line in process.stdout.splitlines()]
    assert values[0]["result"]["serverInfo"]["version"] == __version__
    return next(value for value in values if value.get("id") == 2)


@pytest.mark.parametrize("params", [
    {}, {"_meta": {}}, {"_meta": {"progressToken": "progress-1"}},
    {"_meta": {"progressToken": 0}},
    {"_meta": {"io.example/client": {"name": "test"}}},
])
def test_tools_list_preserves_single_page_with_standard_metadata(params):
    value = _list(params)
    assert "error" not in value
    names = [tool["name"] for tool in value["result"]["tools"]]
    assert len(names) == len(set(names)) == 7
    assert "logrisk_day_evidence" in names
    assert "nextCursor" not in value["result"]


@pytest.mark.parametrize("params", [
    {"cursor": "unissued"}, {"cursor": ""}, {"cursor": None},
    {"cursor": 0}, {"cursor": False}, {"cursor": []},
    {"cursor": "unissued", "_meta": {}}, {"unexpected": True},
    {"_meta": None}, {"_meta": []}, {"_meta": "invalid"},
    {"_meta": {"progressToken": False}},
    {"_meta": {"progressToken": 1.5}},
    {"_meta": {"progressToken": None}},
])
def test_tools_list_rejects_unissued_cursor_and_invalid_fields(params):
    value = _list(params)
    assert value["error"]["code"] == -32602
    assert "result" not in value


def _call(params):
    server = MCPServer("http://127.0.0.1:9")
    responses, attempts = [], []
    server.emit = responses.append
    def transport(method, path, body, cancelled):
        attempts.append((method, path, body))
        return {"items": [], "summary": {}, "total": 0, "next_cursor": None}, False
    server.http = transport
    server.call(1, params, threading.Event())
    return responses[0], attempts


def test_call_metadata_does_not_enter_business_arguments():
    value, attempts = _call({"name": "logrisk_day_evidence", "arguments": {
        "cluster": "prod-a", "node_id": "node-01", "date": "2026-08-31",
        "timezone": "Asia/Shanghai"}, "_meta": {"progressToken": "progress-1"}})
    assert value["result"]["isError"] is False
    assert len(attempts) == 1 and attempts[0][0] == "GET"
    assert "_meta" not in attempts[0][1]
    assert attempts[0][2] is None


@pytest.mark.parametrize("change", ["extra_protocol", "extra_argument", "bad_meta"])
def test_call_metadata_preserves_strict_business_schema(change):
    params = {"name": "logrisk_day_evidence", "arguments": {
        "cluster": "prod-a", "node_id": "node-01", "date": "2026-08-31",
        "timezone": "Asia/Shanghai"}, "_meta": {}}
    if change == "extra_protocol":
        params["actor"] = "admin"
    elif change == "extra_argument":
        params["arguments"]["actor"] = "admin"
    else:
        params["_meta"] = {"progressToken": True}
    value, attempts = _call(params)
    assert value["error"]["code"] == -32602
    assert attempts == []
