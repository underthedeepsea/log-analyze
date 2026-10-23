from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from logrisk import __version__
from logrisk.mcp_server import MCPServer
from logrisk.server_risk.contracts import digest
from logrisk.server_risk.render import CSP
from pipeline.dashboard_server import APP_VERSION
from tests.server_risk_fixtures import fixture_server, report_request, request_http, wait_report


def test_release_metadata_and_runtime_entrypoints_share_application_version(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    release = re.search(r'^version = "([^"]+)"', (root / "pyproject.toml").read_text(), re.M).group(1)
    package = json.loads((root / "frontend/package.json").read_text())
    lock = json.loads((root / "frontend/package-lock.json").read_text())
    assert release == __version__ == APP_VERSION == package["version"] == lock["version"] == lock["packages"][""]["version"]
    assert f"当前版本：`{release}`" in (root / "README.md").read_text()
    assert f"## {release} -" in (root / "releas.md").read_text()
    server = MCPServer("http://127.0.0.1:1")
    responses = []
    monkeypatch.setattr(server, "emit", responses.append)
    server.message({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "fixture", "version": "1"}}})
    assert responses[0]["result"]["serverInfo"]["version"] == release


def test_django_facade_uses_common_application_version(monkeypatch):
    from logrisk_django import service_factory

    monkeypatch.setattr(service_factory, "_facade", None)
    monkeypatch.setattr(service_factory, "get_container", lambda: None)
    monkeypatch.setattr(service_factory, "ApiFacade", lambda container, **kwargs: SimpleNamespace(**kwargs))
    assert service_factory.get_facade().version == __version__


def test_http_fake_report_download_has_current_version_and_preserves_safe_evidence(tmp_path):
    server, fake, base, thread = fixture_server(tmp_path)
    repository = server.server_risk.repository
    original_facts = repository.facts
    repository.facts = lambda *args: [dict(fact, object_id="<script>alert(1)</script>") for fact in original_facts(*args)]
    try:
        status, health, _ = request_http(base, "/api/health")
        assert status == 200 and health["version"] == __version__
        status, submitted, _ = request_http(base, "/api/node-risk-reports", "POST", report_request("release-version"))
        assert status == 202
        completed = wait_report(base, submitted["report_id"])
        assert completed["status"] == "completed", completed
        status, body, headers = request_http(base, completed["artifact"]["url"])
        assert status == 200 and isinstance(body, bytes)
        assert (" · " + health["version"] + " · ").encode() in body
        assert b"1.43.0" not in body
        assert b"<script" not in body and b"&lt;script&gt;alert(1)&lt;/script&gt;" in body
        assert headers["Content-Security-Policy"] == CSP
        assert headers["X-Content-Type-Options"] == "nosniff"
        assert "attachment" in headers["Content-Disposition"] and "text/html" in headers["Content-Type"]
        assert hashlib.sha256(body).hexdigest() == completed["artifact"]["digest"]
        assert len(body) == completed["artifact"]["bytes"]
        with server.database.connect() as conn:
            row = conn.execute("SELECT evidence_snapshot_json FROM server_risk_reports WHERE report_id=?", (completed["report_id"],)).fetchone()
        snapshot = json.loads(row[0])
        evidence_digest = snapshot.pop("evidence_digest")
        assert digest(snapshot) == evidence_digest == submitted["evidence_digest"] == completed["evidence_digest"] == fake.calls[0]["evidence_digest"]
        assert ("digest " + evidence_digest).encode() in body
        assert len(fake.calls) == 1
    finally:
        server.server_risk.source_worker.close()
        server.node_risk_report_worker.close()
        server.shutdown()
        server.server_close()
        thread.join(2)


def entry_environment(root: Path, tmp_path: Path, entry_mode: str) -> tuple[Path, dict[str, str]]:
    env = {
        key: value for key, value in os.environ.items()
        if key not in {"PYTHONPATH", "PYTHONHOME"}
        and not key.startswith(("LOGRISK_", "OLLAMA_", "DASHBOARD_"))
    }
    env.update(PYTHONDONTWRITEBYTECODE="1", PYTHONNOUSERSITE="1")
    if entry_mode == "repository_root":
        return root, env
    external = tmp_path / "external"
    external.mkdir()
    env["PYTHONPATH"] = str(root / "src")
    return external, env


@pytest.mark.parametrize("entry_mode", ("repository_root", "source_path"))
def test_real_package_entry_initializes_and_lists_mcp_tools(tmp_path, entry_mode):
    root = Path(__file__).resolve().parents[1]
    cwd, env = entry_environment(root, tmp_path, entry_mode)
    probe = subprocess.run(
        [sys.executable, "-B", "-c", (
            "import json, logrisk, logrisk.version; "
            "print(json.dumps({'package': logrisk.__file__, "
            "'version_module': logrisk.version.__file__, "
            "'public': logrisk.__version__, 'canonical': logrisk.version.__version__}))"
        )],
        cwd=cwd, env=env, text=True, encoding="utf-8", capture_output=True, timeout=15,
    )
    assert probe.returncode == 0, probe.stderr
    selected = json.loads(probe.stdout)
    package_dir = root if entry_mode == "repository_root" else root / "src"
    assert Path(selected["package"]).resolve() == package_dir / "logrisk/__init__.py"
    assert Path(selected["version_module"]).resolve() == root / "src/logrisk/version.py"
    assert selected["public"] == selected["canonical"] == __version__
    messages = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "entry-fixture", "version": "1"}}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
    ]
    completed = subprocess.run(
        [sys.executable, "-B", "-m", "logrisk.mcp_server", "--api-base", "http://127.0.0.1:1"],
        input="".join(json.dumps(message) + "\n" for message in messages),
        cwd=cwd, env=env, text=True, encoding="utf-8", capture_output=True, timeout=15,
    )
    assert completed.returncode == 0, completed.stderr
    responses = [json.loads(line) for line in completed.stdout.splitlines()]
    assert [response["id"] for response in responses] == [1, 2], responses
    assert all(response["jsonrpc"] == "2.0" and "error" not in response for response in responses)
    assert responses[0]["result"]["serverInfo"]["version"] == __version__
    tools = responses[1]["result"]["tools"]
    assert len(tools) == 7 and len({tool["name"] for tool in tools}) == 7
    assert all(isinstance(tool["inputSchema"], dict) for tool in tools)
    assert "nextCursor" not in responses[1]["result"]


def state_snapshot(state_path: Path) -> dict[str, object]:
    try:
        root_stat = state_path.lstat()
    except FileNotFoundError:
        return {"exists": False}
    paths = [state_path]
    if stat.S_ISDIR(root_stat.st_mode):
        paths.extend(sorted(state_path.rglob("*")))
    entries = {}
    for path in paths:
        info = path.lstat()
        item = {
            "mode": info.st_mode, "size": info.st_size,
            "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns,
            "inode": info.st_ino, "device": info.st_dev,
            "uid": info.st_uid, "gid": info.st_gid, "nlink": info.st_nlink,
        }
        if stat.S_ISREG(info.st_mode):
            item["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        elif stat.S_ISLNK(info.st_mode):
            item["symlink_target"] = os.readlink(path)
        entries[str(path.relative_to(state_path))] = item
    # Reading the snapshot can update atime; content and mutation metadata remain checked.
    return {"exists": True, "entries": entries}


def assert_dashboard_entry_help_preserves_state(
    root: Path, cwd: Path, env: dict[str, str], entry_mode: str,
) -> None:
    state_before = state_snapshot(cwd / "state")
    # The Dashboard launcher supplies src for pipeline; root cwd still selects the logrisk shim.
    env["PYTHONPATH"] = str(root / "src")
    probe = subprocess.run(
        [sys.executable, "-B", "-c", (
            "import json, logrisk; from pipeline.dashboard_server import APP_VERSION; "
            "print(json.dumps({'package': logrisk.__file__, 'dashboard': APP_VERSION}))"
        )],
        cwd=cwd, env=env, text=True, encoding="utf-8", capture_output=True, timeout=15,
    )
    assert probe.returncode == 0, probe.stderr
    assert state_snapshot(cwd / "state") == state_before, "Import changed state"
    selected = json.loads(probe.stdout)
    package_dir = root if entry_mode == "repository_root" else root / "src"
    assert Path(selected["package"]).resolve() == (package_dir / "logrisk/__init__.py").resolve()
    assert selected["dashboard"] == __version__
    completed = subprocess.run(
        [sys.executable, "-B", "-m", "pipeline.dashboard_server", "--help"],
        cwd=cwd, env=env, text=True, encoding="utf-8", capture_output=True, timeout=15,
    )
    assert completed.returncode == 0, completed.stderr
    assert "usage:" in completed.stdout and "--database-provider" in completed.stdout
    assert "Feature review dashboard:" not in completed.stdout
    assert state_snapshot(cwd / "state") == state_before, "--help changed state"


@pytest.mark.parametrize("entry_mode", ("repository_root", "source_path"))
def test_real_dashboard_entry_help_exits_without_starting_service(tmp_path, entry_mode):
    root = Path(__file__).resolve().parents[1]
    cwd, env = entry_environment(root, tmp_path, entry_mode)
    assert_dashboard_entry_help_preserves_state(root, cwd, env, entry_mode)


@pytest.mark.parametrize("entry_mode", ("repository_root", "source_path"))
@pytest.mark.parametrize("preexisting_state", (False, True), ids=("absent_state", "existing_state"))
def test_dashboard_entry_preserves_absent_or_existing_state(tmp_path, entry_mode, preexisting_state):
    source_root = Path(__file__).resolve().parents[1]
    root = tmp_path / "repository"
    (root / "logrisk").mkdir(parents=True)
    shutil.copy2(source_root / "logrisk/__init__.py", root / "logrisk/__init__.py")
    (root / "src").symlink_to(source_root / "src", target_is_directory=True)
    cwd, env = entry_environment(root, tmp_path, entry_mode)
    state_path = cwd / "state"
    if preexisting_state:
        (state_path / "nested/empty").mkdir(parents=True)
        (state_path / "ai_traces.jsonl").write_text('{"fixture":"retained-state"}\n')
        (state_path / "nested/sentinel.bin").write_bytes(b"\x00retained-state\xff")
        (state_path / "sentinel-link").symlink_to("nested/sentinel.bin")
    assert state_snapshot(state_path)["exists"] is preexisting_state
    assert_dashboard_entry_help_preserves_state(root, cwd, env, entry_mode)
    assert state_snapshot(state_path)["exists"] is preexisting_state
