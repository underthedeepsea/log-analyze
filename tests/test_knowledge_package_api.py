from __future__ import annotations

import hashlib
import io
import json
import threading
import zipfile
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from logrisk.feature_jobs import FeatureJobManager
from pipeline.dashboard_server import build_server


def _archive() -> bytes:
    prompt = (
        b"feature_type must use lowercase_snake_case. Output fields: feature_type, title, summary, "
        b"importance, template_hashes, components, tags, selection_reason. "
        b"Every feature must represent exactly one coherent abnormal pattern. "
        b"If selected templates represent different failure semantics, emit separate features."
    )
    manifest = {
        "schema_version": 1,
        "package_id": "api-demo",
        "name": "API 演示包",
        "version": "1.0.0",
        "description": "API test",
        "platform": {"min_version": "1.32.0", "max_version_exclusive": "2.0.0"},
        "dependencies": [],
        "assets": [{"asset_id": "prompt", "type": "feature_prompt", "path": "assets/prompt.md", "sha256": hashlib.sha256(prompt).hexdigest(), "media_type": "text/markdown"}],
    }
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", json.dumps(manifest).encode())
        archive.writestr("assets/prompt.md", prompt)
    return output.getvalue()


@pytest.fixture
def dashboard(tmp_path):
    frontend = tmp_path / "index.html"
    frontend.write_text("<!doctype html>", encoding="utf-8")
    server = build_server(
        "127.0.0.1",
        0,
        manager=FeatureJobManager(auto_start=False),
        frontend_path=frontend,
        database_path=tmp_path / "state" / "logrisk.sqlite3",
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    yield base
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


def _request(url: str, *, method: str = "GET", body: bytes | None = None, headers: dict[str, str] | None = None):
    request = Request(url, data=body, method=method, headers=headers or {})
    with urlopen(request, timeout=3) as response:
        return response.status, response.headers, response.read()


def test_knowledge_package_upload_preview_install_and_materialize_api(dashboard):
    status, _, body = _request(
        dashboard + "/api/knowledge-packages/uploads",
        method="POST",
        body=_archive(),
        headers={"Content-Type": "application/zip", "X-Package-Filename": "api-demo.logrisk-package.zip"},
    )
    upload = json.loads(body)
    assert status == 201
    upload_id = upload["upload_id"]
    _, _, body = _request(dashboard + "/api/knowledge-packages/uploads/" + upload_id)
    preview = json.loads(body)
    assert preview["status"] == "validated"
    _, _, body = _request(
        dashboard + "/api/knowledge-packages/uploads/" + upload_id + "/install",
        method="POST",
        body=json.dumps({"preview_sha256": preview["inspection"]["package_sha256"], "confirmed": True}).encode(),
        headers={"Content-Type": "application/json"},
    )
    installed = json.loads(body)
    assert installed["assets"][0]["status"] == "disabled"
    _, _, body = _request(
        dashboard + "/api/knowledge-packages/api-demo/versions/1.0.0/assets/prompt/materialize",
        method="POST",
        body=b"{}",
        headers={"Content-Type": "application/json"},
    )
    assert json.loads(body)["status"] == "materialized"


def test_knowledge_package_api_rejects_unconfirmed_install(dashboard):
    status, _, body = _request(
        dashboard + "/api/knowledge-packages/uploads",
        method="POST",
        body=_archive(),
        headers={"Content-Type": "application/zip", "X-Package-Filename": "api-demo.logrisk-package.zip"},
    )
    upload_id = json.loads(body)["upload_id"]
    _, _, body = _request(dashboard + "/api/knowledge-packages/uploads/" + upload_id)
    preview = json.loads(body)
    with pytest.raises(HTTPError) as error:
        _request(
            dashboard + "/api/knowledge-packages/uploads/" + upload_id + "/install",
            method="POST",
            body=json.dumps({"preview_sha256": preview["inspection"]["package_sha256"], "confirmed": False}).encode(),
            headers={"Content-Type": "application/json"},
        )
    assert error.value.code == 422
