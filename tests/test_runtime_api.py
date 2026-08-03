from __future__ import annotations

import json
import threading
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from logrisk.feature_jobs import FeatureJobManager
from logrisk.runtime.config import RuntimeConfig
from pipeline.dashboard_server import build_server


@pytest.fixture
def runtime_dashboard(tmp_path):
    frontend = tmp_path / "dist" / "index.html"
    frontend.parent.mkdir()
    frontend.write_text("<!doctype html><title>Runtime Dashboard</title>", encoding="utf-8")
    server = build_server(
        "127.0.0.1",
        0,
        manager=FeatureJobManager(extractor=lambda *_args, **_kwargs: [], auto_start=True),
        frontend_path=frontend,
        database_path=tmp_path / "state" / "logrisk.sqlite3",
        state_root=tmp_path / "state",
        runtime_config=RuntimeConfig.from_mapping(
            {
                "identity": {
                    "enabled": True,
                    "allow_loopback_bypass": False,
                    "trusted_proxy_cidrs": ["127.0.0.0/8"],
                    "write_roles": ["logrisk:operator"],
                },
                "retention": {"enabled": True},
            }
        ),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


@pytest.fixture
def quota_dashboard(tmp_path):
    frontend = tmp_path / "dist" / "index.html"
    frontend.parent.mkdir()
    frontend.write_text("<!doctype html><title>Quota Dashboard</title>", encoding="utf-8")
    server = build_server(
        "127.0.0.1",
        0,
        manager=FeatureJobManager(extractor=lambda *_args, **_kwargs: [], auto_start=True),
        frontend_path=frontend,
        database_path=tmp_path / "state" / "logrisk.sqlite3",
        state_root=tmp_path / "state",
        runtime_config=RuntimeConfig.from_mapping({"quota": {"soft_limit_bytes": 1, "hard_limit_bytes": 1}}),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


def request_json(url: str, method: str = "GET", payload: dict | None = None, headers: dict[str, str] | None = None):
    request = Request(
        url,
        data=json.dumps(payload).encode() if payload is not None else None,
        method=method,
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    try:
        with urlopen(request, timeout=3) as response:
            return response.status, json.load(response), dict(response.headers)
    except HTTPError as exc:
        return exc.code, json.load(exc), dict(exc.headers)


def test_runtime_identity_from_trusted_proxy_is_available(runtime_dashboard) -> None:
    status, payload, _ = request_json(
        runtime_dashboard + "/api/runtime/identity",
        headers={
            "X-LOGRISK-Actor": "alice",
            "X-LOGRISK-Roles": "logrisk:operator",
            "X-Request-ID": "gateway-request-1",
        },
    )

    assert status == 200
    assert payload["identity"]["actor"] == "alice"
    assert payload["identity"]["request_id"] == "gateway-request-1"


def test_runtime_maintenance_write_requires_identity(runtime_dashboard) -> None:
    status, payload, _ = request_json(
        runtime_dashboard + "/api/runtime/retention/execute", "POST", {}
    )

    assert status == 403
    assert payload["error_code"] == "runtime_identity_required"
    assert payload["request_id"].startswith("request-")


def test_runtime_retention_preview_with_external_identity_returns_audit_metadata(runtime_dashboard) -> None:
    status, payload, headers = request_json(
        runtime_dashboard + "/api/runtime/retention/preview",
        "POST",
        {},
        {
            "X-LOGRISK-Actor": "alice",
            "X-LOGRISK-Roles": "logrisk:operator",
            "X-Request-ID": "gateway-request-2",
        },
    )

    assert status == 200
    assert payload["request_id"] == "gateway-request-2"
    assert payload["maintenance"]["mode"] == "dry_run"
    assert {key.lower(): value for key, value in headers.items()}["x-request-id"] == "gateway-request-2"


def test_runtime_retention_policy_is_versioned_and_audited(runtime_dashboard) -> None:
    headers = {
        "X-LOGRISK-Actor": "alice",
        "X-LOGRISK-Roles": "logrisk:operator",
        "X-Request-ID": "gateway-request-3",
    }
    status, payload, _ = request_json(
        runtime_dashboard + "/api/runtime/retention/policy",
        "POST",
        {
            "expected_version": 0,
            "policy": {"enabled": True, "completed_days": 7, "trace_days": 14, "cache_days": 3},
        },
        headers,
    )

    assert status == 200
    assert payload["policy"]["version"] == 1
    assert payload["policy"]["policy"]["completed_days"] == 7
    assert payload["resource_id"] == "default"
    assert payload["version"] == 1

    status, payload, _ = request_json(runtime_dashboard + "/api/runtime/retention")

    assert status == 200
    assert payload["effective"]["completed_days"] == 7
    assert payload["policy"]["policy"]["trace_days"] == 14

    status, audits, _ = request_json(runtime_dashboard + "/api/runtime/audits")

    assert status == 200
    request_audit = next(item for item in audits["items"] if item["action"] == "request.post")
    assert request_audit["actor"] == "alice"
    assert request_audit["attributes"] == {
        "method": "POST",
        "path": "/api/runtime/retention/policy",
        "status": 200,
    }

    status, payload, _ = request_json(
        runtime_dashboard + "/api/runtime/retention/policy",
        "POST",
        {"expected_version": 0, "policy": {"completed_days": 8}},
        headers,
    )

    assert status == 409
    assert payload["error_code"] == "runtime_version_conflict"


def test_release_readiness_requires_identity_and_records_an_idempotent_validation(runtime_dashboard) -> None:
    headers = {
        "X-LOGRISK-Actor": "alice",
        "X-LOGRISK-Roles": "logrisk:operator",
        "X-Request-ID": "gateway-release-ready-1",
    }

    status, overview, _ = request_json(runtime_dashboard + "/api/release-readiness")
    assert status == 200
    assert overview["latest"] is None

    status, denied, _ = request_json(
        runtime_dashboard + "/api/release-readiness/validate",
        "POST",
        {"target_version": "1.30.0", "idempotency_key": "release-ready-api-test"},
    )
    assert status == 403
    assert denied["error_code"] == "runtime_identity_required"

    payload = {"target_version": "1.30.0", "idempotency_key": "release-ready-api-test"}
    status, first, _ = request_json(runtime_dashboard + "/api/release-readiness/validate", "POST", payload, headers)
    duplicate_status, duplicate, _ = request_json(runtime_dashboard + "/api/release-readiness/validate", "POST", payload, headers)
    history_status, history, _ = request_json(runtime_dashboard + "/api/release-readiness/history")

    assert status == 200
    assert first["status"] in {"passed", "warning", "blocked"}
    assert duplicate_status == 200
    assert duplicate["validation_id"] == first["validation_id"]
    assert history_status == 200
    assert history["items"][0]["validation_id"] == first["validation_id"]


def test_runtime_quota_blocks_new_upload_session(quota_dashboard) -> None:
    status, payload, _ = request_json(
        quota_dashboard + "/api/uploads",
        "POST",
        {"filename": "messages", "size_bytes": 128},
    )

    assert status == 507
    assert payload["error_code"] == "runtime_quota_exceeded"
