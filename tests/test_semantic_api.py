from __future__ import annotations

import json
import threading
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from logrisk.feature_jobs import FeatureJobManager
from pipeline.dashboard_server import build_server


def request_json(url: str, method: str = "GET", payload: object | None = None):
    data = json.dumps(payload).encode() if payload is not None else None
    request = Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=3) as response:
        return response.status, json.load(response)


@pytest.fixture
def semantic_api(tmp_path):
    frontend = tmp_path / "index.html"
    frontend.write_text("<!doctype html>", encoding="utf-8")
    server = build_server(
        "127.0.0.1",
        0,
        manager=FeatureJobManager(auto_start=False),
        frontend_path=frontend,
        drain_quality_root=tmp_path / "drain_quality",
        semantic_root=tmp_path / "semantic",
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


def test_dictionary_candidate_validate_publish_and_rollback_api(semantic_api):
    _, listing = request_json(semantic_api + "/api/semantic/dictionaries")
    _, detail = request_json(semantic_api + "/api/semantic/dictionaries/linux")
    created_status, candidate = request_json(
        semantic_api + "/api/semantic/dictionaries/linux/candidates", "POST", {"operator": "qa"},
    )
    saved_status, saved = request_json(
        semantic_api + f"/api/semantic/dictionaries/linux/candidates/{candidate['version']}",
        "PUT", {"custom_rules": [], "operator": "qa"},
    )
    _, validation = request_json(
        semantic_api + f"/api/semantic/dictionaries/linux/candidates/{saved['version']}/validate", "POST", {},
    )
    _, published = request_json(
        semantic_api + f"/api/semantic/dictionaries/linux/candidates/{saved['version']}/publish",
        "POST", {"confirmed": True, "operator": "qa"},
    )
    _, rolled_back = request_json(
        semantic_api + "/api/semantic/dictionaries/linux/rollback",
        "POST", {"version": 1, "confirmed": True, "operator": "qa"},
    )

    assert {item["dictionary_id"] for item in listing["items"]} >= {"linux", "nvidia"}
    assert detail["builtin_read_only"] is True
    assert created_status == 201
    assert saved_status == 201
    assert validation["valid"] is True
    assert published["status"] == "published"
    assert rolled_back["active_version"] == 1


def test_semantic_test_and_template_summary_api(semantic_api):
    _, extracted = request_json(semantic_api + "/api/semantic/test", "POST", {
        "message_core": "NVRM: Xid 79, GPU has fallen off the bus",
        "source_type": "syslog",
        "component": "kernel",
    })
    request_json(semantic_api + "/api/drain-quality/templates/import", "POST", {
        "templates": [{
            "template_hash": "hash-1",
            "template_fingerprint": "fingerprint-1",
            "template": "NVRM: Xid <NUM>",
            "component": "kernel",
            "count": 2,
            "semantic_fields": {"xid_code": [{"value": 79, "count": 2}]},
            "semantic_tags": ["GPU", "Xid"],
            "typed_parameters": [{"field": "xid_code", "typed_mask": "<XID_CODE>", "count": 2}],
        }],
    })
    _, summary = request_json(semantic_api + "/api/templates/fingerprint-1/semantic-summary")

    assert extracted["semantic_fields"]["xid_code"] == 79
    assert summary["semantic_fields"]["xid_code"][0]["value"] == 79
    assert "GPU" in summary["semantic_tags"]


def test_publish_without_validation_is_rejected(semantic_api):
    _, candidate = request_json(
        semantic_api + "/api/semantic/dictionaries/kubernetes/candidates", "POST", {},
    )

    with pytest.raises(HTTPError) as error:
        request_json(
            semantic_api + f"/api/semantic/dictionaries/kubernetes/candidates/{candidate['version']}/publish",
            "POST", {"confirmed": True},
        )

    assert error.value.code == 400
    assert "校验" in json.load(error.value)["error"]
