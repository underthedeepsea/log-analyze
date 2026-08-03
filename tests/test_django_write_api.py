from __future__ import annotations

import json
import os
from pathlib import Path

import django
from django.test import Client, override_settings


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_test_project.settings")
django.setup()


def _result() -> dict[str, object]:
    return {
        "summary": {"total_raw_logs": 1},
        "risk_entities": [{
            "entity_id": "node-1",
            "entity_type": "node",
            "cluster": "prod-a",
            "risk_score": 80,
            "risk_level": "high",
            "window_start": "2026-08-03T00:00:00+00:00",
            "window_end": "2026-08-03T00:05:00+00:00",
            "top_templates": [{"template_hash": "hash-1", "component": "kernel", "template": "OOM", "count": 1}],
            "affected_entities": [],
        }],
    }


def _candidate(source: dict[str, object], **_kwargs: object) -> list[dict[str, object]]:
    return [{
        "candidate_id": "candidate-node-1",
        "status": "pending",
        "reviewer_note": "",
        "approved_at": None,
        "cluster": source["cluster"],
        "entity": {"type": source["entity_type"], "id": source["entity_id"]},
        "window_start": source["window_start"],
        "window_end": source["window_end"],
        "risk_score": source["risk_score"],
        "risk_level": source["risk_level"],
        "feature_type": "memory_pressure",
        "title": "内存压力日志",
        "summary": "检测到内存压力日志。",
        "importance": "high",
        "template_hashes": ["hash-1"],
        "components": ["kernel"],
        "tags": ["内存", "压力"],
        "selection_reason": "kernel 组件包含内存压力模式。",
        "occurrence_count": 1,
        "time_range": {"first_seen": source["window_start"], "last_seen": source["window_end"]},
        "affected_entities": [],
        "source_templates": [{"template_hash": "hash-1", "component": "kernel", "template": "OOM", "count": 1}],
        "provider": "ollama",
        "model": "test-model",
    }]


def _config(tmp_path: Path, resolver: str) -> dict[str, object]:
    return {
        "project_root": str(Path(__file__).resolve().parents[1]),
        "state_root": str(tmp_path / "state"),
        "output_root": str(tmp_path / "output"),
        "database_provider": "sqlite",
        "shared_root": str(tmp_path / "shared"),
        "airflow_base_url": "http://127.0.0.1:18080",
        "airflow_dag_id": "logrisk_analysis",
        "identity_resolver": resolver,
        "write_roles": ["logrisk:operator"],
    }


def _completed_job() -> tuple[object, str]:
    from logrisk_django.service_factory import get_container

    container = get_container()
    container.feature_jobs.extractor = _candidate
    job_id = container.feature_jobs.create_job(_result(), model="test-model")
    container.feature_jobs.run_job(job_id)
    return container, job_id


def test_django_feature_approval_and_export_share_governed_services(tmp_path) -> None:
    from logrisk_django.service_factory import clear_cached_container

    with override_settings(LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.OperatorIdentityResolver")):
        clear_cached_container()
        container, job_id = _completed_job()
        approved = Client().patch(
            f"/api/jobs/{job_id}/features/candidate-node-1",
            data=json.dumps({"status": "approved", "reviewer_note": "已核对"}),
            content_type="application/json",
        )
        exported = Client().post(f"/api/jobs/{job_id}/export")
        audits = container.runtime_repository.list_audits(limit=20)["items"]
        clear_cached_container()

    assert approved.status_code == 200
    assert approved.json()["status"] == "approved"
    assert exported.status_code == 200
    assert exported.json()["review_statistics"]["approved"] == 1
    assert exported["Content-Disposition"] == 'attachment; filename="logrisk-feature-package.json"'
    assert {item["action"] for item in audits} >= {"feature.approved", "feature.exported"}
    assert all("reviewer_note" not in item["attributes"] for item in audits)


def test_django_governed_write_fails_closed_and_audits_denial(tmp_path) -> None:
    from logrisk_django.service_factory import clear_cached_container, get_container

    with override_settings(LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.AnonymousIdentityResolver")):
        clear_cached_container()
        response = Client().post("/api/release-readiness/validate", data="{}", content_type="application/json")
        audits = get_container().runtime_repository.list_audits(limit=20)["items"]
        clear_cached_container()

    assert response.status_code == 403
    assert response.json()["code"] == "runtime_identity_required"
    assert "Cookie" not in json.dumps(response.json())
    assert any(item["action"] == "access.denied" and item["outcome"] == "denied" for item in audits)
