from __future__ import annotations

import json
import os
from pathlib import Path

import django
from django.core.management import call_command
from django.test import Client, override_settings


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_test_project.settings")
django.setup()


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


def _rule() -> dict[str, object]:
    return {
        "id": "user.kernel.oom",
        "display_name": "内核内存压力",
        "description": "识别内核内存压力日志",
        "domain": "compute",
        "category": "memory",
        "risk_type": "node.memory_pressure",
        "match": {
            "source_types": ["kernel"],
            "components": ["kernel"],
            "message_regex": [r"(?i)oom|out of memory"],
        },
        "classification": {"default_severity": "high", "base_score": 80, "confidence": 0.95},
        "dedup": {"key_fields": ["cluster", "node_id", "risk_type"], "window_seconds": 300},
        "test_samples": {"positive": ["kernel: out of memory"], "negative": ["kernel: normal operation"]},
        "tags": ["内核", "内存"],
    }


def test_django_semantic_lifecycle_uses_identity_and_audit(tmp_path) -> None:
    from logrisk_django.service_factory import clear_cached_container, get_container

    with override_settings(LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.OperatorIdentityResolver")):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        created = Client().post("/api/semantics", data=json.dumps(_rule()), content_type="application/json")
        published = Client().post(
            "/api/semantics/user.kernel.oom/publish",
            data=json.dumps({"expected_version": 1, "confirmed": True, "reason": "完成审核"}),
            content_type="application/json",
        )
        effective = Client().get("/api/semantics/effective")
        audits = get_container().runtime_repository.list_audits(limit=30)["items"]
        clear_cached_container()

    assert created.status_code == 201
    assert created.json()["status"] == "draft"
    assert published.status_code == 200
    assert published.json()["status"] == "published"
    assert effective.status_code == 200
    assert any(item["id"] == "user.kernel.oom" for item in effective.json()["items"])
    assert {item["action"] for item in audits} >= {"semantic.created", "semantic.published"}
    assert all("完成审核" not in json.dumps(item, ensure_ascii=False) for item in audits)


def test_django_semantic_writes_fail_closed_without_identity(tmp_path) -> None:
    from logrisk_django.service_factory import clear_cached_container

    with override_settings(LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.AnonymousIdentityResolver")):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        response = Client().post("/api/semantics", data=json.dumps(_rule()), content_type="application/json")
        clear_cached_container()

    assert response.status_code == 403
    assert response.json()["code"] == "runtime_identity_required"
