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


def test_django_benchmark_suite_write_is_governed_and_readable(tmp_path) -> None:
    from logrisk_django.service_factory import clear_cached_container, get_container

    with override_settings(LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.OperatorIdentityResolver")):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        response = Client().post(
            "/api/benchmark-center/suites",
            data=json.dumps({
                "suite_id": "suite-django-test",
                "name": "Django 测试 Suite",
                "source_type": "custom",
                "cases": [{"case_id": "case-1", "evidence": {"templates": [{"template_hash": "h1"}]}}],
            }),
            content_type="application/json",
        )
        listing = Client().get("/api/benchmark-center/suites")
        audits = get_container().runtime_repository.list_audits(limit=30)["items"]
        clear_cached_container()

    assert response.status_code == 201
    assert response.json()["suite_id"] == "suite-django-test"
    assert listing.status_code == 200
    assert any(item["suite_id"] == "suite-django-test" for item in listing.json()["items"])
    assert any(item["action"] == "benchmark.suite_created" for item in audits)


def test_django_benchmark_writes_fail_closed_without_identity(tmp_path) -> None:
    from logrisk_django.service_factory import clear_cached_container

    with override_settings(LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.AnonymousIdentityResolver")):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        response = Client().post(
            "/api/benchmark-center/suites",
            data=json.dumps({"name": "blocked", "source_type": "custom", "cases": []}),
            content_type="application/json",
        )
        clear_cached_container()

    assert response.status_code == 403
    assert response.json()["code"] == "runtime_identity_required"
