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


def _dataset() -> dict[str, object]:
    return {
        "dataset_id": "dataset-django-test",
        "name": "Django Drain3 Dataset",
        "records": [{
            "schema_version": "drain_gold_v1",
            "record_id": "record-1",
            "source_type": "kernel",
            "component": "kernel",
            "message_core": "oom killed process",
            "gold_group_id": "group-1",
            "gold_template": "oom killed process",
            "semantic_fields": {},
            "protected_tokens": [],
            "expected_risk_type": "node.memory_pressure",
            "annotation_status": "draft",
        }],
    }


def test_django_drain_dataset_write_is_governed_and_persisted(tmp_path) -> None:
    from logrisk_django.service_factory import clear_cached_container, get_container

    with override_settings(LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.OperatorIdentityResolver")):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        response = Client().post("/api/drain-quality/datasets", data=json.dumps(_dataset()), content_type="application/json")
        listing = Client().get("/api/drain-quality/datasets")
        audits = get_container().runtime_repository.list_audits(limit=20)["items"]
        clear_cached_container()

    assert response.status_code == 201
    assert response.json()["dataset_id"] == "dataset-django-test"
    assert listing.status_code == 200
    assert any(item["dataset_id"] == "dataset-django-test" for item in listing.json()["items"])
    assert any(item["action"] == "drain.dataset_created" for item in audits)


def test_django_drain_writes_fail_closed_without_identity(tmp_path) -> None:
    from logrisk_django.service_factory import clear_cached_container

    with override_settings(LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.AnonymousIdentityResolver")):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        response = Client().post("/api/drain-quality/datasets", data=json.dumps(_dataset()), content_type="application/json")
        clear_cached_container()

    assert response.status_code == 403
    assert response.json()["code"] == "runtime_identity_required"


def test_django_semantic_dictionary_reads_and_tests_use_shared_service(tmp_path) -> None:
    from logrisk_django.service_factory import clear_cached_container

    with override_settings(LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.OperatorIdentityResolver")):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        dictionaries = Client().get("/api/semantic/dictionaries")
        tested = Client().post(
            "/api/semantic/test",
            data=json.dumps({"message_core": "HTTP status 503", "source_type": "access", "component": "nginx"}),
            content_type="application/json",
        )
        clear_cached_container()

    assert dictionaries.status_code == 200
    assert dictionaries.json()["items"]
    assert tested.status_code == 200
    assert tested.json()["semantic_fields"]["http_status"] == 503
