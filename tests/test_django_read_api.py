from __future__ import annotations

import os
from pathlib import Path

import django
from django.test import Client, override_settings
from django.core.management import call_command


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_test_project.settings")
django.setup()


def test_django_core_read_apis_use_the_shared_api_facade(tmp_path) -> None:
    from logrisk_django.service_factory import clear_cached_container

    root = Path(__file__).resolve().parents[1]
    config = {
        "project_root": str(root),
        "state_root": str(tmp_path / "state"),
        "output_root": str(tmp_path / "output"),
        "database_provider": "sqlite",
        "shared_root": str(tmp_path / "shared"),
        "airflow_base_url": "http://127.0.0.1:18080",
        "airflow_dag_id": "logrisk_analysis",
        "write_roles": ["logrisk:operator"],
    }
    with override_settings(LOGRISK=config):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        client = Client()
        responses = [client.get(path) for path in (
            "/api/health",
            "/api/runtime/readiness",
            "/api/ai-harness/model-profiles",
            "/api/ai-harness/prompts",
            "/api/rule-governance/rules",
            "/api/release-readiness",
        )]
        approvals = client.get("/api/feature-approvals?status=pending&page_size=100")
        clear_cached_container()

    assert all(response.status_code in {200, 503} for response in responses)
    assert approvals.status_code == 200
    assert approvals.json()["schema_version"] == "feature_approval_queue_v1"
    assert responses[0].json()["service"] == "logrisk-dashboard"
    assert "profiles" in responses[2].json()
    assert "items" in responses[3].json()
