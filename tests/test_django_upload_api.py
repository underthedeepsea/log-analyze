from __future__ import annotations

import hashlib
import os
from pathlib import Path

import django
from django.core.management import call_command
from django.test import Client, override_settings


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_test_project.settings")
django.setup()


def _config(tmp_path: Path) -> dict[str, object]:
    return {
        "project_root": str(Path(__file__).resolve().parents[1]),
        "state_root": str(tmp_path / "state"),
        "output_root": str(tmp_path / "output"),
        "database_provider": "sqlite",
        "shared_root": str(tmp_path / "shared"),
        "airflow_base_url": "http://127.0.0.1:18080",
        "airflow_dag_id": "logrisk_analysis",
        "airflow_input_dag_id": "logrisk_input_preprocess",
        "identity_resolver": "tests.django_test_project.resolver.OperatorIdentityResolver",
        "write_roles": ["logrisk:operator"],
    }


def test_django_uploaded_log_is_dispatched_to_airflow_with_stable_ids_only(tmp_path, monkeypatch) -> None:
    from logrisk.orchestration.airflow import AirflowRun
    from logrisk_django.service_factory import clear_cached_container, get_container
    from logrisk_django.views import uploads

    content = b"Aug 03 00:00:00 host kernel: sanitized event\n"
    digest = hashlib.sha256(content).hexdigest()
    calls: list[tuple[str, str, str]] = []

    class ReadyInputAirflow:
        dag_id = "logrisk_input_preprocess"

        def trigger_input(self, input_job_id: str, input_orchestration_run_id: str, request_id: str) -> AirflowRun:
            calls.append((input_job_id, input_orchestration_run_id, request_id))
            return AirflowRun(
                "logrisk_input__" + input_job_id,
                "queued",
                None,
                None,
                request_id,
                input_job_id,
                input_orchestration_run_id,
            )

    monkeypatch.setattr(uploads, "get_input_airflow_orchestrator", lambda: ReadyInputAirflow())
    with override_settings(LOGRISK=_config(tmp_path)):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        client = Client()
        created = client.post(
            "/api/uploads",
            data={"filename": "messages", "size_bytes": len(content), "chunk_size_bytes": len(content)},
            content_type="application/json",
        )
        upload_id = created.json()["upload_id"]
        appended = client.put(
            f"/api/uploads/{upload_id}/chunks/0",
            data=content,
            content_type="application/octet-stream",
            HTTP_X_CHUNK_SHA256=digest,
        )
        completed = client.post(
            f"/api/uploads/{upload_id}/complete",
            data={"sha256": digest},
            content_type="application/json",
        )
        dispatched = client.post(
            "/api/inputs/analyze-upload",
            data={"upload_id": upload_id, "filename": "messages"},
            content_type="application/json",
        )
        input_job_id = dispatched.json()["input_job_id"]
        run = get_container().input_orchestration.for_input_job(input_job_id)
        clear_cached_container()

    assert created.status_code == 201
    assert appended.status_code == 200
    assert completed.status_code == 200
    assert dispatched.status_code == 202
    assert calls == [(input_job_id, run["input_orchestration_run_id"], "request-django-test")]
    assert run and run["status"] == "dispatched"
    assert content.decode("utf-8") not in str(calls)
