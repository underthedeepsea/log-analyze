from __future__ import annotations

import json

from logrisk.database import SQLiteDatabase, utc_now


def _input_job(database: SQLiteDatabase) -> str:
    input_job_id = "input_job_service"
    now = utc_now()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO input_jobs(input_job_id, upload_id, status, stage, job_json, progress_json, created_at, updated_at) "
            "VALUES (?, NULL, 'queued', 'queued', ?, ?, ?, ?)",
            (
                input_job_id,
                json.dumps({"input_job_id": input_job_id, "status": "queued"}),
                json.dumps({"input_job_id": input_job_id, "status": "queued", "progress": 0.0}),
                now,
                now,
            ),
        )
    return input_job_id


def test_input_orchestration_service_marks_the_airflow_lifecycle(tmp_path) -> None:
    from logrisk.orchestration.input_repository import InputOrchestrationRepository
    from logrisk.orchestration.input_service import InputOrchestrationService

    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    service = InputOrchestrationService(InputOrchestrationRepository(database))
    created = service.create_pending(_input_job(database), "request-2", "pacas-alice", ["logrisk:operator"])
    dispatched = service.mark_dispatched(
        created["input_orchestration_run_id"],
        "logrisk_input_preprocess",
        "logrisk_input__input_job_service",
        expected_version=created["state_version"],
    )
    running = service.mark_running(dispatched["input_orchestration_run_id"], expected_version=dispatched["state_version"])
    completed = service.mark_finished(running["input_orchestration_run_id"], "completed", expected_version=running["state_version"])

    assert completed["status"] == "completed"
    assert completed["attempt"] == 1
    assert completed["started_at"]
    assert completed["completed_at"]


def test_input_orchestration_service_lists_only_runs_needing_dispatch(tmp_path) -> None:
    from logrisk.orchestration.input_repository import InputOrchestrationRepository
    from logrisk.orchestration.input_service import InputOrchestrationService

    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    service = InputOrchestrationService(InputOrchestrationRepository(database))
    pending = service.create_pending(_input_job(database), "request-pending", "pacas-alice")

    assert service.list_reconcilable() == [pending]
