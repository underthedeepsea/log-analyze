from __future__ import annotations

import json

import pytest

from logrisk.database import SQLiteDatabase, utc_now


def _input_job(database: SQLiteDatabase, input_job_id: str = "input_job_1") -> str:
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


def test_input_orchestration_run_is_idempotent_and_uses_optimistic_transition(tmp_path) -> None:
    """The Django upload hand-off must persist only an input job ID before Airflow runs."""
    from logrisk.orchestration.input_repository import (
        InputOrchestrationConflict,
        InputOrchestrationRepository,
    )

    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    repository = InputOrchestrationRepository(database)
    input_job_id = _input_job(database)

    first = repository.create_pending(
        input_job_id=input_job_id,
        request_id="request-1",
        actor="pacas-alice",
        roles=["logrisk:operator"],
    )
    duplicate = repository.create_pending(
        input_job_id=input_job_id,
        request_id="request-1",
        actor="pacas-alice",
        roles=["logrisk:operator"],
    )
    dispatched = repository.transition(
        first["input_orchestration_run_id"],
        from_status="pending_dispatch",
        to_status="dispatched",
        expected_version=1,
        external_dag_id="logrisk_input_preprocess",
        external_run_id="logrisk_input__input_job_1",
    )

    assert duplicate["input_orchestration_run_id"] == first["input_orchestration_run_id"]
    assert dispatched["input_job_id"] == input_job_id
    assert dispatched["status"] == "dispatched"
    assert dispatched["state_version"] == 2
    with pytest.raises(InputOrchestrationConflict):
        repository.transition(
            first["input_orchestration_run_id"],
            from_status="pending_dispatch",
            to_status="dispatched",
            expected_version=1,
        )
