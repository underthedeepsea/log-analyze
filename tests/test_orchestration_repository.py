from __future__ import annotations

import json

import pytest

from logrisk.database import SQLiteDatabase, utc_now


def _feature_job(database: SQLiteDatabase, job_id: str = "job-1") -> str:
    now = utc_now()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO feature_jobs(job_id, status, job_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (job_id, "queued", json.dumps({"job_id": job_id}), now, now),
        )
    return job_id


def test_orchestration_run_is_idempotent_and_rejects_stale_transition(tmp_path) -> None:
    """Removing the unique request contract or optimistic predicate must fail this test."""
    from logrisk.orchestration.repository import OrchestrationConflict, OrchestrationRepository

    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    repository = OrchestrationRepository(database)
    job_id = _feature_job(database)

    first = repository.create_pending(
        job_id=job_id,
        orchestrator="airflow",
        request_id="request-1",
        actor="alice",
        roles=["logrisk:operator"],
    )
    duplicate = repository.create_pending(
        job_id=job_id,
        orchestrator="airflow",
        request_id="request-1",
        actor="alice",
        roles=["logrisk:operator"],
    )
    dispatched = repository.transition(
        first["orchestration_run_id"],
        from_status="pending_dispatch",
        to_status="dispatched",
        expected_version=1,
        external_dag_id="logrisk_analysis",
        external_run_id="logrisk__job-1",
    )

    assert duplicate["orchestration_run_id"] == first["orchestration_run_id"]
    assert dispatched["status"] == "dispatched"
    assert dispatched["state_version"] == 2
    with pytest.raises(OrchestrationConflict):
        repository.transition(
            first["orchestration_run_id"],
            from_status="pending_dispatch",
            to_status="dispatched",
            expected_version=1,
        )
