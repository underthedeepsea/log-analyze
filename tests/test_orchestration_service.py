from __future__ import annotations

import json

from logrisk.database import SQLiteDatabase, utc_now


def _feature_job(database: SQLiteDatabase, job_id: str) -> None:
    now = utc_now()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO feature_jobs(job_id, status, job_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (job_id, "queued", json.dumps({"job_id": job_id}), now, now),
        )


def test_orchestration_service_lists_only_runs_needing_dispatch_reconciliation(tmp_path) -> None:
    """Completed runs must never be resent to Airflow during reconciliation."""
    from logrisk.orchestration.repository import OrchestrationRepository
    from logrisk.orchestration.service import OrchestrationService

    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    repository = OrchestrationRepository(database)
    service = OrchestrationService(repository)
    _feature_job(database, "job-pending")
    _feature_job(database, "job-completed")

    pending = service.create_pending("job-pending", "request-pending", "alice", ["logrisk:operator"])
    completed = service.create_pending("job-completed", "request-completed", "alice", ["logrisk:operator"])
    service.mark_dispatched(completed["orchestration_run_id"], "logrisk_analysis", "logrisk__job-completed", expected_version=1)
    service.mark_finished(completed["orchestration_run_id"], "completed", expected_version=2)

    reconcilable = service.list_reconcilable(limit=10)

    assert [item["orchestration_run_id"] for item in reconcilable] == [pending["orchestration_run_id"]]


def test_orchestration_service_heartbeat_advances_the_optimistic_version(tmp_path) -> None:
    """A worker heartbeat must not be rejected as an invalid same-state transition."""
    from logrisk.orchestration.repository import OrchestrationRepository
    from logrisk.orchestration.service import OrchestrationService

    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    service = OrchestrationService(OrchestrationRepository(database))
    _feature_job(database, "job-running")
    created = service.create_pending("job-running", "request-running", "alice", ["logrisk:operator"])
    dispatched = service.mark_dispatched(created["orchestration_run_id"], "logrisk_analysis", "logrisk__job-running", expected_version=1)
    running = service.mark_running(dispatched["orchestration_run_id"], expected_version=2)

    heartbeat = service.heartbeat(running["orchestration_run_id"], expected_version=3)

    assert heartbeat["status"] == "running"
    assert heartbeat["state_version"] == 4
    assert heartbeat["last_heartbeat_at"]


def test_orchestration_service_redacts_sensitive_dispatch_error_details(tmp_path) -> None:
    """External scheduler errors must not persist credentials in orchestration state."""
    from logrisk.orchestration.repository import OrchestrationRepository
    from logrisk.orchestration.service import OrchestrationService

    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    service = OrchestrationService(OrchestrationRepository(database))
    _feature_job(database, "job-sensitive-error")
    created = service.create_pending("job-sensitive-error", "request-sensitive", "alice")

    failed = service.mark_dispatch_failed(
        created["orchestration_run_id"],
        expected_version=1,
        error_code="authorization token leaked",
        error_summary="Airflow returned Authorization: Bearer scheduler-secret",
    )

    assert "scheduler-secret" not in str(failed)
    assert failed["error_code"] == "orchestration_error"
    assert failed["error_summary"] == "编排器返回了受保护的错误详情"
