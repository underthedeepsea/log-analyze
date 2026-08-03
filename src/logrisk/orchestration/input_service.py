from __future__ import annotations

from typing import Any, Sequence

from logrisk.database import utc_now
from logrisk.orchestration.input_repository import InputOrchestrationRepository


class InputOrchestrationService:
    """Lifecycle operations for Airflow-owned uploaded-log preprocessing."""

    def __init__(self, repository: InputOrchestrationRepository) -> None:
        self.repository = repository

    def create_pending(self, input_job_id: str, request_id: str, actor: str, roles: Sequence[str] = ()) -> dict[str, Any]:
        return self.repository.create_pending(
            input_job_id=input_job_id,
            request_id=request_id,
            actor=actor,
            roles=roles,
        )

    def mark_dispatched(
        self,
        input_orchestration_run_id: str,
        dag_id: str,
        dag_run_id: str,
        *,
        expected_version: int,
    ) -> dict[str, Any]:
        return self.repository.transition(
            input_orchestration_run_id,
            from_status="pending_dispatch",
            to_status="dispatched",
            expected_version=expected_version,
            external_dag_id=dag_id,
            external_run_id=dag_run_id,
            attempt=1,
        )

    def mark_running(self, input_orchestration_run_id: str, *, expected_version: int) -> dict[str, Any]:
        now = utc_now()
        return self.repository.transition(
            input_orchestration_run_id,
            from_status="dispatched",
            to_status="running",
            expected_version=expected_version,
            started_at=now,
            last_heartbeat_at=now,
        )

    def heartbeat(self, input_orchestration_run_id: str, *, expected_version: int) -> dict[str, Any]:
        return self.repository.transition(
            input_orchestration_run_id,
            from_status="running",
            to_status="running",
            expected_version=expected_version,
            last_heartbeat_at=utc_now(),
        )

    def mark_finished(
        self,
        input_orchestration_run_id: str,
        status: str,
        *,
        expected_version: int,
        error_code: str | None = None,
        error_summary: str | None = None,
    ) -> dict[str, Any]:
        if status not in {"completed", "failed", "cancelled"}:
            raise ValueError("最终状态必须是 completed、failed 或 cancelled")
        current = self.repository.get(input_orchestration_run_id)
        return self.repository.transition(
            input_orchestration_run_id,
            from_status=current["status"],
            to_status=status,
            expected_version=expected_version,
            completed_at=utc_now(),
            error_code=error_code,
            error_summary=error_summary,
        )

    def mark_dispatch_failed(
        self,
        input_orchestration_run_id: str,
        *,
        expected_version: int,
        error_code: str,
        error_summary: str,
    ) -> dict[str, Any]:
        return self.repository.transition(
            input_orchestration_run_id,
            from_status="pending_dispatch",
            to_status="dispatch_failed",
            expected_version=expected_version,
            error_code=error_code,
            error_summary=error_summary,
        )

    def retry_dispatch(self, input_orchestration_run_id: str, *, expected_version: int) -> dict[str, Any]:
        return self.repository.transition(
            input_orchestration_run_id,
            from_status="dispatch_failed",
            to_status="pending_dispatch",
            expected_version=expected_version,
        )

    def for_input_job(self, input_job_id: str) -> dict[str, Any] | None:
        return self.repository.for_input_job(input_job_id)

    def list_reconcilable(self, *, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_reconcilable(limit=limit)

    def get(self, input_orchestration_run_id: str) -> dict[str, Any]:
        return self.repository.get(input_orchestration_run_id)
