from __future__ import annotations

from typing import Any, Sequence

from logrisk.database import utc_now
from logrisk.orchestration.repository import OrchestrationRepository


_ACTIVE_AIRFLOW_STATES = {"queued", "scheduled", "up_for_retry", "up_for_reschedule", "deferred"}
_FAILED_AIRFLOW_STATES = {"failed", "upstream_failed", "removed"}
_CANCELLED_AIRFLOW_STATES = {"canceled", "cancelled"}


class OrchestrationService:
    """Domain operations for dispatch, recovery, cancellation, and completion."""

    def __init__(self, repository: OrchestrationRepository) -> None:
        self.repository = repository

    def create_pending(self, job_id: str, request_id: str, actor: str, roles: Sequence[str] = ()) -> dict[str, Any]:
        return self.repository.create_pending(
            job_id=job_id,
            orchestrator="airflow",
            request_id=request_id,
            actor=actor,
            roles=roles,
        )

    def mark_dispatched(
        self,
        orchestration_run_id: str,
        dag_id: str,
        dag_run_id: str,
        *,
        expected_version: int,
    ) -> dict[str, Any]:
        return self.repository.transition(
            orchestration_run_id,
            from_status="pending_dispatch",
            to_status="dispatched",
            expected_version=expected_version,
            external_dag_id=dag_id,
            external_run_id=dag_run_id,
            attempt=1,
        )

    def mark_running(self, orchestration_run_id: str, *, expected_version: int) -> dict[str, Any]:
        now = utc_now()
        return self.repository.transition(
            orchestration_run_id,
            from_status="dispatched",
            to_status="running",
            expected_version=expected_version,
            started_at=now,
            last_heartbeat_at=now,
        )

    def heartbeat(self, orchestration_run_id: str, *, expected_version: int) -> dict[str, Any]:
        return self.repository.transition(
            orchestration_run_id,
            from_status="running",
            to_status="running",
            expected_version=expected_version,
            last_heartbeat_at=utc_now(),
        )

    def request_cancel(self, orchestration_run_id: str, *, expected_version: int) -> dict[str, Any]:
        current = self.repository.get(orchestration_run_id)
        return self.repository.transition(
            orchestration_run_id,
            from_status=current["status"],
            to_status="cancel_requested",
            expected_version=expected_version,
        )

    def mark_finished(
        self,
        orchestration_run_id: str,
        status: str,
        *,
        expected_version: int,
        error_code: str | None = None,
        error_summary: str | None = None,
    ) -> dict[str, Any]:
        if status not in {"completed", "failed", "cancelled"}:
            raise ValueError("最终状态必须是 completed、failed 或 cancelled")
        current = self.repository.get(orchestration_run_id)
        return self.repository.transition(
            orchestration_run_id,
            from_status=current["status"],
            to_status=status,
            expected_version=expected_version,
            completed_at=utc_now(),
            error_code=error_code,
            error_summary=error_summary,
        )

    def mark_dispatch_failed(
        self,
        orchestration_run_id: str,
        *,
        expected_version: int,
        error_code: str,
        error_summary: str,
    ) -> dict[str, Any]:
        return self.repository.transition(
            orchestration_run_id,
            from_status="pending_dispatch",
            to_status="dispatch_failed",
            expected_version=expected_version,
            error_code=error_code,
            error_summary=error_summary,
        )

    def retry_dispatch(self, orchestration_run_id: str, *, expected_version: int) -> dict[str, Any]:
        return self.repository.transition(
            orchestration_run_id,
            from_status="dispatch_failed",
            to_status="pending_dispatch",
            expected_version=expected_version,
        )

    def reconcile_external(
        self,
        orchestration_run_id: str,
        external_state: str,
        *,
        expected_version: int,
    ) -> dict[str, Any]:
        """Apply a verified Airflow state without accepting task payloads.

        Airflow is queried by the adapter, but only its small lifecycle state is
        allowed to change the durable LOGRISK orchestration record.  Unknown
        states are rejected instead of being guessed as success or failure.
        """
        state = str(external_state or "").strip().casefold()
        current = self.repository.get(orchestration_run_id)
        if current["status"] in {"completed", "failed", "cancelled"}:
            return current
        if state in _ACTIVE_AIRFLOW_STATES:
            return current
        if state == "running":
            if current["status"] == "dispatched":
                return self.mark_running(orchestration_run_id, expected_version=expected_version)
            if current["status"] == "running":
                return self.heartbeat(orchestration_run_id, expected_version=expected_version)
            return current
        if state == "success":
            target = "cancelled" if current["status"] == "cancel_requested" else "completed"
            return self.mark_finished(orchestration_run_id, target, expected_version=expected_version)
        if state in _CANCELLED_AIRFLOW_STATES:
            return self.mark_finished(orchestration_run_id, "cancelled", expected_version=expected_version)
        if state in _FAILED_AIRFLOW_STATES:
            return self.mark_finished(
                orchestration_run_id,
                "failed",
                expected_version=expected_version,
                error_code="airflow_run_failed",
                error_summary="Airflow DAG Run 执行失败",
            )
        raise ValueError("不支持的 Airflow DAG Run 状态")

    def list_reconcilable(self, *, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_reconcilable(limit=limit)

    def list_active(self, *, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_active(limit=limit)

    def for_job(self, job_id: str) -> dict[str, Any] | None:
        return self.repository.for_job(job_id)

    def get(self, orchestration_run_id: str) -> dict[str, Any]:
        return self.repository.get(orchestration_run_id)
