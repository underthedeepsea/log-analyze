from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from logrisk.application import ApplicationConfig, ApplicationContainer, build_application_container


def build_worker_container() -> ApplicationContainer:
    """Build runtime services only when an Airflow task starts, never at DAG parse time."""
    root = Path(os.environ.get("LOGRISK_PROJECT_ROOT") or ".").resolve()
    state_root = Path(os.environ.get("LOGRISK_STATE_ROOT") or root / "state").resolve()
    output_root = Path(os.environ.get("LOGRISK_OUTPUT_ROOT") or root / "output").resolve()
    provider = os.environ.get("LOGRISK_DATABASE_PROVIDER") or "postgres"
    return build_application_container(ApplicationConfig(
        project_root=root,
        state_root=state_root,
        output_root=output_root,
        database_provider=provider,
        database_url=os.environ.get("LOGRISK_DATABASE_URL") if provider == "postgres" else None,
        database_path=state_root / "logrisk.sqlite3" if provider == "sqlite" else None,
        shared_root=Path(os.environ.get("LOGRISK_SHARED_ROOT") or state_root),
        import_legacy_state=False,
        interrupt_streaming_tasks=False,
        feature_jobs_auto_start=False,
        migrate_database=False,
    ))


def prepare_job(
    job_id: str,
    orchestration_run_id: str,
    request_id: str,
    *,
    container: ApplicationContainer | None = None,
) -> dict[str, str]:
    services = container or build_worker_container()
    run = services.orchestration.get(orchestration_run_id)
    if run["job_id"] != str(job_id) or run["request_id"] != str(request_id):
        raise ValueError("编排运行与任务标识不匹配")
    if run["status"] == "dispatched":
        run = services.orchestration.mark_running(orchestration_run_id, expected_version=run["state_version"])
    if run["status"] != "running":
        raise ValueError("编排运行不处于可执行状态")
    services.feature_jobs.get_job(job_id)
    return {"job_id": str(job_id), "orchestration_run_id": str(orchestration_run_id), "status": "running"}


def preprocess_input(job_id: str, orchestration_run_id: str, *, container: ApplicationContainer | None = None) -> dict[str, str]:
    _ensure_running(job_id, orchestration_run_id, container)
    return {"job_id": str(job_id), "orchestration_run_id": str(orchestration_run_id), "status": "prepared"}


def list_drain_partitions(job_id: str, orchestration_run_id: str, *, container: ApplicationContainer | None = None) -> dict[str, Any]:
    _ensure_running(job_id, orchestration_run_id, container)
    return {"job_id": str(job_id), "orchestration_run_id": str(orchestration_run_id), "partition_ids": []}


def drain_partition(job_id: str, orchestration_run_id: str, partition_id: str, *, container: ApplicationContainer | None = None) -> dict[str, str]:
    _ensure_running(job_id, orchestration_run_id, container)
    return {
        "job_id": str(job_id), "orchestration_run_id": str(orchestration_run_id),
        "partition_id": str(partition_id), "status": "completed",
    }


def merge_templates(job_id: str, orchestration_run_id: str, *, container: ApplicationContainer | None = None) -> dict[str, str]:
    _ensure_running(job_id, orchestration_run_id, container)
    return {"job_id": str(job_id), "orchestration_run_id": str(orchestration_run_id), "status": "merged"}


def score_and_reuse(job_id: str, orchestration_run_id: str, *, container: ApplicationContainer | None = None) -> dict[str, str]:
    _ensure_running(job_id, orchestration_run_id, container)
    return {"job_id": str(job_id), "orchestration_run_id": str(orchestration_run_id), "status": "scored"}


def list_feature_batches(job_id: str, orchestration_run_id: str, *, container: ApplicationContainer | None = None) -> dict[str, Any]:
    services = _ensure_running(job_id, orchestration_run_id, container)
    job = services.feature_jobs.get_job(job_id)
    return {
        "job_id": str(job_id), "orchestration_run_id": str(orchestration_run_id),
        "batch_ids": ["all"] if job.get("entities") else [],
    }


def extract_feature_batch(
    job_id: str,
    orchestration_run_id: str,
    batch_id: str = "all",
    *,
    container: ApplicationContainer | None = None,
) -> dict[str, Any]:
    services = _ensure_running(job_id, orchestration_run_id, container)
    if str(batch_id) != "all":
        raise ValueError("当前仅支持稳定的全量特征批次")
    services.feature_jobs.run_job(job_id)
    job = services.feature_jobs.get_job(job_id)
    return {
        "job_id": str(job_id), "orchestration_run_id": str(orchestration_run_id),
        "batch_id": "all", "status": str(job.get("status") or "unknown"),
        "candidate_count": len(job.get("features") or []),
    }


def validate_candidates(job_id: str, orchestration_run_id: str, *, container: ApplicationContainer | None = None) -> dict[str, Any]:
    services = _ensure_running(job_id, orchestration_run_id, container)
    job = services.feature_jobs.get_job(job_id)
    return {
        "job_id": str(job_id), "orchestration_run_id": str(orchestration_run_id),
        "status": str(job.get("status") or "unknown"), "candidate_count": len(job.get("features") or []),
    }


def finalize_job(job_id: str, orchestration_run_id: str, *, container: ApplicationContainer | None = None) -> dict[str, str]:
    services = container or build_worker_container()
    run = services.orchestration.get(orchestration_run_id)
    if run["job_id"] != str(job_id):
        raise ValueError("编排运行与任务标识不匹配")
    if run["status"] in {"completed", "failed", "cancelled"}:
        return {"job_id": str(job_id), "orchestration_run_id": str(orchestration_run_id), "status": str(run["status"])}
    job = services.feature_jobs.get_job(job_id)
    target = "cancelled" if run["status"] == "cancel_requested" else (
        "failed" if job.get("status") in {"failed", "completed_with_errors"} else "completed"
    )
    finished = services.orchestration.mark_finished(
        orchestration_run_id,
        target,
        expected_version=run["state_version"],
        error_code="feature_job_failed" if target == "failed" else None,
        error_summary="特征识别任务失败" if target == "failed" else None,
    )
    return {"job_id": str(job_id), "orchestration_run_id": str(orchestration_run_id), "status": str(finished["status"])}


def _ensure_running(job_id: str, orchestration_run_id: str, container: ApplicationContainer | None) -> ApplicationContainer:
    services = container or build_worker_container()
    run = services.orchestration.get(orchestration_run_id)
    if run["job_id"] != str(job_id) or run["status"] != "running":
        raise ValueError("编排运行不处于执行状态")
    return services
