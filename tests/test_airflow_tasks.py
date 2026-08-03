from __future__ import annotations

from dataclasses import replace
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _document() -> dict[str, object]:
    return {
        "summary": {"total_raw_logs": 1},
        "risk_entities": [{
            "entity_id": "node-1",
            "entity_type": "node",
            "cluster": "prod-a",
            "risk_score": 90,
            "risk_level": "high",
            "window_start": "2026-08-03T00:00:00+00:00",
            "window_end": "2026-08-03T00:05:00+00:00",
            "top_templates": [{"template_hash": "hash-1", "count": 1, "template": "sanitized error"}],
            "affected_entities": [],
        }],
    }


def test_prepare_job_is_idempotent_and_returns_only_safe_ids(tmp_path) -> None:
    from logrisk.application import ApplicationConfig, build_application_container
    from logrisk.airflow_tasks import prepare_job

    config = replace(
        ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / "state"),
        feature_jobs_auto_start=False,
    )
    container = build_application_container(config)
    job_id = container.feature_jobs.create_job(_document(), model="qwen3:1.7b")
    run = container.orchestration.create_pending(job_id, "request-1", "pacas-alice", ["logrisk:operator"])
    dispatched = container.orchestration.mark_dispatched(
        run["orchestration_run_id"], "logrisk_analysis", "logrisk__" + job_id, expected_version=run["state_version"]
    )

    first = prepare_job(job_id, dispatched["orchestration_run_id"], "request-1", container=container)
    duplicate = prepare_job(job_id, dispatched["orchestration_run_id"], "request-1", container=container)

    assert duplicate == first
    assert first["job_id"] == job_id
    assert first["status"] == "running"
    assert "sanitized error" not in str(first)


def test_finalize_job_marks_orchestration_without_returning_candidates(tmp_path) -> None:
    from logrisk.application import ApplicationConfig, build_application_container
    from logrisk.airflow_tasks import finalize_job, prepare_job

    config = replace(
        ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / "state"),
        feature_jobs_auto_start=False,
    )
    container = build_application_container(config)
    job_id = container.feature_jobs.create_job({"summary": {}, "risk_entities": []}, model="qwen3:1.7b")
    run = container.orchestration.create_pending(job_id, "request-2", "pacas-alice")
    dispatched = container.orchestration.mark_dispatched(
        run["orchestration_run_id"], "logrisk_analysis", "logrisk__" + job_id, expected_version=run["state_version"]
    )
    prepared = prepare_job(job_id, dispatched["orchestration_run_id"], "request-2", container=container)
    container.feature_jobs.run_job(job_id)

    result = finalize_job(job_id, prepared["orchestration_run_id"], container=container)

    assert result["status"] == "completed"
    assert set(result) == {"job_id", "orchestration_run_id", "status"}


def test_finalize_job_marks_model_errors_as_failed_orchestration(tmp_path) -> None:
    from logrisk.application import ApplicationConfig, build_application_container
    from logrisk.airflow_tasks import finalize_job, prepare_job

    config = replace(
        ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / "state"),
        feature_jobs_auto_start=False,
    )
    container = build_application_container(config)
    container.feature_jobs.extractor = lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("model failed"))
    job_id = container.feature_jobs.create_job(_document(), model="qwen3:1.7b")
    run = container.orchestration.create_pending(job_id, "request-3", "pacas-alice")
    dispatched = container.orchestration.mark_dispatched(
        run["orchestration_run_id"], "logrisk_analysis", "logrisk__" + job_id, expected_version=run["state_version"]
    )
    prepared = prepare_job(job_id, dispatched["orchestration_run_id"], "request-3", container=container)
    container.feature_jobs.run_job(job_id)

    result = finalize_job(job_id, prepared["orchestration_run_id"], container=container)

    assert result["status"] == "failed"
