from __future__ import annotations

from dataclasses import replace
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_airflow_style_container_does_not_interrupt_a_persisted_queued_job(tmp_path) -> None:
    """A separately started worker must execute the persisted job, not mark it interrupted on restore."""
    from logrisk.application import ApplicationConfig, build_application_container

    config = replace(
        ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / "state"),
        feature_jobs_auto_start=False,
    )
    creator = build_application_container(config)
    job_id = creator.feature_jobs.create_job(
        {"summary": {}, "risk_entities": []},
        model="test-model",
    )

    worker = build_application_container(replace(config, interrupt_feature_jobs=False))

    assert worker.feature_jobs.get_job(job_id)["status"] == "queued"


def test_web_container_refreshes_completed_state_written_by_another_worker(tmp_path) -> None:
    from logrisk.application import ApplicationConfig, build_application_container

    config = replace(
        ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / "state"),
        feature_jobs_auto_start=False,
        interrupt_feature_jobs=False,
    )
    creator = build_application_container(config)
    job_id = creator.feature_jobs.create_job({"summary": {}, "risk_entities": []}, model="test-model")
    web = build_application_container(config)
    worker = build_application_container(config)
    worker.feature_jobs.run_job(job_id)

    web.feature_jobs.refresh_from_persistence(job_id)

    assert web.feature_jobs.get_job(job_id)["status"] == "completed"
