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


def test_preprocess_uploaded_input_runs_only_on_the_airflow_worker(tmp_path) -> None:
    from logrisk.application import ApplicationConfig, build_application_container
    from logrisk.airflow_tasks import preprocess_uploaded_input

    config = replace(
        ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / "state"),
        feature_jobs_auto_start=False,
    )
    container = build_application_container(config)
    source = container.artifact_store.resolve("uploads/input.log")
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("sanitized source", encoding="utf-8")
    upload = container.upload_store.create(filename="messages", size_bytes=len(b"sanitized source"))
    input_job = container.input_jobs.create(
        upload_id=upload["upload_id"],
        filename="messages",
        source_path=str(source),
    )
    run = container.input_orchestration.create_pending(input_job["input_job_id"], "request-input", "pacas-alice")
    dispatched = container.input_orchestration.mark_dispatched(
        run["input_orchestration_run_id"],
        "logrisk_input_preprocess",
        "logrisk_input__" + input_job["input_job_id"],
        expected_version=run["state_version"],
    )
    called: list[str] = []

    def finish(input_job_id: str) -> None:
        called.append(input_job_id)
        job = container.input_jobs.get_job(input_job_id)
        job.update({"status": "completed", "stage": "completed"})
        container.input_jobs.write_job(input_job_id, job)

    container.run_input_job = finish
    result = preprocess_uploaded_input(
        input_job["input_job_id"],
        dispatched["input_orchestration_run_id"],
        "request-input",
        container=container,
    )

    assert called == [input_job["input_job_id"]]
    assert result == {
        "input_job_id": input_job["input_job_id"],
        "input_orchestration_run_id": dispatched["input_orchestration_run_id"],
        "status": "completed",
    }
    assert container.input_orchestration.get(dispatched["input_orchestration_run_id"])["status"] == "completed"


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


def test_empty_job_still_gets_a_feature_batch_for_completion(tmp_path) -> None:
    from logrisk.application import ApplicationConfig, build_application_container
    from logrisk.airflow_tasks import list_feature_batches, prepare_job

    config = replace(
        ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / "state"),
        feature_jobs_auto_start=False,
    )
    container = build_application_container(config)
    job_id = container.feature_jobs.create_job({"summary": {}, "risk_entities": []}, model="qwen3:1.7b")
    run = container.orchestration.create_pending(job_id, "request-empty-batch", "pacas-alice")
    dispatched = container.orchestration.mark_dispatched(
        run["orchestration_run_id"], "logrisk_analysis", "logrisk__" + job_id, expected_version=run["state_version"]
    )
    prepared = prepare_job(job_id, dispatched["orchestration_run_id"], "request-empty-batch", container=container)

    result = list_feature_batches(job_id, prepared["orchestration_run_id"], container=container)

    assert result["batch_ids"] == ["all"]


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
