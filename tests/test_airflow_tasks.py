from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest


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

    def finish(input_job_id: str, *, raise_retryable: bool = False) -> None:
        assert raise_retryable
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

    assert preprocess_uploaded_input(
        input_job["input_job_id"], dispatched["input_orchestration_run_id"], "request-input", container=container,
    ) == result
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


def test_airflow_replays_the_same_upload_after_transient_storage_failure(tmp_path, monkeypatch):
    from logrisk import large_file_pipeline
    from logrisk.airflow_tasks import preprocess_uploaded_input
    from logrisk.application import ApplicationConfig, build_application_container

    app = build_application_container(ApplicationConfig.for_test(
        project_root=PROJECT_ROOT, state_root=tmp_path / "state",
    ))
    filename = "messages.log"
    data = b"Jul 19 10:00:00 gpu-node-01 kernel: NVRM: Xid (0000:65:00): 79, diagnostic event\n"
    upload = app.upload_store.create(filename=filename, size_bytes=len(data))
    app.upload_store.append_chunk(upload_id=upload["upload_id"], index=0, data=data)
    completed = app.upload_store.complete(upload_id=upload["upload_id"])
    source = app.artifact_store.resolve(completed["artifact_relative_path"])
    job = app.input_jobs.create(upload_id=upload["upload_id"], filename=filename, source_path=str(source))
    job_id, request_id = job["input_job_id"], "input-replay"
    run = app.input_orchestration.create_pending(job_id, request_id, "fixture")
    run_id = run["input_orchestration_run_id"]
    app.input_orchestration.mark_dispatched(
        run_id, "logrisk_input_preprocess", "fixture-run", expected_version=run["state_version"],
    )
    original = large_file_pipeline.spool_normalized_records
    attempts = 0

    def transient_failure(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("fixture transient I/O")
        return original(*args, **kwargs)

    monkeypatch.setattr(large_file_pipeline, "spool_normalized_records", transient_failure)
    with pytest.raises(OSError, match="fixture transient"):
        preprocess_uploaded_input(job_id, run_id, request_id, container=app)
    assert app.input_orchestration.get(run_id)["status"] == "running"
    pending = app.input_jobs.get_job(job_id)
    assert pending["status"] in {"interrupted", "running"}
    storage_task_id = pending["streaming_task_id"]
    result = preprocess_uploaded_input(job_id, run_id, request_id, container=app)
    assert result["status"] == "completed"
    assert app.input_jobs.get_job(job_id)["streaming_task_id"] == storage_task_id
    assert preprocess_uploaded_input(job_id, run_id, request_id, container=app) == result
    assert attempts == 2
    assert len(app.streaming_state.list_tasks()) == 1


def test_airflow_distinguishes_busy_from_terminal_failure(tmp_path):
    import sqlite3

    from logrisk.airflow_tasks import InputPreprocessTerminalError, preprocess_uploaded_input
    from logrisk.application import ApplicationConfig, build_application_container
    from logrisk.input_jobs import is_retryable_input_error
    from logrisk.streaming_state import StreamingTaskBusyError

    app = build_application_container(ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / "state"))
    upload = app.upload_store.create(filename="messages", size_bytes=1)
    source = app.artifact_store.resolve("uploads/source")
    job = app.input_jobs.create(upload_id=upload["upload_id"], filename="messages", source_path=str(source))
    job_id, request_id = job["input_job_id"], "busy-or-failed"
    run = app.input_orchestration.create_pending(job_id, request_id, "fixture")
    run_id = run["input_orchestration_run_id"]
    app.input_orchestration.mark_dispatched(
        run_id, "logrisk_input_preprocess", "fixture-run", expected_version=run["state_version"],
    )
    calls = 0

    def attempt(input_job_id, *, raise_retryable):
        nonlocal calls
        assert raise_retryable
        calls += 1
        if calls == 1:
            raise StreamingTaskBusyError("another fixture worker")
        current = app.input_jobs.get_job(input_job_id)
        app.input_jobs.write_job(input_job_id, dict(current, status="failed"))

    app.run_input_job = attempt
    with pytest.raises(StreamingTaskBusyError):
        preprocess_uploaded_input(job_id, run_id, request_id, container=app)
    assert app.input_orchestration.get(run_id)["status"] == "running"
    with pytest.raises(InputPreprocessTerminalError, match="预处理失败"):
        preprocess_uploaded_input(job_id, run_id, request_id, container=app)
    assert app.input_orchestration.get(run_id)["status"] == "failed"
    with pytest.raises(InputPreprocessTerminalError, match="已失败"):
        preprocess_uploaded_input(job_id, run_id, request_id, container=app)
    assert calls == 2
    assert is_retryable_input_error(sqlite3.OperationalError("database table is locked: job"))
    assert not is_retryable_input_error(sqlite3.OperationalError("no such table: job"))
