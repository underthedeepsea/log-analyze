from __future__ import annotations

from pathlib import Path

import asyncio

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("enabled_first", [True, False])
def test_kafka_containers_isolate_opt_in_and_count_only_active_kafka_tasks(tmp_path, monkeypatch, enabled_first):
    from dataclasses import replace
    from logrisk.application.container import ApplicationConfig, build_application_container
    from logrisk.incremental_sources import IncrementalSourceError, SourceDescriptor, source_capabilities
    from logrisk.kafka_adapter import KafkaPythonConsumerAdapter

    monkeypatch.setattr(KafkaPythonConsumerAdapter, "_new_consumer", lambda *args: pytest.fail("No Broker connection"))
    containers = {}
    for enabled in (enabled_first, not enabled_first):
        config = replace(
            ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / str(enabled)),
            kafka_enabled="true" if enabled else "false",
        )
        containers[enabled] = build_application_container(config)
    enabled, disabled = containers[True], containers[False]
    assert enabled.config.kafka_enabled is True
    assert disabled.config.kafka_enabled is False
    assert enabled.source_capabilities()["kafka"]["enabled"] is True
    assert disabled.source_capabilities()["kafka"]["registered_adapter_ids"] == []
    assert "kafka-python" not in source_capabilities()["kafka"]["registered_adapter_ids"]
    with pytest.raises(IncrementalSourceError, match="未启用"):
        disabled.kafka_source({"adapter_id": "kafka-python"})

    repository = enabled.streaming_state
    for index, status in enumerate(("queued", "running", "completed", "failed", "interrupted")):
        task = repository.create_or_load(
            descriptor=SourceDescriptor("kafka", {}, {}), config_hash="hash", task_id=f"kafka_{index}"
        )
        if status == "running":
            repository.mark_running(task["task_id"])
        elif status == "completed":
            repository._update_task(task["task_id"], status="completed", stage="COMPLETED", event_type="test_completed")
        elif status == "failed":
            repository.mark_failed(task["task_id"], "safe failure")
        elif status == "interrupted":
            repository.mark_interrupted(task["task_id"])
    repository.create_or_load(descriptor=SourceDescriptor("file", {}, {}), config_hash="hash")
    status = enabled.source_capabilities()["kafka"]
    assert status["active_tasks"] == 2
    assert status["connection_check"] == "not_checked"
    assert disabled.source_capabilities()["kafka"]["active_tasks"] == 0


def test_application_container_builds_shared_services_without_starting_http(tmp_path) -> None:
    """A web framework or Airflow worker can obtain LOGRISK services without a socket."""
    from logrisk.application.container import ApplicationConfig, build_application_container

    container = build_application_container(
        ApplicationConfig.for_test(
            project_root=PROJECT_ROOT,
            state_root=tmp_path / "state",
        )
    )

    assert container.database.provider == "sqlite"
    assert container.feature_jobs is not None
    assert container.runtime_service is not None
    assert container.release_readiness is not None
    assert container.artifact_store.root == (tmp_path / "state").resolve()


def test_registered_recompute_entry_creates_an_isolated_schedulable_job(tmp_path) -> None:
    from logrisk.application.container import ApplicationConfig, build_application_container

    container = build_application_container(
        ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / "state")
    )
    payload = b"safe recompute fixture\n"
    upload = container.upload_store.create(filename="recompute.log", size_bytes=len(payload))
    container.upload_store.append_chunk(upload_id=upload["upload_id"], index=0, data=payload)
    completed = container.upload_store.complete(upload_id=upload["upload_id"])
    source = container.artifact_store.resolve(completed["artifact_relative_path"])
    old = container.input_jobs.create(
        upload_id=upload["upload_id"], filename=source.name, source_path=str(source),
    )
    from logrisk.incremental_sources import FileIncrementalSource
    import hashlib

    task = container.streaming_state.create_or_load(
        descriptor=FileIncrementalSource(source, filename=source.name).descriptor(),
        config_hash=hashlib.sha256((PROJECT_ROOT / "configs/drain3_recommended.ini").read_bytes()).hexdigest(),
    )
    container.streaming_state.attach_input_job(task["task_id"], old["input_job_id"])
    old["streaming_task_id"] = task["task_id"]
    container.input_jobs.write_job(old["input_job_id"], old)

    fresh = container.create_recompute_input_job(old["input_job_id"])

    assert fresh["status"] == "queued"
    assert fresh["side_effect_policy"] == "isolated_recompute"
    assert container.run_input_job is not None


def test_input_job_cancellation_closes_both_input_and_streaming_states(tmp_path, monkeypatch) -> None:
    import logrisk.application.container as container_module
    from logrisk.application.container import ApplicationConfig, build_application_container

    container = build_application_container(
        ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / "state")
    )
    payload = b"safe fixture\n"
    upload = container.upload_store.create(filename="cancelled.log", size_bytes=len(payload))
    container.upload_store.append_chunk(upload_id=upload["upload_id"], index=0, data=payload)
    completed_upload = container.upload_store.complete(upload_id=upload["upload_id"])
    source = container.artifact_store.resolve(completed_upload["artifact_relative_path"])
    job = container.input_jobs.create(
        upload_id=upload["upload_id"],
        filename=source.name,
        source_path=str(source),
    )
    interruption = asyncio.CancelledError()

    def cancel_pipeline(**kwargs):
        task = container.streaming_state.claim_task(kwargs["resume_task_id"])
        container.streaming_state.finish_claim(
            task["task_id"], task["lease_token"], error="CancelledError", interrupted=True,
        )
        raise interruption

    monkeypatch.setattr(container_module, "run_large_file_pipeline", cancel_pipeline)

    with pytest.raises(asyncio.CancelledError) as caught:
        container.run_input_job(job["input_job_id"])

    assert caught.value is interruption
    saved = container.input_jobs.get_job(job["input_job_id"])
    assert saved["status"] == "interrupted"
    assert container.input_jobs.get_progress(job["input_job_id"])["status"] == "interrupted"
    streaming = container.streaming_state.get_task(saved["streaming_task_id"])
    assert streaming["status"] == "interrupted"


def test_busy_input_job_wrapper_does_not_stop_current_streaming_owner(tmp_path, monkeypatch) -> None:
    import hashlib
    import logrisk.application.container as container_module
    from logrisk.application.container import ApplicationConfig, build_application_container
    from logrisk.incremental_sources import FileIncrementalSource
    from logrisk.streaming_state import StreamingTaskBusyError

    container = build_application_container(
        ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / "state")
    )
    payload = b"safe fixture\n"
    upload = container.upload_store.create(filename="busy.log", size_bytes=len(payload))
    container.upload_store.append_chunk(upload_id=upload["upload_id"], index=0, data=payload)
    completed_upload = container.upload_store.complete(upload_id=upload["upload_id"])
    source = container.artifact_store.resolve(completed_upload["artifact_relative_path"])
    job = container.input_jobs.create(upload_id=upload["upload_id"], filename=source.name, source_path=str(source))
    task = container.streaming_state.create_or_load(
        descriptor=FileIncrementalSource(source, filename=source.name).descriptor(),
        config_hash=hashlib.sha256((PROJECT_ROOT / "configs/drain3_recommended.ini").read_bytes()).hexdigest(),
    )
    container.streaming_state.attach_input_job(task["task_id"], job["input_job_id"])
    job["streaming_task_id"] = task["task_id"]
    container.input_jobs.write_job(job["input_job_id"], job)
    owner = container.streaming_state.claim_task(task["task_id"])

    monkeypatch.setattr(
        container_module,
        "run_large_file_pipeline",
        lambda **kwargs: (_ for _ in ()).throw(StreamingTaskBusyError("owned")),
    )

    assert container.run_input_job(job["input_job_id"]) is None
    current = container.streaming_state.get_task(task["task_id"])
    assert current["status"] == "running"
    assert current["lease_token"] == owner["lease_token"]
    assert container.input_jobs.get_job(job["input_job_id"])["status"] == "running"


def test_completed_input_job_is_not_downgraded_by_notification_cancellation(tmp_path, monkeypatch) -> None:
    import logrisk.application.container as container_module
    from logrisk.application.container import ApplicationConfig, build_application_container

    container = build_application_container(
        ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / "state")
    )
    payload = b"safe fixture\n"
    upload = container.upload_store.create(filename="complete.log", size_bytes=len(payload))
    container.upload_store.append_chunk(upload_id=upload["upload_id"], index=0, data=payload)
    completed_upload = container.upload_store.complete(upload_id=upload["upload_id"])
    source = container.artifact_store.resolve(completed_upload["artifact_relative_path"])
    job = container.input_jobs.create(upload_id=upload["upload_id"], filename=source.name, source_path=str(source))

    def complete_pipeline(**kwargs):
        task = container.streaming_state.claim_task(kwargs["resume_task_id"])
        container.streaming_state._update_task(
            task["task_id"], status="completed", stage="COMPLETED", event_type="test_completed",
        )
        return {"summary": {}, "risk_entities": [], "top_templates": []}

    interruption = asyncio.CancelledError()
    original_write_progress = container.input_jobs.write_progress

    def cancel_completed_progress(input_job_id, progress):
        if progress.get("status") == "completed":
            raise interruption
        return original_write_progress(input_job_id, progress)

    monkeypatch.setattr(container_module, "run_large_file_pipeline", complete_pipeline)
    monkeypatch.setattr(container.input_jobs, "write_progress", cancel_completed_progress)

    with pytest.raises(asyncio.CancelledError) as caught:
        container.run_input_job(job["input_job_id"])

    assert caught.value is interruption
    assert container.input_jobs.get_job(job["input_job_id"])["status"] == "completed"


def test_application_container_loads_m21_limits_from_committed_config(tmp_path) -> None:
    from logrisk.application.container import ApplicationConfig, build_application_container

    config_root = tmp_path / "project"
    (config_root / "configs").mkdir(parents=True)
    (config_root / "configs" / "runtime.yaml").write_text("runtime: {}\n", encoding="utf-8")
    (config_root / "configs" / "ai_harness.yaml").write_text(
        "agent_workflows:\n  allowed_roles: [evidence_specialist]\n  max_nodes: 1\n  max_concurrency: 1\n  max_tool_calls: 5\n  timeout_seconds: 30\n  max_attempts: 1\n",
        encoding="utf-8",
    )
    for name in ("risk_rules.yaml",):
        (config_root / "configs" / name).write_text("rules: []\n", encoding="utf-8")
    # Reuse repository seed files required by the shared container while replacing only M21 config.
    import shutil
    repository_configs = PROJECT_ROOT / "configs"
    for name in ("model_profiles.yaml", "drain3_recommended.ini"):
        shutil.copy(repository_configs / name, config_root / "configs" / name)
    for name in ("multi_source.yaml",):
        shutil.copy(repository_configs / name, config_root / "configs" / name)
    shutil.copytree(repository_configs / "semantic_dictionary", config_root / "configs" / "semantic_dictionary")
    (config_root / "configs" / "risk_semantics").mkdir()
    shutil.copy(repository_configs / "risk_semantics" / "builtin.yaml", config_root / "configs" / "risk_semantics" / "builtin.yaml")
    shutil.copy(repository_configs / "node_risk.yaml", config_root / "configs" / "node_risk.yaml")
    container = build_application_container(ApplicationConfig(project_root=config_root, state_root=tmp_path / "state", output_root=tmp_path / "output", agentic_enabled=True, agent_workflows_enabled=True))
    assert container.agent_workflows is not None
    assert len(container.agent_workflows.roles.list()) == 1
    assert container.agent_workflows.limits.max_nodes == 1
