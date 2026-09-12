from __future__ import annotations

from pathlib import Path

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
            repository.mark_completed(task["task_id"])
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
