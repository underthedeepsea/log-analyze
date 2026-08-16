from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


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
