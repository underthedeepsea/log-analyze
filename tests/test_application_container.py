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
