from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_api_facade_provides_framework_independent_core_read_responses(tmp_path) -> None:
    from logrisk.application import ApplicationConfig, build_application_container
    from logrisk.application.api import ApiFacade

    container = build_application_container(
        ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / "state")
    )
    facade = ApiFacade(container, version="1.31.0")

    health = facade.dispatch_read("/api/health")
    readiness = facade.dispatch_read("/api/runtime/readiness")
    profiles = facade.dispatch_read("/api/ai-harness/model-profiles")
    prompts = facade.dispatch_read("/api/ai-harness/prompts")
    rules = facade.dispatch_read("/api/rule-governance/rules")
    release = facade.dispatch_read("/api/release-readiness")

    assert health and health.body["storage"] == "sqlite"
    assert readiness and readiness.status in {200, 503}
    assert profiles and "profiles" in profiles.body
    assert prompts and "items" in prompts.body
    assert rules and "items" in rules.body
    assert release and "safe_to_release" in release.body
    assert facade.dispatch_read("/api/not-supported") is None
