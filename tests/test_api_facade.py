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


def test_review_request_uses_trusted_actor_scope_and_does_not_reaudit_replay(tmp_path):
    from types import SimpleNamespace
    from logrisk.application import ApplicationConfig, build_application_container
    from logrisk.application.api import ApiFacade
    from logrisk.runtime.identity import RequestIdentity

    container = build_application_container(
        ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / "state")
    )
    calls = []

    def update(*args, **kwargs):
        calls.append((args, kwargs))
        return {"status": "approved", "decision_id": "decision-a", "idempotent_replay": True}

    facade = ApiFacade(container, version="1.38.1", service_resolver=lambda name, default: (
        SimpleNamespace(update_feature=update) if name == "feature_jobs" else default
    ))
    identity = RequestIdentity("reviewer-a", ("logrisk:operator",), "request-a", True, "trusted_proxy", "127.0.0.1")
    before = container.runtime_repository.list_audits()
    response = facade.update_feature("job-a", "candidate-a", {
        "status": "approved", "expected_updated_at": "2026-09-12T00:00:00Z", "request_key": "review-a",
    }, identity)
    assert response.body["decision_id"] == "decision-a"
    assert calls == [(("job-a", "candidate-a", {"status": "approved"}), {
        "expected_updated_at": "2026-09-12T00:00:00Z", "request_key": "review-a",
        "actor_scope": "trusted_proxy:reviewer-a",
    })]
    assert container.runtime_repository.list_audits() == before
    facade.update_feature("job-a", "candidate-a", {"status": "approved"}, identity)
    assert calls[-1][1] == {"actor_scope": "trusted_proxy:reviewer-a"}


def test_readiness_reports_unavailable_source_status_without_exposing_storage_error(tmp_path, monkeypatch):
    from logrisk.application import ApplicationConfig, build_application_container
    from logrisk.application.api import ApiFacade
    from logrisk.database import DatabaseError

    container = build_application_container(
        ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / "state")
    )

    def unavailable():
        raise DatabaseError("sensitive connection details")

    monkeypatch.setattr(container, "source_capabilities", unavailable)
    response = ApiFacade(container, version="1.38.1").runtime_readiness()
    assert response.status == 503
    assert response.body["ready"] is False
    assert response.body["dependencies"]["kafka"]["active_tasks"] is None
    assert response.body["dependencies"]["kafka"]["status"] == "unavailable"
    assert "sensitive connection details" not in str(response.body)
