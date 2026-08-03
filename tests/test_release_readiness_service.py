from __future__ import annotations

from types import SimpleNamespace

from logrisk.database import SQLiteDatabase
from logrisk.release_readiness.repository import ReleaseReadinessRepository
from logrisk.release_readiness.service import ReleaseReadinessService
from logrisk.runtime.config import RuntimeConfig
from logrisk.runtime.service import RuntimeService


def _service(tmp_path, *, profile_enabled: bool = True) -> ReleaseReadinessService:
    database = SQLiteDatabase(tmp_path / "state" / "logrisk.sqlite3")
    runtime = RuntimeService(
        database,
        state_root=tmp_path / "state",
        output_root=tmp_path / "output",
        config=RuntimeConfig.from_mapping({}),
    )
    root = tmp_path / "workspace"
    (root / "frontend" / "dist").mkdir(parents=True)
    (root / "frontend" / "dist" / "index.html").write_text("<!doctype html>", encoding="utf-8")
    (root / "configs").mkdir()
    for name in ("ai_harness.yaml", "risk_rules.yaml", "drain3_recommended.ini", "multi_source.yaml"):
        (root / "configs" / name).write_text("seed", encoding="utf-8")

    return ReleaseReadinessService(
        ReleaseReadinessRepository(database),
        runtime_service=runtime,
        project_root=root,
        connections=SimpleNamespace(list=lambda: [{"connection_id": "local", "enabled": True, "provider": "ollama"}]),
        model_profiles=SimpleNamespace(
            default_profile_id="profile-1",
            get=lambda _profile_id: SimpleNamespace(
                profile_id="profile-1",
                enabled=profile_enabled,
                connection_id="local",
                default_prompt_id="feature_extract_v3_compact_strict_json_en",
            ),
        ),
        prompt_registry=SimpleNamespace(
            get_default=lambda _analysis_type: SimpleNamespace(prompt_id="feature_extract_v3_compact_strict_json_en"),
        ),
        drain_quality=SimpleNamespace(configs=SimpleNamespace(active_snapshot=lambda: {"config_id": "baseline", "version": 1})),
        semantic_dictionaries=SimpleNamespace(active_snapshot=lambda: {"versions": {"linux": {"version": 1}}}),
        multi_source=SimpleNamespace(rules_view=lambda: {"items": [{"rule_id": "exact-entity-cross-source", "enabled": True}]}),
        benchmark_center=SimpleNamespace(overview=lambda: {"suite_count": 1, "run_count": 2, "gate_counts": {"blocked": 0}}),
    )


def test_release_readiness_validation_is_deterministic_sanitized_and_idempotent(tmp_path) -> None:
    service = _service(tmp_path)

    first = service.validate(target_version="1.30.0", idempotency_key="m18-first")
    duplicate = service.validate(target_version="1.30.0", idempotency_key="m18-first")

    assert first["status"] == "passed"
    assert duplicate["validation_id"] == first["validation_id"]
    assert {item["check_id"] for item in first["checks"]} >= {
        "runtime", "frontend_bundle", "model_profile", "prompt", "drain3", "semantic_dictionary", "multi_source", "benchmark",
    }
    assert "prompt_content" not in str(first)
    assert "raw_sample" not in str(first)
    assert "api_key" not in str(first)


def test_release_readiness_blocks_disabled_default_profile(tmp_path) -> None:
    service = _service(tmp_path, profile_enabled=False)

    result = service.validate(target_version="1.30.0", idempotency_key="m18-disabled-profile")

    assert result["status"] == "blocked"
    profile = next(item for item in result["checks"] if item["check_id"] == "model_profile")
    assert profile["status"] == "blocked"
