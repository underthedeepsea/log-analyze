from __future__ import annotations

import json
import os
from pathlib import Path

import django
import pytest
from django.core.management import call_command
from django.test import Client, override_settings


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_test_project.settings")
django.setup()


def _config(tmp_path: Path, resolver: str) -> dict[str, object]:
    return {
        "project_root": str(Path(__file__).resolve().parents[1]),
        "state_root": str(tmp_path / "state"),
        "output_root": str(tmp_path / "output"),
        "database_provider": "sqlite",
        "shared_root": str(tmp_path / "shared"),
        "airflow_base_url": "http://127.0.0.1:18080",
        "airflow_dag_id": "logrisk_analysis",
        "airflow_input_dag_id": "logrisk_input_preprocess",
        "identity_resolver": resolver,
        "write_roles": ["logrisk:operator"],
    }


@pytest.mark.parametrize("explicit, environment, expected", [
    (None, None, False),
    (None, "false", False),
    (None, "true", True),
    (False, "true", False),
    ("false", "true", False),
    ("true", "false", True),
])
def test_django_kafka_config_uses_explicit_setting_before_environment(tmp_path, monkeypatch, explicit, environment, expected):
    from logrisk_django.service_factory import get_config

    if environment is None:
        monkeypatch.delenv("LOGRISK_KAFKA_ENABLED", raising=False)
    else:
        monkeypatch.setenv("LOGRISK_KAFKA_ENABLED", environment)
    raw = _config(tmp_path, "tests.django_test_project.resolver.OperatorIdentityResolver")
    if explicit is not None:
        raw["kafka_enabled"] = explicit
    with override_settings(LOGRISK=raw):
        config = get_config()
    assert config.kafka_enabled is expected
    assert config.application_config().kafka_enabled is expected
    assert config.public_dict()["kafka_enabled"] is expected


def test_django_kafka_config_rejects_invalid_flag(tmp_path):
    from logrisk_django.service_factory import get_config
    from logrisk_django.settings import LogriskSettingsError

    raw = _config(tmp_path, "tests.django_test_project.resolver.OperatorIdentityResolver")
    with override_settings(LOGRISK={**raw, "kafka_enabled": "maybe"}):
        with pytest.raises(LogriskSettingsError, match="kafka_enabled"):
            get_config()


def test_django_kafka_sources_and_readiness_use_cached_container_without_connecting(tmp_path, monkeypatch):
    from logrisk.kafka_adapter import KafkaPythonConsumerAdapter
    from logrisk_django import service_factory

    monkeypatch.setattr(KafkaPythonConsumerAdapter, "_new_consumer", lambda *args: pytest.fail("No Broker connection"))
    monkeypatch.setattr(service_factory, "get_airflow_readiness", lambda: {"ready": True})
    for enabled in (True, False):
        raw = _config(tmp_path / str(enabled), "tests.django_test_project.resolver.OperatorIdentityResolver")
        with override_settings(LOGRISK={**raw, "kafka_enabled": enabled}):
            service_factory.clear_cached_container()
            try:
                call_command("logrisk_migrate", "--json")
                sources = Client().get("/api/streaming/sources")
                readiness = Client().get("/api/runtime/readiness")
                assert sources.status_code == 200
                assert readiness.status_code == 200
                kafka = sources.json()["sources"]["kafka"]
                assert kafka["enabled"] is enabled
                assert kafka["configured"] is enabled
                assert kafka["connection_check"] == "not_checked"
                assert kafka["active_tasks"] == 0
                assert readiness.json()["dependencies"]["kafka"] == kafka
            finally:
                service_factory.clear_cached_container()


def test_django_model_and_prompt_configuration_writes_are_governed(tmp_path) -> None:
    from logrisk_django.service_factory import clear_cached_container, get_container

    os.environ["REMOTE_CONFIG_KEY"] = "do-not-return"
    with override_settings(
        LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.OperatorIdentityResolver")
    ):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        client = Client()
        connection = client.post(
            "/api/ai-harness/connections",
            data=json.dumps(
                {
                    "connection_id": "remote-config",
                    "display_name": "远端配置",
                    "provider": "openai_compatible",
                    "base_url": "https://llm.example/v1",
                    "api_key_env": "REMOTE_CONFIG_KEY",
                    "timeout_seconds": 30,
                    "enabled": True,
                }
            ),
            content_type="application/json",
        )
        profile = client.post(
            "/api/ai-harness/model-profiles",
            data=json.dumps(
                {
                    "profile_id": "remote-config-profile",
                    "display_name": "远端配置画像",
                    "connection_id": "remote-config",
                    "model": "remote-model",
                    "default_prompt_id": "feature_extract_v3_compact_strict_json_en",
                    "max_output_tokens": 2400,
                }
            ),
            content_type="application/json",
        )
        prompt_id = "feature_extract_v3_compact_strict_json_en"
        current = get_container().prompt_registry.load(prompt_id)
        prompt = client.patch(
            f"/api/ai-harness/prompts/{prompt_id}",
            data=json.dumps({"content": current.content + "\n", "note": "Django 配置接口回归"}),
            content_type="application/json",
        )
        audits = get_container().runtime_repository.list_audits(limit=50)["items"]
        clear_cached_container()

    assert connection.status_code == 200
    assert connection.json()["connection_id"] == "remote-config"
    assert "do-not-return" not in json.dumps(connection.json())
    assert profile.status_code == 200
    assert profile.json()["profile_id"] == "remote-config-profile"
    assert prompt.status_code == 200
    assert prompt.json()["prompt_id"] == prompt_id
    assert prompt.json()["version"].startswith("v")
    actions = {item["action"] for item in audits}
    assert {"ai_connection.saved", "model_profile.saved", "prompt.updated"}.issubset(actions)
    assert all("content" not in json.dumps(item["attributes"], ensure_ascii=False) for item in audits)
    os.environ.pop("REMOTE_CONFIG_KEY", None)


def test_django_configuration_writes_fail_closed_without_identity(tmp_path) -> None:
    from logrisk_django.service_factory import clear_cached_container

    with override_settings(
        LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.AnonymousIdentityResolver")
    ):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        response = Client().post(
            "/api/ai-harness/connections",
            data=json.dumps(
                {
                    "connection_id": "blocked",
                    "display_name": "不应保存",
                    "provider": "ollama",
                    "base_url": "http://127.0.0.1:11434",
                }
            ),
            content_type="application/json",
        )
        clear_cached_container()

    assert response.status_code == 403
    assert response.json()["code"] == "runtime_identity_required"


def test_runtime_readiness_reports_airflow_dag_status(tmp_path, monkeypatch) -> None:
    from logrisk.orchestration.airflow import AirflowHealth
    from logrisk_django import service_factory
    from logrisk_django.service_factory import clear_cached_container

    class ReadyAirflow:
        def __init__(self, dag_id: str) -> None:
            self.dag_id = dag_id

        def health(self) -> AirflowHealth:
            return AirflowHealth(self.dag_id, False)

    monkeypatch.setattr(
        service_factory,
        "get_airflow_orchestrator",
        lambda: ReadyAirflow("logrisk_analysis"),
    )
    monkeypatch.setattr(
        service_factory,
        "get_input_airflow_orchestrator",
        lambda: ReadyAirflow("logrisk_input_preprocess"),
    )
    with override_settings(
        LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.OperatorIdentityResolver")
    ):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        response = Client().get("/api/runtime/readiness")
        clear_cached_container()

    assert response.status_code == 200
    airflow = response.json()["dependencies"]["airflow"]
    assert airflow["online"] is True
    assert airflow["dag_registered"] is True
    assert airflow["ready"] is True
    assert airflow["dags"] == ["logrisk_analysis", "logrisk_input_preprocess"]


def test_django_runtime_and_database_settings_writes_are_governed(tmp_path) -> None:
    from logrisk_django.service_factory import clear_cached_container

    with override_settings(
        LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.OperatorIdentityResolver")
    ):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        client = Client()
        retention = client.post(
            "/api/runtime/retention/policy",
            data=json.dumps({"expected_version": 0, "policy": {"enabled": True, "completed_days": 7}}),
            content_type="application/json",
        )
        database = client.post(
            "/api/system/database/config",
            data=json.dumps(
                {
                    "provider": "postgres",
                    "host": "db.internal",
                    "port": 5432,
                    "database": "logrisk",
                    "user": "logrisk_app",
                    "sslmode": "require",
                    "password_env": "LOGRISK_DB_PASSWORD",
                }
            ),
            content_type="application/json",
        )
        current = client.get("/api/system/database")
        audits = __import__("logrisk_django.service_factory", fromlist=["get_container"]).get_container().runtime_repository.list_audits(limit=50)["items"]
        clear_cached_container()

    assert retention.status_code == 200
    assert retention.json()["policy"]["version"] == 1
    assert database.status_code == 200
    assert database.json()["restart_required"] is True
    assert current.status_code == 200
    assert current.json()["candidate"]["password_env"] == "LOGRISK_DB_PASSWORD"
    assert "secret-value" not in json.dumps(current.json())
    assert any(item["action"] == "runtime.database_candidate.saved" for item in audits)
