from __future__ import annotations

import json

import pytest

from logrisk.ai_harness.connections import ConnectionStore
from logrisk.database import SQLiteDatabase


def test_connection_store_never_persists_resolved_api_key(tmp_path, monkeypatch):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    store = ConnectionStore(database)
    monkeypatch.setenv("REMOTE_LLM_TOKEN", "top-secret-value")

    saved = store.save({
        "connection_id": "remote-main",
        "display_name": "远端主连接",
        "provider": "openai_compatible",
        "base_url": "https://llm.example/v1/",
        "api_key_env": "REMOTE_LLM_TOKEN",
        "timeout_seconds": 45,
        "enabled": True,
    })

    assert saved["base_url"] == "https://llm.example/v1"
    assert saved["api_key_configured"] is True
    assert "api_key" not in saved
    assert "top-secret-value" not in database.path.read_bytes().decode("utf-8", errors="ignore")


def test_connection_store_validates_provider_and_environment_name(tmp_path):
    store = ConnectionStore(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))

    with pytest.raises(ValueError, match="provider"):
        store.save({"connection_id": "bad", "provider": "anthropic", "base_url": "https://example.com/v1"})
    with pytest.raises(ValueError, match="环境变量"):
        store.save({
            "connection_id": "bad-key",
            "provider": "openai_compatible",
            "base_url": "https://example.com/v1",
            "api_key_env": "NOT VALID",
        })


def test_default_ollama_connection_is_seeded(tmp_path):
    store = ConnectionStore(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    store.seed_defaults("http://127.0.0.1:11434")

    connection = store.get("ollama-local")

    assert connection["provider"] == "ollama"
    assert connection["is_default"] is True


def test_extension_connection_persists_only_credential_environment_names(tmp_path, monkeypatch):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    store = ConnectionStore(database)
    monkeypatch.setenv("INTERNAL_ACCESS_TOKEN", "real-internal-token")

    saved = store.save({
        "connection_id": "internal-token",
        "display_name": "内部 Token 模型",
        "provider": "extension",
        "base_url": "https://internal.example/model",
        "adapter_id": "token_auth_template",
        "credential_envs": {"access_token": "INTERNAL_ACCESS_TOKEN"},
        "extension_config": {"tenant": "ops"},
        "timeout_seconds": 30,
        "enabled": True,
    })

    assert saved["adapter_id"] == "token_auth_template"
    assert saved["credential_envs"] == {"access_token": "INTERNAL_ACCESS_TOKEN"}
    assert saved["credential_envs_configured"] == {"access_token": True}
    assert saved["extension_config"] == {"tenant": "ops"}
    assert "real-internal-token" not in database.path.read_bytes().decode("utf-8", errors="ignore")


def test_extension_connection_rejects_unregistered_adapter_and_sensitive_config(tmp_path):
    store = ConnectionStore(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    base = {
        "connection_id": "internal-token",
        "display_name": "内部",
        "provider": "extension",
        "base_url": "https://internal.example/model",
        "credential_envs": {"access_token": "INTERNAL_ACCESS_TOKEN"},
    }

    with pytest.raises(ValueError, match="未注册"):
        store.save({**base, "adapter_id": "not-registered"})
    with pytest.raises(ValueError, match="敏感"):
        store.save({**base, "adapter_id": "token_auth_template", "extension_config": {"token": "not-allowed"}})
