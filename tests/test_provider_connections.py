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
