from __future__ import annotations

from logrisk.ai_harness.prompt_registry import PromptTemplate
from logrisk.database import SQLiteDatabase
from logrisk.observability import (
    ObservabilityRepository,
    PromptSnapshotResolver,
    ReplayService,
    compare_replay,
)


class Traces:
    def __init__(self, trace):
        self.trace = trace

    def get_trace(self, trace_id):
        return self.trace if trace_id == self.trace["trace_id"] else None


class Prompts:
    def load(self, prompt_id):
        return PromptTemplate(prompt_id, "prompt", "hash-1", "memory")


def test_historical_replay_revalidates_without_model_call(tmp_path):
    database = SQLiteDatabase(tmp_path / "state" / "logrisk.sqlite3")
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO ai_traces(trace_id, status, trace_json, created_at) VALUES (?, ?, ?, ?)",
            ("trace-1", "success", "{}", "2026-01-01T00:00:00+00:00"),
        )
    trace = {
        "trace_id": "trace-1",
        "prompt_id": "feature",
        "prompt_hash": "hash-1",
        "parsed_output": {"features": []},
        "validation_result": {"valid": True},
        "evaluator_result": {"passed": True},
    }
    service = ReplayService(
        ObservabilityRepository(database),
        Traces(trace),
        PromptSnapshotResolver(Prompts()),
    )
    replay = service.create(
        {
            "source_trace_id": "trace-1",
            "mode": "historical",
            "confirmed": True,
        },
        idempotency_key="history-1",
    )

    completed = service.execute(replay["replay_id"])

    assert completed["status"] == "completed"
    assert completed["result"]["model_called"] is False
    assert completed["result"]["diff"]["changed"] is False


def test_model_replay_uses_runner_and_is_idempotent(tmp_path):
    database = SQLiteDatabase(tmp_path / "state" / "logrisk.sqlite3")
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO ai_traces(trace_id, status, trace_json, created_at) VALUES (?, ?, ?, ?)",
            ("trace-1", "success", "{}", "2026-01-01T00:00:00+00:00"),
        )
    trace = {
        "trace_id": "trace-1",
        "prompt_id": "feature",
        "prompt_hash": "hash-1",
        "parsed_output": {"features": []},
    }
    calls = []
    service = ReplayService(
        ObservabilityRepository(database),
        Traces(trace),
        PromptSnapshotResolver(Prompts()),
        model_runner=lambda snapshot: calls.append(snapshot) or {"parsed_output": {"features": []}},
    )
    payload = {"source_trace_id": "trace-1", "mode": "model", "confirmed": True}
    first = service.create(payload, idempotency_key="model-1")
    second = service.create(payload, idempotency_key="model-1")

    completed = service.execute(first["replay_id"])

    assert second["replay_id"] == first["replay_id"]
    assert completed["result"]["model_called"] is True
    assert len(calls) == 1


def test_replay_diff_reports_added_removed_and_changed_fields():
    source = {
        "parsed_output": {
            "features": [
                {
                    "feature_type": "kernel_error",
                    "template_hashes": ["hash-1"],
                    "title": "旧标题",
                    "tags": ["内核"],
                },
                {
                    "feature_type": "removed",
                    "template_hashes": ["hash-2"],
                },
            ]
        }
    }
    replay = {
        "parsed_output": {
            "features": [
                {
                    "feature_type": "kernel_error",
                    "template_hashes": ["hash-1"],
                    "title": "新标题",
                    "tags": ["内核", "错误"],
                },
                {
                    "feature_type": "added",
                    "template_hashes": ["hash-3"],
                },
            ]
        }
    }

    diff = compare_replay(source, replay)

    assert diff["added"] == 1
    assert diff["removed"] == 1
    assert diff["modified"] == 1
    assert diff["field_changes"][0]["fields"] == ["title", "tags"]
