from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator

import pytest

from logrisk.ai_harness.cache import AICache
from logrisk.ai_harness.providers.extension import ExtensionModelClient
from logrisk.ai_harness.providers.extensions.base import ExtensionDescriptor, ExtensionResponse
from logrisk.ai_harness.providers.extensions.registry import ADAPTERS
from logrisk.ai_harness.providers.ollama import OllamaModelClient
from logrisk.ai_harness.providers.openai_compatible import OpenAICompatibleModelClient
from logrisk.ai_harness.trace_logger import AITraceLogger
from logrisk.ai_harness.usage_accounting import run_model_attempt
from logrisk.agentic.models import AgentPlan, AgentRunRequest, AgentStepPlan
from logrisk.agentic.planner import FakeAgentPlanner, ModelAgentPlanner
from logrisk.agentic.repository import AgentRepository
from logrisk.agentic.runtime import AgentRuntime
from logrisk.agentic.tool_registry import ToolRegistry
from logrisk.database import SQLiteDatabase
from logrisk.feature_extractor_ollama import extract_features_for_entity
from logrisk.operational_ledgers import OperationalLedgerRepository


SCHEMA = {"type": "object", "properties": {"features": {"type": "array"}}}


@contextmanager
def loopback_responses(responses: list[tuple[int, dict[str, Any]]]) -> Iterator[tuple[str, list[dict[str, Any]]]]:
    queue = list(responses)
    requests: list[dict[str, Any]] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: Any) -> None:
            return None

        def do_POST(self) -> None:  # noqa: N802 - stdlib handler contract
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            requests.append({"path": self.path, "body": json.loads(body), "headers": dict(self.headers)})
            status, payload = queue.pop(0)
            encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def _ledger(tmp_path: Path) -> OperationalLedgerRepository:
    return OperationalLedgerRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))


def _invoke(client: Any, *, ledger: OperationalLedgerRepository, logical: str = "request-1") -> Any:
    return run_model_attempt(
        client,
        [{"role": "user", "content": "SECRET prompt must never be persisted"}],
        SCHEMA,
        model="test-model",
        timeout=5,
        ledger_repository=ledger,
        environment="local-test",
        scope_key="default",
        caller_kind="feature_extractor",
        caller_id="entity-1",
        logical_call_id=logical,
    )


def test_loopback_ollama_records_success_http_failure_and_parse_failure(tmp_path: Path) -> None:
    with loopback_responses(
        [
            (200, {"message": {"content": '{"features": []}'}, "prompt_eval_count": 120, "eval_count": 30}),
            (500, {"error": "SECRET server detail"}),
            (200, {"message": {"content": "not-json"}, "prompt_eval_count": 7, "eval_count": 2}),
        ]
    ) as (base_url, requests):
        client = OllamaModelClient(base_url)
        ledger = _ledger(tmp_path)
        assert _invoke(client, ledger=ledger) == {"features": []}
        with pytest.raises(Exception):
            _invoke(client, ledger=ledger)
        assert client.last_metadata == {}
        with pytest.raises(Exception):
            _invoke(client, ledger=ledger)

        assert [request["path"] for request in requests] == ["/api/chat"] * 3
        with ledger.database.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM operational_physical_calls ORDER BY attempt_index"
            ).fetchall()
        assert [row["status"] for row in rows] == ["succeeded", "failed", "failed"]
        assert rows[0]["total_tokens"] == 150
        assert rows[1]["usage_quality"] == "unknown"
        assert rows[2]["total_tokens"] == 9
        assert "SECRET" not in repr([dict(row) for row in rows])


def test_loopback_openai_usage_keeps_supplier_subcategories_as_subsets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OP_USAGE_KEY", "secret-token")
    with loopback_responses(
        [
            (
                200,
                {
                    "choices": [{"message": {"content": '{"features": []}'}}],
                    "usage": {
                        "prompt_tokens": 120,
                        "completion_tokens": 30,
                        "total_tokens": 130,
                        "prompt_tokens_details": {"cached_tokens": 40},
                        "completion_tokens_details": {"reasoning_tokens": 20},
                    },
                },
            )
        ]
    ) as (base_url, requests):
        client = OpenAICompatibleModelClient(base_url, api_key_env="OP_USAGE_KEY")
        ledger = _ledger(tmp_path)
        assert _invoke(client, ledger=ledger) == {"features": []}
        assert requests[0]["headers"]["Authorization"] == "Bearer secret-token"
        assert client.last_metadata["usage"] == {
            "input_tokens": 120,
            "output_tokens": 30,
            "total_tokens": 130,
            "cached_input_tokens": 40,
            "reasoning_tokens": 20,
        }
        with ledger.database.connect() as connection:
            row = connection.execute("SELECT * FROM operational_physical_calls").fetchone()
        assert row["total_tokens"] == 130
        assert row["cached_input_tokens"] == 40
        assert row["reasoning_tokens"] == 20
        assert "secret-token" not in repr(dict(row))


def test_retries_get_new_attempt_rows_and_finish_replay_does_not_inflate(tmp_path: Path) -> None:
    class RetryingClient:
        provider = "fake"
        metadata_contract = "v1"

        def __init__(self) -> None:
            self.last_metadata: dict[str, Any] = {}
            self.calls = 0

        def generate_json(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("remote unavailable")
            self.last_metadata = {"usage": {"input_tokens": 10, "output_tokens": 5}}
            return {"features": []}

    ledger = _ledger(tmp_path)
    client = RetryingClient()
    with pytest.raises(RuntimeError):
        _invoke(client, ledger=ledger, logical="retryable")
    assert _invoke(client, ledger=ledger, logical="retryable") == {"features": []}
    with ledger.database.connect() as connection:
        rows = connection.execute(
            "SELECT * FROM operational_physical_calls WHERE logical_call_id=? ORDER BY attempt_index",
            ("retryable",),
        ).fetchall()
    assert len(rows) == 2
    assert [row["attempt_index"] for row in rows] == [0, 1]
    assert rows[0]["usage_quality"] == "unknown"
    assert rows[1]["total_tokens"] == 15
    replay = ledger.finish_call(rows[1]["call_id"], status="succeeded", usage={"total_tokens": 15})
    assert replay["total_tokens"] == 15


def _feature_entity() -> dict[str, Any]:
    return {
        "window_start": "2026-06-22T10:00:00+08:00",
        "window_end": "2026-06-22T10:05:00+08:00",
        "cluster": "prod-a",
        "entity_type": "node",
        "entity_id": "node-a",
        "risk_score": 96,
        "risk_level": "critical",
        "affected_entities": ["pay-api-1"],
        "top_templates": [
            {
                "template_hash": "oom-hash",
                "component": "kernel",
                "severity": "ERROR",
                "template": "Memory cgroup out of memory Killed process <*>",
                "category": "node_memory_pressure",
                "count": 3,
                "first_seen": "2026-06-22T10:01:02+08:00",
                "last_seen": "2026-06-22T10:02:02+08:00",
                "feature_hint": "检查内存水位",
                "samples": ["SECRET RAW LOG"],
                "raw_sample": "SECRET RAW SAMPLE",
            }
        ],
    }


def _feature_payload() -> dict[str, Any]:
    feature = {
        "feature_type": "resource_pressure",
        "title": "节点内存耗尽",
        "summary": "内核 OOM 模板在窗口内重复出现",
        "importance": "critical",
        "template_hashes": ["oom-hash"],
        "components": ["kernel"],
        "tags": ["oom", "memory"],
        "selection_reason": "高风险资源压力信号",
    }
    return {"message": {"content": json.dumps({"features": [feature]}, ensure_ascii=False)}, "prompt_eval_count": 10, "eval_count": 4}


def test_feature_cache_hit_creates_no_physical_call_or_stale_usage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with loopback_responses([(200, _feature_payload())]) as (base_url, _requests):
        ledger = _ledger(tmp_path)
        monkeypatch.setattr("logrisk.feature_extractor_ollama.AI_CACHE", AICache(tmp_path / "cache.json"))
        trace_path = tmp_path / "traces.jsonl"
        monkeypatch.setattr("logrisk.feature_extractor_ollama.TRACE_LOGGER", AITraceLogger(trace_path))
        client = OllamaModelClient(base_url)
        first = extract_features_for_entity(
            _feature_entity(), model="qwen3:1.7b", model_client=client, job_id="job-1",
            ledger_repository=ledger, environment="local-test", scope_key="default",
        )
        second = extract_features_for_entity(
            _feature_entity(), model="qwen3:1.7b", model_client=client, job_id="job-1",
            ledger_repository=ledger, environment="local-test", scope_key="default",
        )
        assert first[0]["cache_hit"] is False
        assert second[0]["cache_hit"] is True
        with ledger.database.connect() as connection:
            assert connection.execute("SELECT COUNT(*) FROM operational_physical_calls").fetchone()[0] == 1
        traces = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
        assert traces[0]["usage"]["total_tokens"] == 14
        assert traces[1]["usage"] == {}
        assert "SECRET RAW" not in trace_path.read_text(encoding="utf-8")


class _FakeExtensionAdapter:
    descriptor = ExtensionDescriptor(
        adapter_id="operational_usage_extension",
        display_name="Operational Usage Fake",
        supported_output_modes=("json_schema",),
        credential_fields={},
        config_help="test",
    )

    def __init__(self, responses: list[Any]) -> None:
        self.responses = responses

    def validate_connection(self, _connection: Any) -> None:
        return None

    def check_connection(self, _connection: Any) -> dict[str, Any]:
        return {"online": True}

    def generate_content(self, _request: Any) -> Any:
        return self.responses.pop(0)


def test_extension_legacy_string_and_structured_response_usage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = _FakeExtensionAdapter(
        [
            '{"features": []}',
            ExtensionResponse(
                '{"features": []}',
                usage={
                    "prompt_tokens": 120,
                    "completion_tokens": 30,
                    "total_tokens": 130,
                    "prompt_tokens_details": {"cached_tokens": 40},
                    "completion_tokens_details": {"reasoning_tokens": 20},
                },
            ),
        ]
    )
    monkeypatch.setitem(ADAPTERS, "operational_usage_extension", adapter)
    client = ExtensionModelClient({"adapter_id": "operational_usage_extension", "credential_envs": {}})
    ledger = _ledger(tmp_path)
    assert _invoke(client, ledger=ledger, logical="extension") == {"features": []}
    assert client.last_metadata["usage_quality"] == "unknown"
    assert _invoke(client, ledger=ledger, logical="extension") == {"features": []}
    assert client.last_metadata["usage_quality"] == "known"
    with ledger.database.connect() as connection:
        rows = connection.execute(
            "SELECT * FROM operational_physical_calls WHERE logical_call_id=? ORDER BY attempt_index",
            ("extension",),
        ).fetchall()
    assert rows[0]["usage_quality"] == "unknown"
    assert rows[0]["metadata_contract"] == "legacy-single-invocation"
    assert rows[1]["total_tokens"] == 130


def test_agent_planner_and_tool_retries_are_separate_physical_rows(tmp_path: Path) -> None:
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    ledger = OperationalLedgerRepository(database)
    root = ledger.create_analysis_run(
        request_key="agent-usage",
        environment="local-test",
        scope_key="default",
        input_count=0,
        ranges=(),
    )

    class PlannerClient:
        provider = "fake"
        metadata_contract = "v1"

        def __init__(self) -> None:
            self.last_metadata: dict[str, Any] = {}

        def generate_json(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            self.last_metadata = {"usage": {"input_tokens": 8, "output_tokens": 4}}
            return {"goal": "read", "steps": [{"step_id": "read", "tool_name": "read", "arguments": {"id": "entity-1"}}]}

    planner = ModelAgentPlanner(
        PlannerClient(), model="planner-model", prompt_content="JSON", timeout=5,
        ledger_repository=ledger, analysis_run_id=root["analysis_run_id"], environment="local-test", scope_key="default",
        caller_id="agent-run-1",
    )
    plan = planner.plan(goal="read", evidence_summary={}, tool_descriptions=[{"name": "read"}], max_steps=1)
    assert plan.goal == "read"

    repository = AgentRepository(database)
    request = AgentRunRequest(
        source_job_id="job-1", entity_id="entity-1", entity_type="node", model_profile_id="profile",
        prompt_id="agent_plan_v1", max_steps=1, max_tool_calls=3, timeout_seconds=30,
        allowed_tools=("read",), idempotency_key="agent-usage-run", actor="alice", roles=("operator",), request_id="req-1",
    )
    calls = {"count": 0}
    tools = ToolRegistry()

    def read(_arguments: dict[str, Any], _context: Any) -> dict[str, Any]:
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("temporary tool failure")
        return {"ok": True}

    tools.register(name="read", description="read", required_arguments=("id",), handler=read)
    run = repository.create_run(request, locked_snapshot={"evidence_summary": {}})
    runtime = AgentRuntime(
        repository,
        FakeAgentPlanner(AgentPlan("read", (AgentStepPlan("read", "read", {"id": "entity-1"}),)),),
        tools,
        ledger_repository=ledger,
        analysis_run_id=root["analysis_run_id"],
        environment="local-test",
        scope_key="default",
    )
    assert runtime.execute(run["run_id"])["status"] == "awaiting_human"
    with database.connect() as connection:
        planner_rows = connection.execute(
            "SELECT * FROM operational_physical_calls WHERE caller_kind='agent_planner'"
        ).fetchall()
        tool_rows = connection.execute(
            "SELECT * FROM operational_physical_calls WHERE call_kind='agent_tool' ORDER BY attempt_index"
        ).fetchall()
    assert len(planner_rows) == 1
    assert planner_rows[0]["total_tokens"] == 12
    assert [row["status"] for row in tool_rows] == ["failed", "succeeded"]
    assert all(row["input_tokens"] is None and row["output_tokens"] is None for row in tool_rows)
    assert "SECRET" not in repr([dict(row) for row in planner_rows + tool_rows])
