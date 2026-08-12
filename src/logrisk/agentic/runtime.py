from __future__ import annotations

import hashlib
import json
import time
from typing import Any, Callable

from .errors import AgenticError
from .planner import AgentPlanner
from .repository import AgentRepository
from .tool_registry import AgentToolContext, ToolRegistry


def _fingerprint(feature: Any) -> str:
    return hashlib.sha256(json.dumps(feature, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class AgentRuntime:
    def __init__(self, repository: AgentRepository, planner: AgentPlanner | Callable[[dict[str, Any]], AgentPlanner], tools: ToolRegistry, *, monotonic=time.monotonic) -> None:
        self.repository = repository
        self.planner = planner
        self.tools = tools
        self.monotonic = monotonic

    def execute(self, run_id: str) -> dict[str, Any]:
        run = self.repository.get_run(run_id)
        if run["status"] == "cancelled":
            return run
        started = self.monotonic()
        try:
            if run["status"] == "queued" or (run["status"] == "planning" and not run["steps"]):
                if run["status"] == "queued":
                    run = self.repository.transition(run_id, "planning", allowed_from={"queued"})
                snapshot = run["locked_snapshot"]
                planner = self.planner(run) if callable(self.planner) and not hasattr(self.planner, "plan") else self.planner
                plan = planner.plan(
                    goal=str(snapshot.get("goal") or "提取可审批日志特征"),
                    evidence_summary=dict(snapshot.get("evidence_summary") or {}),
                    tool_descriptions=self.tools.describe(frozenset(run["allowed_tools"])),
                    max_steps=int(run["max_steps"]),
                )
                run = self.repository.replace_plan(run_id, plan)
                self.repository.append_event(run_id, "plan_created", {"step_count": len(plan.steps)})
            if run["status"] != "running":
                run = self.repository.transition(run_id, "running", allowed_from={"planning", "paused"})
            context = AgentToolContext(
                run_id=run_id, source_job_id=run["source_job_id"], entity_id=run["entity_id"],
                allowed_tools=frozenset(run["allowed_tools"]), actor=run["actor"], request_id=run["request_id"],
            )
            passed_evaluations: set[str] = set()
            for item in run["artifacts"]:
                if item["artifact_type"] != "evaluation" or not item["payload"].get("passed"):
                    continue
                evaluated = item["payload"].get("feature")
                if evaluated is not None and item.get("fingerprint") == _fingerprint(evaluated):
                    passed_evaluations.add(str(item["fingerprint"]))
            for step in run["steps"]:
                current = self.repository.get_run(run_id)
                if current["status"] in {"paused", "cancelled"}:
                    return current
                if step["status"] == "completed":
                    continue
                if self.monotonic() - started > float(current["timeout_seconds"]):
                    raise AgenticError("Agent Run 超时", code="agent_timeout")
                tool = self.tools.get(step["tool_name"])
                if int(current["used_tool_calls"]) + tool.cost_units > int(current["max_tool_calls"]):
                    raise AgenticError("Agent 工具调用预算已耗尽", code="agent_budget_exhausted")
                feature = step["arguments"].get("feature")
                fingerprint = _fingerprint(feature) if feature is not None else None
                if tool.writes_candidate and fingerprint not in passed_evaluations:
                    raise AgenticError("Candidate 必须先通过确定性 Evaluator", code="human_gate_bypass")
                started_step = self.repository.start_step(run_id, step["step_id"])
                output: dict[str, Any] | None = None
                for retry_index in range(2):
                    latest = self.repository.get_run(run_id)
                    if int(latest["used_tool_calls"]) + tool.cost_units > int(latest["max_tool_calls"]):
                        self.repository.finish_step(
                            run_id, step["step_id"], status="failed",
                            error_code="agent_budget_exhausted", error_summary="Agent 工具调用预算已耗尽",
                        )
                        raise AgenticError("Agent 工具调用预算已耗尽", code="agent_budget_exhausted")
                    if retry_index and self.monotonic() - started > float(latest["timeout_seconds"]):
                        self.repository.finish_step(
                            run_id, step["step_id"], status="failed",
                            error_code="agent_timeout", error_summary="Agent Run 超时",
                        )
                        raise AgenticError("Agent Run 超时", code="agent_timeout")
                    call_key = f"{run_id}:{step['step_id']}:{started_step['attempt']}:{retry_index}"
                    call_started = self.monotonic()
                    self.repository.append_event(run_id, "tool_call_started", {"step_id": step["step_id"], "tool_name": step["tool_name"], "attempt": retry_index + 1})
                    try:
                        output = self.tools.execute(step["tool_name"], step["arguments"], context)
                        break
                    except Exception as exc:
                        code = getattr(exc, "code", "tool_failed")
                        self.repository.record_tool_call(
                            run_id, step["step_id"], step["tool_name"], step["arguments"], status="failed",
                            idempotency_key=call_key, cost_units=tool.cost_units, latency_ms=int((self.monotonic() - call_started) * 1000),
                            error_code=code, error_summary="Agent 工具执行失败",
                        )
                        self.repository.append_event(run_id, "tool_call_failed", {"step_id": step["step_id"], "tool_name": step["tool_name"], "error_code": code, "attempt": retry_index + 1})
                        if retry_index == 0:
                            self.repository.append_event(run_id, "tool_call_retrying", {"step_id": step["step_id"], "tool_name": step["tool_name"]})
                            continue
                        self.repository.finish_step(run_id, step["step_id"], status="failed", error_code=code, error_summary="Agent 工具执行失败")
                        raise
                assert output is not None
                if self.monotonic() - started > float(current["timeout_seconds"]):
                    self.repository.record_tool_call(
                        run_id, step["step_id"], step["tool_name"], step["arguments"], status="failed",
                        idempotency_key=call_key, cost_units=tool.cost_units, latency_ms=int((self.monotonic() - call_started) * 1000),
                        error_code="agent_timeout", error_summary="Agent Run 超时",
                    )
                    self.repository.finish_step(run_id, step["step_id"], status="failed", error_code="agent_timeout", error_summary="Agent Run 超时")
                    self.repository.append_event(run_id, "tool_call_failed", {"step_id": step["step_id"], "tool_name": step["tool_name"], "error_code": "agent_timeout"})
                    raise AgenticError("Agent Run 超时", code="agent_timeout")
                self.repository.record_tool_call(
                    run_id, step["step_id"], step["tool_name"], step["arguments"], status="completed",
                    idempotency_key=call_key, cost_units=tool.cost_units, result=output,
                    latency_ms=int((self.monotonic() - call_started) * 1000),
                )
                self.repository.finish_step(run_id, step["step_id"], status="completed", result_summary=output)
                self.repository.append_event(run_id, "tool_call_completed", {"step_id": step["step_id"], "tool_name": step["tool_name"]})
                if step["tool_name"] == "evaluate_candidate":
                    evaluated = output.get("feature")
                    evaluated_fingerprint = _fingerprint(evaluated) if evaluated is not None else None
                    if output.get("passed") and fingerprint and evaluated_fingerprint == fingerprint:
                        passed_evaluations.add(fingerprint)
                    self.repository.add_artifact(
                        run_id, "evaluation", output, step_id=step["step_id"],
                        fingerprint=fingerprint if evaluated_fingerprint == fingerprint else None,
                    )
                elif tool.writes_candidate:
                    self.repository.add_artifact(run_id, "candidate", output, step_id=step["step_id"], fingerprint=fingerprint)
            return self.repository.transition(run_id, "awaiting_human", allowed_from={"running"})
        except Exception as exc:
            code = getattr(exc, "code", "agent_run_failed")
            summary = str(exc)[:300] if isinstance(exc, AgenticError) else "Agent Run 执行失败"
            try:
                return self.repository.transition(
                    run_id, "failed", allowed_from={"queued", "planning", "running", "paused"},
                    error_code=code, error_summary=summary,
                )
            except AgenticError:
                return self.repository.get_run(run_id)
