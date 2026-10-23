from __future__ import annotations

import copy
import time
from dataclasses import replace
from typing import Any, Callable

from logrisk.ai_harness.evaluator import EVALUATOR_VERSION
from logrisk.ai_harness.usage_accounting import run_agent_tool_attempt, usage_context

from .artifacts import READ_ARTIFACT_TYPES, canonical_fingerprint, read_tool_artifact
from .errors import AgenticError
from .models import AgentStepPlan, validate_evidence_scope
from .evidence_context import (EXPERT_ROLES, expert_goal, build_expert_context, completed_upstream, expert_rule_bindings, validate_assessment, validate_material)
from .planner import AgentPlanner
from .repository import AgentRepository
from .tool_registry import AgentToolContext, ToolRegistry


def _fingerprint(feature: Any) -> str:
    return canonical_fingerprint(feature)


def _evaluation_fingerprint(output: dict[str, Any], evidence_hash: str | None) -> str | None:
    feature = output.get("feature")
    fingerprint = _fingerprint(feature) if isinstance(feature, dict) else None
    if (
        output.get("passed") is True and fingerprint
        and output.get("fingerprint") == fingerprint
        and output.get("evidence_hash") == evidence_hash
        and output.get("evaluator_version") == EVALUATOR_VERSION
    ):
        return fingerprint
    return None


class AgentRuntime:
    def __init__(
        self,
        repository: AgentRepository,
        planner: AgentPlanner | Callable[[dict[str, Any]], AgentPlanner],
        tools: ToolRegistry,
        *,
        monotonic=time.monotonic,
        ledger_repository: Any | None = None,
        analysis_run_id: str | None = None,
        environment: str | None = None,
        scope_key: str | None = None,
    ) -> None:
        self.repository = repository
        self.planner = planner
        self.tools = tools
        self.monotonic = monotonic
        self.ledger_repository = ledger_repository
        self.analysis_run_id = analysis_run_id
        self.environment = environment
        self.scope_key = scope_key

    def _usage_context(self, run: dict[str, Any]) -> dict[str, Any]:
        return {
            "ledger_repository": self.ledger_repository,
            "analysis_run_id": run.get("analysis_run_id") or self.analysis_run_id,
            "environment": run.get("environment") or self.environment or "production",
            "scope_key": run.get("scope_key") or self.scope_key or "default",
        }

    @staticmethod
    def _planner_evidence_summary(run: dict[str, Any]) -> dict[str, Any]:
        source_job_id, entity_id = run.get("source_job_id"), run.get("entity_id")
        snapshot = run.get("locked_snapshot") or {}
        evidence_summary = snapshot.get("evidence_summary")
        validate_evidence_scope(
            evidence_summary, source_job_id, entity_id, code="agent_scope_invalid",
        )
        result = dict(evidence_summary)
        # Only the model input is augmented. The persisted snapshot is immutable.
        result["source_job_id"] = source_job_id
        result["entity_id"] = entity_id
        return result

    def _check_active(self, run_id: str, started: float) -> dict[str, Any]:
        run = self.repository.get_run(run_id)
        parent = run.get("locked_snapshot",{}).get("workflow_run_id")
        if parent and run["status"] in {"queued","planning","running"}:
            with self.repository.database.connect() as connection:
                row = connection.execute("SELECT status FROM agent_workflow_runs WHERE workflow_run_id=?",(parent,)).fetchone()
            if row and row["status"] in {"cancelled","paused"}:
                run = self.repository.transition(run_id,row["status"],allowed_from={"queued","planning","running"})
        if run["status"] in {"cancelled", "paused", "failed"}:
            raise AgenticError("专家运行已中断", code="agent_interrupted")
        if self.monotonic() - started > float(run["timeout_seconds"]):
            raise AgenticError("专家读取/模型阶段超时", code="agent_timeout")
        return run

    def _expert_context(self, run: dict[str, Any], context: AgentToolContext, started: float) -> dict[str, Any]:
        reads = []
        def read(tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
            latest = self._check_active(run["run_id"], started)
            tool = self.tools.get(tool_name)
            if tool_name not in context.allowed_tools:
                raise AgenticError("专家缺少必要只读工具权限", code="tool_not_allowed")
            if int(latest["used_tool_calls"]) + tool.cost_units > int(latest["max_tool_calls"]):
                raise AgenticError("专家前置工具预算耗尽", code="agent_budget_exhausted")
            step = AgentStepPlan("context-" + str(len(reads)), tool_name, arguments)
            self.repository.add_context_step(run["run_id"], step)
            self.repository.start_step(run["run_id"], step.step_id)
            began = self.monotonic()
            self.repository.append_event(run["run_id"], "tool_call_started", {"step_id": step.step_id, "tool_name": tool_name, "phase": "model_context"})
            try:
                output = run_agent_tool_attempt(lambda: self.tools.execute(tool_name, arguments, context), **self._usage_context(latest), logical_call_id=f"agent_context:{run['run_id']}:{step.step_id}", attempt_index=0, tool_name=tool_name, caller_kind="agent_runtime", caller_id=run["run_id"])
            except Exception as exc:
                self.repository.record_tool_call(run["run_id"], step.step_id, tool_name, arguments, status="failed", idempotency_key=f"context:{step.step_id}", cost_units=tool.cost_units, error_code=getattr(exc,"code","tool_failed"), error_summary="专家前置工具失败")
                self.repository.finish_step(run["run_id"], step.step_id, status="failed", error_code=getattr(exc,"code","tool_failed"), error_summary="专家前置工具失败")
                raise
            call = self.repository.record_tool_call(run["run_id"], step.step_id, tool_name, arguments, status="completed", idempotency_key=f"context:{step.step_id}", cost_units=tool.cost_units, result=output, latency_ms=int((self.monotonic()-began)*1000))
            payload = read_tool_artifact(run_id=run["run_id"], source_job_id=context.source_job_id, entity_id=context.entity_id, tool_name=tool_name, tool_call_id=call["tool_call_id"], evidence_hash=str(context.evidence_hash), output=output)
            self.repository.add_artifact(run["run_id"], READ_ARTIFACT_TYPES[tool_name], payload, step_id=step.step_id)
            self.repository.finish_step(run["run_id"], step.step_id, status="completed", result_summary=output)
            self.repository.append_event(run["run_id"], "tool_call_completed", {"step_id": step.step_id, "tool_name": tool_name, "phase": "model_context"})
            reads.append({"tool_call_id":call["tool_call_id"],"tool_name":tool_name,"result_hash":_fingerprint(output)})
            self._check_active(run["run_id"], started)
            return output
        evidence = read("get_sanitized_evidence", {"job_id":context.source_job_id,"entity_id":context.entity_id})
        validate_material(evidence, context.source_job_id, context.entity_id, expected_cluster=context.cluster)
        rules = {"items":[],"total":0,"matched":0,"truncated":False}
        if run["locked_snapshot"]["workflow_role_id"] in {"rule_specialist","feature_specialist"}:
            rules = read("find_approved_rules", {"template_hashes":sorted({item["template_hash"] for item in evidence["templates"]}),"components":sorted({str(item["component"]) for item in evidence["templates"] if item.get("component")})})
            if rules.get("truncated"):
                raise AgenticError("批准规则结果不完整，不能据预览分析", code="agent_rule_context_truncated")
        upstream = completed_upstream(run, self.repository, evidence, str(context.evidence_hash))
        return build_expert_context(run,evidence,rules,reads,upstream,str(context.evidence_hash))

    def execute(self, run_id: str) -> dict[str, Any]:
        run = self.repository.get_run(run_id)
        if run["status"] in {"cancelled", "completed", "awaiting_human", "failed"}:
            return run
        started = self.monotonic()
        try:
            planner_evidence_summary = self._planner_evidence_summary(run)
            snapshot = run["locked_snapshot"]
            summary = snapshot.get("evidence_summary") or {}
            context = AgentToolContext(run_id=run_id,source_job_id=run["source_job_id"],entity_id=run["entity_id"],allowed_tools=frozenset(run["allowed_tools"]),actor=run["actor"],request_id=run["request_id"],evidence_hash=summary.get("locked_evidence_hash"),cluster=summary.get("locked_evidence_cluster"))
            evidence_hash = self.tools.current_evidence_hash(context)
            context = replace(context,evidence_hash=evidence_hash or _fingerprint(summary))
            planner = self.planner(run) if callable(self.planner) and not hasattr(self.planner,"plan") else self.planner
            expert = snapshot.get("workflow_role_id") in EXPERT_ROLES and getattr(planner,"requires_evidence_context",False)
            planned = any(event["type"] == "plan_created" for event in run["events"])
            if expert and not planned and run["status"] not in {"queued", "planning"}:
                raise AgenticError("规划未结算，禁止直接执行前置步骤冒充完成", code="agent_plan_indeterminate")
            if run["status"] == "queued" or (run["status"] == "planning" and (not planned if expert else not run["steps"])):
                if run["status"] == "queued":
                    run = self.repository.transition(run_id, "planning", allowed_from={"queued"})
                snapshot = run["locked_snapshot"]
                model_context = None
                if expert:
                    if not self.repository.reserve_model_planning(run_id):
                        return self.repository.get_run(run_id)
                    model_context = self._expert_context(run,context,started)
                    bindings = expert_rule_bindings(model_context)
                    context = replace(context, rule_bindings=bindings)
                    self.tools.validate_rule_bindings(bindings, context)
                    planner_evidence_summary = {key:value for key,value in planner_evidence_summary.items() if key != "dependency_artifacts"}
                    planner_evidence_summary["expert_context"] = model_context
                    latest = self._check_active(run_id,started)
                    remaining_steps = int(latest["max_steps"]) - len(latest["steps"])
                    planner = copy.copy(planner)
                    planner.timeout = min(planner.timeout,max(.001,float(latest["timeout_seconds"])-(self.monotonic()-started)))
                else:
                    remaining_steps = int(run["max_steps"])
                with usage_context(**self._usage_context(run)):
                    plan = planner.plan(
                        goal=expert_goal(model_context["role_id"], snapshot.get("goal")) if expert else str(snapshot.get("goal") or "提取可审批日志特征"),
                        evidence_summary=planner_evidence_summary,
                        tool_descriptions=self.tools.describe(frozenset(run["allowed_tools"])),
                        max_steps=remaining_steps,
                    )
                if expert:
                    self._check_active(run_id,started)
                    self.tools.current_evidence_hash(context)
                    assessment = validate_assessment(plan.assessment,model_context,plan.steps)
                    self.tools.validate_rule_bindings(context.rule_bindings, context)
                run = self.repository.replace_plan(run_id,plan,preserve_context=expert)
                if expert:
                    self.repository.add_artifact(run_id,"expert_assessment_v1",{"schema_version":"expert_assessment_v1","origin":"model","scope":model_context["scope"],"context_hash":model_context["context_hash"],"assessment":assessment,"model_response_hash":_fingerprint(plan.model_output),"model_response":plan.model_output,"rule_bindings":list(context.rule_bindings)}, precondition=lambda connection: self.tools.validate_rule_bindings(context.rule_bindings, context, connection=connection))
                self.repository.append_event(run_id,"plan_created",{"step_count":len(run["steps"]),"model_context_hash":model_context["context_hash"] if expert else None})
            if expert:
                opinions = [item for item in run["artifacts"] if item["artifact_type"] == "expert_assessment_v1"]
                # replace_plan returns before add_artifact; reload the persisted binding.
                if not opinions:
                    run = self.repository.get_run(run_id)
                    opinions = [item for item in run["artifacts"] if item["artifact_type"] == "expert_assessment_v1"]
                if len(opinions) != 1:
                    raise AgenticError("缺少唯一已校验专家意见", code="agent_rule_reference_changed")
                payload = opinions[0]["payload"]
                bindings = tuple(copy.deepcopy(payload.get("rule_bindings") or []))
                if set(payload["assessment"]["rule_refs"]) - {item["reference"] for item in bindings}:
                    raise AgenticError("历史专家意见缺少规则绑定", code="agent_rule_reference_changed")
                context = replace(context, rule_bindings=bindings)
                self.tools.validate_rule_bindings(bindings, context)
            if run["status"] != "running":
                run = self.repository.transition(run_id, "running", allowed_from={"planning", "paused"})
            passed_evaluations: set[str] = set()
            for item in run["artifacts"]:
                if item["artifact_type"] != "evaluation" or not item["payload"].get("passed"):
                    continue
                fingerprint = _evaluation_fingerprint(item["payload"], context.evidence_hash)
                if fingerprint and item.get("fingerprint") == fingerprint:
                    passed_evaluations.add(fingerprint)
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
                if tool.writes_candidate:
                    self.tools.current_evidence_hash(context)
                    self.tools.validate_rule_bindings(context.rule_bindings, context)
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
                        usage_args = self._usage_context(latest)
                        output = run_agent_tool_attempt(
                            lambda: self.tools.execute(step["tool_name"], step["arguments"], context),
                            **usage_args,
                            logical_call_id=f"agent_tool:{run_id}:{step['step_id']}:{started_step['attempt']}",
                            attempt_index=retry_index,
                            tool_name=step["tool_name"],
                            caller_kind="agent_runtime",
                            caller_id=run_id,
                        )
                        break
                    except Exception as exc:
                        code = getattr(exc, "code", "tool_failed")
                        self.repository.record_tool_call(
                            run_id, step["step_id"], step["tool_name"], step["arguments"], status="failed",
                            idempotency_key=call_key, cost_units=tool.cost_units, latency_ms=int((self.monotonic() - call_started) * 1000),
                            error_code=code, error_summary="Agent 工具执行失败",
                        )
                        self.repository.append_event(run_id, "tool_call_failed", {"step_id": step["step_id"], "tool_name": step["tool_name"], "error_code": code, "attempt": retry_index + 1})
                        if retry_index == 0 and code != "agent_rule_reference_changed":
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
                call = self.repository.record_tool_call(
                    run_id, step["step_id"], step["tool_name"], step["arguments"], status="completed",
                    idempotency_key=call_key, cost_units=tool.cost_units, result=output,
                    latency_ms=int((self.monotonic() - call_started) * 1000),
                )
                if step["tool_name"] == "evaluate_candidate":
                    evaluated_fingerprint = _evaluation_fingerprint(output, context.evidence_hash)
                    if fingerprint and evaluated_fingerprint == fingerprint:
                        passed_evaluations.add(fingerprint)
                    self.repository.add_artifact(
                        run_id, "evaluation", output, step_id=step["step_id"],
                        fingerprint=fingerprint if evaluated_fingerprint == fingerprint else None,
                    )
                elif tool.writes_candidate:
                    self.repository.add_artifact(run_id, "candidate", output, step_id=step["step_id"], fingerprint=fingerprint)
                elif step["tool_name"] in READ_ARTIFACT_TYPES:
                    payload = read_tool_artifact(
                        run_id=run_id, source_job_id=context.source_job_id, entity_id=context.entity_id,
                        tool_name=step["tool_name"], tool_call_id=call["tool_call_id"],
                        evidence_hash=str(context.evidence_hash), output=output,
                    )
                    self.repository.add_artifact(run_id, READ_ARTIFACT_TYPES[step["tool_name"]], payload, step_id=step["step_id"])
                self.repository.finish_step(run_id, step["step_id"], status="completed", result_summary=output)
                self.repository.append_event(run_id, "tool_call_completed", {"step_id": step["step_id"], "tool_name": step["tool_name"]})
            return self.repository.transition(run_id, "awaiting_human", allowed_from={"running"})
        except Exception as exc:
            if getattr(exc,"code",None) == "agent_interrupted":
                return self.repository.get_run(run_id)
            code = getattr(exc, "code", "agent_run_failed")
            summary = str(exc)[:300] if isinstance(exc, AgenticError) else "Agent Run 执行失败"
            try:
                return self.repository.transition(
                    run_id, "failed", allowed_from={"queued", "planning", "running", "paused"},
                    error_code=code, error_summary=summary,
                )
            except AgenticError:
                return self.repository.get_run(run_id)
