from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from .errors import AgenticError
from .workflow_repository import WorkflowRepository
from .workflow_worker import WorkflowWorker


class WorkflowScheduler:
    def __init__(self, repository: WorkflowRepository, worker: WorkflowWorker, *, monotonic=time.monotonic) -> None:
        self.repository = repository
        self.worker = worker
        self.monotonic = monotonic

    def execute(self, run_id: str) -> dict[str, Any]:
        run = self.repository.get_run(run_id)
        if run["status"] in {"paused", "cancelled", "awaiting_human", "completed"}:
            return run
        started = self.monotonic()
        try:
            if run["status"] == "queued":
                run = self.repository.transition_run(run_id, "running", allowed_from={"queued"})
            while True:
                run = self.repository.get_run(run_id)
                if run["status"] in {"paused", "cancelled"}:
                    return run
                if self.monotonic() - started > float(run["timeout_seconds"]):
                    raise AgenticError("Agent 工作流超时", code="workflow_timeout")
                pending = [node for node in run["nodes"] if node["status"] == "pending"]
                exhausted_pending = [node for node in pending if int(node["attempt"]) >= int(run["max_attempts"])]
                if exhausted_pending:
                    raise AgenticError("Agent 工作流节点重试次数已耗尽", code="workflow_retry_exhausted")
                terminal_failed = [node for node in run["nodes"] if node["status"] == "failed" and int(node["attempt"]) >= int(run["max_attempts"])]
                if terminal_failed:
                    failed = terminal_failed[0]
                    raise AgenticError("Agent 工作流节点失败", code=str(failed.get("error_code") or "workflow_node_failed"))
                if not pending:
                    failed = [node for node in run["nodes"] if node["status"] == "failed"]
                    if failed:
                        raise AgenticError("Agent 工作流节点失败", code=str(failed[0].get("error_code") or "workflow_node_failed"))
                    return self.repository.transition_run(run_id, "awaiting_human", allowed_from={"running"})
                ready = self.repository.ready_nodes(run_id)[: int(run["max_concurrency"])]
                if not ready:
                    raise AgenticError("Agent 工作流依赖无法推进", code="workflow_dependency_blocked")
                remaining_budget = int(run["max_tool_calls"]) - int(run["used_tool_calls"])
                admitted: list[dict[str, Any]] = []
                reserved = 0
                for node in ready:
                    if reserved + int(node["max_tool_calls"]) <= remaining_budget:
                        admitted.append(node)
                        reserved += int(node["max_tool_calls"])
                if not admitted:
                    raise AgenticError("Agent 工作流工具预算已耗尽", code="workflow_budget_exhausted")
                for node in admitted:
                    self.repository.claim_node(run_id, node["node_id"])
                with ThreadPoolExecutor(max_workers=len(admitted), thread_name_prefix="logrisk-agent-workflow") as pool:
                    futures = {pool.submit(self.worker.execute_node, run_id, node["node_id"]): node["node_id"] for node in admitted}
                    for future in as_completed(futures):
                        node_id = futures[future]
                        try:
                            result = future.result()
                        except Exception as exc:
                            result = {"status": "failed", "used_tool_calls": 0, "error_code": getattr(exc, "code", "workflow_node_failed"), "artifacts": []}
                        current_run = self.repository.get_run(run_id)
                        if current_run["status"] == "paused" or result["status"] == "paused":
                            self.repository.requeue_node(run_id, node_id, reason="workflow_paused")
                            continue
                        if current_run["status"] == "cancelled" or result["status"] == "cancelled":
                            self.repository.finish_node(run_id, node_id, status="cancelled", used_tool_calls=0, error_code="workflow_cancelled", error_summary="工作流在节点执行期间被取消")
                            continue
                        used_tool_calls = int(result.get("used_tool_calls") or 0)
                        node_limit = int(next(item for item in current_run["nodes"] if item["node_id"] == node_id)["max_tool_calls"])
                        if used_tool_calls < 0 or used_tool_calls > node_limit or int(current_run["used_tool_calls"]) + used_tool_calls > int(current_run["max_tool_calls"]):
                            self.repository.finish_node(run_id, node_id, status="failed", used_tool_calls=0, error_code="workflow_budget_exhausted", error_summary="节点工具调用超过工作流预算")
                            raise AgenticError("Agent 工作流工具预算已耗尽", code="workflow_budget_exhausted")
                        if result["status"] in {"awaiting_human", "completed"}:
                            self.repository.finish_node(run_id, node_id, status="completed", used_tool_calls=used_tool_calls, result={"child_agent_run_id": result.get("child_agent_run_id"), "artifacts": result["artifacts"]})
                            continue
                        node = next(item for item in self.repository.get_run(run_id)["nodes"] if item["node_id"] == node_id)
                        self.repository.finish_node(run_id, node_id, status="failed", used_tool_calls=used_tool_calls, error_code=str(result.get("error_code") or "workflow_node_failed"), error_summary="受控角色节点执行失败")
                        if int(node["attempt"]) < int(run["max_attempts"]):
                            self.repository.reset_node(run_id, node_id)
        except Exception as exc:
            code = str(getattr(exc, "code", "workflow_run_failed"))
            try:
                return self.repository.transition_run(run_id, "failed", allowed_from={"queued", "running", "paused"}, error_code=code, error_summary="Agent 工作流执行失败")
            except AgenticError:
                return self.repository.get_run(run_id)
