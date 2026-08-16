from __future__ import annotations

from typing import Any

from .errors import AgenticError
from .models import AgentRunRequest
from .tool_registry import _reject_sensitive
from .workflow_repository import WorkflowRepository


class WorkflowWorker:
    def __init__(self, repository: WorkflowRepository, agent_service: Any) -> None:
        self.repository = repository
        self.agent_service = agent_service

    def execute_node(self, workflow_run_id: str, node_id: str) -> dict[str, Any]:
        run = self.repository.get_run(workflow_run_id)
        node = next(item for item in run["nodes"] if item["node_id"] == node_id)
        attempt = int(node["attempt"])
        key = f"workflow:{workflow_run_id}:{node_id}:{attempt}"
        dependency_artifacts = {
            item["node_id"]: dict(item.get("result_summary") or {})
            for item in run["nodes"] if item["node_id"] in node["dependencies"]
        }
        evidence_summary = dict(run["locked_snapshot"].get("evidence_summary") or {})
        evidence_summary["dependency_artifacts"] = dependency_artifacts
        _reject_sensitive(evidence_summary)
        runtime_snapshot = run["locked_snapshot"]
        runtime_keys = ("profile_snapshot", "connection_snapshot", "prompt_id", "prompt_sha256")
        missing_runtime = [key for key in runtime_keys if not runtime_snapshot.get(key)]
        if missing_runtime:
            raise AgenticError(
                f"工作流 Run 缺少锁定模型运行快照: {', '.join(missing_runtime)}",
                code="workflow_runtime_snapshot_invalid",
            )
        locked = {
            "schema_version": "1.0",
            "goal": f"以{node['role_id']}角色处理当前风险实体",
            "evidence_summary": evidence_summary,
            "workflow_run_id": workflow_run_id,
            "workflow_node_id": node_id,
            "workflow_role_id": node["role_id"],
            "dependency_nodes": list(node["dependencies"]),
            "dependency_artifact_refs": [
                {"node_id": item["node_id"], "child_agent_run_id": item["child_agent_run_id"]}
                for item in run["nodes"] if item["node_id"] in node["dependencies"]
            ],
        }
        locked.update({key: runtime_snapshot[key] for key in runtime_keys})
        child = self.agent_service.create_run(AgentRunRequest(
            source_job_id=run["source_job_id"], entity_id=run["entity_id"], entity_type=run["entity_type"],
            model_profile_id=run["model_profile_id"], prompt_id=run["prompt_id"],
            max_steps=node["max_steps"], max_tool_calls=node["max_tool_calls"], timeout_seconds=node["timeout_seconds"],
            allowed_tools=tuple(node["allowed_tools"]), idempotency_key=key, actor=run["actor"],
            roles=tuple(run["roles"]), request_id=run["request_id"],
        ), locked_snapshot=locked)
        self.repository.attach_child_run(workflow_run_id, node_id, str(child["run_id"]))
        result = self.agent_service.execute_run(str(child["run_id"]))
        return {
            "status": str(result["status"]), "child_agent_run_id": str(child["run_id"]),
            "used_tool_calls": int(result.get("used_tool_calls") or 0),
            "artifacts": list(result.get("artifacts") or []), "error_code": result.get("error_code"),
        }

    def pause_active(self, run: dict[str, Any], key: str) -> None:
        for node in run["nodes"]:
            if node["status"] == "running" and node.get("child_agent_run_id"):
                self.agent_service.pause(node["child_agent_run_id"], idempotency_key=f"{key}:{node['node_id']}")

    def cancel_active(self, run: dict[str, Any], key: str) -> None:
        for node in run["nodes"]:
            if node["status"] == "running" and node.get("child_agent_run_id"):
                self.agent_service.cancel(node["child_agent_run_id"], idempotency_key=f"{key}:{node['node_id']}")
