from __future__ import annotations

import json
import uuid
from typing import Any

from logrisk.database import Database, utc_now

from .errors import AgenticError
from .tool_registry import _reject_sensitive
from .workflow_models import CompiledWorkflow


RUNTIME_SNAPSHOT_KEYS = frozenset({"profile_snapshot", "connection_snapshot", "prompt_id", "prompt_sha256"})


def _db_json(database: Database, value: Any) -> Any:
    return value if getattr(database, "provider", "sqlite") == "postgres" else json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _decode(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return default


def _definition(workflow: CompiledWorkflow) -> dict[str, Any]:
    return {
        "schema_version": workflow.schema_version, "name": workflow.name, "description": workflow.description,
        "nodes": [{
            "node_id": node.node_id, "role_id": node.role_id, "depends_on": list(node.depends_on),
            "allowed_tools": list(node.allowed_tools), "max_steps": node.max_steps,
            "max_tool_calls": node.max_tool_calls, "timeout_seconds": node.timeout_seconds,
        } for node in workflow.nodes],
        "budget": {
            "max_nodes": workflow.budget.max_nodes, "max_concurrency": workflow.budget.max_concurrency,
            "max_tool_calls": workflow.budget.max_tool_calls, "timeout_seconds": workflow.budget.timeout_seconds,
        },
        "retry_policy": {"max_attempts": workflow.max_attempts},
        "topological_order": list(workflow.topological_order),
        "parallel_layers": [list(layer) for layer in workflow.parallel_layers],
    }


def validate_runtime_snapshot(
    value: dict[str, Any] | None,
    *,
    model_profile_id: str,
    prompt_id: str,
) -> dict[str, Any]:
    """Validate the immutable model inputs required by every workflow Run."""
    if not isinstance(value, dict) or set(value) != RUNTIME_SNAPSHOT_KEYS:
        raise AgenticError("工作流必须锁定 Profile、连接和 Prompt 快照", code="workflow_runtime_snapshot_invalid")
    _reject_sensitive(value)
    profile, connection = value.get("profile_snapshot"), value.get("connection_snapshot")
    if not isinstance(profile, dict) or not isinstance(connection, dict):
        raise AgenticError("工作流模型运行快照结构无效", code="workflow_runtime_snapshot_invalid")
    if str(profile.get("profile_id") or "") != str(model_profile_id):
        raise AgenticError("工作流 Profile 快照与任务不一致", code="workflow_runtime_snapshot_invalid")
    if str(value.get("prompt_id") or "") != str(prompt_id):
        raise AgenticError("工作流 Prompt 快照与任务不一致", code="workflow_runtime_snapshot_invalid")
    if str(profile.get("connection_id") or "") != str(connection.get("connection_id") or ""):
        raise AgenticError("工作流连接快照与 Profile 不一致", code="workflow_runtime_snapshot_invalid")
    if not bool(profile.get("enabled", True)):
        raise AgenticError("模型 Profile 已停用", code="workflow_profile_unavailable", status_code=409)
    if not bool(connection.get("enabled")):
        raise AgenticError("模型连接已停用", code="workflow_connection_unavailable", status_code=409)
    provider = str(connection.get("provider") or "")
    if provider == "openai_compatible" and not bool(connection.get("api_key_configured")):
        raise AgenticError("远端模型连接缺少 API Key 环境变量", code="workflow_connection_unavailable", status_code=409)
    if provider == "extension" and not all(dict(connection.get("credential_envs_configured") or {}).values()):
        raise AgenticError("扩展模型连接缺少所需凭据环境变量", code="workflow_connection_unavailable", status_code=409)
    if not str(value.get("prompt_sha256") or "").strip():
        raise AgenticError("工作流 Prompt 哈希不能为空", code="workflow_runtime_snapshot_invalid")
    return {key: value[key] for key in RUNTIME_SNAPSHOT_KEYS}


class WorkflowRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    def create_workflow(self, workflow: CompiledWorkflow, *, actor: str, idempotency_key: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute("SELECT workflow_id FROM agent_workflows WHERE actor=? AND idempotency_key=?", (actor, idempotency_key)).fetchone()
        if row:
            return {**self.get_workflow(str(row["workflow_id"])), "idempotent_replay": True}
        workflow_id, now, definition = uuid.uuid4().hex, utc_now(), _definition(workflow)
        try:
            with self.database.transaction() as connection:
                connection.execute(
                    "INSERT INTO agent_workflows(workflow_id,name,description,definition_json,actor,idempotency_key,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)",
                    (workflow_id, workflow.name, workflow.description, _db_json(self.database, definition), actor, idempotency_key, now, now),
                )
        except Exception:
            with self.database.connect() as connection:
                row = connection.execute("SELECT workflow_id FROM agent_workflows WHERE actor=? AND idempotency_key=?", (actor, idempotency_key)).fetchone()
            if row:
                return {**self.get_workflow(str(row["workflow_id"])), "idempotent_replay": True}
            raise
        return {**self.get_workflow(workflow_id), "idempotent_replay": False}

    def get_workflow(self, workflow_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM agent_workflows WHERE workflow_id=?", (workflow_id,)).fetchone()
        if not row:
            raise AgenticError("Agent 工作流不存在", code="workflow_not_found", status_code=404)
        value = dict(row)
        value["definition"] = _decode(value.pop("definition_json"), {})
        return value

    def list_workflows(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT workflow_id FROM agent_workflows ORDER BY created_at DESC, workflow_id DESC").fetchall()
        return [self.get_workflow(str(row["workflow_id"])) for row in rows]

    def create_run(self, workflow_id: str, *, source_job_id: str, entity_id: str, entity_type: str,
                   model_profile_id: str, prompt_id: str, actor: str, roles: tuple[str, ...], request_id: str,
                   idempotency_key: str, parent_run_id: str | None = None,
                   evidence_summary: dict[str, Any] | None = None,
                   runtime_snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
        if not isinstance(evidence_summary or {}, dict):
            raise AgenticError("工作流 Evidence 摘要必须是 object", code="workflow_evidence_invalid")
        _reject_sensitive(evidence_summary or {})
        runtime_snapshot = validate_runtime_snapshot(
            runtime_snapshot, model_profile_id=model_profile_id, prompt_id=prompt_id,
        )
        with self.database.connect() as connection:
            existing = connection.execute("SELECT workflow_run_id FROM agent_workflow_runs WHERE actor=? AND idempotency_key=?", (actor, idempotency_key)).fetchone()
        if existing:
            return {**self.get_run(str(existing["workflow_run_id"])), "idempotent_replay": True}
        definition = dict(self.get_workflow(workflow_id)["definition"])
        definition["evidence_summary"] = dict(evidence_summary or {})
        # Store only the public Profile/Connection snapshot and Prompt hash
        # selected at submission time. The child Agent uses these values rather
        # than resolving mutable configuration again.
        definition.update(runtime_snapshot)
        run_id, now = uuid.uuid4().hex, utc_now()
        try:
            with self.database.transaction() as connection:
                budget, retry = definition["budget"], definition["retry_policy"]
                connection.execute(
                    "INSERT INTO agent_workflow_runs(workflow_run_id,workflow_id,parent_run_id,source_job_id,entity_id,entity_type,model_profile_id,prompt_id,status,max_concurrency,max_tool_calls,used_tool_calls,timeout_seconds,max_attempts,locked_snapshot_json,actor,roles_json,request_id,idempotency_key,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?, 'queued',?,?,0,?,?,?,?,?,?,?,?,?)",
                    (run_id, workflow_id, parent_run_id, source_job_id, entity_id, entity_type, model_profile_id, prompt_id,
                     budget["max_concurrency"], budget["max_tool_calls"], budget["timeout_seconds"], retry["max_attempts"],
                     _db_json(self.database, definition), actor, _db_json(self.database, list(roles)), request_id, idempotency_key, now, now),
                )
                for sequence, node in enumerate(definition["nodes"]):
                    connection.execute(
                        "INSERT INTO agent_workflow_nodes(workflow_run_id,node_id,sequence,role_id,dependencies_json,allowed_tools_json,max_steps,max_tool_calls,timeout_seconds,status,attempt,used_tool_calls,result_summary_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,'pending',0,0,?,?,?)",
                        (run_id, node["node_id"], sequence, node["role_id"], _db_json(self.database, node["depends_on"]),
                         _db_json(self.database, node["allowed_tools"]), node["max_steps"], node["max_tool_calls"], node["timeout_seconds"],
                         _db_json(self.database, {}), now, now),
                    )
                self._append_event(connection, run_id, "workflow_run_created", None, {"status": "queued"}, now)
        except Exception:
            with self.database.connect() as connection:
                existing = connection.execute("SELECT workflow_run_id FROM agent_workflow_runs WHERE actor=? AND idempotency_key=?", (actor, idempotency_key)).fetchone()
            if existing:
                return {**self.get_run(str(existing["workflow_run_id"])), "idempotent_replay": True}
            raise
        return {**self.get_run(run_id), "idempotent_replay": False}

    def get_run(self, run_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM agent_workflow_runs WHERE workflow_run_id=?", (run_id,)).fetchone()
            if not row:
                raise AgenticError("Agent 工作流 Run 不存在", code="workflow_run_not_found", status_code=404)
            nodes = connection.execute("SELECT * FROM agent_workflow_nodes WHERE workflow_run_id=? ORDER BY sequence", (run_id,)).fetchall()
            events = connection.execute("SELECT * FROM agent_workflow_events WHERE workflow_run_id=? ORDER BY sequence", (run_id,)).fetchall()
        value = dict(row)
        value["locked_snapshot"] = _decode(value.pop("locked_snapshot_json"), {})
        value["roles"] = _decode(value.pop("roles_json"), [])
        value["nodes"] = [self._node(node) for node in nodes]
        value["events"] = [self._event(event) for event in events]
        artifacts: list[dict[str, Any]] = []
        for node in value["nodes"]:
            summary = node["result_summary"] or {}
            child_artifacts = summary.get("artifacts") if isinstance(summary, dict) else None
            if isinstance(child_artifacts, list):
                for index, artifact in enumerate(child_artifacts):
                    if not isinstance(artifact, dict):
                        continue
                    payload = artifact.get("payload") if isinstance(artifact.get("payload"), dict) else artifact
                    artifacts.append({
                        "artifact_id": str(artifact.get("artifact_id") or f"{node['node_id']}-artifact-{index}"),
                        "artifact_type": str(artifact.get("artifact_type") or "candidate"),
                        "step_id": node["node_id"], "node_id": node["node_id"], "role_id": node["role_id"],
                        "child_agent_run_id": node["child_agent_run_id"], "payload": payload,
                    })
            elif summary:
                artifacts.append({
                    "artifact_id": f"{node['node_id']}-result", "artifact_type": "node_result",
                    "step_id": node["node_id"], "node_id": node["node_id"], "role_id": node["role_id"],
                    "child_agent_run_id": node["child_agent_run_id"], "payload": summary,
                })
        value["artifacts"] = artifacts
        return value

    def list_runs(self, *, limit: int = 100, workflow_id: str | None = None) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            bounded = max(1, min(int(limit), 500))
            if workflow_id:
                rows = connection.execute("SELECT workflow_run_id FROM agent_workflow_runs WHERE workflow_id=? ORDER BY created_at DESC, workflow_run_id DESC LIMIT ?", (workflow_id, bounded)).fetchall()
            else:
                rows = connection.execute("SELECT workflow_run_id FROM agent_workflow_runs ORDER BY created_at DESC, workflow_run_id DESC LIMIT ?", (bounded,)).fetchall()
        return [self.get_run(str(row["workflow_run_id"])) for row in rows]

    def transition_run(self, run_id: str, status: str, *, allowed_from: set[str], idempotency_key: str | None = None,
                       error_code: str | None = None, error_summary: str | None = None) -> dict[str, Any]:
        now, replay = utc_now(), False
        with self.database.transaction() as connection:
            connection.execute("UPDATE agent_workflow_runs SET state_version=state_version WHERE workflow_run_id=?", (run_id,))
            if idempotency_key and connection.execute("SELECT 1 FROM agent_workflow_events WHERE workflow_run_id=? AND idempotency_key=?", (run_id, idempotency_key)).fetchone():
                replay = True
            row = connection.execute("SELECT status,state_version FROM agent_workflow_runs WHERE workflow_run_id=?", (run_id,)).fetchone()
            if not row:
                raise AgenticError("Agent 工作流 Run 不存在", code="workflow_run_not_found", status_code=404)
            if not replay and str(row["status"]) not in allowed_from:
                raise AgenticError("工作流 Run 状态不允许此操作", code="workflow_state_conflict", status_code=409)
            if not replay:
                terminal = now if status in {"completed", "failed", "cancelled", "awaiting_human"} else None
                connection.execute("UPDATE agent_workflow_runs SET status=?,error_code=?,error_summary=?,started_at=COALESCE(started_at,?),completed_at=?,updated_at=?,state_version=state_version+1 WHERE workflow_run_id=? AND state_version=?", (status, error_code, error_summary, now if status == "running" else None, terminal, now, run_id, row["state_version"]))
                self._append_event(connection, run_id, "workflow_run_" + status, None, {"status": status, "error_code": error_code}, now, idempotency_key=idempotency_key)
        result = self.get_run(run_id)
        if idempotency_key:
            result["idempotent_replay"] = replay
        return result

    def ready_nodes(self, run_id: str) -> list[dict[str, Any]]:
        run = self.get_run(run_id)
        completed = {node["node_id"] for node in run["nodes"] if node["status"] == "completed"}
        return [node for node in run["nodes"] if node["status"] == "pending" and set(node["dependencies"]) <= completed]

    def claim_node(self, run_id: str, node_id: str) -> dict[str, Any]:
        now = utc_now()
        with self.database.transaction() as connection:
            changed = connection.execute("UPDATE agent_workflow_nodes SET status='running',attempt=attempt+1,started_at=?,updated_at=? WHERE workflow_run_id=? AND node_id=? AND status='pending'", (now, now, run_id, node_id))
            if changed.rowcount != 1:
                raise AgenticError("工作流节点状态已变化", code="workflow_node_conflict", status_code=409)
            self._append_event(connection, run_id, "workflow_node_running", node_id, {}, now)
        return next(node for node in self.get_run(run_id)["nodes"] if node["node_id"] == node_id)

    def attach_child_run(self, run_id: str, node_id: str, child_run_id: str) -> None:
        with self.database.transaction() as connection:
            connection.execute("UPDATE agent_workflow_nodes SET child_agent_run_id=?,updated_at=? WHERE workflow_run_id=? AND node_id=? AND status='running'", (child_run_id, utc_now(), run_id, node_id))

    def finish_node(self, run_id: str, node_id: str, *, status: str, used_tool_calls: int = 0, result: dict[str, Any] | None = None,
                    error_code: str | None = None, error_summary: str | None = None) -> dict[str, Any]:
        now = utc_now()
        with self.database.transaction() as connection:
            changed = connection.execute("UPDATE agent_workflow_nodes SET status=?,used_tool_calls=used_tool_calls+?,result_summary_json=?,error_code=?,error_summary=?,completed_at=?,updated_at=? WHERE workflow_run_id=? AND node_id=? AND status='running'", (status, int(used_tool_calls), _db_json(self.database, result or {}), error_code, error_summary, now, now, run_id, node_id))
            if changed.rowcount != 1:
                raise AgenticError("工作流节点状态已变化", code="workflow_node_conflict", status_code=409)
            connection.execute("UPDATE agent_workflow_runs SET used_tool_calls=used_tool_calls+?,updated_at=?,state_version=state_version+1 WHERE workflow_run_id=?", (int(used_tool_calls), now, run_id))
            self._append_event(connection, run_id, "workflow_node_" + status, node_id, {"error_code": error_code}, now)
        return next(node for node in self.get_run(run_id)["nodes"] if node["node_id"] == node_id)

    def requeue_node(self, run_id: str, node_id: str, *, reason: str) -> dict[str, Any]:
        """Return a paused in-flight node to the checkpoint queue without marking it failed."""
        now = utc_now()
        with self.database.transaction() as connection:
            changed = connection.execute(
                "UPDATE agent_workflow_nodes SET status='pending',child_agent_run_id=NULL,result_summary_json=?,error_code=NULL,error_summary=NULL,completed_at=NULL,updated_at=? WHERE workflow_run_id=? AND node_id=? AND status='running'",
                (_db_json(self.database, {}), now, run_id, node_id),
            )
            if changed.rowcount != 1:
                raise AgenticError("工作流节点状态已变化", code="workflow_node_conflict", status_code=409)
            self._append_event(connection, run_id, "workflow_node_pending", node_id, {"reason": reason}, now)
        return next(node for node in self.get_run(run_id)["nodes"] if node["node_id"] == node_id)

    def reset_node(self, run_id: str, node_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        now, replay = utc_now(), False
        with self.database.transaction() as connection:
            if idempotency_key and connection.execute("SELECT 1 FROM agent_workflow_events WHERE workflow_run_id=? AND idempotency_key=?", (run_id, idempotency_key)).fetchone():
                replay = True
            if not replay:
                changed = connection.execute("UPDATE agent_workflow_nodes SET status='pending',child_agent_run_id=NULL,error_code=NULL,error_summary=NULL,completed_at=NULL,updated_at=? WHERE workflow_run_id=? AND node_id=? AND status IN ('running','failed')", (now, run_id, node_id))
                if changed.rowcount != 1:
                    raise AgenticError("工作流节点不可重试", code="workflow_node_conflict", status_code=409)
                self._append_event(connection, run_id, "workflow_node_retry_queued", node_id, {}, now, idempotency_key=idempotency_key)
        node = next(node for node in self.get_run(run_id)["nodes"] if node["node_id"] == node_id)
        node["idempotent_replay"] = replay
        return node

    def recover_active_runs(self) -> list[str]:
        now = utc_now()
        with self.database.transaction() as connection:
            rows = connection.execute("SELECT workflow_run_id FROM agent_workflow_runs WHERE status IN ('queued','running') ORDER BY created_at,workflow_run_id").fetchall()
            run_ids = [str(row["workflow_run_id"]) for row in rows]
            for run_id in run_ids:
                connection.execute("UPDATE agent_workflow_nodes SET status='pending',child_agent_run_id=NULL,error_code=NULL,error_summary=NULL,completed_at=NULL,updated_at=? WHERE workflow_run_id=? AND status='running'", (now, run_id))
                self._append_event(connection, run_id, "workflow_run_recovered", None, {}, now)
        return run_ids

    def recover_run(self, run_id: str) -> dict[str, Any]:
        now = utc_now()
        with self.database.transaction() as connection:
            row = connection.execute("SELECT status FROM agent_workflow_runs WHERE workflow_run_id=?", (run_id,)).fetchone()
            if not row:
                raise AgenticError("Agent 工作流 Run 不存在", code="workflow_run_not_found", status_code=404)
            if str(row["status"]) not in {"queued", "running"}:
                raise AgenticError("工作流 Run 状态不允许恢复", code="workflow_state_conflict", status_code=409)
            connection.execute("UPDATE agent_workflow_nodes SET status='pending',child_agent_run_id=NULL,error_code=NULL,error_summary=NULL,completed_at=NULL,updated_at=? WHERE workflow_run_id=? AND status='running'", (now, run_id))
            self._append_event(connection, run_id, "workflow_run_recovered", None, {}, now)
        return self.get_run(run_id)

    def active_run_ids(self) -> list[str]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT workflow_run_id FROM agent_workflow_runs WHERE status IN ('queued','running') ORDER BY created_at,workflow_run_id").fetchall()
        return [str(row["workflow_run_id"]) for row in rows]

    def _append_event(self, connection: Any, run_id: str, event_type: str, node_id: str | None, attributes: dict[str, Any], now: str, *, idempotency_key: str | None = None) -> None:
        connection.execute("UPDATE agent_workflow_runs SET state_version=state_version WHERE workflow_run_id=?", (run_id,))
        sequence = int(connection.execute("SELECT COALESCE(MAX(sequence),-1)+1 FROM agent_workflow_events WHERE workflow_run_id=?", (run_id,)).fetchone()[0])
        connection.execute("INSERT INTO agent_workflow_events(event_id,workflow_run_id,sequence,event_type,node_id,attributes_json,idempotency_key,created_at) VALUES (?,?,?,?,?,?,?,?)", (uuid.uuid4().hex, run_id, sequence, event_type, node_id, _db_json(self.database, attributes), idempotency_key, now))

    @staticmethod
    def _node(row: Any) -> dict[str, Any]:
        value = dict(row)
        value["dependencies"] = _decode(value.pop("dependencies_json"), [])
        value["allowed_tools"] = _decode(value.pop("allowed_tools_json"), [])
        value["result_summary"] = _decode(value.pop("result_summary_json"), {})
        return value

    @staticmethod
    def _event(row: Any) -> dict[str, Any]:
        value = dict(row)
        value["type"] = value.pop("event_type")
        value["attributes"] = _decode(value.pop("attributes_json"), {})
        return value
