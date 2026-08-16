from __future__ import annotations

from typing import Any

from .compiler import WorkflowLimits, compile_workflow
from .errors import AgenticError
from .roles import RoleRegistry
from .workflow_repository import WorkflowRepository, validate_runtime_snapshot
from .workflow_scheduler import WorkflowScheduler
from .tool_registry import _reject_sensitive


class WorkflowService:
    def __init__(self, repository: WorkflowRepository, scheduler: WorkflowScheduler, roles: RoleRegistry, limits: WorkflowLimits) -> None:
        self.repository, self.scheduler, self.roles, self.limits = repository, scheduler, roles, limits

    @staticmethod
    def _key(value: str) -> str:
        key = str(value or "").strip()
        if not key:
            raise AgenticError("缺少幂等键", code="idempotency_required")
        return key

    def create_workflow(self, definition: dict[str, Any], *, actor: str, idempotency_key: str) -> dict[str, Any]:
        return self.repository.create_workflow(compile_workflow(definition, self.roles, self.limits), actor=actor, idempotency_key=self._key(idempotency_key))

    def list_workflows(self) -> list[dict[str, Any]]:
        return self.repository.list_workflows()

    def get_workflow(self, workflow_id: str) -> dict[str, Any]:
        return self.repository.get_workflow(workflow_id)

    def create_run(self, workflow_id: str, *, evidence_summary: dict[str, Any], runtime_snapshot: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        _reject_sensitive(evidence_summary)
        runtime_snapshot = validate_runtime_snapshot(
            runtime_snapshot,
            model_profile_id=str(kwargs.get("model_profile_id") or ""),
            prompt_id=str(kwargs.get("prompt_id") or ""),
        )
        kwargs["idempotency_key"] = self._key(str(kwargs.get("idempotency_key") or ""))
        if not str(kwargs.get("actor") or "").strip() or not str(kwargs.get("request_id") or "").strip():
            raise AgenticError("工作流操作人和请求标识不能为空", code="workflow_identity_invalid")
        return self.repository.create_run(workflow_id, evidence_summary=evidence_summary, runtime_snapshot=runtime_snapshot, **kwargs)

    def execute_run(self, run_id: str) -> dict[str, Any]:
        return self.scheduler.execute(run_id)

    def list_runs(self, *, limit: int = 100, workflow_id: str | None = None) -> list[dict[str, Any]]:
        return self.repository.list_runs(limit=limit, workflow_id=workflow_id)

    def get_run(self, run_id: str) -> dict[str, Any]:
        return self.repository.get_run(run_id)

    def replay(self, run_id: str) -> dict[str, Any]:
        return {**self.get_run(run_id), "mode": "read_only"}

    def pause(self, run_id: str, *, idempotency_key: str) -> dict[str, Any]:
        key, run = self._key(idempotency_key), self.get_run(run_id)
        self.scheduler.worker.pause_active(run, key)
        return self.repository.transition_run(run_id, "paused", allowed_from={"queued", "running"}, idempotency_key=key)

    def resume(self, run_id: str, *, idempotency_key: str) -> dict[str, Any]:
        return self.repository.transition_run(run_id, "running", allowed_from={"paused"}, idempotency_key=self._key(idempotency_key))

    def cancel(self, run_id: str, *, idempotency_key: str) -> dict[str, Any]:
        key, run = self._key(idempotency_key), self.get_run(run_id)
        self.scheduler.worker.cancel_active(run, key)
        return self.repository.transition_run(run_id, "cancelled", allowed_from={"queued", "running", "paused", "failed", "awaiting_human"}, idempotency_key=key)

    def retry_node(self, run_id: str, node_id: str, *, idempotency_key: str) -> dict[str, Any]:
        key = self._key(idempotency_key)
        node = self.repository.reset_node(run_id, node_id, idempotency_key=key)
        run = self.get_run(run_id)
        if run["status"] == "failed":
            run = self.repository.transition_run(run_id, "queued", allowed_from={"failed"})
        run["idempotent_replay"] = bool(node.pop("idempotent_replay", False))
        return run

    def retry(self, run_id: str, *, idempotency_key: str, request_id: str) -> dict[str, Any]:
        source = self.get_run(run_id)
        snapshot = dict(source["locked_snapshot"])
        runtime_keys = ("profile_snapshot", "connection_snapshot", "prompt_id", "prompt_sha256")
        runtime_snapshot = {key: snapshot[key] for key in runtime_keys if key in snapshot}
        return self.create_run(source["workflow_id"], source_job_id=source["source_job_id"], entity_id=source["entity_id"], entity_type=source["entity_type"], model_profile_id=source["model_profile_id"], prompt_id=source["prompt_id"], actor=source["actor"], roles=tuple(source["roles"]), request_id=request_id, idempotency_key=self._key(idempotency_key), parent_run_id=run_id, evidence_summary=dict(snapshot.get("evidence_summary") or {}), runtime_snapshot=runtime_snapshot or None)

    def recover_active_runs(self) -> list[str]:
        return self.repository.recover_active_runs()

    def recover_run(self, run_id: str) -> dict[str, Any]:
        return self.repository.recover_run(run_id)
