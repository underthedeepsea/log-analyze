from __future__ import annotations

import copy

from .models import AgentRunRequest, validate_evidence_scope
from .tool_registry import AgentToolContext
from .errors import AgenticError
from .repository import AgentRepository
from .runtime import AgentRuntime


class AgentService:
    def __init__(self, repository: AgentRepository, runtime: AgentRuntime) -> None:
        self.repository = repository
        self.runtime = runtime

    def lock_evidence_summary(self, summary: dict, *, source_job_id: str, entity_id: str, actor: str, request_id: str) -> dict:
        value = copy.deepcopy(summary)
        validate_evidence_scope(value, source_job_id, entity_id, code="agent_scope_invalid")
        cluster = (value.get("entity") or {}).get("cluster")
        context = AgentToolContext("scope-lock", source_job_id, entity_id, frozenset(), actor, request_id, cluster=cluster)
        locked = self.runtime.tools.lock_evidence(context)
        if locked is not None:
            if value.get("locked_evidence_hash") not in (None, locked["evidence_hash"]):
                raise AgenticError("锁定Evidence已变化", code="agent_evidence_changed")
            value["locked_evidence_hash"] = locked["evidence_hash"]
            value["locked_evidence_cluster"] = locked["cluster"]
        return value

    def create_run(self, request: AgentRunRequest, *, locked_snapshot: dict) -> dict:
        existing = self.repository._by_idempotency(request.actor, request.idempotency_key)
        if existing:
            saved = self.repository.get_run(existing)
            if saved["source_job_id"] != request.source_job_id or saved["entity_id"] != request.entity_id:
                raise AgenticError("幂等键跨实体复用", code="agent_idempotency_scope_conflict")
            return {**saved, "idempotent_replay": True}
        value = copy.deepcopy(locked_snapshot)
        if value.get("workflow_role_id"):
            value["evidence_summary"] = self.lock_evidence_summary(value.get("evidence_summary") or {}, source_job_id=request.source_job_id, entity_id=request.entity_id, actor=request.actor, request_id=request.request_id)
        return self.repository.create_run(request, locked_snapshot=value)

    def execute_run(self, run_id: str) -> dict:
        return self.runtime.execute(run_id)

    def list_runs(self, *, limit: int = 100) -> list[dict]:
        return self.repository.list_runs(limit=limit)

    def get_run(self, run_id: str) -> dict:
        return self.repository.get_run(run_id)

    @staticmethod
    def _key(value: str) -> str:
        key = str(value or "").strip()
        if not key:
            raise AgenticError("缺少幂等键", code="idempotency_required")
        return key

    def pause(self, run_id: str, *, idempotency_key: str) -> dict:
        return self.repository.transition(
            run_id, "paused", allowed_from={"queued", "planning", "running"},
            event_idempotency_key=self._key(idempotency_key),
        )

    def resume(self, run_id: str, *, idempotency_key: str) -> dict:
        run = self.repository.get_run(run_id)
        target = "running" if run["steps"] else "queued"
        return self.repository.transition(
            run_id, target, allowed_from={"paused"}, event_idempotency_key=self._key(idempotency_key)
        )

    def cancel(self, run_id: str, *, idempotency_key: str) -> dict:
        return self.repository.transition(
            run_id, "cancelled", allowed_from={"queued", "planning", "running", "paused", "awaiting_human"},
            event_idempotency_key=self._key(idempotency_key),
        )

    def retry(self, run_id: str, *, idempotency_key: str, request_id: str) -> dict:
        idempotency_key = self._key(idempotency_key)
        source = self.repository.get_run(run_id)
        request = AgentRunRequest(
            source_job_id=source["source_job_id"], entity_id=source["entity_id"], entity_type=source["entity_type"],
            model_profile_id=source["model_profile_id"], prompt_id=source["prompt_id"], max_steps=source["max_steps"],
            max_tool_calls=source["max_tool_calls"], timeout_seconds=source["timeout_seconds"],
            allowed_tools=tuple(source["allowed_tools"]), idempotency_key=idempotency_key, actor=source["actor"],
            roles=tuple(source["roles"]), request_id=request_id, parent_run_id=run_id,
        )
        return self.repository.create_run(request, locked_snapshot=source["locked_snapshot"])

    def replay(self, run_id: str) -> dict:
        return {**self.repository.get_run(run_id), "mode": "read_only"}

    def mark_dispatch_failed(self, run_id: str, *, error_code: str) -> dict:
        return self.repository.transition(
            run_id, "failed", allowed_from={"queued", "planning", "running"}, error_code=error_code,
            error_summary="Agent Airflow 分派失败",
        )

    def active_run_ids(self) -> list[str]:
        return self.repository.list_active_run_ids()

    def recover_active_runs(self) -> list[str]:
        return self.repository.recover_active_runs()

    def recover_run(self, run_id: str) -> dict:
        return self.repository.recover_run(run_id)
