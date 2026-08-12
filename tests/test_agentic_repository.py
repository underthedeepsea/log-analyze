from __future__ import annotations

from logrisk.agentic.models import AgentPlan, AgentRunRequest, AgentStepPlan
from logrisk.agentic.repository import AgentRepository
from logrisk.database import SQLiteDatabase


def _request() -> AgentRunRequest:
    return AgentRunRequest(
        source_job_id="job-1",
        entity_id="node-a",
        entity_type="node",
        model_profile_id="qwen",
        prompt_id="agent_plan_v1",
        max_steps=5,
        max_tool_calls=8,
        timeout_seconds=120,
        allowed_tools=("get_sanitized_evidence",),
        idempotency_key="request-1",
        actor="alice",
        roles=("operator",),
        request_id="req-1",
    )


def test_agent_repository_persists_snapshot_steps_events_and_artifacts(tmp_path):
    repository = AgentRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    run = repository.create_run(_request(), locked_snapshot={"schema_version": "1.0"})
    repository.replace_plan(
        run["run_id"],
        AgentPlan(
            goal="提取异常证据",
            steps=(
                AgentStepPlan(
                    step_id="step-1",
                    tool_name="get_sanitized_evidence",
                    arguments={"job_id": "job-1", "entity_id": "node-a"},
                ),
            ),
        ),
    )
    repository.append_event(run["run_id"], "plan_created", {"step_count": 1})
    repository.add_artifact(run["run_id"], "evaluation", {"passed": True})

    restored = repository.get_run(run["run_id"])

    assert restored["schema_version"] == "1.0"
    assert restored["locked_snapshot"] == {"schema_version": "1.0"}
    assert restored["steps"][0]["tool_name"] == "get_sanitized_evidence"
    assert restored["steps"][0]["schema_version"] == "1.0"
    assert restored["events"][0]["type"] == "run_created"
    assert restored["events"][0]["schema_version"] == "1.0"
    assert restored["events"][1]["type"] == "plan_created"
    assert restored["artifacts"][0]["artifact_type"] == "evaluation"
    assert restored["artifacts"][0]["schema_version"] == "1.0"


def test_agent_repository_reuses_actor_idempotency_key(tmp_path):
    repository = AgentRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))

    first = repository.create_run(_request(), locked_snapshot={"schema_version": "1.0"})
    second = repository.create_run(_request(), locked_snapshot={"schema_version": "1.0"})

    assert second["run_id"] == first["run_id"]
    assert second["idempotent_replay"] is True
