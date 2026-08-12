from __future__ import annotations

from logrisk.agentic import AgentPlan, AgentRunRequest, AgentStepPlan, FakeAgentPlanner
from logrisk.agentic.repository import AgentRepository
from logrisk.agentic.runtime import AgentRuntime
from logrisk.agentic.service import AgentService
from logrisk.agentic.tool_registry import ToolRegistry
from logrisk.airflow_tasks import execute_agent_run
from logrisk.database import SQLiteDatabase


def test_airflow_task_executes_persisted_agent_run(tmp_path) -> None:
    database = SQLiteDatabase(tmp_path / "db.sqlite3")
    database.migrate()
    repository = AgentRepository(database)
    tools = ToolRegistry()
    tools.register(name="read", description="read", required_arguments=(), handler=lambda _args, _context: {"ok": True})
    service = AgentService(repository, AgentRuntime(
        repository,
        FakeAgentPlanner(AgentPlan("read", (AgentStepPlan("step-1", "read", {}),))),
        tools,
    ))
    run = service.create_run(AgentRunRequest(
        "job-1", "node-a", "node", "profile", "prompt", 2, 2, 60,
        ("read",), "key", "actor", ("operator",), "request-1",
    ), locked_snapshot={"goal": "read", "evidence_summary": {}})

    class Container:
        agent_runs = service

    result = execute_agent_run(run["run_id"], "request-1", container=Container())
    assert result["status"] == "awaiting_human"
    assert result["agent_run_id"] == run["run_id"]


def test_airflow_task_recovers_interrupted_running_step(tmp_path) -> None:
    database = SQLiteDatabase(tmp_path / "db.sqlite3")
    database.migrate()
    repository = AgentRepository(database)
    tools = ToolRegistry()
    tools.register(name="read", description="read", required_arguments=(), handler=lambda _args, _context: {"ok": True})
    plan = AgentPlan("read", (AgentStepPlan("step-1", "read", {}),))
    service = AgentService(repository, AgentRuntime(repository, FakeAgentPlanner(plan), tools))
    run = service.create_run(AgentRunRequest(
        "job-1", "node-a", "node", "profile", "prompt", 2, 2, 60,
        ("read",), "key", "actor", ("operator",), "request-1",
    ), locked_snapshot={"goal": "read", "evidence_summary": {}})
    repository.transition(run["run_id"], "planning", allowed_from={"queued"})
    repository.replace_plan(run["run_id"], plan)
    repository.transition(run["run_id"], "running", allowed_from={"planning"})
    repository.start_step(run["run_id"], "step-1")

    class Container:
        agent_runs = service

    result = execute_agent_run(run["run_id"], "request-1", container=Container())

    assert result["status"] == "awaiting_human"
    assert service.get_run(run["run_id"])["steps"][0]["attempt"] == 2
