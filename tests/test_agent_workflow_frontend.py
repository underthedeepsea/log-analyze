from pathlib import Path


def test_workflow_designer_and_run_graph_are_exposed():
    source = Path("frontend/src/app.js").read_text(encoding="utf-8")
    for marker in (
        '["agentWorkflows", "工作流编排"]', '"/agent-workflows"', "AgentWorkflowsPage",
        "Workflow Designer", "DAG 运行图", "固定角色", "依赖节点", "max_concurrency",
        "ToolCall 预算", "只读回放", "去人工审批", "retryNode", "toggleRole", "toggleDependency", "节点日志与事件",
    ):
        assert marker in source


def test_workflow_graph_distinguishes_execution_kinds_and_states():
    source = Path("frontend/src/app.js").read_text(encoding="utf-8")
    for marker in ("模型推断", "确定性工具", "人工 Gate", "待审批资产", "Artifact 与 Gate", "artifact_type", "workflow-node", "dependency-line", "awaiting_human", "cancelled"):
        assert marker in source


def test_workflow_assets_are_synced():
    source = Path("frontend/src/app.js").read_bytes()
    styles = Path("frontend/src/styles.css").read_bytes()
    assert source == Path("frontend/dist/assets/app.js").read_bytes() == Path("src/logrisk_django/static/logrisk/assets/app.js").read_bytes()
    assert styles == Path("frontend/dist/assets/app.css").read_bytes() == Path("src/logrisk_django/static/logrisk/assets/app.css").read_bytes()
