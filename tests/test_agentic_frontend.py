from pathlib import Path


def test_agent_frontend_separates_model_tools_evaluator_and_human_gate():
    source = Path("frontend/src/app.js").read_text(encoding="utf-8")
    assert '"/agent-runs"' in source
    assert "模型计划" in source
    assert "确定性工具" in source
    assert "质量门禁" in source
    assert "人工 Gate" in source
    assert "等待人工审批" in source
    assert "思维链" not in source


def test_agent_frontend_has_bounded_create_form():
    source = Path("frontend/src/app.js").read_text(encoding="utf-8")
    for label in ("来源任务", "风险实体", "模型 Profile", "规划 Prompt", "最大步骤", "工具预算", "超时秒数", "允许工具"):
        assert label in source
    assert "createAgentRun" in source


def test_agent_frontend_exposes_tool_artifact_failure_replay_and_review():
    source = Path("frontend/src/app.js").read_text(encoding="utf-8")
    for label in ("ToolCall", "Artifact", "运行失败", "只读回放", "进入人工审批"):
        assert label in source
    assert '"/" + action' in source
