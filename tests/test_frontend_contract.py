from pathlib import Path


HTML = Path("frontend/index.html").read_text(encoding="utf-8")


def test_queue_is_scrollable_and_vertically_resizable():
    assert "resize:vertical" in HTML
    assert "overflow-y:scroll" in HTML
    assert "::-webkit-scrollbar" in HTML
    assert 'class="workspace-pane active queue-panel"' in HTML


def test_dashboard_contains_log_metrics_risk_nodes_and_drain3_details():
    for element_id in (
        "metricOriginalLogs",
        "metricPendingLogs",
        "metricAnalyzedLogs",
        "metricReducedLogs",
        "metricTemplateWindows",
        "metricCompressionRatio",
        "riskNodes",
        "drainTemplates",
    ):
        assert f'id="{element_id}"' in HTML


def test_summary_empty_states_do_not_hide_the_work_queue_below_the_fold():
    assert ".risk-node-grid .empty{min-height:100px}" in HTML
    assert ".drain-list .empty{min-height:100px}" in HTML


def test_workspace_is_full_width_with_node_and_review_tabs():
    assert ".workspace{display:block" in HTML
    assert 'data-workspace-tab="nodes"' in HTML
    assert 'data-workspace-tab="review"' in HTML
    assert 'id="workspaceNodes"' in HTML
    assert 'id="workspaceReview"' in HTML
    assert ".workspace-pane.active{display:block}" in HTML


def test_sidebar_workspace_actions_are_real_buttons_and_navigate():
    assert HTML.count('<button class="nav') == 4
    for target in ("overview", "queue", "review", "export"):
        assert f'data-target="{target}"' in HTML
    assert "scrollIntoView" in HTML


def test_release_notes_include_current_bugfix_version():
    release_notes = Path("releas.md").read_text(encoding="utf-8")
    assert "## 1.1.1 - 2026-06-23" in release_notes
