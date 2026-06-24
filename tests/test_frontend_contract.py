from pathlib import Path


FRONTEND = Path("frontend")


def source_text() -> str:
    return "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted((FRONTEND / "src").rglob("*"))
        if path.is_file()
    )


def test_react_source_has_all_workspaces():
    source = source_text()
    for label in ("特征总览", "识别队列", "人工审批", "批准规则库", "导出记录"):
        assert label in source


def test_v3_metrics_and_animations_are_present():
    source = source_text()
    for text in (
        "Drain3 实时压缩",
        "今日 LLM 分析日志",
        "分析速度",
        "规则复用收益",
        "跳过 LLM",
    ):
        assert text in source
    assert "@keyframes" in source
    assert "rolling_60s_logs_per_second" in source


def test_upload_accepts_result_and_raw_log_formats():
    source = source_text()
    for suffix in (".json", ".jsonl", ".txt", ".log"):
        assert suffix in source
    assert "/api/inputs/analyze" in source
    assert "/api/metrics" in source


def test_react_uses_text_rendering_without_raw_html_injection():
    source = source_text()
    assert "dangerouslySetInnerHTML" not in source


def test_approval_workspace_has_selectable_features_evidence_and_editor():
    source = source_text()
    assert 'function FeatureList(props)' in source
    assert 'function FeatureEvidence(props)' in source
    assert 'className: "approval-workspace"' in source
    assert "approval-feature-list" in source
    assert "evidence-panel" in source
    assert 'onClick: function () { props.onSelect(feature.candidate_id); }' in source


def test_feature_evidence_contains_sanitized_template_fields_and_notice():
    source = source_text()
    for field in (
        "template_hash",
        "component",
        "category",
        "severity",
        "count",
        "first_seen",
        "last_seen",
    ):
        assert field in source
    assert "当前展示 Drain3 脱敏特征模板，系统未保存原始日志" in source
    assert "暂无脱敏模板证据" in source
    assert "dangerouslySetInnerHTML" not in source


def test_feature_selection_defaults_and_recovers_from_stale_id():
    source = source_text()
    assert "setSelectedId(function (current)" in source
    assert "features.some(function (feature) { return feature.candidate_id === current; })" in source
    assert "features.length ? features[0].candidate_id : null" in source


def test_committed_bundle_is_local_and_self_contained():
    html = (FRONTEND / "dist" / "index.html").read_text(encoding="utf-8")
    assert "/assets/" in html
    assert "cdn" not in html.lower()
    assert list((FRONTEND / "dist" / "assets").glob("*.js"))
    assert list((FRONTEND / "dist" / "assets").glob("*.css"))


def test_package_uses_react_without_vite_or_a_runtime_build_step():
    package = (FRONTEND / "package.json").read_text(encoding="utf-8")
    assert '"react"' in package
    assert '"react-dom"' in package
    assert "vite" not in package.lower()
    assert '"build"' not in package


def test_release_docs_describe_current_bugfix_version():
    release = Path("releas.md").read_text(encoding="utf-8")
    readme = Path("README.md").read_text(encoding="utf-8")
    guide = Path("CODEX_WORK_GUIDE_LOG_RISK_ANALYSIS.md").read_text(encoding="utf-8")

    assert "## 1.2.1 - 2026-06-24" in release
    assert "当前版本：`1.2.1`" in readme
    assert "审批页" in guide and "脱敏模板证据" in guide
    assert "dashboard.sh restart" in readme
    assert ".txt" in readme
    assert "普通启动不需要 Node.js" in readme
    assert "Vite" not in readme
