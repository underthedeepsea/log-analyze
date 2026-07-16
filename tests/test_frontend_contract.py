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
    for label in ("特征总览", "识别队列", "AI 调用追踪", "Prompt 管理", "模型画像", "人工审批", "批准规则库", "导出记录", "评测中心", "模板质量"):
        assert label in source


def test_drain_quality_center_contract():
    source = source_text()
    for text in (
        "/api/drain-quality/datasets",
        "/api/drain-quality/annotations",
        "/api/drain-quality/eval-runs",
        "/api/drain-quality/profiles",
        "Grouping F1",
        "Over-merge",
        "Over-split",
        "Singleton",
        "Wildcard",
        "标注工作台",
        "可疑模板",
        "配置对比",
        "发布管理",
        "确认发布",
    ):
        assert text in source


def test_runtime_backend_address_contract():
    source = source_text()
    for text in (
        "LOGRISK_CONFIG",
        'localStorage.getItem("logrisk.apiBase")',
        'localStorage.setItem("logrisk.apiBase"',
        'localStorage.removeItem("logrisk.apiBase")',
        "function apiUrl(path)",
        "/api/health",
        "测试连接",
        "测试并保存",
        "恢复默认",
        "DASHBOARD_CORS_ORIGINS",
    ):
        assert text in source
    html = (FRONTEND / "dist" / "index.html").read_text(encoding="utf-8")
    assert '<script src="/config.js"></script>' in html
    assert (FRONTEND / "dist" / "config.js").is_file()


def test_m11_quality_and_settings_use_workspace_design_contract():
    source = source_text()
    for text in (
        "drain-quality-page",
        "settings-page",
        "quality-annotation-layout",
        "annotation-queue",
        "annotation-detail",
        "suspicious-filters",
        "profile-compare-selectors",
        "profile-parameter-table",
        "template-toolbar",
        "release-stage-grid",
        "secondary-button",
    ):
        assert text in source
    assert '!["drainQuality", "settings"].includes(view)' in source
    assert "h(CodeBlock, { value: profile.parameters })" not in source


def test_drain_config_governance_ui_contract():
    source = source_text()
    for text in (
        "/api/drain-quality/configs",
        "drain-config-governance",
        "config-version-list",
        "config-structured-editor",
        "masking-rule-table",
        "config-ini-editor",
        "config-version-diff",
        "配置校验",
        "人工发布",
        "复制为候选",
        "脱敏规则",
        "INI 原文",
        "readIniValue(draft",
        "drainConfigVersion",
        "available_versions",
    ):
        assert text in source


def test_semantic_dictionary_governance_ui_contract():
    source = source_text()
    for text in (
        "/api/semantic/dictionaries",
        "/api/semantic/test",
        "semantic-dictionary-governance",
        "语义词典",
        "内置规则（只读）",
        "自定义扩展规则",
        "语义测试台",
        "Typed Mask",
        "版本历史",
        "创建候选",
        "配置校验",
        "人工发布",
        "回滚版本",
        "semanticDictionaryVersion",
        "validateSemanticDictionary",
    ):
        assert text in source


def test_semantic_dictionary_selection_updates_test_bench_and_allows_copy():
    source = source_text()
    styles = Path("frontend/src/styles.css").read_text(encoding="utf-8")

    for text in (
        "SEMANTIC_TEST_EXAMPLES",
        'container_runtime: { component: "containerd"',
        'kubernetes: { component: "kubelet"',
        'linux: { component: "kernel"',
        'nvidia: { component: "kernel"',
        "setTestComponent(example.component)",
        "setTestInput(example.message)",
        "setTestResult(null)",
    ):
        assert text in source
    assert "user-select:text" in styles


def test_ai_harness_pages_and_routes_are_present():
    source = source_text()
    for text in (
        "/api/ai-harness/status",
        "/api/ai-harness/prompts",
        "/api/ai-harness/traces",
        "/api/ai-harness/model-profiles",
        "/api/ai-harness/observability/summary",
        "/api/ai-harness/jobs/",
        "savePrompt",
        "function PromptManagement",
        "function AITracePage",
        "function AIObservabilityPage",
        "function ModelProfilesPage",
        "模型画像与上下文预算",
        "新增 Profile",
        "保存 Profile",
        "saveModelProfile",
        "Thinking OFF",
        "Context Budget",
        "model_profile_id",
        "evidence_build_meta",
        "AI 分析观测",
        "规则生成漏斗",
        "最近 AI 事件流",
        "实体级 AI 分析状态",
        "当前任务阶段",
        "查看 AI Trace",
        "查看 Trace",
        "去审批",
        "版本历史",
        "保存当前版本",
        "字段说明",
        "function Tabs",
        "Prompt 内容",
        "关联调用",
        "版本信息",
        "模型输出",
        "校验结果",
        "Evaluator 结果",
        "进入 Evaluator",
        "Evaluator 拦截",
        "证据引用错误",
        "RCA / 建议越界",
        "质量门禁通过率",
        "AI Cache 命中",
        "质量门禁：已通过",
        "evaluator_result",
        "evaluator_status",
        "traceFilters",
        "job_id",
        "trace_id",
        "history.pushState",
        "pathToView",
        "Prompt ",
        "feature_extract_v3_compact_strict_json_en",
        "重试次数",
        "retry_count",
        "来源链路",
        "Lineage 状态",
        "查看来源 AI Trace",
        "template_fingerprint",
    ):
        assert text in source


def test_model_profile_page_manages_provider_connections():
    source = source_text()
    for text in (
        "/api/ai-harness/connections",
        "API 连接",
        "api_key_env",
        "openai_compatible",
        "structured_output_mode",
        "测试连接",
    ):
        assert text in source


def test_model_profile_save_has_feedback_and_edits_top_level_output_budget():
    source = source_text()
    for text in (
        'const [saveState, setSaveState] = useState("idle")',
        "async function saveProfile()",
        'saveState === "saving" ? "保存中…" : "保存 Profile"',
        "Profile 已保存",
        'setField("max_output_tokens"',
        'setField("recommended_input_tokens"',
        "runtime_options",
    ):
        assert text in source


def test_v3_metrics_and_animations_are_present():
    source = source_text()
    for text in (
        "Drain3 实时压缩",
        "今日 LLM 分析日志",
        "分析速度",
        "规则复用收益",
        "Cache 命中",
        "跳过 LLM",
    ):
        assert text in source
    assert "@keyframes" in source
    assert "rolling_60s_logs_per_second" in source


def test_upload_accepts_result_and_raw_log_formats():
    source = source_text()
    assert "accept:" not in source
    assert "Linux messages / syslog 无后缀文件也支持上传" in source
    assert "/api/uploads" in source
    assert "/api/inputs/analyze-upload" in source
    assert "/api/input-jobs/" in source
    assert "file.size > INLINE_MAX_BYTES" in source
    assert "上传进度" in source
    assert "预处理阶段" in source
    assert "Drain3 分片" in source
    assert "drain3_partitions_completed" in source
    assert "drain3_partitions_total" in source
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
    assert "selectedTemplateIndex" in source
    assert "setSelectedTemplateIndex" in source
    assert "selectedTemplate" in source
    assert "setSelectedTemplate" in source
    assert "reviewDraftFromFeature" in source
    assert "props.selectedTemplate" in source
    assert "!selectedTemplate || !selectedTemplate.template_hash" in source
    assert "基于当前证据模板生成审批草稿" in source
    assert "当前证据模板" in source
    assert 'className: "evidence-template " + (selectedTemplateIndex === index ? "active" : "")' in source
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


def test_release_docs_describe_current_feature_version():
    release = Path("releas.md").read_text(encoding="utf-8")
    readme = Path("README.md").read_text(encoding="utf-8")
    agents = Path("AGENTS.md").read_text(encoding="utf-8")

    assert "## 1.16.0 - 2026-07-11" in release
    assert "## 1.16.2 - 2026-07-13" in release
    assert "## 1.17.0 - 2026-07-14" in release
    assert "## 1.17.1 - 2026-07-14" in release
    assert "## 1.18.0 - 2026-07-15" in release
    assert "## 1.19.0 - 2026-07-15" in release
    assert "## 1.19.1 - 2026-07-15" in release
    assert "## 1.19.2 - 2026-07-16" in release
    assert "## 1.20.0 - 2026-07-16" in release
    assert "## 1.20.1 - 2026-07-16" in release
    assert "当前版本：`1.20.1`" in readme
    assert "state/logrisk.sqlite3" in readme
    assert "database/migrations/" in readme
    assert "database/schema.yaml" in readme
    assert "configs/semantic_dictionary/" in readme
    assert "Every code update must also update `releas.md`" in agents
    assert "dashboard.sh restart" in readme
    assert ".txt" in readme
    for text in ("OpenAI-compatible", "LOGRISK_DB_PATH", "REMOTE_LLM_API_KEY", "Promptfoo"):
        assert text in readme
    assert "普通启动不需要 Node.js" in readme
    assert "Vite" not in readme
