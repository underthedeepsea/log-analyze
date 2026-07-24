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


def test_brand_icon_is_published_for_readme_and_browser():
    icon_name = "logrisk-app-icon-orange-v2.png"
    source_icon = FRONTEND / "logo" / icon_name
    runtime_icon = FRONTEND / "dist" / "assets" / icon_name

    assert source_icon.is_file()
    assert runtime_icon.is_file()
    assert runtime_icon.read_bytes() == source_icon.read_bytes()

    assert f"frontend/logo/{icon_name}" in Path("README.md").read_text(encoding="utf-8")
    for html_path in (FRONTEND / "index.html", FRONTEND / "dist" / "index.html"):
        assert f'rel="icon" type="image/png" href="/assets/{icon_name}"' in html_path.read_text(encoding="utf-8")


def test_topbar_uses_brand_icon_without_legacy_subtitle():
    icon_path = "/assets/logrisk-app-icon-orange-v2.png"
    styles = (FRONTEND / "src" / "styles.css").read_text(encoding="utf-8")

    for app_path in (FRONTEND / "src" / "app.js", FRONTEND / "dist" / "assets" / "app.js"):
        app = app_path.read_text(encoding="utf-8")
        assert 'className: "brand-logo"' in app
        assert f'src: "{icon_path}"' in app
        assert "FEATURE REVIEW" not in app
    assert ".brand-logo" in styles


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
    assert '!["drainQuality", "benchmarkCenter", "settings", "rules", "nodeRisks", "semanticLibrary"].includes(view)' in source
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


def test_model_profile_page_configures_registered_extension_adapters_without_token_inputs():
    source = source_text()
    for text in (
        "/api/ai-harness/extensions",
        "modelExtensions",
        'value: "extension"',
        "adapter_id",
        "credential_envs",
        "extension_config",
        "实际 Token 不会保存或展示",
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


def test_rule_lifecycle_governance_ui_contract():
    source = source_text()
    for text in (
        "/api/rule-governance/rules",
        "/api/rule-governance/review-queue",
        "/status",
        "/feedback",
        "/rollback",
        "规则生命周期治理",
        "待复审",
        "ruleReviewQueue",
        "规则健康度",
        "7 天命中",
        "30 天命中",
        "误报率",
        "跨集群命中",
        "版本历史",
        "复审工作台",
        "回滚到此版本",
        "复制诊断信息",
        "当前权限：本地治理者",
        "rule-governance-layout",
        "rule-health-score",
        "rule-version-tree",
    ):
        assert text in source
    assert 'window.confirm("确认回滚规则' in source


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


def test_review_draft_preserves_model_title_and_tags_and_combines_drain3_summary():
    source = source_text()
    assert 'title: feature.title || ""' in source
    assert 'tags: (feature.tags || []).join(", ")' in source
    assert 'const templateSummary = "Drain3 模板：" + (selectedTemplate.template || "暂无模板文本");' in source
    assert 'summary: [feature.summary || "", templateSummary].filter(Boolean).join("\\n\\n")' in source
    assert 'title: component + " " + category + " 特征日志"' not in source
    assert 'tags: tags.join(", ")' not in source


def test_feature_evidence_distinguishes_candidate_from_semantic_templates():
    source = source_text()
    assert "当前特征关联的证据模板" in source
    assert 'templates.length + " 个证据模板"' in source
    assert "当前选择的是 1 个候选特征" in source
    assert "同属当前候选特征" in source
    assert "function evidenceSemanticLabel(template, index)" in source
    assert "risk_semantic" in source
    assert "risk_type" in source
    assert "semantic_fields" in source
    assert "evidence-relation" in source
    assert "semantic-evidence-summary" in source


def test_benchmark_center_has_route_api_and_six_decision_views():
    source = source_text()
    assert '["benchmarkCenter", "◎", "评测与基准"]' in source
    assert 'path === "/benchmark-center" ? "benchmarkCenter"' in source
    assert 'view === "benchmarkCenter" ? "/benchmark-center"' in source
    assert 'function BenchmarkCenterPage(props)' in source
    assert "/api/benchmark-center/overview" in source
    assert "/api/benchmark-center/leaderboard" in source
    assert "/api/benchmark-center/gates/evaluate" in source
    for label in ("质量总览", "Prompt 对比", "模型排行榜", "失败 Case", "质量趋势", "发布门禁"):
        assert label in source


def test_real_benchmark_requires_explicit_confirmation_and_diagnostics():
    source = source_text()
    assert "真实模型运行会产生调用成本，必须人工确认" in source
    assert 'type: "checkbox"' in source
    assert "复制诊断信息" in source
    assert "Benchmark 数据加载失败" in source
    assert "连接不可用，不能启动真实模型评测" in source
    assert "connection_ready" in source


def test_benchmark_overview_displays_unified_source_asset_inventory():
    source = source_text()
    assert "统一资产来源" in source
    for label in ("AI Trace", "模型 Profile", "Prompt", "Drain3 评测", "Drain3 模板", "Canonical Case"):
        assert label in source


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
    assert "## 1.21.0 - 2026-07-16" in release
    assert "## 1.22.0 - 2026-07-18" in release
    assert "## 1.23.0 - 2026-07-19" in release
    assert "## 1.23.1 - 2026-07-21" in release
    assert "当前版本：`1.25.1`" in readme
    assert "## 1.24.0 - 2026-07-22" in release
    assert "## 1.24.1 - 2026-07-22" in release
    assert "## 1.24.2 - 2026-07-22" in release
    assert "## 1.25.0 - 2026-07-23" in release
    assert "## 1.25.1 - 2026-07-24" in release
    assert "qwen3.5:9b-mlx" in readme
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


def test_node_risk_and_editable_semantic_ui_contract():
    source = source_text()
    for text in (
        '"nodeRisks", "△", "服务器风险"',
        '"semanticLibrary", "≋", "风险语义库"',
        'function NodeRiskPage',
        'function SemanticLibraryPage',
        '"/api/node-risks"',
        '"/api/semantics"',
        '"创建覆盖草稿"',
        '"24h 风险事件数"',
        'occurrence_count',
    ):
        assert text in source


def test_sidebar_is_collapsible_scrollable_and_uses_dynamic_glass_selection():
    source = (FRONTEND / "src" / "app.js").read_text(encoding="utf-8")
    styles = (FRONTEND / "src" / "styles.css").read_text(encoding="utf-8")

    assert 'className: "sidebar-scroll"' in source
    assert 'className: "sidebar-rail "' in source
    assert 'className: "sidebar-thumb"' in source
    assert 'className: "sidebar-toggle"' in source
    assert 'className: "sidebar-overlay"' in source
    assert 'className: "nav-icon"' not in source
    assert "boundary-note" not in source
    assert "sidebarCollapsed" in source
    assert "mobileMenuOpen" in source
    assert "startSidebarThumbDrag" in source
    for marker in (
        ".sidebar-scroll",
        ".app-shell.sidebar-collapsed",
        ".app-shell.mobile-menu-open",
        "@keyframes navGlassShine",
        "@keyframes navGlassLift",
        "prefers-reduced-motion:reduce",
    ):
        assert marker in styles


def test_active_navigation_glass_is_light_and_does_not_bold_text():
    styles = (FRONTEND / "src" / "styles.css").read_text(encoding="utf-8")
    active_rule = styles.rsplit(".sidebar .nav-item.active{", 1)[1].split("}", 1)[0]

    assert "font-weight:400" in active_rule
    assert "rgba(255,226,207,.28)" in active_rule
    assert "background:linear-gradient(180deg" in active_rule
    assert "inset 0 2px 0 #fff" not in active_rule


def test_system_settings_exposes_postgres_candidate_configuration_without_password_field():
    app = (FRONTEND / "src" / "app.js").read_text(encoding="utf-8")

    assert "/api/system/database" in app
    assert "/api/system/database/config" in app
    assert "PostgreSQL 运行数据库" in app
    assert "密码环境变量名" in app
    assert 'type: "password"' not in app
