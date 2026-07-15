(function () {
  "use strict";
  const h = React.createElement;
  const { useEffect, useMemo, useRef, useState } = React;
  const INLINE_MAX_BYTES = 10 * 1024 * 1024;
  const LARGE_CHUNK_BYTES = 1024 * 1024;
  const DEPLOYMENT_API_BASE = String(window.LOGRISK_CONFIG && window.LOGRISK_CONFIG.apiBase || "").replace(/\/$/, "");

  function currentApiBase() {
    const configured = String(localStorage.getItem("logrisk.apiBase") || DEPLOYMENT_API_BASE || (window.location.origin === "null" ? "" : window.location.origin));
    return configured.replace(/\/$/, "");
  }

  function apiUrl(path) { return currentApiBase() + path; }

  async function jsonRequest(path, options) {
    const response = await fetch(apiUrl(path), Object.assign({}, options, {
      headers: Object.assign({ "Content-Type": "application/json" }, (options && options.headers) || {}),
    }));
    const payload = await response.json().catch(function () { return {}; });
    if (!response.ok) throw new Error(payload.error || "请求失败 (" + response.status + ")");
    return payload;
  }

  const api = {
    health: function () { return jsonRequest("/api/health"); },
    config: function () { return jsonRequest("/api/config"); },
    status: function () { return jsonRequest("/api/ollama/status"); },
    rules: function () { return jsonRequest("/api/rules"); },
    metrics: function () { return jsonRequest("/api/metrics"); },
    drainDatasets: function () { return jsonRequest("/api/drain-quality/datasets"); },
    drainAnnotations: function () { return jsonRequest("/api/drain-quality/annotations"); },
    drainEvalRuns: function () { return jsonRequest("/api/drain-quality/eval-runs"); },
    drainProfiles: function () { return jsonRequest("/api/drain-quality/profiles"); },
    drainTemplates: function () { return jsonRequest("/api/drain-quality/templates"); },
    drainConfigs: function () { return jsonRequest("/api/drain-quality/configs"); },
    createDrainConfig: function (payload) { return jsonRequest("/api/drain-quality/configs", { method: "POST", body: JSON.stringify(payload) }); },
    saveDrainConfig: function (configId, payload) { return jsonRequest("/api/drain-quality/configs/" + encodeURIComponent(configId) + "/versions", { method: "POST", body: JSON.stringify(payload) }); },
    validateDrainConfig: function (configId, payload) { return jsonRequest("/api/drain-quality/configs/" + encodeURIComponent(configId) + "/validate", { method: "POST", body: JSON.stringify(payload) }); },
    publishDrainConfig: function (configId, payload) { return jsonRequest("/api/drain-quality/configs/" + encodeURIComponent(configId) + "/publish", { method: "POST", body: JSON.stringify(payload) }); },
    rollbackDrainConfig: function (configId, payload) { return jsonRequest("/api/drain-quality/configs/" + encodeURIComponent(configId) + "/rollback", { method: "POST", body: JSON.stringify(payload) }); },
    importDrainTemplates: function (templates) { return jsonRequest("/api/drain-quality/templates/import", { method: "POST", body: JSON.stringify({ templates: templates }) }); },
    annotateDrainTemplate: function (payload) { return jsonRequest("/api/drain-quality/annotations", { method: "POST", body: JSON.stringify(payload) }); },
    changeDrainTemplate: function (templateHash, payload) { return jsonRequest("/api/drain-quality/templates/" + encodeURIComponent(templateHash) + "/changes", { method: "POST", body: JSON.stringify(payload) }); },
    rollbackDrainTemplate: function (templateHash, payload) { return jsonRequest("/api/drain-quality/templates/" + encodeURIComponent(templateHash) + "/rollback", { method: "POST", body: JSON.stringify(payload) }); },
    promoteDrainProfile: function (profileId, action, payload) { return jsonRequest("/api/drain-quality/profiles/" + encodeURIComponent(profileId) + "/" + action, { method: "POST", body: JSON.stringify(payload) }); },
    harnessStatus: function () { return jsonRequest("/api/ai-harness/status"); },
    observabilitySummary: function () { return jsonRequest("/api/ai-harness/observability/summary"); },
    observabilityProgress: function (jobId) { return jsonRequest("/api/ai-harness/jobs/" + encodeURIComponent(jobId) + "/progress"); },
    observabilityEvents: function (jobId) { return jsonRequest("/api/ai-harness/jobs/" + encodeURIComponent(jobId) + "/events"); },
    observabilityRecentEvents: function () { return jsonRequest("/api/ai-harness/events/recent?limit=100"); },
    prompts: function () { return jsonRequest("/api/ai-harness/prompts"); },
    modelProfiles: function () { return jsonRequest("/api/ai-harness/model-profiles"); },
    saveModelProfile: function (profile) { return jsonRequest("/api/ai-harness/model-profiles", { method: "POST", body: JSON.stringify(profile) }); },
    prompt: function (id) { return jsonRequest("/api/ai-harness/prompts/" + encodeURIComponent(id)); },
    savePrompt: function (id, content, note) { return jsonRequest("/api/ai-harness/prompts/" + encodeURIComponent(id), { method: "PATCH", body: JSON.stringify({ content: content, note: note || "" }) }); },
    traces: function (query) { return jsonRequest("/api/ai-harness/traces" + (query || "")); },
    trace: function (id) { return jsonRequest("/api/ai-harness/traces/" + encodeURIComponent(id)); },
    job: function (id) { return jsonRequest("/api/jobs/" + id); },
    createJob: function (result, model, minScore, promptId, modelProfileId, retryCount) {
      return jsonRequest("/api/jobs", { method: "POST", body: JSON.stringify({ result: result, model: model, min_score: Number(minScore), prompt_id: promptId, model_profile_id: modelProfileId, retry_count: Number(retryCount), cache_enabled: true }) });
    },
    retry: function (jobId, entityId) {
      return jsonRequest("/api/jobs/" + jobId + "/entities/" + encodeURIComponent(entityId) + "/retry", { method: "POST", body: "{}" });
    },
    update: function (jobId, candidateId, changes) {
      return jsonRequest("/api/jobs/" + jobId + "/features/" + candidateId, { method: "PATCH", body: JSON.stringify(changes) });
    },
    async analyzeFile(file) {
      if (file.size > INLINE_MAX_BYTES) return this.uploadAndAnalyzeLargeFile(file);
      const content = await file.text();
      if (file.name.toLowerCase().endsWith(".json")) {
        try {
          const value = JSON.parse(content);
          if (value && Array.isArray(value.risk_entities)) return value;
        } catch (_) { /* Server returns the authoritative format error. */ }
      }
      const payload = await jsonRequest("/api/inputs/analyze", { method: "POST", body: JSON.stringify({ filename: file.name, content: content }) });
      return payload.result;
    },
    async uploadAndAnalyzeLargeFile(file, callbacks) {
      callbacks = callbacks || {};
      const session = await jsonRequest("/api/uploads", { method: "POST", body: JSON.stringify({ filename: file.name, size_bytes: file.size, chunk_size_bytes: LARGE_CHUNK_BYTES }) });
      for (let index = 0; index < session.total_chunks; index++) {
        const start = index * session.chunk_size_bytes;
        const response = await fetch(apiUrl("/api/uploads/" + session.upload_id + "/chunks/" + index), { method: "PUT", headers: { "Content-Type": "application/octet-stream" }, body: file.slice(start, Math.min(file.size, start + session.chunk_size_bytes)) });
        const chunkStatus = await response.json().catch(function () { return {}; });
        if (!response.ok) throw new Error(chunkStatus.error || "分片上传失败");
        callbacks.onUploadProgress && callbacks.onUploadProgress(chunkStatus);
      }
      await jsonRequest("/api/uploads/" + session.upload_id + "/complete", { method: "POST", body: "{}" });
      const job = await jsonRequest("/api/inputs/analyze-upload", { method: "POST", body: JSON.stringify({ upload_id: session.upload_id, filename: file.name }) });
      while (true) {
        const progress = await jsonRequest("/api/input-jobs/" + job.input_job_id);
        callbacks.onPreprocessProgress && callbacks.onPreprocessProgress(progress);
        if (progress.status === "completed") {
          const payload = await jsonRequest("/api/input-jobs/" + job.input_job_id + "/result");
          return payload.result;
        }
        if (progress.status === "failed") throw new Error(progress.error || "大文件预处理失败");
        await new Promise(function (resolve) { setTimeout(resolve, 1200); });
      }
    },
    subscribe: function (jobId, onEvent) {
      const source = new EventSource(apiUrl("/api/jobs/" + jobId + "/events?cursor=0"));
      ["job_created", "job_started", "entity_rule_matched", "entity_cache_hit", "entity_started", "entity_retrying", "entity_completed", "entity_failed", "feature_updated", "job_completed"].forEach(function (type) {
        source.addEventListener(type, onEvent);
      });
      return source;
    },
    async exportApproved(jobId) {
      const response = await fetch(apiUrl("/api/jobs/" + jobId + "/export"), { method: "POST", body: "{}" });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "导出失败");
      const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = "logrisk-feature-package-" + new Date().toISOString().slice(0, 10) + ".json";
      anchor.click();
      URL.revokeObjectURL(url);
    },
  };

  const navItems = [
    ["overview", "▦", "特征总览"], ["queue", "◫", "识别队列"], ["observability", "◉", "AI 分析观测"], ["traces", "⌁", "AI 调用追踪"], ["prompts", "{}", "Prompt 管理"], ["modelProfiles", "◌", "模型画像"], ["review", "✓", "人工审批"],
    ["rules", "⌘", "批准规则库"], ["drainQuality", "◇", "评测中心 · 模板质量"], ["export", "⇩", "导出记录"], ["settings", "⚙", "系统设置"],
  ];
  const statusNames = { queued: "等待分析", running: "识别中", completed: "Ollama 完成", failed: "识别失败", skipped: "低风险跳过", rule_matched: "规则复用" };

  function Sidebar(props) {
    return h("aside", { className: "sidebar" },
      h("div", { className: "nav-label" }, "工作区"),
      navItems.map(function (item) {
        return h("button", { key: item[0], type: "button", className: "nav-item " + (props.active === item[0] ? "active" : ""), onClick: function () { props.onChange(item[0]); } },
          h("span", { className: "nav-icon" }, item[1]), h("span", null, item[2]));
      }),
      h("div", { className: "boundary-note" }, h("strong", null, "系统边界"), "Ollama 只识别聚合日志特征，本系统不输出 RCA 结论。"));
  }

  function Metric(props) {
    return h("div", { className: "metric-card " + (props.tone || "") }, h("strong", null, String(props.value)), h("span", null, props.label));
  }

  function shortHash(value) { return value ? String(value).slice(0, 10) : "—"; }
  function timeText(value) { return value ? new Date(value).toLocaleString() : "—"; }
  function pathToView(path) { return path === "/prompts" ? "prompts" : (path === "/model-profiles" ? "modelProfiles" : (path === "/ai-traces" ? "traces" : (path === "/ai-observability" ? "observability" : (path === "/drain-quality" ? "drainQuality" : (path === "/settings" ? "settings" : "overview"))))); }
  function routeForView(view) { return view === "prompts" ? "/prompts" : (view === "modelProfiles" ? "/model-profiles" : (view === "traces" ? "/ai-traces" : (view === "observability" ? "/ai-observability" : (view === "drainQuality" ? "/drain-quality" : (view === "settings" ? "/settings" : "/"))))); }
  function traceFiltersFromSearch(search) {
    const params = new URLSearchParams(search || "");
    return { job_id: params.get("job_id") || "", trace_id: params.get("trace_id") || "", status: params.get("status") || "", prompt_id: params.get("prompt_id") || "" };
  }
  function traceFilterQuery(filters) {
    const params = new URLSearchParams();
    Object.keys(filters).forEach(function (key) { if (filters[key]) params.set(key, filters[key]); });
    const query = params.toString();
    return query ? "?" + query : "?limit=50";
  }
  const analysisLabels = { feature_extract: "日志特征识别", rca_analysis: "RCA 证据分析", rule_generate: "规则候选生成", false_positive_review: "误报复核", risk_summary: "风险摘要生成" };
  const traceStatus = { success: "成功", cache_hit: "Cache 命中", validation_failed: "校验失败", evaluator_failed: "Evaluator 拦截", parse_failed: "解析失败", model_failed: "调用失败", trace_failed: "Trace 异常" };
  const promptFieldHelp = {
    prompt_id: "Prompt 文件名去掉 .md 后的唯一标识，分析任务会按它加载内容。",
    display_name: "给人工看的名称，用于区分不同 Prompt 版本。",
    description: "说明这个 Prompt 适用的分析目标和边界。",
    analysis_type: "分析流程类型；当前默认用于日志特征识别。",
    status: "active 可被选择，draft/deprecated 仅作管理展示。",
    is_default: "同一分析流程默认使用的 Prompt。",
    prompt_hash: "由 Prompt 内容计算出的 SHA256，用于审计版本。",
    path: "当前 Prompt 文件在仓库中的路径。",
    version: "人工维护的版本标签，内容差异以 hash 为准。",
  };

  function MetricsGrid(props) {
    const summary = props.result && props.result.summary || props.snapshot && props.snapshot.source_summary || {};
    const stats = props.snapshot && props.snapshot.log_statistics || {};
    const live = props.snapshot && props.snapshot.live_metrics || {};
    const daily = props.daily || {};
    return h("section", { className: "metrics-grid", "aria-label": "实时处理指标" },
      h(Metric, { label: "原始日志", value: stats.original_logs != null ? stats.original_logs : summary.total_raw_logs || "—" }),
      h(Metric, { label: "Drain3 压缩率", value: (stats.drain3_compression_ratio_percent != null ? stats.drain3_compression_ratio_percent : summary.drain3_compression_ratio_percent || 0) + "%", tone: "orange" }),
      h(Metric, { label: "模板窗口", value: stats.template_windows != null ? stats.template_windows : summary.total_template_windows || "—" }),
      h(Metric, { label: "今日 LLM 分析日志", value: live.today_llm_logs != null ? live.today_llm_logs : daily.today_llm_logs || 0 }),
      h(Metric, { label: "分析速度", value: (live.processing_logs_per_second || 0) + " 条/秒" }),
      h(Metric, { label: "Cache 命中", value: live.cache_hit_calls || 0, tone: "green" }),
      h(Metric, { label: "历史规则复用", value: live.saved_llm_calls || 0, tone: "green" }));
  }

  function LiveProcessing(props) {
    const snapshot = props.snapshot || {};
    const progress = snapshot.progress || { percent: 0, completed: 0, total: 0 };
    const stats = snapshot.log_statistics || props.result && props.result.summary || {};
    const live = snapshot.live_metrics || {};
    const raw = stats.original_logs != null ? stats.original_logs : stats.total_raw_logs || 0;
    const windows = stats.template_windows != null ? stats.template_windows : stats.total_template_windows || 0;
    const reduced = stats.drain3_reduced_logs != null ? stats.drain3_reduced_logs : Math.max(0, raw - windows);
    const ratio = stats.drain3_compression_ratio_percent || 0;
    const reusePercent = progress.total ? Math.round((live.saved_llm_calls || 0) / progress.total * 100) : 0;
    return h(React.Fragment, null,
      h("section", { className: "progress-panel" },
        h("div", { className: "panel-title-row" }, h("div", { className: "live-title" }, h("i"), snapshot.status === "running" ? "正在分析日志特征" : "日志特征分析进度"), h("strong", null, (progress.percent || 0) + "%")),
        h("div", { className: "progress-track" }, h("div", { className: "progress-fill", style: { width: (progress.percent || 0) + "%" } })),
        h("div", { className: "progress-meta" }, h("span", null, (progress.completed || 0) + " / " + (progress.total || 0) + " 个实体已处理"), h("span", null, "预计剩余 " + (live.eta_seconds == null ? "—" : live.eta_seconds + " 秒")))),
      h("section", { className: "realtime-grid" },
        h("div", { className: "compression-card" },
          h("div", { className: "panel-title-row" }, h("b", null, "Drain3 实时压缩"), h("span", null, "持续更新")),
          h("div", { className: "compression-flow" }, h("div", null, h("strong", null, raw), h("span", null, "输入日志行")), h("div", { className: "flow-arrow" }, "⟶"), h("div", { className: "compressed" }, h("strong", null, windows), h("span", null, "模板窗口"))),
          h("div", { className: "compression-track" }, h("div", { style: { width: ratio + "%" } })), h("small", null, "压缩率 " + ratio + "% · 已减少 " + reduced + " 条重复日志")),
        h("div", { className: "speed-card" },
          h("div", { className: "panel-title-row" }, h("b", null, "分析速度"), h("span", null, "近 60 秒")),
          h("svg", { viewBox: "0 0 300 70", preserveAspectRatio: "none", "aria-label": "分析速度趋势" }, h("path", { className: "speed-area", d: "M0 60 C30 56 46 38 72 46 S116 28 146 35 S194 18 221 27 S266 10 300 17 L300 70 L0 70Z" }), h("path", { className: "speed-line", d: "M0 60 C30 56 46 38 72 46 S116 28 146 35 S194 18 221 27 S266 10 300 17" })), h("strong", null, (live.rolling_60s_logs_per_second || 0) + " 条/秒")),
        h("div", { className: "benefit-wrap" }, h("div", { className: "benefit-orb" }, h("b", null, "规则复用收益"), h("strong", null, reusePercent + "%"), h("span", null, "减少 Ollama 调用"), h("hr"), h("small", null, (live.saved_llm_calls || 0) + " 个实体跳过模型", h("br"), "Cache 命中 " + (live.cache_hit_calls || 0) + " 次 · 节省 " + (live.saved_llm_logs || 0) + " 条日志")))));
  }

  function Workspace(props) {
    const entities = props.snapshot && props.snapshot.entities || [];
    const features = props.snapshot && props.snapshot.features || [];
    return h("section", { className: "workspace-grid" },
      h("div", { className: "surface queue-surface" }, h("div", { className: "surface-head" }, h("b", null, "风险实体与识别状态"), h("span", null, entities.length + " 个实体")),
        h("div", { className: "entity-list" }, entities.length === 0 && h("div", { className: "empty-state" }, "上传日志后显示识别队列"), entities.map(function (entity) {
          const traceCount = features.filter(function (feature) { return feature.entity && feature.entity.id === entity.entity_id && feature.trace_id; }).length;
          return h("div", { className: "entity-row", key: (entity.cluster || "") + "-" + entity.entity_id }, h("div", null, h("b", null, entity.entity_id), h("span", null, (entity.cluster || "default") + " · " + entity.log_count + " 条关联日志 · AI 调用 " + traceCount + " 次")), h("span", { className: "status-chip " + entity.status }, statusNames[entity.status] || entity.status), h("span", { className: "risk-score" }, Number(entity.risk_score || 0).toFixed(0)), h("button", { className: "text-button", onClick: function () { props.onOpenObservability(props.snapshot.job_id || ""); } }, "查看 AI 观测"), traceCount > 0 && h("button", { className: "text-button", onClick: function () { props.onOpenTraces("?job_id=" + encodeURIComponent(props.snapshot.job_id || "")); } }, "查看 Trace"), entity.cache_hit && h("span", { className: "skip-llm" }, "Cache 命中"), entity.status === "rule_matched" && h("span", { className: "skip-llm" }, "跳过 LLM"), entity.status === "failed" && h("button", { className: "text-button", onClick: function () { props.onRetry(entity.entity_id); } }, "重试"));
        }))),
      h("div", { className: "surface feature-surface" }, h("div", { className: "surface-head" }, h("b", null, "候选与复用特征"), h("span", null, features.length + " 条")),
        h("div", { className: "feature-list" }, features.length === 0 && h("div", { className: "empty-state" }, "识别后显示候选特征"), features.map(function (feature) {
          return h("button", { className: "feature-row " + (props.selectedId === feature.candidate_id ? "active" : ""), key: feature.candidate_id, onClick: function () { props.onSelect(feature.candidate_id); } }, h("div", null, h("b", null, feature.title), h("span", null, (feature.entity && feature.entity.id || "") + " · " + feature.summary)), h("span", { className: "status-chip " + feature.status }, feature.origin === "approved_rule" ? "规则复用" : feature.status));
        }))));
  }

  function FeatureList(props) {
    const features = props.features || [];
    return h("section", { className: "surface approval-feature-list" },
      h("div", { className: "surface-head" }, h("b", null, "候选特征"), h("span", null, features.length + " 条")),
      h("div", { className: "feature-list" },
        features.length === 0 && h("div", { className: "empty-state" }, "暂无可审批特征"),
        features.map(function (feature) {
          return h("button", {
            className: "feature-row " + (props.selectedId === feature.candidate_id ? "active" : ""),
            key: feature.candidate_id,
            onClick: function () { props.onSelect(feature.candidate_id); },
          }, h("div", null, h("b", null, feature.title), h("span", null,
            (feature.entity && feature.entity.id || "unknown") + " · " + feature.summary), h("small", null, "质量门禁：" + (feature.evaluator_result && feature.evaluator_result.passed ? "已通过" : "未记录") + (feature.trace_id ? " · 来源：" + (feature.prompt_id || "feature_extract_v3_compact_strict_json_en") + " · " + (feature.model || "—") + " · " + shortHash(feature.trace_id) : " · 来源：历史数据 / 未记录 Trace"))),
          h("span", { className: "status-chip " + feature.status },
            feature.origin === "approved_rule" ? "规则复用" : feature.status));
        })
      )
    );
  }

  function FeatureEvidence(props) {
    const feature = props.feature;
    const templates = feature && feature.source_templates || [];
    const [selectedTemplateIndex, setSelectedTemplateIndex] = useState(0);
    useEffect(function () {
      setSelectedTemplateIndex(0);
      props.onSelectTemplate && props.onSelectTemplate(templates[0] || null);
    }, [feature && feature.candidate_id]);
    return h("section", { className: "surface evidence-panel" },
      h("div", { className: "surface-head" }, h("b", null, "特征日志证据"), h("span", null, templates.length + " 个模板")),
      h("div", { className: "evidence-body" },
        h("div", { className: "evidence-notice" }, "当前展示 Drain3 脱敏特征模板，系统未保存原始日志"),
        !feature && h("div", { className: "empty-state" }, "选择特征后查看日志证据"),
        feature && templates.length === 0 && h("div", { className: "empty-state" }, "暂无脱敏模板证据"),
        templates.map(function (template, index) {
          const firstSeen = template.first_seen || feature.window_start || "—";
          const lastSeen = template.last_seen || feature.window_end || "—";
          return h("button", { className: "evidence-template " + (selectedTemplateIndex === index ? "active" : ""), key: (template.template_hash || "template") + "-" + index, onClick: function () { setSelectedTemplateIndex(index); props.onSelectTemplate && props.onSelectTemplate(template); } },
            h("div", { className: "evidence-meta" },
              h("span", null, template.component || "unknown"),
              h("span", null, template.category || "unknown"),
              h("span", null, template.severity || "unknown"),
              h("b", null, String(template.count || 0) + " 次")),
            h("code", null, template.template || "无模板文本"),
            h("small", null, "Hash " + (template.template_hash || "—")),
            h("small", null, firstSeen + " — " + lastSeen));
        })
      )
    );
  }

  function reviewDraftFromFeature(feature, selectedTemplate) {
    if (!feature) return null;
    if (!selectedTemplate || !selectedTemplate.template_hash) {
      return { title: feature.title || "", summary: feature.summary || "", importance: feature.importance || "medium", tags: (feature.tags || []).join(", "), reviewer_note: feature.reviewer_note || "" };
    }
    const component = selectedTemplate.component || "unknown";
    const category = selectedTemplate.category || selectedTemplate.severity || "日志";
    const template = selectedTemplate.template || "暂无模板文本";
    const tags = Array.from(new Set([component, selectedTemplate.severity, selectedTemplate.category].filter(Boolean)));
    return {
      title: component + " " + category + " 特征日志",
      summary: "检测到 " + component + " 组件的 " + category + " 日志模板：" + template,
      importance: feature.importance || "medium",
      tags: tags.join(", "),
      reviewer_note: "基于当前证据模板生成审批草稿：" + selectedTemplate.template_hash,
    };
  }

  function ReviewEditor(props) {
    const [draft, setDraft] = useState(null);
    const selectedTemplate = props.selectedTemplate || {};
    useEffect(function () {
      setDraft(reviewDraftFromFeature(props.feature, props.selectedTemplate));
    }, [props.feature, props.selectedTemplate]);
    if (!props.feature || !draft) return h("section", { className: "surface review-editor empty-state" }, "选择一条特征进行人工审批");
    function field(label, key, type) {
      const controlProps = { value: draft[key], onChange: function (event) { setDraft(Object.assign({}, draft, { [key]: event.target.value })); } };
      return h("label", null, label, type === "textarea" ? h("textarea", controlProps) : h("input", controlProps));
    }
    function save(status) { props.onSave(Object.assign({}, draft, { tags: draft.tags.split(",").map(function (tag) { return tag.trim(); }).filter(Boolean), status: status })); }
    return h("section", { className: "surface review-editor" }, h("div", { className: "surface-head" }, h("b", null, "人工审批"), h("span", null, props.feature.origin === "approved_rule" ? "来自批准规则库" : "来自 Ollama")), h("div", { className: "editor-body" }, field("特征标题", "title"), field("特征摘要", "summary", "textarea"), field("标签（逗号分隔）", "tags"), field("审批备注", "reviewer_note", "textarea"), h("div", { className: "fact-box" }, "当前证据模板：", selectedTemplate.template_hash || "—", h("br"), "组件 " + (selectedTemplate.component || "—") + " · 类别 " + (selectedTemplate.category || "—") + " · 次数 " + (selectedTemplate.count || 0), h("br"), selectedTemplate.template || "暂无模板文本"), h("div", { className: "fact-box" }, "质量门禁：已通过", h("br"), "Evaluator Score：" + (props.feature.evaluator_result && props.feature.evaluator_result.score != null ? props.feature.evaluator_result.score : "—"), h("br"), "实体 " + (props.feature.entity && props.feature.entity.id || "") + " · 风险分 " + props.feature.risk_score + " · 出现 " + props.feature.occurrence_count + " 次", h("br"), props.feature.trace_id ? "来源 " + (props.feature.prompt_id || "feature_extract_v3_compact_strict_json_en") + " · " + (props.feature.model || "—") + " · " + props.feature.trace_id : "来源：历史数据 / 未记录 Trace"), props.feature.trace_id && h("button", { className: "text-button trace-link", onClick: function () { props.onOpenTrace(props.feature.trace_id); } }, "查看 AI Trace"), h("div", { className: "editor-actions" }, h("button", { className: "reject-button", onClick: function () { save("rejected"); } }, "驳回"), h("button", { className: "primary-button", onClick: function () { save("approved"); } }, "批准并写入规则库"))));
  }

  function PromptManagement(props) {
    const items = props.prompts || [];
    const active = items.filter(function (item) { return item.status === "active"; }).length;
    const types = Array.from(new Set(items.map(function (item) { return item.analysis_type; })));
    const groups = types.map(function (type) { return [type, items.filter(function (item) { return item.analysis_type === type; })]; });
    return h(React.Fragment, null,
      h("section", { className: "metrics-grid" }, h(Metric, { label: "Prompt 总数", value: items.length }), h(Metric, { label: "启用中", value: active, tone: "green" }), h(Metric, { label: "分析流程数", value: types.length }), h(Metric, { label: "默认 Prompt", value: props.currentPrompt || "—", tone: "orange" })),
      h("section", { className: "surface harness-surface" }, h("div", { className: "surface-head" }, h("b", null, "Prompt 管理"), h("span", null, "管理不同 AI 分析流程使用的 Prompt 版本、默认配置和调用关系")),
        items.length === 0 && h("div", { className: "empty-state" }, "暂无 Prompt，请检查 prompts/ 目录和 Prompt Registry 配置。"),
        groups.map(function (group) { return h("div", { className: "prompt-group", key: group[0] }, h("h3", null, analysisLabels[group[0]] || group[0]), group[1].map(function (prompt) { return h("div", { className: "prompt-row", key: prompt.prompt_id }, h("div", { className: "prompt-name" }, h("b", null, prompt.display_name || prompt.prompt_id), h("span", null, prompt.prompt_id + " · " + (prompt.description || ""))), h("span", { className: "status-chip " + prompt.status }, prompt.status === "active" ? "启用" : prompt.status), h("span", null, prompt.is_default ? "默认" : "—"), h("code", null, shortHash(prompt.prompt_hash)), h("span", null, (prompt.used_by_models || []).join(", ") || "—"), h("span", null, timeText(prompt.last_used_at)), h("button", { className: "text-button", onClick: function () { props.onOpenPrompt(prompt.prompt_id); } }, "查看")); })); })));
  }

  function FieldHelp(props) {
    return h("div", { className: "field-help" }, h("b", null, props.name), h("span", null, props.value == null || props.value === "" ? "—" : String(props.value)), h("small", null, props.help));
  }

  function Tabs(props) {
    return h("div", { className: "tabs" }, props.items.map(function (item) {
      return h("button", { key: item[0], className: props.active === item[0] ? "active" : "", onClick: function () { props.onChange(item[0]); } }, item[1]);
    }));
  }

  function PromptDrawer(props) {
    const item = props.item;
    const [tab, setTab] = useState("overview");
    const [content, setContent] = useState(item.content || "");
    const [note, setNote] = useState("");
    useEffect(function () { setContent(item.content || ""); setNote(""); }, [item.prompt_id, item.prompt_hash]);
    const overview = h(React.Fragment, null,
      h("h3", null, "字段说明"),
      h("div", { className: "field-grid" }, Object.keys(promptFieldHelp).map(function (key) { return h(FieldHelp, { key: key, name: key, value: key === "prompt_hash" ? shortHash(item[key]) : item[key], help: promptFieldHelp[key] }); })));
    const promptContent = h(React.Fragment, null,
      h("h3", null, "Prompt 内容（当前版本可编辑）"),
      h("textarea", { className: "prompt-editor", value: content, onChange: function (event) { setContent(event.target.value); } }),
      h("label", { className: "prompt-note" }, "变更说明", h("input", { value: note, placeholder: "例如：补充 JSON 输出约束", onChange: function (event) { setNote(event.target.value); } })),
      h("div", { className: "editor-actions drawer-actions" }, h("button", { className: "primary-button", onClick: function () { props.onSave(item.prompt_id, content, note); } }, "保存当前版本")));
    const recent = item.recent_traces || [];
    const calls = h(React.Fragment, null,
      h("h3", null, "关联调用"),
      recent.length === 0 && h("p", { className: "drawer-empty" }, "暂无关联调用。"),
      recent.map(function (trace) {
        return h("article", { className: "trace-card", key: trace.trace_id }, h("b", null, shortHash(trace.trace_id)), h("span", null, timeText(trace.created_at) + " · " + (trace.model || "—") + " · " + (traceStatus[trace.status] || trace.status)), h("button", { className: "text-button", onClick: function () { props.onOpenTrace(trace.trace_id); } }, "查看 Trace"));
      }));
    const versions = h(React.Fragment, null,
      h("h3", null, "版本信息"),
      h(CodeBlock, { value: { version: item.version, prompt_hash: item.prompt_hash, path: item.path, status: item.status, is_default: item.is_default, created_at: item.created_at, updated_at: item.updated_at } }),
      h("h3", null, "版本历史"),
      (item.history || []).length === 0 && h("p", { className: "drawer-empty" }, "暂无历史版本。首次保存后，旧内容会进入版本历史。"),
      (item.history || []).map(function (version) { return h("article", { className: "version-card", key: version.saved_at + version.sha256 }, h("b", null, shortHash(version.sha256)), h("span", null, timeText(version.saved_at) + (version.note ? " · " + version.note : "")), h(CodeBlock, { value: version.content })); }));
    return h(React.Fragment, null,
      h(Tabs, { active: tab, onChange: setTab, items: [["overview", "概览"], ["content", "Prompt 内容"], ["calls", "关联调用"], ["versions", "版本信息"]] }),
      tab === "overview" && overview,
      tab === "content" && promptContent,
      tab === "calls" && calls,
      tab === "versions" && versions);
  }

  function AITracePage(props) {
    const traces = props.traces || [];
    const status = props.harness || {};
    return h(React.Fragment, null,
      h("section", { className: "metrics-grid" }, h(Metric, { label: "今日 AI 调用", value: status.today_calls || 0 }), h(Metric, { label: "成功率", value: Math.round((status.success_rate || 0) * 100) + "%", tone: "green" }), h(Metric, { label: "平均耗时", value: (status.avg_latency_ms || 0) + " ms" }), h(Metric, { label: "当前 Prompt", value: status.current_prompt_id || "—", tone: "orange" }), h(Metric, { label: "Trace 状态", value: status.trace_enabled ? "ON" : "OFF" })),
      h("section", { className: "surface harness-surface" }, h("div", { className: "surface-head" }, h("b", null, "AI 调用追踪"), h("span", null, "查看每次模型调用的 Prompt、Evidence、输出和校验状态")),
        h("div", { className: "trace-filters" }, ["job_id", "trace_id", "status", "prompt_id"].map(function (key) {
          return h("label", { key: key }, key, h("input", { value: props.traceFilters[key] || "", onChange: function (event) { props.onFilter(Object.assign({}, props.traceFilters, { [key]: event.target.value })); } }));
        }), h("button", { className: "text-button", onClick: function () { props.onFilter({ job_id: "", trace_id: "", status: "", prompt_id: "" }); } }, "清空过滤")),
        traces.length === 0 && h("div", { className: "empty-state" }, "暂无 AI 调用记录。完成一次日志特征识别后会在这里记录。"),
        traces.length > 0 && h("div", { className: "trace-table" }, h("div", { className: "trace-head" }, "时间", "Job", "实体", "Prompt", "模型", "状态", "耗时", "操作"), traces.map(function (trace) { return h("div", { className: "trace-row", key: trace.trace_id }, h("span", null, timeText(trace.created_at)), h("code", null, shortHash(trace.job_id)), h("span", null, (trace.entity_type || "") + ":" + (trace.entity_id || "—")), h("span", null, trace.prompt_id), h("span", null, trace.model || "—"), h("span", { className: "status-chip " + trace.status }, traceStatus[trace.status] || trace.status), h("span", null, (trace.latency_ms || 0) + " ms"), h("button", { className: "text-button", onClick: function () { props.onOpenTrace(trace.trace_id); } }, "查看")); }))));
  }

  function TraceDrawer(props) {
    const item = props.item;
    const [tab, setTab] = useState("overview");
    return h(React.Fragment, null,
      h(Tabs, { active: tab, onChange: setTab, items: [["overview", "概览"], ["profile", "Model Profile"], ["evidence", "Evidence"], ["prompt", "Prompt"], ["output", "模型输出"], ["validation", "校验结果"], ["evaluator", "Evaluator 结果"]] }),
      tab === "overview" && h(React.Fragment, null, h(CodeBlock, { value: { trace_id: item.trace_id, job_id: item.job_id, candidate_id: item.candidate_id, entity_type: item.entity_type, entity_id: item.entity_id, provider: item.provider, model: item.model, prompt_id: item.prompt_id, prompt_hash: item.prompt_hash, input_evidence_hash: item.input_evidence_hash, latency_ms: item.latency_ms, created_at: item.created_at, status: item.status } }), props.rule && h("button", { className: "primary-button lineage-trace", onClick: function () { props.onOpenRule(props.rule.rule_id); } }, "查看关联批准规则")),
      tab === "profile" && h(CodeBlock, { value: { model_profile_id: item.model_profile_id || "无数据", parameter_size: item.parameter_size || "无数据", context_window_tokens: item.context_window_tokens || "无数据", recommended_input_tokens: item.recommended_input_tokens || "无数据", max_output_tokens: item.max_output_tokens || "无数据", thinking_enabled: item.thinking_enabled, model_options: item.model_options || {}, context_budget: item.context_budget || {}, evidence_build_meta: item.evidence_build_meta || {} } }),
      tab === "evidence" && h(CodeBlock, { value: item.input_evidence || "当前 trace 未保存完整 evidence，仅保存 hash。" }),
      tab === "prompt" && h(CodeBlock, { value: { prompt_id: item.prompt_id, prompt_hash: item.prompt_hash, prompt_path: item.prompt_path, prompt_content: item.prompt_content || "当前接口未返回 Prompt 内容。" } }),
      tab === "output" && h(CodeBlock, { value: { raw_output: item.raw_output, parsed_output: item.parsed_output } }),
      tab === "validation" && h(CodeBlock, { value: item.validation_result }),
      tab === "evaluator" && h(CodeBlock, { value: item.evaluator_result || { passed: false, errors: [], warnings: [], score: 0, rule_results: [] } }));
  }

  function thinkingBadge(enabled) {
    if (enabled === true) return h("span", { className: "status-chip success" }, "Thinking ON");
    if (enabled === false) return h("span", { className: "status-chip model_failed" }, "Thinking OFF");
    return h("span", { className: "status-chip" }, "未知");
  }

  function Drawer(props) {
    if (!props.item) return null;
    return h("div", { className: "drawer-mask", onClick: props.onClose }, h("aside", { className: "drawer", onClick: function (event) { event.stopPropagation(); } }, h("div", { className: "drawer-head" }, h("div", null, h("b", null, props.title), h("span", null, props.subtitle || "")), h("button", { onClick: props.onClose }, "×")), props.children));
  }

  function CodeBlock(props) { return h("pre", { className: "code-block" }, typeof props.value === "string" ? props.value : JSON.stringify(props.value || {}, null, 2)); }

  const obsStatusText = {
    reused_rule: "规则复用", model_running: "模型调用中", model_success: "模型成功", model_timeout: "模型超时",
    model_failed: "模型失败", parse_failed: "解析失败", schema_failed: "Schema 失败", evaluator_failed: "Evaluator 拦截",
    no_feature: "无关键特征", candidate_generated: "已生成候选", waiting_review: "等待审批", approved: "已批准", rejected: "已驳回",
    pending: "等待分析", skipped: "低风险跳过",
  };

  function AIObservabilityPage(props) {
    const summary = props.summary || {};
    const progress = props.progress || null;
    const events = props.events || [];
    const funnel = progress && progress.summary || {};
    const total = funnel.risk_entities_total || 0;
    const funnelItems = [
      ["输入风险实体", funnel.risk_entities_total || 0],
      ["规则复用命中", funnel.rule_reused || 0],
      ["进入 AI 分析", funnel.ai_required || 0],
      ["模型成功返回", funnel.model_success || 0],
      ["Schema 通过", funnel.schema_passed || 0],
      ["Evaluator 通过", funnel.evaluator_passed || 0],
      ["Evaluator 拦截", funnel.evaluator_failed || 0],
      ["候选特征", funnel.candidate_features || 0],
      ["已批准规则", funnel.approved_rules || 0],
    ];
    const stages = [
      ["任务创建", "Job 已创建"], ["实体筛选", total + " 个风险实体"], ["规则复用", (funnel.rule_reused || 0) + " 个命中"],
      ["Evidence 构造", (funnel.ai_required || 0) + " 个待分析"], ["Prompt 加载", progress && progress.prompt_id || "—"],
      ["模型调用", progress && progress.current_message || "—"], ["JSON 解析", (funnel.parse_success || 0) + " 个成功"],
      ["Schema 校验", (funnel.schema_passed || 0) + " 个通过"], ["Evaluator", (funnel.evaluator_passed || 0) + " 个通过"],
      ["候选特征", (funnel.candidate_features || 0) + " 个生成"], ["人工审批", "等待处理"], ["规则沉淀", (funnel.approved_rules || 0) + " 条"],
    ];
    if (!progress) return h("section", { className: "surface export-surface" }, h("h2", null, "暂无 AI 分析任务"), h("p", null, "完成一次日志特征识别后，系统会在这里展示 AI 分析进度、失败原因和规则生成路径。"), h("button", { className: "primary-button", onClick: props.onNewAnalysis }, "去新建分析"));
    return h(React.Fragment, null,
      h("div", { className: "page-head obs-head" }, h("div", null, h("h1", null, "AI 分析观测"), h("p", null, "实时查看 AI 分析进度、失败原因和规则生成路径")), h("button", { className: "primary-button", onClick: props.onRefresh }, "刷新状态")),
      h("section", { className: "metrics-grid" },
        h(Metric, { label: "当前运行任务", value: summary.running_jobs || 0, tone: "orange" }),
        h(Metric, { label: "进入 AI 分析实体", value: summary.ai_required || 0 }),
        h(Metric, { label: "模型成功率", value: Math.round((summary.model_success_rate || 0) * 1000) / 10 + "%", tone: "green" }),
        h(Metric, { label: "进入 Evaluator", value: summary.evaluator && summary.evaluator.total || 0 }),
        h(Metric, { label: "Evaluator 拦截", value: summary.evaluator && summary.evaluator.failed || 0, tone: "red" }),
        h(Metric, { label: "证据引用错误", value: summary.evaluator && summary.evaluator.evidence_reference_errors || 0 }),
        h(Metric, { label: "RCA / 建议越界", value: summary.evaluator && summary.evaluator.forbidden_claim_errors || 0 }),
        h(Metric, { label: "质量门禁通过率", value: Math.round(((summary.evaluator && summary.evaluator.pass_rate) || 0) * 1000) / 10 + "%", tone: "green" }),
        h(Metric, { label: "AI Cache 命中", value: summary.cache_hit_count || 0, tone: "green" }),
        h(Metric, { label: "候选特征生成", value: summary.candidate_feature_count || 0, tone: "orange" }),
        h(Metric, { label: "正常完成但无特征", value: summary.no_feature_count || 0 })),
      h("section", { className: "surface obs-progress" }, h("div", { className: "surface-head" }, h("b", null, "当前任务阶段"), h("span", null, progress.job_id + " · " + progress.status)),
        h("div", { className: "obs-tags" }, h("span", null, progress.source_file || "上传结果"), h("span", null, "Profile " + (progress.model_profile_id || "—")), h("span", null, "模型 " + (progress.model || "—")), h("span", null, "Thinking " + ((progress.model_profile && progress.model_profile.thinking_enabled) ? "ON" : "OFF")), h("span", null, "预算 " + ((progress.model_profile && progress.model_profile.max_templates) || "—") + " templates / " + ((progress.model_profile && progress.model_profile.max_evidence_chars) || "—") + " chars"), h("span", null, "Prompt " + (progress.prompt_id || "—")), h("span", null, "轮询刷新 2s")),
        h("div", { className: "stage-grid" }, stages.map(function (stage, index) {
          const done = index < 4 || (progress.status !== "running" && index < 10);
          const running = progress.status === "running" && index === 5;
          return h("div", { className: "stage-card " + (running ? "running" : done ? "completed" : "pending"), key: stage[0], title: stage[0] + " · " + stage[1] }, h("i"), h("b", null, stage[0]), h("span", null, stage[1]));
        }))),
      h("section", { className: "obs-grid" },
        h("div", { className: "surface" }, h("div", { className: "surface-head" }, h("b", null, "规则生成漏斗"), h("span", null, "解释为什么最后规则数量减少")),
          h("div", { className: "funnel-list" }, funnelItems.map(function (item) {
            const width = total ? Math.round(item[1] / total * 100) : 0;
            return h("div", { className: "funnel-row", key: item[0] }, h("span", null, item[0]), h("div", null, h("i", { style: { width: width + "%" } })), h("b", null, item[1]));
          }), h("p", null, "当前规则减少主要来自 " + ((summary.schema_failed_count || 0) + (summary.evaluator_failed_count || 0)) + " 个拦截、" + (summary.no_feature_count || 0) + " 个无关键特征，以及待人工审批的候选特征。"))),
        h("div", { className: "surface" }, h("div", { className: "surface-head" }, h("b", null, "最近 AI 事件流"), h("span", null, "实时刷新")),
          h("div", { className: "event-list" }, events.length === 0 && h("div", { className: "empty-state" }, "暂无 AI 事件"), events.map(function (event) {
            const title = event.entity_id ? ((event.entity_type || "entity") + "/" + event.entity_id) : event.stage;
            return h("article", { className: "event-row " + event.status, key: event.event_id + event.created_at }, h("time", null, event.created_at ? new Date(event.created_at).toLocaleTimeString() : "—"), h("div", null, h("b", null, title), h("span", null, event.message)), event.trace_id && h("button", { className: "text-button", onClick: function () { props.onOpenTrace(event.trace_id); } }, "查看 Trace"));
          })))),
      h("section", { className: "surface obs-table" }, h("div", { className: "surface-head" }, h("b", null, "实体级 AI 分析状态"), h("span", null, "区分没跑完、失败、拦截、无特征、等待审批")),
        h("div", { className: "entity-ai-head" }, "实体", "风险分", "模型 Profile", "Thinking", "Evidence 预算", "截断", "模型", "Schema", "Evaluator", "Trace", "操作"),
        (progress.entities || []).length === 0 && h("div", { className: "empty-state" }, "暂无实体分析状态"),
        (progress.entities || []).map(function (entity) {
          const canReview = entity.status === "candidate_generated" || entity.status === "waiting_review";
          const budget = entity.context_budget || {}, meta = entity.evidence_build_meta || {};
          return h("div", { className: "entity-ai-row", key: entity.entity_type + entity.entity_id }, h("span", null, entity.entity_type + "/" + entity.entity_id), h("b", null, entity.risk_score || "—"), h("span", null, entity.model_profile_id || "—"), h("span", null, entity.thinking_enabled === true ? "ON" : (entity.thinking_enabled === false ? "OFF" : "—")), h("span", null, (budget.max_templates || "—") + " / " + (budget.max_evidence_chars || "—")), h("span", null, meta.truncated ? "已裁剪 " + (meta.kept_template_count || "—") + "/" + (meta.original_template_count || "—") : "否"), h("span", null, entity.model_status || "—"), h("span", null, entity.schema_status || "—"), h("span", { className: "status-chip " + (entity.evaluator_status || "skipped") }, entity.evaluator_status === "passed" ? "通过" : (entity.evaluator_status === "failed" ? "拦截" : (entity.evaluator_status === "warning" ? "通过，有警告" : "未执行"))), entity.trace_id ? h("button", { className: "text-button", onClick: function () { props.onOpenTrace(entity.trace_id); } }, "查看 Trace") : h("span", null, entity.failure_reason || "—"), h("button", { className: "text-button", onClick: canReview ? props.onReview : (entity.reused_rule ? props.onRules : props.onRefresh) }, canReview ? "去审批" : (entity.reused_rule ? "查看规则" : "查看进度")));
        })));
  }

  function thinkingBadge(enabled) {
    if (enabled === true) return h("span", { className: "status-chip success" }, "Thinking ON");
    if (enabled === false) return h("span", { className: "status-chip model_failed" }, "Thinking OFF");
    return h("span", { className: "status-chip" }, "未知");
  }

  function ModelProfilesPage(props) {
    const profiles = props.profiles && props.profiles.profiles || [];
    const currentId = props.selectedProfileId || props.profiles && props.profiles.default_profile_id;
    const current = profiles.find(function (profile) { return profile.profile_id === currentId; }) || profiles[0] || {};
    const [draft, setDraft] = useState(current);
    useEffect(function () { setDraft(current); }, [current.profile_id]);
    const budget = draft.evidence_budget || {};
    function setField(key, value) { setDraft(Object.assign({}, draft, { [key]: value })); }
    function setBudget(key, value) { setDraft(Object.assign({}, draft, { evidence_budget: Object.assign({}, budget, { [key]: Number(value) || 0 }) })); }
    function createProfile() {
      const next = Object.assign({}, current, {
        profile_id: (current.profile_id || "custom") + "_copy",
        display_name: (current.display_name || current.profile_id || "Custom") + " 副本",
      });
      props.onSelect(next);
      setDraft(next);
    }
    const head = h("div", { className: "page-head obs-head" }, h("div", null, h("h1", null, "模型画像与上下文预算"), h("p", null, "按模型参数量、上下文窗口、Prompt 策略和 Thinking 开关，控制 Evidence 输入规模与调用行为")));
    const metrics = h("section", { className: "metrics-grid" },
        h(Metric, { label: "当前模型参数量", value: current.parameter_size || "—", tone: "orange" }),
        h(Metric, { label: "上下文窗口", value: current.context_window_tokens ? Math.round(current.context_window_tokens / 1000) + "K" : "—" }),
        h(Metric, { label: "最大模板数", value: budget.max_templates || "—" }),
        h(Metric, { label: "单模板字符上限", value: budget.max_template_chars || "—" }),
        h(Metric, { label: "Thinking 模式", value: current.thinking_enabled ? "ON" : "OFF", tone: current.thinking_enabled ? "green" : "red" }),
        h(Metric, { label: "默认 Prompt", value: current.default_prompt_id || "—" }));
    const list = h("div", { className: "surface" }, h("div", { className: "surface-head" }, h("b", null, "模型 Profile 列表"), h("span", null, profiles.length + " 个启用")),
      h("div", { className: "profile-list" }, profiles.map(function (profile) {
        return h("button", { className: "profile-card " + (profile.profile_id === current.profile_id ? "active" : ""), key: profile.profile_id, onClick: function () { props.onSelect(profile); } },
          h("b", null, profile.display_name || profile.profile_id),
          h("span", null, profile.model + " · " + profile.provider),
          h("small", null, "prompt: " + profile.default_prompt_id + " · context: " + (profile.context_window_tokens || "—") + " tokens"),
          thinkingBadge(profile.thinking_enabled));
      }), h("button", { className: "primary-button", onClick: createProfile }, "新增 Profile")));
    const detail = h("div", { className: "surface" }, h("div", { className: "surface-head" }, h("b", null, "当前 Profile 配置"), h("span", null, draft.profile_id || "—")),
      h("div", { className: "profile-detail" },
        h("label", null, "Profile ID", h("input", { value: draft.profile_id || "", onChange: function (event) { setField("profile_id", event.target.value); } })),
        h("label", null, "模型名称", h("input", { value: draft.model || "", onChange: function (event) { setField("model", event.target.value); } })),
        h("label", null, "显示名称", h("input", { value: draft.display_name || "", onChange: function (event) { setField("display_name", event.target.value); } })),
        h("label", null, "Provider", h("input", { value: draft.provider || "ollama", onChange: function (event) { setField("provider", event.target.value); } })),
        h("label", null, "参数量", h("input", { value: draft.parameter_size || "", onChange: function (event) { setField("parameter_size", event.target.value); } })),
        h("label", null, "默认 Prompt", h("input", { value: draft.default_prompt_id || "", onChange: function (event) { setField("default_prompt_id", event.target.value); } })),
        h("label", null, "Thinking", h("select", { value: draft.thinking_enabled ? "on" : "off", onChange: function (event) { setField("thinking_enabled", event.target.value === "on"); } }, h("option", { value: "off" }, "Thinking OFF"), h("option", { value: "on" }, "Thinking ON"))),
        h("p", null, "关闭 Thinking 模式用于 Ollama / 支持 thinking 参数的模型。关闭后降低推理耗时，减少中间思考内容影响 JSON 输出稳定性。"),
        h("h3", null, "Context Budget"),
        h("div", { className: "budget-grid" }, ["max_templates", "max_template_chars", "max_affected_entities", "max_evidence_chars", "recommended_input_tokens", "max_output_tokens"].map(function (key) {
          return h("label", { key: key }, h("b", null, key), h("input", { type: "number", value: budget[key] || draft[key] || 0, onChange: function (event) { key in budget ? setBudget(key, event.target.value) : setField(key, Number(event.target.value) || 0); } }));
        })),
        h("button", { className: "primary-button", onClick: function () { props.onSave(draft); } }, "保存 Profile"),
        h("h3", null, "调用配置预览"),
        h(CodeBlock, { value: { model_profile_id: draft.profile_id, provider: draft.provider, model: draft.model, parameter_size: draft.parameter_size, default_prompt_id: draft.default_prompt_id, thinking_enabled: draft.thinking_enabled, context_budget: budget, model_options: draft.options || {} } })));
    return h(React.Fragment, null, head, metrics, h("section", { className: "model-profile-grid" }, list, detail));
  }

  function RuleLibrary(props) {
    const [selected, setSelected] = useState(null);
    useEffect(function () {
      if (props.focusRuleId) setSelected(props.rules.find(function (rule) { return rule.rule_id === props.focusRuleId; }) || null);
    }, [props.focusRuleId, props.rules]);
    function lineageStatus(rule) {
      const lineage = rule.lineage;
      if (!lineage) return ["历史规则", "history"];
      if (lineage.trace_id && lineage.prompt_id && lineage.model && lineage.evidence_hash && rule.approved_at) return ["可追溯", "complete"];
      return ["部分可追溯", "partial"];
    }
    function identity(item) { return item.template_fingerprint || item.template_hash || "—"; }
    const detail = selected && h(React.Fragment, null,
      h("h3", null, "基础信息"), h(CodeBlock, { value: { rule_id: selected.rule_id, feature_type: selected.feature_type, title: selected.title, importance: selected.importance, approved_at: selected.approved_at } }),
      h("h3", null, "匹配条件"), h(CodeBlock, { value: selected.template_signatures || [] }),
      h("h3", null, "来源链路"), h("div", { className: "lineage-timeline" }, ["Input Job", "Candidate", "AI Trace", "Prompt / Model / Evidence", "Evaluator", "Manual Approval", "Approved Rule", "Reuse"].map(function (step) { return h("span", { key: step }, step); })),
      h(CodeBlock, { value: selected.lineage || { status: "历史规则，无 Lineage 数据" } }),
      selected.lineage && selected.lineage.trace_id && h("button", { className: "primary-button lineage-trace", onClick: function () { props.onOpenTrace(selected.lineage.trace_id); } }, "查看来源 AI Trace"),
      h("h3", null, "复用记录"), h(CodeBlock, { value: { reuse_count: selected.reuse_count || 0, last_reused_at: selected.last_reused_at || null } }));
    return h(React.Fragment, null,
      h("section", { className: "surface rules-surface" },
        h("div", { className: "surface-head" }, h("b", null, "批准规则库"), h("span", null, "全局跨集群复用 · " + props.rules.length + " 条规则")),
        h("div", { className: "rule-table" },
          h("div", { className: "rule-head" }, h("span", null, "规则"), h("span", null, "模板 / 类别"), h("span", null, "来源模型 / Prompt"), h("span", null, "Lineage 状态 / 复用")),
          props.rules.length === 0 && h("div", { className: "empty-state" }, "批准首条 Ollama 特征后建立规则库"),
          props.rules.map(function (rule) {
            const status = lineageStatus(rule), lineage = rule.lineage || {};
            return h("button", { className: "rule-row", key: rule.rule_id, onClick: function () { setSelected(rule); } },
              h("div", null, h("b", null, rule.title), h("span", null, rule.rule_id + " · " + rule.feature_type)),
              h("div", null, (rule.template_signatures || []).map(function (item) { const value = identity(item); return h("span", { className: "signature", key: value + "-" + item.category }, value.slice(0, 10) + " · " + (item.category || "未分类")); })),
              h("div", null, h("b", null, lineage.model || "历史数据"), h("span", null, lineage.prompt_id || "未记录 Prompt"), h("span", null, "质量门禁：来源候选已通过")),
              h("div", null, h("span", { className: "lineage-badge " + status[1] }, status[0]), h("b", null, (rule.reuse_count || 0) + " 次复用")));
          }))),
      selected && h(Drawer, { title: "规则详情", subtitle: selected.rule_id, item: selected, onClose: function () { setSelected(null); } }, detail));
  }

  function BackendSettings(props) {
    const [address, setAddress] = useState(currentApiBase());
    const [status, setStatus] = useState("");
    async function testAddress(save) {
      const candidate = address.trim().replace(/\/$/, "");
      if (!/^https?:\/\//.test(candidate)) { setStatus("后端地址必须以 http:// 或 https:// 开头"); return; }
      try {
        const response = await fetch(candidate + "/api/health", { headers: { "Content-Type": "application/json" } });
        const payload = await response.json().catch(function () { return {}; });
        if (!response.ok || payload.status !== "ok") throw new Error(payload.error || "健康检查失败");
        if (save) localStorage.setItem("logrisk.apiBase", candidate);
        setStatus(save ? "连接成功，后端地址已保存" : "连接成功：" + (payload.service || "LOGRISK"));
        props.onSaved && props.onSaved();
      } catch (reason) { setStatus("连接失败：" + reason.message); }
    }
    function reset() {
      localStorage.removeItem("logrisk.apiBase");
      setAddress(DEPLOYMENT_API_BASE || (window.location.origin === "null" ? "" : window.location.origin));
      setStatus("已恢复部署默认地址");
      props.onSaved && props.onSaved();
    }
    return h("div", { className: "settings-page" },
      h("div", { className: "page-head" }, h("div", null, h("h1", null, "系统设置"), h("p", null, "管理前后端分离部署时使用的 LOGRISK 后端连接"))),
      h("section", { className: "surface settings-panel" },
        h("div", { className: "surface-head" },
          h("div", null, h("b", null, "后端连接"), h("span", null, "浏览器级配置")),
          h("span", { className: "connection-chip" }, "● 本地配置")),
        h("div", { className: "settings-body" },
          h("div", { className: "settings-notice" }, h("b", null, "地址优先级"), h("span", null, "浏览器配置 → frontend/config.js → 当前页面同源地址")),
          h("label", { className: "settings-field" }, h("span", null, "后端 API 地址"), h("small", null, "请输入包含协议的完整地址，例如 http://127.0.0.1:8080"), h("input", { value: address, placeholder: "http://127.0.0.1:8080", onChange: function (event) { setAddress(event.target.value); } })),
          h("div", { className: "button-row" },
            h("button", { className: "secondary-button", onClick: function () { testAddress(false); } }, "测试连接"),
            h("button", { className: "primary-button", onClick: function () { testAddress(true); } }, "测试并保存"),
            h("button", { className: "text-button", onClick: reset }, "恢复默认")),
          status && h("div", { className: "settings-status" }, status),
          h("div", { className: "effective-address" }, h("span", null, "当前生效地址"), h("code", null, currentApiBase() || "同源")),
          h("p", { className: "settings-help" }, "跨域部署时，后端需将本页面来源加入 DASHBOARD_CORS_ORIGINS。"))));
  }

  function replaceIniValue(content, section, key, value) {
    let current = "";
    let replaced = false;
    const lines = String(content || "").split("\n").map(function (line) {
      const heading = line.match(/^\s*\[([^\]]+)\]\s*$/);
      if (heading) current = heading[1];
      if (current === section && new RegExp("^\\s*" + key + "\\s*=").test(line)) {
        replaced = true;
        return key + " = " + value;
      }
      return line;
    });
    if (!replaced) throw new Error(section + "." + key + " 不存在于当前 INI");
    return lines.join("\n");
  }

  function readIniValue(content, section, key) {
    let current = "";
    for (const line of String(content || "").split("\n")) {
      const heading = line.match(/^\s*\[([^\]]+)\]\s*$/);
      if (heading) current = heading[1];
      const value = current === section && line.match(new RegExp("^\\s*" + key + "\\s*=\\s*(.*)$"));
      if (value) return value[1].trim();
    }
    return "";
  }

  function replaceMaskingRules(content, rules) {
    const start = content.indexOf("masking =");
    const end = content.indexOf("\nmask_prefix", start);
    if (start < 0 || end < 0) throw new Error("当前 INI 缺少 MASKING.masking");
    const formatted = JSON.stringify(rules.map(function (rule) { return { regex_pattern: rule.regex_pattern, mask_with: rule.mask_with }; }), null, 3);
    return content.slice(0, start) + "masking = " + formatted + "\n" + content.slice(end + 1);
  }

  function DrainConfigGovernance(props) {
    const catalog = props.configs || { items: [], active: null };
    const items = catalog.items || [];
    const [selectedId, setSelectedId] = useState("");
    const [detailTab, setDetailTab] = useState("structured");
    const [draft, setDraft] = useState("");
    const [draftRules, setDraftRules] = useState([]);
    const [validation, setValidation] = useState(null);
    const [evalRunId, setEvalRunId] = useState("");
    const selected = items.find(function (item) { return item.config_id === selectedId; }) || items.find(function (item) { return catalog.active && item.config_id === catalog.active.config_id; }) || items[0] || null;
    const baseline = items.find(function (item) { return item.config_id === "baseline"; }) || null;
    const candidates = items.filter(function (item) { return item.config_id !== "baseline"; });
    const matchingRuns = (props.evalRuns || []).filter(function (run) { return selected && run.status === "completed" && run.config_id === selected.config_id && Number(run.config_version) === Number(selected.version) && run.config_hash === selected.content_hash; });
    useEffect(function () {
      if (!selected) return;
      setSelectedId(selected.config_id);
      setDraft(selected.ini_content || "");
      setDraftRules((selected.masking_rules || []).map(function (rule) { return Object.assign({}, rule); }));
      setValidation(null);
      setEvalRunId("");
    }, [selected && selected.config_id, selected && selected.version, selected && selected.content_hash]);
    function createCandidate() {
      const name = window.prompt("候选配置名称", "Drain3 candidate");
      if (!name) return;
      props.onCreate({ source_config_id: selected ? selected.config_id : "baseline", source_version: selected ? selected.version : 1, name: name, operator: "local-operator" });
    }
    function updateParameter(section, key, value) {
      try { setDraft(replaceIniValue(draft, section, key, value)); } catch (reason) { window.alert(reason.message); }
    }
    function updateRules(next) {
      try { setDraftRules(next); setDraft(replaceMaskingRules(draft, next)); } catch (reason) { window.alert(reason.message); }
    }
    function save() {
      if (!selected || selected.config_id === "baseline") return;
      props.onSave(selected.config_id, { expected_version: selected.version, ini_content: draft, operator: "local-operator" });
    }
    function validate() {
      if (!selected) return;
      props.onValidate(selected.config_id, { version: selected.version }).then(setValidation).catch(function () {});
    }
    function publish() {
      if (!selected || !evalRunId || !window.confirm("确认将此配置发布给后续新分析任务？")) return;
      props.onPublish(selected.config_id, { version: selected.version, eval_run_id: evalRunId, confirmed: true, operator: "local-operator" });
    }
    function rollback() {
      if (!selected || selected.config_id === "baseline") return;
      const version = Number(window.prompt("回滚到哪个版本？", "1"));
      if (!version || !window.confirm("确认回滚活动 Drain3 配置？")) return;
      props.onRollback(selected.config_id, { version: version, confirmed: true, operator: "local-operator" });
    }
    const parameterFields = [
      ["DRAIN", "sim_th", "相似度阈值", "模板归并的最小相似度"],
      ["DRAIN", "depth", "解析树深度", "规范化日志推荐 5"],
      ["DRAIN", "max_children", "最大子节点", "控制树分支数量"],
      ["DRAIN", "max_clusters", "最大模板簇", "每个分片的模板上限"],
      ["DRAIN", "parametrize_numeric_tokens", "数字参数化", "true / false"],
      ["DRAIN", "extra_delimiters", "额外分隔符", "JSON 字符串数组"],
      ["SNAPSHOT", "snapshot_interval_minutes", "快照间隔（分钟）", "状态保存频率"],
      ["PROFILING", "enabled", "性能分析", "true / false"],
    ];
    const diffRows = !selected || !baseline ? [] : String(draft).split("\n").map(function (line, index) { return { index: index + 1, before: String(baseline.ini_content || "").split("\n")[index] || "", after: line }; }).filter(function (row) { return row.before !== row.after; });
    return h("section", { className: "drain-config-governance" },
      h("div", { className: "config-governance-summary" },
        h("div", null, h("span", null, "当前生效配置"), h("b", null, catalog.active ? catalog.active.name + " v" + catalog.active.version : "—"), h("small", { className: "positive" }, "● " + (catalog.active && catalog.active.status === "baseline" ? "系统基线" : "已发布"))),
        h("div", null, h("span", null, "脱敏规则"), h("b", null, catalog.active ? (catalog.active.masking_rules || []).length : 0), h("small", null, "算法运行前脱敏")),
        h("div", null, h("span", null, "候选配置"), h("b", null, candidates.length), h("small", null, "完整 INI 版本")),
        h("div", null, h("span", null, "关联评测"), h("b", null, (props.evalRuns || []).filter(function (run) { return run.config_id; }).length), h("small", null, "通过门槛后人工发布"))),
      h("div", { className: "config-governance-layout" },
        h("aside", { className: "surface config-version-list" },
          h("div", { className: "surface-head" }, h("div", null, h("b", null, "配置版本"), h("span", null, items.length + " 个配置")), h("button", { className: "mini-add", onClick: createCandidate }, "+")),
          items.map(function (item) { return h("button", { className: "config-version-item " + (selected && selected.config_id === item.config_id ? "active" : ""), key: item.config_id, onClick: function () { setSelectedId(item.config_id); } }, h("div", null, h("b", null, item.name + " v" + item.version), h("span", { className: "status-chip " + item.status }, item.status)), h("small", null, item.config_id), h("em", null, (item.masking_rules || []).length + " 条脱敏规则 · " + item.content_hash.slice(0, 10))); })),
        h("div", { className: "surface config-governance-detail" }, selected ? h(React.Fragment, null,
          h("div", { className: "surface-head" }, h("div", null, h("b", null, selected.name + " v" + selected.version), h("span", null, selected.config_id + " · " + selected.content_hash.slice(0, 16))), h("span", { className: "status-chip " + selected.status }, selected.status)),
          h("div", { className: "config-detail-tabs" }, [["structured", "结构化配置"], ["masking", "脱敏规则（" + draftRules.length + "）"], ["ini", "INI 原文"], ["diff", "版本差异"]].map(function (tab) { return h("button", { key: tab[0], className: detailTab === tab[0] ? "active" : "", onClick: function () { setDetailTab(tab[0]); } }, tab[1]); })),
          detailTab === "structured" && h("div", { className: "config-structured-editor" }, parameterFields.map(function (field) { const value = readIniValue(draft, field[0], field[1]); return h("label", { key: field[0] + field[1] }, h("span", null, field[2]), h("input", { value: value, disabled: selected.config_id === "baseline", onChange: function (event) { updateParameter(field[0], field[1], event.target.value); } }), h("small", null, field[0] + "." + field[1] + " · " + field[3])); })),
          detailTab === "masking" && h("div", { className: "masking-rule-table" }, h("div", { className: "masking-rule-row head" }, h("span", null, "占位符"), h("span", null, "正则表达式"), h("span", null, "操作")), draftRules.map(function (rule, index) { return h("div", { className: "masking-rule-row", key: index }, h("input", { value: rule.mask_with, disabled: selected.config_id === "baseline", onChange: function (event) { const next = draftRules.slice(); next[index] = Object.assign({}, rule, { mask_with: event.target.value }); updateRules(next); } }), h("input", { value: rule.regex_pattern, disabled: selected.config_id === "baseline", onChange: function (event) { const next = draftRules.slice(); next[index] = Object.assign({}, rule, { regex_pattern: event.target.value }); updateRules(next); } }), h("div", { className: "row-actions" }, h("button", { className: "text-button", disabled: selected.config_id === "baseline" || index === 0, onClick: function () { const next = draftRules.slice(); const moved = next.splice(index, 1)[0]; next.splice(index - 1, 0, moved); updateRules(next); } }, "上移"), h("button", { className: "text-button danger-text", disabled: selected.config_id === "baseline", onClick: function () { updateRules(draftRules.filter(function (_, ruleIndex) { return ruleIndex !== index; })); } }, "移除"))); }), selected.config_id !== "baseline" && h("button", { className: "secondary-button add-mask-rule", onClick: function () { updateRules(draftRules.concat([{ mask_with: "CUSTOM", regex_pattern: "" }])); } }, "＋ 新增脱敏规则")),
          detailTab === "ini" && h("textarea", { className: "config-ini-editor", value: draft, readOnly: selected.config_id === "baseline", spellCheck: false, onChange: function (event) { setDraft(event.target.value); } }),
          detailTab === "diff" && h("div", { className: "config-version-diff" }, diffRows.length === 0 ? h("div", { className: "empty-state" }, "与系统基线无差异") : diffRows.slice(0, 100).map(function (row) { return h("div", { className: "config-diff-row", key: row.index }, h("span", null, "L" + row.index), h("code", { className: "before" }, row.before || "∅"), h("code", { className: "after" }, row.after || "∅")); })),
          validation && h("div", { className: "config-validation " + (validation.valid ? "valid" : "invalid") }, validation.valid ? "配置校验通过：参数与 " + validation.masking_rules.length + " 条脱敏正则有效" : "配置校验失败"),
          h("div", { className: "config-governance-actions" }, selected.config_id === "baseline" ? h("button", { className: "primary-button", onClick: createCandidate }, "复制为候选") : h(React.Fragment, null, h("button", { className: "secondary-button", onClick: save }, "保存新版本"), h("button", { className: "secondary-button", onClick: validate }, "配置校验"), h("select", { value: evalRunId, onChange: function (event) { setEvalRunId(event.target.value); } }, h("option", { value: "" }, matchingRuns.length ? "选择关联评测" : "暂无匹配评测"), matchingRuns.map(function (run) { return h("option", { key: run.run_id, value: run.run_id }, run.run_id + " · F1 " + ((run.metrics && run.metrics.labeled && run.metrics.labeled.pairwise_grouping_f1) || "—")); })), h("button", { className: "primary-button", disabled: !evalRunId, onClick: publish }, "人工发布"), h("button", { className: "text-button", onClick: rollback }, "回滚")))) : h("div", { className: "empty-state" }, "暂无 Drain3 配置"))));
  }

  function DrainQualityPage(props) {
    const data = props.data || { datasets: [], annotations: [], evalRuns: [], profiles: [], templates: [] };
    const [tab, setTab] = useState("overview");
    const [selectedHash, setSelectedHash] = useState("");
    const [suspiciousFilter, setSuspiciousFilter] = useState("all");
    const [profileA, setProfileA] = useState("");
    const [profileB, setProfileB] = useState("");
    const [templateQuery, setTemplateQuery] = useState("");
    const [componentFilter, setComponentFilter] = useState("all");
    const [statusFilter, setStatusFilter] = useState("all");
    const latest = data.evalRuns[0] || {};
    const labeled = latest.metrics && latest.metrics.labeled || {};
    const unlabeled = latest.metrics && latest.metrics.unlabeled || {};
    const templates = data.templates || [];
    const suspicious = templates.filter(function (item) { return item.status !== "active" || (String(item.effective_template || "").match(/<[^>]+>/g) || []).length >= 2; });
    const selectedTemplate = templates.find(function (item) { return item.template_hash === selectedHash; }) || templates[0] || null;
    const profiles = data.profiles || [];
    const selectedProfileA = profiles.find(function (item) { return item.profile_id === profileA; }) || profiles[0] || null;
    const selectedProfileB = profiles.find(function (item) { return item.profile_id === profileB; }) || profiles[1] || profiles[0] || null;
    const components = Array.from(new Set(templates.map(function (item) { return item.component || "unknown"; }))).sort();
    const visibleSuspicious = suspicious.filter(function (item) {
      const wildcard = (String(item.effective_template || "").match(/<[^>]+>/g) || []).length >= 2;
      return suspiciousFilter === "all" || (suspiciousFilter === "wildcard" ? wildcard : item.status === suspiciousFilter);
    });
    const visibleTemplates = templates.filter(function (item) {
      const haystack = [item.component, item.template_hash, item.effective_template].join(" ").toLowerCase();
      return (!templateQuery || haystack.includes(templateQuery.toLowerCase())) && (componentFilter === "all" || item.component === componentFilter) && (statusFilter === "all" || item.status === statusFilter);
    });
    function percent(value) { return value == null ? "—" : Math.round(value * 1000) / 10 + "%"; }
    function profileValue(profile, key) {
      if (!profile || !profile.parameters) return "—";
      const value = profile.parameters[key];
      return typeof value === "boolean" ? (value ? "开启" : "关闭") : value == null || value === "" ? "—" : String(value);
    }
    function confirmedChange(item, action) {
      if (!window.confirm("确认对模板 " + item.template_hash + " 执行“" + action + "”？此操作会写入审计历史。")) return;
      const payload = { action: action, expected_version: item.version, confirmed: true, operator: "local-operator" };
      if (action === "edit") {
        const template = window.prompt("输入新的有效模板", item.effective_template);
        if (!template) return;
        payload.template = template;
      }
      if (action === "merge") {
        const target = window.prompt("输入目标 template_hash", "");
        if (!target) return;
        payload.target_template_hash = target;
      }
      props.onTemplateChange(item.template_hash, payload);
    }
    function rollback(item) {
      const version = Number(window.prompt("回滚到哪个版本？", "1"));
      if (!version || !window.confirm("确认回滚模板？回滚也会新增一条审计事件。")) return;
      props.onTemplateRollback(item.template_hash, { target_version: version, expected_version: item.version, confirmed: true, operator: "local-operator" });
    }
    const tabs = [["overview", "质量概览"], ["annotation", "标注工作台"], ["suspicious", "可疑模板"], ["compare", "配置对比"], ["templates", "模板管理"], ["configs", "Drain3 配置"], ["release", "发布管理"]];
    const annotationActions = selectedTemplate && h("div", { className: "button-row annotation-actions" },
      h("button", { className: "primary-button", onClick: function () { props.onAnnotate({ cluster_id: selectedTemplate.template_hash, action: "accept", reviewer: "local-operator" }); } }, "接受当前簇"),
      h("button", { className: "secondary-button", onClick: function () { confirmedChange(selectedTemplate, "edit"); } }, "编辑模板"),
      h("button", { className: "secondary-button", onClick: function () { confirmedChange(selectedTemplate, "merge"); } }, "合并簇"),
      h("button", { className: "text-button danger-text", onClick: function () { props.onAnnotate({ cluster_id: selectedTemplate.template_hash, action: "ignore", reviewer: "local-operator" }); } }, "忽略"));
    return h("div", { className: "drain-quality-page" },
      h("div", { className: "page-head" }, h("div", null, h("h1", null, "评测中心 · 模板质量"), h("p", null, "评估 Drain3 分组、语义保留、稳定性及下游一致性")), h("div", { className: "button-row" }, h("button", { className: "secondary-button", onClick: props.onImport }, "导入当前结果模板"), h("button", { className: "primary-button", onClick: props.onRefresh }, "刷新"))),
      h("nav", { className: "quality-tabs" }, tabs.map(function (item) { return h("button", { key: item[0], className: tab === item[0] ? "active" : "", onClick: function () { setTab(item[0]); } }, item[1]); })),
      tab === "overview" && h(React.Fragment, null,
        h("section", { className: "metrics-grid" },
          h(Metric, { label: "Grouping F1", value: percent(labeled.pairwise_grouping_f1), tone: "orange" }),
          h(Metric, { label: "Over-merge", value: percent(labeled.over_merge_rate) }),
          h(Metric, { label: "Over-split", value: percent(labeled.over_split_rate) }),
          h(Metric, { label: "Singleton", value: percent(unlabeled.singleton_ratio) }),
          h(Metric, { label: "Wildcard", value: percent(unlabeled.wildcard_ratio) }),
          h(Metric, { label: "Churn", value: latest.metrics && latest.metrics.stability ? percent(latest.metrics.stability.template_churn || 0) : "—" })),
        h("section", { className: "surface quality-baseline" }, h("div", { className: "surface-head" }, h("div", null, h("b", null, "评测基线"), h("span", null, "当前质量资产与评测覆盖")), h("span", { className: "status-chip active" }, latest.run_id ? "已建立" : "待建立")), h("div", { className: "baseline-stats" }, h("div", null, h("b", null, data.datasets.length), h("span", null, "Gold Dataset")), h("div", null, h("b", null, data.evalRuns.length), h("span", null, "评测任务")), h("div", null, h("b", null, templates.length), h("span", null, "受管模板"))), !latest.run_id && h("div", { className: "empty-state" }, "创建 Gold Dataset 和评测任务后显示质量指标"))),
      tab === "annotation" && h("section", { className: "quality-annotation-layout" },
        h("div", { className: "surface annotation-queue" }, h("div", { className: "surface-head" }, h("div", null, h("b", null, "待标注模板"), h("span", null, "逐条选择并审核模板簇")), h("span", null, templates.length + " 条")), templates.length === 0 && h("div", { className: "empty-state" }, "导入模板后开始人工标注"), templates.slice(0, 100).map(function (item) { return h("button", { className: "annotation-queue-item " + (selectedTemplate && selectedTemplate.template_hash === item.template_hash ? "active" : ""), key: item.template_hash, onClick: function () { setSelectedHash(item.template_hash); } }, h("span", null, h("b", null, item.component || "unknown"), h("small", null, item.template_hash)), h("em", null, item.count + " 条"), h("code", null, item.effective_template)); })),
        h("div", { className: "surface annotation-detail" }, selectedTemplate ? h(React.Fragment, null, h("div", { className: "surface-head" }, h("div", null, h("b", null, "模板审核"), h("span", null, "确认分组语义与模板质量")), h("span", { className: "status-chip " + selectedTemplate.status }, selectedTemplate.status)), h("div", { className: "annotation-summary" }, h("div", null, h("span", null, "组件"), h("b", null, selectedTemplate.component || "unknown")), h("div", null, h("span", null, "日志量"), h("b", null, selectedTemplate.count || 0)), h("div", null, h("span", null, "版本"), h("b", null, "v" + selectedTemplate.version))), h("label", { className: "detail-label" }, "有效模板"), h("code", { className: "template-preview" }, selectedTemplate.effective_template), h("label", { className: "detail-label" }, "模板标识"), h("code", { className: "hash-value" }, selectedTemplate.template_hash), annotationActions) : h("div", { className: "empty-state" }, "请选择左侧模板"))),
      tab === "suspicious" && h("section", { className: "surface suspicious-panel" }, h("div", { className: "surface-head" }, h("div", null, h("b", null, "可疑模板"), h("span", null, "聚焦高通配符及非活跃模板")), h("span", null, visibleSuspicious.length + " 条")), h("div", { className: "suspicious-filters" }, [["all", "全部"], ["wildcard", "高通配符"], ["ignored", "已忽略"], ["merged", "已合并"], ["deleted", "软删除"]].map(function (filter) { return h("button", { key: filter[0], className: suspiciousFilter === filter[0] ? "active" : "", onClick: function () { setSuspiciousFilter(filter[0]); } }, filter[1]); })), visibleSuspicious.length === 0 && h("div", { className: "empty-state" }, "当前筛选条件下暂无可疑模板"), h("div", { className: "suspicious-list" }, visibleSuspicious.map(function (item) { const wildcardCount = (String(item.effective_template || "").match(/<[^>]+>/g) || []).length; return h("article", { className: "suspicious-item", key: item.template_hash }, h("div", null, h("span", { className: "issue-icon" }, "!"), h("div", null, h("b", null, item.component || "unknown"), h("small", null, item.template_hash + " · v" + item.version))), h("code", null, item.effective_template), h("div", { className: "issue-meta" }, h("span", null, wildcardCount + " 个通配符"), h("span", { className: "status-chip " + item.status }, item.status))); }))),
      tab === "compare" && h("section", { className: "surface profile-compare-panel" }, h("div", { className: "surface-head" }, h("div", null, h("b", null, "Profile 配置对比"), h("span", null, "并排检查候选 Drain3 参数差异"))), h("div", { className: "profile-compare-selectors" }, h("label", null, h("span", null, "Profile A"), h("select", { value: selectedProfileA && selectedProfileA.profile_id || "", onChange: function (event) { setProfileA(event.target.value); } }, profiles.map(function (profile) { return h("option", { key: profile.profile_id, value: profile.profile_id }, profile.name); }))), h("span", null, "对比"), h("label", null, h("span", null, "Profile B"), h("select", { value: selectedProfileB && selectedProfileB.profile_id || "", onChange: function (event) { setProfileB(event.target.value); } }, profiles.map(function (profile) { return h("option", { key: profile.profile_id, value: profile.profile_id }, profile.name); })))), profiles.length === 0 ? h("div", { className: "empty-state" }, "暂无可对比的 Profile") : h("div", { className: "profile-parameter-table" }, h("div", { className: "parameter-row head" }, h("span", null, "参数"), h("span", null, selectedProfileA.name), h("span", null, selectedProfileB.name)), [["sim_th", "相似度阈值"], ["depth", "树深度"], ["max_children", "最大子节点"], ["parametrize_numeric_tokens", "数字参数化"], ["extra_delimiters", "额外分隔符"]].map(function (entry) { const a = profileValue(selectedProfileA, entry[0]), b = profileValue(selectedProfileB, entry[0]); return h("div", { className: "parameter-row " + (a !== b ? "different" : ""), key: entry[0] }, h("span", null, h("b", null, entry[1]), h("small", null, entry[0])), h("code", null, a), h("code", null, b)); }))),
      tab === "templates" && h("section", { className: "surface template-management" }, h("div", { className: "surface-head" }, h("div", null, h("b", null, "Drain3 模板管理"), h("span", null, "搜索、筛选并执行受审计的模板操作")), h("span", null, visibleTemplates.length + " / " + templates.length)), h("div", { className: "template-toolbar" }, h("input", { value: templateQuery, placeholder: "搜索组件、Hash 或模板内容", onChange: function (event) { setTemplateQuery(event.target.value); } }), h("select", { value: componentFilter, onChange: function (event) { setComponentFilter(event.target.value); } }, h("option", { value: "all" }, "全部组件"), components.map(function (component) { return h("option", { value: component, key: component }, component); })), h("select", { value: statusFilter, onChange: function (event) { setStatusFilter(event.target.value); } }, h("option", { value: "all" }, "全部状态"), ["active", "ignored", "merged", "deleted"].map(function (status) { return h("option", { value: status, key: status }, status); }))), templates.length === 0 && h("div", { className: "empty-state" }, "导入分析结果中的脱敏模板后进行管理"), h("div", { className: "managed-template-table" }, h("div", { className: "managed-template-row head" }, h("span", null, "模板"), h("span", null, "有效内容"), h("span", null, "状态"), h("span", null, "操作")), visibleTemplates.map(function (item) { return h("div", { className: "managed-template-row", key: item.template_hash }, h("div", null, h("b", null, item.component || "unknown"), h("small", null, item.template_hash + " · v" + item.version + " · " + item.count + " 条")), h("code", null, item.effective_template), h("span", { className: "status-chip " + item.status }, item.status), h("div", { className: "row-actions" }, h("button", { className: "text-button", onClick: function () { confirmedChange(item, "edit"); } }, "编辑"), h("button", { className: "text-button", onClick: function () { confirmedChange(item, "merge"); } }, "合并"), h("button", { className: "text-button", onClick: function () { confirmedChange(item, item.status === "ignored" || item.status === "deleted" ? "restore" : "ignore"); } }, item.status === "ignored" || item.status === "deleted" ? "恢复" : "忽略"), h("button", { className: "text-button danger-text", onClick: function () { confirmedChange(item, "delete"); } }, "软删除"), h("button", { className: "text-button", onClick: function () { rollback(item); } }, "回滚"))); }))),
      tab === "configs" && h(DrainConfigGovernance, { configs: data.configs, evalRuns: data.evalRuns, onCreate: props.onConfigCreate, onSave: props.onConfigSave, onValidate: props.onConfigValidate, onPublish: props.onConfigPublish, onRollback: props.onConfigRollback }),
      tab === "release" && h("section", { className: "release-stage-grid" }, profiles.length === 0 && h("div", { className: "empty-state" }, "暂无待治理 Profile"), profiles.map(function (profile) { const promoted = profile.status === "promoted"; return h("article", { className: "surface release-card", key: profile.profile_id }, h("div", { className: "release-card-head" }, h("div", null, h("span", { className: "release-icon" }, promoted ? "✓" : "↑"), h("div", null, h("h3", null, profile.name), h("small", null, profile.profile_id))), h("span", { className: "status-chip " + profile.status }, profile.status)), h("div", { className: "release-flow" }, ["候选", "人工确认", "已发布"].map(function (stage, index) { return h("span", { className: index <= (promoted ? 2 : 0) ? "done" : "", key: stage }, stage); })), h("p", null, "发布仅记录旧版 Profile 决策；实际运行配置请在 Drain3 配置页评测并发布。"), h("div", { className: "button-row" }, h("button", { className: "primary-button", disabled: promoted, onClick: function () { if (window.confirm("确认发布 Profile " + profile.profile_id + "？")) props.onProfile(profile.profile_id, "promote"); } }, promoted ? "已发布" : "确认发布"), h("button", { className: "secondary-button", onClick: function () { if (window.confirm("确认回滚 Profile " + profile.profile_id + "？")) props.onProfile(profile.profile_id, "rollback"); } }, "回滚"))); })));
  }

  function App() {
    const [view, setView] = useState(pathToView(window.location.pathname)), [model, setModel] = useState("qwen3:1.7b"), [threshold, setThreshold] = useState(40), [promptId, setPromptId] = useState("feature_extract_v3_compact_strict_json_en"), [retryCount, setRetryCount] = useState(1);
    const [ollama, setOllama] = useState({ online: false }), [result, setResult] = useState(null), [fileName, setFileName] = useState("");
    const [snapshot, setSnapshot] = useState(null), [jobId, setJobId] = useState(null), [rules, setRules] = useState([]);
    const [systemMetrics, setSystemMetrics] = useState({ today_llm_logs: 0 });
    const [harness, setHarness] = useState({ trace_enabled: true, current_prompt_id: "feature_extract_v3_compact_strict_json_en" }), [prompts, setPrompts] = useState([]), [traces, setTraces] = useState([]);
    const [modelProfiles, setModelProfiles] = useState({ default_profile_id: "", profiles: [] }), [modelProfileId, setModelProfileId] = useState("");
    const [obsSummary, setObsSummary] = useState({}), [obsProgress, setObsProgress] = useState(null), [obsEvents, setObsEvents] = useState([]);
    const [traceFilters, setTraceFilters] = useState(traceFiltersFromSearch(window.location.search));
    const [drawer, setDrawer] = useState({ type: null, item: null });
    const [selectedId, setSelectedId] = useState(null), [busy, setBusy] = useState(false), [error, setError] = useState("");
    const [selectedTemplate, setSelectedTemplate] = useState(null);
    const [ruleFocus, setRuleFocus] = useState("");
    const [uploadProgress, setUploadProgress] = useState(null), [preprocessProgress, setPreprocessProgress] = useState(null);
    const [drainQuality, setDrainQuality] = useState({ datasets: [], annotations: [], evalRuns: [], profiles: [], templates: [], configs: { items: [], active: null } });
    const events = useRef(null);
    const selected = useMemo(function () { return snapshot && snapshot.features && snapshot.features.find(function (feature) { return feature.candidate_id === selectedId; }) || null; }, [snapshot, selectedId]);
    useEffect(function () {
      const features = snapshot && snapshot.features || [];
      setSelectedId(function (current) {
        if (current && features.some(function (feature) { return feature.candidate_id === current; })) return current;
        return features.length ? features[0].candidate_id : null;
      });
    }, [snapshot]);
    function changeView(next) {
      setView(next);
      history.pushState({}, "", routeForView(next));
      if (next === "drainQuality") loadDrainQuality().catch(function (reason) { setError(reason.message); });
    }
    function applyTraceFilters(next) {
      const query = traceFilterQuery(next);
      setTraceFilters(next);
      setView("traces");
      history.pushState({}, "", "/ai-traces" + (query === "?limit=50" ? "" : query));
      loadHarness(query).catch(function (reason) { setError(reason.message); });
    }
    async function loadHarness(query) { const values = await Promise.all([api.harnessStatus(), api.prompts(), api.traces(query || "?limit=50"), api.modelProfiles()]); setHarness(values[0]); setPrompts(values[1].items || []); setPromptId(values[1].current_prompt_id || "feature_extract_v3_compact_strict_json_en"); setTraces(values[2].items || []); setModelProfiles(values[3]); setModelProfileId(function (current) { return current || values[3].default_profile_id || ""; }); }
    async function loadDrainQuality() {
      const values = await Promise.all([api.drainDatasets(), api.drainAnnotations(), api.drainEvalRuns(), api.drainProfiles(), api.drainTemplates(), api.drainConfigs()]);
      setDrainQuality({ datasets: values[0].items || [], annotations: values[1].items || [], annotationState: values[1].state || {}, evalRuns: values[2].items || [], profiles: values[3].items || [], templates: values[4].items || [], configs: values[5] });
    }
    async function loadObservability(id) {
      const summary = await api.observabilitySummary();
      const target = id || new URLSearchParams(window.location.search).get("job_id") || summary.current_job_id;
      setObsSummary(summary);
      if (!target) { setObsProgress(null); setObsEvents((await api.observabilityRecentEvents()).items || []); return; }
      const values = await Promise.all([api.observabilityProgress(target), api.observabilityEvents(target)]);
      setObsProgress(values[0]);
      setObsEvents(values[1].items || []);
    }
    async function refresh(id) { const next = await api.job(id || jobId); setSnapshot(next); if (["completed", "completed_with_errors"].includes(next.status) && events.current) events.current.close(); loadHarness().catch(function () {}); }
    useEffect(function () {
      const query = window.location.pathname === "/ai-traces" ? traceFilterQuery(traceFiltersFromSearch(window.location.search)) : "?limit=50";
      Promise.all([api.config(), api.status(), api.rules(), api.metrics(), api.harnessStatus(), api.prompts(), api.traces(query), api.modelProfiles()]).then(function (values) { setModel(values[0].default_model); setOllama(values[1]); setRules(values[2].rules || []); setSystemMetrics(values[3]); setHarness(values[4]); setPrompts(values[5].items || []); setPromptId(values[5].current_prompt_id || values[4].current_prompt_id || "feature_extract_v3_compact_strict_json_en"); setTraces(values[6].items || []); setModelProfiles(values[7]); setModelProfileId(values[7].default_profile_id || ""); if (window.location.pathname === "/ai-observability") loadObservability().catch(function (reason) { setError(reason.message); }); if (window.location.pathname === "/drain-quality") loadDrainQuality().catch(function (reason) { setError(reason.message); }); }).catch(function (reason) { setError(reason.message); });
      function onPop() { const filters = traceFiltersFromSearch(window.location.search); setView(pathToView(window.location.pathname)); setTraceFilters(filters); if (window.location.pathname === "/ai-traces") loadHarness(traceFilterQuery(filters)).catch(function () {}); if (window.location.pathname === "/ai-observability") loadObservability().catch(function () {}); if (window.location.pathname === "/drain-quality") loadDrainQuality().catch(function () {}); }
      window.addEventListener("popstate", onPop);
      return function () { window.removeEventListener("popstate", onPop); if (events.current) events.current.close(); };
    }, []);
    useEffect(function () {
      if (view !== "observability") return;
      loadObservability().catch(function (reason) { setError(reason.message); });
      if (document.hidden || !obsProgress || !["queued", "running", "partial_failed", "waiting_review"].includes(obsProgress.status)) return;
      const timer = setInterval(function () { if (!document.hidden) loadObservability(obsProgress.job_id).catch(function (reason) { setError(reason.message); }); }, 2000);
      return function () { clearInterval(timer); };
    }, [view, obsProgress && obsProgress.job_id, obsProgress && obsProgress.status]);
    async function loadFile(file) { if (!file) return; setBusy(true); setError(""); setUploadProgress(null); setPreprocessProgress(null); try { const next = file.size > INLINE_MAX_BYTES ? await api.uploadAndAnalyzeLargeFile(file, { onUploadProgress: setUploadProgress, onPreprocessProgress: setPreprocessProgress }) : await api.analyzeFile(file); setResult(next); setFileName(file.name); setSnapshot(null); setJobId(null); changeView("overview"); } catch (reason) { setError(reason.message); } finally { setBusy(false); } }
    async function start() { if (!result) return; setBusy(true); setError(""); try { const created = await api.createJob(result, model, threshold, promptId, modelProfileId, retryCount); setJobId(created.job_id); changeView("queue"); await refresh(created.job_id); if (events.current) events.current.close(); events.current = api.subscribe(created.job_id, function () { refresh(created.job_id).catch(function (reason) { setError(reason.message); }); }); } catch (reason) { setError(reason.message); } finally { setBusy(false); } }
    async function save(changes) { try { await api.update(jobId, selectedId, changes); await refresh(); setRules((await api.rules()).rules || []); } catch (reason) { setError(reason.message); } }
    function retry(entityId) { api.retry(jobId, entityId).then(function () { return refresh(); }).catch(function (reason) { setError(reason.message); }); }
    function openTrace(traceId) { api.trace(traceId).then(function (item) { setDrawer({ type: "trace", item: item }); applyTraceFilters({ job_id: "", trace_id: traceId, status: "", prompt_id: "" }); }).catch(function (reason) { setError(reason.message); }); }
    function openPrompt(id) { api.prompt(id).then(function (item) { setDrawer({ type: "prompt", item: item }); setView("prompts"); history.pushState({}, "", "/prompts?prompt_id=" + encodeURIComponent(id)); }).catch(function (reason) { setError(reason.message); }); }
    function savePrompt(id, content, note) { api.savePrompt(id, content, note).then(function (item) { setDrawer({ type: "prompt", item: item }); return loadHarness(); }).catch(function (reason) { setError(reason.message); }); }
    function saveModelProfile(profile) { api.saveModelProfile(profile).then(function (saved) { setModelProfileId(saved.profile_id); setModel(saved.model || model); setPromptId(saved.default_prompt_id || promptId); return loadHarness(); }).catch(function (reason) { setError(reason.message); }); }
    function openTraceList(query) { const filters = traceFiltersFromSearch(query || ""); applyTraceFilters(filters); }
    function openObservability(id) { setView("observability"); history.pushState({}, "", "/ai-observability" + (id ? "?job_id=" + encodeURIComponent(id) : "")); loadObservability(id).catch(function (reason) { setError(reason.message); }); }
    function openRule(ruleId) { setDrawer({ type: null, item: null }); setRuleFocus(ruleId); changeView("rules"); }
    async function importCurrentTemplates() {
      const templates = (result && result.top_templates || []).map(function (item) { return { template_hash: item.template_hash, template: item.template, component: item.component, count: item.count || 0, risk_levels: item.risk_levels || [] }; }).filter(function (item) { return item.template_hash && item.template; });
      if (!templates.length) { setError("当前分析结果没有可导入的 Drain3 模板"); return; }
      try { await api.importDrainTemplates(templates); await loadDrainQuality(); } catch (reason) { setError(reason.message); }
    }
    async function annotateTemplate(payload) { try { await api.annotateDrainTemplate(payload); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function changeTemplate(templateHash, payload) { try { await api.changeDrainTemplate(templateHash, payload); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function rollbackTemplate(templateHash, payload) { try { await api.rollbackDrainTemplate(templateHash, payload); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function changeDrainProfile(profileId, action) { try { await api.promoteDrainProfile(profileId, action, { confirmed: true, reviewer: "local-operator" }); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function createDrainConfig(payload) { try { await api.createDrainConfig(payload); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function saveDrainConfig(configId, payload) { try { await api.saveDrainConfig(configId, payload); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function validateDrainConfig(configId, payload) { try { return await api.validateDrainConfig(configId, payload); } catch (reason) { setError(reason.message); throw reason; } }
    async function publishDrainConfig(configId, payload) { try { await api.publishDrainConfig(configId, payload); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function rollbackDrainConfig(configId, payload) { try { await api.rollbackDrainConfig(configId, payload); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    const workspace = h(Workspace, { snapshot: snapshot, selectedId: selectedId, onSelect: setSelectedId, onRetry: retry, onOpenTraces: openTraceList, onOpenObservability: openObservability });
    const activePrompts = prompts.filter(function (prompt) { return prompt.analysis_type === "feature_extract" && prompt.status === "active"; });
    const activeProfiles = modelProfiles.profiles || [];
    const traceRule = drawer.item && rules.find(function (rule) { return rule.lineage && rule.lineage.trace_id === drawer.item.trace_id; });
    const drawerContent = !drawer.item ? null : (drawer.type === "prompt" ? h(PromptDrawer, { item: drawer.item, onSave: savePrompt, onOpenTrace: openTrace }) : h(TraceDrawer, { item: drawer.item, rule: traceRule, onOpenRule: openRule }));
    return h("div", { className: "app-shell" },
      h("header", { className: "topbar" }, h("div", { className: "brand" }, h("i", null, "L"), h("div", null, h("b", null, "LOGRISK"), h("span", null, "FEATURE REVIEW"))), h("div", { className: "system-status" }, h("span", { className: ollama.online ? "online" : "offline" }, "● Ollama " + (ollama.online ? "在线" : "离线")), h("span", null, model), h("button", { className: "prompt-pill", onClick: function () { changeView("prompts"); } }, "Prompt " + (harness.current_prompt_id || promptId)), h("span", { className: harness.trace_enabled ? "trace-on" : "trace-off" }, "● Trace " + (harness.trace_enabled ? "ON" : "OFF")))),
      h(Sidebar, { active: view, onChange: changeView }),
      h("main", null,
        !["drainQuality", "settings"].includes(view) && h("div", { className: "page-head" }, h("div", null, h("h1", null, "日志特征工作台"), h("p", null, "上传日志、复用规则、识别未知特征并人工审批")), h("label", { className: "new-analysis" }, "＋ 新建分析", h("input", { type: "file", onChange: function (event) { loadFile(event.target.files && event.target.files[0]); } }))),
        error && h("div", { className: "error-banner" }, error, h("button", { onClick: function () { setError(""); } }, "×")),
        view === "overview" && h(React.Fragment, null,
          h("section", { className: "upload-panel" }, h("div", null, h("b", null, fileName || "选择 result.json、JSONL、TXT、LOG、GZ 或无后缀日志"), h("span", null, result ? (result.risk_entities || []).length + " 个风险实体，已完成本地预处理" : "10MB 以内直接分析；超过 10MB 自动分片上传，Linux messages / syslog 无后缀文件也支持上传")), busy && h("div", { className: "upload-progress" }, uploadProgress && h("span", null, "上传进度：" + Math.round((uploadProgress.progress || 0) * 100) + "%（" + uploadProgress.received_chunks + " / " + uploadProgress.total_chunks + " chunks）"), preprocessProgress && h("span", null, "预处理阶段：" + (preprocessProgress.stage || "queued") + "，记录 " + (preprocessProgress.records_parsed || 0) + (preprocessProgress.drain3_partitions_total ? "，Drain3 分片 " + (preprocessProgress.drain3_partitions_completed || 0) + " / " + preprocessProgress.drain3_partitions_total : ""))), h("div", { className: "analysis-config" }, h("label", null, "分析流程", h("select", { value: "feature_extract", disabled: true }, h("option", { value: "feature_extract" }, "日志特征识别"))), h("label", null, "模型 Profile", h("select", { value: modelProfileId, onChange: function (event) { const profile = activeProfiles.find(function (item) { return item.profile_id === event.target.value; }) || {}; setModelProfileId(event.target.value); if (profile.model) setModel(profile.model); if (profile.default_prompt_id) setPromptId(profile.default_prompt_id); } }, activeProfiles.map(function (profile) { return h("option", { value: profile.profile_id, key: profile.profile_id }, profile.profile_id); }))), h("label", null, "模型", h("input", { value: model, onChange: function (event) { setModel(event.target.value); } })), h("label", null, "Prompt", h("select", { value: promptId, onChange: function (event) { setPromptId(event.target.value); } }, activePrompts.map(function (prompt) { return h("option", { value: prompt.prompt_id, key: prompt.prompt_id }, prompt.prompt_id); }))), h("label", null, "重试次数", h("select", { value: retryCount, onChange: function (event) { setRetryCount(Number(event.target.value)); } }, [0, 1, 2, 3].map(function (count) { return h("option", { value: count, key: count }, count + " 次"); }))), h("label", null, "阈值", h("input", { type: "number", value: threshold, onChange: function (event) { setThreshold(event.target.value); } })), h("button", { className: "primary-button", disabled: !result || busy, onClick: start }, busy ? "处理中…" : "开始识别"))), h(MetricsGrid, { snapshot: snapshot, result: result, daily: systemMetrics }), h(LiveProcessing, { snapshot: snapshot, result: result })),
        view === "queue" && h(React.Fragment, null, h(MetricsGrid, { snapshot: snapshot, result: result, daily: systemMetrics }), h(LiveProcessing, { snapshot: snapshot, result: result }), workspace),
        view === "observability" && h(AIObservabilityPage, { summary: obsSummary, progress: obsProgress, events: obsEvents, onRefresh: function () { loadObservability(obsProgress && obsProgress.job_id).catch(function (reason) { setError(reason.message); }); }, onOpenTrace: openTrace, onReview: function () { changeView("review"); }, onRules: function () { changeView("rules"); }, onNewAnalysis: function () { changeView("overview"); } }),
        view === "traces" && h(AITracePage, { traces: traces, harness: harness, traceFilters: traceFilters, onFilter: applyTraceFilters, onOpenTrace: openTrace }),
        view === "prompts" && h(PromptManagement, { prompts: prompts, currentPrompt: harness.current_prompt_id || promptId, onOpenPrompt: openPrompt }),
        view === "modelProfiles" && h(ModelProfilesPage, { profiles: modelProfiles, selectedProfileId: modelProfileId, onSelect: function (profile) { setModelProfileId(profile.profile_id); setModel(profile.model || model); setPromptId(profile.default_prompt_id || promptId); }, onSave: saveModelProfile }),
        view === "review" && h("section", { className: "approval-workspace" },
          h(FeatureList, { features: snapshot && snapshot.features || [], selectedId: selectedId, onSelect: setSelectedId }),
          h(FeatureEvidence, { feature: selected, onSelectTemplate: setSelectedTemplate }),
          h(ReviewEditor, { feature: selected, selectedTemplate: selectedTemplate, onSave: save, onOpenTrace: openTrace })),
        view === "rules" && h(RuleLibrary, { rules: rules, focusRuleId: ruleFocus, onOpenTrace: openTrace }),
        view === "drainQuality" && h(DrainQualityPage, { data: drainQuality, onRefresh: loadDrainQuality, onImport: importCurrentTemplates, onAnnotate: annotateTemplate, onTemplateChange: changeTemplate, onTemplateRollback: rollbackTemplate, onProfile: changeDrainProfile, onConfigCreate: createDrainConfig, onConfigSave: saveDrainConfig, onConfigValidate: validateDrainConfig, onConfigPublish: publishDrainConfig, onConfigRollback: rollbackDrainConfig }),
        view === "settings" && h(BackendSettings, { onSaved: function () { setError(""); } }),
        view === "export" && h("section", { className: "surface export-surface" }, h("h2", null, "导出记录"), h("p", null, "导出包只包含人工批准或历史规则复用的脱敏特征，不包含原始日志和 RCA 结论。"), h("button", { className: "primary-button", disabled: !jobId || !(snapshot && snapshot.features || []).some(function (feature) { return feature.status === "approved"; }), onClick: function () { api.exportApproved(jobId).catch(function (reason) { setError(reason.message); }); } }, "导出已批准特征 JSON"))),
      h(Drawer, { title: drawer.type === "prompt" ? "Prompt 详情" : "Trace 详情", subtitle: drawer.item && (drawer.item.prompt_id || drawer.item.trace_id), item: drawer.item, onClose: function () { setDrawer({ type: null, item: null }); } }, drawerContent));
  }

  ReactDOM.createRoot(document.getElementById("root")).render(h(React.StrictMode, null, h(App)));
}());
