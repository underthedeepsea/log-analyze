(function () {
  "use strict";
  const h = React.createElement;
  const { useEffect, useMemo, useRef, useState } = React;
  const INLINE_MAX_BYTES = 10 * 1024 * 1024;
  const LARGE_CHUNK_BYTES = 1024 * 1024;
  const DEPLOYMENT_API_BASE = String(window.LOGRISK_CONFIG && window.LOGRISK_CONFIG.apiBase || "").replace(/\/$/, "");
  const SEMANTIC_TEST_EXAMPLES = {
    container_runtime: { component: "containerd", message: "container exited with code 137" },
    kubernetes: { component: "kubelet", message: "Pod eviction event Reason=Evicted" },
    linux: { component: "kernel", message: "request failed errno=5 signal=9 HTTP 503" },
    nvidia: { component: "kernel", message: "NVRM: Xid 79, GPU has fallen off the bus" },
  };

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
    if (!response.ok) {
      const error = new Error(payload.error || "请求失败 (" + response.status + ")");
      error.code = payload.code || "http_" + response.status;
      error.requestId = payload.request_id || "";
      throw error;
    }
    return payload;
  }

  const api = {
    health: function () { return jsonRequest("/api/health"); },
    config: function () { return jsonRequest("/api/config"); },
    status: function () { return jsonRequest("/api/ollama/status"); },
    rules: function () { return jsonRequest("/api/rules"); },
    governedRules: function (query) { return jsonRequest("/api/rule-governance/rules" + (query || "")); },
    ruleReviewQueue: function () { return jsonRequest("/api/rule-governance/review-queue"); },
    ruleDetail: function (ruleId) { return jsonRequest("/api/rule-governance/rules/" + encodeURIComponent(ruleId)); },
    changeRuleStatus: function (ruleId, payload) { return jsonRequest("/api/rule-governance/rules/" + encodeURIComponent(ruleId) + "/status", { method: "POST", body: JSON.stringify(payload) }); },
    addRuleFeedback: function (ruleId, payload) { return jsonRequest("/api/rule-governance/rules/" + encodeURIComponent(ruleId) + "/feedback", { method: "POST", body: JSON.stringify(payload) }); },
    rollbackRule: function (ruleId, payload) { return jsonRequest("/api/rule-governance/rules/" + encodeURIComponent(ruleId) + "/rollback", { method: "POST", body: JSON.stringify(payload) }); },
    metrics: function () { return jsonRequest("/api/metrics"); },
    drainDatasets: function () { return jsonRequest("/api/drain-quality/datasets"); },
    drainAnnotations: function () { return jsonRequest("/api/drain-quality/annotations"); },
    drainEvalRuns: function () { return jsonRequest("/api/drain-quality/eval-runs"); },
    benchmarkOverview: function () { return jsonRequest("/api/benchmark-center/overview"); },
    benchmarkSuites: function () { return jsonRequest("/api/benchmark-center/suites?page_size=100"); },
    benchmarkRuns: function () { return jsonRequest("/api/benchmark-center/runs?page_size=100"); },
    benchmarkTrends: function () { return jsonRequest("/api/benchmark-center/trends"); },
    benchmarkLeaderboard: function () { return jsonRequest("/api/benchmark-center/leaderboard"); },
    benchmarkRun: function (runId) { return jsonRequest("/api/benchmark-center/runs/" + encodeURIComponent(runId)); },
    createBenchmarkRun: function (payload) { return jsonRequest("/api/benchmark-center/runs", { method: "POST", body: JSON.stringify(payload) }); },
    cancelBenchmarkRun: function (runId) { return jsonRequest("/api/benchmark-center/runs/" + encodeURIComponent(runId) + "/cancel", { method: "POST", body: JSON.stringify({ operator: "local-operator" }) }); },
    compareBenchmarkRuns: function (payload) { return jsonRequest("/api/benchmark-center/comparisons", { method: "POST", body: JSON.stringify(payload) }); },
    evaluateBenchmarkGate: function (payload) { return jsonRequest("/api/benchmark-center/gates/evaluate", { method: "POST", body: JSON.stringify(payload) }); },
    drainProfiles: function () { return jsonRequest("/api/drain-quality/profiles"); },
    drainTemplates: function () { return jsonRequest("/api/drain-quality/templates"); },
    drainConfigs: function () { return jsonRequest("/api/drain-quality/configs"); },
    drainConfigVersion: function (configId, version) { return jsonRequest("/api/drain-quality/configs/" + encodeURIComponent(configId) + "/versions/" + Number(version)); },
    createDrainConfig: function (payload) { return jsonRequest("/api/drain-quality/configs", { method: "POST", body: JSON.stringify(payload) }); },
    saveDrainConfig: function (configId, payload) { return jsonRequest("/api/drain-quality/configs/" + encodeURIComponent(configId) + "/versions", { method: "POST", body: JSON.stringify(payload) }); },
    validateDrainConfig: function (configId, payload) { return jsonRequest("/api/drain-quality/configs/" + encodeURIComponent(configId) + "/validate", { method: "POST", body: JSON.stringify(payload) }); },
    publishDrainConfig: function (configId, payload) { return jsonRequest("/api/drain-quality/configs/" + encodeURIComponent(configId) + "/publish", { method: "POST", body: JSON.stringify(payload) }); },
    rollbackDrainConfig: function (configId, payload) { return jsonRequest("/api/drain-quality/configs/" + encodeURIComponent(configId) + "/rollback", { method: "POST", body: JSON.stringify(payload) }); },
    semanticDictionaries: function () { return jsonRequest("/api/semantic/dictionaries"); },
    semanticDictionaryVersion: function (dictionaryId, version) { return jsonRequest("/api/semantic/dictionaries/" + encodeURIComponent(dictionaryId) + "/versions/" + Number(version)); },
    createSemanticCandidate: function (dictionaryId) { return jsonRequest("/api/semantic/dictionaries/" + encodeURIComponent(dictionaryId) + "/candidates", { method: "POST", body: JSON.stringify({ operator: "local-operator" }) }); },
    saveSemanticDictionary: function (dictionaryId, version, customRules) { return jsonRequest("/api/semantic/dictionaries/" + encodeURIComponent(dictionaryId) + "/candidates/" + Number(version), { method: "PUT", body: JSON.stringify({ custom_rules: customRules, operator: "local-operator" }) }); },
    validateSemanticDictionary: function (dictionaryId, version) { return jsonRequest("/api/semantic/dictionaries/" + encodeURIComponent(dictionaryId) + "/candidates/" + Number(version) + "/validate", { method: "POST", body: "{}" }); },
    publishSemanticDictionary: function (dictionaryId, version) { return jsonRequest("/api/semantic/dictionaries/" + encodeURIComponent(dictionaryId) + "/candidates/" + Number(version) + "/publish", { method: "POST", body: JSON.stringify({ confirmed: true, operator: "local-operator" }) }); },
    rollbackSemanticDictionary: function (dictionaryId, version) { return jsonRequest("/api/semantic/dictionaries/" + encodeURIComponent(dictionaryId) + "/rollback", { method: "POST", body: JSON.stringify({ version: Number(version), confirmed: true, operator: "local-operator" }) }); },
    testSemanticDictionary: function (payload) { return jsonRequest("/api/semantic/test", { method: "POST", body: JSON.stringify(payload) }); },
    nodeRisks: function (query) { return jsonRequest("/api/node-risks" + (query || "")); },
    nodeRisk: function (cluster, node) { return jsonRequest("/api/node-risks/" + encodeURIComponent(cluster) + "/" + encodeURIComponent(node)); },
    nodeRiskDaily: function (cluster, node) { return jsonRequest("/api/node-risks/" + encodeURIComponent(cluster) + "/" + encodeURIComponent(node) + "/daily"); },
    acknowledgeNodeEvent: function (eventId, payload) { return jsonRequest("/api/node-risks/events/" + encodeURIComponent(eventId) + "/acknowledge", { method: "POST", body: JSON.stringify(payload) }); },
    recoverNodeEvent: function (eventId, payload) { return jsonRequest("/api/node-risks/events/" + encodeURIComponent(eventId) + "/recover", { method: "POST", body: JSON.stringify(payload) }); },
    riskSemantics: function () { return jsonRequest("/api/semantics"); },
    riskSemanticVersions: function (id) { return jsonRequest("/api/semantics/" + encodeURIComponent(id) + "/versions"); },
    unclassifiedRiskSemantics: function () { return jsonRequest("/api/semantics/unclassified"); },
    createRiskSemantic: function (payload) { return jsonRequest("/api/semantics", { method: "POST", body: JSON.stringify(payload) }); },
    updateRiskSemantic: function (id, payload) { return jsonRequest("/api/semantics/" + encodeURIComponent(id), { method: "PUT", body: JSON.stringify(payload) }); },
    publishRiskSemantic: function (id, payload) { return jsonRequest("/api/semantics/" + encodeURIComponent(id) + "/publish", { method: "POST", body: JSON.stringify(payload) }); },
    testRiskSemantic: function (payload) { return jsonRequest("/api/semantics/test", { method: "POST", body: JSON.stringify(payload) }); },
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
    modelConnections: function () { return jsonRequest("/api/ai-harness/connections"); },
    saveModelConnection: function (connection) { return jsonRequest("/api/ai-harness/connections", { method: "POST", body: JSON.stringify(connection) }); },
    updateModelConnection: function (id, changes) { return jsonRequest("/api/ai-harness/connections/" + encodeURIComponent(id), { method: "PATCH", body: JSON.stringify(changes) }); },
    testModelConnection: function (id) { return jsonRequest("/api/ai-harness/connections/" + encodeURIComponent(id) + "/test", { method: "POST", body: "{}" }); },
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
    ["rules", "⌘", "规则治理"], ["nodeRisks", "△", "服务器风险"], ["semanticLibrary", "≋", "风险语义库"], ["drainQuality", "◇", "评测中心 · 模板质量"], ["benchmarkCenter", "◎", "评测与基准"], ["export", "⇩", "导出记录"], ["settings", "⚙", "系统设置"],
  ];
  const statusNames = { queued: "等待分析", running: "识别中", completed: "Ollama 完成", failed: "识别失败", skipped: "低风险跳过", rule_matched: "规则复用" };

  function Sidebar(props) {
    const scrollRef = useRef(null), railRef = useRef(null), dragRef = useRef(null);
    const [scrollbar, setScrollbar] = useState({ visible: false, height: 46, top: 0 });
    function updateSidebarScroll() {
      const scroll = scrollRef.current, rail = railRef.current;
      if (!scroll || !rail) return;
      const visible = scroll.scrollHeight > scroll.clientHeight + 1;
      const height = Math.max(46, rail.clientHeight * scroll.clientHeight / Math.max(1, scroll.scrollHeight));
      const travel = Math.max(0, rail.clientHeight - height);
      const top = travel * scroll.scrollTop / Math.max(1, scroll.scrollHeight - scroll.clientHeight);
      setScrollbar({ visible: visible, height: height, top: top });
    }
    useEffect(function () {
      const scroll = scrollRef.current;
      if (!scroll) return undefined;
      const observer = window.ResizeObserver ? new window.ResizeObserver(updateSidebarScroll) : null;
      scroll.addEventListener("scroll", updateSidebarScroll, { passive: true });
      window.addEventListener("resize", updateSidebarScroll);
      if (observer) observer.observe(scroll);
      requestAnimationFrame(updateSidebarScroll);
      return function () {
        scroll.removeEventListener("scroll", updateSidebarScroll);
        window.removeEventListener("resize", updateSidebarScroll);
        if (observer) observer.disconnect();
      };
    }, []);
    function startSidebarThumbDrag(event) {
      event.stopPropagation();
      event.currentTarget.setPointerCapture(event.pointerId);
      dragRef.current = { y: event.clientY, scrollTop: scrollRef.current.scrollTop };
    }
    function moveSidebarThumb(event) {
      if (!dragRef.current || !event.currentTarget.hasPointerCapture(event.pointerId)) return;
      const scroll = scrollRef.current, rail = railRef.current;
      const maxScroll = scroll.scrollHeight - scroll.clientHeight;
      const travel = rail.clientHeight - scrollbar.height;
      scroll.scrollTop = dragRef.current.scrollTop + (event.clientY - dragRef.current.y) * maxScroll / Math.max(1, travel);
    }
    function stopSidebarThumbDrag(event) {
      dragRef.current = null;
      if (event.currentTarget.hasPointerCapture(event.pointerId)) event.currentTarget.releasePointerCapture(event.pointerId);
    }
    function jumpSidebarScroll(event) {
      if (event.target !== event.currentTarget) return;
      const scroll = scrollRef.current, rect = railRef.current.getBoundingClientRect();
      scroll.scrollTop = (event.clientY - rect.top) / Math.max(1, rect.height) * (scroll.scrollHeight - scroll.clientHeight);
    }
    return h("aside", { className: "sidebar", id: "primary-navigation" },
      h("div", { className: "nav-label" }, "功能导航"),
      h("div", { className: "sidebar-frame" },
        h("nav", { className: "sidebar-scroll", ref: scrollRef }, navItems.map(function (item) {
          return h("button", { key: item[0], type: "button", className: "nav-item " + (props.active === item[0] ? "active" : ""), onClick: function () { props.onChange(item[0]); } }, h("span", null, item[2]));
        })),
        h("div", { className: "sidebar-rail " + (scrollbar.visible ? "visible" : ""), ref: railRef, onPointerDown: jumpSidebarScroll, "aria-hidden": "true" },
          h("span", { className: "sidebar-thumb", style: { height: scrollbar.height + "px", transform: "translateY(" + scrollbar.top + "px)" }, onPointerDown: startSidebarThumbDrag, onPointerMove: moveSidebarThumb, onPointerUp: stopSidebarThumbDrag, onPointerCancel: stopSidebarThumbDrag }))));
  }

  function Metric(props) {
    return h("div", { className: "metric-card " + (props.tone || "") }, h("strong", null, String(props.value)), h("span", null, props.label));
  }

  function shortHash(value) { return value ? String(value).slice(0, 10) : "—"; }
  function timeText(value) { return value ? new Date(value).toLocaleString() : "—"; }
  function pathToView(path) { return path === "/benchmark-center" ? "benchmarkCenter" : (path === "/prompts" ? "prompts" : (path === "/model-profiles" ? "modelProfiles" : (path === "/ai-traces" ? "traces" : (path === "/ai-observability" ? "observability" : (path === "/drain-quality" ? "drainQuality" : (path === "/rules" ? "rules" : (path.startsWith("/node-risks") ? "nodeRisks" : (path === "/semantic-library" ? "semanticLibrary" : (path === "/settings" ? "settings" : "overview"))))))))); }
  function routeForView(view) { return view === "benchmarkCenter" ? "/benchmark-center" : (view === "prompts" ? "/prompts" : (view === "modelProfiles" ? "/model-profiles" : (view === "traces" ? "/ai-traces" : (view === "observability" ? "/ai-observability" : (view === "drainQuality" ? "/drain-quality" : (view === "rules" ? "/rules" : (view === "nodeRisks" ? "/node-risks" : (view === "semanticLibrary" ? "/semantic-library" : (view === "settings" ? "/settings" : "/"))))))))); }
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

  function evidenceSemanticLabel(template, index) {
    const riskSemantic = template && template.risk_semantic || {};
    const semanticFields = riskSemantic.semantic_fields || {};
    const extractedFields = template && template.semantic_fields || {};
    const extractedXid = Array.isArray(extractedFields.xid_code) && extractedFields.xid_code.length
      ? extractedFields.xid_code[0].value : extractedFields.xid_code;
    const xidCode = semanticFields.xid_code != null ? semanticFields.xid_code : extractedXid;
    return {
      name: xidCode != null ? "Xid " + xidCode : "证据模板 " + String(index + 1).padStart(2, "0"),
      riskType: riskSemantic.risk_type || template.category || "未分类语义",
      severity: riskSemantic.severity || template.severity || "unknown",
    };
  }

  function FeatureEvidence(props) {
    const feature = props.feature;
    const templates = feature && feature.source_templates || [];
    const semanticLabels = templates.map(evidenceSemanticLabel);
    const [selectedTemplateIndex, setSelectedTemplateIndex] = useState(0);
    useEffect(function () {
      setSelectedTemplateIndex(0);
      props.onSelectTemplate && props.onSelectTemplate(templates[0] || null);
    }, [feature && feature.candidate_id]);
    return h("section", { className: "surface evidence-panel" },
      h("div", { className: "surface-head" }, h("b", null, "当前特征关联的证据模板"), h("span", null, templates.length + " 个证据模板")),
      h("div", { className: "evidence-body" },
        h("div", { className: "evidence-notice" }, feature ? "当前选择的是 1 个候选特征；以下 " + templates.length + " 个 Drain3 模板是它的脱敏证据，不是 " + templates.length + " 个候选特征。系统未保存原始日志。" : "当前展示 Drain3 脱敏特征模板，系统未保存原始日志"),
        !feature && h("div", { className: "empty-state" }, "选择特征后查看日志证据"),
        feature && templates.length === 0 && h("div", { className: "empty-state" }, "暂无脱敏模板证据"),
        feature && templates.length > 1 && h("div", { className: "evidence-relation" },
          h("b", null, "归并关系"),
          h("span", null, semanticLabels.map(function (item) { return item.name; }).join(" + ") + " → " + (feature.title || "当前候选特征")),
          h("small", null, "同属当前候选特征，分别保留模板 Hash 与确定性风险语义")),
        templates.map(function (template, index) {
          const firstSeen = template.first_seen || feature.window_start || "—";
          const lastSeen = template.last_seen || feature.window_end || "—";
          const semantic = semanticLabels[index];
          return h("button", { className: "evidence-template " + (selectedTemplateIndex === index ? "active" : ""), key: (template.template_hash || "template") + "-" + index, onClick: function () { setSelectedTemplateIndex(index); props.onSelectTemplate && props.onSelectTemplate(template); } },
            h("div", { className: "evidence-meta" },
              h("span", { className: "evidence-number" }, "证据模板 " + String(index + 1).padStart(2, "0")),
              h("span", null, template.component || "unknown"),
              h("span", null, template.category || "unknown"),
              h("span", null, template.severity || "unknown"),
              h("b", null, String(template.count || 0) + " 次")),
            h("div", { className: "semantic-evidence-summary" }, h("b", null, semantic.name), h("span", null, "风险语义 " + semantic.riskType), h("em", { className: "risk-level " + String(semantic.severity).toLowerCase() }, String(semantic.severity).toUpperCase())),
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
    const templateSummary = "Drain3 模板：" + (selectedTemplate.template || "暂无模板文本");
    return {
      title: feature.title || "",
      summary: [feature.summary || "", templateSummary].filter(Boolean).join("\n\n"),
      importance: feature.importance || "medium",
      tags: (feature.tags || []).join(", "),
      reviewer_note: feature.reviewer_note || "基于当前证据模板生成审批草稿：" + selectedTemplate.template_hash,
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
    return h("section", { className: "surface review-editor" }, h("div", { className: "surface-head" }, h("b", null, "人工审批"), h("span", null, props.feature.origin === "approved_rule" ? "来自批准规则库" : "来自模型 + Drain3")), h("div", { className: "editor-body" }, field("特征标题（来自模型）", "title"), field("特征摘要（模型语义 + 当前 Drain3 模板）", "summary", "textarea"), field("标签（来自模型，逗号分隔）", "tags"), field("审批备注", "reviewer_note", "textarea"), h("div", { className: "fact-box" }, "当前证据模板：", selectedTemplate.template_hash || "—", h("br"), "组件 " + (selectedTemplate.component || "—") + " · 类别 " + (selectedTemplate.category || "—") + " · 次数 " + (selectedTemplate.count || 0), h("br"), selectedTemplate.template || "暂无模板文本"), h("div", { className: "fact-box" }, "质量门禁：已通过", h("br"), "Evaluator Score：" + (props.feature.evaluator_result && props.feature.evaluator_result.score != null ? props.feature.evaluator_result.score : "—"), h("br"), "实体 " + (props.feature.entity && props.feature.entity.id || "") + " · 风险分 " + props.feature.risk_score + " · 出现 " + props.feature.occurrence_count + " 次", h("br"), props.feature.trace_id ? "来源 " + (props.feature.prompt_id || "feature_extract_v3_compact_strict_json_en") + " · " + (props.feature.model || "—") + " · " + props.feature.trace_id : "来源：历史数据 / 未记录 Trace"), props.feature.trace_id && h("button", { className: "text-button trace-link", onClick: function () { props.onOpenTrace(props.feature.trace_id); } }, "查看 AI Trace"), h("div", { className: "editor-actions" }, h("button", { className: "reject-button", onClick: function () { save("rejected"); } }, "驳回"), h("button", { className: "primary-button", onClick: function () { save("approved"); } }, "批准并写入规则库"))));
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
    const [connections, setConnections] = useState([]);
    const [connectionDraft, setConnectionDraft] = useState({ connection_id: "", display_name: "", provider: "openai_compatible", base_url: "https://api.example.com/v1", api_key_env: "REMOTE_LLM_API_KEY", timeout_seconds: 120, enabled: true });
    const [connectionStatus, setConnectionStatus] = useState(null);
    const [connectionError, setConnectionError] = useState("");
    const [saveState, setSaveState] = useState("idle");
    const [saveMessage, setSaveMessage] = useState("");
    useEffect(function () { setDraft(current); setSaveState("idle"); setSaveMessage(""); }, [current.profile_id]);
    useEffect(function () { api.modelConnections().then(function (value) { const items = value.items || []; setConnections(items); if (items[0]) setConnectionDraft(items[0]); }).catch(function (reason) { setConnectionError(reason.message); }); }, []);
    const budget = draft.evidence_budget || {};
    function setField(key, value) { setDraft(Object.assign({}, draft, { [key]: value })); setSaveState("idle"); setSaveMessage(""); }
    function setBudget(key, value) { setDraft(Object.assign({}, draft, { evidence_budget: Object.assign({}, budget, { [key]: Number(value) || 0 }) })); setSaveState("idle"); setSaveMessage(""); }
    function createProfile() {
      const next = Object.assign({}, current, {
        profile_id: (current.profile_id || "custom") + "_copy",
        display_name: (current.display_name || current.profile_id || "Custom") + " 副本",
      });
      props.onSelect(next);
      setDraft(next);
    }
    function selectConnection(connection) { setConnectionDraft(connection); setConnectionStatus(null); setConnectionError(""); }
    function newConnection() { setConnectionDraft({ connection_id: "remote-" + Date.now(), display_name: "远端模型 API", provider: "openai_compatible", base_url: "https://api.example.com/v1", api_key_env: "REMOTE_LLM_API_KEY", timeout_seconds: 120, enabled: true, is_default: false }); setConnectionStatus(null); }
    async function saveConnection() { try { const saved = await api.saveModelConnection(connectionDraft); const value = await api.modelConnections(); setConnections(value.items || []); setConnectionDraft(saved); setConnectionError(""); } catch (reason) { setConnectionError(reason.message); } }
    async function testConnection() { try { setConnectionStatus(await api.testModelConnection(connectionDraft.connection_id)); setConnectionError(""); } catch (reason) { setConnectionError(reason.message); } }
    async function saveProfile() {
      setSaveState("saving");
      setSaveMessage("");
      try {
        const saved = await props.onSave(draft);
        if (saved) setDraft(saved);
        setSaveState("saved");
        setSaveMessage("Profile 已保存");
      } catch (reason) {
        setSaveState("error");
        setSaveMessage(reason.message || "Profile 保存失败");
      }
    }
    function setConnectionField(key, value) { setConnectionDraft(Object.assign({}, connectionDraft, { [key]: value })); }
    const head = h("div", { className: "page-head obs-head" }, h("div", null, h("h1", null, "模型画像与上下文预算"), h("p", null, "按模型参数量、上下文窗口、Prompt 策略和 Thinking 开关，控制 Evidence 输入规模与调用行为")));
    const metrics = h("section", { className: "metrics-grid" },
        h(Metric, { label: "当前模型参数量", value: current.parameter_size || "—", tone: "orange" }),
        h(Metric, { label: "上下文窗口", value: current.context_window_tokens ? Math.round(current.context_window_tokens / 1000) + "K" : "—" }),
        h(Metric, { label: "最大模板数", value: budget.max_templates || "—" }),
        h(Metric, { label: "单模板字符上限", value: budget.max_template_chars || "—" }),
        h(Metric, { label: "Thinking 模式", value: current.thinking_enabled ? "ON" : "OFF", tone: current.thinking_enabled ? "green" : "red" }),
        h(Metric, { label: "默认 Prompt", value: current.default_prompt_id || "—" }));
    const connectionPanel = h("section", { className: "surface connection-manager" },
      h("div", { className: "surface-head" }, h("div", null, h("b", null, "API 连接"), h("span", null, "Ollama 与 OpenAI-compatible 服务地址和鉴权引用")), h("button", { className: "secondary-button", onClick: newConnection }, "新增连接")),
      h("div", { className: "connection-layout" },
        h("div", { className: "connection-list" }, connections.map(function (connection) { return h("button", { className: "connection-card " + (connection.connection_id === connectionDraft.connection_id ? "active" : ""), key: connection.connection_id, onClick: function () { selectConnection(connection); } }, h("div", null, h("b", null, connection.display_name), h("span", { className: "status-chip " + (connection.enabled ? "active" : "ignored") }, connection.enabled ? "启用" : "停用")), h("small", null, connection.provider + " · " + connection.base_url), h("small", null, connection.api_key_env ? (connection.api_key_configured ? "密钥环境变量已配置" : "密钥环境变量未配置") : "无需 API Key")); })),
        h("div", { className: "connection-form" },
          h("label", null, "连接 ID", h("input", { value: connectionDraft.connection_id || "", onChange: function (event) { setConnectionField("connection_id", event.target.value); } })),
          h("label", null, "显示名称", h("input", { value: connectionDraft.display_name || "", onChange: function (event) { setConnectionField("display_name", event.target.value); } })),
          h("label", null, "Provider", h("select", { value: connectionDraft.provider || "ollama", onChange: function (event) { setConnectionField("provider", event.target.value); } }, h("option", { value: "ollama" }, "Ollama"), h("option", { value: "openai_compatible" }, "OpenAI-compatible"))),
          h("label", { className: "wide" }, "API 基础地址", h("input", { value: connectionDraft.base_url || "", onChange: function (event) { setConnectionField("base_url", event.target.value); } }), h("small", null, connectionDraft.provider === "ollama" ? "示例：http://127.0.0.1:11434" : "地址需包含 /v1，例如：https://api.example.com/v1")),
          h("label", null, "API Key 环境变量", h("input", { value: connectionDraft.api_key_env || "", disabled: connectionDraft.provider === "ollama", onChange: function (event) { setConnectionField("api_key_env", event.target.value); } }), h("small", null, "仅保存变量名，不保存密钥")),
          h("label", null, "超时（秒）", h("input", { type: "number", value: connectionDraft.timeout_seconds || 120, onChange: function (event) { setConnectionField("timeout_seconds", Number(event.target.value) || 120); } })),
          h("label", null, "状态", h("select", { value: connectionDraft.enabled === false ? "off" : "on", onChange: function (event) { setConnectionField("enabled", event.target.value === "on"); } }, h("option", { value: "on" }, "启用"), h("option", { value: "off" }, "停用"))),
          h("div", { className: "button-row wide" }, h("button", { className: "primary-button", onClick: saveConnection }, "保存连接"), h("button", { className: "secondary-button", disabled: !connectionDraft.connection_id, onClick: testConnection }, "测试连接"), connectionStatus && h("span", { className: "connection-result " + (connectionStatus.online ? "success" : "failed") }, connectionStatus.online ? "连接成功 · " + (connectionStatus.models || []).length + " 个模型" : connectionStatus.error), connectionError && h("span", { className: "connection-result failed" }, connectionError)))));
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
        h("label", null, "API 连接", h("select", { value: draft.connection_id || "ollama-local", onChange: function (event) { const connection = connections.find(function (item) { return item.connection_id === event.target.value; }); setDraft(Object.assign({}, draft, { connection_id: event.target.value, provider: connection ? connection.provider : draft.provider })); } }, connections.map(function (connection) { return h("option", { value: connection.connection_id, key: connection.connection_id }, connection.display_name + " · " + connection.provider); }))),
        h("label", null, "结构化输出", h("select", { value: draft.structured_output_mode || "json_schema", onChange: function (event) { setField("structured_output_mode", event.target.value); } }, h("option", { value: "json_schema" }, "JSON Schema"), h("option", { value: "json_object" }, "JSON Object"), h("option", { value: "prompt_only" }, "仅 Prompt 约束"))),
        h("label", null, "参数量", h("input", { value: draft.parameter_size || "", onChange: function (event) { setField("parameter_size", event.target.value); } })),
        h("label", null, "默认 Prompt", h("input", { value: draft.default_prompt_id || "", onChange: function (event) { setField("default_prompt_id", event.target.value); } })),
        h("label", null, "Thinking", h("select", { value: draft.thinking_enabled ? "on" : "off", onChange: function (event) { setField("thinking_enabled", event.target.value === "on"); } }, h("option", { value: "off" }, "Thinking OFF"), h("option", { value: "on" }, "Thinking ON"))),
        h("p", null, "关闭 Thinking 模式用于 Ollama / 支持 thinking 参数的模型。关闭后降低推理耗时，减少中间思考内容影响 JSON 输出稳定性。"),
        h("h3", null, "Context Budget"),
        h("div", { className: "budget-grid" }, ["max_templates", "max_template_chars", "max_affected_entities", "max_evidence_chars", "recommended_input_tokens", "max_output_tokens"].map(function (key) {
          const topLevel = key === "recommended_input_tokens" || key === "max_output_tokens";
          return h("label", { key: key }, h("b", null, key), h("input", { type: "number", value: topLevel ? (draft[key] || 0) : (budget[key] || 0), onChange: function (event) { const value = Number(event.target.value) || 0; if (key === "max_output_tokens") setField("max_output_tokens", value); else if (key === "recommended_input_tokens") setField("recommended_input_tokens", value); else setBudget(key, value); } }));
        })),
        h("div", { className: "button-row profile-save-row" }, h("button", { className: "primary-button", disabled: saveState === "saving", onClick: saveProfile }, saveState === "saving" ? "保存中…" : "保存 Profile"), saveMessage && h("span", { className: "connection-result " + (saveState === "saved" ? "success" : "failed") }, saveMessage)),
        h("h3", null, "调用配置预览"),
        h(CodeBlock, { value: { model_profile_id: draft.profile_id, connection_id: draft.connection_id, provider: draft.provider, model: draft.model, structured_output_mode: draft.structured_output_mode, parameter_size: draft.parameter_size, default_prompt_id: draft.default_prompt_id, thinking_enabled: draft.thinking_enabled, context_budget: Object.assign({}, budget, { recommended_input_tokens: draft.recommended_input_tokens, max_output_tokens: draft.max_output_tokens }), model_options: Object.assign({}, draft.options || {}, { think: !!draft.thinking_enabled, num_predict: draft.max_output_tokens, structured_output_mode: draft.structured_output_mode }), runtime_options: draft.runtime_options || {} } })));
    return h(React.Fragment, null, head, connectionPanel, metrics, h("section", { className: "model-profile-grid" }, list, detail));
  }

  function RuleLibrary(props) {
    const [selectedId, setSelectedId] = useState("");
    const [detail, setDetail] = useState(null);
    const [statusFilter, setStatusFilter] = useState("all");
    const [nextStatus, setNextStatus] = useState("under_review");
    const [reason, setReason] = useState("");
    const [feedbackOutcome, setFeedbackOutcome] = useState("false_positive");
    const [feedbackNote, setFeedbackNote] = useState("");
    const [loading, setLoading] = useState(false);
    const [failure, setFailure] = useState(null);
    const [busy, setBusy] = useState(false);
    useEffect(function () {
      const preferred = props.focusRuleId && props.rules.some(function (rule) { return rule.rule_id === props.focusRuleId; }) ? props.focusRuleId : selectedId;
      setSelectedId(preferred && props.rules.some(function (rule) { return rule.rule_id === preferred; }) ? preferred : (props.rules[0] && props.rules[0].rule_id || ""));
    }, [props.focusRuleId, props.rules]);
    useEffect(function () {
      if (!selectedId) { setDetail(null); return; }
      loadDetail(selectedId);
    }, [selectedId]);
    async function loadDetail(ruleId) {
      setLoading(true); setFailure(null);
      try { setDetail(await api.ruleDetail(ruleId)); }
      catch (error) { setFailure({ message: error.message, code: error.code || "request_failed", request_id: error.requestId || "", rule_id: ruleId }); }
      finally { setLoading(false); }
    }
    async function refresh(ruleId) {
      await props.onChanged();
      await loadDetail(ruleId || selectedId);
    }
    async function changeStatus(status) {
      if (!detail || !reason.trim()) { window.alert("请填写状态变更原因"); return; }
      setBusy(true); setFailure(null);
      try {
        await api.changeRuleStatus(detail.rule.rule_id, { status: status, expected_version: detail.rule.current_version, operator: "local-operator", reason: reason.trim() });
        setReason(""); await refresh(detail.rule.rule_id);
      } catch (error) { setFailure({ message: error.message, code: error.code || "request_failed", request_id: error.requestId || "", rule_id: detail.rule.rule_id }); }
      finally { setBusy(false); }
    }
    async function recordFeedback() {
      if (!detail) return;
      setBusy(true); setFailure(null);
      try {
        await api.addRuleFeedback(detail.rule.rule_id, { outcome: feedbackOutcome, operator: "local-operator", note: feedbackNote.trim() });
        setFeedbackNote(""); await refresh(detail.rule.rule_id);
      } catch (error) { setFailure({ message: error.message, code: error.code || "request_failed", request_id: error.requestId || "", rule_id: detail.rule.rule_id }); }
      finally { setBusy(false); }
    }
    async function rollbackVersion(version) {
      if (!detail || !window.confirm("确认回滚规则 " + detail.rule.rule_id + " 到 v" + version + "？系统会创建新版本，不覆盖历史记录。")) return;
      const rollbackReason = reason.trim() || "人工确认回滚到 v" + version;
      setBusy(true); setFailure(null);
      try {
        await api.rollbackRule(detail.rule.rule_id, { target_version: version, expected_version: detail.rule.current_version, confirmed: true, operator: "local-operator", reason: rollbackReason });
        setReason(""); await refresh(detail.rule.rule_id);
      } catch (error) { setFailure({ message: error.message, code: error.code || "request_failed", request_id: error.requestId || "", rule_id: detail.rule.rule_id }); }
      finally { setBusy(false); }
    }
    function copyDiagnostic() {
      const text = JSON.stringify({ page: "rule-governance", api_base: currentApiBase(), diagnostic: failure }, null, 2);
      if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(text);
      else window.prompt("复制诊断信息", text);
    }
    function lineageStatus(rule) {
      const lineage = rule.lineage;
      if (!lineage) return ["历史规则", "history"];
      if (lineage.trace_id && lineage.prompt_id && lineage.model && lineage.evidence_hash && rule.approved_at) return ["可追溯", "complete"];
      return ["部分可追溯", "partial"];
    }
    function identity(item) { return item.template_fingerprint || item.template_hash || "—"; }
    const statusNames = { active: "启用", disabled: "停用", under_review: "复审中", deprecated: "已废弃", archived: "已归档" };
    const reviewIds = new Set((props.reviewQueue && props.reviewQueue.items || []).map(function (rule) { return rule.rule_id; }));
    const visible = props.rules.filter(function (rule) { return statusFilter === "all" || (statusFilter === "review" ? reviewIds.has(rule.rule_id) : rule.status === statusFilter); });
    const totalHits = props.rules.reduce(function (sum, rule) { return sum + Number(rule.health && rule.health.hits_30d || 0); }, 0);
    const risky = props.rules.filter(function (rule) { return rule.health && rule.health.level !== "healthy"; }).length;
    const selectedRule = detail && detail.rule;
    const health = detail && detail.health || {};
    return h("div", { className: "rule-governance-page" },
      h("section", { className: "rule-governance-hero" }, h("div", null, h("span", { className: "eyebrow" }, "RULE LIFECYCLE GOVERNANCE"), h("h2", null, "规则生命周期治理"), h("p", null, "管理人工批准规则的状态、健康度、复审、版本与审计回滚。")), h("span", { className: "permission-chip" }, "当前权限：本地治理者")),
      h("section", { className: "rule-governance-metrics" },
        h("div", null, h("span", null, "规则资产"), h("b", null, props.rules.length), h("small", null, "全部人工批准规则")),
        h("div", null, h("span", null, "启用中"), h("b", null, props.rules.filter(function (rule) { return rule.status === "active"; }).length), h("small", null, "参与特征匹配")),
        h("div", null, h("span", null, "待复审"), h("b", null, props.reviewQueue && props.reviewQueue.total != null ? props.reviewQueue.total : risky), h("small", null, "健康度或复审异常")),
        h("div", null, h("span", null, "30 天命中"), h("b", null, totalHits), h("small", null, "规则复用事件"))),
      failure && h("section", { className: "rule-diagnostic" }, h("div", null, h("b", null, "规则治理请求失败"), h("span", null, failure.message), h("code", null, (failure.code || "unknown") + (failure.request_id ? " · " + failure.request_id : ""))), h("button", { className: "secondary-button", onClick: copyDiagnostic }, "复制诊断信息")),
      h("div", { className: "rule-governance-layout" },
        h("section", { className: "surface rule-catalog" },
          h("div", { className: "surface-head" }, h("div", null, h("b", null, "批准规则库"), h("span", null, "状态筛选与规则健康度")), h("span", null, visible.length + " 条")),
          h("div", { className: "rule-status-filters" }, [["all", "全部"], ["review", "待复审"], ["active", "启用"], ["under_review", "复审中"], ["disabled", "停用"], ["deprecated", "废弃"], ["archived", "归档"]].map(function (item) { return h("button", { key: item[0], className: statusFilter === item[0] ? "active" : "", onClick: function () { setStatusFilter(item[0]); } }, item[1]); })),
          props.loading && h("div", { className: "empty-state compact" }, "正在加载规则资产…"),
          !props.loading && visible.length === 0 && h("div", { className: "empty-state" }, props.rules.length ? "当前筛选条件下没有规则" : "批准首条特征后建立规则资产库"),
          h("div", { className: "rule-catalog-list" }, visible.map(function (rule) {
            const score = Number(rule.health && rule.health.score || 0), status = lineageStatus(rule);
            return h("button", { className: "rule-catalog-item " + (selectedId === rule.rule_id ? "active" : ""), key: rule.rule_id, onClick: function () { setSelectedId(rule.rule_id); } },
              h("div", { className: "rule-health-score " + (rule.health && rule.health.level || "attention"), style: { "--score": score + "%" } }, h("b", null, score), h("span", null, "健康")),
              h("div", { className: "rule-catalog-copy" }, h("div", null, h("b", null, rule.title || rule.feature_type), h("span", { className: "status-chip " + rule.status }, statusNames[rule.status] || rule.status)), h("small", null, rule.rule_id + " · v" + rule.current_version), h("p", null, rule.summary || "暂无摘要"), h("div", { className: "rule-mini-meta" }, h("span", null, "7 天命中 " + (rule.health && rule.health.hits_7d || 0)), h("span", null, "误报率 " + Math.round(Number(rule.health && rule.health.false_positive_rate_30d || 0) * 100) + "%"), h("span", { className: "lineage-badge " + status[1] }, status[0]))));
          }))),
        h("section", { className: "surface rule-detail-panel" },
          loading && h("div", { className: "empty-state" }, "正在加载规则详情…"),
          !loading && !selectedRule && h("div", { className: "empty-state" }, "请选择规则查看健康度和版本历史"),
          !loading && selectedRule && h(React.Fragment, null,
            h("div", { className: "rule-detail-head" }, h("div", null, h("span", { className: "eyebrow" }, selectedRule.feature_type), h("h3", null, selectedRule.title), h("p", null, selectedRule.rule_id + " · 当前 v" + selectedRule.current_version)), h("span", { className: "status-chip " + selectedRule.status }, statusNames[selectedRule.status] || selectedRule.status)),
            h("section", { className: "rule-health-section" }, h("div", { className: "section-title" }, h("div", null, h("h3", null, "规则健康度"), h("p", null, "基于命中、误报、跨集群复用与复审时间计算")), h("b", { className: "health-level " + health.level }, health.score + " / 100")), h("div", { className: "rule-health-grid" }, [["7 天命中", health.hits_7d || 0], ["30 天命中", health.hits_30d || 0], ["误报率", Math.round(Number(health.false_positive_rate_30d || 0) * 100) + "%"], ["跨集群命中", health.cluster_count_30d || 0]].map(function (item) { return h("div", { key: item[0] }, h("span", null, item[0]), h("b", null, item[1])); })), health.review_reasons && health.review_reasons.length > 0 && h("div", { className: "review-reasons" }, health.review_reasons.map(function (item) { return h("span", { key: item }, item); }))),
            h("section", { className: "rule-facts" }, h("div", { className: "section-title" }, h("div", null, h("h3", null, "规则事实"), h("p", null, "审批内容、匹配签名和来源链路均保持脱敏"))), h("p", null, selectedRule.summary || "暂无摘要"), h("small", { className: "lineage-state" }, "Lineage 状态：" + lineageStatus(selectedRule)[0]), h("div", { className: "rule-tag-row" }, (selectedRule.tags || []).map(function (tag) { return h("span", { key: tag }, tag); })), h("div", { className: "rule-signature-list" }, (selectedRule.template_signatures || []).map(function (item) { const value = identity(item); return h("code", { key: value + item.category }, value + " · " + (item.category || "未分类")); })), selectedRule.lineage && selectedRule.lineage.trace_id && h("button", { className: "text-button", onClick: function () { props.onOpenTrace(selectedRule.lineage.trace_id); } }, "查看来源 AI Trace")),
            h("section", { className: "rule-review-workbench" }, h("div", { className: "section-title" }, h("div", null, h("h3", null, "复审工作台"), h("p", null, "所有状态变更和反馈均写入追加式审计记录"))), h("div", { className: "review-control-grid" }, h("label", null, h("span", null, "目标状态"), h("select", { value: nextStatus, onChange: function (event) { setNextStatus(event.target.value); } }, Object.keys(statusNames).map(function (status) { return h("option", { key: status, value: status }, statusNames[status]); }))), h("label", { className: "review-reason-field" }, h("span", null, "变更或回滚原因"), h("input", { value: reason, placeholder: "填写可审计的操作原因", onChange: function (event) { setReason(event.target.value); } })), h("button", { className: "primary-button", disabled: busy, onClick: function () { changeStatus(nextStatus); } }, busy ? "提交中…" : "应用状态")), h("div", { className: "review-control-grid feedback" }, h("label", null, h("span", null, "反馈结论"), h("select", { value: feedbackOutcome, onChange: function (event) { setFeedbackOutcome(event.target.value); } }, h("option", { value: "false_positive" }, "误报"), h("option", { value: "confirmed" }, "命中有效"))), h("label", { className: "review-reason-field" }, h("span", null, "反馈备注"), h("input", { value: feedbackNote, placeholder: "记录集群差异或复核依据", onChange: function (event) { setFeedbackNote(event.target.value); } })), h("button", { className: "secondary-button", disabled: busy, onClick: recordFeedback }, "记录反馈"))),
            h("section", { className: "rule-version-section" }, h("div", { className: "section-title" }, h("div", null, h("h3", null, "版本历史"), h("p", null, "回滚会追加新版本，不会覆盖历史快照")), h("span", null, (detail.versions || []).length + " 个版本")), h("div", { className: "rule-version-tree" }, (detail.versions || []).map(function (version) { return h("article", { key: version.version, className: version.version === selectedRule.current_version ? "current" : "" }, h("i", null), h("div", null, h("div", null, h("b", null, "v" + version.version), h("span", null, version.change_type)), h("p", null, version.change_reason || "无备注"), h("small", null, timeText(version.created_at) + " · " + version.operator)), h("button", { className: "text-button", disabled: busy || version.version === selectedRule.current_version, onClick: function () { rollbackVersion(version.version); } }, version.version === selectedRule.current_version ? "当前版本" : "回滚到此版本")); }))),
            h("section", { className: "rule-audit-section" }, h("div", { className: "section-title" }, h("div", null, h("h3", null, "最近审计"), h("p", null, "状态、反馈、审批更新与回滚事件"))), (detail.audit_events || []).slice(0, 6).map(function (event) { return h("div", { className: "rule-audit-row", key: event.event_id }, h("b", null, event.event_type), h("span", null, "v" + (event.from_version || "—") + " → v" + event.to_version), h("small", null, timeText(event.created_at) + " · " + event.operator)); }))))));
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
    const [loadedVersion, setLoadedVersion] = useState(null);
    const catalogSelected = items.find(function (item) { return item.config_id === selectedId; }) || items.find(function (item) { return catalog.active && item.config_id === catalog.active.config_id; }) || items[0] || null;
    const selected = loadedVersion && catalogSelected && loadedVersion.config_id === catalogSelected.config_id ? loadedVersion : catalogSelected;
    const baseline = items.find(function (item) { return item.config_id === "baseline"; }) || null;
    const candidates = items.filter(function (item) { return item.config_id !== "baseline"; });
    const editable = Boolean(selected && catalogSelected && selected.config_id !== "baseline" && Number(selected.version) === Number(catalogSelected.version));
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
    function selectConfig(configId) { setLoadedVersion(null); setSelectedId(configId); }
    function selectVersion(version) {
      if (!selected) return;
      props.onLoadVersion(selected.config_id, Number(version)).then(setLoadedVersion).catch(function () {});
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
          items.map(function (item) { return h("button", { className: "config-version-item " + (selected && selected.config_id === item.config_id ? "active" : ""), key: item.config_id, onClick: function () { selectConfig(item.config_id); } }, h("div", null, h("b", null, item.name + " v" + item.version), h("span", { className: "status-chip " + item.status }, item.status)), h("small", null, item.config_id), h("em", null, (item.masking_rules || []).length + " 条脱敏规则 · " + item.content_hash.slice(0, 10))); })),
        h("div", { className: "surface config-governance-detail" }, selected ? h(React.Fragment, null,
          h("div", { className: "surface-head" }, h("div", null, h("b", null, selected.name + " v" + selected.version), h("span", null, selected.config_id + " · " + selected.content_hash.slice(0, 16))), h("div", { className: "config-version-picker" }, h("select", { value: selected.version, onChange: function (event) { selectVersion(event.target.value); } }, (catalogSelected.available_versions || [catalogSelected.version]).map(function (version) { return h("option", { key: version, value: version }, "版本 v" + version); })), h("span", { className: "status-chip " + selected.status }, selected.status))),
          h("div", { className: "config-detail-tabs" }, [["structured", "结构化配置"], ["masking", "脱敏规则（" + draftRules.length + "）"], ["ini", "INI 原文"], ["diff", "版本差异"]].map(function (tab) { return h("button", { key: tab[0], className: detailTab === tab[0] ? "active" : "", onClick: function () { setDetailTab(tab[0]); } }, tab[1]); })),
          detailTab === "structured" && h("div", { className: "config-structured-editor" }, parameterFields.map(function (field) { const value = readIniValue(draft, field[0], field[1]); return h("label", { key: field[0] + field[1] }, h("span", null, field[2]), h("input", { value: value, disabled: !editable, onChange: function (event) { updateParameter(field[0], field[1], event.target.value); } }), h("small", null, field[0] + "." + field[1] + " · " + field[3])); })),
          detailTab === "masking" && h("div", { className: "masking-rule-table" }, h("div", { className: "masking-rule-row head" }, h("span", null, "占位符"), h("span", null, "正则表达式"), h("span", null, "操作")), draftRules.map(function (rule, index) { return h("div", { className: "masking-rule-row", key: index }, h("input", { value: rule.mask_with, disabled: !editable, onChange: function (event) { const next = draftRules.slice(); next[index] = Object.assign({}, rule, { mask_with: event.target.value }); updateRules(next); } }), h("input", { value: rule.regex_pattern, disabled: !editable, onChange: function (event) { const next = draftRules.slice(); next[index] = Object.assign({}, rule, { regex_pattern: event.target.value }); updateRules(next); } }), h("div", { className: "row-actions" }, h("button", { className: "text-button", disabled: !editable || index === 0, onClick: function () { const next = draftRules.slice(); const moved = next.splice(index, 1)[0]; next.splice(index - 1, 0, moved); updateRules(next); } }, "上移"), h("button", { className: "text-button danger-text", disabled: !editable, onClick: function () { updateRules(draftRules.filter(function (_, ruleIndex) { return ruleIndex !== index; })); } }, "移除"))); }), editable && h("button", { className: "secondary-button add-mask-rule", onClick: function () { updateRules(draftRules.concat([{ mask_with: "CUSTOM", regex_pattern: "" }])); } }, "＋ 新增脱敏规则")),
          detailTab === "ini" && h("textarea", { className: "config-ini-editor", value: draft, readOnly: !editable, spellCheck: false, onChange: function (event) { setDraft(event.target.value); } }),
          detailTab === "diff" && h("div", { className: "config-version-diff" }, diffRows.length === 0 ? h("div", { className: "empty-state" }, "与系统基线无差异") : diffRows.slice(0, 100).map(function (row) { return h("div", { className: "config-diff-row", key: row.index }, h("span", null, "L" + row.index), h("code", { className: "before" }, row.before || "∅"), h("code", { className: "after" }, row.after || "∅")); })),
          validation && h("div", { className: "config-validation " + (validation.valid ? "valid" : "invalid") }, validation.valid ? "配置校验通过：参数与 " + validation.masking_rules.length + " 条脱敏正则有效" : "配置校验失败"),
          h("div", { className: "config-governance-actions" }, selected.config_id === "baseline" ? h("button", { className: "primary-button", onClick: createCandidate }, "复制为候选") : h(React.Fragment, null, h("button", { className: "secondary-button", disabled: selected.version !== catalogSelected.version, onClick: save }, selected.version === catalogSelected.version ? "保存新版本" : "历史版本只读"), h("button", { className: "secondary-button", onClick: validate }, "配置校验"), h("select", { value: evalRunId, onChange: function (event) { setEvalRunId(event.target.value); } }, h("option", { value: "" }, matchingRuns.length ? "选择关联评测" : "暂无匹配评测"), matchingRuns.map(function (run) { return h("option", { key: run.run_id, value: run.run_id }, run.run_id + " · F1 " + ((run.metrics && run.metrics.labeled && run.metrics.labeled.pairwise_grouping_f1) || "—")); })), h("button", { className: "primary-button", disabled: !evalRunId, onClick: publish }, "人工发布"), h("button", { className: "text-button", onClick: rollback }, "回滚")))) : h("div", { className: "empty-state" }, "暂无 Drain3 配置"))));
  }

  function SemanticDictionaryGovernance(props) {
    const catalog = props.catalog || { items: [], active: {} };
    const items = catalog.items || [];
    const [selectedId, setSelectedId] = useState("");
    const [loaded, setLoaded] = useState(null);
    const [customRules, setCustomRules] = useState([]);
    const [validation, setValidation] = useState(null);
    const [testInput, setTestInput] = useState("NVRM: Xid 79, GPU has fallen off the bus");
    const [testComponent, setTestComponent] = useState("kernel");
    const [testResult, setTestResult] = useState(null);
    const catalogSelected = items.find(function (item) { return item.dictionary_id === selectedId; }) || items[0] || null;
    const selected = loaded && catalogSelected && loaded.dictionary_id === catalogSelected.dictionary_id ? loaded : catalogSelected;
    const editable = Boolean(selected && Number(selected.version) > 1 && Number(selected.version) === Number(catalogSelected.latest_version));
    useEffect(function () {
      if (!selected) return;
      setSelectedId(selected.dictionary_id);
      setCustomRules((selected.custom_rules || []).map(function (rule) { return Object.assign({}, rule); }));
      setValidation(null);
    }, [selected && selected.dictionary_id, selected && selected.version, selected && selected.content_hash]);
    useEffect(function () {
      if (!selected) return;
      const example = SEMANTIC_TEST_EXAMPLES[selected.dictionary_id];
      if (!example) return;
      setTestComponent(example.component);
      setTestInput(example.message);
      setTestResult(null);
    }, [selected && selected.dictionary_id]);
    function selectDictionary(id) { setLoaded(null); setSelectedId(id); }
    function selectVersion(version) { props.onLoadVersion(selected.dictionary_id, Number(version)).then(setLoaded).catch(function () {}); }
    function updateRule(index, key, value) {
      const next = customRules.slice();
      next[index] = Object.assign({}, next[index], { [key]: key === "priority" ? Number(value) : value });
      setCustomRules(next);
    }
    function addRule() {
      setCustomRules(customRules.concat([{
        rule_id: "custom-rule-" + (customRules.length + 1), field: "errno_name", pattern: "error_name=(?<value>[A-Z0-9_]+)", group: "value", value_type: "string", typed_mask: "ERRNO", tags: ["自定义语义"], priority: 50, source_types: [], components: [],
      }]));
    }
    function runTest() {
      props.onTest({ message_core: testInput, source_type: "unknown", component: testComponent }).then(setTestResult).catch(function () {});
    }
    return h("section", { className: "semantic-dictionary-governance" },
      h("div", { className: "semantic-summary-grid" },
        h("div", null, h("span", null, "独立词典"), h("b", null, items.length), h("small", null, "按领域独立发布")),
        h("div", null, h("span", null, "活动规则"), h("b", null, items.reduce(function (sum, item) { return sum + (item.rules || []).length; }, 0)), h("small", null, "内置 + 已发布扩展")),
        h("div", null, h("span", null, "候选版本"), h("b", null, items.filter(function (item) { return Number(item.latest_version) > Number(item.active_version); }).length), h("small", null, "校验后人工发布"))),
      h("div", { className: "semantic-governance-layout" },
        h("aside", { className: "surface semantic-dictionary-list" },
          h("div", { className: "surface-head" }, h("div", null, h("b", null, "语义词典"), h("span", null, "版本与活动状态"))),
          items.map(function (item) { return h("button", { key: item.dictionary_id, className: "semantic-dictionary-item " + (selected && selected.dictionary_id === item.dictionary_id ? "active" : ""), onClick: function () { selectDictionary(item.dictionary_id); } }, h("div", null, h("b", null, item.name), h("span", { className: "status-chip " + item.status }, item.status)), h("small", null, item.dictionary_id + " · v" + item.version), h("em", null, (item.rules || []).length + " 条规则 · " + shortHash(item.content_hash))); })),
        h("div", { className: "surface semantic-dictionary-detail" }, selected ? h(React.Fragment, null,
          h("div", { className: "surface-head" }, h("div", null, h("b", null, selected.name), h("span", null, selected.dictionary_id + " · " + shortHash(selected.content_hash))), h("div", { className: "semantic-version-actions" }, h("select", { value: selected.version, onChange: function (event) { selectVersion(event.target.value); } }, (catalogSelected.available_versions || [1]).map(function (version) { return h("option", { value: version, key: version }, "版本 v" + version); })), h("span", { className: "status-chip " + selected.status }, selected.status))),
          h("section", { className: "semantic-rule-section" }, h("div", { className: "section-title" }, h("div", null, h("h3", null, "内置规则（只读）"), h("p", null, "系统规则不可覆盖，只能通过自定义规则扩展。")), h("span", null, (selected.builtin_rules || []).length + " 条")), h("div", { className: "semantic-rule-table" }, h("div", { className: "semantic-rule-row head" }, h("span", null, "字段 / Typed Mask"), h("span", null, "匹配正则"), h("span", null, "范围")), (selected.builtin_rules || []).map(function (rule) { return h("div", { className: "semantic-rule-row", key: rule.rule_id }, h("div", null, h("b", null, rule.field), h("small", null, rule.rule_id + " · <" + rule.typed_mask + ">")), h("code", null, rule.pattern), h("span", null, (rule.components || []).join(", ") || "全部组件")); }))),
          h("section", { className: "semantic-rule-section custom" }, h("div", { className: "section-title" }, h("div", null, h("h3", null, "自定义扩展规则"), h("p", null, editable ? "当前候选可编辑，保存会追加新版本。" : "内置或历史版本只读，请先创建候选。")), h("span", null, customRules.length + " 条")), customRules.length === 0 && h("div", { className: "empty-state compact" }, "当前没有自定义规则"), customRules.map(function (rule, index) { return h("div", { className: "semantic-custom-rule", key: rule.rule_id + index }, h("input", { value: rule.rule_id, disabled: !editable, title: "规则 ID", onChange: function (event) { updateRule(index, "rule_id", event.target.value); } }), h("select", { value: rule.field, disabled: !editable, onChange: function (event) { updateRule(index, "field", event.target.value); } }, ["http_status", "errno", "errno_name", "exit_code", "signal", "xid_code", "k8s_reason"].map(function (field) { return h("option", { value: field, key: field }, field); })), h("input", { value: rule.typed_mask, disabled: !editable, title: "Typed Mask", onChange: function (event) { updateRule(index, "typed_mask", event.target.value); } }), h("input", { className: "rule-pattern-input", value: rule.pattern, disabled: !editable, title: "正则表达式", onChange: function (event) { updateRule(index, "pattern", event.target.value); } }), h("input", { type: "number", value: rule.priority, disabled: !editable, title: "优先级", onChange: function (event) { updateRule(index, "priority", event.target.value); } }), h("button", { className: "text-button danger-text", disabled: !editable, onClick: function () { setCustomRules(customRules.filter(function (_, ruleIndex) { return ruleIndex !== index; })); } }, "移除")); }), editable && h("button", { className: "secondary-button", onClick: addRule }, "＋ 新增规则")),
          h("section", { className: "semantic-test-bench" }, h("div", { className: "section-title" }, h("div", null, h("h3", null, "语义测试台"), h("p", null, "输入单条日志，检查结构化字段与 Typed Mask。"))), h("div", { className: "semantic-test-inputs" }, h("input", { value: testComponent, placeholder: "组件，例如 kernel", onChange: function (event) { setTestComponent(event.target.value); } }), h("textarea", { value: testInput, onChange: function (event) { setTestInput(event.target.value); } }), h("button", { className: "secondary-button", onClick: runTest }, "运行测试")), testResult && h("div", { className: "semantic-test-result" }, h("div", null, h("span", null, "字段"), h(CodeBlock, { value: testResult.semantic_fields })), h("div", null, h("span", null, "Typed Mask"), h("code", null, testResult.typed_message || "—")), h("div", null, h("span", null, "标签"), h("p", null, (testResult.semantic_tags || []).join(" · ") || "未命中")))),
          validation && h("div", { className: "config-validation " + (validation.valid ? "valid" : "invalid") }, validation.valid ? "配置校验通过：六类核心语义用例全部通过" : "配置校验失败：" + (validation.errors || []).join("；")),
          h("div", { className: "semantic-governance-actions" },
            h("span", null, "版本历史：" + (catalogSelected.available_versions || []).map(function (version) { return "v" + version; }).join(" → ")),
            Number(selected.version) === 1 && h("button", { className: "primary-button", onClick: function () { props.onCreate(selected.dictionary_id); } }, "创建候选"),
            editable && h("button", { className: "secondary-button", onClick: function () { props.onSave(selected.dictionary_id, selected.version, customRules); } }, "保存新版本"),
            Number(selected.version) > 1 && h("button", { className: "secondary-button", onClick: function () { props.onValidate(selected.dictionary_id, selected.version).then(setValidation).catch(function () {}); } }, "配置校验"),
            Number(selected.version) > 1 && h("button", { className: "primary-button", disabled: !(validation && validation.valid), onClick: function () { if (window.confirm("确认发布该词典版本给后续新任务？")) props.onPublish(selected.dictionary_id, selected.version); } }, "人工发布"),
            h("button", { className: "text-button", onClick: function () { const version = Number(window.prompt("回滚到哪个版本？", String(selected.active_version || 1))); if (version && window.confirm("确认回滚版本？")) props.onRollback(selected.dictionary_id, version); } }, "回滚版本"))) : h("div", { className: "empty-state" }, "暂无语义词典"))));
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
    const tabs = [["overview", "质量概览"], ["annotation", "标注工作台"], ["suspicious", "可疑模板"], ["compare", "配置对比"], ["templates", "模板管理"], ["configs", "Drain3 配置"], ["semantics", "语义词典"], ["release", "发布管理"]];
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
      tab === "configs" && h(DrainConfigGovernance, { configs: data.configs, evalRuns: data.evalRuns, onCreate: props.onConfigCreate, onLoadVersion: props.onConfigLoadVersion, onSave: props.onConfigSave, onValidate: props.onConfigValidate, onPublish: props.onConfigPublish, onRollback: props.onConfigRollback }),
      tab === "semantics" && h(SemanticDictionaryGovernance, { catalog: data.semantics, onCreate: props.onSemanticCreate, onLoadVersion: props.onSemanticLoadVersion, onSave: props.onSemanticSave, onValidate: props.onSemanticValidate, onPublish: props.onSemanticPublish, onRollback: props.onSemanticRollback, onTest: props.onSemanticTest }),
      tab === "release" && h("section", { className: "release-stage-grid" }, profiles.length === 0 && h("div", { className: "empty-state" }, "暂无待治理 Profile"), profiles.map(function (profile) { const promoted = profile.status === "promoted"; return h("article", { className: "surface release-card", key: profile.profile_id }, h("div", { className: "release-card-head" }, h("div", null, h("span", { className: "release-icon" }, promoted ? "✓" : "↑"), h("div", null, h("h3", null, profile.name), h("small", null, profile.profile_id))), h("span", { className: "status-chip " + profile.status }, profile.status)), h("div", { className: "release-flow" }, ["候选", "人工确认", "已发布"].map(function (stage, index) { return h("span", { className: index <= (promoted ? 2 : 0) ? "done" : "", key: stage }, stage); })), h("p", null, "发布仅记录旧版 Profile 决策；实际运行配置请在 Drain3 配置页评测并发布。"), h("div", { className: "button-row" }, h("button", { className: "primary-button", disabled: promoted, onClick: function () { if (window.confirm("确认发布 Profile " + profile.profile_id + "？")) props.onProfile(profile.profile_id, "promote"); } }, promoted ? "已发布" : "确认发布"), h("button", { className: "secondary-button", onClick: function () { if (window.confirm("确认回滚 Profile " + profile.profile_id + "？")) props.onProfile(profile.profile_id, "rollback"); } }, "回滚"))); })));
  }

  function NodeRiskPage(props) {
    const items = props.catalog.items || [], selected = props.selected && props.selected.snapshot;
    const critical = items.filter(function (item) { return item.overall_level === "critical"; }).length;
    const high = items.filter(function (item) { return item.overall_level === "high"; }).length;
    const active = items.filter(function (item) { return item.active_event_count > 0; }).length, events24h = items.reduce(function (sum, item) { return sum + Number(item.event_count_24h || 0); }, 0);
    return h("section", { className: "node-risk-page" },
      h("div", { className: "risk-page-hero" }, h("div", null, h("span", { className: "eyebrow" }, "NODE RISK INTELLIGENCE"), h("h2", null, "服务器风险总览"), h("p", null, "基于确定性语义规则计算节点当前风险；事件数与日志命中次数分别统计。")), h("button", { className: "secondary-button", onClick: props.onRefresh }, "刷新")),
      h("div", { className: "node-risk-metrics" }, [[items.length, "节点总数"], [critical, "Critical 节点"], [high, "High 节点"], [active, "存在未恢复风险"], [events24h, "24h 风险事件数"]].map(function (entry) { return h("div", { key: entry[1] }, h("b", null, entry[0]), h("span", null, entry[1])); })),
      h("div", { className: "node-risk-layout" },
        h("div", { className: "surface node-risk-table" }, h("div", { className: "node-risk-row head" }, h("span", null, "集群 / 节点"), h("span", null, "风险"), h("span", null, "事件统计"), h("span", null, "主要风险")), items.length === 0 && h("div", { className: "empty-state" }, "尚无节点风险事件。上传包含节点名的日志后，命中的风险语义会写入统一数据库。"), items.map(function (item) { return h("button", { className: "node-risk-row " + (props.selected && props.selected.node_id === item.node_id && props.selected.cluster === item.cluster ? "active" : ""), key: item.cluster + item.node_id, onClick: function () { props.onSelect(item); } }, h("span", null, h("b", null, item.node_id), h("small", null, item.cluster)), h("span", null, h("i", { className: "risk-level " + item.overall_level }, item.overall_level), h("b", null, item.overall_score)), h("span", null, h("b", null, item.event_count_24h + " 事件"), h("small", null, item.occurrence_count_24h + " 次日志命中 · " + item.active_event_count + " 未恢复")), h("span", null, (item.primary_risks || []).slice(0, 2).map(function (risk) { return h("code", { key: risk.risk_type }, risk.risk_type); }))); })),
        h("aside", { className: "surface node-risk-detail" }, !selected ? h("div", { className: "empty-state" }, "选择节点查看评分解释和事件时间线") : h(React.Fragment, null,
          h("div", { className: "surface-head" }, h("div", null, h("b", null, props.selected.node_id), h("span", null, props.selected.cluster)), h("span", { className: "risk-level " + selected.overall_level }, selected.overall_level + " · " + selected.overall_score)),
          h("div", { className: "risk-explanation" }, h("h3", null, "综合判断"), (selected.assessment_reasons || []).map(function (reason) { return h("p", { key: reason }, reason); }), h("div", { className: "score-bars" }, Object.keys(selected.score_breakdown && selected.score_breakdown.contributions || {}).map(function (key) { const value = selected.score_breakdown.contributions[key]; return h("div", { key: key }, h("span", null, key), h("i", null, h("em", { style: { width: Math.min(100, value * 3) + "%" } })), h("b", null, value)); }))),
          h("div", { className: "risk-event-list" }, h("h3", null, "最近风险事件"), (props.selected.events || []).map(function (event) { return h("article", { key: event.event_id }, h("div", null, h("b", null, event.risk_type), h("span", { className: "status-chip " + event.status }, event.status)), h("small", null, event.semantic_rule_id + " · occurrence_count " + event.occurrence_count), h("div", { className: "button-row" }, event.status === "active" && h("button", { className: "text-button", onClick: function () { props.onEvent(event.event_id, "acknowledge"); } }, "确认"), event.status !== "recovered" && h("button", { className: "text-button", onClick: function () { props.onEvent(event.event_id, "recover"); } }, "标记恢复"))); })))))
    );
  }

  function SemanticLibraryPage(props) {
    const items = props.catalog.items || [], selected = items.find(function (item) { return item.id === props.selectedId; }) || items[0];
    const [draft, setDraft] = useState(selected ? JSON.stringify(selected, null, 2) : "");
    const [sample, setSample] = useState("NVRM: Xid (0000:65:00): 79, GPU has fallen off the bus.");
    const [testResult, setTestResult] = useState(null);
    useEffect(function () { setDraft(selected ? JSON.stringify(selected, null, 2) : ""); const positive = selected && selected.test_samples && selected.test_samples.positive; if (positive && positive[0]) setSample(positive[0]); }, [selected && selected.id, selected && selected.version]);
    function parsed() { return JSON.parse(draft); }
    return h("section", { className: "semantic-library-page" },
      h("div", { className: "risk-page-hero" }, h("div", null, h("span", { className: "eyebrow" }, "EDITABLE RISK SEMANTICS"), h("h2", null, "风险语义库"), h("p", null, "结构模板保持稳定，语义规则负责识别 Xid、OOM、磁盘、Kubernetes 等风险含义。内置规则只读，可创建覆盖版本。")), h("button", { className: "secondary-button", onClick: props.onRefresh }, "热加载状态")),
      h("div", { className: "semantic-library-tabs" }, [["published", "Effective"], ["builtin", "Built-in"], ["user", "User Overrides"], ["draft", "Drafts"], ["disabled", "Disabled"]].map(function (entry) { const count = items.filter(function (item) { return entry[0] === "published" ? item.status === "published" : (entry[0] === "builtin" || entry[0] === "user" ? item.source === entry[0] : item.status === entry[0]); }).length; return h("span", { key: entry[0] }, h("b", null, count), entry[1]); })),
      h("div", { className: "semantic-library-layout" },
        h("div", { className: "surface semantic-risk-list" }, h("div", { className: "surface-head" }, h("div", null, h("b", null, "语义目录"), h("span", null, items.length + " 条规则"))), items.map(function (item) { return h("button", { key: item.id, className: "semantic-risk-item " + (selected && selected.id === item.id ? "active" : ""), onClick: function () { props.onSelect(item.id); } }, h("div", null, h("b", null, item.display_name), h("span", { className: "status-chip " + item.status }, item.status)), h("code", null, item.risk_type), h("small", null, item.source + " · v" + item.version + " · " + item.classification.default_severity)); })),
        h("div", { className: "surface semantic-risk-editor" }, !selected ? h("div", { className: "empty-state" }, "暂无风险语义") : h(React.Fragment, null,
          h("div", { className: "surface-head" }, h("div", null, h("b", null, selected.display_name), h("span", null, selected.id)), h("span", { className: "status-chip " + selected.status }, selected.source === "builtin" ? "系统默认 · 只读" : selected.status)),
          h("div", { className: "semantic-editor-grid" }, h("label", null, h("span", null, "规则 JSON · 字段、正则、分级与去重配置"), h("textarea", { value: draft, onChange: function (event) { setDraft(event.target.value); }, spellCheck: false })), h("div", { className: "semantic-test-card" }, h("h3", null, "正则测试台"), h("p", null, "测试只验证规则命中和字段提取，不调用 AI。"), h("textarea", { value: sample, onChange: function (event) { setSample(event.target.value); } }), h("button", { className: "secondary-button", onClick: function () { props.onTest({ rule: parsed(), positive_samples: [sample], negative_samples: [] }).then(setTestResult); } }, "测试样例"), testResult && h("pre", { className: "code-block" }, JSON.stringify(testResult, null, 2)), h("div", { className: "semantic-history" }, h("h3", null, "Version History"), (props.versions || []).map(function (version) { return h("span", { key: version.version }, "v" + version.version + " · " + version.changed_by); }), !(props.versions || []).length && h("small", null, "暂无历史版本")), h("div", { className: "semantic-history" }, h("h3", null, "Unclassified Candidates"), h("b", null, (props.unclassified || []).filter(function (item) { return item.status === "open"; }).length + " 条待补充")))),
          h("div", { className: "semantic-editor-actions" }, h("button", { className: "primary-button", onClick: function () { props.onSave(selected, parsed()); } }, selected.source === "builtin" ? "创建覆盖草稿" : "保存新版本"), selected.source !== "builtin" && selected.status === "draft" && h("button", { className: "secondary-button", onClick: function () { props.onPublish(selected); } }, "验证并发布"), h("span", null, "发布后新匹配立即使用；历史事件保留原语义版本。")))))
    );
  }

  function BenchmarkCenterPage(props) {
    const data = props.data || {};
    const suites = data.suites || [], runs = data.runs || [], leaderboard = data.leaderboard || [], trends = data.trends || [];
    const completedRuns = runs.filter(function (run) { return run.status === "completed"; });
    const [tab, setTab] = useState("overview"), [suiteId, setSuiteId] = useState(""), [mode, setMode] = useState("fake");
    const [caseLimit, setCaseLimit] = useState(1), [prompt, setPrompt] = useState(""), [profile, setProfile] = useState("");
    const [budget, setBudget] = useState(4000), [confirmed, setConfirmed] = useState(false), [baseline, setBaseline] = useState(""), [candidate, setCandidate] = useState("");
    const [working, setWorking] = useState(false), [gateResult, setGateResult] = useState(null);
    useEffect(function () {
      if (!suiteId && suites[0]) { setSuiteId(suites[0].suite_id); setCaseLimit(Math.max(1, suites[0].case_count || 1)); }
      if (!baseline && completedRuns[1]) setBaseline(completedRuns[1].run_id);
      if (!candidate && completedRuns[0]) setCandidate(completedRuns[0].run_id);
      if (!prompt && props.prompts && props.prompts[0]) setPrompt(props.prompts[0].prompt_id);
      if (!profile && props.profiles && props.profiles[0]) setProfile(props.profiles[0].profile_id);
    }, [suites.length, completedRuns.length, props.prompts && props.prompts.length, props.profiles && props.profiles.length]);
    const tabs = [["overview", "质量总览"], ["prompts", "Prompt 对比"], ["models", "模型排行榜"], ["failures", "失败 Case"], ["trends", "质量趋势"], ["gates", "发布门禁"]];
    const latest = data.overview && data.overview.latest_metrics || {};
    const assets = data.overview && data.overview.source_assets || {};
    const selectedProfile = (props.profiles || []).find(function (item) { return item.profile_id === profile; }) || {};
    const failures = data.selectedRun && data.selectedRun.cases && data.selectedRun.cases.items ? data.selectedRun.cases.items.filter(function (item) { return !item.passed; }) : [];
    function percent(value) { return Math.round(Number(value || 0) * 100) + "%"; }
    function diagnostic() {
      const value = JSON.stringify({ page: "benchmark-center", api_base: currentApiBase(), failure: data.failure || null }, null, 2);
      if (navigator.clipboard) navigator.clipboard.writeText(value);
    }
    async function createRun() {
      const suite = suites.find(function (item) { return item.suite_id === suiteId; }) || {};
      const payload = { suite_id: suiteId, mode: mode, case_limit: Math.min(Number(caseLimit), Number(suite.case_count || caseLimit)), prompt_id: prompt || null, model_profile_id: profile || null, timeout_seconds: 120, retry_count: 1, budget_units: Number(budget), confirmed: mode !== "real" || confirmed, idempotency_key: "ui-" + Date.now() };
      setWorking(true);
      try { await props.onCreateRun(payload); }
      finally { setWorking(false); }
    }
    async function evaluateGate() {
      setWorking(true);
      try { setGateResult(await props.onEvaluateGate({ baseline_run_id: baseline, candidate_run_id: candidate, thresholds: { min_pass_rate: 0.8, min_schema_valid_rate: 0.95, min_template_reference_accuracy: 0.95, max_pass_rate_drop: 0.05, max_latency_increase_percent: 20 }, operator: "local-reviewer" })); }
      finally { setWorking(false); }
    }
    if (data.loading) return h("div", { className: "benchmark-page" }, h("div", { className: "benchmark-state" }, h("b", null, "正在加载评测资产"), h("span", null, "统一读取 Eval、Trace、模型 Profile 与 Drain3 质量数据…")));
    if (data.failure) return h("div", { className: "benchmark-page" }, h("div", { className: "benchmark-state error" }, h("b", null, "Benchmark 数据加载失败"), h("span", null, data.failure), h("div", null, h("button", { className: "secondary-button", onClick: props.onRefresh }, "重试"), h("button", { className: "text-button", onClick: diagnostic }, "复制诊断信息"))));
    return h("div", { className: "benchmark-page" },
      h("section", { className: "benchmark-hero" }, h("div", null, h("span", { className: "eyebrow" }, "EVAL & BENCHMARK CENTER"), h("h2", null, "评测与基准"), h("p", null, "统一比较 Prompt、模型与版本质量；门禁只输出人工决策依据，不自动修改生产资产。")), h("button", { className: "secondary-button", onClick: props.onRefresh }, "刷新数据")),
      h("nav", { className: "benchmark-tabs" }, tabs.map(function (item) { return h("button", { key: item[0], className: tab === item[0] ? "active" : "", onClick: function () { setTab(item[0]); } }, item[1]); })),
      h("section", { className: "surface benchmark-launcher" }, h("div", { className: "surface-head" }, h("div", null, h("b", null, "新建 Benchmark Run"), h("span", null, "Fake/历史回放默认安全；真实模型必须锁定预算并人工确认")), h("span", { className: "status-chip active" }, working ? "执行中" : "待创建")),
        h("div", { className: "benchmark-form" },
          h("label", null, h("span", null, "Suite"), h("select", { value: suiteId, onChange: function (event) { const value = event.target.value; const suite = suites.find(function (item) { return item.suite_id === value; }) || {}; setSuiteId(value); setCaseLimit(Math.max(1, suite.case_count || 1)); } }, suites.map(function (suite) { return h("option", { value: suite.suite_id, key: suite.suite_id }, suite.name + " · " + suite.case_count + " cases"); }))),
          h("label", null, h("span", null, "运行模式"), h("select", { value: mode, onChange: function (event) { setMode(event.target.value); setConfirmed(false); } }, h("option", { value: "fake" }, "Fake Model"), h("option", { value: "history" }, "历史 Trace 回放"), h("option", { value: "real" }, "真实模型矩阵"))),
          h("label", null, h("span", null, "Case 数"), h("input", { type: "number", min: 1, value: caseLimit, onChange: function (event) { setCaseLimit(event.target.value); } })),
          h("label", null, h("span", null, "Prompt"), h("select", { value: prompt, onChange: function (event) { setPrompt(event.target.value); } }, h("option", { value: "" }, "未锁定"), (props.prompts || []).map(function (item) { return h("option", { value: item.prompt_id, key: item.prompt_id }, item.prompt_id); }))),
          h("label", null, h("span", null, "模型 Profile"), h("select", { value: profile, onChange: function (event) { setProfile(event.target.value); } }, h("option", { value: "" }, "未锁定"), (props.profiles || []).map(function (item) { return h("option", { value: item.profile_id, key: item.profile_id }, item.display_name || item.profile_id); }))),
          h("label", null, h("span", null, "运行连接"), h("div", { className: "benchmark-connection-state " + (selectedProfile.connection_ready ? "ready" : "unavailable") }, selectedProfile.connection_ready ? ((selectedProfile.provider || "Provider") + " · " + (selectedProfile.connection_id || "连接可用")) : "连接不可用，不能启动真实模型评测")),
          h("label", null, h("span", null, "预算单位"), h("input", { type: "number", min: 1, value: budget, onChange: function (event) { setBudget(event.target.value); } })),
          mode === "real" && h("label", { className: "benchmark-confirm" }, h("input", { type: "checkbox", checked: confirmed, onChange: function (event) { setConfirmed(event.target.checked); } }), h("span", null, "真实模型运行会产生调用成本，必须人工确认")),
          h("button", { className: "primary-button", disabled: working || !suiteId || (mode === "real" && (!confirmed || !prompt || !profile || !selectedProfile.connection_ready)), onClick: createRun }, working ? "创建中…" : "创建评测"))),
      !runs.length && h("div", { className: "benchmark-state" }, h("b", null, "暂无评测运行"), h("span", null, "先使用 Fake Model 建立第一条可比较基线。")),
      tab === "overview" && h(React.Fragment, null,
        h("section", { className: "benchmark-metrics" }, [[percent(latest.pass_rate), "通过率"], [percent(latest.schema_valid_rate), "Schema 有效率"], [percent(latest.template_reference_accuracy), "模板引用准确率"], [String(latest.latency_p95_ms || 0) + " ms", "P95 延迟"], [String(data.overview && data.overview.completed_run_count || 0), "已完成 Run"]].map(function (item) { return h("div", { key: item[1] }, h("b", null, item[0]), h("span", null, item[1])); })),
        h("section", { className: "surface benchmark-source-assets" }, h("div", { className: "surface-head" }, h("div", null, h("b", null, "统一资产来源"), h("span", null, "只统计脱敏评测资产，不读取或展示原始日志"))), h("div", null, [["AI Trace", assets.ai_traces], ["模型 Profile", assets.model_profiles], ["Prompt", assets.prompt_templates], ["Drain3 评测", assets.drain_eval_runs], ["Drain3 模板", assets.drain_templates], ["Canonical Case", assets.canonical_eval_cases]].map(function (item) { return h("span", { key: item[0] }, h("b", null, String(item[1] || 0)), h("small", null, item[0])); }))),
        h("section", { className: "surface benchmark-run-table" }, h("div", { className: "surface-head" }, h("b", null, "最近运行"), h("span", null, runs.length + " 条")), runs.slice(0, 8).map(function (run) { return h("button", { key: run.run_id, onClick: function () { props.onSelectRun(run.run_id); } }, h("span", null, h("b", null, run.run_id), h("small", null, run.mode + " · " + timeText(run.created_at))), h("span", { className: "status-chip " + run.status }, run.status), h("span", null, percent(run.metrics && run.metrics.pass_rate), h("small", null, "通过率")), h("span", null, run.progress.completed + "/" + run.progress.total)); }))),
      tab === "prompts" && h("section", { className: "surface benchmark-card-grid" }, completedRuns.map(function (run) { return h("article", { key: run.run_id }, h("span", { className: "eyebrow" }, "PROMPT SNAPSHOT"), h("h3", null, run.snapshot.prompt_id || "未锁定 Prompt"), h("p", null, (run.snapshot.model_profile_id || "未锁定模型") + " · " + run.run_id), h("b", null, percent(run.metrics.pass_rate)), h("small", null, "通过率 · Schema " + percent(run.metrics.schema_valid_rate))); })),
      tab === "models" && h("section", { className: "surface benchmark-leaderboard" }, h("div", { className: "surface-head" }, h("b", null, "模型排行榜"), h("span", null, "固定 Dataset 与 Prompt 后比较")), leaderboard.map(function (item, index) { return h("div", { key: item.run_id }, h("b", null, "#" + (index + 1)), h("span", null, h("strong", null, item.model_profile_id), h("small", null, item.prompt_id || "未锁定 Prompt")), h("span", null, percent(item.pass_rate)), h("span", null, item.latency_p95_ms + " ms")); })),
      tab === "failures" && h("section", { className: "surface benchmark-failures" }, h("div", { className: "surface-head" }, h("b", null, "失败 Case"), h("select", { value: data.selectedRun && data.selectedRun.run && data.selectedRun.run.run_id || "", onChange: function (event) { props.onSelectRun(event.target.value); } }, runs.map(function (run) { return h("option", { key: run.run_id, value: run.run_id }, run.run_id); }))), !failures.length && h("div", { className: "empty-state compact" }, "当前 Run 没有失败 Case"), failures.map(function (item) { return h("article", { key: item.case_id }, h("div", null, h("b", null, item.case_id), h("span", { className: "status-chip failed" }, item.error_type || "failed")), h("p", null, (item.result.errors || []).join("；") || "未记录失败详情"), h("small", null, "Schema " + (item.schema_valid ? "通过" : "失败") + " · Template Ref " + (item.template_reference_ok ? "通过" : "失败"))); })),
      tab === "trends" && h("section", { className: "surface benchmark-trends" }, h("div", { className: "surface-head" }, h("b", null, "质量趋势"), h("span", null, trends.length + " 个数据点")), trends.map(function (item) { const value = Number(item.metrics.pass_rate || 0); return h("div", { key: item.run_id }, h("span", null, timeText(item.created_at)), h("i", null, h("em", { style: { width: Math.round(value * 100) + "%" } })), h("b", null, percent(value)), h("small", null, item.snapshot.model_profile_id || item.run_id)); })),
      tab === "gates" && h("section", { className: "surface benchmark-gate" }, h("div", { className: "surface-head" }, h("div", null, h("b", null, "发布门禁"), h("span", null, "比较基线和候选；结果不会自动发布任何生产资产"))), h("div", { className: "gate-form" }, h("label", null, "基线 Run", h("select", { value: baseline, onChange: function (event) { setBaseline(event.target.value); } }, completedRuns.map(function (run) { return h("option", { value: run.run_id, key: run.run_id }, run.run_id); }))), h("span", null, "→"), h("label", null, "候选 Run", h("select", { value: candidate, onChange: function (event) { setCandidate(event.target.value); } }, completedRuns.map(function (run) { return h("option", { value: run.run_id, key: run.run_id }, run.run_id); }))), h("button", { className: "primary-button", disabled: working || !baseline || !candidate, onClick: evaluateGate }, "执行门禁")), gateResult && h("div", { className: "gate-result " + gateResult.decision }, h("b", null, gateResult.decision), h("p", null, (gateResult.reasons || []).join("；")), h("code", null, JSON.stringify(gateResult.deltas)))));
  }

  function App() {
    const [view, setView] = useState(pathToView(window.location.pathname)), [model, setModel] = useState("qwen3:1.7b"), [threshold, setThreshold] = useState(40), [promptId, setPromptId] = useState("feature_extract_v3_compact_strict_json_en"), [retryCount, setRetryCount] = useState(1);
    const [ollama, setOllama] = useState({ online: false }), [result, setResult] = useState(null), [fileName, setFileName] = useState("");
    const [snapshot, setSnapshot] = useState(null), [jobId, setJobId] = useState(null), [rules, setRules] = useState([]);
    const [ruleLoading, setRuleLoading] = useState(false), [ruleReviewQueue, setRuleReviewQueue] = useState({ items: [], total: 0 });
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
    const [drainQuality, setDrainQuality] = useState({ datasets: [], annotations: [], evalRuns: [], profiles: [], templates: [], configs: { items: [], active: null }, semantics: { items: [], active: {} } });
    const [nodeRiskCatalog, setNodeRiskCatalog] = useState({ items: [], total: 0 }), [selectedNodeRisk, setSelectedNodeRisk] = useState(null);
    const [riskSemanticCatalog, setRiskSemanticCatalog] = useState({ items: [] }), [selectedRiskSemanticId, setSelectedRiskSemanticId] = useState("");
    const [riskSemanticVersions, setRiskSemanticVersions] = useState([]), [unclassifiedRiskSemantics, setUnclassifiedRiskSemantics] = useState([]);
    const [benchmarkData, setBenchmarkData] = useState({ overview: {}, suites: [], runs: [], trends: [], leaderboard: [], selectedRun: null, loading: false, failure: "" });
    const [sidebarCollapsed, setSidebarCollapsed] = useState(false), [mobileMenuOpen, setMobileMenuOpen] = useState(false);
    const [narrowSidebar, setNarrowSidebar] = useState(function () { return window.matchMedia("(max-width: 900px)").matches; });
    const events = useRef(null);
    const selected = useMemo(function () { return snapshot && snapshot.features && snapshot.features.find(function (feature) { return feature.candidate_id === selectedId; }) || null; }, [snapshot, selectedId]);
    useEffect(function () {
      const features = snapshot && snapshot.features || [];
      setSelectedId(function (current) {
        if (current && features.some(function (feature) { return feature.candidate_id === current; })) return current;
        return features.length ? features[0].candidate_id : null;
      });
    }, [snapshot]);
    useEffect(function () {
      const media = window.matchMedia("(max-width: 900px)");
      function updateSidebarMode(event) { setNarrowSidebar(event.matches); setMobileMenuOpen(false); }
      media.addEventListener("change", updateSidebarMode);
      return function () { media.removeEventListener("change", updateSidebarMode); };
    }, []);
    function changeView(next) {
      setView(next);
      setMobileMenuOpen(false);
      history.pushState({}, "", routeForView(next));
      if (next === "drainQuality") loadDrainQuality().catch(function (reason) { setError(reason.message); });
      if (next === "rules") loadRules().catch(function (reason) { setError(reason.message); });
      if (next === "nodeRisks") loadNodeRisks().catch(function (reason) { setError(reason.message); });
      if (next === "semanticLibrary") loadRiskSemantics().catch(function (reason) { setError(reason.message); });
      if (next === "benchmarkCenter") loadBenchmark().catch(function () {});
    }
    function toggleSidebar() {
      if (narrowSidebar) setMobileMenuOpen(function (open) { return !open; });
      else setSidebarCollapsed(function (collapsed) { return !collapsed; });
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
      const values = await Promise.all([api.drainDatasets(), api.drainAnnotations(), api.drainEvalRuns(), api.drainProfiles(), api.drainTemplates(), api.drainConfigs(), api.semanticDictionaries()]);
      setDrainQuality({ datasets: values[0].items || [], annotations: values[1].items || [], annotationState: values[1].state || {}, evalRuns: values[2].items || [], profiles: values[3].items || [], templates: values[4].items || [], configs: values[5], semantics: values[6] });
    }
    async function loadNodeRisks() { const value = await api.nodeRisks("?page_size=100"); setNodeRiskCatalog(value); return value; }
    async function selectNodeRisk(item) { const detail = await api.nodeRisk(item.cluster, item.node_id); setSelectedNodeRisk(detail); history.replaceState({}, "", "/node-risks/" + encodeURIComponent(item.cluster) + "/" + encodeURIComponent(item.node_id)); }
    async function changeNodeEvent(eventId, action) { await (action === "acknowledge" ? api.acknowledgeNodeEvent : api.recoverNodeEvent)(eventId, { operator: "local-operator", reason: action === "acknowledge" ? "人工确认风险事件" : "人工确认风险已恢复" }); await loadNodeRisks(); if (selectedNodeRisk) await selectNodeRisk(selectedNodeRisk); }
    async function selectRiskSemantic(id) { setSelectedRiskSemanticId(id); const value = await api.riskSemanticVersions(id); setRiskSemanticVersions(value.items || []); }
    async function loadRiskSemantics() { const values = await Promise.all([api.riskSemantics(), api.unclassifiedRiskSemantics()]); const value = values[0]; setRiskSemanticCatalog(value); setUnclassifiedRiskSemantics(values[1].items || []); const target = selectedRiskSemanticId || value.items && value.items[0] && value.items[0].id; if (target) await selectRiskSemantic(target); return value; }
    async function loadBenchmark(selectedRunId) {
      setBenchmarkData(function (current) { return Object.assign({}, current, { loading: true, failure: "" }); });
      try {
        const values = await Promise.all([api.benchmarkOverview(), api.benchmarkSuites(), api.benchmarkRuns(), api.benchmarkTrends(), api.benchmarkLeaderboard()]);
        const runs = values[2].items || [];
        const target = selectedRunId || (benchmarkData.selectedRun && benchmarkData.selectedRun.run && benchmarkData.selectedRun.run.run_id) || (runs[0] && runs[0].run_id);
        const detail = target ? await api.benchmarkRun(target) : null;
        setBenchmarkData({ overview: values[0], suites: values[1].items || [], runs: runs, trends: values[3].items || [], leaderboard: values[4].items || [], selectedRun: detail, loading: false, failure: "" });
        return detail;
      } catch (reason) {
        setBenchmarkData(function (current) { return Object.assign({}, current, { loading: false, failure: reason.message || "未知错误" }); });
        throw reason;
      }
    }
    async function createBenchmarkRun(payload) { const created = await api.createBenchmarkRun(payload); await loadBenchmark(created.run_id); return created; }
    async function evaluateBenchmarkGate(payload) { const gate = await api.evaluateBenchmarkGate(payload); await loadBenchmark(payload.candidate_run_id); return gate; }
    async function saveRiskSemantic(selectedRule, content) { let target = selectedRule.id; if (selectedRule.source === "builtin") { const override = Object.assign({}, content, { id: "user." + selectedRule.id.replace(/^builtin\./, ""), source: "user", override_of: selectedRule.id, operator: "local-operator", reason: "创建内置语义覆盖" }); await api.createRiskSemantic(override); target = override.id; setSelectedRiskSemanticId(target); } else { await api.updateRiskSemantic(selectedRule.id, { changes: content, expected_version: selectedRule.version, operator: "local-operator", reason: "编辑风险语义" }); } await loadRiskSemantics(); await selectRiskSemantic(target); }
    async function publishRiskSemantic(rule) { await api.publishRiskSemantic(rule.id, { expected_version: rule.version, confirmed: true, operator: "local-operator", reason: "人工验证后发布" }); await loadRiskSemantics(); }
    async function loadRules() {
      setRuleLoading(true);
      try { const values = await Promise.all([api.governedRules("?page_size=100"), api.ruleReviewQueue()]); setRules(values[0].items || []); setRuleReviewQueue(values[1]); return values[0]; }
      finally { setRuleLoading(false); }
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
      Promise.all([api.config(), api.status(), Promise.all([api.governedRules("?page_size=100"), api.ruleReviewQueue()]), api.metrics(), api.harnessStatus(), api.prompts(), api.traces(query), api.modelProfiles()]).then(function (values) { setModel(values[0].default_model); setOllama(values[1]); setRules(values[2][0].items || []); setRuleReviewQueue(values[2][1]); setSystemMetrics(values[3]); setHarness(values[4]); setPrompts(values[5].items || []); setPromptId(values[5].current_prompt_id || values[4].current_prompt_id || "feature_extract_v3_compact_strict_json_en"); setTraces(values[6].items || []); setModelProfiles(values[7]); setModelProfileId(values[7].default_profile_id || ""); if (window.location.pathname === "/ai-observability") loadObservability().catch(function (reason) { setError(reason.message); }); if (window.location.pathname === "/drain-quality") loadDrainQuality().catch(function (reason) { setError(reason.message); }); if (window.location.pathname.startsWith("/node-risks")) loadNodeRisks().catch(function (reason) { setError(reason.message); }); if (window.location.pathname === "/semantic-library") loadRiskSemantics().catch(function (reason) { setError(reason.message); }); if (window.location.pathname === "/benchmark-center") loadBenchmark().catch(function () {}); }).catch(function (reason) { setError(reason.message); });
      function onPop() { const filters = traceFiltersFromSearch(window.location.search); setView(pathToView(window.location.pathname)); setTraceFilters(filters); if (window.location.pathname === "/ai-traces") loadHarness(traceFilterQuery(filters)).catch(function () {}); if (window.location.pathname === "/ai-observability") loadObservability().catch(function () {}); if (window.location.pathname === "/drain-quality") loadDrainQuality().catch(function () {}); if (window.location.pathname === "/rules") loadRules().catch(function () {}); if (window.location.pathname.startsWith("/node-risks")) loadNodeRisks().catch(function () {}); if (window.location.pathname === "/semantic-library") loadRiskSemantics().catch(function () {}); if (window.location.pathname === "/benchmark-center") loadBenchmark().catch(function () {}); }
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
    useEffect(function () {
      if (view !== "benchmarkCenter" || !(benchmarkData.runs || []).some(function (run) { return run.status === "pending" || run.status === "running"; })) return;
      const timer = setInterval(function () { if (!document.hidden) loadBenchmark().catch(function () {}); }, 1500);
      return function () { clearInterval(timer); };
    }, [view, (benchmarkData.runs || []).map(function (run) { return run.run_id + ":" + run.status; }).join("|")]);
    async function loadFile(file) { if (!file) return; setBusy(true); setError(""); setUploadProgress(null); setPreprocessProgress(null); try { const next = file.size > INLINE_MAX_BYTES ? await api.uploadAndAnalyzeLargeFile(file, { onUploadProgress: setUploadProgress, onPreprocessProgress: setPreprocessProgress }) : await api.analyzeFile(file); setResult(next); setFileName(file.name); setSnapshot(null); setJobId(null); changeView("overview"); } catch (reason) { setError(reason.message); } finally { setBusy(false); } }
    async function start() { if (!result) return; setBusy(true); setError(""); try { const created = await api.createJob(result, model, threshold, promptId, modelProfileId, retryCount); setJobId(created.job_id); changeView("queue"); await refresh(created.job_id); if (events.current) events.current.close(); events.current = api.subscribe(created.job_id, function () { refresh(created.job_id).catch(function (reason) { setError(reason.message); }); }); } catch (reason) { setError(reason.message); } finally { setBusy(false); } }
    async function save(changes) { try { await api.update(jobId, selectedId, changes); await refresh(); await loadRules(); } catch (reason) { setError(reason.message); } }
    function retry(entityId) { api.retry(jobId, entityId).then(function () { return refresh(); }).catch(function (reason) { setError(reason.message); }); }
    function openTrace(traceId) { api.trace(traceId).then(function (item) { setDrawer({ type: "trace", item: item }); applyTraceFilters({ job_id: "", trace_id: traceId, status: "", prompt_id: "" }); }).catch(function (reason) { setError(reason.message); }); }
    function openPrompt(id) { api.prompt(id).then(function (item) { setDrawer({ type: "prompt", item: item }); setView("prompts"); history.pushState({}, "", "/prompts?prompt_id=" + encodeURIComponent(id)); }).catch(function (reason) { setError(reason.message); }); }
    function savePrompt(id, content, note) { api.savePrompt(id, content, note).then(function (item) { setDrawer({ type: "prompt", item: item }); return loadHarness(); }).catch(function (reason) { setError(reason.message); }); }
    async function saveModelProfile(profile) { try { const saved = await api.saveModelProfile(profile); setModelProfileId(saved.profile_id); setModel(saved.model || model); setPromptId(saved.default_prompt_id || promptId); await loadHarness(); setError(""); return saved; } catch (reason) { setError(reason.message); throw reason; } }
    function openTraceList(query) { const filters = traceFiltersFromSearch(query || ""); applyTraceFilters(filters); }
    function openObservability(id) { setView("observability"); history.pushState({}, "", "/ai-observability" + (id ? "?job_id=" + encodeURIComponent(id) : "")); loadObservability(id).catch(function (reason) { setError(reason.message); }); }
    function openRule(ruleId) { setDrawer({ type: null, item: null }); setRuleFocus(ruleId); changeView("rules"); }
    async function importCurrentTemplates() {
      const templates = (result && result.top_templates || []).map(function (item) { return { template_hash: item.template_hash, template_fingerprint: item.template_fingerprint, template: item.template, component: item.component, count: item.count || 0, risk_levels: item.risk_levels || [], semantic_fields: item.semantic_fields || {}, semantic_tags: item.semantic_tags || [], typed_parameters: item.typed_parameters || [], semantic_dictionary_versions: item.semantic_dictionary_versions || {}, semantic_extractor_version: item.semantic_extractor_version || null }; }).filter(function (item) { return item.template_hash && item.template; });
      if (!templates.length) { setError("当前分析结果没有可导入的 Drain3 模板"); return; }
      try { await api.importDrainTemplates(templates); await loadDrainQuality(); } catch (reason) { setError(reason.message); }
    }
    async function annotateTemplate(payload) { try { await api.annotateDrainTemplate(payload); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function changeTemplate(templateHash, payload) { try { await api.changeDrainTemplate(templateHash, payload); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function rollbackTemplate(templateHash, payload) { try { await api.rollbackDrainTemplate(templateHash, payload); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function changeDrainProfile(profileId, action) { try { await api.promoteDrainProfile(profileId, action, { confirmed: true, reviewer: "local-operator" }); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function createDrainConfig(payload) { try { await api.createDrainConfig(payload); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function loadDrainConfigVersion(configId, version) { try { return await api.drainConfigVersion(configId, version); } catch (reason) { setError(reason.message); throw reason; } }
    async function saveDrainConfig(configId, payload) { try { await api.saveDrainConfig(configId, payload); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function validateDrainConfig(configId, payload) { try { return await api.validateDrainConfig(configId, payload); } catch (reason) { setError(reason.message); throw reason; } }
    async function publishDrainConfig(configId, payload) { try { await api.publishDrainConfig(configId, payload); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function rollbackDrainConfig(configId, payload) { try { await api.rollbackDrainConfig(configId, payload); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function createSemanticCandidate(dictionaryId) { try { await api.createSemanticCandidate(dictionaryId); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function loadSemanticDictionaryVersion(dictionaryId, version) { try { return await api.semanticDictionaryVersion(dictionaryId, version); } catch (reason) { setError(reason.message); throw reason; } }
    async function saveSemanticDictionary(dictionaryId, version, rules) { try { await api.saveSemanticDictionary(dictionaryId, version, rules); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function validateSemanticDictionary(dictionaryId, version) { try { return await api.validateSemanticDictionary(dictionaryId, version); } catch (reason) { setError(reason.message); throw reason; } }
    async function publishSemanticDictionary(dictionaryId, version) { try { await api.publishSemanticDictionary(dictionaryId, version); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function rollbackSemanticDictionary(dictionaryId, version) { try { await api.rollbackSemanticDictionary(dictionaryId, version); await loadDrainQuality(); } catch (reason) { setError(reason.message); } }
    async function testSemanticDictionary(payload) { try { return await api.testSemanticDictionary(payload); } catch (reason) { setError(reason.message); throw reason; } }
    const workspace = h(Workspace, { snapshot: snapshot, selectedId: selectedId, onSelect: setSelectedId, onRetry: retry, onOpenTraces: openTraceList, onOpenObservability: openObservability });
    const activePrompts = prompts.filter(function (prompt) { return prompt.analysis_type === "feature_extract" && prompt.status === "active"; });
    const activeProfiles = modelProfiles.profiles || [];
    const selectedModelProfile = activeProfiles.find(function (profile) { return profile.profile_id === modelProfileId; }) || {};
    const traceRule = drawer.item && rules.find(function (rule) { return rule.lineage && rule.lineage.trace_id === drawer.item.trace_id; });
    const drawerContent = !drawer.item ? null : (drawer.type === "prompt" ? h(PromptDrawer, { item: drawer.item, onSave: savePrompt, onOpenTrace: openTrace }) : h(TraceDrawer, { item: drawer.item, rule: traceRule, onOpenRule: openRule }));
    return h("div", { className: "app-shell" + (sidebarCollapsed ? " sidebar-collapsed" : "") + (mobileMenuOpen ? " mobile-menu-open" : "") },
      h("header", { className: "topbar" }, h("div", { className: "topbar-left" }, h("button", { className: "sidebar-toggle", type: "button", "aria-label": narrowSidebar ? (mobileMenuOpen ? "关闭菜单" : "打开菜单") : (sidebarCollapsed ? "展开菜单" : "折叠菜单"), "aria-controls": "primary-navigation", "aria-expanded": narrowSidebar ? mobileMenuOpen : !sidebarCollapsed, onClick: toggleSidebar }, h("i"), h("i"), h("i")), h("div", { className: "brand" }, h("i", null, "L"), h("div", null, h("b", null, "LOGRISK"), h("span", null, "FEATURE REVIEW")))), h("div", { className: "system-status" }, h("span", { className: ollama.online ? "online" : "offline" }, "● Ollama " + (ollama.online ? "在线" : "离线")), h("span", null, model), h("button", { className: "prompt-pill", onClick: function () { changeView("prompts"); } }, "Prompt " + (harness.current_prompt_id || promptId)), h("span", { className: harness.trace_enabled ? "trace-on" : "trace-off" }, "● Trace " + (harness.trace_enabled ? "ON" : "OFF")))),
      h(Sidebar, { active: view, onChange: changeView }),
      h("button", { className: "sidebar-overlay", type: "button", "aria-label": "关闭菜单", onClick: function () { setMobileMenuOpen(false); } }),
      h("main", null,
        !["drainQuality", "benchmarkCenter", "settings", "rules", "nodeRisks", "semanticLibrary"].includes(view) && h("div", { className: "page-head" }, h("div", null, h("h1", null, "日志特征工作台"), h("p", null, "上传日志、复用规则、识别未知特征并人工审批")), h("label", { className: "new-analysis" }, "＋ 新建分析", h("input", { type: "file", onChange: function (event) { loadFile(event.target.files && event.target.files[0]); } }))),
        error && h("div", { className: "error-banner" }, error, h("button", { onClick: function () { setError(""); } }, "×")),
        view === "overview" && h(React.Fragment, null,
          h("section", { className: "upload-panel" }, h("div", null, h("b", null, fileName || "选择 result.json、JSONL、TXT、LOG、GZ 或无后缀日志"), h("span", null, result ? (result.risk_entities || []).length + " 个风险实体，已完成本地预处理" : "10MB 以内直接分析；超过 10MB 自动分片上传，Linux messages / syslog 无后缀文件也支持上传")), busy && h("div", { className: "upload-progress" }, uploadProgress && h("span", null, "上传进度：" + Math.round((uploadProgress.progress || 0) * 100) + "%（" + uploadProgress.received_chunks + " / " + uploadProgress.total_chunks + " chunks）"), preprocessProgress && h("span", null, "预处理阶段：" + (preprocessProgress.stage || "queued") + "，记录 " + (preprocessProgress.records_parsed || 0) + (preprocessProgress.drain3_partitions_total ? "，Drain3 分片 " + (preprocessProgress.drain3_partitions_completed || 0) + " / " + preprocessProgress.drain3_partitions_total : ""))), h("div", { className: "analysis-config" }, h("label", null, "分析流程", h("select", { value: "feature_extract", disabled: true }, h("option", { value: "feature_extract" }, "日志特征识别"))), h("label", null, "模型 Profile", h("select", { value: modelProfileId, onChange: function (event) { const profile = activeProfiles.find(function (item) { return item.profile_id === event.target.value; }) || {}; setModelProfileId(event.target.value); if (profile.model) setModel(profile.model); if (profile.default_prompt_id) setPromptId(profile.default_prompt_id); } }, activeProfiles.map(function (profile) { return h("option", { value: profile.profile_id, key: profile.profile_id }, (profile.display_name || profile.profile_id) + " · " + profile.provider); }))), h("label", null, "Provider / 连接", h("input", { value: (selectedModelProfile.provider || "—") + " / " + (selectedModelProfile.connection_id || "—"), disabled: true })), h("label", null, "模型", h("input", { value: model, onChange: function (event) { setModel(event.target.value); } })), h("label", null, "Prompt", h("select", { value: promptId, onChange: function (event) { setPromptId(event.target.value); } }, activePrompts.map(function (prompt) { return h("option", { value: prompt.prompt_id, key: prompt.prompt_id }, prompt.prompt_id); }))), h("label", null, "重试次数", h("select", { value: retryCount, onChange: function (event) { setRetryCount(Number(event.target.value)); } }, [0, 1, 2, 3].map(function (count) { return h("option", { value: count, key: count }, count + " 次"); }))), h("label", null, "阈值", h("input", { type: "number", value: threshold, onChange: function (event) { setThreshold(event.target.value); } })), h("button", { className: "primary-button", disabled: !result || busy, onClick: start }, busy ? "处理中…" : "开始识别"))), h(MetricsGrid, { snapshot: snapshot, result: result, daily: systemMetrics }), h(LiveProcessing, { snapshot: snapshot, result: result })),
        view === "queue" && h(React.Fragment, null, h(MetricsGrid, { snapshot: snapshot, result: result, daily: systemMetrics }), h(LiveProcessing, { snapshot: snapshot, result: result }), workspace),
        view === "observability" && h(AIObservabilityPage, { summary: obsSummary, progress: obsProgress, events: obsEvents, onRefresh: function () { loadObservability(obsProgress && obsProgress.job_id).catch(function (reason) { setError(reason.message); }); }, onOpenTrace: openTrace, onReview: function () { changeView("review"); }, onRules: function () { changeView("rules"); }, onNewAnalysis: function () { changeView("overview"); } }),
        view === "traces" && h(AITracePage, { traces: traces, harness: harness, traceFilters: traceFilters, onFilter: applyTraceFilters, onOpenTrace: openTrace }),
        view === "prompts" && h(PromptManagement, { prompts: prompts, currentPrompt: harness.current_prompt_id || promptId, onOpenPrompt: openPrompt }),
        view === "modelProfiles" && h(ModelProfilesPage, { profiles: modelProfiles, selectedProfileId: modelProfileId, onSelect: function (profile) { setModelProfileId(profile.profile_id); setModel(profile.model || model); setPromptId(profile.default_prompt_id || promptId); }, onSave: saveModelProfile }),
        view === "review" && h("section", { className: "approval-workspace" },
          h(FeatureList, { features: snapshot && snapshot.features || [], selectedId: selectedId, onSelect: setSelectedId }),
          h(FeatureEvidence, { feature: selected, onSelectTemplate: setSelectedTemplate }),
          h(ReviewEditor, { feature: selected, selectedTemplate: selectedTemplate, onSave: save, onOpenTrace: openTrace })),
        view === "rules" && h(RuleLibrary, { rules: rules, reviewQueue: ruleReviewQueue, loading: ruleLoading, focusRuleId: ruleFocus, onOpenTrace: openTrace, onChanged: loadRules }),
        view === "nodeRisks" && h(NodeRiskPage, { catalog: nodeRiskCatalog, selected: selectedNodeRisk, onRefresh: loadNodeRisks, onSelect: function (item) { selectNodeRisk(item).catch(function (reason) { setError(reason.message); }); }, onEvent: function (eventId, action) { changeNodeEvent(eventId, action).catch(function (reason) { setError(reason.message); }); } }),
        view === "semanticLibrary" && h(SemanticLibraryPage, { catalog: riskSemanticCatalog, selectedId: selectedRiskSemanticId, versions: riskSemanticVersions, unclassified: unclassifiedRiskSemantics, onSelect: function (id) { selectRiskSemantic(id).catch(function (reason) { setError(reason.message); }); }, onRefresh: loadRiskSemantics, onSave: function (rule, content) { saveRiskSemantic(rule, content).catch(function (reason) { setError(reason.message); }); }, onPublish: function (rule) { publishRiskSemantic(rule).catch(function (reason) { setError(reason.message); }); }, onTest: api.testRiskSemantic }),
        view === "drainQuality" && h(DrainQualityPage, { data: drainQuality, onRefresh: loadDrainQuality, onImport: importCurrentTemplates, onAnnotate: annotateTemplate, onTemplateChange: changeTemplate, onTemplateRollback: rollbackTemplate, onProfile: changeDrainProfile, onConfigCreate: createDrainConfig, onConfigLoadVersion: loadDrainConfigVersion, onConfigSave: saveDrainConfig, onConfigValidate: validateDrainConfig, onConfigPublish: publishDrainConfig, onConfigRollback: rollbackDrainConfig, onSemanticCreate: createSemanticCandidate, onSemanticLoadVersion: loadSemanticDictionaryVersion, onSemanticSave: saveSemanticDictionary, onSemanticValidate: validateSemanticDictionary, onSemanticPublish: publishSemanticDictionary, onSemanticRollback: rollbackSemanticDictionary, onSemanticTest: testSemanticDictionary }),
        view === "benchmarkCenter" && h(BenchmarkCenterPage, { data: benchmarkData, prompts: activePrompts, profiles: activeProfiles, onRefresh: loadBenchmark, onCreateRun: createBenchmarkRun, onSelectRun: loadBenchmark, onEvaluateGate: evaluateBenchmarkGate }),
        view === "settings" && h(BackendSettings, { onSaved: function () { setError(""); } }),
        view === "export" && h("section", { className: "surface export-surface" }, h("h2", null, "导出记录"), h("p", null, "导出包只包含人工批准或历史规则复用的脱敏特征，不包含原始日志和 RCA 结论。"), h("button", { className: "primary-button", disabled: !jobId || !(snapshot && snapshot.features || []).some(function (feature) { return feature.status === "approved"; }), onClick: function () { api.exportApproved(jobId).catch(function (reason) { setError(reason.message); }); } }, "导出已批准特征 JSON"))),
      h(Drawer, { title: drawer.type === "prompt" ? "Prompt 详情" : "Trace 详情", subtitle: drawer.item && (drawer.item.prompt_id || drawer.item.trace_id), item: drawer.item, onClose: function () { setDrawer({ type: null, item: null }); } }, drawerContent));
  }

  ReactDOM.createRoot(document.getElementById("root")).render(h(React.StrictMode, null, h(App)));
}());
