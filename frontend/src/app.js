(function () {
  "use strict";
  const h = React.createElement;
  const { useEffect, useMemo, useRef, useState } = React;

  async function jsonRequest(path, options) {
    const response = await fetch(path, Object.assign({}, options, {
      headers: Object.assign({ "Content-Type": "application/json" }, (options && options.headers) || {}),
    }));
    const payload = await response.json().catch(function () { return {}; });
    if (!response.ok) throw new Error(payload.error || "请求失败 (" + response.status + ")");
    return payload;
  }

  const api = {
    config: function () { return jsonRequest("/api/config"); },
    status: function () { return jsonRequest("/api/ollama/status"); },
    rules: function () { return jsonRequest("/api/rules"); },
    metrics: function () { return jsonRequest("/api/metrics"); },
    job: function (id) { return jsonRequest("/api/jobs/" + id); },
    createJob: function (result, model, minScore) {
      return jsonRequest("/api/jobs", { method: "POST", body: JSON.stringify({ result: result, model: model, min_score: Number(minScore) }) });
    },
    retry: function (jobId, entityId) {
      return jsonRequest("/api/jobs/" + jobId + "/entities/" + encodeURIComponent(entityId) + "/retry", { method: "POST", body: "{}" });
    },
    update: function (jobId, candidateId, changes) {
      return jsonRequest("/api/jobs/" + jobId + "/features/" + candidateId, { method: "PATCH", body: JSON.stringify(changes) });
    },
    async analyzeFile(file) {
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
    subscribe: function (jobId, onEvent) {
      const source = new EventSource("/api/jobs/" + jobId + "/events?cursor=0");
      ["job_created", "job_started", "entity_rule_matched", "entity_started", "entity_completed", "entity_failed", "feature_updated", "job_completed"].forEach(function (type) {
        source.addEventListener(type, onEvent);
      });
      return source;
    },
    async exportApproved(jobId) {
      const response = await fetch("/api/jobs/" + jobId + "/export", { method: "POST", body: "{}" });
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
    ["overview", "▦", "特征总览"], ["queue", "◫", "识别队列"], ["review", "✓", "人工审批"],
    ["rules", "⌘", "批准规则库"], ["export", "⇩", "导出记录"],
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
        h("div", { className: "benefit-wrap" }, h("div", { className: "benefit-orb" }, h("b", null, "规则复用收益"), h("strong", null, reusePercent + "%"), h("span", null, "减少 Ollama 调用"), h("hr"), h("small", null, (live.saved_llm_calls || 0) + " 个实体命中规则", h("br"), "节省 " + (live.saved_llm_logs || 0) + " 条 LLM 关联日志")))));
  }

  function Workspace(props) {
    const entities = props.snapshot && props.snapshot.entities || [];
    const features = props.snapshot && props.snapshot.features || [];
    return h("section", { className: "workspace-grid" },
      h("div", { className: "surface queue-surface" }, h("div", { className: "surface-head" }, h("b", null, "风险实体与识别状态"), h("span", null, entities.length + " 个实体")),
        h("div", { className: "entity-list" }, entities.length === 0 && h("div", { className: "empty-state" }, "上传日志后显示识别队列"), entities.map(function (entity) {
          return h("div", { className: "entity-row", key: (entity.cluster || "") + "-" + entity.entity_id }, h("div", null, h("b", null, entity.entity_id), h("span", null, (entity.cluster || "default") + " · " + entity.log_count + " 条关联日志")), h("span", { className: "status-chip " + entity.status }, statusNames[entity.status] || entity.status), h("span", { className: "risk-score" }, Number(entity.risk_score || 0).toFixed(0)), entity.status === "rule_matched" && h("span", { className: "skip-llm" }, "跳过 LLM"), entity.status === "failed" && h("button", { className: "text-button", onClick: function () { props.onRetry(entity.entity_id); } }, "重试"));
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
            (feature.entity && feature.entity.id || "unknown") + " · " + feature.summary)),
          h("span", { className: "status-chip " + feature.status },
            feature.origin === "approved_rule" ? "规则复用" : feature.status));
        })
      )
    );
  }

  function FeatureEvidence(props) {
    const feature = props.feature;
    const templates = feature && feature.source_templates || [];
    return h("section", { className: "surface evidence-panel" },
      h("div", { className: "surface-head" }, h("b", null, "特征日志证据"), h("span", null, templates.length + " 个模板")),
      h("div", { className: "evidence-body" },
        h("div", { className: "evidence-notice" }, "当前展示 Drain3 脱敏特征模板，系统未保存原始日志"),
        !feature && h("div", { className: "empty-state" }, "选择特征后查看日志证据"),
        feature && templates.length === 0 && h("div", { className: "empty-state" }, "暂无脱敏模板证据"),
        templates.map(function (template, index) {
          const firstSeen = template.first_seen || feature.window_start || "—";
          const lastSeen = template.last_seen || feature.window_end || "—";
          return h("article", { className: "evidence-template", key: (template.template_hash || "template") + "-" + index },
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

  function ReviewEditor(props) {
    const [draft, setDraft] = useState(null);
    useEffect(function () {
      setDraft(props.feature ? { title: props.feature.title || "", summary: props.feature.summary || "", importance: props.feature.importance || "medium", tags: (props.feature.tags || []).join(", "), reviewer_note: props.feature.reviewer_note || "" } : null);
    }, [props.feature]);
    if (!props.feature || !draft) return h("section", { className: "surface review-editor empty-state" }, "选择一条特征进行人工审批");
    function field(label, key, type) {
      const controlProps = { value: draft[key], onChange: function (event) { setDraft(Object.assign({}, draft, { [key]: event.target.value })); } };
      return h("label", null, label, type === "textarea" ? h("textarea", controlProps) : h("input", controlProps));
    }
    function save(status) { props.onSave(Object.assign({}, draft, { tags: draft.tags.split(",").map(function (tag) { return tag.trim(); }).filter(Boolean), status: status })); }
    return h("section", { className: "surface review-editor" }, h("div", { className: "surface-head" }, h("b", null, "人工审批"), h("span", null, props.feature.origin === "approved_rule" ? "来自批准规则库" : "来自 Ollama")), h("div", { className: "editor-body" }, field("特征标题", "title"), field("特征摘要", "summary", "textarea"), field("标签（逗号分隔）", "tags"), field("审批备注", "reviewer_note", "textarea"), h("div", { className: "fact-box" }, "实体 " + (props.feature.entity && props.feature.entity.id || "") + " · 风险分 " + props.feature.risk_score + " · 出现 " + props.feature.occurrence_count + " 次"), h("div", { className: "editor-actions" }, h("button", { className: "reject-button", onClick: function () { save("rejected"); } }, "驳回"), h("button", { className: "primary-button", onClick: function () { save("approved"); } }, "批准并写入规则库"))));
  }

  function RuleLibrary(props) {
    return h("section", { className: "surface rules-surface" }, h("div", { className: "surface-head" }, h("b", null, "批准规则库"), h("span", null, "全局跨集群复用 · " + props.rules.length + " 条规则")), h("div", { className: "rule-table" }, h("div", { className: "rule-head" }, h("span", null, "规则"), h("span", null, "模板 / 类别"), h("span", null, "批准时间"), h("span", null, "复用")), props.rules.length === 0 && h("div", { className: "empty-state" }, "批准首条 Ollama 特征后建立规则库"), props.rules.map(function (rule) {
      return h("div", { className: "rule-row", key: rule.rule_id }, h("div", null, h("b", null, rule.title), h("span", null, rule.rule_id + " · " + rule.feature_type)), h("div", null, (rule.template_signatures || []).map(function (item) { return h("span", { className: "signature", key: item.template_hash + "-" + item.category }, item.template_hash.slice(0, 10) + " · " + (item.category || "未分类")); })), h("div", null, rule.approved_at ? new Date(rule.approved_at).toLocaleString() : "—"), h("div", null, h("b", null, (rule.reuse_count || 0) + " 次"), h("span", null, rule.last_reused_at ? "最近 " + new Date(rule.last_reused_at).toLocaleString() : "尚未复用")));
    })));
  }

  function App() {
    const [view, setView] = useState("overview"), [model, setModel] = useState("qwen3:1.7b"), [threshold, setThreshold] = useState(40);
    const [ollama, setOllama] = useState({ online: false }), [result, setResult] = useState(null), [fileName, setFileName] = useState("");
    const [snapshot, setSnapshot] = useState(null), [jobId, setJobId] = useState(null), [rules, setRules] = useState([]);
    const [systemMetrics, setSystemMetrics] = useState({ today_llm_logs: 0 });
    const [selectedId, setSelectedId] = useState(null), [busy, setBusy] = useState(false), [error, setError] = useState("");
    const events = useRef(null);
    const selected = useMemo(function () { return snapshot && snapshot.features && snapshot.features.find(function (feature) { return feature.candidate_id === selectedId; }) || null; }, [snapshot, selectedId]);
    useEffect(function () {
      const features = snapshot && snapshot.features || [];
      setSelectedId(function (current) {
        if (current && features.some(function (feature) { return feature.candidate_id === current; })) return current;
        return features.length ? features[0].candidate_id : null;
      });
    }, [snapshot]);
    async function refresh(id) { const next = await api.job(id || jobId); setSnapshot(next); if (["completed", "completed_with_errors"].includes(next.status) && events.current) events.current.close(); }
    useEffect(function () { Promise.all([api.config(), api.status(), api.rules(), api.metrics()]).then(function (values) { setModel(values[0].default_model); setOllama(values[1]); setRules(values[2].rules || []); setSystemMetrics(values[3]); }).catch(function (reason) { setError(reason.message); }); return function () { if (events.current) events.current.close(); }; }, []);
    async function loadFile(file) { if (!file) return; setBusy(true); setError(""); try { const next = await api.analyzeFile(file); setResult(next); setFileName(file.name); setSnapshot(null); setJobId(null); setView("overview"); } catch (reason) { setError(reason.message); } finally { setBusy(false); } }
    async function start() { if (!result) return; setBusy(true); setError(""); try { const created = await api.createJob(result, model, threshold); setJobId(created.job_id); setView("queue"); await refresh(created.job_id); if (events.current) events.current.close(); events.current = api.subscribe(created.job_id, function () { refresh(created.job_id).catch(function (reason) { setError(reason.message); }); }); } catch (reason) { setError(reason.message); } finally { setBusy(false); } }
    async function save(changes) { try { await api.update(jobId, selectedId, changes); await refresh(); setRules((await api.rules()).rules || []); } catch (reason) { setError(reason.message); } }
    function retry(entityId) { api.retry(jobId, entityId).then(function () { return refresh(); }).catch(function (reason) { setError(reason.message); }); }
    const workspace = h(Workspace, { snapshot: snapshot, selectedId: selectedId, onSelect: setSelectedId, onRetry: retry });
    return h("div", { className: "app-shell" },
      h("header", { className: "topbar" }, h("div", { className: "brand" }, h("i", null, "L"), h("div", null, h("b", null, "LOGRISK"), h("span", null, "FEATURE REVIEW"))), h("div", { className: "system-status" }, h("span", { className: ollama.online ? "online" : "offline" }, "● Ollama " + (ollama.online ? "在线" : "离线")), h("span", null, model))),
      h(Sidebar, { active: view, onChange: setView }),
      h("main", null,
        h("div", { className: "page-head" }, h("div", null, h("h1", null, "日志特征工作台"), h("p", null, "上传日志、复用规则、识别未知特征并人工审批")), h("label", { className: "new-analysis" }, "＋ 新建分析", h("input", { type: "file", accept: ".json,.jsonl,.txt,.log,application/json,text/plain", onChange: function (event) { loadFile(event.target.files && event.target.files[0]); } }))),
        error && h("div", { className: "error-banner" }, error, h("button", { onClick: function () { setError(""); } }, "×")),
        view === "overview" && h(React.Fragment, null,
          h("section", { className: "upload-panel" }, h("div", null, h("b", null, fileName || "选择 result.json、JSONL、TXT 或 LOG"), h("span", null, result ? (result.risk_entities || []).length + " 个风险实体，已完成本地预处理" : "纯文本会自动经过规范化、Drain3 和风险评分")), h("div", { className: "analysis-config" }, h("label", null, "模型", h("input", { value: model, onChange: function (event) { setModel(event.target.value); } })), h("label", null, "阈值", h("input", { type: "number", value: threshold, onChange: function (event) { setThreshold(event.target.value); } })), h("button", { className: "primary-button", disabled: !result || busy, onClick: start }, busy ? "处理中…" : "开始识别"))), h(MetricsGrid, { snapshot: snapshot, result: result, daily: systemMetrics }), h(LiveProcessing, { snapshot: snapshot, result: result })),
        view === "queue" && h(React.Fragment, null, h(MetricsGrid, { snapshot: snapshot, result: result, daily: systemMetrics }), h(LiveProcessing, { snapshot: snapshot, result: result }), workspace),
        view === "review" && h("section", { className: "approval-workspace" },
          h(FeatureList, { features: snapshot && snapshot.features || [], selectedId: selectedId, onSelect: setSelectedId }),
          h(FeatureEvidence, { feature: selected }),
          h(ReviewEditor, { feature: selected, onSave: save })),
        view === "rules" && h(RuleLibrary, { rules: rules }),
        view === "export" && h("section", { className: "surface export-surface" }, h("h2", null, "导出记录"), h("p", null, "导出包只包含人工批准或历史规则复用的脱敏特征，不包含原始日志和 RCA 结论。"), h("button", { className: "primary-button", disabled: !jobId || !(snapshot && snapshot.features || []).some(function (feature) { return feature.status === "approved"; }), onClick: function () { api.exportApproved(jobId).catch(function (reason) { setError(reason.message); }); } }, "导出已批准特征 JSON"))));
  }

  ReactDOM.createRoot(document.getElementById("root")).render(h(React.StrictMode, null, h(App)));
}());
