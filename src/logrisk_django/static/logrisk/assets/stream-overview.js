(function () {
  "use strict";
  const h = React.createElement;
  const { useEffect, useRef, useState } = React;
  const ROOT = "/api/stream-overview";
  const ROLE = { evidence_specialist: "证据专家", rule_specialist: "规则专家", feature_specialist: "特征专家" };
  const STATUS = { queued: "排队中", pending: "等待执行", running: "运行中", waiting: "等待中", completed: "已完成", failed: "失败", cancelled: "已取消", paused: "已暂停", awaiting_human: "等待人工", rejected: "已拒绝", skipped: "未执行", blocked: "受阻" };
  const MAIN = [
    { id: "source", name: "日志输入", hint: "登记来源与消费", destination: "streaming" },
    { id: "drain", name: "Drain3 模板化", hint: "归并日志模板", destination: "drainQuality" },
    { id: "risk", name: "语义与风险评分", hint: "形成风险事实", destination: "nodeRisks" },
    { id: "rules", name: "批准规则匹配", hint: "复用已批准规则", destination: "rules" },
  ];
  function validNumber(value) { return typeof value === "number" && Number.isFinite(value) && value >= 0; }
  function number(value) { return validNumber(value) ? new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 2 }).format(value) : "未知"; }
  function dateLabel(value) {
    const raw = String(value || "");
    const direct = /^(\d{4})-(\d{2})-(\d{2})$/.exec(raw);
    if (direct) return direct[2] + "/" + direct[3];
    const time = Date.parse(raw);
    return Number.isFinite(time) ? new Intl.DateTimeFormat("zh-CN", { timeZone: "Asia/Shanghai", month: "2-digit", day: "2-digit" }).format(time) : "时间未知";
  }
  function clockLabel(value) {
    const time = Date.parse(value || "");
    return Number.isFinite(time) ? new Intl.DateTimeFormat("zh-CN", { timeZone: "Asia/Shanghai", hour: "2-digit", minute: "2-digit", hour12: false }).format(time) : "时间未知";
  }
  function stamp(value) {
    const time = Date.parse(value || "");
    return Number.isFinite(time) ? new Intl.DateTimeFormat("zh-CN", { timeZone: "Asia/Shanghai", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }).format(time) : "未知";
  }
  function roleName(role) { return ROLE[role] || (role ? String(role) : "未记录角色"); }
  function stateName(value) { return STATUS[value] || (value ? String(value) : "未记录"); }
  function shortRun(value) {
    const id = String(value || "");
    return id.length > 18 ? id.slice(0, 8) + "…" + id.slice(-4) : id;
  }
  function nodeNames(ids, nodes) {
    return ids.map(function (id) {
      const node = nodes.find(function (item) { return item.id === id; });
      if (!node || !ROLE[node.role]) return String(id);
      const duplicates = nodes.filter(function (item) { return item.role === node.role; }).length > 1;
      return roleName(node.role) + (duplicates ? "（" + String(id) + "）" : "");
    }).join("、");
  }
  function terminalUnexecuted(node, run) {
    return !!(run && ["failed", "cancelled", "completed", "awaiting_human"].includes(run.status) && node.status === "pending");
  }
  function upstreamFailed(node, run) {
    return !!(terminalUnexecuted(node, run) && run.status === "failed" && Array.isArray(node.blocked_by) && Array.isArray(run.nodes) && run.nodes.some(function (item) { return node.blocked_by.includes(item.id) && item.status === "failed"; }));
  }
  function nodeStatus(node, run) {
    if (node.structure) return node.status;
    if (!terminalUnexecuted(node, run)) return stateName(node.status);
    if (!validNumber(node.attempts)) return "未继续 · 尝试次数未知";
    if (node.attempts > 0) return upstreamFailed(node, run) ? "已尝试 · 上游失败后未继续" : "已尝试 · 未继续";
    return upstreamFailed(node, run) ? "未执行 · 上游失败" : "未执行";
  }
  function terminalPendingDetail(node) {
    if (!validNumber(node.attempts)) return "本次专家运行已停止，此节点此前是否执行未知。";
    return node.attempts > 0 ? "本次专家运行已停止，此节点先前已尝试，未继续执行。" : "本次专家运行已停止，此节点未执行。";
  }
  function nodeTone(node, run) { return node.structure || terminalUnexecuted(node, run) ? "idle" : tone(node.status); }
  function tone(value) {
    if (["failed", "rejected", "blocked"].includes(value)) return "fault";
    if (["waiting", "awaiting_human", "queued", "pending", "paused"].includes(value)) return "wait";
    if (value === "completed") return "done";
    if (value === "running") return "running";
    return "idle";
  }
  function blockData(block) { return block && block.data && typeof block.data === "object" && !Array.isArray(block.data) ? block.data : {}; }
  function blockState(block, loading) {
    if (loading) return "读取中";
    if (!block || block.status === "unavailable") return "数据不可用";
    if (block.status === "partial") return "部分数据";
    return block.status === "available" ? "" : "数据未知";
  }
  function Notice(props) {
    const state = blockState(props.block, props.loading);
    return state ? h("p", { className: "ov-availability", role: "status" }, state, props.block && props.block.reason ? " · " + String(props.block.reason) : "") : null;
  }
  function LineChart(props) {
    const rows = Array.isArray(props.rows) ? props.rows : [];
    const series = props.series;
    const valid = rows.some(function (row) { return series.some(function (line) { return validNumber(row[line.key]); }); });
    if (!valid) return h("div", { className: "ov-chart-empty" }, props.empty || "暂无历史记录");
    let max = 0;
    rows.forEach(function (row) { series.forEach(function (line) { if (validNumber(row[line.key])) max = Math.max(max, row[line.key]); }); });
    const ceiling = Math.max(1, max);
    const times = rows.map(function (row) { return Date.parse(String(row[props.dateKey] || "")); }).filter(Number.isFinite);
    if (!times.length) return h("div", { className: "ov-chart-empty" }, "历史时间未知");
    const first = Math.min(...times), lastTime = Math.max(...times);
    const x = function (row) { const at = Date.parse(String(row[props.dateKey] || "")); return first === lastTime ? 150 : 5 + 290 * (at - first) / (lastTime - first); };
    const y = function (value) { return 66 - value / ceiling * 55; };
    const lines = series.map(function (line) {
      let path = "", last = null;
      const dots = [];
      rows.forEach(function (row, index) {
        const value = row[line.key], at = Date.parse(String(row[props.dateKey] || ""));
        if (!validNumber(value) || !Number.isFinite(at)) { last = null; return; }
        const sameStep = last && (!props.daily || Number.isFinite(at) && Number.isFinite(last.at) && at - last.at === 86400000);
        path += (sameStep ? " L " : " M ") + x(row).toFixed(2) + " " + y(value).toFixed(2);
        dots.push(h("circle", { key: line.key + index, cx: x(row), cy: y(value), r: 2.7, className: "ov-line-dot " + line.key }, h("title", null, (row[props.dateKey] || "时间未知") + " · " + line.label + " " + number(value))));
        last = { at: at };
      });
      return h(React.Fragment, { key: line.key }, h("path", { className: "ov-line " + line.key, d: path }), dots);
    });
    return h("figure", { className: "ov-line-chart" + (props.plain ? " ov-plain-plot" : "") }, h("svg", { viewBox: "0 0 300 72", preserveAspectRatio: "none", role: "img", "aria-label": props.label },
      h("line", { className: "ov-chart-grid", x1: 4, x2: 296, y1: 12, y2: 12 }), h("line", { className: "ov-chart-grid", x1: 4, x2: 296, y1: 39, y2: 39 }), h("line", { className: "ov-chart-grid", x1: 4, x2: 296, y1: 66, y2: 66 }), lines),
      h("div", { className: "ov-chart-labels" }, h("span", null, (props.labelFor || dateLabel)(new Date(first).toISOString())), h("span", null, first !== lastTime ? (props.labelFor || dateLabel)(new Date(lastTime).toISOString()) : "")),
      h("figcaption", null, "仅显示已有记录；缺失处不连线。"));
  }
  function BarChart(props) {
    const rows = Array.isArray(props.rows) ? props.rows.filter(function (row) { return row && typeof row === "object"; }) : [];
    const values = rows.map(function (row) { return props.value(row); });
    if (!values.some(validNumber)) return h("div", { className: "ov-chart-empty" }, props.empty || "暂无历史记录");
    const max = Math.max(1, ...values.filter(validNumber));
    return h("div", { className: "ov-bars", role: "img", "aria-label": props.label }, rows.map(function (row, index) {
      const value = values[index];
      return h("div", { className: "ov-bar-column" + (!validNumber(value) ? " unknown" : ""), key: index, title: props.labelFor(row) + " · " + (validNumber(value) ? number(value) : "未知") },
        h("span", { className: "ov-bar-value" }, validNumber(value) ? number(value) : "未知"),
        h("span", { className: "ov-bar-track" }, validNumber(value) && h("i", { style: { height: Math.max(value > 0 ? 2 : 0, value / max * 100) + "%" } })),
        h("small", null, props.labelFor(row)));
    }));
  }
  function TokenChart(props) {
    const rows = Array.isArray(props.rows) ? props.rows : [];
    if (!rows.some(function (row) { return validNumber(row.input_tokens) || validNumber(row.output_tokens) || validNumber(row.started_calls); })) return h("div", { className: "ov-chart-empty" }, "暂无分时用量回执");
    const max = Math.max(1, ...rows.map(function (row) { return (validNumber(row.input_tokens) ? row.input_tokens : 0) + (validNumber(row.output_tokens) ? row.output_tokens : 0); }));
    return h("div", { className: "ov-token-bars", role: "img", "aria-label": "今日已入账物理调用的分时输入与输出 Token；缺失回执显示未知" }, rows.map(function (row, index) {
      const input = validNumber(row.input_tokens) ? row.input_tokens : null, output = validNumber(row.output_tokens) ? row.output_tokens : null;
      const label = clockLabel(row.start_at) + "–" + clockLabel(row.end_at);
      let note = "";
      if (row.started_calls === 0 && input == null && output == null) note = "无调用";
      else if (validNumber(row.started_calls) && row.started_calls > 0 && validNumber(row.receipted_calls) && row.receipted_calls < row.started_calls) note = "缺少回执";
      else if (input == null && output == null) note = "输入/输出未知";
      else if (input == null || output == null) note = "部分未知";
      return h("div", { className: "ov-token-column", key: index, title: label + " · 输入 " + number(input) + " / 输出 " + number(output) + (note ? " · " + note : "") },
        h("span", { className: "ov-token-stack" }, input != null && h("i", { style: { height: (input / max * 100) + "%" } }), output != null && h("u", { style: { height: (output / max * 100) + "%" } })),
        h("small", null, label), note && h("small", { className: note === "无调用" ? "ov-empty-bin" : "ov-missing" }, note));
    }));
  }
  function RiskCard(props) {
    const data = blockData(props.block), today = data.today || {}, recovery = today.recovery_groups || {}, hosts = Array.isArray(data.hosts) ? data.hosts : [];
    const [selectedHost, setSelectedHost] = useState("");
    const selected = hosts.find(function (host) { return JSON.stringify([host.cluster, host.node]) === selectedHost; });
    return h("section", { className: "ov-risk", "aria-labelledby": "ov-risk-title" },
      h("div", { className: "ov-topline" }, h("h2", { id: "ov-risk-title" }, "服务器风险"), h("span", { className: "ov-range" }, "今天 · 全工作台")),
      h(Notice, { block: props.block, loading: props.loading }),
      h("div", { className: "ov-risk-body" }, h("div", { className: "ov-hero-value" }, h("strong", null, number(today.hosts)), h("span", null, validNumber(today.hosts) ? "台" : ""), h("div", null, "今日观测风险主机")),
        h("div", { className: "ov-risk-trend" }, h("div", { className: "ov-plot-head" }, h("b", null, "近 7 天风险主机"), h("span", null, "— 全部　", h("i", null, "┄ 高风险"))), h(LineChart, { rows: data.trend, dateKey: "date", daily: true, series: [{ key: "hosts", label: "全部" }, { key: "high_hosts", label: "高风险" }], label: "按日去重风险主机及高风险主机" }))),
      h("div", { className: "ov-risk-stats" }, [[today.high_hosts, "台", "今日最高为高风险"], [recovery.unrecovered, "组", "当前未恢复"], [recovery.unknown, "组", "恢复状态未知"]].map(function (stat) { return h("div", { key: stat[2] }, h("b", null, number(stat[0]), " ", h("em", null, validNumber(stat[0]) ? stat[1] : "")), h("span", null, stat[2])); })),
      h("details", { className: "ov-risk-details" }, h("summary", null, h("span", null, "定位风险主机"), h("small", null, number(today.groups), " 组异常 · 已知至少 ", number(today.known_occurrences), " 次 ▾")),
        h("p", null, "今日按集群和主机去重。已恢复 " + number(recovery.recovered) + " 组；次数未确证 " + number(today.unknown_groups) + " 组。恢复状态未知保留为未知。"),
        hosts.length ? h("div", { className: "ov-host-list" }, hosts.map(function (host) { const key = JSON.stringify([host.cluster, host.node]); return h("button", { type: "button", key: key, onClick: function () { setSelectedHost(key); }, "aria-pressed": selectedHost === key }, h("span", null, String(host.node || "主机未知")), h("small", null, String(host.cluster || "集群未知"), " · ", String(host.level || "风险等级未知"), " · ", number(host.groups), " 组")); })) : h("p", null, validNumber(today.hosts) && today.hosts === 0 ? "今日没有风险主机记录。" : "暂无可展示的主机记录。"),
        selected && h("p", { className: "ov-host-fact", "aria-live": "polite" }, String(selected.cluster || "集群未知") + " / " + String(selected.node || "主机未知") + " · " + number(selected.groups) + " 组 · 已知至少 " + number(selected.known_occurrences) + " 次 · 最近 " + stamp(selected.last_seen)),
        h("button", { className: "ov-text-link", type: "button", onClick: function () { props.onNavigate("nodeRisks"); } }, "查看服务器风险详情 ↗")));
  }
  function UsageCard(props) {
    const data = blockData(props.block), models = Array.isArray(data.models) ? data.models : [];
    const exact = validNumber(data.total_tokens), known = validNumber(data.known_tokens);
    const displayed = exact ? data.total_tokens : known && (data.receipted_calls > 0 || data.started_calls === 0) ? data.known_tokens : null;
    return h("section", { className: "ov-token", "aria-labelledby": "ov-token-title" }, h("div", { className: "ov-topline" }, h("h2", { id: "ov-token-title" }, "模型 Token 用量"), h("span", { className: "ov-range" }, exact ? "今日已入账" : "今日已知")),
      h(Notice, { block: props.block, loading: props.loading }),
      h("div", { className: "ov-token-value" }, !exact && displayed != null && h("span", null, "至少"), h("strong", null, number(displayed)), h("span", null, displayed != null ? "Token" : "")),
      h("div", { className: "ov-token-split" }, h("span", null, h("i", { className: "input" }), "输入 ", h("b", null, number(data.input_tokens))), h("span", null, h("i", { className: "output" }), "输出 ", h("b", null, number(data.output_tokens)))),
      h(TokenChart, { rows: data.buckets }),
      h("p", { className: "ov-usage-note" }, "仅计已登记的物理调用（含特征识别与专家规划）；旧 Trace 未纳入。"),
      h("div", { className: "ov-coverage" }, h("span", null, "已登记物理调用回执覆盖"), h("b", null, number(data.receipted_calls), " / ", number(data.started_calls), " ", h("small", null, "次调用"))),
      h("details", { className: "ov-token-details" }, h("summary", null, h("span", null, "已入账模型贡献与计价"), h("small", null, (Array.isArray(data.models) ? number(models.length) : "未知"), " 个模型 ▾")),
        models.length ? models.map(function (model, index) { return h("div", { className: "ov-model-row", key: index }, h("span", null, String(model.model || "模型未知"), h("small", null, " · " + String(model.provider || "Provider 未知"))), h("b", null, number(model.known_tokens), " 已知")); }) : h("p", null, Array.isArray(data.models) ? "暂无模型用量记录。" : "模型用量记录不可用。"),
        h("p", null, "已登记物理调用的完整回执 " + number(data.complete_calls) + " / " + number(data.started_calls) + " 次；缺失回执时已入账总量保留未知，失败及重试按实际记录计入。"),
        h("div", { className: "ov-invoice" }, h("b", null, "估算费用"), h("span", null, validNumber(data.estimated_cost_usd) ? "$" + number(data.estimated_cost_usd) : "不可计算")),
        h("button", { className: "ov-text-link", type: "button", onClick: function () { props.onNavigate("observability"); } }, "查看模型运行观测 ↗")));
  }
  function nodeFact(node, run) {
    if (node.structure) return { name: roleName(node.role), status: node.status, dependency: node.role === "feature_specialist" ? "示意：证据专家与规则专家的输出。" : "示意：聚合脱敏证据；无上游专家。", impact: "此处仅说明结构；实际运行和输出以已登记记录为准。", evidence: "未取得可归因的运行记录。", tone: "idle", destination: "agentWorkflows" };
    const deps = Array.isArray(node.depends_on) ? node.depends_on : null, blocked = Array.isArray(node.blocked_by) ? node.blocked_by : null;
    const nodes = run && Array.isArray(run.nodes) ? run.nodes : [];
    const dependency = deps ? (deps.length ? nodeNames(deps, nodes) : "无上游节点") : "依赖未记录";
    let impact = "查看运行详情确认节点输出和后续状态。";
    if (blocked && blocked.length) impact = (terminalUnexecuted(node, run) ? terminalPendingDetail(node) : "") + "记录的阻塞节点：" + nodeNames(blocked, nodes) + "。";
    else if (node.status === "failed") impact = "节点记录为失败；后续执行以各节点记录为准。";
    else if (terminalUnexecuted(node, run)) impact = terminalPendingDetail(node);
    else if (node.status === "pending") impact = "节点尚在等待；后续执行以依赖状态为准。";
    if (Array.isArray(node.downstream) && node.downstream.length) impact += " 直接下游：" + nodeNames(node.downstream, nodes) + "。";
    const evidence = ["尝试 " + number(node.attempts) + " 次", "开始 " + stamp(node.started_at), "结束 " + stamp(node.finished_at)];
    if (validNumber(node.used_tool_calls)) evidence.push("工具调用 " + number(node.used_tool_calls) + " 次");
    if (node.result_code) evidence.push("结果代码 " + String(node.result_code));
    if (run && run.run_id) evidence.push("运行 ID " + String(run.run_id));
    if (node.id) evidence.push("节点 ID " + String(node.id));
    if (run && validNumber(run.candidate_count)) evidence.push("此运行登记候选 " + number(run.candidate_count) + " 个");
    return { name: roleName(node.role), status: nodeStatus(node, run), dependency: dependency, impact: impact, evidence: evidence.join(" · "), tone: nodeTone(node, run), destination: "agentWorkflows" };
  }
  function hasVolume(data) {
    return validNumber(data.today_records) || validNumber(data.total_records) || Array.isArray(data.seven_day_records) && data.seven_day_records.some(function (row) { return row && validNumber(row.records); });
  }
  function volumeSets(activity) {
    const batch = activity.committed_batch_records && typeof activity.committed_batch_records === "object" ? activity.committed_batch_records : {};
    const ledger = { today_records: activity.today_records, total_records: activity.total_records, seven_day_records: activity.seven_day_records };
    const verifiedPrimary = !hasVolume(batch) && hasVolume(ledger);
    return { primary: verifiedPrimary ? ledger : batch, secondary: verifiedPrimary ? batch : ledger, source: verifiedPrimary ? "verified" : "batch" };
  }
  function mainFact(id, risk, activity) {
    const today = risk.today || {}, drain = activity.drain || {}, volume = volumeSets(activity);
    const volumeEvidence = volume.source === "batch" ? "今日已提交流批次处理 " + number(volume.primary.today_records) + " 条（未跨任务去重）。" : "今日已核验去重收据 " + number(volume.primary.today_records) + " 条。";
    const facts = {
      source: ["登记来源与消费进展", "已登记来源与对应输入；所选 Kafka 来源的状态见下方。", "日志经输入后进入模板化流程。", volumeEvidence + " 流来源状态见 Kafka 运行观测。", "streaming"],
      drain: ["Drain3 模板化", "输入日志与已登记的 Drain3 配置。", "归并模板供语义评分和特征识别使用。", "今日模板窗口 " + number(drain.template_windows) + "；待识别模板 " + number(drain.unknown_templates) + "。", "drainQuality"],
      risk: ["语义与风险评分", "模板与确定性语义、风险规则。", "产生服务器风险事实，可通过风险页面查看。", "今日 " + number(today.hosts) + " 台风险主机、" + number(today.groups) + " 组异常。", "nodeRisks"],
      rules: ["批准规则匹配", "模板证据与已批准规则版本。", "命中时可复用规则；未命中可按配置进入识别流程。", "累计批准规则 " + number(activity.approved_rules) + " 条。", "rules"]
    };
    const fact = facts[id] || facts.risk;
    return { name: fact[0], status: "流程说明", dependency: fact[1], impact: fact[2], evidence: fact[3], tone: "idle", destination: fact[4] };
  }
  function GateFact(kind, run) {
    const gate = run && ((run.gates && run.gates[kind]) || run[kind]);
    if (kind === "evaluator") {
      const recorded = gate && (validNumber(gate.passed) || validNumber(gate.failed));
      const state = !recorded ? "独立回执未记录" : gate.failed > 0 ? "记录有校验失败" : gate.passed > 0 ? "记录有校验通过" : "无校验结果";
      return { name: "确定性校验", status: state, dependency: "特征输出与锁定的允许引用集合。", impact: "校验通过后才可登记候选；按回执区分通过与失败。", evidence: recorded ? "通过 " + number(gate.passed) + " · 失败 " + number(gate.failed) + " · 登记候选 " + number(run.candidate_count) : "独立校验回执未记录；登记候选 " + (run ? number(run.candidate_count) : "未知") + "。", tone: recorded && gate.failed > 0 ? "fault" : recorded && gate.passed > 0 ? "done" : "idle", destination: "agentWorkflows" };
    }
    const recorded = gate && [gate.pending, gate.approved, gate.rejected, gate.resolved].some(validNumber);
    const waiting = recorded && gate.pending > 0;
    return { name: "人工审批", status: waiting ? "有候选等待人工" : run && run.status === "awaiting_human" ? "运行等待人工" : recorded ? "已有审批记录" : "独立回执未记录", dependency: "经确定性校验并登记的候选。", impact: "只有人工批准后才能成为批准资产。", evidence: recorded ? "待审批 " + number(gate.pending) + " · 已批准 " + number(gate.approved) + " · 已拒绝 " + number(gate.rejected) + " · 无人工决议的批准结果 " + number(gate.resolved) : "审批回执未记录；运行状态 " + (run ? stateName(run.status) : "未知") + "。", tone: waiting || run && run.status === "awaiting_human" ? "wait" : "idle", destination: "review" };
  }
  function Workflow(props) {
    const data = blockData(props.block), runs = Array.isArray(data.runs) ? data.runs : [], active = data.active_nodes || {}, hour = data.last_hour || {};
    const [runId, setRunId] = useState(""), [run, setRun] = useState(null), [failure, setFailure] = useState(""), [detailLoading, setDetailLoading] = useState(false), [selection, setSelection] = useState({ kind: "main", id: "risk" });
    const seq = useRef(0);
    useEffect(function () {
      setRunId(function (old) { return runs.some(function (item) { return item.run_id === old; }) ? old : runs.length ? String(runs[0].run_id) : ""; });
    }, [props.block]);
    useEffect(function () {
      const current = ++seq.current;
      setRun(null); setFailure("");
      if (!runId) { setDetailLoading(false); return; }
      const controller = new AbortController(); setDetailLoading(true);
      props.request(ROOT + "/workflow-runs/" + encodeURIComponent(runId), { signal: controller.signal }).then(function (value) {
        if (current !== seq.current) return;
        if (!value || value.run_id !== runId || !Array.isArray(value.nodes)) throw new Error("run identity mismatch");
        setRun(value);
      }).catch(function (error) { if (current === seq.current && error.name !== "AbortError") setFailure("运行详情读取失败，节点状态未知。"); }).finally(function () { if (current === seq.current) setDetailLoading(false); });
      return function () { controller.abort(); };
    }, [runId, props.request, props.refreshKey, props.block]);
    const nodes = run && Array.isArray(run.nodes) ? run.nodes : [];
    const structureStatus = runId || !Array.isArray(data.runs) || props.block && props.block.status === "unavailable" && data.enabled !== false ? "状态未知" : "暂无运行";
    const schematic = [
      { id: "structure-evidence", role: "evidence_specialist", depends_on: [], status: structureStatus, structure: true },
      { id: "structure-rules", role: "rule_specialist", depends_on: [], status: structureStatus, structure: true },
      { id: "structure-feature", role: "feature_specialist", depends_on: ["structure-evidence", "structure-rules"], status: structureStatus, structure: true }
    ];
    const displayedNodes = nodes.length ? nodes : schematic;
    const evidenceNode = displayedNodes.find(function (node) { return node.role === "evidence_specialist"; });
    const ruleNode = displayedNodes.find(function (node) { return node.role === "rule_specialist"; });
    const featureNode = displayedNodes.find(function (node) { return node.role === "feature_specialist"; });
    const roots = evidenceNode && ruleNode && Array.isArray(evidenceNode.depends_on) && !evidenceNode.depends_on.length && Array.isArray(ruleNode.depends_on) && !ruleNode.depends_on.length;
    const featureDeps = featureNode && Array.isArray(featureNode.depends_on) ? featureNode.depends_on : [];
    const standard = displayedNodes.length === 3 && roots && featureDeps.length === 2 && featureDeps.includes(evidenceNode.id) && featureDeps.includes(ruleNode.id);
    function nodeButton(node) {
      const deps = Array.isArray(node.depends_on) ? node.depends_on : null;
      return h("button", { className: "ov-node", type: "button", key: node.id, "data-tone": nodeTone(node, run), "aria-pressed": selection.kind === "node" && selection.id === node.id, onClick: function () { setSelection({ kind: "node", id: node.id }); } }, h("b", null, roleName(node.role)), h("small", null, h("i", { className: "ov-state-mark" }), nodeStatus(node, run)), !node.structure && h("span", null, "依赖 ", deps ? (deps.length ? nodeNames(deps, displayedNodes) : "无") : "未知"));
    }
    function gateButton(id) { const item = GateFact(id, run); return h("button", { type: "button", key: id, className: "ov-gate", "data-tone": item.tone, "aria-pressed": selection.kind === "gate" && selection.id === id, onClick: function () { setSelection({ kind: "gate", id: id }); } }, h("b", null, item.name), h("small", null, item.status)); }
    let fact = selection.kind === "main" ? mainFact(selection.id, props.risk, props.activity) : selection.kind === "gate" ? GateFact(selection.id, run) : null;
    if (selection.kind === "node") { const node = displayedNodes.find(function (item) { return item.id === selection.id; }); if (node) fact = nodeFact(node, run); }
    if (!fact) fact = { name: "选择流程节点", status: "未选择", dependency: "选择当前运行中的节点查看依赖。", impact: "状态取自所选运行记录。", evidence: "当前节点详情未提供。", tone: "idle", destination: "agentWorkflows" };
    return h("section", { className: "ov-flow-section", "aria-labelledby": "ov-flow-title" },
      h("div", { className: "ov-section-title" }, h("div", null, h("h2", { id: "ov-flow-title" }, "风险识别与特征流转"), h("p", null, "主链生成风险事实，按配置可进入专家分析。"))),
      h("div", { className: "ov-flow-frame" },
        h("div", { className: "ov-main-lane" }, h("div", { className: "ov-lane-label" }, h("i"), "确定性主链"), h("div", { className: "ov-main-chain" }, MAIN.map(function (item) { return h("button", { type: "button", key: item.id, "aria-pressed": selection.kind === "main" && selection.id === item.id, onClick: function () { setSelection({ kind: "main", id: item.id }); } }, h("b", null, item.name), h("small", null, item.hint)); }))),
        h("div", { className: "ov-branch" }, h("div", { className: "ov-branch-header" }, h("div", null, h("div", { className: "ov-lane-label accent" }, "⑂ 按需分析分支"), h("h3", null, "多专家工作流")), h("label", { className: "ov-run-select" }, h("span", null, "工作流运行"), h("select", { value: runId, title: runId || "暂无运行", disabled: !runs.length, onChange: function (event) { setRunId(event.target.value); setSelection({ kind: "main", id: "risk" }); } }, !runs.length && h("option", { value: "" }, "暂无运行"), runs.map(function (item) { const at = item.updated_at || item.created_at; return h("option", { key: item.run_id, value: item.run_id, title: String(item.run_id) }, shortRun(item.run_id) + (at ? " · " + stamp(at) : "") + " · " + stateName(item.status)); })))),
          (data.enabled === false ? h("p", { className: "ov-availability" }, "未启用 · 历史运行仍可查看") : h(Notice, { block: props.block, loading: props.loading })),
          h("div", { className: "ov-run-info" }, h("span", null, data.enabled === true ? "专家工作流已启用" : data.enabled === false ? "专家工作流已关闭" : "专家工作流启用状态未知"), h("span", null, run ? "所选运行 · " + stateName(run.status) : detailLoading ? "正在读取运行详情" : runId ? "运行状态未知" : "暂无所选运行"), run && h("span", null, "更新 " + stamp(run.updated_at))),
          failure && h("p", { className: "ov-inline-error", role: "alert" }, failure),
          !nodes.length && h("p", { className: "ov-structure-note" }, structureStatus === "状态未知" ? "示意结构 · 运行节点状态未知" : "示意结构 · 尚无所选运行"),
          standard ? h("div", { className: "ov-dag", "aria-label": nodes.length ? "所选运行的专家依赖与后续门禁" : "专家流程示意结构" },
            h("div", { className: "ov-fork-pair" }, nodeButton(evidenceNode), nodeButton(ruleNode)),
            h("div", { className: "ov-converge", "aria-hidden": "true" }, h("svg", { viewBox: "0 0 42 100", preserveAspectRatio: "none" }, h("path", { d: "M0 24 H10 Q17 24 17 31 V44 Q17 50 24 50 H42" }), h("path", { d: "M0 76 H10 Q17 76 17 69 V56 Q17 50 24 50 H42" }))),
            h("div", { className: "ov-role-place ov-feature-place" }, nodeButton(featureNode)), h("div", { className: "ov-bridge ov-bridge-one", "aria-hidden": "true" }),
            h("div", { className: "ov-role-place ov-evaluator-place" }, gateButton("evaluator")), h("div", { className: "ov-bridge ov-bridge-two", "aria-hidden": "true" }),
            h("div", { className: "ov-role-place ov-human-place" }, gateButton("human"))) : h(React.Fragment, null,
            h("p", { className: "ov-structure-note" }, "此运行拓扑与示意不同，以下列出实际节点及直接依赖。"),
            h("div", { className: "ov-expert-graph", "aria-label": "所选运行的实际节点与直接依赖" }, displayedNodes.map(nodeButton)),
            h("div", { className: "ov-gates" }, h("span", null, "后续门禁"), gateButton("evaluator"), gateButton("human"))),
          h("p", { className: "ov-dag-caption" }, nodes.length ? "节点依赖来自所选运行；校验与人工审批为后续门禁。" : "示意中两路输出汇入特征专家；实际依赖以所选运行记录为准。"),
          h("div", { className: "ov-inspect", "data-tone": fact.tone, "aria-live": "polite" }, h("div", null, h("small", null, fact.status), h("h4", null, fact.name)), h("dl", null,
            [["直接依赖", fact.dependency], ["影响", fact.impact], ["记录证据", fact.evidence]].map(function (entry) { return h("div", { key: entry[0] }, h("dt", null, entry[0]), h("dd", null, entry[1])); })), h("button", { type: "button", className: "ov-text-link", onClick: function () { props.onNavigate(fact.destination); } }, "查看详情 ↗"))),
        h("div", { className: "ov-fleet-summary" }, h("h4", null, "工作台运行汇总"), h("div", { className: "ov-rollup" }, h("div", null, h("small", null, "专家节点 · 当前"), h("p", null, "排队 " + number(active.queued) + " · 运行 " + number(active.running) + " · 等待 " + number(active.waiting))), h("div", null, h("small", null, "多专家分支 · 近 1 小时"), h("p", null, "启动 " + number(hour.started) + " · 完成 " + number(hour.completed) + " · 失败 " + number(hour.failed) + " · 等待人工 " + number(hour.awaiting_human)))),
          h("div", { className: "ov-fleet" }, h("span", null, "待调度工作流 ", h("b", null, number(data.queued))), h("span", null, "当前待审批 ", h("b", null, number(props.approvals.pending_groups))), h("span", null, "近 1 小时人工通过 ", h("b", null, number(props.approvals.approved_last_hour)))))));
  }
  function Activity(props) {
    const data = blockData(props.block), drain = data.drain || {}, approvals = props.approvals || {}, volume = volumeSets(data);
    const primary = volume.primary, secondary = volume.secondary;
    const primaryLabel = volume.source === "batch" ? "已提交流批次 · 未跨任务去重" : "已核验去重收据 · 账本覆盖";
    const secondaryLabel = volume.source === "batch" ? "已核验去重收据 · 覆盖摘要" : "已提交流批次 · 未跨任务去重";
    const rate = validNumber(drain.compression_rate) && drain.compression_rate <= 1 ? number(drain.compression_rate * 100) + "%" : "未知";
    return h("section", { className: "ov-activity", "aria-labelledby": "ov-activity-title" }, h("div", { className: "ov-section-title" }, h("div", null, h("h2", { id: "ov-activity-title" }, "识别工作量"), h("p", null, "工作台已登记来源 · 北京时间"))),
      h(Notice, { block: props.block, loading: props.loading }),
      h("div", { className: "ov-business" }, h("section", { "data-volume-source": volume.source }, h("h3", null, "已消费日志"), h("p", { className: "ov-volume-note" }, primaryLabel),
        h("div", { className: "ov-volume-line" }, h("b", null, number(primary.today_records), " ", h("em", null, validNumber(primary.today_records) ? "条" : "")), h("span", null, "累计 ", h("strong", null, number(primary.total_records)), " 条")),
        h("div", { className: "ov-measure-title" }, "近 7 天每日消费", h("small", null, "条")),
        h("div", { className: "ov-volume-primary" }, h(BarChart, { rows: primary.seven_day_records, value: function (row) { return row.records; }, labelFor: function (row) { return dateLabel(row.date); }, label: "近七天" + primaryLabel })),
        h("details", { className: "ov-batch-details" }, h("summary", null, h("span", null, secondaryLabel), h("small", null, "今日 " + number(secondary.today_records) + " · 累计 " + number(secondary.total_records) + " 条 ▾")),
          h("p", null, "两种口径独立统计，不相加；未登记日期显示未知。"), h(BarChart, { rows: secondary.seven_day_records, value: function (row) { return row.records; }, labelFor: function (row) { return dateLabel(row.date); }, label: "近七天" + secondaryLabel }))),
        h("section", null, h("h3", null, "待审批组数"), h("div", { className: "ov-volume-line" }, h("b", null, number(approvals.pending_groups), " ", h("em", null, validNumber(approvals.pending_groups) ? "组" : "")), h("span", null, "当前工作台队列")), h(Notice, { block: props.approvalBlock, loading: props.loading }), h("div", { className: "ov-measure-title" }, "今日待审批", h("small", null, "组 · 北京时间")), h(LineChart, { rows: props.approvalHistory, dateKey: "at", labelFor: clockLabel, plain: true, series: [{ key: "pending_groups", label: "待审批" }], label: "今日待审批组数历史，缺失处不连线", empty: "暂无审批队列历史" }), h("button", { className: "ov-text-link", type: "button", onClick: function () { props.onNavigate("review"); } }, "查看审批队列 ↗"))),
      h("div", { className: "ov-drain-band" }, h("div", null, h("small", null, "Drain3 模板化"), h("b", null, rate), h("span", null, "今日归并压缩率")), [[drain.template_windows, "今日模板窗口"], [drain.unknown_templates, "当前待识别模板"], [data.approved_rules, "累计已批准规则"]].map(function (item) { return h("div", { key: item[1] }, h("b", null, number(item[0])), h("span", null, item[1])); })));
  }
  function App(props) {
    const [snapshot, setSnapshot] = useState(null), [failure, setFailure] = useState(""), [loading, setLoading] = useState(true);
    const sequence = useRef(0), approvalHistory = useRef({ day: "", points: [], lastAt: null });
    function appendApprovalGap() {
      const state = approvalHistory.current;
      if (state.points.length && state.points[state.points.length - 1].pending_groups != null) state.points = state.points.concat([{ at: null, pending_groups: null }]).slice(-120);
    }
    function beijingDay(value) {
      const time = Date.parse(value || "");
      if (!Number.isFinite(time)) return "";
      const parts = new Intl.DateTimeFormat("en-US", { timeZone: "Asia/Shanghai", year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(time);
      const fields = {}; parts.forEach(function (part) { fields[part.type] = part.value; });
      return fields.year + "-" + fields.month + "-" + fields.day;
    }
    function recordApproval(value) {
      const day = beijingDay(value.as_of), block = value.approvals, content = blockData(block);
      if (!day) { appendApprovalGap(); return; }
      let state = approvalHistory.current;
      if (state.day !== day) { state = { day: day, points: [], lastAt: null }; approvalHistory.current = state; }
      if (!block || block.status === "unavailable" || !validNumber(content.pending_groups)) { appendApprovalGap(); return; }
      const history = Array.isArray(content.history) ? content.history : [];
      function append(at, count) {
        const time = Date.parse(at || "");
        if (!Number.isFinite(time) || beijingDay(at) !== day) return;
        if (state.lastAt != null && time < state.lastAt) { appendApprovalGap(); return; }
        if (state.lastAt === time) return;
        state.points.push({ at: at, pending_groups: validNumber(count) ? count : null });
        state.lastAt = time; state.points = state.points.slice(-120);
      }
      history.slice().sort(function (a, b) { return Date.parse(a.at || "") - Date.parse(b.at || ""); }).forEach(function (point) {
        if (state.lastAt == null || Date.parse(point.at || "") > state.lastAt) append(point.at, point.pending_groups);
      });
      append(value.as_of, content.pending_groups);
    }
    useEffect(function () {
      let disposed = false, active = null, timer = null;
      function load() {
        if (disposed || document.hidden || active) return;
        const seq = ++sequence.current;
        const controller = new AbortController(); active = controller;
        props.request(ROOT, { signal: controller.signal }).then(function (value) {
          if (disposed || seq !== sequence.current) return;
          if (!value || typeof value !== "object") throw new Error("overview unavailable");
          recordApproval(value);
          setSnapshot(value); setFailure(""); setLoading(false);
        }).catch(function (error) {
          if (disposed || seq !== sequence.current || error.name === "AbortError") return;
          appendApprovalGap(); setSnapshot(null); setFailure("总览数据读取失败，请刷新重试；各项当前状态未知。"); setLoading(false);
        }).finally(function () { if (active === controller) active = null; });
      }
      function visibility() {
        if (document.hidden) {
          appendApprovalGap();
          if (active) { ++sequence.current; active.abort(); active = null; }
        } else load();
      }
      load(); timer = setInterval(load, 30000); document.addEventListener("visibilitychange", visibility);
      return function () { disposed = true; if (active) active.abort(); clearInterval(timer); document.removeEventListener("visibilitychange", visibility); };
    }, [props.request, props.refreshKey]);
    const data = snapshot || {}, risk = blockData(data.risk), activity = blockData(data.activity), approvals = blockData(data.approvals);
    return h("div", { className: "ov", "data-overview-state": loading ? "loading" : failure ? "error" : "ready" },
      h("div", { className: "ov-asof" }, h("span", null, failure || (snapshot && snapshot.as_of ? "数据截至 " + stamp(snapshot.as_of) + " · Asia/Shanghai" : loading ? "正在读取工作台数据…" : "数据时间未知"))),
      h("div", { className: "ov-lead" }, h(RiskCard, { block: data.risk, loading: loading, onNavigate: props.onNavigate }), h(UsageCard, { block: data.usage, loading: loading, onNavigate: props.onNavigate })),
      h(Workflow, { block: data.workflows, risk: risk, activity: activity, approvals: approvals, request: props.request, refreshKey: props.refreshKey, loading: loading, onNavigate: props.onNavigate }),
      h(Activity, { block: data.activity, approvals: approvals, approvalBlock: data.approvals, approvalHistory: approvalHistory.current.points, loading: loading, onNavigate: props.onNavigate }));
  }
  window.LOGRISKStreamOverview = App;
}());
