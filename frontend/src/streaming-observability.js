(function () {
  "use strict";
  const h = React.createElement;
  const { useEffect, useMemo, useRef, useState } = React;
  const ROOT = "/api/streaming/observability/sources";
  const POLL_MS = 15000;
  const STALE_MS = 45000;
  const WINDOW_MS = 15 * 60 * 1000;
  const statusText = {
    disabled: "已关闭", unknown: "未知", stale: "观测陈旧", error: "采样失败", recovering: "采样恢复中", available: "可用",
    running: "消费进程运行中", waiting: "调度器等待下一批", completed: "本批已完成", stopped: "消费进程已停止",
    processing: "处理中", idle: "空闲", advancing: "正在推进", stalled: "持久化未推进", blocked: "处理受阻",
    commit_delayed: "提交滞后", commit_failed: "提交失败", incomparable: "前沿不可比较", batch_completed: "本批已完成",
    covered: "已覆盖", pending: "等待覆盖", delayed: "结果覆盖延迟",
  };
  const reasonText = {
    never_committed: "尚无组提交记录", sample_expired: "采样已过期", timeout: "超时",
    forbidden: "无权读取", unavailable: "读数不可用", not_sampled: "尚未采样",
    no_sample: "尚无采样", not_available: "读数不可用", no_worker_heartbeat: "尚无消费进程心跳",
    no_result: "尚无可核对结果", no_task: "尚无任务", source_disabled: "来源已关闭",
    shared_group: "共享组无法确认写者", incomparable: "前沿不可比较", counter_reset: "计数基线已变化",
    insufficient_samples: "样本不足，正在建立基线", no_progress: "未观察到推进",
    not_observed: "尚未观测", no_current_sample: "尚无有效采样", worker_disabled: "来源已关闭",
    heartbeat_expired: "消费进程心跳已过期", process_unverified: "任务记录存在，进程未验证",
    bounded_batch_completed: "固定批次已完成", frontier_incomparable: "前沿不一致",
    group_writer_unverified: "组写者未确认", sample_misaligned: "采样时间未对齐",
    epoch_mismatch: "来源时代不一致", baseline_required: "样本不足，正在建立基线",
    external_commit_pending: "等待外部提交确认", durable_frontier_stationary: "持久化前沿未推进",
    delivered_not_durable: "已交付，尚未持久化", insufficient_evidence: "证据不足", partition_limit: "分区超过采样上限",
    coverage_unverified: "结果覆盖尚未核对", sample_recovery_pending: "已有一次有效采样，等待下一次确认",
    result_frontier_behind: "结果尚未覆盖已落库数据", missing_partition: "采样缺少分区", worker_epoch_expired: "消费进程心跳已过期",
    scheduler_waiting: "调度器等待下一批", scheduler_checking: "调度器正在检查下一批",
    scheduler_heartbeat_expired: "调度器心跳已过期", external_commit_failed: "外部提交失败",
    batch_time_budget_exceeded: "本轮时间预算已到，保留原批次目标待续",
    topic_identity_changed: "主题身份已变化", partition_set_changed: "分区集合已变化",
    offset_out_of_range: "位点超出保留范围", authentication: "身份验证失败", authorization: "权限不足",
  };
  const actionText = {
    check_sampler: "检查采样器和 API，确认最近一次采样是否成功。",
    check_source: "检查来源配置及最近采样事件。",
    check_worker: "检查消费进程心跳、阶段和最近处理事件。",
    check_downstream: "检查解析、持久化及下游等待。",
    check_commit: "检查提交事件、组权限和协调器。",
    check_result: "检查结果构建和发布时间。",
    check_frontiers: "核对来源时代与分区前沿，确认是否有重置或采样偏差。",
    inspect_partitions: "检查分区差距和最近推进时间。",
    configure_source: "查看来源配置要求；页面不会自动启用消费。",
    inspect_sampler_error: "检查采样失败事件及来源权限。",
    inspect_sampler_freshness: "检查采样器和 API，确认最近一次采样是否成功。",
    inspect_frontier_identity: "核对来源时代和分区前沿，确认是否有重置或采样偏差。",
    inspect_worker_phase: "检查消费进程当前阶段、心跳和持久化事件。",
    inspect_pending_commit: "检查提交事件、组权限和协调器。",
    inspect_result_projection: "检查结果构建与发布事件。",
  };
  function explanation(code) {
    if (!code) return "";
    return reasonText[code] || String(code);
  }
  function stamp(value) {
    if (!value) return "未知";
    const d = new Date(value);
    return Number.isFinite(d.getTime()) ? d.toLocaleString() : "未知";
  }
  function ageText(ms) {
    if (ms == null || !Number.isFinite(ms) || ms < 0) return "时间不可比较";
    const seconds = Math.floor(ms / 1000);
    return seconds < 60 ? seconds + " 秒前" : Math.floor(seconds / 60) + " 分钟前";
  }
  function sourceLabel(source) {
    return [source.environment || "环境未知", source.cluster_id || "来源未知", source.topic || "Topic 未知", source.consumer_group || "Group 未知"].join(" / ");
  }
  function sampleAge(snapshot, receivedAt, now) {
    if (!snapshot || !Number.isFinite(receivedAt)) return null;
    const sample = Date.parse(snapshot.sampled_at), served = Date.parse(snapshot.served_at);
    if (!Number.isFinite(sample) || !Number.isFinite(served) || served < sample || now < receivedAt) return null;
    return served - sample + now - receivedAt;
  }
  function metricText(metric, stale) {
    if (offsetValue(metric) == null) return "未知";
    const value = String(metric.value);
    return (stale ? "历史 " : "") + (metric.coverage === "lower_bound" ? "至少 " : "") + value;
  }
  function metricNote(metric, stale) {
    if (!metric) return "尚无读数";
    const coverage = metric.coverage === "lower_bound" ? "部分覆盖" : metric.coverage === "complete" ? "完整覆盖" : "覆盖未知";
    const count = metric.total_partitions != null ? " · " + (metric.known_partitions == null ? "?" : metric.known_partitions) + "/" + metric.total_partitions + " 分区" : "";
    return (stale ? "当前未知 · 以下为最后已知值 · " : "") + coverage + count + (metric.reason_code ? " · " + explanation(metric.reason_code) : "");
  }
  function groupReading(metric, stale) {
    const value = offsetValue(metric);
    let note = "";
    if (stale) note = value == null ? "当前未知 · " + (explanation(metric && metric.reason_code) || "暂无读数") : "当前未知 · 最后已知读数";
    else if (value == null) note = explanation(metric && metric.reason_code) || "暂无读数";
    else if (metric.coverage === "lower_bound") note = "部分覆盖" + (metric.known_partitions != null && metric.total_partitions != null ? " · " + metric.known_partitions + "/" + metric.total_partitions + " 分区" : "");
    return { value: metricText(metric, stale), note: note };
  }
  function rateReading(rate, stale) {
    const valid = rate && typeof rate.value === "number" && Number.isFinite(rate.value) && rate.value >= 0;
    if (!valid) return { value: "未知", note: (stale ? "当前未知 · " : "") + (explanation(rate && rate.reason_code) || "暂无读数") };
    return { value: (stale ? "历史 " : "") + String(rate.value) + " 条/秒", note: stale ? "当前未知 · 最后已知读数" : "" };
  }
  function resultReading(result, axis, stale) {
    const status = axis && axis.status;
    if (status === "covered") return { value: (stale ? "历史 " : "") + "已覆盖", note: stale ? "当前未知" : "" };
    if (status === "pending" || status === "delayed") {
      const waiting = status === "pending" ? "等待覆盖" : "覆盖延迟";
      const valid = typeof result.wait_seconds === "number" && Number.isFinite(result.wait_seconds) && result.wait_seconds >= 0 && typeof result.oldest_uncovered_commit_at === "string" && Number.isFinite(Date.parse(result.oldest_uncovered_commit_at));
      if (valid) return { value: (stale ? "历史 " : "") + String(result.wait_seconds) + " 秒", note: (stale ? "当前未知 · " : "") + waiting };
      return { value: "未知", note: (stale ? "当前未知 · " : "") + waiting + " · 等待时间未知" };
    }
    return { value: "未知", note: (stale ? "当前未知 · " : "") + (explanation(axis && axis.reason_code) || "暂无覆盖证据") };
  }
  function reading(reading, stale) {
    if (!reading) return { value: "未知", note: "暂无读数" };
    if (reading.value == null) return { value: reading.last_known_value == null ? "未知" : "历史 " + String(reading.last_known_value), note: "当前未知 · " + (explanation(reading.reason_code) || "暂无读数") };
    const availability = reading.availability || "unknown";
    const names = { known: "已知", unknown: "未知", stale: "陈旧", error: "错误", forbidden: "无权读取", not_applicable: "不适用", partial: "部分" };
    const historical = stale || availability !== "known";
    return { value: (historical ? "历史 " : "") + String(reading.value), note: (stale ? "当前未知 · 采样陈旧" : names[availability] || availability) + (reading.reason_code ? " · " + explanation(reading.reason_code) : "") };
  }
  function stateLabel(axis, stale) {
    if (stale) return "当前未知 · 观测陈旧";
    return statusText[axis && axis.status] || (axis && axis.status) || "未知";
  }
  function workerStateLabel(axis, stale) {
    if (!stale && axis && axis.status === "running") return "运行中";
    if (!stale && axis && axis.status === "stopped") return "已停止";
    if (!stale && axis && axis.status === "waiting" && axis.reason_code === "scheduler_checking") return "调度器检查下一批";
    if (!stale && axis && axis.status === "stale" && axis.reason_code === "scheduler_heartbeat_expired") return "调度器心跳陈旧";
    return stateLabel(axis, stale);
  }
  function schedulerDescription(scheduler) {
    if (!scheduler) return "";
    const states = { active: "运行中", stale: "心跳陈旧", inactive: "未运行" };
    const phases = { checking: "检查下一批", awaiting_batch: "等待下一批" };
    return "调度器 " + (states[scheduler.state] || "未知") + (phases[scheduler.phase] ? " · " + phases[scheduler.phase] : "") + " · 调度心跳 " + stamp(scheduler.last_heartbeat_at);
  }
  function sourcePath(id) { return ROOT + "/" + encodeURIComponent(id); }
  function addPoint(previous, detail, invalid) {
    const at = Date.parse(detail && detail.sampled_at);
    const source = detail && detail.source_id;
    const epoch = detail && detail.source_epoch;
    if (!source || !epoch || !Number.isFinite(at)) return previous;
    const same = previous.filter(function (p) { return p.source === source && p.epoch === epoch && at - p.at <= WINDOW_MS; });
    const gap = detail.derived && detail.derived.undurable_offset_gap;
    // A repeated Broker timestamp can still have a newly incomparable D value.
    // Preserve the interruption before deduplicating valid observations.
    if (invalid || !gap || gap.coverage !== "complete" || offsetValue(gap) == null) return appendGap(same);
    if (same.some(function (p) { return p.at === at && p.value != null; })) return same;
    return same.concat([{ source: source, epoch: epoch, at: at, value: String(gap.value) }]).slice(-60);
  }
  function appendGap(previous) {
    if (!previous.length || previous[previous.length - 1].value == null) return previous;
    const last = previous[previous.length - 1];
    return previous.concat([{ source: last.source, epoch: last.epoch, at: last.at + 1, value: null }]).slice(-60);
  }
  function offsetValue(metric) {
    if (!metric || metric.value == null || !["complete", "lower_bound"].includes(metric.coverage)) return null;
    const value = String(metric.value);
    return /^\d{1,64}$/.test(value) ? BigInt(value) : null;
  }
  function shortTime(timestamp) {
    const value = new Date(timestamp);
    return Number.isFinite(value.getTime()) ? value.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "未知";
  }
  function Trend(props) {
    const points = props.points || [];
    const valid = points.filter(function (point) { return point.value != null; });
    if (!valid.length) return h("div", { className: "so-chart-empty" }, "暂无可比采样");
    let min = BigInt(valid[0].value), max = min;
    valid.forEach(function (point) { const value = BigInt(point.value); if (value < min) min = value; if (value > max) max = value; });
    const first = points[0].at, last = points[points.length - 1].at;
    const duration = Math.max(1, last - first), range = max - min;
    const xFor = function (point) { return 8 + 484 * (point.at - first) / duration; };
    const yFor = function (value) { return range === 0n ? 80 : 144 - Number((BigInt(value) - min) * 1280000n / range) / 10000; };
    let path = "", previous = null, segments = 0;
    points.forEach(function (point) {
      if (point.value == null) { previous = null; return; }
      const x = xFor(point), y = yFor(point.value);
      const continues = previous && point.at - previous.at <= STALE_MS;
      path += (continues ? " L " : " M ") + x.toFixed(2) + " " + y.toFixed(2);
      if (continues) segments += 1;
      previous = point;
    });
    const ticks = range === 0n ? [{ value: min, y: 80 }] : [{ value: max, y: 16 }, { value: min, y: 144 }];
    const midpoint = (min + max) / 2n;
    if (range !== 0n && midpoint !== min && midpoint !== max) ticks.splice(1, 0, { value: midpoint, y: yFor(midpoint) });
    function axisLabel(value) {
      const exact = String(value);
      return exact.length <= 22 ? exact : exact[0] + "." + exact.slice(1, 4) + "… × 10^" + (exact.length - 1);
    }
    const axisWidth = Math.max(3, ...ticks.map(function (tick) { return axisLabel(tick.value).length; }));
    return h("figure", { className: "so-chart" },
      h("div", { className: "so-chart-layout" },
        h("div", { className: "so-chart-y", "aria-hidden": "true", style: { width: axisWidth + "ch" } }, ticks.map(function (tick) { return h("span", { key: String(tick.value), title: String(tick.value), style: { top: (tick.y / 160 * 100) + "%" } }, axisLabel(tick.value)); })),
        h("svg", { className: "so-trend-chart", viewBox: "0 0 500 160", role: "img", "aria-label": "本会话落库前积压趋势，缺失样本处断线", preserveAspectRatio: "none" },
          h("title", null, "落库前积压趋势"), h("desc", null, valid.length + " 个有效采样，范围 " + String(min) + " 至 " + String(max) + " 位点差；缺失样本处不连线。"),
          ticks.map(function (tick) { return h("line", { key: String(tick.value), className: "so-chart-grid", x1: 8, x2: 492, y1: tick.y, y2: tick.y }); }),
          segments > 0 && h("path", { className: "so-chart-line", d: path }),
          valid.map(function (point, index) { return h("circle", { className: "so-chart-point", key: point.at + ":" + index, cx: xFor(point), cy: yFor(point.value), r: 4 }, h("title", null, stamp(new Date(point.at).toISOString()) + " · " + point.value + " 位点差")); }))),
      h("div", { className: "so-chart-times", "aria-hidden": "true", style: { paddingLeft: "calc(" + axisWidth + "ch + 8px)" } }, h("span", null, shortTime(valid[0].at)), h("span", null, valid.length > 1 ? shortTime(valid[valid.length - 1].at) : "")),
      h("figcaption", null, valid.length === 1 ? "1 个有效采样点" : segments ? "本次会话 · 缺失处断线" : "采样不连续 · 未连接"));
  }
  function StageChart(props) {
    const stages = [
      { id: "read", label: "已读未落库", metric: props.derived.read_undurable_offset_gap },
      { id: "commit", label: "已落库未提交", metric: props.derived.unconfirmed_commit_offset_gap },
    ];
    if (props.task) stages.push({ id: "batch", label: "本批待落库", metric: props.derived.batch_remaining_offset_gap });
    function historical(stage) { return props.stale || (stage.id === "commit" && props.sourceStale); }
    let max = 0n;
    stages.forEach(function (stage) { const value = historical(stage) ? null : offsetValue(stage.metric); if (value != null && value > max) max = value; });
    return h("div", { className: "so-stage-chart", "aria-label": "各处理阶段位点差距" }, stages.map(function (stage) {
      const isHistorical = historical(stage), value = isHistorical ? null : offsetValue(stage.metric);
      const quality = value == null ? "unknown" : stage.metric.coverage;
      const width = value != null && max > 0n ? Number(value * 100000n / max) / 1000 : 0;
      const reason = stage.metric && stage.metric.reason_code;
      const note = quality === "lower_bound" ? "部分 " + stage.metric.known_partitions + "/" + stage.metric.total_partitions + " 分区" : quality === "unknown" ? (isHistorical ? "当前未知" : explanation(reason) || "暂无读数") : "";
      return h("div", { className: "so-stage", "data-stage": stage.id, "data-quality": quality, key: stage.id },
        h("div", { className: "so-stage-top" }, h("span", null, stage.label), h("strong", null, metricText(stage.metric, isHistorical))),
        h("div", { className: "so-stage-track " + quality, "aria-hidden": "true" }, value != null && value > 0n && h("span", { className: "so-stage-fill", style: { width: width + "%" } })),
        note && h("small", { className: "so-stage-note" }, note));
    }));
  }
  function App(props) {
    const [sources, setSources] = useState([]), [selected, setSelected] = useState(""), [moreSources, setMoreSources] = useState(false);
    const [detail, setDetail] = useState(null), [rows, setRows] = useState([]), [cursor, setCursor] = useState(null);
    const [events, setEvents] = useState([]), [eventsFailure, setEventsFailure] = useState(false), [failure, setFailure] = useState(""), [pageFailure, setPageFailure] = useState("");
    const [loading, setLoading] = useState(true), [moreBusy, setMoreBusy] = useState(false), [now, setNow] = useState(performance.now());
    const [points, setPoints] = useState([]), [overviewRefresh, setOverviewRefresh] = useState(0), [dark, setDark] = useState(function () {
      const saved = localStorage.getItem("logrisk.streaming.theme");
      return saved ? saved === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
    });
    const history = useRef({}), requestSequence = useRef(0), latest = useRef(null), refreshRef = useRef(null), moreAbort = useRef(null), polling = useRef(null), sourcePageBudget = useRef(Math.min(1000, Math.max(10, Number(localStorage.getItem("logrisk.streaming.sourcePages")) || 10)));
    const selectedRef = useRef(selected); selectedRef.current = selected;
    const sourceLabels = useMemo(function () {
      const labels = sources.map(sourceLabel), counts = {};
      labels.forEach(function (label) { counts[label] = (counts[label] || 0) + 1; });
      return labels.map(function (label, index) { return counts[label] > 1 ? label + " · 来源 " + (index + 1) : label; });
    }, [sources]);
    useEffect(function () {
      const timer = setInterval(function () { if (!document.hidden) setNow(performance.now()); }, 1000);
      return function () { clearInterval(timer); };
    }, []);
    useEffect(function () {
      let disposed = false, active = false, timer = null, controller = null, failures = 0, immediate = false;
      const seq = ++requestSequence.current;
      function isCurrent() { return !disposed && seq === requestSequence.current && selectedRef.current === selected; }
      function schedule(delay) { clearTimeout(timer); if (!disposed && !document.hidden) timer = setTimeout(poll, delay); }
      async function listSources(signal) {
        let result = [], next = "", seen = new Set(), pages = 0;
        do {
          const query = "?limit=100" + (next ? "&cursor=" + encodeURIComponent(next) : "");
          const value = await props.request(ROOT + query, { signal: signal });
          result = result.concat(Array.isArray(value.sources) ? value.sources : []); pages += 1;
          next = value.next_cursor || "";
          if (next && seen.has(next)) throw new Error("source_cursor_repeated");
          if (next) seen.add(next);
        } while (next && pages < sourcePageBudget.current);
        return { sources: result, more: !!next };
      }
      function markHistoryGap() {
        if (!latest.current || !latest.current.__historyIdentity) return;
        const identity = latest.current.__historyIdentity;
        history.current[identity] = appendGap(history.current[identity] || []);
        setPoints(history.current[identity]);
      }
      async function poll() {
        if (active || disposed || document.hidden) return;
        if (moreAbort.current || polling.current != null) { schedule(100); return; }
        active = true; polling.current = seq; controller = new AbortController();
        try {
          const sourcePage = await listSources(controller.signal), list = sourcePage.sources;
          if (!isCurrent()) return;
          setSources(list); setMoreSources(sourcePage.more);
          if (!selected) {
            const remembered = localStorage.getItem("logrisk.streaming.source") || "";
            const target = list.find(function (source) { return source.source_id === remembered; }) || list[0];
            setLoading(false); setFailure("");
            if (target) { selectedRef.current = target.source_id; setSelected(target.source_id); }
            return;
          }
          if (!list.some(function (source) { return source.source_id === selected; })) {
            setDetail(null); setRows([]); setCursor(null); setEvents([]); setPoints([]); latest.current = null;
            setFailure("所选来源不在当前可访问列表中，请重新选择。"); setLoading(false); return;
          }
          const snapshot = await props.request(sourcePath(selected), { signal: controller.signal });
          if (!isCurrent()) return;
          if (snapshot.source_id !== selected || !snapshot.snapshot_id) throw new Error("snapshot_identity_mismatch");
          const old = latest.current;
          const sampleAt = Date.parse(snapshot.sampled_at), oldAt = old ? Date.parse(old.sampled_at) : NaN;
          if (old && old.source_epoch === snapshot.source_epoch && Number.isFinite(oldAt) && (!Number.isFinite(sampleAt) || sampleAt < oldAt)) {
            setFailure("收到较旧采样，保留当前快照并等待下一次读取。"); failures += 1; markHistoryGap(); return;
          }
          const parts = await props.request(sourcePath(selected) + "/partitions?snapshot_id=" + encodeURIComponent(snapshot.snapshot_id) + "&limit=100", { signal: controller.signal });
          if (!isCurrent()) return;
          if (parts.snapshot_id !== snapshot.snapshot_id) throw new Error("snapshot_identity_mismatch");
          let recent = [], recentFailed = false;
          try { const result = await props.request(sourcePath(selected) + "/events?limit=30", { signal: controller.signal }); recent = Array.isArray(result.events) ? result.events : []; }
          catch (reason) { if (reason.name === "AbortError") throw reason; recentFailed = true; }
          if (!isCurrent()) return;
          const receivedAt = performance.now(), age = sampleAge(snapshot, receivedAt, receivedAt);
          snapshot.__receivedAt = receivedAt;
          latest.current = snapshot;
          setDetail(snapshot); setRows(Array.isArray(parts.partitions) ? parts.partitions : []); setCursor(parts.next_cursor || null); setEvents(recent); setEventsFailure(recentFailed);
          const partitionKey = String(snapshot.partition_count == null ? "unknown" : snapshot.partition_count) + ":" + (parts.partitions || []).map(function (row) { return String(row.partition); }).join(",");
          const identity = selected + "|" + String(snapshot.source_epoch || "") + "|" + partitionKey;
          snapshot.__historyIdentity = identity;
          const updated = addPoint(history.current[identity] || [], snapshot, age == null || age > STALE_MS || snapshot.state && snapshot.state.source && ["stale", "error", "unknown", "disabled", "recovering"].includes(snapshot.state.source.status));
          history.current[identity] = updated; setPoints(updated);
          setNow(receivedAt); setFailure(""); setPageFailure(""); setLoading(false); failures = 0;
        } catch (reason) {
          if (!isCurrent() || reason.name === "AbortError") return;
          failures += 1; setLoading(false);
          setFailure(reason.status === 403 ? "无权读取该来源或观测数据。" : reason.code === "snapshot_expired" ? "快照已过期，将重新读取。" : "读取观测快照失败；当前状态未知，保留最后已知数据。");
          markHistoryGap();
        } finally {
          active = false; controller = null; if (polling.current === seq) polling.current = null;
          if (isCurrent()) { const delay = immediate ? 0 : failures ? Math.min(60000, POLL_MS * Math.pow(2, Math.min(failures - 1, 2))) * (0.9 + Math.random() * 0.2) : POLL_MS; immediate = false; schedule(delay); }
        }
      }
      function visibility() {
        clearTimeout(timer);
        if (document.hidden) { if (controller) controller.abort(); if (moreAbort.current) moreAbort.current.abort(); }
        else if (active) { immediate = true; if (controller) controller.abort(); }
        else schedule(0);
      }
      refreshRef.current = function () { clearTimeout(timer); if (!active) poll(); };
      document.addEventListener("visibilitychange", visibility);
      poll();
      return function () {
        disposed = true; clearTimeout(timer); if (controller) controller.abort(); if (moreAbort.current) moreAbort.current.abort();
        document.removeEventListener("visibilitychange", visibility); refreshRef.current = null;
      };
    }, [selected, props.request]);
    function choose(id) {
      if (id === selected) return;
      if (moreAbort.current) moreAbort.current.abort();
      ++requestSequence.current; selectedRef.current = id; latest.current = null;
      setDetail(null); setRows([]); setCursor(null); setEvents([]); setEventsFailure(false); setPoints([]); setFailure(""); setPageFailure(""); setLoading(true); setMoreBusy(false);
      localStorage.setItem("logrisk.streaming.source", id); setSelected(id);
    }
    async function morePartitions() {
      if (!detail || !cursor || moreBusy) return;
      if (polling.current != null) { setPageFailure("正在刷新快照，请稍后加载分区。"); return; }
      const snapshot = detail.snapshot_id, id = selected, requestedCursor = cursor;
      const controller = new AbortController(); moreAbort.current = controller; setMoreBusy(true); setPageFailure("");
      try {
        const page = await props.request(sourcePath(id) + "/partitions?snapshot_id=" + encodeURIComponent(snapshot) + "&limit=100&cursor=" + encodeURIComponent(requestedCursor), { signal: controller.signal });
        if (selectedRef.current !== id || !latest.current || latest.current.snapshot_id !== snapshot || page.snapshot_id !== snapshot) return;
        setRows(function (old) { return old.concat(Array.isArray(page.partitions) ? page.partitions : []); }); setCursor(page.next_cursor || null);
      } catch (reason) {
        if (reason.name !== "AbortError" && selectedRef.current === id) setPageFailure(reason.code === "snapshot_expired" ? "快照已过期，请刷新后重新展开分区。" : "分区加载失败，请重试。");
      } finally { if (moreAbort.current === controller) { moreAbort.current = null; setMoreBusy(false); } }
    }
    const age = detail ? sampleAge(detail, detail.__receivedAt, now) : null;
    const axes = detail && detail.state || {}, derived = detail && detail.derived || {}, worker = detail && detail.worker || {}, result = detail && detail.result || {}, task = detail && detail.task;
    const stale = !!detail && (!!failure || now < detail.__receivedAt || now - detail.__receivedAt > STALE_MS);
    const sourceRecovering = !!(axes.source && axes.source.status === "recovering");
    const sourceStale = stale || !!detail && (age == null || age > STALE_MS || !axes.source || !["available", "recovering"].includes(axes.source.status));
    const source = detail && detail.source || {}, scheduler = worker.scheduler;
    const rate = derived.durable_records_per_second || {}, gap = derived.undurable_offset_gap;
    const next = detail && detail.next_action;
    const nextCopy = stale ? "检查采样器和 API。" : axes.source && axes.source.status === "disabled" ? "查看来源配置。" : next ? (actionText[next] || "检查相关事件与分区读数。") : sourceRecovering && !sourceStale ? "等待下一轮确认恢复。" : sourceStale ? "检查采样器。" : "继续观察分区进展及结果覆盖。";
    const blockStageText = { group_commit: "组提交", source_check: "来源检查" };
    let diagnostic = "";
    if (failure) diagnostic = failure;
    else if (detail && stale) diagnostic = "观测陈旧 · 检查采样器和 API";
    else if (detail && axes.source && axes.source.status === "disabled") diagnostic = "来源已关闭 · 当前积压未知";
    else if (detail && axes.source && axes.source.status === "error") diagnostic = "采样失败 · " + (explanation(axes.source.reason_code) || "原因未知");
    else if (detail && sourceStale) diagnostic = "当前采样未知 · " + (age == null ? "采样时间不可比较" : "采样已过期");
    else if (detail && sourceRecovering) diagnostic = "采样恢复中 · 等待再次确认";
    else if (detail && axes.progress && (["stalled", "blocked", "commit_failed", "commit_delayed", "incomparable"].includes(axes.progress.status) || axes.progress.reason_code === "batch_time_budget_exceeded")) diagnostic = (statusText[axes.progress.status] || "进展异常") + " · " + (explanation(axes.progress.reason_code) || "原因未知");
    else if (detail && axes.worker && ["unknown", "stale"].includes(axes.worker.status)) diagnostic = workerStateLabel(axes.worker, false) + " · " + (explanation(axes.worker.reason_code) || "进程未验证");
    else if (detail && axes.result && axes.result.status === "delayed") diagnostic = "结果覆盖延迟 · " + (explanation(axes.result.reason_code) || "等待结果发布");
    if (diagnostic && next && !failure) diagnostic += " · " + (actionText[next] || "查看运行证据。");
    const gapNote = sourceStale ? (offsetValue(gap) == null ? "当前未知 · " + (explanation(gap && gap.reason_code) || "暂无读数") : "当前未知 · 最后已知读数") : gap && gap.coverage === "lower_bound" ? "部分覆盖 · " + gap.known_partitions + "/" + gap.total_partitions + " 分区" : !gap || gap.value == null ? explanation(gap && gap.reason_code) || "暂无读数" : sourceRecovering ? "恢复中 · 等待确认" : "";
    const health = [
      { label: "消费进程", value: workerStateLabel(axes.worker, stale) },
      { label: "进展", value: stateLabel(axes.progress, stale) },
    ];
    const metrics = [
      { id: "group-backlog", label: "消费组积压", unit: "位点差", reading: groupReading(derived.group_offset_gap, sourceStale) },
      { id: "durable-rate", label: "落库速率", reading: rateReading(rate, sourceStale) },
      { id: "result-wait", label: "结果发布等待", reading: resultReading(result, axes.result, stale) },
    ];
    return h("div", { className: "so-page", "data-theme": dark ? "dark" : "light", "data-snapshot-id": detail && detail.snapshot_id || "" },
      h("header", { className: "so-heading" }, h("h1", null, "流运行总览"),
        h("div", { className: "so-heading-actions" }, h("button", { type: "button", onClick: function () { const nextDark = !dark; setDark(nextDark); localStorage.setItem("logrisk.streaming.theme", nextDark ? "dark" : "light"); }, "aria-pressed": dark }, dark ? "浅色模式" : "深色模式"), h("button", { type: "button", onClick: function () { if (refreshRef.current) refreshRef.current(); setOverviewRefresh(function (value) { return value + 1; }); } }, "刷新快照"))),
      h(window.LOGRISKStreamOverview, { request: props.request, onNavigate: props.onNavigate, refreshKey: overviewRefresh }),
      h("div", { className: "so-kafka-title" }, h("div", null, h("h2", null, "Kafka 运行观测"), h("p", null, "所选消费组 · 分区位点与处理进展"))),
      h("div", { className: "so-toolbar" },
        h("label", null, h("span", null, "观测来源"), h("select", { value: selected, onChange: function (event) { choose(event.target.value); }, disabled: !sources.length }, !sources.length && h("option", { value: "" }, "暂无可用来源"), sources.map(function (item, index) { return h("option", { key: item.source_id, value: item.source_id }, sourceLabels[index]); }))),
        moreSources && h("button", { type: "button", onClick: function () { sourcePageBudget.current += 1; localStorage.setItem("logrisk.streaming.sourcePages", String(sourcePageBudget.current)); if (refreshRef.current) refreshRef.current(); } }, "加载更多来源"),
        h("div", { className: "so-freshness", role: "status" }, h("b", null, sourceStale ? "当前未知" : sourceRecovering ? "采样恢复中" : detail ? "采样有效" : "等待读取"), h("span", null, detail ? "最近采样 " + ageText(age) : "尚无采样"))),
      loading && !detail && h("div", { className: "so-empty" }, "正在读取观测数据…"),
      !loading && !detail && !sources.length && !failure && h("div", { className: "so-empty" }, h("b", null, "尚未登记来源"), h("p", null, "登记来源后可查看流运行状态。"), h("button", { type: "button", onClick: props.onTasks }, "查看流式任务与来源")),
      !loading && !detail && sources.length > 0 && !failure && h("div", { className: "so-empty" }, "该来源尚无观测快照。"),
      !detail && failure && h("div", { className: "so-diagnostic", role: "alert" }, failure),
      detail && h(React.Fragment, null,
        h("dl", { className: "so-health", "aria-label": "流状态" }, health.map(function (entry) { return h("div", { key: entry.label }, h("dt", null, entry.label), h("dd", null, entry.value)); })),
        diagnostic && h("div", { className: "so-diagnostic", role: "status" }, diagnostic),
        h("dl", { className: "so-metrics", "aria-label": "辅助运行指标" }, metrics.map(function (entry) {
          return h("div", { key: entry.id, "data-metric": entry.id }, h("dt", null, entry.label, entry.unit && h("span", null, entry.unit)),
            h("dd", null, h("strong", null, entry.reading.value), entry.reading.note && h("small", null, entry.reading.note)));
        })),
        h("div", { className: "so-visuals" },
          h("section", { className: "so-panel so-trend", "aria-labelledby": "so-trend-title" },
            h("div", { className: "so-chart-heading" }, h("div", null, h("h2", { id: "so-trend-title" }, "落库前积压趋势"), h("span", { className: "so-unit" }, "位点差")),
              h("div", { className: "so-current" + (metricText(gap, sourceStale).length > 14 ? " so-current-long" : "") }, h("strong", null, metricText(gap, sourceStale)), gapNote && h("small", null, gapNote))),
            h(Trend, { points: points })),
          h("section", { className: "so-panel so-stages", "aria-labelledby": "so-stage-title" },
            h("div", { className: "so-chart-heading" }, h("div", null, h("h2", { id: "so-stage-title" }, "处理阶段差距"), h("span", { className: "so-unit" }, "位点差 · 独立读数"))),
            h(StageChart, { derived: derived, task: task, stale: stale, sourceStale: sourceStale }),
            h("p", { className: "so-stage-caption" }, "按本图最大已知值缩放"))),
        h("div", { className: "so-disclosures" },
          h("details", { className: "so-disclosure so-frontiers" }, h("summary", { "aria-label": "分区与位点" }, h("span", null, "分区与位点"), h("span", { "aria-hidden": "true" }, "查看明细")),
            h("div", { className: "so-disclosure-body" },
              h("p", { className: "so-meta" }, "来源 " + selected + " · 时代 " + (detail.source_epoch || "未知") + " · 快照 " + detail.snapshot_id),
              h("p", { className: "so-meta" }, "实时末端（E） · 已读取（R） · 已落库（D） · 组提交（G） · 本批目标（T） · " + (detail.offset_semantics || "语义未知")),
              h("p", { className: "so-meta" }, "落库前积压 E − D · 已读未落库 R − D · 已落库未提交 D − G · 本批待落库 T − D · 消费组积压 E − G。"),
              h("p", { className: "so-meta" }, "组提交差距 E − G：" + metricText(derived.group_offset_gap, sourceStale) + " offset · " + metricNote(derived.group_offset_gap, sourceStale)),
              h("p", { className: "so-meta" }, "位点差距不等于精确消息数；各差距可能重叠，不能相加。"),
              h("p", { className: "so-meta" }, "落库速率为相邻有效采样之间已落库记录数的增量除以时间（条/秒）；结果发布等待为最早未覆盖的落库批次等待时间，覆盖状态与结果发布时间分别判断。"),
              h("div", { className: "so-table-wrap" }, h("table", { className: "so-table" },
                h("thead", null, h("tr", null, ["分区", "实时末端（E）", "已读取（R）", "已落库（D）", "组提交（G）", "本批目标（T）", "结果前沿"].map(function (title) { return h("th", { scope: "col", key: title }, title); }))),
                h("tbody", null, rows.map(function (row) { return h("tr", { key: String(row.partition) }, h("th", { scope: "row" }, String(row.partition)), ["broker_end", "delivered", "durable", "group_committed", "batch_target", "result_frontier"].map(function (key) { const value = reading(row[key], stale || ((key === "broker_end" || key === "group_committed") && sourceStale)); return h("td", { key: key }, h("b", null, value.value), h("small", null, value.note), row[key] && row[key].observed_at && h("small", null, "采样 " + stamp(row[key].observed_at)), row[key] && row[key].last_changed_at && h("small", null, "变更 " + stamp(row[key].last_changed_at))); })); })))),
              !rows.length && h("p", { className: "so-meta" }, "此快照没有分区读数。"), pageFailure && h("p", { className: "so-diagnostic", role: "alert" }, pageFailure), cursor && h("button", { type: "button", onClick: morePartitions, disabled: moreBusy }, moreBusy ? "加载中…" : "加载更多分区"), h("p", { className: "so-meta" }, "已展示 " + rows.length + " / " + (detail.partition_count == null ? "未知" : detail.partition_count) + " 个分区。"))),
          h("details", { className: "so-disclosure so-evidence" }, h("summary", { "aria-label": "任务与运行证据" }, h("span", null, "任务与运行证据"), h("span", { "aria-hidden": "true" }, "任务、心跳与结果")),
            h("div", { className: "so-disclosure-body so-evidence-grid" },
              h("section", null, h("h3", null, "任务与结果"), h("dl", { className: "so-definition" },
                h("dt", null, "任务"), h("dd", null, task && task.task_id || "未知"), h("dt", null, "状态 / 阶段"), h("dd", null, task ? (task.status || "未知") + " / " + (task.stage || "未知") : "未知"), h("dt", null, "最后更新"), h("dd", null, stamp(task && task.updated_at)),
                h("dt", null, "结果代次"), h("dd", null, result.generation == null ? "未知" : String(result.generation)), h("dt", null, "结果发布时间"), h("dd", null, stamp(result.published_at)), h("dt", null, "覆盖核对时间"), h("dd", null, stamp(result.coverage_checked_at)), h("dt", null, "覆盖目标"), h("dd", null, result.coverage_target_seconds == null ? "未知" : result.coverage_target_seconds + " 秒"),
                result.oldest_uncovered_commit_at && h(React.Fragment, null, h("dt", null, "最早未覆盖"), h("dd", null, stamp(result.oldest_uncovered_commit_at) + (result.wait_seconds == null ? "" : " · 等待 " + result.wait_seconds + " 秒")))), h("button", { type: "button", onClick: props.onTasks }, "查看任务与未知模板")),
              h("section", null, h("h3", null, "运行证据"), h("dl", { className: "so-definition" },
                h("dt", null, "来源状态"), h("dd", null, stateLabel(axes.source, stale) + (axes.source && axes.source.reason_code ? " · " + explanation(axes.source.reason_code) : "")),
                h("dt", null, "采样时间"), h("dd", null, stamp(detail.sampled_at)), h("dt", null, "分配方式"), h("dd", null, detail.assignment_mode || "未知"),
                h("dt", null, "进程原因"), h("dd", null, explanation(axes.worker && axes.worker.reason_code) || "无"), h("dt", null, "进展原因"), h("dd", null, explanation(axes.progress && axes.progress.reason_code) || "无"), h("dt", null, "结果原因"), h("dd", null, explanation(axes.result && axes.result.reason_code) || "无"),
                h("dt", null, "消费进程"), h("dd", null, "阶段 " + (worker.phase || "未知") + " · 心跳 " + stamp(worker.last_heartbeat_at)), scheduler && h(React.Fragment, null, h("dt", null, "来源调度"), h("dd", null, schedulerDescription(scheduler))),
                h("dt", null, "最近拉取返回"), h("dd", null, stamp(worker.last_poll_returned_at)), h("dt", null, "最近持久化"), h("dd", null, stamp(worker.last_durable_commit_at)), h("dt", null, "最近外部提交"), h("dd", null, stamp(worker.last_external_commit_at)), h("dt", null, "落库速率"), h("dd", null, rateReading(rate, sourceStale).value),
                source.block_reason && h(React.Fragment, null, h("dt", null, "阻塞原因"), h("dd", null, explanation(source.block_reason) + (source.block_stage ? " · " + (blockStageText[source.block_stage] || source.block_stage) : ""))), h("dt", null, "下一步"), h("dd", null, nextCopy))))),
          h("details", { className: "so-disclosure so-recent" }, h("summary", { "aria-label": "最近事件" }, h("span", null, "最近事件"), h("span", { "aria-hidden": "true" }, events.length ? events.length + " 条" : "查看事件")),
            h("div", { className: "so-disclosure-body" }, eventsFailure ? h("p", { className: "so-meta" }, "事件读取失败，请稍后重试。") : !events.length && h("p", { className: "so-meta" }, "暂无事件。"), h("div", { className: "so-events-scroll", tabIndex: 0, role: "region", "aria-label": "最近事件，可滚动" }, h("ul", { className: "so-events" }, events.map(function (event, index) { return h("li", { key: String(event.created_at) + ":" + index }, h("b", null, event.event_type || "事件"), h("span", null, explanation(event.reason_code) || "无原因码"), h("small", null, stamp(event.created_at) + (event.task_id ? " · " + event.task_id : ""))); }))))))));
  }
  window.LOGRISKStreamingObservability = App;
}());
