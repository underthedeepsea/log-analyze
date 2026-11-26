/* Isolated frontend fixtures; all API and asset requests are intercepted locally. */
'use strict';
const assert = require('assert/strict');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');
const root = path.resolve(__dirname, '..');
const out = path.resolve(process.env.LOGRISK_FRONTEND_EVIDENCE || '/tmp/logrisk-stream-overview-browser');
fs.mkdirSync(out, { recursive: true });
let minute = 0, fail = false, disabled = false, irregular = false, noData = false, activeRun = false, budgetExhausted = false, usageComplete = false, budgetAttempts = 0, featureAttempts = 0, volumeMode = 'both', slowMode = false, slowReads = 0, slowRelease = null;
let workflowSummaryMissing = false;
const runA = '0123456789abcdef0123456789abcdef', runB = '11111111111111111111111111111111';
function at() { return new Date(Date.UTC(2026, 10, 23, 3, minute)).toISOString(); }
const block = data => ({ status: 'available', data });
function overview() {
  const now = at();
  const payload = { as_of: now, timezone: 'Asia/Shanghai',
    risk: { status: 'partial', reason: '存在历史缺口', data: { today: { hosts: 2, high_hosts: 1, groups: 3, known_occurrences: 4, unknown_groups: 1, recovery_groups: { unrecovered: 1, recovered: 1, unknown: 1 } }, trend: [{ date: '2026-11-21', hosts: 2, high_hosts: 1 }, { date: '2026-11-22', hosts: null, high_hosts: null }, { date: '2026-11-23', hosts: 2, high_hosts: 1 }], hosts: [{ cluster: 'cluster', node: '<script>evil</script>', level: 'high', groups: 3, known_occurrences: 4, last_seen: now }] } },
    usage: { status: 'partial', reason: '部分回执未知', data: { started_calls: 3, receipted_calls: 2, complete_calls: 1, input_tokens: 3000, output_tokens: null, known_tokens: 3600, total_tokens: null, estimated_cost_usd: null, models: [{ provider: 'local', model: 'model-a', known_tokens: 3600 }], buckets: [{ start_at: '2026-11-23T00:00:00+08:00', end_at: '2026-11-23T03:00:00+08:00', input_tokens: null, output_tokens: null, started_calls: 0, receipted_calls: 0 }, { start_at: '2026-11-23T03:00:00+08:00', end_at: '2026-11-23T06:00:00+08:00', input_tokens: null, output_tokens: null, started_calls: 1, receipted_calls: 0 }, { start_at: '2026-11-23T06:00:00+08:00', end_at: '2026-11-23T09:00:00+08:00', input_tokens: null, output_tokens: null, started_calls: 1, receipted_calls: 1 }, { start_at: '2026-11-23T09:00:00+08:00', end_at: '2026-11-23T12:00:00+08:00', input_tokens: 3000, output_tokens: null, started_calls: 1, receipted_calls: 1 }] } },
    workflows: { status: disabled ? 'unavailable' : 'available', data: { enabled: !disabled, queued: disabled ? null : 2, active_nodes: { queued: disabled ? null : 1, running: disabled ? null : 1, waiting: disabled ? null : 2 }, last_hour: { started: 3, completed: 1, failed: 1, awaiting_human: 1 }, runs: disabled ? [] : [{ run_id: runA, status: activeRun ? 'running' : 'failed', updated_at: now }, { run_id: runB, status: 'completed', updated_at: now }], nodes: [] } },
    activity: { status: 'partial', reason: '仅有部分记录', data: { today_records: 7, total_records: 1000, seven_day_records: [{ date: '2026-11-22', records: 12 }, { date: '2026-11-23', records: 7 }], committed_batch_records: { today_records: 50, total_records: 100, seven_day_records: [{ date: '2026-11-22', records: null }, { date: '2026-11-23', records: 50 }] }, approved_rules: 5, drain: { compression_rate: null, template_windows: 10, unknown_templates: 2 } } },
    approvals: { status: 'partial', reason: '历史缺少可重建的队列快照', data: { pending_groups: 4 + minute, approved_last_hour: 1, history: [{ at: '2026-11-23T00:00:00+08:00', pending_groups: null }, { at: now, pending_groups: 4 + minute }] } },
  };
  const activity = payload.activity.data;
  if (volumeMode === 'batch-only') Object.assign(activity, { today_records: null, total_records: null, seven_day_records: [{ date: '2026-11-22', records: null }, { date: '2026-11-23', records: null }] });
  if (volumeMode === 'ledger-only') activity.committed_batch_records = { today_records: null, total_records: null, seven_day_records: [{ date: '2026-11-22', records: null }, { date: '2026-11-23', records: null }] };
  if (volumeMode === 'partial-batch') activity.committed_batch_records = { today_records: null, total_records: 100, seven_day_records: [{ date: '2026-11-22', records: 44 }, { date: '2026-11-23', records: null }] };
  if (volumeMode === 'zero-batch') activity.committed_batch_records = { today_records: 0, total_records: 0, seven_day_records: [{ date: '2026-11-23', records: 0 }] };
  if (usageComplete) Object.assign(payload.usage.data, { receipted_calls: 3, complete_calls: 3, output_tokens: 600, total_tokens: 3600 });
  if (workflowSummaryMissing) payload.workflows = { status: 'unavailable', data: null, reason: '多专家运行记录暂时不可读取。' };
  return payload;
}
function detail(id) {
  const nodes = id === runB || irregular ? [
    { id: 'evidence', role: 'evidence_specialist', status: 'completed', depends_on: [], downstream: ['rules'], blocked_by: [], attempts: 1 },
    { id: 'rules', role: 'rule_specialist', status: 'completed', depends_on: ['evidence'], downstream: [], blocked_by: [], attempts: 1 },
  ] : [
    { id: 'evidence', role: 'evidence_specialist', status: 'completed', depends_on: [], downstream: ['feature'], blocked_by: [], attempts: 1 },
    { id: 'rules', role: 'rule_specialist', status: 'failed', depends_on: [], downstream: ['feature'], blocked_by: [], attempts: 1, result_code: 'workflow_node_failed' },
    { id: 'feature', role: 'feature_specialist', status: 'pending', depends_on: ['evidence', 'rules'], downstream: [], blocked_by: ['rules'], attempts: 0 },
  ];
  if (id === runA) nodes.forEach(node => { if (budgetExhausted) { node.status = 'pending'; node.attempts = node.id === 'feature' ? budgetAttempts : 0; node.blocked_by = node.id === 'feature' ? ['evidence', 'rules'] : []; node.result_code = null; } else if (node.id === 'feature') node.attempts = featureAttempts; });
  return { run_id: id, status: id === runB ? 'completed' : activeRun ? 'running' : 'failed', updated_at: at(), nodes, evaluator: id === runB ? { passed: 1, failed: 0 } : null, human: id === runB ? { pending: 1, approved: 0, rejected: 0, resolved: 0 } : null, candidate_count: id === runB ? 1 : null };
}
async function verifyPalette(page) {
  const colors = await page.evaluate(() => {
    const style = selector => getComputedStyle(document.querySelector(selector));
    const owner = document.querySelector('.so-page'), probe = document.createElement('span');
    owner.appendChild(probe);
    function token(name) { probe.style.color = 'var(' + name + ')'; return getComputedStyle(probe).color; }
    probe.style.backgroundColor = 'var(--so-accent-soft)';
    const accentSoft = getComputedStyle(probe).backgroundColor;
    const selected = document.querySelector('.ov-main-chain button[aria-pressed=\"true\"]') || document.querySelector('.ov :is(.ov-node,.ov-gate)[aria-pressed=\"true\"]:is([data-tone=\"idle\"],[data-tone=\"running\"])');
    const result = {
      risk: style('.ov-risk').backgroundColor, token: style('.ov-token').backgroundColor,
      kafka: style('.so-toolbar').backgroundColor, riskBorder: style('.ov-risk').borderTopColor,
      kafkaBorder: style('.so-toolbar').borderTopColor, riskText: style('.ov-risk').color,
      tokenText: style('.ov-token').color, heading: style('.so-heading h1').color,
      trend: style('.ov-risk .ov-line.hosts').stroke, input: style('.ov-token-split i.input').backgroundColor,
      accent: token('--so-accent'), accentText: token('--so-accent-text'), link: style('.ov-risk .ov-text-link').color,
      highRisk: style('.ov-risk .ov-line.high_hosts').stroke, issue: token('--so-issue'),
      selected: selected && getComputedStyle(selected).backgroundColor, accentSoft,
    };
    probe.remove(); return result;
  });
  assert.equal(colors.risk, colors.kafka, 'risk and existing Kafka surfaces share a background');
  assert.equal(colors.token, colors.kafka, 'Token and Kafka surfaces share a background');
  assert.equal(colors.riskBorder, colors.kafkaBorder, 'risk and Kafka surfaces share a border');
  assert.equal(colors.riskText, colors.heading, 'risk text matches the console heading');
  assert.equal(colors.tokenText, colors.heading, 'Token text matches the console heading');
  assert.equal(colors.trend, colors.accent, 'risk trend uses the console accent');
  assert.equal(colors.input, colors.accent, 'input Token uses the console accent');
  assert.equal(colors.link, colors.accentText, 'risk link uses the console accent text');
  assert.equal(colors.highRisk, colors.issue, 'high risk uses the console issue color');
  assert.equal(colors.selected, colors.accentSoft, 'neutral selection uses the console soft accent');
}
async function main() {
  const browser = await chromium.launch({ headless: true, ...(process.env.LOGRISK_CHROMIUM_PATH ? { executablePath: process.env.LOGRISK_CHROMIUM_PATH } : {}) });
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce', colorScheme: 'light' });
  const errors = []; page.on('pageerror', error => errors.push(error.message));
  await page.addInitScript(() => {
    const original = window.setInterval.bind(window);
    window.setInterval = (fn, delay, ...args) => {
      if (delay === 30000) window.__tickOverview = fn;
      return original(fn, window.__fastOverviewPoll && delay === 30000 ? 40 : delay, ...args);
    };
    const realFetch = window.fetch.bind(window);
    window.__detailReads = [];
    window.fetch = (url, options) => {
      const match = /\/api\/stream-overview\/workflow-runs\/([^/?]+)/.exec(String(url));
      if (!window.__holdDetails || !match) return realFetch(url, options);
      return new Promise(resolve => {
        // Ignore cancellation in the transport so the component has to drop late replies itself.
        window.__detailReads.push({ id: match[1], signal: options.signal, deliver(status, body) {
          resolve(new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } }));
        } });
      });
    };
  });
  await page.route('**/*', async route => {
    const url = new URL(route.request().url());
    if (url.pathname === '/api/stream-overview') {
      let held = false;
      if (slowMode) { slowReads += 1; held = slowReads === 1; if (held) await new Promise(resolve => { slowRelease = resolve; }); }
      try { return await (fail ? route.fulfill({ status: 503, json: { error: 'fixture-failure' } }) : route.fulfill({ json: noData ? { as_of: at(), usage: { status: 'unavailable', data: null }, workflows: { status: 'unavailable', data: null }, approvals: { status: 'unavailable', data: null } } : overview() })); }
      catch (error) { if (!held) throw error; }
    }
    if (url.pathname.startsWith('/api/stream-overview/workflow-runs/')) return route.fulfill({ json: detail(decodeURIComponent(url.pathname.split('/').pop())) });
    if (url.pathname === '/api/streaming/observability/sources') return route.fulfill({ json: { sources: [], next_cursor: null } });
    if (url.pathname.startsWith('/api/')) return route.fulfill({ json: { items: [], tasks: [], profiles: [], summary: {}, default_model: 'fixture', online: false } });
    const basename = url.pathname.split('/').pop();
    const overrides = { 'index.html': 'frontend/index.html', 'app.js': 'frontend/src/app.js', 'streaming-observability.js': 'frontend/src/streaming-observability.js', 'streaming-observability.css': 'frontend/src/streaming-observability.css', 'stream-overview.js': 'frontend/src/stream-overview.js', 'stream-overview.css': 'frontend/src/stream-overview.css' };
    const file = path.join(root, overrides[basename] || ((url.pathname === '/' || url.pathname === '/streaming') ? 'frontend/index.html' : url.pathname === '/config.js' ? 'frontend/dist/config.js' : 'frontend/dist' + url.pathname));
    const type = file.endsWith('.js') ? 'application/javascript' : file.endsWith('.css') ? 'text/css' : file.endsWith('.png') ? 'image/png' : 'text/html';
    return route.fulfill({ status: 200, contentType: type, body: fs.readFileSync(file) });
  });
  try {
    await page.goto('http://fixture.invalid/');
    await page.locator('.ov-node[data-tone=fault]').waitFor();
    assert.match(await page.locator('.ov-token-value').innerText(), /至少\s*3,600/);
    assert.equal(await page.locator('.ov-token .ov-range').innerText(), '今日已知');
    assert.match(await page.locator('.ov-usage-note').innerText(), /仅计已登记的物理调用.*特征识别与专家规划.*旧 Trace 未纳入/);
    assert.match(await page.locator('.ov-coverage').innerText(), /已登记物理调用回执覆盖/);
    const volume = page.locator('.ov-business [data-volume-source]');
    assert.equal(await volume.getAttribute('data-volume-source'), 'batch');
    assert.match(await volume.locator('.ov-volume-note').innerText(), /已提交流批次 · 未跨任务去重/);
    assert.match(await volume.locator('.ov-volume-line').innerText(), /^50 条\s*累计 100 条$/);
    assert.deepEqual(await volume.locator('.ov-volume-primary .ov-bar-value').allInnerTexts(), ['未知', '50']);
    assert.match(await volume.locator('.ov-batch-details summary').innerText(), /已核验去重收据[\s\S]*今日 7 · 累计 1,000/);
    await volume.locator('.ov-batch-details summary').click();
    assert.deepEqual(await volume.locator('.ov-batch-details .ov-bar-value').allInnerTexts(), ['12', '7']);
    assert.match(await volume.locator('.ov-batch-details p').innerText(), /不相加/);
    await page.getByRole('button', { name: /日志输入/ }).click();
    assert.match(await page.locator('.ov-inspect').innerText(), /今日已提交流批次处理 50 条（未跨任务去重）/);
    volumeMode = 'ledger-only'; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.locator('.ov-business [data-volume-source=verified]').waitFor();
    assert.match(await volume.locator('.ov-volume-note').innerText(), /已核验去重收据 · 账本覆盖/);
    assert.match(await volume.locator('.ov-volume-line').innerText(), /^7 条\s*累计 1,000 条$/);
    assert.deepEqual(await volume.locator('.ov-volume-primary .ov-bar-value').allInnerTexts(), ['12', '7']);
    assert.match(await page.locator('.ov-inspect').innerText(), /今日已核验去重收据 7 条/);
    volumeMode = 'partial-batch'; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.locator('.ov-business [data-volume-source=batch]').waitFor();
    await page.waitForFunction(() => document.querySelector('.ov-volume-primary .ov-bar-value')?.textContent === '44');
    assert.match(await volume.locator('.ov-volume-line').innerText(), /^未知\s*累计 100 条$/);
    assert.deepEqual(await volume.locator('.ov-volume-primary .ov-bar-value').allInnerTexts(), ['44', '未知']);
    volumeMode = 'zero-batch'; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.waitForFunction(() => document.querySelector('.ov-volume-line b')?.textContent.trim() === '0 条');
    assert.equal(await volume.getAttribute('data-volume-source'), 'batch');
    volumeMode = 'batch-only'; usageComplete = true; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.locator('.ov-token .ov-range').getByText('今日已入账', { exact: true }).waitFor();
    assert.match(await volume.locator('.ov-volume-line').innerText(), /^50 条\s*累计 100 条$/);
    assert.match(await volume.locator('.ov-batch-details summary').innerText(), /已核验去重收据[\s\S]*今日 未知 · 累计 未知/);
    assert.doesNotMatch(await page.locator('.ov-token').innerText(), /今日总量/);
    await page.locator('.ov-token-details summary').click();
    assert.match(await page.locator('.ov-token-details').innerText(), /已登记物理调用的完整回执 3 \/ 3/);
    volumeMode = 'both'; usageComplete = false; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.locator('.ov-token .ov-range').getByText('今日已知', { exact: true }).waitFor();
    assert.match(await page.locator('.ov-risk-stats').innerText(), /当前未恢复/);
    await verifyPalette(page);
    assert.deepEqual(await page.locator('.ov-token-column').allInnerTexts(), ['00:00–03:00\n无调用', '03:00–06:00\n缺少回执', '06:00–09:00\n输入/输出未知', '09:00–12:00\n部分未知']);
    assert.match(await page.getByLabel('工作流运行').locator('option:checked').innerText(), /^01234567…cdef · .* · 失败$/);
    assert.equal(await page.getByLabel('工作流运行').getAttribute('title'), runA);
    assert.equal(await page.locator('.ov-line-chart:not(.ov-plain-plot) .ov-line.hosts').getAttribute('d').then(d => (d.match(/ M /g) || []).length), 2, 'risk gaps do not connect');
    assert.equal(await page.locator('.ov-plain-plot circle').count(), 1, 'one real approval point is visible');
    assert.doesNotMatch(await page.locator('.ov-plain-plot path').getAttribute('d'), / L /, 'single point is not a fabricated trend');
    await page.locator('.ov-risk-details summary').click();
    assert.equal(await page.locator('.ov-host-list script').count(), 0);
    assert.match(await page.locator('.ov-host-list').innerText(), /<script>evil<\/script>/);
    await page.getByRole('button', { name: /特征专家/ }).click();
    assert.match(await page.locator('.ov-feature-place .ov-node').innerText(), /未执行 · 上游失败/);
    assert.equal(await page.locator('.ov-feature-place .ov-node').getAttribute('data-tone'), 'idle');
    assert.match(await page.locator('.ov-inspect').innerText(), /证据专家、规则专家/);
    assert.match(await page.locator('.ov-inspect').innerText(), /阻塞节点：规则专家/);
    assert.match(await page.locator('.ov-inspect').innerText(), /节点 ID feature/);
    assert.match(await page.locator('.ov-inspect').innerText(), new RegExp('运行 ID ' + runA));
    const runRead = page.locator('.ov-run-info [role=status]');
    const runSelect = page.getByLabel('工作流运行');
    const releaseDetail = (index, status, body) => page.evaluate(([i, code, response]) => window.__detailReads[i].deliver(code, response), [index, status, body]);
    await page.evaluate(() => { window.__holdDetails = true; window.__tickOverview(); });
    await page.waitForFunction(() => window.__detailReads.length === 1);
    const duringRefresh = await runRead.innerText();
    assert.match(duringRefresh, /^正在刷新 · 上次成功读取 \d{4}/);
    assert.equal(await page.locator('.ov-node[data-tone=fault]').count(), 1);
    assert.match(await page.locator('.ov-inspect').innerText(), new RegExp('运行 ID ' + runA));
    await releaseDetail(0, 503, {});
    await page.getByText('运行详情刷新失败，以下为此前读取的节点状态。').waitFor();
    const retainedRead = duringRefresh.replace('正在刷新 · ', '展示');
    assert.equal(await runRead.innerText(), retainedRead);
    assert.equal(await page.locator('.ov-node[data-tone=fault]').count(), 1);

    workflowSummaryMissing = true;
    await page.evaluate(() => window.__tickOverview());
    await page.getByText('正在查看此前读取的运行列表').waitFor();
    assert.equal(await page.locator('.ov').getAttribute('data-overview-state'), 'ready');
    assert.equal(await runSelect.getAttribute('title'), runA);
    assert.equal(await runRead.innerText(), retainedRead);
    assert.equal(await page.locator('.ov-node[data-tone=fault]').count(), 1);
    assert.match(await page.locator('.ov-rollup').innerText(), /排队 未知 · 运行 未知 · 等待 未知/);
    assert.match(await page.locator('.ov-rollup').innerText(), /启动 未知 · 完成 未知 · 失败 未知 · 等待人工 未知/);
    assert.match(await page.locator('.ov-fleet').innerText(), /待调度工作流\s*未知/);
    assert.match(await page.locator('.ov-fleet').innerText(), /当前待审批\s*4/);
    assert.equal(await page.evaluate(() => window.__detailReads.length), 1);
    workflowSummaryMissing = false;
    await page.evaluate(() => window.__tickOverview());
    await page.waitForFunction(() => window.__detailReads.length === 2);
    await runSelect.selectOption(runB);
    await page.waitForFunction(() => window.__detailReads.length === 3);
    assert.equal(await runSelect.getAttribute('title'), runB);
    assert.equal(await page.locator('.ov-node[data-tone=fault]').count(), 0);
    assert.match(await page.locator('.ov-run-info').innerText(), /正在读取运行详情/);
    assert.deepEqual(await page.evaluate(() => window.__detailReads.slice(1).map(item => [item.id, item.signal.aborted])), [[runA, true], [runB, false]]);
    await releaseDetail(2, 200, detail(runB));
    await page.locator('.ov-expert-graph').waitFor();
    await releaseDetail(1, 200, detail(runA));
    await page.evaluate(() => new Promise(requestAnimationFrame));
    assert.equal(await page.locator('.ov-expert-graph').count(), 1);
    assert.equal(await page.locator('.ov-node[data-tone=fault]').count(), 0);

    await runSelect.selectOption(runA);
    await page.waitForFunction(() => window.__detailReads.length === 4);
    assert.match(await page.locator('.ov-run-info').innerText(), /正在读取运行详情/);
    assert.equal(await page.locator('.ov-node[data-tone=fault]').count(), 0);
    await releaseDetail(3, 200, detail(runA));
    await page.locator('.ov-node[data-tone=fault]').waitFor();
    await page.evaluate(() => window.__tickOverview());
    await page.waitForFunction(() => window.__detailReads.length === 5);
    await releaseDetail(4, 403, {});
    await page.getByText('运行详情读取失败，节点状态未知。').waitFor();
    assert.equal(await page.locator('.ov-node[data-tone=fault]').count(), 0);
    assert.equal(await runRead.count(), 0);
    await page.evaluate(() => { window.__holdDetails = false; window.__tickOverview(); });
    await page.locator('.ov-node[data-tone=fault]').waitFor();
    await page.getByRole('button', { name: /特征专家/ }).click();
    activeRun = true; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.locator('.ov-feature-place .ov-node').getByText('等待执行').waitFor();
    assert.equal(await page.locator('.ov-feature-place .ov-node').getAttribute('data-tone'), 'wait');
    activeRun = false; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.locator('.ov-feature-place .ov-node').getByText('未执行 · 上游失败').waitFor();
    budgetExhausted = true; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.locator('.ov-feature-place .ov-node').getByText('未执行', { exact: true }).waitFor();
    assert.doesNotMatch(await page.locator('.ov-feature-place .ov-node').innerText(), /上游失败|等待执行/);
    assert.match(await page.locator('.ov-inspect').innerText(), /阻塞节点：证据专家、规则专家/);
    assert.equal(await page.locator('.ov-feature-place .ov-node').getAttribute('data-tone'), 'idle');
    budgetAttempts = 1; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.locator('.ov-feature-place .ov-node').getByText('已尝试 · 未继续', { exact: true }).waitFor();
    assert.match(await page.locator('.ov-inspect').innerText(), /先前已尝试，未继续执行/);
    assert.doesNotMatch(await page.locator('.ov-inspect').innerText(), /未执行|上游失败/);
    budgetAttempts = null; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.locator('.ov-feature-place .ov-node').getByText('未继续 · 尝试次数未知', { exact: true }).waitFor();
    assert.match(await page.locator('.ov-inspect').innerText(), /此前是否执行未知/);
    budgetExhausted = false; featureAttempts = 1; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.locator('.ov-feature-place .ov-node').getByText('已尝试 · 上游失败后未继续', { exact: true }).waitFor();
    assert.doesNotMatch(await page.locator('.ov-inspect').innerText(), /未执行/);
    assert.match(await page.locator('.ov-inspect').innerText(), /阻塞节点：规则专家/);
    featureAttempts = 0; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.locator('.ov-feature-place .ov-node').getByText('未执行 · 上游失败').waitFor();
    minute = 1; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.waitForFunction(() => document.querySelectorAll('.ov-plain-plot circle').length === 2);
    assert.match(await page.locator('.ov-plain-plot path').getAttribute('d'), / L /, 'consecutive session samples connect');
    fail = true; minute = 2; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.locator('.ov[data-overview-state=error]').waitFor();
    assert.match(await runRead.innerText(), /^展示上次成功读取 \d{4}/);
    assert.equal(await page.locator('.ov-node[data-tone=fault]').count(), 1);
    await page.getByText('正在查看此前读取的运行列表').waitFor();
    fail = false; minute = 3; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.waitForFunction(() => document.querySelectorAll('.ov-plain-plot circle').length === 3);
    assert.equal((await page.locator('.ov-plain-plot path').getAttribute('d')).match(/ M /g).length, 2, 'failed reads break the approval line');
    slowMode = true; slowReads = 0; slowRelease = null; minute = 4;
    await page.getByRole('button', { name: '刷新快照' }).click();
    await new Promise(resolve => setTimeout(resolve, 100));
    assert.equal(slowReads, 1, 'a pre-hide request is in flight');
    await page.evaluate(() => {
      window.__fixtureHidden = true;
      Object.defineProperty(document, 'hidden', { configurable: true, get: () => window.__fixtureHidden });
      document.dispatchEvent(new Event('visibilitychange'));
    });
    slowRelease(); await new Promise(resolve => setTimeout(resolve, 100));
    assert.equal(await page.locator('.ov-plain-plot circle').count(), 3, 'a late response while hidden is discarded');
    slowMode = false; minute = 5;
    await page.evaluate(() => { window.__fixtureHidden = false; document.dispatchEvent(new Event('visibilitychange')); });
    await page.waitForFunction(() => document.querySelectorAll('.ov-plain-plot circle').length === 4);
    assert.equal((await page.locator('.ov-plain-plot path').getAttribute('d')).match(/ M /g).length, 3, 'hidden interval breaks the approval line');
    minute = 6; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.waitForFunction(() => document.querySelectorAll('.ov-plain-plot circle').length === 5);
    const visiblePath = await page.locator('.ov-plain-plot path').getAttribute('d');
    assert.equal(visiblePath.match(/ M /g).length, 3);
    assert.equal(visiblePath.match(/ L /g).length, 2, 'fresh samples after visibility recovery reconnect');
    slowReads = 0; slowRelease = null;
    await page.getByLabel('工作流运行').selectOption(runB);
    await page.getByText('此运行拓扑与示意不同', { exact: false }).waitFor();
    assert.match(await page.locator('.ov-expert-graph').innerText(), /依赖 证据专家/);
    await page.getByRole('button', { name: /确定性校验/ }).click();
    assert.match(await page.locator('.ov-inspect').innerText(), /通过 1 · 失败 0/);
    await page.getByRole('button', { name: /人工审批.*有候选等待人工/ }).click();
    assert.match(await page.locator('.ov-inspect').innerText(), /待审批 1/);
    disabled = true; minute = 7; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.getByText('示意结构 · 尚无所选运行').waitFor();
    assert.equal(await page.locator('.ov-dag .ov-node').count(), 3);
    assert.match(await page.locator('.ov-dag').innerText(), /暂无运行/);
    assert.doesNotMatch(await page.locator('.ov-dag').innerText(), /已完成|失败/);
    minute = 810; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.waitForFunction(() => document.querySelectorAll('.ov-plain-plot circle').length === 1);
    assert.doesNotMatch(await page.locator('.ov-plain-plot path').getAttribute('d'), / L /, 'Beijing day boundary discards yesterday points');
    await page.screenshot({ path: path.join(out, 'overview-light.png'), fullPage: true });
    await page.getByRole('button', { name: '深色模式' }).click();
    await verifyPalette(page);
    await page.setViewportSize({ width: 390, height: 900 });
    await page.waitForTimeout(400);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, 'mobile fits viewport');
    await page.screenshot({ path: path.join(out, 'overview-dark-mobile.png'), fullPage: true });
    await page.setViewportSize({ width: 320, height: 900 }); await page.waitForTimeout(400);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, '320px fits viewport');
    noData = true; minute = 811; await page.getByRole('button', { name: '刷新快照' }).click();
    await page.getByText('模型用量记录不可用。').waitFor({ state: 'attached' });
    assert.match(await page.locator('.ov-token-details summary').innerText(), /未知 个模型/);
    assert.match(await page.locator('.ov-structure-note').first().innerText(), /状态未知/);
    noData = false; slowMode = true;
    await page.evaluate(() => { window.__fastOverviewPoll = true; });
    await page.getByRole('button', { name: '刷新快照' }).click();
    await new Promise(resolve => setTimeout(resolve, 220));
    assert.equal(slowReads, 1, 'interval ticks must not cancel or duplicate an in-flight overview request');
    assert.equal(typeof slowRelease, 'function'); slowRelease();
    await page.locator('.ov-token .ov-range').getByText('今日已知', { exact: true }).waitFor();
    await new Promise(resolve => setTimeout(resolve, 120));
    assert(slowReads > 1, 'polling resumes after the slow request completes');
    slowMode = false;
    await page.goto('http://fixture.invalid/streaming'); await page.locator('.so-heading h1').waitFor();
    assert.equal(await page.locator('.so-heading h1').innerText(), '流运行总览');
    assert.deepEqual(errors, []);
    console.log('PASS: gap-safe history, workflow topology and gates, retained detail, stale/denied reads, selection races, text safety, dark/mobile');
  } finally { await browser.close(); }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
