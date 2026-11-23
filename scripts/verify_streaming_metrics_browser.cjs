/* Real static product entry with intercepted, synthetic API fixtures only. */
'use strict';
const fs = require('fs');
const path = require('path');
const assert = require('assert/strict');
const { createRequire } = require('module');
const root = path.resolve(__dirname, '..');
const { chromium } = createRequire(path.join(root, 'package.json'))(process.env.LOGRISK_PLAYWRIGHT_PATH || 'playwright');
const output = path.resolve(process.env.LOGRISK_FRONTEND_EVIDENCE || path.join(root, 'docs/kafka-observability-home-20261119/metrics-20261120/author'));
fs.mkdirSync(output, { recursive: true });
const report = { fixture: 'synthetic route interception', network_services: 0, broker_calls: 0, model_calls: 0, checks: {}, page_errors: [], requests: [] };
let mode = 'normal', revision = 0, failDetail = false;
const source = { source_id: 'fixture-source', environment: 'fixture', cluster_id: 'cluster', topic: 'topic', consumer_group: 'group', mode: 'continuous_bounded', enabled: true };
const metric = (value, coverage = value == null ? 'unknown' : 'complete', reason = null, known = value == null ? 0 : 1, total = 1) => ({ value: value == null ? null : String(value), unit: 'offset', coverage, known_partitions: known, total_partitions: total, reason_code: reason });
function fixture() {
  const time = Date.now(), epoch = 'epoch-' + (mode === 'second' ? 'normal' : mode);
  const sampled = new Date(time - (['source_stale', 'old_timestamp'].includes(mode) ? 65000 : 1000) + revision).toISOString();
  const served = new Date(time + revision).toISOString();
  let end = 130n, durable = 112n, delivered = 122n, group = 108n, target = 130n;
  let rate = { value: 0.25, reason_code: null }, resultStatus = 'pending', wait = 63, oldest = new Date(time - 63000).toISOString();
  if (mode === 'second') end = 128n;
  if (mode === 'partial') { end = 9223372036854775807n; durable = end - 18n; delivered = durable + 10n; group = 100n; target = end; rate = { value: 0, reason_code: null }; wait = 0; oldest = served; }
  if (mode === 'unknown' || mode === 'unknown_with_wait') { group = null; rate = { value: null, reason_code: 'baseline_required' }; resultStatus = 'unknown'; wait = mode === 'unknown_with_wait' ? 77 : null; oldest = null; }
  if (mode === 'missing_wait_evidence') { resultStatus = 'pending'; wait = 63; oldest = null; }
  if (mode === 'delayed') { resultStatus = 'delayed'; wait = 145; oldest = new Date(time - 145000).toISOString(); }
  if (mode === 'covered' || mode === 'zero') { resultStatus = 'covered'; wait = null; oldest = null; }
  if (mode === 'zero') { end = durable; delivered = durable; group = durable; target = durable; rate = { value: 0, reason_code: null }; }
  const partial = mode === 'partial', count = partial ? 2 : 1;
  const gap = (value, reason) => value == null ? metric(null, 'unknown', reason, 0, count) : metric(value, partial ? 'lower_bound' : 'complete', partial ? 'not_observed' : null, 1, count);
  const read = value => ({ value: value == null ? null : String(value), availability: value == null ? 'unknown' : 'known', reason_code: value == null ? 'not_observed' : null, observed_at: sampled, epoch });
  const row = { partition: 0, broker_end: read(end), delivered: read(delivered), durable: read(durable), group_committed: read(group), batch_target: read(target), result_frontier: read(resultStatus === 'covered' ? durable : durable - 1n) };
  const rows = [row];
  if (partial) rows.push({ partition: 1, broker_end: read(null), delivered: read(null), durable: read(null), group_committed: read(null), batch_target: read(null), result_frontier: read(null) });
  const snapshot = {
    schema_version: 'streaming_observability_v1', source_id: source.source_id, source_epoch: epoch, snapshot_id: 'fixture-' + revision,
    source: { ...source, source_epoch: epoch }, sampled_at: sampled, served_at: served, assignment_mode: 'manual', offset_semantics: 'next_offset/read_uncommitted', partition_count: count,
    state: { source: { status: mode === 'source_stale' ? 'stale' : 'available', reason_code: mode === 'source_stale' ? 'sample_expired' : null }, worker: { status: 'running' }, progress: { status: 'processing', reason_code: 'delivered_not_durable' }, result: { status: resultStatus, reason_code: resultStatus === 'unknown' ? 'coverage_unverified' : ['pending', 'delayed'].includes(resultStatus) ? 'result_frontier_behind' : null } },
    worker: { phase: 'persisting', last_heartbeat_at: sampled, last_poll_returned_at: sampled },
    task: { task_id: 'fixture-task', status: 'running', stage: 'persisting' },
    result: { status: resultStatus, published_at: sampled, coverage_checked_at: served, oldest_uncovered_commit_at: oldest, coverage_target_seconds: 120, wait_seconds: wait },
    derived: { undurable_offset_gap: gap(end - durable), group_offset_gap: gap(group == null ? null : end - group, 'never_committed'), read_undurable_offset_gap: gap(delivered - durable), unconfirmed_commit_offset_gap: gap(group == null ? null : durable - group, 'never_committed'), batch_remaining_offset_gap: gap(target - durable), durable_records_per_second: { ...rate, unit: 'records/s' } },
    next_action: null,
  };
  return { snapshot, rows };
}
async function intercept(page) {
  await page.route('**/*', async route => {
    const request = route.request(), url = new URL(request.url());
    if (url.pathname.startsWith('/api/')) report.requests.push({ method: request.method(), path: url.pathname });
    if (url.pathname === '/api/streaming/observability/sources') return route.fulfill({ json: { sources: [source], next_cursor: null } });
    if (url.pathname === '/api/streaming/observability/sources/fixture-source') return failDetail ? route.fulfill({ status: 503, json: { error: 'fixture failure' } }) : route.fulfill({ json: fixture().snapshot });
    if (url.pathname === '/api/streaming/observability/sources/fixture-source/partitions') return route.fulfill({ json: { snapshot_id: url.searchParams.get('snapshot_id'), partitions: fixture().rows, partition_count: fixture().rows.length, next_cursor: null } });
    if (url.pathname === '/api/streaming/observability/sources/fixture-source/events') return route.fulfill({ json: { events: [], next_cursor: null } });
    if (url.pathname === '/api/streaming/sources') return route.fulfill({ json: { sources: { file: { resume_supported: true }, kafka: { enabled: false, dependency_ready: false } } } });
    if (url.pathname.startsWith('/api/')) return route.fulfill({ json: { items: [], tasks: [], profiles: [], summary: {}, online: false } });
    const relative = url.pathname === '/config.js' ? 'config.js' : url.pathname.startsWith('/assets/') ? url.pathname.slice(1) : 'index.html';
    const file = path.join(root, 'frontend/dist', relative), contentType = file.endsWith('.js') ? 'application/javascript' : file.endsWith('.css') ? 'text/css' : file.endsWith('.png') ? 'image/png' : 'text/html';
    return route.fulfill({ status: 200, contentType, body: fs.readFileSync(file) });
  });
}
(async () => {
  let browser;
  try {
    browser = await chromium.launch({ headless: true, ...(process.env.LOGRISK_CHROMIUM_PATH ? { executablePath: process.env.LOGRISK_CHROMIUM_PATH } : {}) });
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, colorScheme: 'light', reducedMotion: 'reduce' });
    const page = await context.newPage(); page.on('pageerror', error => report.page_errors.push(error.message)); await intercept(page);
    const metricNode = id => page.locator('.so-metrics [data-metric="' + id + '"]');
    async function value(id, expected, note) {
      assert.equal(await metricNode(id).locator('strong').innerText(), expected, id);
      if (note) assert.match(await metricNode(id).innerText(), note, id + ' note');
    }
    async function change(next) {
      mode = next; revision += 1; failDetail = false;
      await page.getByRole('button', { name: '刷新快照' }).click();
      await page.locator('.so-page[data-snapshot-id="fixture-' + revision + '"]').waitFor();
    }
    async function fits(width, theme) {
      const layout = await page.evaluate(() => {
        const rects = [...document.querySelectorAll('.so-metrics dt, .so-metrics dd')].map(e => { const r = e.getBoundingClientRect(); return { left: r.left, right: r.right }; });
        return { viewport: innerWidth, document: document.documentElement.scrollWidth, rects };
      });
      assert(layout.document <= layout.viewport, `${theme} ${width} document width: ${JSON.stringify(layout)}`);
      assert(layout.rects.every(r => r.left >= 0 && r.right <= layout.viewport), `${theme} ${width} metric bounds: ${JSON.stringify(layout)}`);
      (report.layouts ||= {})[theme + '-' + width] = layout;
    }
    await page.goto('http://fixture.invalid/'); await page.locator('.so-page[data-snapshot-id="fixture-0"]').waitFor();
    await value('group-backlog', '22'); await value('durable-rate', '0.25 条/秒'); await value('result-wait', '63 秒', /等待覆盖/);
    assert.match(await page.locator('.so-health').innerText(), /消费进程\s+运行中/);
    assert.equal(await page.locator('#so-trend-title').innerText(), '落库前积压趋势');
    assert.match(await page.locator('.so-stage[data-stage=read]').innerText(), /已读未落库/);
    assert.match(await page.locator('.so-stage[data-stage=commit]').innerText(), /已落库未提交/);
    assert.match(await page.locator('.so-stage[data-stage=batch]').innerText(), /本批待落库/);
    const front = await page.locator('.so-page').innerText(), chart = await page.locator('.so-trend-chart').textContent();
    assert.doesNotMatch(front, /\boffset\b|\bWorker\b|\b[ERDGT]\s*[−-]\s*[ERDGT]\b/i);
    assert.doesNotMatch(chart, /\boffset\b|\b[ERDGT]\s*[−-]\s*[ERDGT]\b/i);
    assert.equal(await page.locator('.so-disclosure[open]').count(), 0);
    report.checks.clear_chinese_metrics_and_no_first_screen_codes = true;
    await change('second'); await page.locator('.so-chart-line').waitFor(); await value('group-backlog', '20');
    await page.screenshot({ path: path.join(output, 'normal-light-1440.png'), fullPage: true });
    await page.getByRole('button', { name: '深色模式' }).click(); await page.screenshot({ path: path.join(output, 'normal-dark-1440.png'), fullPage: true });
    report.checks.real_points_and_original_stage_structure = true;
    await change('partial'); await value('group-backlog', '至少 9223372036854775707', /部分覆盖 · 1\/2 分区/); await value('durable-rate', '0 条/秒'); await value('result-wait', '0 秒', /等待覆盖/);
    report.checks.partial_exact_integer_and_valid_zero = true;
    await page.setViewportSize({ width: 390, height: 900 }); await fits(390, 'dark');
    const summary = page.getByLabel('分区与位点', { exact: true }); await summary.focus(); await page.keyboard.press('Enter');
    assert.match(await page.locator('.so-frontiers').innerText(), /落库前积压 E − D/);
    assert.match(await page.locator('.so-frontiers').innerText(), /落库速率为相邻有效采样/);
    assert.match(await page.locator('.so-frontiers').innerText(), /结果发布等待为最早未覆盖/);
    assert.match(await page.locator('.so-table thead').innerText(), /实时末端（E）/);
    const table = await page.locator('.so-table-wrap').evaluate(e => ({ client: e.clientWidth, scroll: e.scrollWidth })); assert(table.scroll > table.client);
    await fits(390, 'dark-open'); await page.screenshot({ path: path.join(output, 'partial-dark-open-390.png'), fullPage: true });
    await page.getByRole('button', { name: '浅色模式' }).click(); await page.setViewportSize({ width: 320, height: 900 }); await fits(320, 'light-open');
    await page.screenshot({ path: path.join(output, 'partial-light-open-320.png'), fullPage: true });
    report.checks.small_screens_expand_and_keep_table_scroll = true;
    await page.setViewportSize({ width: 1440, height: 900 }); await summary.click();
    await change('unknown'); await value('group-backlog', '未知', /尚无组提交记录/); await value('durable-rate', '未知', /样本不足/); await value('result-wait', '未知', /覆盖尚未核对/);
    await page.screenshot({ path: path.join(output, 'unknown-light-1440.png'), fullPage: true });
    await change('missing_wait_evidence'); await value('result-wait', '未知', /等待时间未知/);
    await change('delayed'); await value('result-wait', '145 秒', /覆盖延迟/); assert.match(await page.locator('.so-diagnostic').innerText(), /结果尚未覆盖已落库数据/);
    await change('unknown_with_wait'); await value('result-wait', '未知');
    await change('covered'); await value('result-wait', '已覆盖');
    failDetail = true; await page.getByRole('button', { name: '刷新快照' }).click(); await page.locator('.so-diagnostic').waitFor(); await value('result-wait', '历史 已覆盖', /当前未知/);
    await change('zero'); await value('group-backlog', '0'); await value('durable-rate', '0 条/秒'); await value('result-wait', '已覆盖');
    report.checks.unknown_evidence_and_actual_zero_are_distinct = true;
    await change('source_stale'); await value('group-backlog', '历史 22', /当前未知/); await value('durable-rate', '历史 0.25 条/秒', /当前未知/); await value('result-wait', '63 秒', /等待覆盖/);
    await change('old_timestamp'); await value('group-backlog', '历史 22', /当前未知/); await value('durable-rate', '历史 0.25 条/秒', /当前未知/); await value('result-wait', '63 秒', /等待覆盖/);
    report.checks.broker_sample_staleness_does_not_stale_fresh_result = true;
    await change('normal'); failDetail = true; await page.getByRole('button', { name: '刷新快照' }).click(); await page.locator('.so-diagnostic').waitFor();
    await value('group-backlog', '历史 22', /当前未知/); await value('durable-rate', '历史 0.25 条/秒', /当前未知/); await value('result-wait', '历史 63 秒', /当前未知/);
    report.checks.failed_snapshot_marks_all_last_readings_historical = true;
    assert(report.requests.every(request => request.method === 'GET')); assert.deepEqual(report.page_errors, []); report.checks.read_only_and_no_page_errors = true;
    report.status = 'passed'; await context.close();
  } catch (error) { report.status = 'failed'; report.error = error.stack || String(error); process.exitCode = 1; }
  finally { if (browser) await browser.close(); fs.writeFileSync(path.join(output, 'metrics-browser-report.json'), JSON.stringify(report, null, 2) + '\n'); console.log(JSON.stringify(report, null, 2)); }
})();
