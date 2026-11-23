/* Browser checks use synthetic HTTP fixtures and never contact a Broker or model. */
'use strict';
const fs = require('fs');
const path = require('path');
const http = require('http');
const assert = require('assert/strict');
const { createRequire } = require('module');
const root = path.resolve(__dirname, '..');
const { chromium } = createRequire(path.join(root, 'package.json'))(process.env.LOGRISK_PLAYWRIGHT_PATH || 'playwright');
const output = path.resolve(process.env.LOGRISK_FRONTEND_EVIDENCE || path.join(root, 'docs/kafka-observability-home-20261119/author/browser'));
fs.mkdirSync(output, { recursive: true });
const report = { source: 'synthetic loopback HTTP fixture', broker_calls: 0, model_calls: 0, checks: {}, page_errors: [], requests: [] };
const state = { revision: 0, error: false, old: false, slowA: false, partialB: true, many: false, large: false, initialTime: Date.UTC(2026, 10, 17, 6, 30, 0) };
const sources = [
  { source_id: 'source-a', source_epoch: 'epoch-a', environment: 'test', cluster_id: 'cluster-a', topic: 'topic-a', consumer_group: 'group-a', mode: 'continuous_bounded', enabled: true },
  { source_id: 'source-b', source_epoch: 'epoch-b', environment: 'test', cluster_id: 'cluster-b', topic: 'topic-b', consumer_group: 'group-b', mode: 'bounded_high_water', enabled: false },
];
function json(response, body, status = 200) { if (response.destroyed) return; response.writeHead(status, { 'Content-Type': 'application/json' }); response.end(JSON.stringify(body)); }
function fixtureTime() { return new Date(state.initialTime + state.revision * 15000).toISOString(); }
function reading(value, time, epoch, reason) { return { value: value == null ? null : String(value), availability: value == null ? 'unknown' : 'known', reason_code: reason || null, observed_at: value == null ? null : time, last_success_at: value == null ? null : time, collector_id: 'fixture', epoch, last_changed_at: time }; }
function metric(value, coverage, reason) { return { value: value == null ? null : String(value), unit: 'offset', coverage, known_partitions: coverage === 'complete' ? 2 : coverage === 'lower_bound' ? 1 : 0, total_partitions: 2, reason_code: reason || null }; }
function snapshot(id) {
  const base = sources.find(s => s.source_id === id), time = fixtureTime(), epoch = state.large && id === 'source-a' ? 'epoch-a-large' : base.source_epoch, source = {...base,source_epoch:epoch};
  const partial = id === 'source-b' && state.partialB;
  const large = state.large && id === 'source-a';
  return { schema_version: 'streaming_observability_v1', source_id: id, source_epoch: epoch, snapshot_id: id + '-s' + state.revision, sampled_at: time, served_at: new Date(Date.parse(time) + (state.old ? 90000 : 2000)).toISOString(), source,
    assignment_mode: 'manual', offset_semantics: 'next_offset/read_uncommitted', group_writer_scope: 'exclusive_configured', partition_count: 2,
    state: { source: {status:'available',reason_code:null}, worker:{status:id === 'source-a' ? 'running' : 'completed',reason_code:null}, progress:{status:id === 'source-a' ? 'advancing' : 'batch_completed',reason_code:null}, result:{status:id === 'source-a' ? 'covered' : 'pending',reason_code:null} },
    worker:{phase:id === 'source-a' ? 'persisting' : null,last_heartbeat_at:id === 'source-a' ? time : null,last_poll_returned_at:id === 'source-a' ? time : null,last_durable_commit_at:time}, result:{generation:1,published_at:'2026-11-17T06:20:00Z',coverage_checked_at:time,oldest_uncovered_commit_at:id === 'source-b' ? new Date(Date.parse(time) - 60000).toISOString() : null,coverage_target_seconds:120,wait_seconds:id === 'source-b' ? 60 : null}, task:{task_id:id+'-task',status:id === 'source-a' ? 'running' : 'completed',stage:id === 'source-a' ? 'persisting' : 'completed',updated_at:time}, next_action:null,
    derived:{ undurable_offset_gap:large ? metric('18446744073709551614','complete') : partial ? metric(String(10 + state.revision),'lower_bound','not_observed') : metric(String(20 + 2 * state.revision),'complete'), group_offset_gap:metric(null,'unknown','never_committed'), unconfirmed_commit_offset_gap:metric(null,'unknown','never_committed'), read_undurable_offset_gap:metric(large ? '18446744073709551612' : id === 'source-a' ? '14' : '0','complete'), batch_remaining_offset_gap:metric(large ? '18446744073709551614' : id === 'source-a' ? '6' : '0','complete'), durable_records_per_second:{value:id === 'source-a' ? 1.2 : 0,unit:'records/s',reason_code:null} } };
}
function row(id, partition) {
  const time = fixtureTime(), epoch = state.large && id === 'source-a' ? 'epoch-a-large' : sources.find(s=>s.source_id===id).source_epoch;
  if (state.large && id === 'source-a') return { partition, broker_end:reading('9223372036854775807',time,epoch), delivered:reading('9223372036854775806',time,epoch), durable:reading('0',time,epoch), group_committed:reading(null,time,epoch,'never_committed'), batch_target:reading('9223372036854775807',time,epoch), result_frontier:reading('0',time,epoch) };
  return { partition, broker_end:reading(id === 'source-b' && partition === 1 && state.partialB ? null : String(9007199254741003n + BigInt(state.revision)),time,epoch, id === 'source-b' && partition === 1 && state.partialB ? 'not_observed' : null), delivered:reading(id === 'source-a' ? '9007199254741000' : '9007199254740993',time,epoch), durable:reading('9007199254740993',time,epoch), group_committed:reading(null,time,epoch,'never_committed'), batch_target:reading(id === 'source-a' ? '9007199254740996' : '9007199254740993',time,epoch), result_frontier:reading(id === 'source-a' ? '9007199254740993' : '9007199254740990',time,epoch) };
}
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://127.0.0.1');
  report.requests.push({ method:req.method, path:url.pathname, query:url.search });
  if (url.pathname === '/api/streaming/observability/sources') {
    if (!state.many) return json(res,{sources,next_cursor:null});
    const all=sources.concat(Array.from({length:999},(_,index)=>({...sources[0],source_id:'source-extra-'+String(index).padStart(4,'0')})));
    const start=Number(url.searchParams.get('cursor')||0), end=Math.min(start+100,all.length);
    return json(res,{sources:all.slice(start,end),next_cursor:end<all.length ? String(end) : null});
  }
  const match = url.pathname.match(/^\/api\/streaming\/observability\/sources\/([^/]+)(?:\/(partitions|events))?$/);
  if (match) {
    const id = decodeURIComponent(match[1]), kind = match[2];
    if (state.error && !kind) return json(res,{code:'timeout'},503);
    if (state.slowA && id === 'source-a' && !kind) await new Promise(resolve=>setTimeout(resolve,500));
    if (!kind) return json(res,snapshot(id));
    if (kind === 'events') return json(res,{events:[{event_type:'durable_commit',reason_code:null,created_at:fixtureTime(),task_id:id+'-task'}],next_cursor:null});
    const isMore = url.searchParams.has('cursor');
    return json(res,{snapshot_id:url.searchParams.get('snapshot_id'),partitions:[row(id,isMore ? 1 : 0)],next_cursor:isMore ? null : 'page-2',partition_count:2});
  }
  if (url.pathname === '/api/streaming/tasks') return json(res,{tasks:[{task_id:'fixture-file',status:'failed',source:{kind:'file'},cursor:{value:{}},updated_at:fixtureTime()},{task_id:'fixture-kafka',status:'failed',kafka_batch:true,source:{kind:'kafka'},cursor:{value:{}},updated_at:fixtureTime()}]});
  if (url.pathname === '/api/feature-approvals') { const representative={candidate_id:'fixture-candidate',job_id:'fixture-job',status:'pending',title:'候选标题',summary:'脱敏摘要',importance:'medium',tags:[],source_templates:[],entity:{id:'fixture-entity'},risk_score:40,occurrence_count:1}; return json(res,{items:[{review_key:'fixture-review',title:'候选标题',problem_code:'fixture',candidate_count:1,affected_entity_count:1,occurrence_count:1,representative}],total_groups:1,total_candidates:1,next_cursor:null}); }
  if (url.pathname === '/api/ai-harness/status' || url.pathname === '/api/ollama/status') return json(res,{code:'fixture_unavailable'},503);
  if (url.pathname === '/api/uploads' && req.method === 'POST') return json(res,{upload_id:'fixture-upload',total_chunks:1,chunk_size_bytes:1048576});
  if (/^\/api\/uploads\/fixture-upload\/chunks\/0$/.test(url.pathname)) { req.resume(); return json(res,{progress:1,received_chunks:1,total_chunks:1}); }
  if (url.pathname === '/api/uploads/fixture-upload/complete') return json(res,{});
  if (url.pathname === '/api/inputs/analyze-upload') return json(res,{input_job_id:'fixture-input'});
  if (url.pathname === '/api/input-jobs/fixture-input') return json(res,{status:'completed',stage:'completed'});
  if (url.pathname === '/api/input-jobs/fixture-input/result') return json(res,{result:{risk_entities:[],summary:{total_risk_entities:0}}});
  if (url.pathname === '/api/ai-harness/model-profiles') return json(res,{profiles:[],default_profile_id:''});
  if (url.pathname.startsWith('/api/')) return json(res,{items:[],tasks:[],profiles:[],sources:[],summary:{},default_model:'fixture-no-model',online:false});
  const file = path.join(root,'frontend/dist',url.pathname.startsWith('/assets/') ? url.pathname.slice(1) : url.pathname === '/config.js' ? 'config.js' : 'index.html');
  const type = file.endsWith('.js') ? 'application/javascript' : file.endsWith('.css') ? 'text/css' : file.endsWith('.png') ? 'image/png' : 'text/html';
  try { res.writeHead(200,{'Content-Type':type}); res.end(fs.readFileSync(file)); } catch (_) { res.writeHead(404); res.end(); }
});
async function ready(page, id) { await page.locator('.so-page[data-snapshot-id^="' + id + '-s"]').waitFor(); assert.equal(await page.getByLabel('观测来源').inputValue(),id); }
async function refresh(page) { await page.getByRole('button',{name:'刷新快照'}).click(); await page.waitForTimeout(150); }
(async () => {
  let browser;
  try {
    await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
    const base='http://127.0.0.1:'+server.address().port;
    browser=await chromium.launch({headless:true, ...(process.env.LOGRISK_CHROMIUM_PATH ? {executablePath:process.env.LOGRISK_CHROMIUM_PATH} : {})});
    const context=await browser.newContext({viewport:{width:1440,height:1000},colorScheme:'light',reducedMotion:'reduce'});
    const page=await context.newPage();
    page.on('pageerror', e=>report.page_errors.push(e.message));
    await page.goto(base+'/'); await ready(page,'source-a');
    assert.equal(await page.locator('.so-health > div').count(),2);
    assert.equal(await page.locator('.so-metrics [data-metric]').count(),3);
    assert.equal(await page.locator('.so-visuals section').count(),2);
    assert.equal(await page.locator('.so-disclosure[open]').count(),0);
    assert.doesNotMatch(await page.locator('.so-page').innerText(),/source-a|source-a-task|epoch-a/);
    assert.equal(await page.locator('.so-chart-point').count(),1);
    assert.equal(await page.locator('.so-chart-line').count(),0);
    assert.match(await page.locator('.so-chart figcaption').innerText(),/1 个有效采样点/);
    assert.equal(await page.locator('.so-chart-y').innerText(),'20');
    assert.match(await page.locator('.so-current').innerText(),/20/);
    const readStage=page.locator('.so-stage[data-stage=read]'), commitStage=page.locator('.so-stage[data-stage=commit]');
    assert.match(await readStage.innerText(),/已读未落库[\s\S]*14/);
    assert.equal(await readStage.getAttribute('data-quality'),'complete');
    assert.equal(await readStage.locator('.so-stage-fill').count(),1);
    assert.match(await commitStage.innerText(),/未知[\s\S]*尚无组提交记录/);
    assert.equal(await commitStage.locator('.so-stage-fill').count(),0);
    await page.screenshot({path:path.join(output,'home-normal-1440-light.png'),fullPage:true});
    const frontierSummary=page.locator('summary[aria-label="分区与位点"]');
    await frontierSummary.focus(); await page.keyboard.press('Enter');
    assert.equal(await page.locator('.so-frontiers').evaluate(element=>element.open),true);
    assert.match(await page.locator('.so-table').innerText(),/9007199254741003/);
    assert.match(await page.locator('.so-table').innerText(),/尚无组提交记录/);
    assert.equal(report.requests.filter(r=>r.path==='/api/ollama/status').length,0);
    assert.equal(report.requests.filter(r=>r.path.startsWith('/api/') && r.method!=='GET').length,0);
    report.checks.home_reads_real_endpoint_without_model_or_mutation=true;
    report.checks.exact_offsets_unknown_states_and_single_real_point=true;
    state.slowA=true;
    const detailPath='/api/streaming/observability/sources/source-a';
    const beforeRefresh=report.requests.filter(r=>r.path===detailPath).length;
    await page.getByRole('button',{name:'刷新快照'}).click();
    await page.waitForTimeout(100);
    for (let i=0;i<3;i++) await page.getByRole('button',{name:'刷新快照'}).click();
    await page.waitForTimeout(650);
    assert.equal(report.requests.filter(r=>r.path===detailPath).length,beforeRefresh+1);
    state.slowA=false; report.checks.refresh_does_not_overlap_inflight_cycle=true;
    await page.getByRole('button',{name:'加载更多分区'}).click();
    await page.getByText('已展示 2 / 2 个分区',{exact:false}).waitFor();
    assert(report.requests.some(r=>r.path.endsWith('/partitions') && r.query.includes('snapshot_id=source-a-s0') && r.query.includes('cursor=page-2')));
    report.checks.partition_pagination_pins_snapshot=true;
    await frontierSummary.focus(); await page.keyboard.press('Enter');
    assert.equal(await page.locator('.so-frontiers').evaluate(element=>element.open),false);
    report.checks.details_default_closed_and_keyboard_operable=true;
    state.revision=1; await refresh(page);
    await page.locator('.so-chart-line').waitFor();
    assert.match(await page.locator('.so-chart-line').getAttribute('d'),/L/);
    assert.equal(await page.locator('.so-chart-point').count(),2);
    assert.match(await page.locator('.so-chart-y').innerText(),/22[\s\S]*20/);
    state.error=true; await refresh(page); await page.locator('.so-diagnostic').getByText(/当前状态未知/).waitFor();
    state.error=false; state.revision=2; await refresh(page);
    const chartPath=await page.locator('.so-chart-line').getAttribute('d');
    assert((chartPath.match(/M/g)||[]).length >= 2, 'failure creates a chart gap');
    report.checks.only_actual_samples_and_failure_gap=true;
    await page.getByLabel('观测来源').selectOption('source-b'); await ready(page,'source-b');
    assert.match(await page.locator('.so-current').innerText(),/至少 12/);
    assert.match(await page.locator('.so-current').innerText(),/1\/2 分区/);
    assert.equal(await page.locator('.so-chart-line').count(),0);
    assert.match(await page.locator('.so-chart-empty').innerText(),/暂无可比采样/);
    assert.match(await page.locator('.so-health').innerText(),/本批已完成/);
    assert.equal(await page.locator('.so-stage[data-stage=read] strong').innerText(),'0');
    assert.equal(await page.locator('.so-stage[data-stage=read] .so-stage-fill').count(),0);
    assert.equal(await page.locator('.so-stage[data-stage=commit] .so-stage-fill').count(),0);
    report.checks.zero_and_unknown_never_render_positive_bars=true;
    report.checks.partial_coverage_and_source_history_isolation=true;
    state.slowA=true; await page.getByLabel('观测来源').selectOption('source-a');
    await page.waitForTimeout(70); await page.getByLabel('观测来源').selectOption('source-b'); await ready(page,'source-b');
    await page.waitForTimeout(650);
    assert.equal(await page.getByLabel('观测来源').inputValue(),'source-b');
    assert.match(await page.getByLabel('观测来源').locator('option:checked').innerText(),/topic-b/);
    report.checks.late_source_response_cannot_override_selection=true;
    state.slowA=false; state.old=true; state.revision=3; await refresh(page);
    assert.match(await page.locator('.so-freshness').innerText(),/当前未知/);
    assert.match(await page.locator('.so-current').innerText(),/历史 至少 13/);
    report.checks.stale_sample_is_historical_not_current=true;
    state.old=false; state.revision=4; await refresh(page);
    state.revision=5;
    await page.waitForResponse(response=>response.url().endsWith('/api/streaming/observability/sources/source-b'),{timeout:18000});
    await page.getByText('至少 15',{exact:false}).first().waitFor();
    report.checks.automatic_poll_reads_new_snapshot=true;
    let count=report.requests.length;
    await page.evaluate(()=>{Object.defineProperty(document,'hidden',{configurable:true,value:true}); document.dispatchEvent(new Event('visibilitychange'));});
    await page.waitForTimeout(16000); assert.equal(report.requests.length,count);
    await page.evaluate(()=>{Object.defineProperty(document,'hidden',{configurable:true,value:false}); document.dispatchEvent(new Event('visibilitychange'));});
    await page.waitForFunction(previous=>window.performance.getEntriesByType('resource').filter(e=>e.name.includes('/api/streaming/observability/')).length>previous, (await page.evaluate(()=>performance.getEntriesByType('resource').filter(e=>e.name.includes('/api/streaming/observability/')).length)) - 1).catch(()=>{});
    await page.waitForTimeout(250); assert(report.requests.length>count);
    report.checks.hidden_pauses_and_return_refreshes=true;
    for (const width of [390,768,1440]) {
      await page.setViewportSize({width,height:900});
      await page.waitForTimeout(400);
      for (const dark of [false,true]) {
        const button=page.getByRole('button',{name:dark?'深色模式':'浅色模式'});
        if (await button.count()) await button.click();
        const dims=await page.evaluate(()=>({scroll:document.documentElement.scrollWidth,client:document.documentElement.clientWidth}));
        assert(dims.scroll<=dims.client+1,`page overflow at ${width}, dark=${dark}: ${JSON.stringify(dims)}`);
        await page.screenshot({path:path.join(output,`home-${width}-${dark?'dark':'light'}.png`),fullPage:true});
      }
    }
    report.checks.responsive_light_dark_no_document_overflow=true;
    await page.getByRole('button',{name:'刷新快照'}).focus();
    await page.keyboard.press('Enter');
    await page.waitForTimeout(150);
    assert.equal(await page.evaluate(()=>document.activeElement.textContent),'刷新快照');
    report.checks.keyboard_refresh_preserves_focus=true;
    state.large=true; state.revision=6;
    await page.setViewportSize({width:390,height:900}); await page.waitForTimeout(400);
    await page.getByLabel('观测来源').selectOption('source-a'); await ready(page,'source-a');
    await page.locator('.so-page[data-snapshot-id="source-a-s6"]').waitFor();
    assert.equal(await page.locator('.so-chart-y').innerText(),'18446744073709551614');
    const axis=await page.evaluate(()=>{const label=document.querySelector('.so-chart-y span'), box=document.querySelector('.so-chart-y'), plot=document.querySelector('.so-trend-chart');return {label:label.getBoundingClientRect().toJSON(),box:box.getBoundingClientRect().toJSON(),plot:plot.getBoundingClientRect().toJSON(),whitespace:getComputedStyle(label).whiteSpace,documentWidth:document.documentElement.scrollWidth,viewportWidth:window.innerWidth};});
    assert.equal(axis.whitespace,'nowrap'); assert(axis.box.width>100 && axis.label.height<22 && axis.label.right<=axis.plot.left && axis.plot.width>80,JSON.stringify(axis));
    const headline=await page.evaluate(()=>{const title=document.querySelector('#so-trend-title').getBoundingClientRect(), value=document.querySelector('.so-current strong').getBoundingClientRect();return {titleHeight:title.height,titleBottom:title.bottom,valueTop:value.top,valueHeight:value.height};});
    assert(headline.titleHeight<24 && headline.valueTop>=headline.titleBottom && headline.valueHeight<25,JSON.stringify(headline));
    assert(axis.documentWidth<=axis.viewportWidth+1,JSON.stringify(axis));
    const light=page.getByRole('button',{name:'浅色模式'}); if(await light.count()) await light.click();
    await page.screenshot({path:path.join(output,'home-large-axis-390-light.png'),fullPage:true});
    await page.getByRole('button',{name:'深色模式'}).click();
    await page.screenshot({path:path.join(output,'home-large-axis-390-dark.png'),fullPage:true});
    report.checks.large_offset_axis_is_exact_single_line_and_readable=axis;
    await page.getByRole('button',{name:'打开菜单'}).click();
    assert.equal(await page.getByRole('button',{name:'文件上传 / 离线分析 · 特征总览'}).isVisible(),true);
    await page.getByRole('button',{name:'关闭菜单'}).first().click();
    report.checks.mobile_navigation_remains_available=true;
    state.large=false; await page.setViewportSize({width:1440,height:1000}); await page.waitForTimeout(400);
    await page.goto(base+'/streaming'); await page.getByRole('heading',{name:'流运行总览'}).waitFor();
    await page.goto(base+'/offline-analysis'); await page.locator('.upload-panel').waitFor();
    await page.locator('input[type=file]').first().setInputFiles({name:'fixture.log',mimeType:'text/plain',buffer:Buffer.from('synthetic log\n')});
    await page.getByText('fixture.log',{exact:false}).first().waitFor();
    assert(report.requests.some(r=>r.path==='/api/inputs/analyze-upload' && r.method==='POST'));
    await page.goto(base+'/streaming-tasks'); await page.getByText('流式处理',{exact:false}).first().waitFor();
    const kafkaTask=page.locator('.streaming-task-table article').filter({hasText:'fixture-kafka'});
    const fileTask=page.locator('.streaming-task-table article').filter({hasText:'fixture-file'});
    await kafkaTask.getByText('连续批次请通过受控 CLI 恢复').waitFor();
    assert.equal(await kafkaTask.getByRole('button',{name:'从 Checkpoint 恢复'}).count(),0);
    assert.equal(await fileTask.getByRole('button',{name:'从 Checkpoint 恢复'}).count(),1);
    report.checks.direct_routes_offline_chunk_upload_and_control_boundary=true;
    await page.goto(base+'/review'); await page.getByLabel('特征标题').fill('尚未保存的审批草稿');
    let guarded=false; page.once('dialog',async dialog=>{guarded=true; await dialog.dismiss();});
    await page.getByRole('button',{name:'流运行总览'}).click();
    assert(guarded); assert.equal(new URL(page.url()).pathname,'/review');
    assert.equal(await page.getByLabel('特征标题').inputValue(),'尚未保存的审批草稿');
    report.checks.dirty_approval_draft_blocks_navigation=true;
    state.many=true;
    const listing=await context.newPage(); listing.on('pageerror', e=>report.page_errors.push(e.message));
    await listing.goto(base+'/'); await listing.getByRole('button',{name:'加载更多来源'}).waitFor();
    assert.equal(await listing.locator('.so-toolbar select option').count(),1000);
    await listing.getByRole('button',{name:'加载更多来源'}).click();
    await listing.waitForFunction(()=>document.querySelectorAll('.so-toolbar select option').length===1001);
    assert.match(await listing.locator('.so-toolbar select option').first().innerText(),/来源 1$/);
    assert.match(await listing.locator('.so-toolbar select option').last().innerText(),/来源 1001$/);
    report.checks.paged_source_list_can_load_beyond_initial_budget=true;
    await listing.close();
    assert.deepEqual(report.page_errors,[]);
    await context.close();
    report.checks.no_browser_exceptions=true;
    fs.writeFileSync(path.join(output,'browser-report.json'),JSON.stringify(report,null,2)+'\n');
    process.stdout.write(JSON.stringify(report.checks,null,2)+'\n');
  } catch(error) {
    report.error=error.stack || String(error); fs.writeFileSync(path.join(output,'browser-report.json'),JSON.stringify(report,null,2)+'\n');
    process.stderr.write(report.error+'\n'); process.exitCode=1;
  } finally { if(browser) await browser.close(); await new Promise(resolve=>server.close(resolve)); }
})();
