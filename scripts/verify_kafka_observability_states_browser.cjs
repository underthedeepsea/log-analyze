/* Synthetic loopback fixtures only; no Kafka broker or model is contacted. */
'use strict';
const fs = require('fs');
const path = require('path');
const http = require('http');
const assert = require('assert/strict');
const { createRequire } = require('module');
const root = path.resolve(__dirname, '..');
const { chromium } = createRequire(path.join(root, 'package.json'))(process.env.LOGRISK_PLAYWRIGHT_PATH || 'playwright');
const output = path.resolve(process.env.LOGRISK_FRONTEND_EVIDENCE || path.join(root, 'docs/kafka-observability-home-20261119/author/states'));
fs.mkdirSync(output, { recursive: true });
const report = { source: 'synthetic loopback HTTP fixture', broker_calls: 0, model_calls: 0, checks: {}, page_errors: [] };
const state = { mode: 'waiting', revision: 0 };
const source = { source_id: 'fixture-source', source_epoch: 'fixture-epoch', environment: 'fixture', cluster_id: 'fixture-cluster', topic: 'fixture-topic', consumer_group: 'fixture-group', mode: 'continuous_bounded', enabled: true };
function json(response, body, status = 200) { response.writeHead(status, { 'Content-Type': 'application/json' }); response.end(JSON.stringify(body)); }
function metric(value, unit = 'offset') { return { value, unit, coverage: value == null ? 'unknown' : 'complete', known_partitions: value == null ? 0 : 1, total_partitions: 1, reason_code: value == null ? 'not_observed' : null }; }
function fixture() {
  const now = new Date(), time = new Date(now.getTime() + (state.mode === 'recovery_stale' ? 90000 : 0)).toISOString(), sampled = new Date(now.getTime() - 2000).toISOString();
  const scheduler = { state: 'active', phase: state.mode === 'checking' ? 'checking' : 'awaiting_batch', last_heartbeat_at: sampled, owner_epoch: 'scheduler-fixture' };
  const sample = { schema_version: 'streaming_observability_v1', source_id: source.source_id, source_epoch: source.source_epoch, snapshot_id: 'fixture-snapshot-' + state.revision, source: { ...source, block_reason: null, block_stage: null }, sampled_at: sampled, served_at: time, assignment_mode: 'manual', offset_semantics: 'next_offset/read_uncommitted', partition_count: 1,
    state: { source:{status:'available',reason_code:null}, worker:{status:'waiting',reason_code:'scheduler_waiting'}, progress:{status:'batch_completed',reason_code:'bounded_batch_completed'}, result:{status:'covered',reason_code:null} },
    worker: { scheduler, phase:null, last_heartbeat_at:null, last_poll_returned_at:null, last_durable_commit_at:sampled },
    task: { task_id:'fixture-task', status:'completed',stage:'completed',updated_at:sampled },
    result: { generation:1,published_at:sampled,coverage_checked_at:sampled,coverage_target_seconds:120,oldest_uncovered_commit_at:null },
    derived:{undurable_offset_gap:metric('10'),group_offset_gap:metric(null),unconfirmed_commit_offset_gap:metric(null),read_undurable_offset_gap:metric(null),batch_remaining_offset_gap:metric('0'),durable_records_per_second:{value:null,reason_code:'baseline_required'}}, next_action:null };
  if (state.mode === 'checking') sample.state.worker.reason_code='scheduler_checking';
  if (state.mode === 'scheduler_stale') { sample.state.worker={status:'stale',reason_code:'scheduler_heartbeat_expired'}; scheduler.state='stale'; }
  if (state.mode === 'unknown') { sample.state.worker={status:'unknown',reason_code:'process_unverified'}; sample.worker.scheduler={state:'inactive',phase:null,last_heartbeat_at:null,owner_epoch:null}; sample.task.status='running'; }
  if (state.mode === 'normal') { sample.state.worker={status:'running',reason_code:null}; sample.worker.phase='persisting'; sample.worker.last_heartbeat_at=sampled; sample.worker.last_poll_returned_at=sampled; sample.task.status='running'; }
  if (state.mode === 'commit_failed') { sample.state.progress={status:'commit_failed',reason_code:'external_commit_failed'}; sample.source.block_reason='authorization'; sample.source.block_stage='group_commit'; sample.next_action='inspect_pending_commit'; }
  if (state.mode === 'blocked') { sample.state.progress={status:'blocked',reason_code:'timeout'}; sample.source.block_reason='timeout'; sample.source.block_stage='source_check'; sample.next_action='inspect_worker_phase'; }
  if (state.mode === 'budget') { sample.state.progress={status:'processing',reason_code:'batch_time_budget_exceeded'}; sample.source.block_reason='batch_time_budget_exceeded'; sample.source.block_stage='source_check'; }
  if (state.mode === 'recovering' || state.mode === 'recovery_stale') sample.state.source={status:'recovering',reason_code:'sample_recovery_pending'};
  if (state.mode === 'recovery_stale') sample.derived.unconfirmed_commit_offset_gap=metric('3');
  if (state.mode === 'source_unknown') { sample.source_epoch='fixture-unknown-epoch'; sample.sampled_at=null; sample.state.source={status:'unknown',reason_code:'no_current_sample'}; sample.derived.undurable_offset_gap=metric(null); }
  if (state.mode === 'processing' || state.mode === 'partial') {
    sample.state.worker={status:'running',reason_code:null}; sample.state.progress={status:'processing',reason_code:'delivered_not_durable'};
    sample.worker.phase='persisting'; sample.worker.last_heartbeat_at=sampled; sample.worker.last_poll_returned_at=sampled;
    sample.task.status='running'; sample.task.stage='persisting';
    sample.derived.read_undurable_offset_gap=metric('7'); sample.derived.unconfirmed_commit_offset_gap=metric('3');
    sample.derived.batch_remaining_offset_gap=metric('10'); sample.derived.group_offset_gap=metric('13');
    if (state.mode === 'partial') {
      sample.partition_count=2;
      for (const key of ['undurable_offset_gap','read_undurable_offset_gap','unconfirmed_commit_offset_gap','group_offset_gap']) sample.derived[key]={...sample.derived[key],coverage:'lower_bound',known_partitions:1,total_partitions:2,reason_code:'not_observed'};
      sample.derived.batch_remaining_offset_gap={...sample.derived.batch_remaining_offset_gap,known_partitions:2,total_partitions:2};
    }
  }
  return sample;
}
function reading(value, time) { return {value,availability:value == null ? 'unknown' : 'known',reason_code:value == null ? 'not_observed' : null,observed_at:value == null ? null : time,epoch:'fixture-epoch'}; }
const server = http.createServer((request,response)=>{
  const url = new URL(request.url,'http://127.0.0.1');
  if (url.pathname === '/api/streaming/observability/sources') return json(response,{sources:[source],next_cursor:null});
  if (url.pathname === '/api/streaming/observability/sources/fixture-source') return json(response,fixture());
  if (url.pathname === '/api/streaming/observability/sources/fixture-source/partitions') {
    const time=new Date().toISOString(), processing=state.mode === 'processing' || state.mode === 'partial';
    const row={partition:0,broker_end:reading('110',time),delivered:reading(processing?'107':null,time),durable:reading('100',time),group_committed:reading(processing || state.mode === 'recovery_stale' ? '97' : null,time),batch_target:reading(processing?'110':'100',time),result_frontier:reading('100',time)};
    const rows=state.mode === 'partial' ? [row,{partition:1,broker_end:reading(null,time),delivered:reading(null,time),durable:reading('100',time),group_committed:reading(null,time),batch_target:reading('100',time),result_frontier:reading('100',time)}] : [row];
    return json(response,{snapshot_id:url.searchParams.get('snapshot_id'),partitions:rows,partition_count:rows.length,next_cursor:null});
  }
  if (url.pathname === '/api/streaming/observability/sources/fixture-source/events') return json(response,{events:Array.from({length:30},(_,i)=>({event_type:'fixture_event_'+String(i).padStart(2,'0'),reason_code:null,created_at:'2026-11-17T06:30:00Z',task_id:'fixture-task'})),next_cursor:null});
  if (url.pathname.startsWith('/api/')) return json(response,{},404);
  const file=path.join(root,'frontend/dist',url.pathname.startsWith('/assets/') ? url.pathname.slice(1) : url.pathname === '/config.js' ? 'config.js' : 'index.html');
  const type=file.endsWith('.js')?'application/javascript':file.endsWith('.css')?'text/css':file.endsWith('.png')?'image/png':'text/html';
  try {response.writeHead(200,{'Content-Type':type});response.end(fs.readFileSync(file));} catch (_) {response.writeHead(404);response.end();}
});
(async ()=>{
  let browser;
  try {
    await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
    browser=await chromium.launch({headless:true,...(process.env.LOGRISK_CHROMIUM_PATH ? {executablePath:process.env.LOGRISK_CHROMIUM_PATH} : {})});
    const page=await browser.newPage({viewport:{width:1440,height:1000},reducedMotion:'reduce'});
    page.on('pageerror',error=>report.page_errors.push(error.message));
    await page.goto('http://127.0.0.1:'+server.address().port+'/');
    await page.locator('.so-page[data-snapshot-id="fixture-snapshot-0"]').waitFor();
    async function change(mode) {
      state.mode=mode; state.revision+=1;
      await page.getByRole('button',{name:'刷新快照'}).click();
      await page.locator('.so-page[data-snapshot-id="fixture-snapshot-'+state.revision+'"]').waitFor();
      assert.equal(await page.getByLabel('观测来源').inputValue(),'fixture-source');
    }
    async function disclosure(name, open) {
      const summary=page.locator('summary[aria-label="'+name+'"]');
      if (await summary.locator('..').evaluate(element=>element.open) !== open) await summary.click();
      assert.equal(await summary.locator('..').evaluate(element=>element.open),open);
    }
    assert.equal(await page.locator('.so-disclosure[open]').count(),0);
    assert.match(await page.locator('.so-health').innerText(),/调度器等待下一批/);
    assert.equal(await page.locator('.so-stage[data-stage=read]').getAttribute('data-quality'),'unknown');
    assert.equal(await page.locator('.so-stage[data-stage=read] .so-stage-fill').count(),0);
    assert.equal(await page.locator('.so-stage[data-stage=batch] strong').innerText(),'0');
    assert.equal(await page.locator('.so-stage[data-stage=batch] .so-stage-fill').count(),0);
    await disclosure('任务与运行证据',true);
    assert.match(await page.locator('.so-evidence').innerText(),/阶段 未知 · 心跳 未知/);
    assert.match(await page.locator('.so-evidence').innerText(),/最近拉取返回\s+未知/);
    assert.match(await page.locator('.so-evidence').innerText(),/调度器 运行中 · 等待下一批 · 调度心跳/);
    assert.doesNotMatch(await page.locator('.so-health').innerText(),/消费进程\s+运行中/);
    await change('checking'); assert.match(await page.locator('.so-health').innerText(),/调度器检查下一批/);
    assert.match(await page.locator('.so-evidence').innerText(),/检查下一批/);
    await change('scheduler_stale'); assert.match(await page.locator('.so-health').innerText(),/调度器心跳陈旧/);
    assert.match(await page.locator('.so-evidence').innerText(),/调度器心跳已过期/);
    await change('unknown'); assert.match(await page.locator('.so-diagnostic').innerText(),/任务记录存在，进程未验证/);
    assert.match(await page.locator('.so-evidence').innerText(),/阶段 未知 · 心跳 未知/);
    await disclosure('任务与运行证据',false);
    await page.screenshot({path:path.join(output,'home-unknown-1440-light.png'),fullPage:true});
    await disclosure('任务与运行证据',true);
    await change('normal'); assert.match(await page.locator('.so-health').innerText(),/消费进程\s+运行中/);
    assert.doesNotMatch(await page.locator('.so-evidence').innerText(),/最近拉取返回\s+未知/);
    report.checks.scheduler_is_distinct_from_consumer_worker=true;
    await change('commit_failed');
    assert.match(await page.locator('.so-health').innerText(),/提交失败/);
    assert.match(await page.locator('.so-diagnostic').innerText(),/外部提交失败/);
    assert.match(await page.locator('.so-evidence').innerText(),/权限不足 · 组提交/);
    assert.match(await page.locator('.so-evidence').innerText(),/检查提交事件/);
    await change('blocked');
    assert.match(await page.locator('.so-health').innerText(),/处理受阻/);
    assert.match(await page.locator('.so-evidence').innerText(),/超时 · 来源检查/);
    assert.match(await page.locator('.so-evidence').innerText(),/检查消费进程/);
    await change('budget'); assert.match(await page.locator('.so-diagnostic').innerText(),/本轮时间预算已到，保留原批次目标待续/);
    report.checks.commit_block_and_batch_budget_have_specific_reasons=true;
    await change('recovering');
    assert.match(await page.locator('.so-freshness').innerText(),/采样恢复中/);
    assert.match(await page.locator('.so-evidence').innerText(),/已有一次有效采样，等待下一次确认/);
    assert.match(await page.locator('.so-current').innerText(),/恢复中 · 等待确认/);
    assert.doesNotMatch(await page.locator('.so-current').innerText(),/历史 10/);
    assert.match(await page.locator('.so-evidence').innerText(),/等待下一轮确认恢复/);
    await change('recovery_stale');
    assert.match(await page.locator('.so-current').innerText(),/历史 10/);
    assert.match(await page.locator('.so-diagnostic').innerText(),/采样已过期/);
    assert.match(await page.locator('.so-stage[data-stage=commit]').innerText(),/历史 3[\s\S]*当前未知/);
    assert.equal(await page.locator('.so-stage[data-stage=commit] .so-stage-fill').count(),0);
    assert.match(await page.locator('.so-evidence').innerText(),/检查采样器/);
    report.checks.recovery_waits_for_confirmation_and_old_sample_is_historical=true;
    await change('source_unknown');
    assert.match(await page.locator('.so-current').innerText(),/未知[\s\S]*当前未知/);
    assert.doesNotMatch(await page.locator('.so-current').innerText(),/最后已知/);
    report.checks.absent_sample_has_no_fabricated_last_value=true;
    await change('processing'); await disclosure('任务与运行证据',false);
    const stages=page.locator('.so-stage');
    assert.match(await stages.filter({hasText:'已读未落库'}).innerText(),/7/);
    assert.match(await stages.filter({hasText:'已落库未提交'}).innerText(),/3/);
    assert.equal(await page.locator('.so-stage[data-stage=read] .so-stage-fill').evaluate(element=>element.style.width),'70%');
    assert.equal(await page.locator('.so-stage[data-stage=commit] .so-stage-fill').evaluate(element=>element.style.width),'30%');
    assert.equal(await page.locator('.so-stage[data-stage=batch] .so-stage-fill').evaluate(element=>element.style.width),'100%');
    assert.equal(await page.locator('.so-disclosure[open]').count(),0);
    await page.screenshot({path:path.join(output,'home-processing-1440-light.png'),fullPage:true});
    await page.setViewportSize({width:390,height:1000}); await page.waitForTimeout(400);
    await page.getByRole('button',{name:'深色模式'}).click();
    await page.screenshot({path:path.join(output,'home-processing-390-dark.png'),fullPage:true});
    report.checks.positive_stage_gaps_are_separate_and_scaled=true;
    await change('partial');
    assert.match(await page.locator('.so-current').innerText(),/至少 10[\s\S]*1\/2 分区/);
    assert.match(await page.locator('.so-stage[data-stage=read]').innerText(),/至少 7[\s\S]*1\/2 分区/);
    assert.equal(await page.locator('.so-stage[data-stage=read]').getAttribute('data-quality'),'lower_bound');
    assert.equal(await page.locator('.so-chart-point').count(),0);
    assert.match(await page.locator('.so-chart-empty').innerText(),/暂无可比采样/);
    await page.getByRole('button',{name:'浅色模式'}).click();
    await page.screenshot({path:path.join(output,'home-partial-390-light.png'),fullPage:true});
    await page.setViewportSize({width:1440,height:1000}); await page.waitForTimeout(400);
    await page.getByRole('button',{name:'深色模式'}).click();
    await page.screenshot({path:path.join(output,'home-partial-1440-dark.png'),fullPage:true});
    report.checks.partial_lower_bounds_do_not_create_trend=true;
    await change('waiting'); await disclosure('最近事件',true);
    assert.equal(await page.locator('.so-events li').count(),30);
    for (const width of [1440,768,390]) {
      await page.setViewportSize({width,height:1000}); await page.waitForTimeout(400);
      const sizes=await page.evaluate(()=>{
        const event=document.querySelector('.so-events-scroll');
        const kafkaTop=document.querySelector('.so-kafka-title').getBoundingClientRect().top+window.scrollY;
        return {
          client:event.clientHeight,scroll:event.scrollHeight,documentWidth:document.documentElement.scrollWidth,
          viewportWidth:window.innerWidth,kafkaHeight:document.documentElement.scrollHeight-kafkaTop,
        };
      });
      assert(sizes.client<=320 && sizes.scroll>sizes.client,JSON.stringify(sizes));
      assert(sizes.documentWidth<=sizes.viewportWidth+1,JSON.stringify(sizes));
      if (width===1440) assert(sizes.kafkaHeight<2500,JSON.stringify(sizes));
      report.checks['event_layout_'+width]=sizes;
    }
    await page.getByRole('region',{name:'最近事件，可滚动'}).focus();
    await page.keyboard.press('End'); await page.waitForTimeout(300);
    assert((await page.locator('.so-events-scroll').evaluate(element=>element.scrollTop))>0);
    report.checks.event_region_keyboard_scrolls_all_30=true;
    assert.deepEqual(report.page_errors,[]); report.checks.no_browser_exceptions=true; report.status='passed';
    await browser.close(); browser=null;
  } catch(error) { report.status='failed'; report.error=error.stack || String(error); process.exitCode=1; }
  finally {if(browser) await browser.close(); await new Promise(resolve=>server.close(resolve)); fs.writeFileSync(path.join(output,'states-browser-report.json'),JSON.stringify(report,null,2)+'\n'); console.log(JSON.stringify(report,null,2));}
})();
