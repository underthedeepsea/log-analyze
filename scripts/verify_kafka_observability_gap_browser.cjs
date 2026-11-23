/* All HTTP requests are fulfilled in-memory. No Broker, model, or real service is contacted. */
'use strict';
const fs = require('fs');
const path = require('path');
const assert = require('assert/strict');
const { createRequire } = require('module');
const root = path.resolve(__dirname, '..');
const { chromium } = createRequire(path.join(root, 'package.json'))(process.env.LOGRISK_PLAYWRIGHT_PATH || 'playwright');
const output = path.resolve(process.env.LOGRISK_FRONTEND_EVIDENCE || path.join(root, 'docs/kafka-observability-home-20261119/author/repair-002'));
fs.mkdirSync(output, { recursive: true });
const report = { source: 'Playwright route interception and synthetic responses', network_services: 0, broker_calls: 0, model_calls: 0, cases: {}, checks: {}, page_errors: [], requests: [] };
const first = Date.now() - 20000;
const at = ms => new Date(first + ms).toISOString();
const sources = [
  { source_id:'s1',source_epoch:'epoch-1',environment:'fixture',cluster_id:'cluster-1',topic:'topic-1',consumer_group:'group-1',mode:'continuous_bounded',enabled:true },
  { source_id:'s2',source_epoch:'epoch-2',environment:'fixture',cluster_id:'cluster-2',topic:'topic-2',consumer_group:'group-2',mode:'continuous_bounded',enabled:true },
];
const metric = (value, coverage='complete', reason=null, known=2, total=2) => ({value,coverage,unit:'offset',known_partitions:known,total_partitions:total,reason_code:reason});
let mode='first', scenario='unknown', seq=0;
function sampleTime() {
  if (mode === 'older') return at(-5000);
  if (mode === 'missing') return null;
  if (mode === 'next1' || mode === 'repeat1') return at(15000);
  if (mode === 'next2' || mode === 'repeat2' || mode === 'epoch') return at(30000);
  if (mode === 'partition') return at(45000);
  if (mode === 'failed_diagnostics') return at(60000);
  return at(0);
}
function identity(id) { return id === 's1' && ['epoch','partition','failed_diagnostics'].includes(mode) ? 'epoch-new' : sources.find(s=>s.source_id===id).source_epoch; }
function snapshot(id) {
  const source=sources.find(s=>s.source_id===id), sampled=sampleTime(), epoch=identity(id);
  const served=new Date(Math.max(Date.now(),sampled ? Date.parse(sampled)+1000 : Date.now())).toISOString();
  let gap=metric('20');
  if (mode === 'unknown') gap=metric(null,'unknown','sample_misaligned',0,2);
  else if (mode === 'partial') gap=metric('9','lower_bound','sample_misaligned',1,2);
  else if (mode === 'malformed') gap=metric('not-an-offset','complete',null,2,2);
  else if (mode === 'next1' || mode === 'repeat1') gap=metric('10');
  else if (['next2','repeat2','epoch'].includes(mode)) gap=metric('8');
  else if (mode === 'partition') gap=metric('12','complete',null,3,3);
  else if (mode === 'failed_diagnostics') gap=metric(null,'unknown','timeout',0,0);
  return {schema_version:'streaming_observability_v1',source_id:id,source_epoch:epoch,snapshot_id:id+'-'+seq,source:{...source,source_epoch:epoch},sampled_at:sampled,served_at:served,partition_count:mode==='failed_diagnostics'?0:mode==='partition'?3:2,assignment_mode:'manual',offset_semantics:'next_offset/read_uncommitted',
    state:{source:mode==='failed_diagnostics'?{status:'error',reason_code:'timeout'}:{status:'available'},worker:{status:'running'},progress:{status:'advancing'},result:{status:'covered'}},worker:{phase:'persisting',last_heartbeat_at:sampled},task:{task_id:'fixture-task',status:'running',stage:'persisting'},result:{},
    derived:{undurable_offset_gap:gap,group_offset_gap:metric(null,'unknown','not_observed',0),read_undurable_offset_gap:metric('0'),unconfirmed_commit_offset_gap:metric(null,'unknown','not_observed',0),batch_remaining_offset_gap:metric('0'),durable_records_per_second:{value:null}},next_action:null};
}
function rows(id) {
  if (mode === 'failed_diagnostics') return [];
  const epoch=identity(id), sample=sampleTime() || at(0), isFirst=['first','repeat0','unknown','partial','malformed','older','missing'].includes(mode);
  const end=isFirst?'100':mode==='next1'||mode==='repeat1'?'101':mode==='partition'?'103':'102';
  const durable=isFirst?(mode==='unknown'||mode==='partial'?'91':'90'):mode==='next1'||mode==='repeat1'?'96':mode==='partition'?'99':'98';
  const count=mode==='partition'?3:2;
  const reading=(value,observed)=>({value,availability:'known',reason_code:null,observed_at:observed,epoch});
  return Array.from({length:count},(_,i)=>{
    const durableAt=mode==='unknown'?at(8000):mode==='partial'?(i===0?at(3000):at(8000)):sample;
    return {partition:i,broker_end:reading(end,sample),delivered:reading(durable,durableAt),durable:reading(durable,durableAt),group_committed:{value:null,availability:'unknown',reason_code:'not_observed'},batch_target:reading(durable,durableAt),result_frontier:reading(durable,durableAt)};
  });
}
async function runCase(browser, invalid) {
  scenario=invalid; mode='first'; seq=0;
  const context=await browser.newContext({viewport:{width:1440,height:900},reducedMotion:'reduce'}), page=await context.newPage();
  page.on('pageerror',error=>report.page_errors.push(error.message));
  await page.route('**/*',async route=>{
    const url=new URL(route.request().url()); report.requests.push({method:route.request().method(),path:url.pathname});
    if (url.pathname==='/api/streaming/observability/sources') return route.fulfill({json:{sources,next_cursor:null}});
    const match=url.pathname.match(/^\/api\/streaming\/observability\/sources\/(s[12])(?:\/(partitions|events))?$/);
    if (match) {
      if (!match[2]) return route.fulfill({json:snapshot(match[1])});
      if (match[2]==='partitions') return route.fulfill({json:{snapshot_id:match[1]+'-'+seq,partitions:rows(match[1]),partition_count:mode==='failed_diagnostics'?0:mode==='partition'?3:2,next_cursor:null}});
      return route.fulfill({json:{events:[],next_cursor:null}});
    }
    if (url.pathname.startsWith('/api/')) return route.fulfill({status:404,json:{}});
    const relative=url.pathname==='/config.js'?'config.js':url.pathname.startsWith('/assets/')?url.pathname.slice(1):'index.html';
    const file=path.join(root,'frontend/dist',relative);
    const contentType=file.endsWith('.js')?'application/javascript':file.endsWith('.css')?'text/css':file.endsWith('.png')?'image/png':'text/html';
    return route.fulfill({status:200,contentType,body:fs.readFileSync(file)});
  });
  await page.goto('http://fixture.invalid/');
  await page.locator('.so-page[data-snapshot-id="s1-0"]').waitFor();
  assert.equal(await page.locator('.so-chart-point').count(),1);
  assert.match(await page.locator('.so-current').innerText(),/20/);
  async function refresh(next, accepted=true) {
    mode=next; seq+=1;
    const oldId=await page.locator('.so-page').getAttribute('data-snapshot-id');
    await page.getByRole('button',{name:'刷新快照'}).click();
    if (accepted) await page.locator('.so-page[data-snapshot-id="s1-'+seq+'"]').waitFor();
    else { await page.locator('.so-diagnostic').getByText(/收到较旧采样/).waitFor(); assert.equal(await page.locator('.so-page').getAttribute('data-snapshot-id'),oldId); }
  }
  await refresh('repeat0'); assert.equal(await page.locator('.so-chart-point').count(),1);
  await refresh(invalid,!['older','missing'].includes(invalid));
  if (invalid === 'partial') assert.match(await page.locator('.so-current').innerText(),/至少 9/);
  else assert.match(await page.locator('.so-current').innerText(),/未知/);
  assert.equal(await page.locator('.so-chart-point').count(),1);
  assert.equal(await page.locator('.so-chart-line').count(),0);
  await refresh('next1');
  assert.equal(await page.locator('.so-chart-point').count(),2);
  assert.equal(await page.locator('.so-chart-line').count(),0,'the first valid sample after a gap must not connect backwards');
  assert.match(await page.locator('.so-chart figcaption').innerText(),/采样不连续/);
  await refresh('repeat1');
  assert.equal(await page.locator('.so-chart-point').count(),2,'duplicate after gap must not add a point');
  assert.equal(await page.locator('.so-chart-line').count(),0);
  await refresh('next2');
  assert.equal(await page.locator('.so-chart-point').count(),3);
  const pathValue=await page.locator('.so-chart-line').getAttribute('d');
  assert.equal((pathValue.match(/\bM\b/g)||[]).length,2,'the earlier point must remain a separate segment');
  assert.equal((pathValue.match(/\bL\b/g)||[]).length,1,'only the two later valid points may connect');
  await refresh('repeat2');
  assert.equal(await page.locator('.so-chart-point').count(),3);
  assert.equal(await page.locator('.so-chart-line').getAttribute('d'),pathValue);
  report.cases[scenario]={status:'passed', invalid_snapshot_accepted:!['older','missing'].includes(invalid), points_after_gap:2, connected_segments_after_recovery:1, duplicate_points_added:0, line_path:pathValue};
  if (scenario==='unknown') {
    await page.screenshot({path:path.join(output,'same-time-gap-recovered.png'),fullPage:true});
    await page.getByLabel('观测来源').selectOption('s2');
    await page.locator('.so-page[data-snapshot-id="s2-'+seq+'"]').waitFor();
    assert.equal(await page.locator('.so-chart-point').count(),1);
    assert.equal(await page.locator('.so-chart-line').count(),0);
    await page.getByLabel('观测来源').selectOption('s1');
    await page.locator('.so-page[data-snapshot-id="s1-'+seq+'"]').waitFor();
    assert.equal(await page.locator('.so-chart-point').count(),3);
    assert.equal(await page.locator('.so-chart-line').getAttribute('d'),pathValue);
    await refresh('epoch'); assert.equal(await page.locator('.so-chart-point').count(),1); assert.equal(await page.locator('.so-chart-line').count(),0);
    await refresh('partition'); assert.equal(await page.locator('.so-chart-point').count(),1); assert.equal(await page.locator('.so-chart-line').count(),0);
    report.checks.source_epoch_and_partition_histories_are_isolated=true;
    await refresh('failed_diagnostics');
    assert.equal(await page.locator('.so-chart-point').count(),0);
    assert.equal(await page.locator('.so-table tbody tr').count(),0);
    const exactStamp=await page.evaluate(value=>new Date(value).toLocaleString(),sampleTime());
    assert.doesNotMatch(await page.locator('.so-page').innerText(),/manual/);
    await page.locator('summary[aria-label="任务与运行证据"]').click();
    const evidence=await page.locator('.so-evidence').innerText();
    assert(evidence.includes(exactStamp),'the failed sample timestamp must be preserved');
    assert.match(evidence,/采样时间[\s\S]*分配方式\s+manual/);
    report.checks.failed_sample_preserves_exact_time_and_assignment_in_details=true;
  }
  await context.close();
}
(async()=>{
  let browser;
  try {
    browser=await chromium.launch({headless:true,...(process.env.LOGRISK_CHROMIUM_PATH?{executablePath:process.env.LOGRISK_CHROMIUM_PATH}:{})});
    for(const item of ['unknown','partial','older','missing','malformed']) await runCase(browser,item);
    assert.deepEqual(report.page_errors,[]);
    assert(report.requests.every(request=>request.method==='GET'));
    report.checks.only_read_requests=true; report.status='passed';
  } catch(error) { report.status='failed'; report.error=error.stack||String(error); process.exitCode=1; }
  finally { if(browser) await browser.close(); fs.writeFileSync(path.join(output,'gap-browser-report.json'),JSON.stringify(report,null,2)+'\n'); console.log(JSON.stringify({status:report.status,cases:report.cases,checks:report.checks,page_errors:report.page_errors,error:report.error},null,2)); }
})();
