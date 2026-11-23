/* All requests are intercepted with synthetic fixtures; no service is contacted. */
'use strict';
const fs = require('fs');
const path = require('path');
const assert = require('assert/strict');
const { createRequire } = require('module');
const root = path.resolve(__dirname, '..');
const { chromium } = createRequire(path.join(root, 'package.json'))(process.env.LOGRISK_PLAYWRIGHT_PATH || 'playwright');
const output = path.resolve(process.env.LOGRISK_FRONTEND_EVIDENCE || path.join(root, 'docs/kafka-observability-home-20261119/style-20261120/repair-002/after'));
const observe = process.argv.includes('--observe');
fs.mkdirSync(output, {recursive:true});
const report={fixture:'synthetic Playwright route interception',network_services:0,broker_calls:0,model_calls:0,observe,checks:{},styles:{},page_errors:[]};
const source={source_id:'fixture-source',source_epoch:'fixture-epoch',environment:'fixture',cluster_id:'cluster',topic:'topic',consumer_group:'group',mode:'continuous_bounded',enabled:true};
const base=Date.now()-16000; let revision=0, failed=false;
const metric=value=>({value:value==null?null:String(value),unit:'offset',coverage:value==null?'unknown':'complete',known_partitions:value==null?0:1,total_partitions:1,reason_code:value==null?'timeout':null});
const sampled=()=>new Date(base+revision*15000).toISOString();
function snapshot(){const time=sampled(), end=revision?130:140, durable=revision?105:100, read=revision?112:108, group=revision?103:96, target=revision?110:103;
 return {schema_version:'streaming_observability_v1',source_id:source.source_id,source_epoch:source.source_epoch,snapshot_id:'fixture-'+revision,source,sampled_at:time,served_at:new Date(Math.max(Date.now(),Date.parse(time)+1000)).toISOString(),partition_count:1,assignment_mode:'manual',offset_semantics:'next_offset/read_uncommitted',state:{source:failed?{status:'error',reason_code:'timeout'}:{status:'available'},worker:{status:'running'},progress:{status:'processing',reason_code:'delivered_not_durable'},result:{status:'covered'}},worker:{phase:'persisting',last_heartbeat_at:time,last_poll_returned_at:time},task:{task_id:'fixture-task',status:'running',stage:'persisting'},result:{published_at:time,coverage_checked_at:time,coverage_target_seconds:120},derived:{undurable_offset_gap:metric(failed?null:end-durable),group_offset_gap:metric(failed?null:end-group),read_undurable_offset_gap:metric(read-durable),unconfirmed_commit_offset_gap:metric(failed?null:durable-group),batch_remaining_offset_gap:metric(target-durable),durable_records_per_second:{value:1}},next_action:failed?'inspect_sampler_error':null};}
function row(){const time=sampled(), end=revision?130:140,durable=revision?105:100,read=revision?112:108,group=revision?103:96,target=revision?110:103;const r=value=>({value:String(value),availability:'known',reason_code:null,observed_at:time,epoch:source.source_epoch});return {partition:0,broker_end:r(end),delivered:r(read),durable:r(durable),group_committed:r(group),batch_target:r(target),result_frontier:r(durable)};}
async function routeAll(page){await page.route('**/*',async route=>{const url=new URL(route.request().url());
 if(url.pathname==='/api/streaming/observability/sources') return route.fulfill({json:{sources:[source],next_cursor:null}});
 if(url.pathname==='/api/streaming/observability/sources/fixture-source') return route.fulfill({json:snapshot()});
 if(url.pathname==='/api/streaming/observability/sources/fixture-source/partitions') return route.fulfill({json:{snapshot_id:'fixture-'+revision,partitions:[row()],partition_count:1,next_cursor:null}});
 if(url.pathname==='/api/streaming/observability/sources/fixture-source/events') return route.fulfill({json:{events:[],next_cursor:null}});
 if(url.pathname==='/api/streaming/sources') return route.fulfill({json:{sources:{file:{resume_supported:true},kafka:{enabled:false,dependency_ready:false}}}});
 if(url.pathname==='/api/ai-harness/model-profiles') return route.fulfill({json:{profiles:[],default_profile_id:''}});
 if(url.pathname.startsWith('/api/')) return route.fulfill({json:{items:[],tasks:[],profiles:[],summary:{},default_model:'fixture',online:false}});
 const relative=url.pathname==='/config.js'?'config.js':url.pathname.startsWith('/assets/')?url.pathname.slice(1):'index.html';const file=path.join(root,'frontend/dist',relative);
 const contentType=file.endsWith('.js')?'application/javascript':file.endsWith('.css')?'text/css':file.endsWith('.png')?'image/png':'text/html';return route.fulfill({status:200,contentType,body:fs.readFileSync(file)});});}
const styles=async(locator)=>locator.evaluate(element=>{const s=getComputedStyle(element);return {color:s.color,background:s.backgroundColor,border:s.borderTopColor,radius:s.borderTopLeftRadius,shadow:s.boxShadow,font:s.fontFamily,size:s.fontSize,weight:s.fontWeight,stroke:s.stroke};});
async function checkOpenDisclosures(page, theme, width) {
 const layout=await page.locator('.so-table-wrap').evaluate(element=>{
  const result={viewport:innerWidth,document:document.documentElement.scrollWidth,client:element.clientWidth,scroll:element.scrollWidth};
  element.scrollLeft=element.scrollWidth;result.scrollLeft=element.scrollLeft;
  const last=element.querySelector('thead th:last-child').getBoundingClientRect(),box=element.getBoundingClientRect();
  result.lastColumnVisible=last.left>=box.left && last.right<=box.right;element.scrollLeft=0;return result;
 });
 (report.disclosure_layouts ||= {})[theme+'-'+width]=layout;
 await page.screenshot({path:path.join(output,'disclosures-'+theme+'-'+width+'.png'),fullPage:true});
 assert(layout.document<=layout.viewport,`${theme} ${width} document overflow: ${JSON.stringify(layout)}`);
 assert(layout.scroll>layout.client,`${theme} ${width} table must have its own horizontal overflow: ${JSON.stringify(layout)}`);
 assert(layout.scrollLeft>0,`${theme} ${width} table must actually scroll: ${JSON.stringify(layout)}`);
 assert(layout.lastColumnVisible,`${theme} ${width} the last partition column must be reachable: ${JSON.stringify(layout)}`);
 assert.equal(await page.locator('.so-table thead th').count(),7,`${theme} ${width} all partition columns remain present`);
}
(async()=>{let browser;try{
 browser=await chromium.launch({headless:true,...(process.env.LOGRISK_CHROMIUM_PATH?{executablePath:process.env.LOGRISK_CHROMIUM_PATH}:{})});
 const context=await browser.newContext({viewport:{width:1440,height:900},colorScheme:'light',reducedMotion:'reduce'}),page=await context.newPage();page.on('pageerror',e=>report.page_errors.push(e.message));await routeAll(page);
 await page.goto('http://fixture.invalid/'); await page.locator('.so-page[data-snapshot-id="fixture-0"]').waitFor();
 revision=1;await page.getByRole('button',{name:'刷新快照'}).click();await page.locator('.so-page[data-snapshot-id="fixture-1"]').waitFor();await page.locator('.so-chart-line').waitFor();await page.mouse.move(0,0);
 report.styles.home={page:await styles(page.locator('.so-page')),heading:await styles(page.locator('.so-heading h1')),panel:await styles(page.locator('.so-trend')),button:await styles(page.getByRole('button',{name:'刷新快照'})),accent:await styles(page.locator('.so-stage-fill').first()),line:await styles(page.locator('.so-chart-line'))};
 report.styles.tokens=await page.evaluate(()=>{const root=getComputedStyle(document.documentElement);const resolve=name=>{const e=document.createElement('i');e.style.color='var('+name+')';document.body.appendChild(e);const result=getComputedStyle(e).color;e.remove();return result;};return {orange:resolve('--orange'),orangeDark:resolve('--orange-dark'),line:resolve('--line'),muted:resolve('--muted'),red:resolve('--red'),font:root.fontFamily};});
 await page.screenshot({path:path.join(output,'home-light-1440.png'),fullPage:true});
 await page.getByRole('button',{name:'深色模式'}).click();report.styles.dark={page:await styles(page.locator('.so-page')),panel:await styles(page.locator('.so-trend')),accent:await styles(page.locator('.so-stage-fill').first())};await page.screenshot({path:path.join(output,'home-dark-1440.png'),fullPage:true});
 await page.setViewportSize({width:390,height:900});await page.waitForTimeout(350);assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));await page.screenshot({path:path.join(output,'home-dark-390.png'),fullPage:true});
 for(const name of ['分区与位点','任务与运行证据','最近事件']) {
  const summary=page.getByLabel(name,{exact:true});await summary.focus();await page.keyboard.press('Enter');
  assert.equal(await summary.evaluate(e=>e.parentElement.open),true,`${name} opens from the keyboard`);
 }
 await checkOpenDisclosures(page,'dark',390);
 await page.getByRole('button',{name:'浅色模式'}).click();await checkOpenDisclosures(page,'light',390);
 await page.setViewportSize({width:320,height:900});await checkOpenDisclosures(page,'light',320);
 await page.getByRole('button',{name:'深色模式'}).click();await checkOpenDisclosures(page,'dark',320);
 report.checks.mobile_open_disclosures_use_internal_table_scroll=true;
 await page.getByRole('button',{name:'浅色模式'}).click();await page.setViewportSize({width:1440,height:900});
 assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'desktop document overflow with open disclosures');
 assert.equal(await page.locator('.so-table thead th').count(),7,'all partition columns remain present on desktop');
 await page.screenshot({path:path.join(output,'disclosures-light-1440.png'),fullPage:true});report.checks.desktop_open_disclosures_no_overflow=true;
 for(const name of ['分区与位点','任务与运行证据','最近事件']) await page.getByLabel(name,{exact:true}).click();
 await page.setViewportSize({width:390,height:900});assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));await page.screenshot({path:path.join(output,'home-light-390.png'),fullPage:true});
 await page.setViewportSize({width:1440,height:900});await page.waitForTimeout(350);
 failed=true;revision=2;await page.getByRole('button',{name:'刷新快照'}).click();await page.locator('.so-page[data-snapshot-id="fixture-2"]').waitFor();await page.locator('.so-diagnostic').waitFor();report.styles.diagnostic=await styles(page.locator('.so-diagnostic'));
 assert.match(await page.locator('.so-diagnostic').innerText(),/采样失败/);assert.equal(await page.locator('.so-stage[data-stage=commit] .so-stage-fill').count(),0);report.checks.failure_keeps_unknown_and_diagnostic=true;
 await page.goto('http://fixture.invalid/queue');await page.locator('.queue-surface').waitFor();report.styles.queue={heading:await styles(page.locator('.page-head h1')),panel:await styles(page.locator('.queue-surface'))};await page.screenshot({path:path.join(output,'queue-light-1440.png'),fullPage:true});
 await page.goto('http://fixture.invalid/streaming-tasks');await page.locator('.streaming-hero .secondary-button').waitFor();await page.mouse.move(0,0);report.styles.adjacent_button=await styles(page.locator('.streaming-hero .secondary-button'));
 if(!observe){const {home,queue,adjacent_button:button,tokens,diagnostic,dark}=report.styles;
  assert.equal(home.heading.color,queue.heading.color);assert.equal(home.heading.font,queue.heading.font);assert.equal(home.heading.size,queue.heading.size);
  for(const key of ['background','border','radius','shadow','color']) assert.equal(home.panel[key],queue.panel[key],`panel ${key}`);
  for(const key of ['background','border','radius','color','font','weight']) assert.equal(home.button[key],button[key],`button ${key}`);
  assert.equal(home.accent.background,tokens.orange);assert.equal(home.line.stroke,tokens.orange);assert.equal(diagnostic.color,tokens.red);
  assert.notEqual(dark.page.background,home.page.background);assert.notEqual(dark.page.color,home.page.color);assert.notEqual(dark.accent.background,home.accent.background);
  report.checks.shared_console_colors_typography_surfaces_and_buttons=true;report.checks.dark_mode_retained=true;}
 assert.deepEqual(report.page_errors,[]);report.checks.mobile_no_document_overflow=true;report.status='passed';await context.close();
 }catch(error){report.status='failed';report.error=error.stack||String(error);process.exitCode=1;}finally{if(browser) await browser.close();fs.writeFileSync(path.join(output,'style-report.json'),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report,null,2));}})();
