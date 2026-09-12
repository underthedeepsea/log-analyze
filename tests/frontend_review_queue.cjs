const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('frontend/src/app.js', 'utf8');
const start = source.indexOf('  function createReviewSubmissionQueue(');
const end = source.indexOf('  function ReviewGroupList(', start);
const context = { Promise, Object, JSON, crypto: require('node:crypto').webcrypto };
vm.runInNewContext(source.slice(start, end), context);
const tick = () => new Promise(resolve => setImmediate(resolve));
(async () => {
  const requests = [], snapshots = [];
  const queue = context.createReviewSubmissionQueue(entry => new Promise((resolve, reject) => {
    requests.push({ entry, resolve, reject });
  }), state => snapshots.push(state));
  const entry = id => ({ representative: { candidate_id: id }, changes: { status: 'rejected' } });
  assert.equal(queue.submit('a', entry('a')), true);
  assert.equal(queue.submit('a', entry('a')), false);
  queue.submit('b', entry('b'));
  queue.submit('c', entry('c'));
  assert.equal(snapshots.at(-1).c.phase, 'queued');
  assert.equal(queue.hasUnfinished(), true);
  await tick();
  assert.equal(requests.length, 2);
  // A late reply for b must not change a or the newly selected c.
  requests[1].resolve({ candidate_id: 'b' });
  await tick();
  assert.equal(requests.length, 3);
  assert.equal(snapshots.at(-1).b.phase, 'saved');
  assert.equal(snapshots.at(-1).a.phase, 'saving');
  requests[0].reject(new Error('Network response lost'));
  await tick();
  assert.equal(snapshots.at(-1).a.phase, 'error');
  assert.equal(snapshots.at(-1).a.changes.status, 'rejected');
  assert.equal(queue.retry('a'), true);
  assert.equal(queue.retry('a'), false);
  await tick();
  assert.equal(requests[3].entry.representative.candidate_id, 'a');
  assert.equal(requests[3].entry.changes.status, 'rejected');
  assert.ok(requests[3].entry.request_key);
  assert.equal(requests[3].entry.request_key, requests[0].entry.request_key);
  requests[2].resolve({ candidate_id: 'c' });
  requests[3].resolve({ candidate_id: 'a' });
  await tick();
  assert.equal(queue.hasUnfinished(), false);
  assert.equal(Object.values(snapshots.at(-1)).every(item => item.phase === 'saved'), true);
  queue.clearSaved();
  assert.deepEqual(Object.keys(snapshots.at(-1)), []);

  queue.submit('failed', entry('failed'));
  await tick();
  requests.at(-1).reject(new Error('validation failed'));
  await tick();
  assert.equal(queue.editFailed('failed'), true);
  assert.equal(queue.editFailed('failed'), false);
  assert.equal(snapshots.at(-1).failed, undefined);
  queue.submit('confirmed', entry('confirmed'));
  await tick();
  requests.at(-1).reject(new Error('response lost'));
  await tick();
  queue.confirm('confirmed', { status: 'rejected', tags: [] });
  assert.equal(snapshots.at(-1).confirmed.phase, 'saved');
  assert.equal(snapshots.at(-1).confirmed.changes.status, 'rejected');
  assert.equal(snapshots.at(-1).confirmed.verified, true);
  assert.equal(queue.hasUnfinished(), false);

  queue.submit('conflict', entry('conflict'));
  await tick();
  requests.at(-1).reject(Object.assign(new Error('changed'), { status: 409 }));
  await tick();
  assert.equal(snapshots.at(-1).conflict.phase, 'conflict');
  assert.equal(queue.retry('conflict'), false);
  queue.confirm('conflict', { status: 'approved', title: 'Another reviewer', updated_at: 'new' });
  assert.equal(snapshots.at(-1).conflict.phase, 'conflict');
  assert.equal(snapshots.at(-1).conflict.changes.status, 'rejected');
  assert.equal(snapshots.at(-1).conflict.latest.title, 'Another reviewer');
  assert.equal(queue.editFailed('conflict'), true);

  queue.submit('changed-draft', { representative: { candidate_id: 'changed-draft' }, changes: { status: 'approved', title: 'My draft' } });
  await tick();
  requests.at(-1).reject(new Error('response lost'));
  await tick();
  queue.confirm('changed-draft', { status: 'approved', title: 'Different title' });
  assert.equal(snapshots.at(-1)['changed-draft'].phase, 'conflict');
  assert.equal(snapshots.at(-1)['changed-draft'].changes.title, 'My draft');

  const draftStart = source.indexOf('  function reviewDraftFromFeature(');
  const draftEnd = source.indexOf('  function ReviewEditor(', draftStart);
  vm.runInNewContext(source.slice(draftStart, draftEnd), context);
  const next = { title: 'next', summary: 'next summary', source_templates: [{ template_hash: 'new', template: 'new evidence' }] };
  const draft = context.reviewDraftFromFeature(next, { template_hash: 'old', template: 'old evidence' });
  assert.equal(draft.summary.includes('old evidence'), false);
  assert.equal(draft.summary.includes('new evidence'), true);
  assert.equal(draft.reviewer_note.includes('new'), true);

  // Editing a conflict must respect the server's immutable terminal decision.
  const editorEnd = source.indexOf('  function PromptManagement(', draftEnd);
  const editorDraft = { title: 'Retained draft', summary: 'Evidence', importance: 'high', tags: '', reviewer_note: '' };
  let hookIndex = 0;
  Object.assign(context, {
    h: (type, props, ...children) => ({ type, props: props || {}, children: children.flat(Infinity) }),
    React: { Fragment: 'fragment' },
    useState: () => [hookIndex++ === 0 ? editorDraft : false, () => {}],
    useRef: () => ({ current: '' }), useEffect: () => {},
    featureQualityLabel: () => 'Passed', timeText: value => value,
  });
  vm.runInNewContext(source.slice(draftEnd, editorEnd), context);
  function allNodes(node) {
    return node && typeof node === 'object' ? [node, ...node.children.flatMap(allNodes)] : [];
  }
  for (const status of ['pending', 'approved', 'rejected']) {
    hookIndex = 0;
    const tree = context.ReviewEditor({ feature: { candidate_id: 'candidate', status }, selectedTemplate: {} });
    const buttons = allNodes(tree).filter(node => node.type === 'button');
    assert.equal(!!buttons.find(node => node.children.includes('驳回')).props.disabled, status === 'approved');
    assert.equal(!!buttons.find(node => node.children.includes('批准并写入规则库')).props.disabled, status === 'rejected');
    assert.equal(allNodes(tree).find(node => node.type === 'input').props.value, 'Retained draft');
  }
  console.log('Background review queue: bounded concurrency, duplicate suppression, out-of-order replies, retry and draft isolation passed.');
})().catch(error => { console.error(error); process.exitCode = 1; });
