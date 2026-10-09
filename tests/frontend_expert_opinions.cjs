const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('frontend/src/app.js', 'utf8');
const start = source.indexOf('  function ExpertOpinionsPanel(');
const end = source.indexOf('  function evidenceSemanticLabel(', start);
const states = [], effects = [], calls = [];
let hookIndex = 0, effectIndex = 0;
const context = {
  React: { Fragment: 'fragment' },
  h: (type, props, ...children) => ({ type, props: props || {}, children: children.flat(Infinity) }),
  timeText: value => value,
  useState(initial) {
    const index = hookIndex++;
    if (!(index in states)) states[index] = initial;
    return [states[index], value => { states[index] = typeof value === 'function' ? value(states[index]) : value; }];
  },
  useEffect(callback, dependencies) {
    const index = effectIndex++, old = effects[index];
    if (!old || dependencies.some((value, i) => value !== old.dependencies[i])) {
      if (old && old.cleanup) old.cleanup();
      effects[index] = { dependencies, callback };
    }
  },
  api: { expertOpinions: (job, candidate) => new Promise((resolve, reject) => calls.push({ job, candidate, resolve, reject })) },
};
vm.createContext(context);
vm.runInContext(source.slice(start, end), context);
function render(candidate) {
  hookIndex = effectIndex = 0;
  return context.ExpertOpinionsPanel({ feature: candidate ? { job_id: 'job', candidate_id: candidate } : null });
}
function flushEffects() {
  for (const effect of effects) if (effect.callback) {
    effect.cleanup = effect.callback();
    effect.callback = null;
  }
}
function nodes(node) { return node && typeof node === 'object' ? [node, ...node.children.flatMap(nodes)] : []; }
const tick = () => new Promise(resolve => setImmediate(resolve));
const data = title => ({ opinions: [{ role_id: 'feature', name: '特征专家', state: 'recorded', conclusion: title,
  basis: '<script>unsafe markup</script>', confirmation: '确认范围', source: 'fixture', records: [] }], overview: title });
(async () => {
  render('a'); flushEffects();
  calls[0].resolve(data('A opinion')); await tick();
  assert.ok(JSON.stringify(render('a')).includes('A opinion'));
  // Before the next effect, the previous candidate must already be hidden.
  assert.equal(JSON.stringify(render('b')).includes('A opinion'), false); flushEffects();
  render('c'); flushEffects();
  calls[1].resolve(data('B stale opinion')); await tick();
  assert.equal(JSON.stringify(render('c')).includes('B stale opinion'), false);
  calls[2].reject(new Error('failed')); await tick();
  const failed = render('c');
  const retry = nodes(failed).find(node => node.type === 'button' && node.children.includes('重新读取'));
  assert.ok(retry); retry.props.onClick(); render('c'); flushEffects();
  calls[3].resolve(data('C opinion')); await tick();
  const tree = render('c');
  assert.ok(JSON.stringify(tree).includes('C opinion'));
  assert.ok(nodes(tree).some(node => node.type === 'p' && node.children.includes('<script>unsafe markup</script>')));
  assert.equal(nodes(tree).some(node => node.props.dangerouslySetInnerHTML), false);
  assert.equal(JSON.stringify(render(null)).includes('C opinion'), false); flushEffects();
  console.log('Expert opinions: switching, stale replies, retry, empty selection and text rendering passed.');
})().catch(error => { console.error(error); process.exitCode = 1; });
