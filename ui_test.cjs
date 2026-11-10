// Run with node ui_test.cjs; no browser or npm dependencies required.
const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const vm = require('node:vm');

function element(tag) {
  return {
    tag, attrs: {}, children: [], style: {}, listeners: {},
    setAttribute(key, value) { this.attrs[key] = String(value); },
    addEventListener(type, listener) { this.listeners[type] = listener; },
    replaceChildren() { this.children = []; },
    append(...children) { this.children.push(...children); },
  };
}
const source = readFileSync(`${__dirname}/ui.html`, 'utf8').split('<script>')[1].split('async function start()')[0];
const context = vm.createContext({
  location: { pathname: '/monitor/' },
  document: { createElement: element, createElementNS: (_, tag) => element(tag), createTextNode: text => text },
});
vm.runInContext(source + `
  definition = {series: [{id: 'latency'}], sampleIntervalMs: 1000};
`, context);

for (const width of [270, 350, 660, 1382]) {
  const chart = vm.runInContext(`chart(
    {id: 'latency', type: 'line', series: ['latency']},
    {time: 2000, series: {latency: {history: [[1000, 10], [2000, 20]]}}}, ${width}
  )`, context).children[0];
  assert.equal(chart.attrs.viewBox, `0 0 ${width} 180`);
  const grid = chart.children.filter(node => node.tag === 'line');
  assert.equal(grid.length, 4);
  for (const line of grid) {
    assert.equal(line.attrs.x1, '55');
    assert.equal(line.attrs.x2, String(width - 15));
  }
  assert.equal(chart.children.find(node => node.tag === 'path').attrs.d,
    `M55.00,82.50L${(width - 15).toFixed(2)},20.00`);
  assert.equal(chart.children.filter(node => node.tag === 'text').at(-1).attrs.x, String(width - 15));
}
console.log('Chart geometry fills narrow and wide panels at a fixed 180px height.');

(async () => {
  const script = readFileSync(`${__dirname}/ui.html`, 'utf8').split('<script>')[1].split("window.addEventListener('resize'")[0];
  for (const count of [1, 2]) {
    const nodes = Object.fromEntries(['nav', 'title', 'panels', 'checkpoint'].map(id => [id, element('div')]));
    const dashboards = Array.from({length: count}, (_, i) => ({id: `dashboard-${i}`, title: `Dashboard ${i}`, panels: []}));
    const page = vm.createContext({
      location: {pathname: '/monitor/'},
      document: {createElement: element, getElementById: id => nodes[id]},
      fetch: async () => ({ok: true, json: async () => ({dashboards})}),
      EventSource: class { addEventListener() {} },
    });
    vm.runInContext(script, page);
    await vm.runInContext('start()', page);
    assert.equal(nodes.title.textContent, 'Dashboard 0');
    assert.equal(nodes.nav.children.length, count === 1 ? 0 : count);
    if (count > 1) {
      assert.equal(nodes.nav.children[0].attrs['aria-pressed'], 'true');
      vm.runInContext('latest = {status: {persistence: false}}', page);
      nodes.nav.children[1].listeners.click();
      assert.equal(nodes.title.textContent, 'Dashboard 1');
      assert.equal(vm.runInContext('active.id', page), 'dashboard-1');
      assert.equal(nodes.nav.children[0].attrs['aria-pressed'], 'false');
      assert.equal(nodes.nav.children[1].attrs['aria-pressed'], 'true');
    }
  }
  console.log('Single dashboard has no buttons; multiple dashboards switch correctly.');
})().catch(error => { console.error(error); process.exitCode = 1; });
