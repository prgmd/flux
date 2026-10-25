const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const html = fs.readFileSync(process.argv[2], 'utf8');
const payload = html.match(/<script id="catalog-data" type="application\/json">([\s\S]*?)<\/script>/)[1];
const code = html.match(/<script>\s*([\s\S]*?)<\/script>/)[1];
class Element {
  constructor(tag) { this.tag = tag; this.children = []; this.value = ''; this.listeners = {}; this.attributes = {}; }
  set textContent(value) { this.text = String(value); }
  get textContent() { return (this.text || '') + this.children.map(c => c.textContent).join(' '); }
  append(child) { this.children.push(child); }
  replaceChildren() { this.children = []; this.text = ''; }
  setAttribute(key, value) { this.attributes[key] = value; }
  addEventListener(key, callback) { this.listeners[key] = callback; }
}
const ids = Object.fromEntries(['catalog-data', 'models', 'search', 'detail', 'stats', 'scope', 'footer']
  .map(id => [id, new Element('div')]));
ids['catalog-data'].textContent = payload;
const location = { hash: '' };
const context = vm.createContext({ Node: Element,
  document: { getElementById: id => ids[id], createElement: tag => new Element(tag) },
  location, history: { replaceState: (_state, _title, hash) => { location.hash = hash; } } });
vm.runInContext(code, context, { timeout: 5000 });
assert(ids.detail.textContent.includes('monthly_song_metrics'));
assert(ids.scope.textContent.includes('WARN 2'));
assert(ids.scope.textContent.includes('SKIP 1'));
assert(ids.footer.textContent.includes('정적 카탈로그'));
const originalButtons = ids.models.children.filter(c => c.tag === 'button').length;
ids.search.value = 'no-such-model-123';
ids.search.listeners.input();
assert.equal(ids.models.children.filter(c => c.tag === 'button').length, 0);
ids.search.value = '';
ids.search.listeners.input();
assert.equal(ids.models.children.filter(c => c.tag === 'button').length, originalButtons);
const sourceButton = ids.models.children.find(c => c.tag === 'button' && c.textContent === 'monthly');
sourceButton.onclick();
assert(ids.detail.textContent.includes('고정 입력은 dbt가 생성한 모델이 아니므로'));
assert(location.hash.includes('source.flux_gold.pinned.monthly'));
const modelButton = ids.models.children.find(c => c.tag === 'button' && c.textContent === 'monthly_song_metrics');
modelButton.onclick();
assert(location.hash.includes('model.flux_gold.monthly_song_metrics'));
assert(ids.detail.textContent.includes('직접 연결된 dbt 검사'));
console.log(JSON.stringify({ passed: true, models_and_sources: originalButtons,
  checks: ['initial render', 'warning and skip', 'search', 'source selection', 'model selection'] }));
