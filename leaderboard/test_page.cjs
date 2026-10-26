// Run from the repository root: node leaderboard/test_page.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {spawnSync} = require('node:child_process');
const vm = require('node:vm');

// Only the DOM operations used by the page; execute its real script unchanged.
class Element {
    constructor(tag, document) {
        this.tag = tag; this.ownerDocument = document; this.children = []; this.style = {setProperty(name, value) { this[name] = String(value); }};
        this.attributes = {}; this.listeners = {}; this.hidden = false;
    }
    append(...children) { this.children.push(...children); }
    replaceChildren(...children) { this.children = children; }
    set textContent(text) { this.children = [String(text)]; }
    get textContent() { return this.children.map(c => typeof c === 'string' ? c : c.textContent).join(''); }
    setAttribute(name, value) {
        this.attributes[name] = String(value);
        if (name === 'id') this.id = value;
        if (name === 'class') this.className = value;
        if (name === 'hidden') this.hidden = true;
    }
    getAttribute(name) { return this.attributes[name] ?? null; }
    get classList() {
        const names = () => (this.className ?? '').split(' ').filter(Boolean), keep = list => { this.className = [...new Set(list)].join(' '); };
        return {add: name => keep([...names(), name]), remove: name => keep(names().filter(n => n !== name)), contains: name => names().includes(name),
            toggle: (name, on = !names().includes(name)) => { keep(on ? [...names(), name] : names().filter(n => n !== name)); return on; }};
    }
    removeAttribute(name) { delete this.attributes[name]; }
    matches(selector) { return selector === ':popover-open' && Boolean(this.popoverOpen); }
    showPopover() { this.popoverOpen = true; }
    hidePopover() { this.popoverOpen = false; }
    showModal() { this.open = true; }
    close() { this.open = false; for (const callback of this.listeners.close ?? []) callback(); }
    closest() { return null; }
    remove() { this.removed = true; }
    getBoundingClientRect() { return {left: 20, right: 44, top: 20, bottom: 44, width: 344, height: 310}; }
    scrollIntoView() { this.scrolledIntoView = (this.scrolledIntoView ?? 0) + 1; }
    addEventListener(type, callback) { (this.listeners[type] ?? (this.listeners[type] = [])).push(callback); }
    click() { const event = {target: this, defaultPrevented: false, preventDefault() { this.defaultPrevented = true; }}; for (const callback of this.listeners.click ?? []) callback(event); return event; }
    change() { for (const callback of this.listeners.change ?? []) callback(); }
    focus() { this.ownerDocument.activeElement = this; }
    child(tag) { const node = new Element(tag, this.ownerDocument); this.append(node); return node; }
    insertRow() { return this.child('tr'); }
    insertCell() { return this.child('td'); }
    createTHead() { return this.child('thead'); }
    createTBody() { return this.child('tbody'); }
    // A 2D context that records every drawing call; `document.noCanvas` is a browser without Canvas.
    getContext(type) {
        if (this.ownerDocument.throwCanvas) throw new Error('getContext is not available');
        if (this.tag !== 'canvas' || type !== '2d' || this.ownerDocument.noCanvas) return null;
        if (!this.ctx) {
            const target = {ops: [], measureText: text => ({width: String(text).length * 6.5})};
            this.ctx = new Proxy(target, {get: (t, name) => name in t ? t[name] : (...args) => { t.ops.push([name, args, {font: t.font, align: t.textAlign, lineWidth: t.lineWidth}]); }, set: (t, name, value) => { t[name] = value; return true; }});
        }
        return this.ctx;
    }
}
const descendants = node => [node, ...node.children.flatMap(c => typeof c === 'string' ? [] : descendants(c))];
const classNodes = (root, name) => descendants(root).filter(e => (e.className ?? '').split(' ').includes(name));
const tableScores = page => page.byClass('summary').map(table => table.children[1].children.map(row => classNodes(row, 'score-value').map(e => e.textContent)));
// A shaded cell carries its strength; the stylesheet turns it into the --heat color at that strength.
const heatmapColors = page => classNodes(page.el('breakdowns'), 'heatmap').flatMap(table => descendants(table).filter(e => e.tag === 'td').map(e => e.style['--strength']));
const suite = JSON.parse(fs.readFileSync(`${__dirname}/suite.json`, 'utf8'));
const html = fs.readFileSync(`${__dirname}/index.html`, 'utf8');
const css = fs.readFileSync(`${__dirname}/styles.css`, 'utf8');
// The page's scripts, in the order its script tags load them.
const scripts = [...html.matchAll(/<script src="(js\/[\w-]+\.js)"><\/script>/g)].map(([, src]) => fs.readFileSync(`${__dirname}/${src}`, 'utf8'));
assert.ok(scripts.length > 0 && !/<script>/.test(html), 'The page has no inline script');
const script = scripts.join('\n');  // source checks read every script at once
// The :root tokens as getComputedStyle reports them: every var() reference already substituted.
const rootTokens = Object.fromEntries([...css.match(/:root \{([\s\S]*?)\n\}/)[1].replace(/\/\*[\s\S]*?\*\//g, '').matchAll(/(--[\w-]+):\s*([^;]+);/g)].map(([, name, value]) => [name, value.trim()]));
const computed = name => (rootTokens[name] ?? '').replace(/var\((--[\w-]+)\)/g, (_, inner) => computed(inner));
assert.match(html.match(/<main\b[^>]*>\s*<section\b([^>]*)>/)?.[1] ?? '', /\bid="leaderboard"/,
    'The leaderboard is the first main section, not a marketing hero');
assert.match(html, /<h1\b[^>]*\bid="leaderboard-title"[^>]*>Leaderboard<\/h1>/);
assert.doesNotMatch(html, /branch-visual|<section\b[^>]*class="hero\b|class="eyebrow">Database branching/);
const entry = (system, value, tags) => ({
    system, machine: 'test', hosted: 'no', date: '2026-09-30', source: 'test.json', commit: '0'.repeat(40), tags,
    result: suite.rows.map(() => [value, value, value]), load_time: value, provision_time: value,
});
// Observers and frames never fire on their own: a test triggers them, so the scene's lifecycle is checked step by step.
class FakeObserver {
    constructor(callback, options, registry) { this.callback = callback; this.options = options; this.targets = []; this.disconnected = false; registry.push(this); }
    observe(target) { this.targets.push(target); }
    disconnect() { this.disconnected = true; }
}
function mount(data = [], excluded = [], macro = null, hash = '', phase = 'local', {reduced = false, canvas = true, throwing = false, width = 900} = {}) {
    const elements = new Map(), timers = [], listeners = {}, frames = [], observers = {intersection: [], resize: []}, clock = {now: 1000};
    const location = {hash};  // the page writes its own state with history.replaceState, which updates the hash without an entry
    const media = {matches: reduced, listeners: [], addEventListener(type, callback) { this.listeners.push(callback); }, removeEventListener(type, callback) { this.listeners = this.listeners.filter(c => c !== callback); },
        change(matches) { this.matches = matches; for (const callback of [...this.listeners]) callback({matches}); }};
    const document = {
        createElement: tag => new Element(tag, document),
        createElementNS: (_, tag) => new Element(tag, document),
        addEventListener(type, callback) { (listeners[type] ?? (listeners[type] = [])).push(callback); },
        removeEventListener(type, callback) { listeners[type] = (listeners[type] ?? []).filter(c => c !== callback); },
        dispatch(type) { for (const callback of listeners[type] ?? []) callback(); }, hidden: false, noCanvas: !canvas, throwCanvas: throwing,
        getElementById: id => elements.get(id) ?? [...elements.values()]
            .flatMap(descendants).find(e => e.id === id) ?? null,
    };
    // A missing static ID must fail, rather than silently inventing its element.
    for (const [, tag, attrs] of html.matchAll(/<([a-z][\w-]*)\b([^>]*)>/gi)) {
        const id = attrs.match(/\bid="([^"]+)"/)?.[1];
        if (!id) continue;
        assert.equal(elements.has(id), false, `Duplicate static ID: ${id}`);
        const element = new Element(tag, document);
        for (const [, name, value] of attrs.matchAll(/([\w:-]+)(?:="([^"]*)")?/g)) element.setAttribute(name, value ?? '');
        elements.set(id, element);
    }
    elements.get('workflow-scene').clientWidth = width;
    document.body = new Element('body', document); document.documentElement = new Element('html', document);
    const setTimeout = (callback, ms) => timers.push({callback, ms}), clearTimeout = id => { if (id) timers[id - 1] = null; };
    const context = vm.createContext({document, suite, data, excluded, setTimeout, clearTimeout, site: {phase},
        ...(macro ? {macroSuite: macro.suite, macroCampaigns: macro.campaigns, macroExcluded: macro.excluded} : {}),
        requestAnimationFrame: callback => frames.push(callback), cancelAnimationFrame: id => { frames[id - 1] = null; },
        performance: {now: () => clock.now}, getComputedStyle: () => ({getPropertyValue: computed}),
        IntersectionObserver: class extends FakeObserver { constructor(callback, options) { super(callback, options, observers.intersection); } },
        ResizeObserver: class extends FakeObserver { constructor(callback, options) { super(callback, options, observers.resize); } },
        window: {addEventListener() {}, location, history: {replaceState: (state, title, url) => { location.hash = url; }}, innerWidth: 1440, innerHeight: 1000, devicePixelRatio: 2, matchMedia: query => query.includes('prefers-reduced-motion') ? media : {matches: false, addEventListener() {}, removeEventListener() {}}}});
    const run = source => vm.runInContext(source, context);
    const byClass = name => classNodes(elements.get('groups'), name);
    for (const script of scripts) run(script);
    const pending = () => timers.filter(Boolean);
    const fire = () => { const [timer] = pending(); assert.ok(timer, 'a timer is pending'); timers[timers.indexOf(timer)] = null; timer.callback(); };
    const pendingFrames = () => frames.filter(Boolean);
    const frame = ms => { const [callback] = pendingFrames(); assert.ok(callback, 'a frame is pending'); frames[frames.indexOf(callback)] = null; clock.now += ms; callback(clock.now); };
    return {document, elements, run, byClass, pending, fire, pendingFrames, frame, observers, clock, listeners, media, el: id => elements.get(id) ?? document.getElementById(id)};
}
const data = [entry('Tagless', 0.03, []), entry('Measured', 0.06, ['sql'])];
data[0].date = '2026-09-29';
const page = mount(data);
const {document, elements, run, byClass} = page;
const click = id => { const element = document.getElementById(id); assert.ok(element, id); element.click(); };
const select = (page, id, value) => { const element = page.el(id); element.value = value; element.change(); assert.equal(page.document.activeElement, element, `${id} keeps focus after a change`); };
const check = (page, id) => { const box = page.document.getElementById(id); assert.ok(box, id); box.checked = !box.checked; box.change(); assert.equal(page.document.activeElement.id, id, 'A rebuilt checkbox keeps focus'); };
const firstText = th => typeof th.children[0] === 'string' ? th.children[0] : th.children[0].textContent;
const headRow = table => table.children.find(e => e.tag === 'thead').children[0].children;
const bodyRows = table => table.children.find(e => e.tag === 'tbody').children;
const inside = (page, id, name) => classNodes(page.el(id), name);
const nameOf = row => classNodes(row, 'system-name')[0];

// The filters are chips in the board's filter bar: a checklist per dimension, one open at a time, hidden when there is nothing to choose.
assert.equal(run('FILTERS.tags(data[0])'), true, 'All includes tagless entries');
check(page, 'filter-tag-all');
assert.equal(run('FILTERS.tags(data[0])'), false, 'Turning all tags off excludes tagless entries too');
assert.equal(elements.get('empty-state').hidden, false); assert.equal(elements.get('empty-title').textContent, 'No matches');
assert.equal(elements.get('value-tag').textContent, 'None'); assert.equal(elements.get('chip-tag').className, 'chip chip-active');
assert.equal(elements.get('reset-filters').hidden, false);
click('clear-empty-filters');
assert.equal(elements.get('empty-state').hidden, true); assert.equal(elements.get('reset-filters').hidden, true);
assert.equal(document.activeElement.id, `choice-mode-${run('state.mode')}`, 'Reset returns focus to the current try mode');
assert.equal(elements.get('chip-tag').hidden, true, 'A dimension with a single value has no chip');
assert.equal(elements.get('chip-machine').hidden, true); assert.equal(elements.get('chip-system').hidden, false);
assert.deepEqual(elements.get('selectors-system').children.map(o => o.textContent), [' All', ' Measured', ' Tagless']);
check(page, 'filter-system-0');
assert.equal(elements.get('value-system').textContent, 'Tagless'); assert.equal(elements.get('shown').textContent, '1 system · filtered');
click('reset-filters');
assert.equal(elements.get('shown').getAttribute('aria-live'), 'polite'); assert.equal(elements.get('shown').textContent, '2 systems');
elements.get('chip-system').open = true; elements.get('chip-hosted').open = true;
for (const callback of elements.get('chip-hosted').listeners.toggle) callback();
assert.equal(elements.get('chip-system').open, false, 'Opening one chip closes the others');
elements.get('chip-hosted').open = false;
// One fixed number format: this test runs under whatever locale the machine has, and a pt-BR viewer must still read 1,000 rows, not 1.000.
assert.equal(run('fmt(1000)'), '1,000');
assert.equal(elements.get('dataset-size').textContent, 'CH-benCHmark seed · 21,121 initial rows · 12 tables');
assert.equal(elements.get('dataset-target').textContent, `${suite.table}, ${suite.dataset.row_counts[suite.table].toLocaleString('en-US')} initial rows; each data workload gets its own child branch`);
assert.equal(elements.get('workload-protocol').textContent, `${suite.tries} tries per workload · ${suite.chain_length}-branch chain · batch totals, not per-operation averages`);
assert.equal(elements.get('dataset-tables').children.length, Object.keys(suite.dataset.row_counts).length);
assert.equal(classNodes(elements.get('dataset-tables'), 'dataset-target')[0].children[0].textContent, `${suite.table}Timed table`);
assert.equal(elements.get('dataset-hash').textContent, suite.dataset.sha256);
run("window.location.hash = '#dataset'; reveal()");
assert.equal(elements.get('dataset-details').open, true, 'The run page link opens the dataset details');
assert.equal(elements.get('updated').textContent, 'Last run 2026-09-30');

// The operations are defined like the workflows: a grouped picker, one stage with a drawn diagram, what is timed and the measured batch times.
const opList = elements.get('selectors-operation');
assert.deepEqual(opList.children.filter(c => c.tag === 'h3').map(h => h.textContent), ['Branching', 'Reads & writes', 'Setup']);
assert.deepEqual(opList.children.filter(c => c.tag === 'button').map(b => b.textContent),
    [...run('suiteRows').map(row => `${row.label}×${row.batch.toLocaleString('en-US')}`), 'Loadonce', 'Provisioningonce'], 'Every operation, with its batch size');
assert.equal(elements.get('operation-name').textContent, 'Time to first query'); assert.equal(elements.get('operation-id').textContent, 'Branching');
assert.match(elements.get('operation-caption').textContent, /^Create a child branch, connect to it, and read one row\./);
assert.deepEqual(elements.get('operation-facts').children.map(li => li.textContent), ['1 operation per batch', `${suite.tries} tries; the board shows the first try`, 'one batch total, not a per-operation average']);
const opBars = inside(page, 'operation-results', 'result-bars')[0];
assert.deepEqual(opBars.children.map(li => li.children[0].children[1].textContent), ['Tagless', 'Measured'], 'Fastest first');
assert.equal(classNodes(opBars, 'best-mark').length, 1); assert.equal(inside(page, 'operation-results', 'result-group')[0].children[0].tag, 'ol', 'One hosting group needs no group label');
assert.match(elements.get('operation-scene').children[0].getAttribute('aria-label'), /^Time to first query: Create a child branch/);
assert.equal(run('scene.key'), 'op:time_to_first_query');
click('operation-read');
assert.equal(run('state.op'), 'read'); assert.equal(run('window.location.hash'), '#dataset?operation=read'); assert.equal(document.activeElement.id, 'operation-read');
assert.equal(run('scene.key'), 'op:read'); assert.deepEqual(elements.get('operation-facts').children.map(li => li.textContent).at(-1), 'on its own child branch');
assert.equal(mount(data, [], null, '#dataset?operation=range_update').run('state.op'), 'range_update', 'An operation is a deep link');
assert.equal(mount(data, [], null, '#dataset?operation=nope').run('state.op'), 'time_to_first_query', 'An unknown operation falls back to the first');

// Header, tabs and production framing.
assert.equal(elements.get('nav-board').href, '#microbench'); assert.equal(elements.get('nav-workflows').href, '#workflows?workflow=rl_env'); assert.equal(elements.get('nav-operations').href, '#dataset?operation=read');
assert.equal(elements.get('doc-nav').href, 'README.md', 'Local serves its own README');
assert.equal(elements.get('nav-run').removed, undefined, 'The local phase links its run wizard from the header');
assert.doesNotMatch(html, /dev-banner|Development preview|Development diagnostics|Illustrative previews|popover|class="eyebrow"/, 'No preview or development framing, no popovers, no kicker labels');
const tabs = elements.get('selectors-suite').children;
assert.equal(elements.get('selectors-suite').getAttribute('role'), 'tablist');
assert.deepEqual(tabs.map(t => t.textContent), ['Macrobench6 agent workflows', `Microbench${suite.rows.length} operations`]);
assert.deepEqual(tabs.map(t => [t.getAttribute('role'), t.getAttribute('aria-selected'), t.tabIndex, t.getAttribute('aria-controls')]), [['tab', 'false', -1, 'macro-view'], ['tab', 'true', 0, 'micro-view']], 'Only the active tab is in the tab order');
assert.equal(elements.get('micro-view').getAttribute('role'), 'tabpanel'); assert.equal(elements.get('micro-view').getAttribute('aria-labelledby'), 'tab-micro');
const key = (page, id, name) => { for (const callback of page.document.getElementById(id).listeners.keydown) callback({key: name, preventDefault() {}}); };
key(page, 'tab-micro', 'Home');
assert.equal(run('state.suite'), 'macro'); assert.equal(document.activeElement.id, 'tab-macro'); assert.equal(run('window.location.hash'), '#agent-workflows');
assert.equal(elements.get('micro-view').hidden, true); assert.equal(elements.get('micro-more').hidden, true); assert.equal(elements.get('macro-more').hidden, false);
key(page, 'tab-macro', 'ArrowRight');
assert.equal(run('state.suite'), 'micro'); assert.equal(document.activeElement.id, 'tab-micro');
const at = marker => { const index = html.indexOf(marker); assert.ok(index >= 0, marker); return index; };
assert.ok(at('id="macro-view"') < at('id="micro-view"'), 'The macrobench comes first');
assert.ok(at('id="selectors-suite"') < at('id="macro-groups"'), 'Tabs, then the table, in one card');
assert.ok(at('id="selectors-mode"') < at('id="chip-hosted"') && at('id="chip-tag"') < at('id="groups"'), 'Every microbench filter sits in the toolbar above its tables');
assert.doesNotMatch(html, /<select|Rank by/, 'Sorting is the column headers\' job, as in any data table; there is no separate rank control');
assert.match(html, /<div class="header-actions"><a class="button" href="https:\/\/github\.com\/elaineang\/db-fork">GitHub[^<]*<span aria-hidden="true">↗<\/span><\/a><a class="button button-primary" id="nav-run" href="run\.html">Run a benchmark<\/a><\/div>/, 'Run a benchmark is the header\'s call to action');
assert.ok(at('id="macro-groups"') < at('id="macro-breakdowns"') && at('id="macro-breakdowns"') < at('id="workflows"') && at('id="workflows"') < at('id="macro-guide"'), 'The board, its evidence, then the definitions of its columns, then the methodology');
assert.ok(at('id="groups"') < at('id="breakdowns"') && at('id="breakdowns"') < at('id="dataset"') && at('id="dataset"') < at('id="methodology"'), 'Microbench results, their breakdown, then what is measured');

// The microbench board: one table per comparable group, ranked by one chosen score, every category score beside it with a speed bar.
for (const mode of ['first', 'best']) {
    click(`choice-mode-${mode}`);
    const active = document.getElementById(`choice-mode-${mode}`);
    assert.equal(active.getAttribute('aria-pressed'), 'true'); assert.equal(active.type, 'button');
    assert.equal(document.activeElement, active, 'Re-rendering a choice preserves focus');
    assert.equal(byClass('summary').length, 1, 'One ranking table per hosting group');
    assert.deepEqual(tableScores(page), [[['×1.00', '×1.00', '×1.00'], ['×1.75', '×1.75', '×2.00']]], 'Branch, reads/writes and load scores side by side');
    const table = byClass('summary')[0];
    assert.deepEqual(headRow(table).map(th => th.textContent.replace('?', '')), ['System', 'First query', 'Reads & writes', 'Load', 'Details'], 'The last column is the row action');
    assert.deepEqual(headRow(table).map(th => th.getAttribute('aria-sort')), [null, 'ascending', null, null, null], 'The rank is explicitly tied to the first-query score');
    assert.equal(classNodes(table, 'help-trigger').length, 0, 'A header only sorts; the definitions live below the board');
    assert.deepEqual(headRow(table).map(th => classNodes(th, 'sort-icon').length), [0, 1, 1, 1, 0], 'Every sortable header shows that it sorts');
    for (const row of bodyRows(table)) { assert.equal(row.children.length, 5); assert.equal(classNodes(row.children[4], 'details-button').length, 1); assert.equal(row.children[0].tag, 'th'); assert.equal(row.children[0].scope, 'row'); for (const td of row.children.slice(1)) assert.equal(td.tag, 'td'); }
    assert.deepEqual(bodyRows(table).map(row => classNodes(row, 'rank-number')[0].textContent), ['1', '2']);
    assert.deepEqual(bodyRows(table).map(row => nameOf(row).textContent), ['Tagless', 'Measured']);
    assert.deepEqual(bodyRows(table).map(row => classNodes(row, 'system-meta')[0].children.filter(c => typeof c !== 'string').map(c => c.className)), [['nowrap', 'nowrap'], ['nowrap', 'nowrap']], 'Meta lines wrap between items, never inside one');
    assert.deepEqual(byClass('score-time').map(e => e.textContent), ['0.030s', 'composite', '0.030s', '0.060s', 'composite', '0.060s'], 'A single-workload score shows its elapsed time; a composite says so');
    assert.deepEqual(byClass('score-cell').map(td => td.style['--strength']), ['0.130', '0.130', '0.130', '0.087', '0.087', '0.080'], 'Scores are shaded against the best in their column, as on the workflow board');
    assert.deepEqual(byClass('score-cell').map(td => classNodes(td, 'best-mark').length), [1, 1, 1, 0, 0, 0], 'The best score in each column is marked');
    assert.equal(byClass('bar').length, 0, 'No bars in the table: a cell is its value');
    assert.ok(table.getAttribute('aria-label').includes('ranked by First query score'));
    assert.match(byClass('group-count')[0].textContent, /^2 systems · ranked by First query · lower is better$/);
    const region = byClass('scroll')[0];
    assert.equal(region.children[0], table, 'The table scrolls inside its own bounded region'); assert.equal(region.getAttribute('role'), 'region');
    assert.equal(region.tabIndex, -1, 'A region that fits is not a tab stop');
    Object.assign(region, {scrollWidth: 900, clientWidth: 400}); run('syncScrollHints()');
    assert.deepEqual([region.tabIndex, region.parent?.hidden ?? byClass('scroll-hint')[0].hidden], [0, false], 'A region that overflows becomes a tab stop, with its hint');
    Object.assign(region, {scrollWidth: 400}); run('syncScrollHints()');
    // The evidence behind the scores sits right under the card: the workload heatmap and the trade-off plot.
    const matrix = inside(page, 'breakdowns', 'heatmap')[0];
    assert.equal(inside(page, 'breakdowns', 'breakdown').length, 1); assert.equal(inside(page, 'breakdowns', 'chart-panel').length, 1);
    assert.equal(inside(page, 'breakdowns', 'breakdown-title').length, 0, 'One group needs no breakdown heading');
    const tradeoffs = inside(page, 'breakdowns', 'chart-point');
    for (const axis of ['x', 'y']) tradeoffs.forEach((point, i) => assert.ok(Math.abs(Number(point.getAttribute(`data-${axis}-score`)) - [1, 1.75][i]) < 1e-12));
    const ticks = classNodes(inside(page, 'breakdowns', 'chart-panel')[0], 'chart-tick').map(t => t.textContent);
    assert.deepEqual(ticks, ['×1', '×1.1', '×1.2', '×1.5', '×2', '×1', '×1.1', '×1.2', '×1.5', '×2'], 'Each logarithmic axis gets its own range and readable ticks');
    assert.equal(bodyRows(matrix).length, suite.rows.length + 2);
    assert.equal(descendants(matrix).filter(e => e.tag === 'input' && e.type === 'checkbox').length, 5, 'Inclusion checkboxes stay in the workload matrix');
    for (const th of headRow(matrix).slice(1)) assert.match(th.title, /^test · 2026-/);
    assert.equal(classNodes(matrix, 'bar').length, 0, 'Timing cells show raw times, never bars or another ratio');
    assert.deepEqual(heatmapColors(page), Array.from({length: suite.rows.length + 2}, () => ['0.130', '0.080']).flat());
    assert.equal(classNodes(matrix, 'best-mark').length, suite.rows.length + 2, 'Fastest cells have a non-color indicator');
    assert.deepEqual(classNodes(matrix, 'workload-link').map(a => a.href), [...suite.rows.map(r => `#dataset?operation=${r.op}`), '#dataset?operation=load', '#dataset?operation=provision'], 'Every workload name links to its definition');
    assert.equal(classNodes(matrix, 'help-trigger').length, 0, 'No "?" tab stops between the checkboxes');
    for (const t of [...byClass('summary'), matrix]) {
        assert.ok(t.getAttribute('aria-label'), 'Tables have accessible names');
        for (const th of descendants(t).filter(e => e.tag === 'th')) assert.ok(['col', 'row'].includes(th.scope));
    }
    assert.match(elements.get('operation-facts').children[1].textContent, new RegExp(`the board shows the ${mode === 'first' ? 'first try' : 'best repeat'}$`), 'The definition says which try the board shows');
}
// Warm-up: a dot per system and workload at its first try over its best repeat, on a logarithmic axis.
{
    const fast = entry('Fast', 0.1, []), cold = entry('Cold', 0.1, []);
    cold.result = suite.rows.map(() => [0.2, 0.1, 0.1]);  // every first try twice its best repeat
    fast.result[1] = [null, 0.1, 0.1];  // a failed branch-create first try: no dot
    fast.result[2] = [0.095, 0.1, 0.1];  // a first try faster than both repeats sits left of ×1
    const warm = mount([fast, cold]), chart = inside(warm, 'breakdowns', 'dotplot-panel')[0], dots = classNodes(chart, 'dotplot-dot');
    assert.equal(classNodes(chart, 'dotplot-row').length, suite.rows.length);
    assert.equal(dots.length, 2 * suite.rows.length - 1, 'One dot per system and workload, none for a failed try');
    assert.deepEqual(classNodes(chart, 'dotplot-tick').map(t => [t.textContent, t.style.left]), [['×1', '8.75%'], ['×1.5', '42.43%'], ['×2', '66.32%'], ['×3', '100.00%']],
        'The axis starts below the fastest first try; ticks too close to ×1 or the axis end are dropped so labels never touch');
    assert.equal(classNodes(chart, 'dotplot-ref')[0].style.left, '8.75%', 'The ×1 line, no warm-up, is drawn apart');
    assert.deepEqual(dots.filter(d => d.title.startsWith('Cold')).map(d => d.style.left), Array(suite.rows.length).fill('66.32%'));
    assert.match(dots.find(d => d.title.startsWith('Fast · Branch connect')).title, /: first try .+, best repeat .+ \(×0\.95\)$/);
    assert.equal(dots.find(d => d.title.startsWith('Fast · Branch connect')).style.left, '4.49%');
    for (const row of classNodes(chart, 'dotplot-row')) { const tops = classNodes(row, 'dotplot-dot').map(d => d.style.top); assert.equal(new Set(tops).size, tops.length, 'Systems keep apart within a row'); }
    const legends = inside(warm, 'breakdowns', 'chart-legend').map(l => classNodes(l, 'chart-key').map(k => k.textContent));
    assert.deepEqual(legends, Array(2).fill(classNodes(inside(warm, 'breakdowns', 'heatmap')[0], 'system-name').map(n => n.textContent)), 'Both legends list the systems in the heatmap\'s column order');
    assert.match(classNodes(chart, 'panel-note').at(-1).textContent, /A workload with a failed try has no dot for that system\.$/);
    const untried = entry('Untried', 0.1, []); untried.result = suite.rows.map(() => [null, 0.1, 0.1]);
    assert.equal(inside(mount([untried]), 'breakdowns', 'dotplot-panel')[0].children.at(-1).className, 'chart-empty', 'Without a first try there is nothing to compare');
}
// The rank metric is chosen from the score column headers; the other scores stay in place.
click('sort-load-no');
assert.deepEqual(headRow(byClass('summary')[0]).map(th => th.getAttribute('aria-sort')), [null, null, null, 'ascending', null]);
assert.ok(byClass('summary')[0].getAttribute('aria-label').includes('ranked by Load score'));
click('sort-data-no');
assert.equal(run('state.sort'), 'data'); assert.equal(document.activeElement.id, 'sort-data-no', 'A header keeps focus after ranking by it');
assert.deepEqual(headRow(byClass('summary')[0]).map(th => th.getAttribute('aria-sort')), [null, null, 'ascending', null, null]);
click('sort-data-no');
assert.deepEqual(headRow(byClass('summary')[0]).map(th => th.getAttribute('aria-sort')), [null, null, 'descending', null, null], 'Sorting the sorted score again flips the order');
assert.deepEqual(bodyRows(byClass('summary')[0]).map(row => nameOf(row).textContent), ['Measured', 'Tagless']);
click('sort-data-no');
assert.match(byClass('summary')[0].children[1].children[0].children[2].title, /composite of several workloads/);
click('sort-branch-no');
assert.deepEqual(run('Object.keys(SORTS)'), run('Object.keys(VIEWS)'), 'Every rank metric is an existing category score; there is no invented overall score');

assert.equal(run("pick([0.3, 0.2, 0.1], 'first')"), 0.3);
assert.equal(run("pick([null, 0.2, 0.1], 'best')"), 0.1);
assert.equal(run("pick([0.3, null, 0.1], 'best')"), null);
assert.equal(run("pick([0.3, 0.2, null], 'best')"), null);
assert.equal(run('seconds(0.017706)'), '0.017706s', 'Close first-query results remain distinguishable');
assert.equal(run('seconds(0.017746)'), '0.017746s');
assert.deepEqual([1, 2, 3, 4, 11, 12, 13, 21, 22, 101].map(n => run(`ordinal(${n})`)), ['1st', '2nd', '3rd', '4th', '11th', '12th', '13th', '21st', '22nd', '101st']);
assert.match(byClass('group-heading')[0].textContent, /^Operations/);
// A "?" opens the definition below the board and moves focus there.
// Following a link: the hash changes, the section opens on the linked definition and keyboard focus moves there.
const follow = (page, hash) => { page.run(`window.location.hash = ${JSON.stringify(hash)}`); page.run('{ const before = routeKey(); applyHash(); if (before !== routeKey()) render(); reveal(true); }'); };
elements.get('dataset').getBoundingClientRect = () => ({top: 2400});  // the definitions sit far below the board
follow(page, inside(page, 'breakdowns', 'workload-link').find(a => a.textContent === 'Point read').href);
assert.equal(run('state.op'), 'read'); assert.equal(run('window.location.hash'), '#dataset?operation=read');
assert.ok(elements.get('dataset').scrolledIntoView >= 1); assert.equal(document.activeElement.id, 'operation-read');
assert.equal(descendants(elements.get('breakdowns')).find(e => e.id === 'row-3-no').getAttribute('aria-label'), 'Count Point read in the reads & writes score', 'A checkbox without a visible label still says what it counts');

// The details dialog: why this position first, then every try, the environment, and provenance behind a disclosure.
const [firstRow, secondRow] = bodyRows(byClass('summary')[0]);
assert.equal(nameOf(firstRow).tag, 'span'); assert.equal(classNodes(firstRow, 'details-button')[0].getAttribute('aria-label'), 'Tagless details', 'The name is text; the row opens from its Details button');
assert.equal(firstRow.listeners.click, undefined, 'The row itself is not clickable');
classNodes(secondRow, 'details-button')[0].click();
assert.equal(elements.get('run-dialog').open, true, 'Result opens in a native dialog');
assert.equal(elements.get('run-title').textContent, 'Measured');
assert.equal(elements.get('run-subtitle').textContent, 'Self-hosted · test · 2026-09-30 · version not recorded');
assert.equal(elements.get('run-source').href, 'test.json');
assert.equal(classNodes(elements.get('run-body'), 'verdict')[0].textContent, '2nd of 2 by First query, score ×1.75; the leader, Tagless, has ×1.00. ×1 is the fastest time in every counted workload.');
const why = classNodes(elements.get('run-body'), 'why-table')[0];
assert.equal(bodyRows(why).length, 3 + suite.rows.length + 2, 'Each category, then each workload that feeds it');
assert.deepEqual(classNodes(why, 'why-category').map(tr => tr.children[0].textContent), ['First query', 'Reads & writes', 'Load']);
assert.equal(classNodes(why, 'ratio-value')[0].textContent, '×1.75'); assert.equal(classNodes(why, 'ratio-value').at(-2).textContent, '×2.00', 'Load ratios use no shift');
assert.match(why.textContent, /Branch createnot in score/, 'Workloads outside the score are named, never silently counted');
const whyBars = classNodes(why, 'time-bar');
assert.equal(whyBars.length, suite.rows.length + 2, 'Every compared workload has a time bar');
assert.deepEqual([whyBars[0].children[0].style.width, whyBars[0].children[1].style.left], ['100.0%', '50.0%'], 'The slower system fills its bar; the tick marks the fastest, as every bar on the page does');
const attempts = classNodes(elements.get('run-body'), 'run-timings')[0];
assert.equal(bodyRows(attempts).length, suite.rows.length);
assert.equal(bodyRows(attempts)[0].children.length, suite.tries + 2);
assert.equal(classNodes(elements.get('run-body'), 'inline-more')[0].children[0].textContent, 'Reproducibility details');
click('close-run');
assert.equal(elements.get('run-dialog').open, false);
assert.equal(document.activeElement, classNodes(secondRow, 'details-button')[0], 'Closing restores focus to the button that opened it');
classNodes(firstRow, 'details-button')[0].click();
assert.match(classNodes(elements.get('run-body'), 'verdict')[0].textContent, /^1st of 2 by First query, score ×1\.00\. /, 'The leader is not compared with itself');
click('close-run');
// The page has no notion of mock data: the local site's mock rows are ordinary rows. A row without a source or
// reproducibility fields simply has no raw-record link and no empty reproducibility section.
assert.doesNotMatch(scripts.join('\n'), /mock/i, 'No page script knows about mock data');
const bare = mount([entry('Measured', 0.06, ['sql']), {...entry('Sample', 0.001, ['sql']), source: undefined, commit: undefined}]);
classNodes(bodyRows(bare.byClass('summary')[0])[0], 'details-button')[0].click();
assert.deepEqual([bare.el('run-title').textContent, bare.el('run-source').hidden, classNodes(bare.el('run-body'), 'inline-more').length, classNodes(bare.el('run-body'), 'notice').length], ['Sample', true, 0, 0]);

// Agent workflows: a separate lane with explicit states, never a score. Every campaign the page receives passed
// validation in the build, so a row is a result and nothing else: no flag, no caveat, no comparability state.
const KEYS = ['rl_env', 'context_mgmt', 'multi_agent', 'dev_agent', 'ops_agent', 'data_agent'];
const NAMES_6 = ['Evaluate agent rollouts', 'Manage context history', 'Coordinate multiple agents', 'Develop against live data', 'Recover from a bad deployment', 'Ingest and merge data'];
const checks = (passed, failed = 0, notApplicable = 0, enabled = true) => ({enabled, checked: passed + failed, passed, failed, not_applicable: notApplicable, results: [
    ...Array.from({length: passed}, (_, i) => ({name: `P${i + 1}`, passed: true, detail: ''})), ...Array.from({length: failed}, (_, i) => ({name: `F${i + 1}`, passed: false, detail: 'mismatch'})),
    ...Array.from({length: notApplicable}, (_, i) => ({name: `N${i + 1}`, passed: null, detail: 'needs an unsupported verb'}))]});
const evidence = (extra = {}) => ({run_id: 'bb_00000001', status: 'completed', elapsed_sec: 12.5, timed_out: false, seed: 42,
    setup: {schema_failed: [], seed_commit: 'OK', setup_sec: 1.5}, capabilities: {merge: true}, implementation: {verbs: {merge: 'native'}, notes: {merge: 'dolt_merge'}},
    invariants: checks(2), metrics: {leaves: 6}, work: {expected: 8, succeeded: 8, failed: 0, skipped: 0}, worker_errors: [], failed_verbs: {expected: {spine: 2}, unexpected: {}},
    unsupported_ops: [], op_status_counts: {BRANCH: {ok: 8, failed: 0, unsupported: 0, ok_seconds: 0.4}, EXEC: {ok: 20, failed: 0, unsupported: 0, ok_seconds: 3.2}, READ: {ok: 40, failed: 2, unsupported: 0, ok_seconds: 1.1}},
    commit_ref_fallbacks: 0, ...extra});
// Evidence that the classifier would actually turn into each status: skipped work with an unsupported verb and a check it made not applicable, failed work and a failed check, or work cut off at the deadline.
const EVIDENCE = {verified: {}, incomplete: {work: {expected: 8, succeeded: 6, failed: 0, skipped: 2}, invariants: checks(1, 0, 1), unsupported_ops: ['MERGE:not implemented', 'MERGE:timed out', 'REBASE']},
    failed: {work: {expected: 8, succeeded: 2, failed: 6, skipped: 0}, invariants: checks(1, 1)}, timed_out: {work: {expected: 8, succeeded: 5, failed: 0, skipped: 0}, timed_out: true}};
const attempt = (id, status, seconds = 12.5, extra = {}) => ({attempt_id: id, date: '2026-10-10', source: `dolt_mysql/macro/20261010/${id}.json`,
    outcome: {status, reasons: status === 'verified' ? [] : [`${status} reason`], elapsed_sec: 12.5, setup_sec: 1.5, completion_sec: status === 'verified' ? seconds : null},
    cleanup: {status: 'verified', detail: `${id} deleted and absent`}, child: {exit_code: 0, ended: 'exited', error: null, deadline_sec: 1800, log_tail: 'E2E stats written'},
    evidence: evidence(EVIDENCE[status]), config_sha256: '3'.repeat(64), base_config_sha256: '2'.repeat(64), effective: {seed: 42}, ...extra});
const six = (prefix, statuses, seconds) => Object.fromEntries(KEYS.map((k, i) => [k, attempt(`bb_${prefix}${i}`, statuses[i], seconds[i])]));
const campaign = (id, system, folder, started, attempts, extra = {}) => ({campaign_id: id, system, folder, machine: 'box', preset: 'mini', scenarios: KEYS,
    started, finished: started, source: `${folder}/macro/${started.slice(0, 10).replaceAll('-', '')}/campaign_${id}.json`, version: '1.81.2', commit: '0'.repeat(40),
    source_sha256: '1'.repeat(64), lock_sha256: '4'.repeat(64), python: '3.13.14', platform: 'Darwin 27.0.0 arm64', environment: {cpu: 'Test CPU', cpus: 8, memory_bytes: 17179869184},
    tags: [], attempts, group: 'abc123def456', ...extra});
const V = Array(6).fill('verified');
// Evidence the page must never turn into numbers, even on an attempt whose outcome the validator accepted.
const malformed = evidence({work: {expected: 8, succeeded: 9, failed: 0, skipped: 0}, invariants: {enabled: 'yes', checked: 2, passed: 3, failed: 0, not_applicable: -1, results: [null]}, unsupported_ops: 'BRANCH'});
const contradicted = evidence({invariants: {...checks(2), results: [{name: 'P1', passed: true, detail: ''}, {name: 'F1', passed: false, detail: 'mismatch'}]}, worker_errors: {}});
const unlisted = evidence({invariants: {...checks(2), results: {}}, op_status_counts: {BRANCH: {ok: 3}}, failed_verbs: null, metrics: 'none'});
const other = six('0000003', ['verified', 'failed', 'failed', 'failed', 'verified', 'verified'], [15, 0, 0, 0, 15, 15]);
other.context_mgmt.evidence = contradicted; other.multi_agent.evidence = unlisted; other.dev_agent.evidence = malformed;
const macroData = {suite: {suite: 'macro-1'}, excluded: [{system: 'dolt', source: 'dolt/macro/20261010/campaign_deadbeef.json', reason: 'failed validation: run from uncommitted sources'},
    {system: 'dolt', source: 'dolt/macro/20261010/bb_deadbeef.json', reason: 'invalid: not a JSON object'}], campaigns: [
    campaign('c1000000', 'Dolt', 'dolt_mysql', '2026-10-10T12:00:00+00:00', six('0000001', V, [10, 10, 10, 10, 10, 10])),
    campaign('c2000000', 'Doltgres', 'dolt', '2026-10-10T11:00:00+00:00', six('0000002', ['verified', 'incomplete', 'failed', 'timed_out', 'verified', 'verified'], [20, 0, 0, 0, 5, 20])),
    campaign('c3000000', 'MatrixOne', 'matrixone', '2026-10-09T12:00:00+00:00', other, {machine: 'box2', group: 'fedcba987654', commit: '1'.repeat(40), environment: {cpu: 'Other CPU', cpus: 4, memory_bytes: 8589934592}}),
    campaign('c4000000', 'Dolt', 'dolt_mysql', '2026-10-08T12:00:00+00:00', six('0000004', V, [30, 30, 30, 30, 30, 30])),
]};
const macroPage = mount(data, [], macroData, '#agent-workflows');
const macroEl = macroPage.el;
const boards = (page = macroPage) => classNodes(page.el('macro-groups'), 'workflow-table');
const rowsOf = (page = macroPage) => boards(page).flatMap(bodyRows);
const rowOf = (system, page = macroPage) => rowsOf(page).find(r => nameOf(r).textContent === system);
const cellOf = (system, key, page = macroPage) => rowOf(system, page).children[KEYS.indexOf(key) + 1];
const cellText = (system, key, page) => cellOf(system, key, page).textContent;
const detailsOf = row => classNodes(row, 'details-button')[0];
assert.equal(macroPage.run('state.suite'), 'macro', 'A direct suite URL opens the agent-workflow results');
assert.equal(macroEl('micro-view').hidden, true); assert.equal(macroEl('macro-view').hidden, false);
assert.doesNotMatch(scripts.join('\n') + html, /preset|smoke|ineligible|comparable campaign|not comparable|uncommitted/i, 'The page has no preset, smoke or comparability vocabulary at all');
assert.equal(macroPage.run('state.rank'), 'rl_env', 'Ranked by the first workflow until the viewer picks another');
assert.equal(macroPage.run('state.workflow'), 'rl_env', 'The definition selection is separate state');
assert.equal(macroPage.document.getElementById('status-legend'), null, 'No status key: every status is spelled out in its cell');
assert.deepEqual(macroEl('selectors-suite').children.map(b => b.getAttribute('aria-selected')), ['true', 'false']);
assert.equal(macroEl('nav-board').href, '#agent-workflows'); assert.equal(macroEl('nav-workflows').href, '#workflows?workflow=rl_env');
// All campaigns render in one unified table, headed by 'Agent workflows' and how many systems it compares.
assert.equal(boards().length, 1, 'One unified table for all campaigns');
assert.deepEqual(classNodes(macroEl('macro-groups'), 'group-heading').map(h => [h.children[0].textContent, h.children[1].textContent]),
    [['Agent workflows', '3 systems · ranked by Evaluate agent rollouts · lower is better']]);
for (const table of boards()) {
    assert.deepEqual(headRow(table).map(firstText), ['System', ...NAMES_6, 'Completed'], 'Workflows are columns, named by the job; the upstream id is secondary');
    assert.deepEqual(headRow(table).slice(1, 7).map(th => [th.children.length, classNodes(th, 'sort-label')[0].textContent, classNodes(th, 'sort-button')[0].title]), NAMES_6.map(name => [1, name, name]), 'A header is one sort button: its label, clamped to two lines, with the full name as its title');
    assert.deepEqual(headRow(table).map(th => th.getAttribute('aria-sort')), [null, 'ascending', null, null, null, null, null, null]);
    assert.equal(classNodes(table, 'sort-button').length, 6); assert.equal(classNodes(table, 'sort-icon').length, 6, 'Every workflow header shows that it sorts'); assert.equal(classNodes(table, 'help-trigger').length, 0, 'A header only sorts; the definitions live below the board');
    assert.equal(table.getAttribute('aria-label'), 'Agent workflows: completion time per system and workflow, ranked by Evaluate agent rollouts');
    for (const row of bodyRows(table)) { assert.equal(row.children.length, 8); assert.equal(row.children[0].tag, 'th'); assert.equal(row.children[0].scope, 'row'); for (const td of row.children.slice(1)) assert.equal(td.tag, 'td'); }
}
assert.deepEqual(rowsOf().map(r => [classNodes(r, 'rank-number')[0].textContent, nameOf(r).textContent, classNodes(r, 'system-meta')[0].textContent]),
    [['1', 'Dolt', 'v1.81.2 · 2026-10-10'], ['2', 'MatrixOne', 'v1.81.2 · 2026-10-09'], ['3', 'Doltgres', 'v1.81.2 · 2026-10-10']], 'The newest run of each system and machine in one unified ranking');
assert.equal(classNodes(macroEl('macro-groups'), 'flag').length, 0, 'No caveat flag on any row');
assert.deepEqual(rowsOf().map(r => classNodes(r, 'hosting-badge')[0].textContent), ['Self-hosted', 'Self-hosted', 'Self-hosted'], 'Hosting badge on every row');
assert.deepEqual(rowsOf().map(r => classNodes(r, 'spec-badge')[0].textContent), ['8 CPUs · 16 GiB', '4 CPUs · 8 GiB', '8 CPUs · 16 GiB'], 'Hardware spec badge on every row');
// One control per row: the Details button. The name is text, and no cell or row responds to a click.
for (const row of rowsOf()) {
    assert.equal(nameOf(row).tag, 'span');
    assert.deepEqual(descendants(row).filter(e => e.tag === 'button').map(b => [b.className, b.textContent, b.getAttribute('aria-label'), b.getAttribute('aria-haspopup')]),
        [['details-button', 'Details', `${nameOf(row).textContent} details`, 'dialog']]);
    assert.ok(descendants(row.children.at(-1)).includes(detailsOf(row)), 'Details sits last, beside the completion count');
    assert.equal(row.listeners.click, undefined);
}
// Every cell says what happened: a time for a completed workflow, a drawn status with its word otherwise. Each column marks and shades against its own fastest time.
assert.deepEqual(KEYS.map(k => cellText('Dolt', k)), Array(6).fill('10.000s'));
assert.deepEqual(KEYS.map(k => cellText('Doltgres', k)), ['20.000s', 'IncompleteUnsupported: MERGE, REBASE', 'Failed', 'Timed out', '5.000s', '20.000s'], 'One deduplicated unsupported-operation note');
assert.deepEqual(KEYS.map(k => classNodes(cellOf('Doltgres', k), 'best-mark').length), [0, 0, 0, 0, 1, 0], 'Each column marks its own fastest time');
assert.deepEqual(KEYS.map(k => classNodes(cellOf('Dolt', k), 'best-mark').length), [1, 0, 0, 0, 0, 1], 'A time with nothing to compare against is never the fastest');
assert.deepEqual(KEYS.map(k => cellOf('Dolt', k).style['--strength']), ['0.130', undefined, undefined, undefined, '0.080', '0.130'], 'Only a column that compares two times is shaded');
assert.deepEqual(KEYS.map(k => cellOf('MatrixOne', k).style['--strength']), ['0.097', undefined, undefined, undefined, '0.063', '0.097']);
assert.ok(rowsOf().every(r => r.children.slice(1).every(td => !/\bsorted\b/.test(td.className))), 'No column is tinted for being sorted');
assert.deepEqual(KEYS.map(k => cellOf('Doltgres', k).style['--strength']), ['0.080', undefined, undefined, undefined, '0.130', '0.080'], 'A status is never shaded');
assert.equal(cellOf('Doltgres', 'dev_agent').title, 'timed_out reason', 'The reason is one hover away; the partial time is in the details');
assert.equal(classNodes(cellOf('Doltgres', 'context_mgmt'), 'status-icon')[0].tag, 'svg', 'Status icons are drawn, not glyphs');
assert.deepEqual(rowsOf().map(r => classNodes(r, 'done-count')[0].textContent), ['6 / 6', '3 / 6', '3 / 6']);
assert.deepEqual(rowsOf().map(r => classNodes(r, 'status-strip')[0].getAttribute('aria-label')), ['6 of 6 workflows completed', '3 of 6 workflows completed', '3 of 6 workflows completed']);
assert.doesNotMatch(css, /\.(workflow-table|board)[^{]*\{[^}]*display:\s*(block|grid|flex)/, 'Table, rows and cells keep their native display at every width');
assert.doesNotMatch(css, /\.(wf|done|score|ratio|system)-cell\s*(,[^{]*)?\{[^}]*display:\s*(flex|grid|block)/, 'A cell is never a flex, grid or block box: its border must be the row border');
assert.match(css, /\.board \.system-col, \.board \.system-cell \{ position: sticky; left: 0;/, 'Narrow screens scroll the workflows past a pinned system column');
assert.doesNotMatch(css, /data-label|attr\(data-label\)/, 'No stacked-card labels');
assert.match(css, /\.status-chip \{[^}]*overflow-wrap: anywhere/); assert.doesNotMatch(css, /\.status-chip[^{]*\{[^}]*word-break: break-all/, 'Status words wrap at word boundaries, never letter by letter');
// Below the card, the unified breakdown: branch operations timed per call, beside the share of each
// completed workflow's operation time they take.
assert.deepEqual(classNodes(macroEl('macro-breakdowns'), 'breakdown-title').map(h => h.textContent), [], 'One unified breakdown needs no group heading');
{
    const soloGroup = classNodes(macroEl('macro-breakdowns'), 'breakdown-group')[0];
    assert.ok(classNodes(soloGroup, 'dotplot-dot').some(d => d.style.backgroundColor === 'var(--sys-other)'), 'A system without its own color takes the neutral one');
    const dolt = campaign('d1000000', 'Dolt', 'dolt_mysql', '2026-10-10T12:00:00+00:00', six('0000011', V, Array(6).fill(10)));
    const pg = campaign('d2000000', 'Doltgres', 'dolt', '2026-10-10T11:00:00+00:00', six('0000012', ['verified', 'failed', 'failed', 'failed', 'verified', 'verified'], [20, 0, 0, 0, 5, 20]));
    for (const a of Object.values(pg.attempts)) a.evidence.op_status_counts.BRANCH.ok_seconds = 0.8;  // 100 ms a call against Dolt's 50
    dolt.attempts.dev_agent.evidence.op_status_counts.REBASE = {ok: 9, failed: 4, unsupported: 0, ok_seconds: 1.8};
    pg.attempts.dev_agent.evidence.op_status_counts.REBASE = {ok: 0, failed: 12, unsupported: 0, ok_seconds: 0};
    pg.attempts.multi_agent.evidence.op_status_counts.LOG = {ok: 3};  // malformed, so this attempt counts nowhere
    const page = mount(data, [], {suite: {suite: 'macro-1'}, excluded: [], campaigns: [pg, dolt]}, '#agent-workflows'), box = page.el('macro-breakdowns');  // newest first, shown in name order
    const verbs = classNodes(box, 'heatmap')[0], chart = classNodes(box, 'dotplot-panel')[0], dots = classNodes(chart, 'dotplot-dot');
    assert.equal(classNodes(box, 'breakdown-title').length, 0, 'One group needs no heading');
    assert.deepEqual(headRow(verbs).slice(1).map(th => classNodes(th, 'system-name')[0].textContent), ['Dolt', 'Doltgres']);
    assert.deepEqual(bodyRows(verbs).map(r => r.children.map(c => c.textContent)), [['Branch', '50.0 ms', '100.0 ms'], ['Rebase', '200.0 ms4 failed of 13 calls', 'Failed12 failed of 12 calls']],
        'Only operations some system called, each a mean per successful call, with its failures under it');
    assert.deepEqual(bodyRows(verbs).map(r => r.children.slice(1).map(td => [td.className.split(' ').filter(c => c !== 'heat').join(' '), td.style['--strength'], classNodes(td, 'best-mark').length])),
        [[['cell-best', '0.130', 1], ['', '0.080', 0]], [['', undefined, 0], ['cell-missing', undefined, 0]]], 'A row with two times shades and marks its fastest; one time compares nothing');
    assert.equal(bodyRows(verbs)[0].children[2].title, '40 of 40 calls succeeded, 4.000s in all', 'The malformed attempt adds no calls');
    assert.deepEqual(classNodes(chart, 'dotplot-row').map(r => r.children[0].textContent), NAMES_6);
    assert.deepEqual(classNodes(chart, 'dotplot-tick').map(t => [t.textContent, t.style.left]), [['0%', '0.00%'], ['25%', '25.00%'], ['50%', '50.00%'], ['75%', '75.00%'], ['100%', '100.00%']]);
    assert.deepEqual(dots.map(d => [d.title.split(' · ')[0], d.style.left]).sort(), [...Array(5).fill(['Dolt', '11.11%']), ['Dolt', '40.74%'], ...Array(3).fill(['Doltgres', '20.00%'])].sort(),
        'Only completed workflows: branch time over branch and agent-step time');
    assert.equal(dots.find(d => d.title.includes('Develop against live data')).title, 'Dolt · Develop against live data: 41% in branch operations, 2.200s of 5.400s');
    assert.deepEqual([...new Set(dots.map(d => d.style.backgroundColor))], ['var(--sys-dolt)', 'var(--sys-doltgres)']);
    assert.deepEqual(classNodes(chart, 'chart-key').map(k => k.textContent), ['Dolt', 'Doltgres']);
    assert.match(classNodes(chart, 'panel-note').at(-1).textContent, /A workflow that did not complete has no dot\.$/);
    for (const a of Object.values(dolt.attempts)) a.evidence = null;
    const bare = mount(data, [], {suite: {suite: 'macro-1'}, excluded: [], campaigns: [dolt]}, '#agent-workflows').el('macro-breakdowns');
    assert.equal(classNodes(bare, 'empty-cell')[0].textContent, 'No branch operations recorded.');
    assert.equal(classNodes(bare, 'chart-empty')[0].textContent, 'No completed workflow recorded its operation time.');

    // Native-only and execution_profile cohorts render in separate tables/breakdowns, and shareChart is invariant across filters
    const stdC = campaign('d3000000', 'Dolt', 'dolt_mysql', '2026-10-10T12:00:00+00:00', six('0000021', V, Array(6).fill(10)), {execution_profile: 'local_single_host'});
    const econC = campaign('d4000000', 'Doltgres', 'dolt', '2026-10-10T12:00:00+00:00', six('0000022', V, Array(6).fill(15)), {execution_profile: 'economic_single_vm'});
    stdC.attempts.rl_env.evidence.worker_time = {branch_sec: 3.0, data_sec: 1.0, total_sec: 4.0, spine_sec: 0.5};
    const natC = {...campaign('d5000000', 'Dolt', 'dolt_mysql', '2026-10-10T13:00:00+00:00', six('0000023', V, Array(6).fill(8)), {execution_profile: 'local_single_host'}), native_only: true};
    const mixedPage = mount(data, [], {suite: {suite: 'macro-1'}, excluded: [], campaigns: [natC, stdC, econC]}, '#agent-workflows');
    assert.equal(boards(mixedPage).length, 3, 'local_single_host, economic_single_vm, and Native-only campaigns render in separate tables');
    assert.deepEqual(classNodes(mixedPage.el('macro-groups'), 'group-heading').map(h => h.children[0].textContent),
        ['Agent workflows (economic_single_vm)', 'Agent workflows (local_single_host)', 'Agent workflows (Native-only)']);
    const mixedDots = classNodes(classNodes(mixedPage.el('macro-breakdowns'), 'dotplot-panel')[1], 'dotplot-dot');
    assert.equal(mixedDots.find(d => d.title.startsWith('Dolt · Evaluate agent rollouts')).style.left, '11.11%', 'shareChart uses op_status_counts consistently across filters');
}

// Design tokens: every color is written once, as a :root token in styles.css; rules, markup and scripts name the token.
// A literal anywhere else fails with its place and the nearest token. Browser chrome cannot read CSS, so the favicon and
// theme-color keep literals; theme-color must still equal the canvas token.
{
    const literal = /#[0-9a-f]{3,8}\b|\b(?:rgba?|hsla?|hwb|lab|lch|oklab|oklch|color)\(|(?<![\w-])(?:white|black)(?![\w-])/gi;
    const rgb = hex => { const h = hex.slice(1), full = h.length <= 4 ? [...h].map(c => c + c).join('') : h; return [0, 2, 4].map(i => parseInt(full.slice(i, i + 2), 16)); };
    const palette = Object.keys(rootTokens).map(name => [name, computed(name)]).filter(([, value]) => /^#[0-9a-f]{3,8}$/i.test(value));
    const nearest = found => { const value = {white: '#fff', black: '#000'}[found.toLowerCase()] ?? found;
        return /^#/.test(value) ? `; use var(${palette.map(([name, hex]) => [name, rgb(hex).reduce((sum, c, i) => sum + (c - rgb(value)[i]) ** 2, 0)]).sort((a, b) => a[1] - b[1])[0][0]}) or add a token` : '; use a token'; };
    const blank = text => text.replace(/\/\*[\s\S]*?\*\//g, m => m.replace(/[^\n]/g, ' '));
    const offences = [];
    const report = (file, text, at, found, where) => offences.push(`${file}:${text.slice(0, at).split('\n').length} ${where}uses ${found}${nearest(found)}`);
    const sheet = blank(css);
    for (const m of sheet.matchAll(literal)) {
        const open = sheet.lastIndexOf('{', m.index), selector = sheet.slice(Math.max(sheet.lastIndexOf('}', open), sheet.lastIndexOf('{', open - 1)) + 1, open).trim();
        if (selector !== ':root') report('styles.css', sheet, m.index, m[0], `${selector} `);
    }
    for (const src of html.matchAll(/<script src="(js\/[\w-]+\.js)"><\/script>/g).map(([, src]) => src)) {
        const code = fs.readFileSync(`${__dirname}/${src}`, 'utf8').replace(/^\s*\/\/.*$|\/\*[\s\S]*?\*\//gm, m => m.replace(/[^\n]/g, ' '));
        for (const m of code.matchAll(literal)) report(src, code, m.index, m[0], '');
    }
    const wizard = fs.readFileSync(`${__dirname}/run.html`, 'utf8');
    const arch = fs.readFileSync(`${__dirname}/architecture.html`, 'utf8');
    const readme = fs.readFileSync(`${__dirname}/README.md`, 'utf8');
    const requirements = fs.readFileSync(`${__dirname}/requirements/README.md`, 'utf8');
    for (const [file, markup] of [['index.html', html], ['run.html', wizard], ['architecture.html', arch]]) {
        assert.equal(markup.match(/<meta name="theme-color" content="([^"]+)">/)?.[1], computed('--canvas'), `${file}: theme-color is the --canvas token's value`);
        const rest = markup.replace(/<meta name="theme-color"[^>]*>|<link rel="icon"[^>]*>/g, m => m.replace(/[^\n]/g, ' '));
        for (const m of rest.matchAll(literal)) report(file, rest, m.index, m[0], '');
    }
    assert.deepEqual(offences, [], `Color literals outside :root:\n${offences.join('\n')}`);
    assert.doesNotMatch(arch, /--preset=full|<sys>\s+<machine>|\bProject=BranchBench(?!Leaderboard)\b|\.phase-pill|run_verbs|validate_verbs\.py|role="tablist"/,
        'architecture.html must match real CLI syntax, AWS tags, ARIA roles, and CSS selectors');
    assert.match(arch, /Chronos/, 'architecture.html must list Chronos among self-hosted engines');
    assert.match(arch, /python -m leaderboard\.verbs/, 'architecture.html must reference python -m leaderboard.verbs');
    assert.ok(
        arch.includes('The planned launcher assigns one On-Demand <code>m7a.2xlarge</code> to each self-hosted database. Each worker has a dedicated 200 GiB <code>gp3</code> data volume.'),
        'architecture.html must show the fixed production target without claiming that its launcher exists',
    );
    assert.ok(
        arch.includes('A future per-database map may override it. Different profiles must use separate hardware cohorts.'),
        'architecture.html must preserve the future placement boundary without mixing hardware cohorts',
    );
    assert.match(readme, /All workers in one ranked cohort use this fixed profile\./, 'README.md must document the fixed production cohort');
    assert.match(readme, /`project` tag and the operator's `billing-tag1`/, 'README.md must document required DAPLab billing tags');
    assert.match(readme, /does not call a password manager or store AWS credentials/, 'README.md must keep operator credentials outside the project');
    assert.match(readme, /The alpha phase is locked to `us-east-1`/, 'README.md must lock the DAPLab alpha region');
    assert.match(arch, /--daplab-account-id &lt;account&gt;/, 'architecture.html must require the expected DAPLab account ID');
    assert.match(arch, /\{project\}-\{owner\}-\{purpose\}/, 'architecture.html must document DAPLab resource naming');
    assert.match(arch, /On-Demand t3a\.xlarge/, 'architecture.html must label the default alpha worker as On-Demand');
    assert.match(arch, /low-cost smoke profile is fixed to <code>t3a\.xlarge<\/code>; not rankable/, 'architecture.html must lock the alpha smoke profile and separate it from ranked production');
    assert.match(requirements, /one On-Demand `m7a\.2xlarge` worker for each database system\./, 'requirements must lock the production instance type');
    assert.match(requirements, /dedicated 200 GiB `gp3` data volume/, 'requirements must lock the production data volume');
    const svgBodies = [...arch.matchAll(/<svg\s+class="arch-svg"[\s\S]*?<\/svg>/g)].map(([s]) => s);
    assert.equal(svgBodies.length, 2, 'architecture.html must render both architecture and lifecycle SVG diagrams');
    const decodeEntities = s => s.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&#x([0-9a-fA-F]+);/g, (_, h) => String.fromCodePoint(parseInt(h, 16)));
    const overflows = [];
    for (const svgBody of svgBodies) {
        const rects = [...svgBody.matchAll(/<rect\b([^>]*)\/>/g)].map(([, a]) => {
            const num = k => Number(a.match(new RegExp(`\\b${k}="([^"]+)"`))?.[1] ?? NaN);
            return {x: num('x'), y: num('y'), w: num('width'), h: num('height')};
        }).filter(r => Number.isFinite(r.x) && r.w < 1050);
        for (const [, a, raw] of svgBody.matchAll(/<text\b([^>]*)>([\s\S]*?)<\/text>/g)) {
            const tx = Number(a.match(/\bx="([^"]+)"/)?.[1] ?? NaN), ty = Number(a.match(/\by="([^"]+)"/)?.[1] ?? NaN);
            const cls = a.match(/\bclass="([^"]+)"/)?.[1] ?? '';
            const text = decodeEntities(raw.replace(/<[^>]+>/g, '').trim());
            const candidates = rects.filter(r => tx >= r.x && tx <= r.x + r.w && ty >= r.y && ty <= r.y + r.h).sort((u, v) => (u.w * u.h) - (v.w * v.h));
            if (!candidates.length) continue;
            const box = candidates[0];
            const charW = cls.includes('mono') ? 6.4 : cls.includes('title') ? 6.8 : cls.includes('kicker') ? 7.2 : 5.9;
            const estW = text.length * charW;
            if ((tx - box.x) + estW > box.w - 4) overflows.push(`"${text}" (${Math.round((tx - box.x) + estW)}px > ${box.w}px)`);
        }
    }
    assert.deepEqual(overflows, [], `SVG text overflows in architecture.html:\n${overflows.join('\n')}`);
}


// One consolidated details dialog per row: the rank, every workflow with its time against the fastest and its evidence behind
// its own disclosure, the other runs of this system on this machine, and provenance. Nothing navigates to a second view.
assert.equal(html.includes('drawer'), false, 'No drawer and no back navigation');
detailsOf(rowOf('Doltgres')).click();
assert.equal(macroEl('run-dialog').open, true); assert.equal(macroEl('run-dialog').className, 'dialog');
assert.equal(macroPage.document.activeElement, macroEl('run-body'), 'Opening focuses the scrollable body');
assert.equal(macroEl('run-title').textContent, 'Doltgres'); assert.equal(macroEl('run-subtitle').textContent, 'v1.81.2 · box · 2026-10-10');
assert.equal(macroEl('run-source').href, 'dolt/macro/20261010/campaign_c2000000.json');
assert.equal(classNodes(macroEl('run-body'), 'verdict')[0].textContent, '3rd of 3 by Evaluate agent rollouts: 20.000s, ×2.00 the fastest.');
const workflowItems = () => classNodes(macroEl('run-body'), 'why-item');
assert.deepEqual(workflowItems().map(item => [item.tag, classNodes(item, 'why-name')[0].children[0].textContent]), NAMES_6.map(name => ['details', name]), 'Every workflow, in suite order, each a disclosure');
assert.deepEqual(workflowItems().map(item => classNodes(item.children[0], 'why-value')[0].textContent),
    ['20.000s3rd of 3', 'Incompleteincomplete reason', 'Failedfailed reason', 'Timed outtimed_out reason', '5.000s1st of 3', '20.000s3rd of 3']);
assert.equal(classNodes(macroEl('run-body'), 'bar-tick').length, 3, 'Each timed workflow bar marks the fastest time');
const incompleteText = workflowItems()[1].textContent;
for (const word of ['Incomplete', 'incomplete reason', 'Partial elapsed 12.500s, not a completion time', 'Setup time1.500s', 'Work units', 'Correctness checks', 'Every check (2)', 'Unsupported: MERGE:not implemented, MERGE:timed out, REBASE', 'Attempt record', 'Workload identity SHA-256']) assert.ok(incompleteText.includes(word), word);
// Each reason appears once in an open workflow: its row names the first, the body lists only the others.
const occurrences = (text, part) => text.split(part).length - 1;
assert.equal(occurrences(incompleteText, 'incomplete reason'), 1, 'The open body does not repeat the reason its row names');
const twoReasons = structuredClone(macroData); twoReasons.campaigns[1].attempts.multi_agent.outcome.reasons = ['first cause', 'second cause'];
const twoPage = mount(data, [], twoReasons, '#agent-workflows');
detailsOf(rowOf('Doltgres', twoPage)).click();
assert.deepEqual(['first cause', 'second cause'].map(reason => occurrences(classNodes(twoPage.el('run-body'), 'why-item')[2].textContent, reason)), [1, 1], 'Each of several reasons appears once');
const workBar = classNodes(workflowItems()[0], 'stack-bar')[0];
assert.equal(workBar.getAttribute('aria-label'), 'Work units: 8 succeeded of 8'); assert.equal(workBar.children.length, 1, 'Only parts with a count get a segment');
assert.doesNotMatch(workflowItems()[0].textContent, /Expected units/, 'Valid counters are drawn once, not listed again');
const opsTable = classNodes(workflowItems()[0], 'ops-table')[0];
assert.deepEqual(bodyRows(opsTable).map(tr => tr.children[0].textContent), ['branch', 'commit', 'diff', 'log', 'merge', 'rebase', 'revert', 'reset', 'delete'], 'All nine verbs, exercised or not');
assert.match(bodyRows(opsTable)[4].textContent, /native · dolt_merge.*not exercised/, 'Declared implementation stays separate from observation');
assert.match(bodyRows(opsTable)[0].textContent, /8 ok.*0\.400s/);
assert.equal(descendants(workflowItems()[1]).find(e => e.tag === 'a').href, 'dolt_mysql/macro/20261010/bb_00000021.json', 'Each attempt links its raw record');
const dialogText = macroEl('run-body').textContent;
assert.match(dialogText, /3 of 6 workflows completed.*ProvenanceRun IDc2000000Comparison groupabc123def456Git commit0{40}/s);
assert.doesNotMatch(dialogText, /parquet|\.runs|comparab|preset|uncommitted|cleanup (failed|unknown)|server stop/i, 'Private artifacts are never linked, and a validated run carries no caveat');
macroEl('run-body').scrollTop = 500;
macroEl('close-run').click();
assert.equal(macroEl('run-dialog').open, false);
assert.equal(macroPage.document.activeElement, detailsOf(rowOf('Doltgres')), 'Closing restores focus to the Details button that opened it');
// Evidence is shown as recorded: malformed counters and lists are named, never drawn as proportions, never iterated.
detailsOf(rowOf('MatrixOne')).click();
assert.equal(macroEl('run-body').scrollTop, 0, 'A reopened dialog starts at the top');
const [, contradictedItem, unlistedItem, malformedItem] = workflowItems();
assert.match(malformedItem.textContent, /Not recordedwork counters malformed.*Work counters are malformed or disagree with each other; shown as recorded\..*Expected units8Succeeded9.*Check results are not a list of records.*Enabled"yes".*Passed3/s, 'Malformed evidence is named and still shown as recorded');
assert.equal(classNodes(malformedItem, 'stack-bar').length, 0, 'Malformed counters are never drawn as proportions');
assert.match(malformedItem.textContent, /Unsupported: "BRANCH" \(not a list; kept as recorded\)/, 'A string instead of a list is shown as recorded, not iterated');
assert.match(contradictedItem.textContent, /Worker errorsmalformed, not a list.*Check counts are malformed or disagree with the listed results.*Passed2Failed0.*F1failedmismatch/s, 'Counts that contradict their results are never summarised as passed, but every record is still listed');
assert.match(unlistedItem.textContent, /Unexplained failed operationsmalformed, not a tally.*Check results are not a list of records.*branchnot declared3 okNot recorded/s, 'Non-list results, a null tally and a count without a time all render instead of throwing');
assert.equal(classNodes(unlistedItem, 'check-table').length, 0, 'No check table is built from a non-list');
macroEl('close-run').click();
// Older runs of the same system and machine are chosen from that system's details, and stay hash-addressable.
detailsOf(rowOf('Dolt')).click();
assert.deepEqual(['campaign-c1000000', 'campaign-c4000000'].map(id => [macroEl(id).textContent, macroEl(id).getAttribute('aria-pressed')]),
    [['2026-10-10 12:00 · 6 of 6 workflows completed', 'true'], ['2026-10-08 12:00 · 6 of 6 workflows completed', 'false']], 'Newest first');
macroEl('campaign-c4000000').click();
assert.equal(macroPage.run('window.location.hash'), '#agent-workflows?campaign=c4000000');
assert.deepEqual(rowsOf().slice(0, 2).map(r => [classNodes(r, 'rank-number')[0].textContent, nameOf(r).textContent]), [['1', 'MatrixOne'], ['2', 'Doltgres']], 'The board ranks the chosen run');
assert.equal(macroEl('run-subtitle').textContent, 'v1.81.2 · box · 2026-10-08', 'The dialog follows the chosen run');
macroEl('campaign-c1000000').click();
assert.equal(macroPage.run('window.location.hash'), '#agent-workflows');
macroEl('close-run').click();
assert.equal(macroPage.document.activeElement.id, 'details:dolt_mysql:box', 'Switching runs rebuilds the board, yet closing still returns focus to that row\'s Details');
assert.equal(macroPage.document.activeElement, detailsOf(rowOf('Dolt')), 'and to the rebuilt button, not the detached one');
// Screen readers hear the rank, or that there is none, before the name; the hover text says why a row is unranked.
assert.deepEqual(rowsOf().map(r => classNodes(r.children[0], 'sr-only')[0].textContent), ['Rank 1, ', 'Rank 2, ', 'Rank 3, ']);
assert.equal(classNodes(rowOf('Dolt'), 'rank-number')[0].getAttribute('aria-hidden'), 'true');
// Macrobench filter bar: filter by machine or hosting and reset.
assert.equal(macroEl('macro-shown').textContent, '3 systems');
assert.equal(macroEl('macro-reset-filters').hidden, true);
check(macroPage, 'filter-macroMachine-1');  // uncheck 'box2' -> only 'box' remains
assert.equal(macroEl('macro-shown').textContent, '2 systems · filtered');
assert.equal(macroEl('macro-reset-filters').hidden, false);
assert.deepEqual(rowsOf().map(r => nameOf(r).textContent), ['Dolt', 'Doltgres']);
macroEl('macro-reset-filters').click();
assert.equal(macroEl('macro-shown').textContent, '3 systems');
assert.equal(rowsOf().length, 3);
assert.equal(macroEl('nav-run').href, 'run.html#agent-workflows', 'Run a benchmark opens the wizard on the suite in view');
// A header link to the section already in the URL fires no hashchange, so it reveals and focuses the section itself.
macroPage.run("window.location.hash = '#workflows?workflow=rl_env'");
assert.equal(macroEl('nav-workflows').click().defaultPrevented, true);
assert.equal(macroPage.document.activeElement.id, 'workflow-rl_env', 'Focus moves to the linked definition');
macroPage.run("window.location.hash = '#agent-workflows'");
assert.equal(macroEl('nav-board').click().defaultPrevented, true);
assert.equal(macroPage.document.activeElement.id, 'leaderboard-title', 'The Leaderboard link lands on the board');
macroPage.run("window.location.hash = '#microbench'");
assert.equal(macroEl('nav-board').click().defaultPrevented, false, 'A link to another hash is left to the browser and the hashchange handler');
macroPage.run("window.location.hash = '#agent-workflows'");
// Runs that failed validation are listed below the board with their reason, never on it.
assert.match(macroEl('macro-excluded').textContent, /Runs not on the board2.*failed validation: run from uncommitted sources.*not a JSON object/);
assert.equal(descendants(macroEl('macro-excluded')).find(e => e.tag === 'a').href, 'dolt/macro/20261010/campaign_deadbeef.json', 'Development links the excluded file');


// Ranking: ties share a rank and the next rank skips; a workflow that did not complete is unranked in that column only.
const timed = (id, system, folder, seconds, statuses = V) => campaign(id, system, folder, '2026-10-11T12:00:00+00:00', six(id.slice(1, 8), statuses, seconds), {group: 'aaaa11112222'});
const rankedData = {suite: macroData.suite, excluded: [], campaigns: [timed('d0000001', 'Dolt', 'dolt_mysql', [10, 10, 10, 10, 10, 10]), timed('d0000002', 'Doltgres', 'dolt', [20, 20, 20, 20, 20, 20]),
    timed('d0000003', 'Tie', 'tie', [10, 30, 30, 30, 30, 30]), timed('d0000004', 'Late', 'late', [0, 5, 5, 5, 5, 5], ['failed', ...V.slice(1)])]};
const ranked = mount(data, [], rankedData, '#agent-workflows');
const rankedRows = () => rowsOf(ranked), rankedBoard = () => boards(ranked)[0];
assert.equal(boards(ranked).length, 1, 'One comparison group, one table');
assert.deepEqual(classNodes(ranked.el('macro-groups'), 'group-heading')[0].children.map(c => c.textContent), ['Agent workflows', '4 systems · ranked by Evaluate agent rollouts · lower is better', 'SlowerFaster'], 'One unified table heading with shading key');
assert.deepEqual(rankedRows().map(r => [nameOf(r).textContent, classNodes(r, 'rank-number')[0].textContent]), [['Dolt', '1'], ['Tie', '1'], ['Doltgres', '3'], ['Late', '—']], 'Tied times share a rank; the next rank skips; a failed workflow is listed last, unranked');
assert.deepEqual([classNodes(rankedRows()[3].children[0], 'sr-only')[0].textContent, rankedRows()[3].children[0].title], ['Not ranked, ', 'Not ranked by Evaluate agent rollouts: Failed'], 'An unranked row says so, and why');
assert.deepEqual(rankedRows().map(r => classNodes(r.children[1], 'best-mark').length), [1, 1, 0, 0]);
assert.deepEqual(rankedRows().map(r => r.children[1].style['--strength']), ['0.130', '0.130', '0.080', undefined]);
assert.deepEqual(rankedRows().map(r => classNodes(r.children[2], 'best-mark').length), [0, 0, 0, 1], 'The run that failed one workflow is still the fastest in another');
assert.deepEqual(headRow(rankedBoard()).map(th => th.getAttribute('aria-sort')), [null, 'ascending', null, null, null, null, null, null]);
// The rank metric: a select in the filter bar and each workflow header; the definitions below keep their own selection.
ranked.document.getElementById('rank-ops_agent-aaaa11112222').click();
assert.equal(ranked.run('window.location.hash'), '#agent-workflows?workflow=ops_agent');
assert.deepEqual(headRow(rankedBoard()).map(th => th.getAttribute('aria-sort')), [null, null, null, null, null, 'ascending', null, null]);
assert.equal(ranked.run('state.workflow'), 'rl_env', 'Ranking never changes the definition selection');
ranked.document.getElementById('rank-rl_env-aaaa11112222').click();
assert.equal(ranked.run('window.location.hash'), '#agent-workflows');
ranked.document.getElementById('rank-context_mgmt-aaaa11112222').click();
assert.equal(ranked.run('state.rank'), 'context_mgmt'); assert.equal(ranked.document.activeElement.id, 'rank-context_mgmt-aaaa11112222');
assert.deepEqual(rankedRows().map(r => [nameOf(r).textContent, classNodes(r, 'rank-number')[0].textContent]), [['Late', '1'], ['Dolt', '2'], ['Doltgres', '3'], ['Tie', '4']]);
ranked.document.getElementById('rank-rl_env-aaaa11112222').click();
ranked.document.getElementById('rank-rl_env-aaaa11112222').click();
assert.equal(ranked.run('window.location.hash'), '#agent-workflows?order=desc', 'Sorting the sorted column again flips the order');
assert.equal(headRow(rankedBoard())[1].getAttribute('aria-sort'), 'descending');
assert.equal(ranked.document.getElementById('rank-rl_env-aaaa11112222').getAttribute('aria-label'), 'Evaluate agent rollouts, sorted slowest first; sort fastest first');
assert.deepEqual(rankedRows().map(r => [nameOf(r).textContent, classNodes(r, 'rank-number')[0].textContent]), [['Doltgres', '3'], ['Tie', '1'], ['Dolt', '1'], ['Late', '—']], 'Slowest first keeps each rank, and a workflow without a time stays last');
assert.equal(mount(data, [], rankedData, '#agent-workflows?workflow=ops_agent&order=desc').run('[state.rank, state.rankDesc].join()'), 'ops_agent,true', 'The order is part of the link');
assert.deepEqual(['#agent-workflows?workflow=constructor', '#workflows?workflow=toString'].map(hash => mount(data, [], rankedData, hash).run('[state.rank, state.workflow].join()')), ['rl_env,rl_env', 'rl_env,rl_env'], 'An inherited name is not a workflow');
ranked.document.getElementById('rank-rl_env-aaaa11112222').click();
assert.equal(ranked.run('window.location.hash'), '#agent-workflows');
detailsOf(rankedRows()[2]).click();
assert.equal(classNodes(ranked.el('run-body'), 'verdict')[0].textContent, '3rd of 3 by Evaluate agent rollouts: 20.000s, ×2.00 the fastest.');
assert.deepEqual(classNodes(ranked.el('run-body'), 'why-row').map(b => classNodes(b, 'row-meta').at(-1).textContent), ['3rd of 3', '3rd of 4', '3rd of 4', '3rd of 4', '3rd of 4', '3rd of 4']);
ranked.el('close-run').click();
detailsOf(rankedRows()[3]).click();
assert.equal(classNodes(ranked.el('run-body'), 'verdict')[0].textContent, 'Not ranked by Evaluate agent rollouts: Failed, failed reason.');
assert.equal(classNodes(ranked.el('run-body'), 'bar-tick').length, 5, 'Its five timed workflows still show the fastest time');
ranked.el('close-run').click();
const results = classNodes(ranked.el('workflow-results'), 'result-bars')[0];
assert.deepEqual(results.children.map(li => li.children[0].children[1].textContent), ['Dolt', 'Tie', 'Doltgres', 'Late'], 'A definition shows the measured times, fastest first');
assert.equal(results.children[3].children[2].textContent, 'Failed', 'A system that did not complete the workflow is named with its outcome, never left out');
assert.equal(classNodes(results, 'best-mark').length, 2);
assert.equal(classNodes(results, 'bar-tick').length, 3, 'Each bar is a time against the slowest, with a tick at the fastest');
// A lone time has nothing to beat: no fastest mark, no bar, and no claim that it is the fastest.
const alone = mount(data, [], {suite: macroData.suite, excluded: [], campaigns: [timed('f0000001', 'Solo', 'solo', [7, 7, 7, 7, 7, 7]), timed('f0000002', 'Broken', 'broken', [0, 0, 0, 0, 0, 0], Array(6).fill('failed'))]}, '#workflows');
const aloneResults = classNodes(alone.el('workflow-results'), 'result-bars')[0];
assert.deepEqual([classNodes(aloneResults, 'best-mark').length, classNodes(aloneResults, 'time-bar').length], [0, 0]);
assert.equal(classNodes(alone.el('macro-groups'), 'heat-key').length, 0, 'Nothing is shaded, so there is no key');
classNodes(rowsOf(alone)[0], 'details-button')[0].click();
assert.equal(classNodes(alone.el('run-body'), 'verdict')[0].textContent, '1st of 1 by Evaluate agent rollouts: 7.000s, the only completed time in this comparison.');
assert.deepEqual(classNodes(alone.el('run-body'), 'why-row').map(b => classNodes(b, 'row-meta').at(-1).textContent), Array(6).fill('the only completed time'));
assert.equal(classNodes(alone.el('run-body'), 'time-bar').length, 0);
alone.el('close-run').click();
classNodes(rowsOf(alone)[1], 'details-button')[0].click();
assert.equal(classNodes(alone.el('run-body'), 'verdict')[0].className, 'verdict verdict-unranked', 'A missing rank does not wear the accent');
assert.equal(classNodes(mount(data, [], {suite: macroData.suite, campaigns: [], excluded: []}, '#workflows').el('workflow-results'), 'fine')[0].textContent, 'No results yet.');
macroEl('tab-micro').click();
assert.equal(macroPage.run('window.location.hash'), '#microbench');
assert.equal(macroEl('micro-view').hidden, false);
assert.equal(classNodes(macroEl('groups'), 'score-value').length, 6, 'Switching back renders the microbench unchanged');
macroEl('tab-macro').click();

// Routes: the definitions are a hash under the workflow board, results links stay usable, and neither selection leaks into the other.
const goto = (page, hash) => { page.run(`window.location.hash = ${JSON.stringify(hash)}`); page.run('{ const before = routeKey(); applyHash(); if (before !== routeKey()) render(); reveal(); }'); };
goto(macroPage, '#workflows');
assert.equal(macroPage.run('state.suite'), 'macro'); assert.equal(macroEl('macro-view').hidden, false); assert.equal(macroEl('macro-more').hidden, false);
const picker = macroEl('selectors-workflow');
assert.deepEqual(picker.children.map(b => b.textContent), NAMES_6.map((name, i) => name + ['S1 RL environment', 'S2 Context management', 'S3 Multi-agent', 'S4 Dev agent', 'S5 Ops agent', 'S6 Data agent'][i]), 'A picker named by the job, upstream id below');
assert.deepEqual(picker.children.map(b => b.getAttribute('aria-label')), ['Evaluate agent rollouts (S1 RL environment)', 'Manage context history (S2 Context management)', 'Coordinate multiple agents (S3 Multi-agent)', 'Develop against live data (S4 Dev agent)', 'Recover from a bad deployment (S5 Ops agent)', 'Ingest and merge data (S6 Data agent)']);
assert.equal(picker.children[0].getAttribute('aria-pressed'), 'true');
assert.equal(macroEl('workflow-name').textContent, 'Evaluate agent rollouts'); assert.equal(macroEl('workflow-id').textContent, 'S1 RL environment');
assert.equal(macroEl('workflow-caption').textContent, 'Try several repair paths from a faulty task state, compare their results, then discard the branches.');
assert.deepEqual(macroEl('workflow-ops').children.map(li => li.textContent), ['branch from a recorded commit', 'commit checkpoints', 'read across branches', 'diff', 'delete']);
assert.equal(macroEl('workflow-text').tag, 'details', 'The full text description is an optional disclosure');
assert.equal(classNodes(macroEl('workflows'), 'workflow-table').length + classNodes(macroEl('workflows'), 'summary').length, 0, 'No results table inside the definitions');
assert.doesNotMatch(html.slice(at('id="workflows"'), at('id="macro-guide"')), /selectors-suite|sort-button|<table/, 'No result controls in the definitions markup');
macroEl('workflow-ops_agent').click();
assert.equal(macroPage.run('window.location.hash'), '#workflows?workflow=ops_agent');
assert.equal(macroPage.document.activeElement.id, 'workflow-ops_agent', 'Choosing a workflow keeps focus on it');
assert.equal(macroPage.run('state.rank'), 'rl_env', 'Choosing a definition never re-ranks the board');
goto(macroPage, '#agent-workflows?workflow=dev_agent');
assert.equal(macroPage.run('state.rank'), 'dev_agent'); assert.equal(macroPage.run('state.workflow'), 'ops_agent', 'An old per-workflow results link ranks by it and leaves the definition alone');
assert.equal(macroEl('nav-workflows').href, '#workflows?workflow=ops_agent');
goto(macroPage, '#workflows');
assert.equal(macroPage.run('state.workflow'), 'ops_agent', 'A bare definitions link keeps the last definition');
goto(macroPage, '#leaderboard');
assert.equal(macroPage.run('state.suite'), 'macro'); assert.equal(macroPage.run('state.rank'), 'dev_agent', 'Going back to the leaderboard changes no ranking');
assert.equal(macroEl('nav-board').href, '#agent-workflows?workflow=dev_agent');
goto(macroPage, '#macro-guide');
assert.equal(macroPage.run('state.suite'), 'macro'); assert.equal(macroEl('macro-guide').open, true);
for (const hash of ['#dataset', '#methodology', '#microbench']) { goto(macroPage, hash); assert.equal(macroPage.run('state.suite'), 'micro', `${hash} belongs to the microbench`); }
assert.equal(macroEl('methodology').open, true);
goto(macroPage, '#agent-workflows');
assert.equal(macroPage.run('state.rank'), 'rl_env', 'The bare results link ranks by the first workflow');
assert.equal(mount(data, [], macroData, '#agent-workflows?workflow=ops_agent').run('state.rank'), 'ops_agent', 'The old workflow link ranks by that workflow');
assert.equal(mount(data, [], macroData, '#agent-workflows?workflow=s9').run('state.rank'), 'rl_env', 'An unknown workflow ranks by the first');
assert.equal(mount(data, [], macroData, '#workflows?workflow=data_agent').run('state.workflow'), 'data_agent', 'A definition is a deep link');
assert.equal(mount(data, [], macroData, '#workflows?workflow=data_agent').run('state.suite'), 'macro');
assert.equal(mount(data, [], macroData, '#workflows?workflow=s9').run('state.workflow'), 'rl_env', 'An unknown definition falls back to the first');
assert.equal(mount(data, [], macroData, '#agent-workflows?campaign=nope').run('state.campaign.size'), 0, 'An unknown campaign id selects nothing');
// A mangled link never prevents the first render: a malformed pair is ignored and its valid siblings survive.
for (const [hash, rank, note] of [['#agent-workflows?workflow=%&view=old', 'rl_env', 'a bare % in a value'], ['#agent-workflows?workflow=%E0%A4%A&view=old', 'rl_env', 'truncated UTF-8 in a value'],
        ['#agent-workflows?%zz=1&workflow=ops_agent&view=old', 'ops_agent', 'a malformed key beside a valid pair'], ['#agent-workflows?view=old&workflow=ops_agent&group=%', 'ops_agent', 'a malformed trailing pair']]) {
    const mangled = mount(data, [], macroData, hash);
    assert.equal(mangled.run('state.suite'), 'macro', note); assert.equal(mangled.run('state.rank'), rank, note);
    assert.equal(boards(mangled).length, 1, `${note}: the table rendered`);
}
assert.equal(mount(data, [], macroData, '#agent-workflows?a%ZZ=1&b=%C3&c=ok').run('JSON.stringify(parseHash().params)'), '{"c":"ok"}', 'Only the well-formed pair is kept');
// Empty datasets are honest: no fabricated rows, one sentence in the table shell, and no rank control without a result.
const emptyRow = page => { const tables = boards(page); assert.equal(tables.length, 1); assert.deepEqual(headRow(tables[0]).map(firstText), ['System', ...NAMES_6, 'Completed'], 'The table shell stays'); const rows = bodyRows(tables[0]); assert.equal(rows.length, 1); assert.equal(rows[0].className, 'empty-row'); assert.equal(rows[0].children[0].getAttribute('colspan'), '8'); return rows[0].children[0]; };
for (const [macro, note] of [[null, 'no workflow data built'], [{suite: macroData.suite, campaigns: [], excluded: []}, 'no campaigns']]) {
    const empty = mount(data, [], macro, '#agent-workflows');
    assert.equal(emptyRow(empty).textContent, 'No published results yet. Results appear here once a validated run is published.', note);
    assert.equal(descendants(emptyRow(empty)).filter(e => e.tag === 'a').length, 0, `${note}: no instruction to run anything`);
    empty.document.getElementById('tab-micro').click();
    assert.equal(classNodes(empty.el('groups'), 'score-value').length, 6, `${note}: the microbench still renders`);
}
assert.equal(mount(data, [], null, '').run('state.suite'), 'micro', 'Without workflow data, the page opens on the microbench');
// More workflows add columns and picker items; an upstream scenario without a page description keeps its upstream name and draws nothing.
const manySuite = {suite: 'macro-1', scenarios: {...Object.fromEntries(KEYS.map(k => [k, k])), s7_new: 'S7 New scenario', s8_new: 'S8 Another', s9_new: 'S9 Third'}};
const nine = Object.fromEntries(Object.keys(manySuite.scenarios).map((k, i) => [k, attempt(`bb_0000009${i}`, 'verified')]));
const many = mount(data, [], {suite: manySuite, campaigns: [campaign('e0000001', 'Dolt', 'dolt_mysql', '2026-10-10T12:00:00+00:00', nine)], excluded: []}, '#workflows?workflow=s8_new');
assert.equal(many.elements.get('selectors-workflow').children.length, 9);
assert.equal(many.elements.get('workflow-name').textContent, 'S8 Another');
assert.equal(many.elements.get('workflow-scene').children[0].className, 'diagram-missing');
assert.equal(many.elements.get('workflow-scene').children[0].textContent, 'Diagram not available. See the attempt details.');
assert.equal(many.elements.get('workflow-caption').hidden, true, 'No invented caption'); assert.equal(many.elements.get('workflow-ops-block').hidden, true, 'No invented operations'); assert.equal(many.elements.get('workflow-text').hidden, true);
assert.equal(many.observers.intersection.length, 0, 'Nothing is animated for an unknown workflow');
assert.equal(headRow(boards(many)[0]).length, 11, 'Nine workflow columns between system and completion');
assert.equal(classNodes(many.elements.get('macro-groups'), 'done-count')[0].textContent, '9 / 9');

// Production and alpha: the same page with no developer wizard affordances; alpha still links bundled README and excluded records.
// Phase badge: shown in local ('local') and alpha ('alpha'), completely removed from the DOM in prod.
assert.equal(macroEl('phase-badge').removed, undefined);
assert.equal(macroEl('phase-badge').hidden, false);
assert.equal(macroEl('phase-badge').textContent, 'local');
assert.equal(macroEl('phase-badge').getAttribute('data-phase'), 'local');
const prod = mount(data, [], macroData, '#agent-workflows', 'prod');
assert.equal(prod.elements.get('nav-run').removed, true, 'The run wizard is not in the production page');
assert.equal(prod.elements.get('phase-badge').removed, true, 'Production removes the phase badge completely');
assert.equal(prod.elements.get('doc-nav').href, 'https://github.com/elaineang/db-fork/blob/main/leaderboard/README.md');
assert.equal(prod.elements.get('doc-macro').href, 'https://github.com/elaineang/db-fork/blob/main/leaderboard/README.md#agent-workflows');
assert.equal(rowsOf(prod).length, 3);
prod.document.getElementById('rank-data_agent-abc123def456').click();
assert.equal(prod.run('window.location.hash'), '#agent-workflows?workflow=data_agent');
assert.equal(descendants(prod.elements.get('macro-excluded')).find(e => e.tag === 'a'), undefined, 'Production never links a file it does not serve');
assert.equal(mount(data, [], macroData, '', 'prod').run('state.suite'), 'macro', 'Production opens on the agent workflows');
assert.equal(mount(data, [], macroData, '#microbench', 'prod').run('state.suite'), 'micro', 'The microbench stays reachable');
const alpha = mount(data, [], macroData, '#agent-workflows', 'alpha');
assert.equal(alpha.elements.get('nav-run').removed, true, 'The macOS run wizard is not in the alpha AWS page');
assert.equal(alpha.elements.get('phase-badge').removed, undefined, 'Alpha keeps the phase badge');
assert.equal(alpha.elements.get('phase-badge').hidden, false);
assert.equal(alpha.elements.get('phase-badge').textContent, 'alpha');
assert.equal(alpha.elements.get('phase-badge').getAttribute('data-phase'), 'alpha');
assert.equal(alpha.elements.get('doc-nav').href, 'README.md', 'Alpha serves its bundled README');
assert.equal(descendants(alpha.elements.get('macro-excluded')).find(e => e.tag === 'a').href, 'dolt/macro/20261010/campaign_deadbeef.json', 'Alpha links bundled excluded records');
assert.equal(mount(data, [], macroData, '', 'alpha').run('state.suite'), 'macro', 'Alpha opens on the agent workflows');
const archHtml = fs.readFileSync(`${__dirname}/architecture.html`, 'utf8');
assert.equal((archHtml.match(/<svg class="arch-svg"/g) || []).length, 2, 'Architecture page carries end-to-end and single-run lifecycle SVG diagrams');
for (const token of ['STAGE 1', 'STAGE 2', 'MICROBENCH LANE', 'MACROBENCH LANE', 'STAGE 4', 'STAGE 5', 'aws_alpha.sh', 'aws_worker.sh', 'site.py']) {
    assert.ok(archHtml.includes(token), `architecture.html diagram includes ${token}`);
}
const prodEmpty = mount([], [{system: 'Old', machine: 'box', source: 'dolt/results/20261001/box.json', reason: 'server did not start'}], {suite: macroData.suite, campaigns: [], excluded: []}, '', 'prod');
assert.equal(emptyRow(prodEmpty).textContent, 'No published results yet. Results appear here once a validated run is published.');
prodEmpty.document.getElementById('tab-micro').click();
assert.equal(prodEmpty.elements.get('empty-title').textContent, 'No published results yet');
assert.equal(prodEmpty.elements.get('empty-description').textContent, 'Microbenchmark results appear here once they are published. 1 run without a result is listed below.');
assert.equal(prodEmpty.elements.get('updated').textContent, 'No published runs yet');
assert.match(prodEmpty.elements.get('excluded').textContent, /Runs without a result.*server did not start/, 'A published error record stays visible');
assert.equal(prodEmpty.el('excluded-panel').open, true, 'With no result at all, the reasons start open');

// The branch-history scene: a Canvas that plays one gentle sequence when it is seen, then holds; nothing loops, nothing is a control.
assert.doesNotMatch(script, /DIAGRAM_STEPS|diagram-ring|setDiagramStep|playDiagram|setInterval|\.animate\(|<svg/, 'The PNG step player and every tween API are gone');
assert.doesNotMatch(html, /\bPrev\b|Replay|diagram-play|diagram-step|diagram-hint/, 'No playback buttons, no step captions');
assert.doesNotMatch(css, /infinite|diagram-ring|diagram-box|diagram-settle/, 'No endless animation, no rings or boxes');
assert.match(css, /\.workflow-scene \{ display: block; width: 100%;/);
assert.match(css, /@media \(prefers-reduced-motion: reduce\) \{[^}]*\}[^}]*animation: none !important; transition: none !important/);
assert.match(html, /<figcaption class="diagram-disclaimer">Illustrative branch history, not a trace of any attempt: commit spacing and counts are not measured\.<\/figcaption>/, 'A static caption says what the figure is');
const scenePage = mount(data, [], macroData, '#workflows?workflow=rl_env');
const sceneEl = scenePage.el;
const canvasOf = page => page.el('workflow-scene').children[0];
let canvas = canvasOf(scenePage);
assert.equal(canvas.tag, 'canvas'); assert.equal(canvas.className, 'workflow-scene'); assert.equal(canvas.getAttribute('role'), 'img');
assert.match(canvas.getAttribute('aria-label'), /^Evaluate agent rollouts, branch history: main keeps only its seeded commits; .*deleted without merging\.$/, 'The Canvas is named by the full truthful description');
assert.equal(sceneEl('workflow-alt').textContent, scenePage.run('WORKFLOWS.rl_env.alt'), 'The description is also readable as text');
const PNG_SIZES = Object.fromEntries(fs.readdirSync(`${__dirname}/workflow-diagrams`).filter(f => f.endsWith('.png')).map(f => {
    const bytes = fs.readFileSync(`${__dirname}/workflow-diagrams/${f}`);
    assert.equal(bytes.subarray(0, 8).toString('hex'), '89504e470d0a1a0a', `${f} is a PNG`);
    return [f.replace('.png', ''), [bytes.readUInt32BE(16), bytes.readUInt32BE(20)]];
}));
assert.deepEqual(Object.keys(PNG_SIZES).sort(), [...KEYS].sort(), 'Exactly the six workflow diagrams ship as the PNG fallback');
for (const key of KEYS) assert.equal(scenePage.run(`DIAGRAM_HEIGHTS.${key}`) * 2, PNG_SIZES[key][1], `${key}: the fallback height matches the PNG`);
assert.equal(canvas.width, 1800); assert.equal(canvas.height, (68 + 3 * 76) * 2, 'The backing store is the CSS size times the device pixel ratio');
assert.equal(canvas.style.width, '900px');
const ops = () => canvas.getContext('2d').ops, count = name => ops().filter(([op]) => op === name).length;
assert.deepEqual(ops()[0].slice(0, 2), ['setTransform', [2, 0, 0, 2, 0, 0]]);
assert.equal(count('clearRect'), 1, 'The first frame is drawn at once, before any animation');
assert.equal(scenePage.pendingFrames().length, 0, 'Nothing animates until the scene is visible');
assert.equal(scenePage.observers.intersection.length, 1); assert.equal(scenePage.observers.intersection[0].targets[0], canvas);
assert.equal(scenePage.observers.resize.length, 1); assert.equal(scenePage.observers.resize[0].targets[0], sceneEl('workflow-scene'));
const io = () => scenePage.observers.intersection.at(-1), ro = () => scenePage.observers.resize.at(-1);
io().callback([{isIntersecting: true}]);
assert.equal(scenePage.pendingFrames().length, 1, 'Entering the viewport starts the one sequence');
scenePage.frame(16);
assert.equal(count('clearRect'), 2); assert.equal(scenePage.pendingFrames().length, 1, 'Each frame requests the next while the sequence runs');
scenePage.frame(1000);
assert.ok(Math.abs(scenePage.run('scene.elapsed') - 1.016) < 1e-9);
scenePage.document.hidden = true; scenePage.document.dispatch('visibilitychange');
assert.equal(scenePage.pendingFrames().length, 0, 'A hidden tab suspends the frame loop');
scenePage.clock.now += 60000;
scenePage.document.hidden = false; scenePage.document.dispatch('visibilitychange');
assert.equal(scenePage.pendingFrames().length, 1);
scenePage.frame(16);
assert.ok(Math.abs(scenePage.run('scene.elapsed') - 1.032) < 1e-9, 'Hidden time does not advance the sequence');
io().callback([{isIntersecting: false}]);
assert.equal(scenePage.pendingFrames().length, 0, 'Scrolling the scene out suspends it');
io().callback([{isIntersecting: true}]); scenePage.frame(16);
const captionBefore = sceneEl('workflow-caption').textContent, altBefore = canvas.getAttribute('aria-label');
scenePage.frame(2000); scenePage.frame(2000);
assert.equal(scenePage.run('scene.done'), true); assert.equal(scenePage.pendingFrames().length, 0, 'The sequence ends within 4.5 seconds and never loops');
assert.equal(scenePage.run('scene.elapsed') >= 4.5, true);
assert.equal(sceneEl('workflow-caption').textContent, captionBefore); assert.equal(canvas.getAttribute('aria-label'), altBefore, 'No caption changes during or after playback');
const frames = count('clearRect');
io().callback([{isIntersecting: false}]); io().callback([{isIntersecting: true}]);
assert.equal(scenePage.pendingFrames().length, 0, 'Scrolling away and back does not replay');
ro().callback();
assert.equal(count('clearRect'), frames + 1, 'A resize redraws the held final state');
assert.equal(scenePage.run("played.has('rl_env')"), true);
// Switching workflow tears the old scene down and mounts the next; coming back shows the held state without a replay.
sceneEl('workflow-context_mgmt').click();
assert.equal(io.call(null) && scenePage.observers.intersection[0].disconnected, true); assert.equal(scenePage.observers.resize[0].disconnected, true);
assert.equal(scenePage.listeners.visibilitychange.length, 1, 'The old visibility listener is removed and only the new scene listens');
canvas = canvasOf(scenePage);
assert.match(canvas.getAttribute('aria-label'), /^Manage context history, branch history: /);
assert.equal(scenePage.observers.intersection.length, 2); assert.equal(scenePage.pendingFrames().length, 0);
io().callback([{isIntersecting: true}]); assert.equal(scenePage.pendingFrames().length, 1);
sceneEl('workflow-context_mgmt').click();
assert.equal(canvasOf(scenePage), canvas, 'Re-rendering the same workflow keeps the running scene');
assert.equal(scenePage.pendingFrames().length, 1);
goto(scenePage, '#microbench');
assert.equal(scenePage.pendingFrames().length, 0, 'Leaving the workflows cancels the frame'); assert.equal(scenePage.observers.intersection[1].disconnected, true);
assert.equal(scenePage.run('scene.key'), 'op:time_to_first_query', 'The microbench mounts its own operation scene in its place');
assert.equal(scenePage.listeners.visibilitychange.length, 1, 'Only the scene on screen listens');
goto(scenePage, '#workflows?workflow=rl_env');
assert.equal(scenePage.observers.intersection.length, 3, 'A scene that already played is drawn complete, with no viewport observer and no frame');
assert.equal(scenePage.observers.intersection[2].disconnected, true); assert.equal(scenePage.listeners.visibilitychange.length, 0);
assert.equal(scenePage.pendingFrames().length, 0); assert.equal(canvasOf(scenePage).getContext('2d').ops.filter(([op]) => op === 'arc').length, scenePage.run('SCENES.rl_env.items.filter(i => i.kind === "dot").length'), 'Every commit bead of the final state is drawn');
assert.equal(scenePage.observers.resize.length, 4); assert.equal(ro().targets[0], sceneEl('workflow-scene'), 'A finished scene still follows its host');
sceneEl('workflow-scene').clientWidth = 480; ro().callback();
assert.equal(canvasOf(scenePage).style.width, '480px'); assert.equal(canvasOf(scenePage).width, 960); assert.equal(canvasOf(scenePage).height, (68 + 3 * 76) * 2, 'A narrower host redraws the held scene at its width');
assert.equal(canvasOf(scenePage).getContext('2d').ops.filter(([op]) => op === 'clearRect').length, 2);
scenePage.run('window.devicePixelRatio = 3'); ro().callback();
assert.equal(canvasOf(scenePage).width, 1440); assert.deepEqual(canvasOf(scenePage).getContext('2d').ops.filter(([op]) => op === 'setTransform').at(-1)[1], [3, 0, 0, 3, 0, 0], 'A device pixel ratio change resizes the backing store');
assert.equal(canvasOf(scenePage).getContext('2d').ops.filter(([op]) => op === 'clearRect').length, 3); assert.equal(scenePage.pendingFrames().length, 0);
// Reduced motion requested while the sequence runs: it stops, the complete history is drawn and held, and the listener goes away with the scene.
const midway = mount(data, [], macroData, '#workflows?workflow=multi_agent');
midway.observers.intersection[0].callback([{isIntersecting: true}]); midway.frame(16); midway.frame(800);
assert.equal(midway.pendingFrames().length, 1); assert.equal(midway.media.listeners.length, 1);
midway.media.change(true);
assert.equal(midway.pendingFrames().length, 0); assert.equal(midway.run('scene.done'), true); assert.equal(midway.run("played.has('multi_agent')"), true);
assert.equal(canvasOf(midway).getContext('2d').ops.filter(([op]) => op === 'arc').length >= midway.run('SCENES.multi_agent.items.filter(i => i.kind === "dot").length'), true, 'The final frame is drawn at once');
midway.observers.intersection[0].callback([{isIntersecting: false}]); midway.observers.intersection[0].callback([{isIntersecting: true}]);
assert.equal(midway.pendingFrames().length, 0, 'Nothing restarts');
goto(midway, '#microbench');
assert.equal(midway.media.listeners.length, 0, 'The workflow scene stops listening, and with reduced motion on the operation scene is drawn complete without listening');
assert.equal(midway.run('scene.key'), 'op:time_to_first_query');
// Reduced motion: the complete history at once, no frame loop, no observers.
const still = mount(data, [], macroData, '#workflows?workflow=ops_agent', 'local', {reduced: true});
assert.equal(still.observers.intersection.length, 0); assert.equal(still.pendingFrames().length, 0); assert.equal(still.media.listeners.length, 0);
assert.equal(canvasOf(still).getContext('2d').ops.filter(([op]) => op === 'arc').length, still.run('SCENES.ops_agent.items.filter(i => i.kind === "dot").length'));
assert.ok(canvasOf(still).getContext('2d').ops.filter(([op]) => op === 'roundRect').length >= 1, 'Ref chips are drawn');
assert.equal(still.observers.resize.length, 1, 'A static scene still observes its host');
still.el('workflow-scene').clientWidth = 600; still.observers.resize[0].callback();
assert.equal(canvasOf(still).style.width, '600px'); assert.equal(canvasOf(still).getContext('2d').ops.filter(([op]) => op === 'clearRect').length, 2, 'A static scene redraws at the new width without any frame');
assert.equal(canvasOf(mount(data, [], macroData, '#workflows', 'local', {reduced: true, width: 300})).style.width, '300px', 'The canvas takes the host width as it is, never a wider minimum');
const labelsIn = ops => ops.filter(([op, , st]) => op === 'fillText' && /^12px/.test(st.font)).map(([, args]) => args[0]);
const stillOps = canvasOf(still).getContext('2d').ops, redraw = stillOps.map(([op]) => op).lastIndexOf('clearRect');
assert.equal(labelsIn(stillOps.slice(0, redraw)).length, still.run('SCENES.ops_agent.items.filter(i => i.kind === "text").length'), 'The 900px frame draws every label');
assert.deepEqual(labelsIn(stillOps.slice(redraw + 1)), labelsIn(stillOps.slice(0, redraw)).filter(text => !/^reset/.test(text)), 'The 600px redraw keeps every label that has room and leaves out the one that has none');
const chips = canvasOf(mount(data, [], macroData, '#workflows?workflow=data_agent', 'local', {reduced: true, width: 240})).getContext('2d').ops.filter(([op]) => op === 'roundRect').map(([, [x, , width]]) => [x, x + width]);
assert.ok(chips.length >= 1 && chips.every(([left, right]) => left >= 1.25 && right <= 240 - 1.25), `Every ref chip, border included, lies inside a 240px canvas: ${JSON.stringify(chips)}`);
// Legibility: in every scene's finished frame, no text touches a line, curve, commit, read or write mark, another ref
// chip or another text, at every width from 520px. The fake canvas measures 6.5px per character.
const overlapsIn = ops => {
    const frame = ops.slice(ops.map(([op]) => op).lastIndexOf('clearRect') + 1), segs = [], dots = [], rects = [], texts = [];
    let path = [], cur = null;
    for (const [op, args, st] of frame) {
        if (op === 'beginPath') path = [];
        else if (op === 'moveTo') cur = args;
        else if (op === 'lineTo') { path.push({seg: [...cur, ...args], pad: st.lineWidth / 2}); cur = args; }
        else if (op === 'arc') path.push({dot: args.slice(0, 3)});
        else if (op === 'roundRect') path.push({chip: [args[0], args[1], args[0] + args[2], args[1] + args[3]]});
        else if (op === 'stroke') { segs.push(...path.filter(p => p.seg)); dots.push(...path.filter(p => p.dot).map(p => p.dot)); }
        else if (op === 'fill') { dots.push(...path.filter(p => p.dot).map(p => p.dot)); rects.push(...path.filter(p => p.chip).map(p => p.chip)); }
        else if (op === 'fillRect') rects.push([args[0], args[1], args[0] + args[2], args[1] + args[3]]);
        else if (op === 'fillText') {
            const width = String(args[0]).length * 6.5, px = Number(st.font.match(/([\d.]+)px/)[1]), left = st.align === 'center' ? args[1] - width / 2 : st.align === 'right' ? args[1] - width : args[1];
            texts.push({text: args[0], box: [left, args[2] - px * .55, left + width, args[2] + px * .55]});
        }
    }
    const near = (b, x, y, pad) => x >= b[0] - pad && x <= b[2] + pad && y >= b[1] - pad && y <= b[3] + pad;
    const meet = (a, b) => a[0] < b[2] && b[0] < a[2] && a[1] < b[3] && b[1] < a[3];
    const chipOf = t => rects.find(r => near(r, (t.box[0] + t.box[2]) / 2, (t.box[1] + t.box[3]) / 2, 0));
    const found = [];
    for (const [i, t] of texts.entries()) {
        const own = chipOf(t);
        if (segs.some(({seg: [x1, y1, x2, y2], pad}) => Array.from({length: 21}, (_, k) => k / 20).some(u => near(t.box, x1 + (x2 - x1) * u, y1 + (y2 - y1) * u, pad + 1)))) found.push(`${t.text} touches a line`);
        if (dots.some(([x, y, r]) => near(t.box, x, y, r + 1))) found.push(`${t.text} touches a commit or mark`);
        if (rects.some(r => r !== own && meet(t.box, r))) found.push(`${t.text} touches a chip or mark`);
        for (const u of texts.slice(i + 1)) if (!(own && chipOf(u) === own) && meet(t.box, u.box)) found.push(`${t.text} touches ${u.text}`);
    }
    return found;
};
for (const width of [520, 600, 760, 900, 1000, 1300]) {
    for (const key of page.run('Object.keys(SCENES)')) {
        const shown = mount(data, [], macroData, `#workflows?workflow=${key}`, 'local', {reduced: true, width});
        assert.deepEqual(overlapsIn(canvasOf(shown).getContext('2d').ops), [], `${key} at ${width}px: every label sits clear of the drawing`);
    }
    for (const op of page.run('allRows.map(r => r.op)')) {
        const shown = mount(data, [], macroData, `#dataset?operation=${op}`, 'local', {reduced: true});
        shown.el('operation-scene').clientWidth = width; for (const observer of shown.observers.resize) observer.callback();
        assert.deepEqual(overlapsIn(shown.el('operation-scene').children[0].getContext('2d').ops), [], `${op} at ${width}px: every label sits clear of the drawing`);
    }
}
// Without a 2D context, whether getContext returns null or throws, the host itself shows the PNG with its full text and nothing is scheduled.
for (const [options, note] of [[{canvas: false}, 'getContext returns null'], [{throwing: true}, 'getContext throws']]) {
    const fallback = mount(data, [], macroData, '#workflows?workflow=dev_agent', 'local', options);
    const shown = fallback.el('workflow-scene').children;
    assert.equal(shown.length, 1, note); assert.equal(shown[0].tag, 'img', `${note}: the image is the host's visible child, not hidden inside a canvas`);
    assert.equal(shown[0].getAttribute('src'), 'workflow-diagrams/dev_agent.png'); assert.equal(shown[0].getAttribute('width'), '900');
    assert.match(shown[0].getAttribute('alt'), /^Develop against live data, branch history: production keeps committing/);
    assert.equal(fallback.observers.intersection.length + fallback.observers.resize.length + fallback.pendingFrames().length + fallback.media.listeners.length, 0, `${note}: nothing scheduled`);
}
// The scenes are grounded in the scenario sources: every ref sits on a drawn commit, every branch grows from an existing one, and each history ends within the sequence.
const SCENES = scenePage.run('SCENES');
const dots = (scene, lane) => scene.items.filter(i => i.kind === 'dot' && i.lane === lane);
const hasDot = (scene, lane, x, before = Infinity) => dots(scene, lane).some(d => Math.abs(d.x - x) < 1e-9 && d.t <= before + 1e-9);
for (const [key, scene] of Object.entries(SCENES)) {
    const lanes = scene.lanes.map(l => l[0]);
    for (const item of scene.items) {
        const last = item.kind === 'ref' ? Math.max(item.at.at(-1)[0], (item.end ?? 0) + .35) : item.kind === 'text' ? item.t + .35 : item.kind === 'dot' ? item.t + .3 : item.t1;
        assert.ok(last <= 4.5, `${key}: ${item.kind} ends at ${last}, inside the 4.5 s sequence`);
        if (item.kind === 'curve') { assert.ok(lanes.includes(item.from) && lanes.includes(item.to)); assert.ok(hasDot(scene, item.from, item.x1, item.t0), `${key}: a branch curve grows from an existing commit at ${item.x1} on ${item.from}`); }
        if (item.kind === 'ref') for (const [t, x, lane = item.lane] of item.at) assert.ok(hasDot(scene, lane, x, t), `${key}: ref ${item.name} at ${x} on ${lane} sits on a commit that exists by ${t}`);
        if (item.kind === 'line') assert.ok(lanes.includes(item.lane));
    }
    assert.ok(scene.items.filter(i => i.kind === 'text').length <= 6, `${key}: a few short labels, not a caption track`);
}
const refOf = (key, name) => SCENES[key].items.find(i => i.kind === 'ref' && i.name === name);
const joins = (key, lane) => SCENES[key].items.filter(i => i.kind === 'curve' && i.to === lane);
assert.equal(joins('rl_env', 'main').length, 0, 'S1 never merges'); assert.equal(refOf('rl_env', 'main').at.length, 1, 'S1 main never moves');
for (const name of ['task', 'repair A', 'repair B']) assert.ok(refOf('rl_env', name).end, `S1 releases ${name}`);
assert.doesNotMatch(JSON.stringify(SCENES.rl_env), /merge/);
const forksTo = (key, lane) => SCENES[key].items.filter(i => i.kind === 'curve' && i.to === lane);
assert.ok(forksTo('rl_env', 'rb')[0].t0 >= Math.max(...dots(SCENES.rl_env, 'ra').map(d => d.t)), 'S1: repair B forks only after repair A has committed its last checkpoint, one rollout at a time');
const s2main = refOf('context_mgmt', 'main').at;
assert.equal(JSON.stringify(s2main.filter(k => k[2]).map(k => k[2])), JSON.stringify(['rb', 'rb', 'rb', 'rb', 'rb', 'ra']), 'S2: main fast-forwards onto the selected candidate and is later rebased onto the retained one');
assert.equal(dots(SCENES.context_mgmt, 'main').filter(d => d.x > .28).length, 0, 'S2: the fast-forward adds no commit on the main lane');
assert.ok(SCENES.context_mgmt.items.some(i => i.kind === 'text' && /revert commit/.test(i.text)), 'S2: the revert is a new commit');
assert.ok(forksTo('context_mgmt', 'rb')[0].t0 >= forksTo('context_mgmt', 'ra')[0].t1, 'S2: the candidates are built in turn');
const s2ff = s2main.find(k => k[2] === 'rb')[0], s2revert = dots(SCENES.context_mgmt, 'rb').at(-1).t, s2rebase = s2main.find(k => k[2] === 'ra')[0];
assert.ok(SCENES.context_mgmt.items.filter(i => i.kind === 'line' && i.lane === 'rb').every(l => l.t0 >= s2ff), 'S2: later work on the selected candidate starts after main adopts it');
assert.ok(SCENES.context_mgmt.items.filter(i => i.kind === 'line' && i.lane === 'ra').every(l => l.t0 > s2revert && l.t0 < s2rebase), 'S2: the replayed history starts after the revert and lands before main moves onto it');
assert.ok(dots(SCENES.context_mgmt, 'rb').filter(d => d.fade).length >= 3 && dots(SCENES.context_mgmt, 'ra').filter(d => d.t > 3).length === 2, 'S2: later history is replayed after A and the old copies soften');
assert.ok(SCENES.context_mgmt.items.some(i => i.kind === 'ref' && i.name === 'candidate B' && i.end), 'S2: candidates are released');
assert.equal(joins('multi_agent', 'main').length, 2); assert.ok(joins('multi_agent', 'main')[0].from === 'a' && joins('multi_agent', 'main')[1].from === 'b' && joins('multi_agent', 'main')[0].t1 <= joins('multi_agent', 'main')[1].t0, 'S3: A then B, serially');
assert.match(SCENES.multi_agent.items.find(i => i.kind === 'text' && /serial/.test(i.text)).text, /fixed policy/);
assert.match(scenePage.run('WORKFLOWS.multi_agent.alt'), /how many merge commits that leaves depends on the backend/, 'S3 does not promise two physical merge commits');
assert.equal(joins('dev_agent', 'prod').length, 0, 'S4 never joins production'); assert.ok(!refOf('dev_agent', 'production').end && refOf('dev_agent', 'feature').end);
assert.ok(dots(SCENES.dev_agent, 'f').every(d => d.fade) && dots(SCENES.dev_agent, 'f2').length >= 3, 'S4: the feature is replayed onto the newer base and the old copies soften');
assert.doesNotMatch(scenePage.run('WORKFLOWS.dev_agent.alt'), /\bmerge[sd]?\b/i);
const reset = refOf('ops_agent', 'production').at;
assert.equal(JSON.stringify(reset.at(-1).slice(1)), '[0.32]', 'S5: production moves back to the original checkpoint');
assert.ok(hasDot(SCENES.ops_agent, 'prod', .32, 1) && !dots(SCENES.ops_agent, 'prod').find(d => Math.abs(d.x - .32) < 1e-9).fade, 'S5: that checkpoint is an existing, unsoftened production commit');
assert.equal(dots(SCENES.ops_agent, 'prod').filter(d => d.t >= reset.at(-2)[0]).length, 0, 'S5: the reset adds no commit');
assert.ok(!dots(SCENES.ops_agent, 'ia').concat(dots(SCENES.ops_agent, 'ib')).some(d => Math.abs(d.x - .32) < 1e-9), 'S5: the target is not a findings commit');
assert.ok(dots(SCENES.ops_agent, 'prod').filter(d => d.x > .32).every(d => d.fade), 'S5: history after the checkpoint softens but stays');
assert.ok(refOf('ops_agent', 'investigation A').end && refOf('ops_agent', 'investigation B').end);
const batch = refOf('data_agent', 'batch').at, warehouse = refOf('data_agent', 'warehouse').at;
assert.ok(batch.some((k, i) => i && k[1] < batch[i - 1][1]), 'S6: the batch ref rewinds for the retry');
assert.match(SCENES.data_agent.items.find(i => i.kind === 'text' && /retry/.test(i.text)).text, /^optional retry/);
const rewind = batch.findIndex((k, i) => i && k[1] < batch[i - 1][1]), abandoned = batch[rewind - 1], retained = batch[rewind];
assert.notEqual(abandoned[2], retained[2] ?? 'bt', 'S6: the abandoned step sits on its own lane, so the redo never runs through it');
assert.equal(dots(SCENES.data_agent, 'bt').filter(d => Math.abs(d.x - abandoned[1]) < 1e-9).length, 0);
assert.ok(dots(SCENES.data_agent, abandoned[2]).find(d => Math.abs(d.x - abandoned[1]) < 1e-9).fade <= retained[0], 'S6: the abandoned commit softens when the ref rewinds');
assert.equal(dots(SCENES.data_agent, 'bt').filter(d => Math.abs(d.x - retained[1]) < 1e-9 && d.t >= retained[0]).length, 0, 'S6: the rewind adds no commit');
const redo = SCENES.data_agent.items.find(i => i.kind === 'line' && i.lane === 'bt' && Math.abs(i.x1 - retained[1]) < 1e-9 && i.t0 >= retained[0]);
assert.ok(redo && redo.x2 > retained[1] && !redo.fade || redo, 'S6: the redo grows from the retained commit along the batch lane');
assert.ok(dots(SCENES.data_agent, 'bt').filter(d => d.x > retained[1] && d.x < redo.x2).length === 0, 'S6: no bead lies on the redo edge');
assert.equal(JSON.stringify(warehouse.at(-1).slice(1)), '[0.84,"bt2"]', 'S6: the warehouse ref moves to the existing batch tip');
assert.equal(dots(SCENES.data_agent, 'wh').filter(d => d.t > warehouse.at(-2)[0]).length, 0, 'S6: the fast-forward adds no warehouse commit');
assert.match(SCENES.data_agent.items.find(i => i.kind === 'text' && /analytics/.test(i.text)).text, /analytics paused; rebase, then fast-forward merge/);
assert.ok(refOf('data_agent', 'batch').end && !refOf('data_agent', 'warehouse').end);
assert.doesNotMatch(scenePage.run('WORKFLOWS.ops_agent.alt'), /healthy|restored|latest good|review/i, 'S5 claims no healthy endpoint and has no review phase');
assert.doesNotMatch(scenePage.run('WORKFLOWS.context_mgmt.alt'), /candidate [AB]\b/, 'S2 names roles, not the literal candidates the code happens to select');
assert.ok(scenePage.run('WORKFLOWS.dev_agent.alt').indexOf('rebased') < scenePage.run('WORKFLOWS.dev_agent.alt').indexOf('quiet review'), 'S4 reviews after rebasing, never before');

// The operation scenes speak the same language: every branch grows from a commit or a head, every ref and every read or write sits on one,
// operations other than the load add no commit, and the marks say which rows are read or written.
const OPS = scenePage.run('OP_SCENES');
assert.deepEqual(Object.keys(OPS).sort(), [...scenePage.run('allRows.map(r => r.op)')].sort(), 'Every operation has a diagram');
for (const [key, sc] of Object.entries(OPS)) {
    const lanes = sc.lanes.map(l => l[0]);
    const anchor = (lane, x, before) => sc.items.some(i => ['dot', 'head'].includes(i.kind) && i.lane === lane && Math.abs(i.x - x) < 1e-9 && i.t <= before + 1e-9);
    for (const item of sc.items) {
        const last = item.kind === 'ref' ? Math.max(item.at.at(-1)[0], (item.end ?? 0) + .35) : ['text', 'dot', 'head', 'read', 'write'].includes(item.kind) ? item.t + .35 : item.t1;
        assert.ok(last <= 4.5, `${key}: ${item.kind} ends at ${last}, inside the 4.5 s sequence`);
        if (item.kind === 'curve') { assert.ok(lanes.includes(item.from) && lanes.includes(item.to)); assert.ok(anchor(item.from, item.x1, item.t0), `${key}: a branch grows from an existing commit or head`); }
        if (item.kind === 'ref') for (const [t, x, lane = item.lane] of item.at) assert.ok(anchor(lane, x, t), `${key}: ref ${item.name} sits on a commit or head that exists by ${t}`);
        if (['read', 'write'].includes(item.kind) && key !== 'load') assert.ok(anchor(item.lane, item.x, item.t), `${key}: rows are read or written at an existing head`);
    }
    assert.ok(sc.items.filter(i => i.kind === 'text').length <= 6, `${key}: a few short labels, not a caption track`);
    assert.ok(sc.items.filter(i => i.kind === 'dot').every(d => d.lane === 'main' || key === 'load'), `${key}: only the seed and the load commit`);
}
const marks = (key, kind) => [...OPS[key].items.filter(i => i.kind === kind).map(i => i.n)];
assert.deepEqual([marks('read', 'read'), marks('range_read', 'read'), marks('insert', 'write'), marks('update', 'write'), marks('range_update', 'write')], [[1], [3], [1], [1], [3]], 'A range reads or writes several rows at once');
assert.deepEqual(['time_to_first_query', 'branch_create', 'branch_connect', 'provision'].map(k => marks(k, 'write').length), [0, 0, 0, 0], 'Branching and provisioning write no rows');
assert.deepEqual([...OPS.branch_connect.items.find(i => i.kind === 'ref' && i.name === 'session').at.map(k => k[2] ?? 'main')], ['main', 'b1', 'b2', 'b3', 'main'], 'The session visits each branch of the chain in turn');
const opPage = mount(data, [], null, '#dataset?operation=range_update', 'local', {reduced: true});
const opCanvas = opPage.el('operation-scene').children[0];
assert.equal(opCanvas.getAttribute('aria-label'), `Range update: ${opPage.run('DESCRIPTIONS.range_update')}`);
assert.equal(opCanvas.getContext('2d').ops.filter(([op]) => op === 'fillRect').length, 3, 'Three squares: a range write');
assert.equal(opPage.observers.intersection.length, 0, 'Reduced motion draws the operation complete, with no frame loop');
const oneLane = mount(data, [], null, '#dataset?operation=provision', 'local', {reduced: true}).el('operation-scene').children[0];
assert.equal(oneLane.height, (68 + 76) * 2, 'A one-lane diagram keeps a readable height');
const noCanvasOp = mount(data, [], null, '#dataset?operation=load', 'local', {canvas: false}).el('operation-scene').children[0];
assert.equal(noCanvasOp.className, 'diagram-missing'); assert.match(noCanvasOp.textContent, /needs Canvas/);

// Check the wizard's actual command generator without executing any shell commands.
const wizardHtml = fs.readFileSync(`${__dirname}/run.html`, 'utf8');
const wizardScript = wizardHtml.match(/<script>([\s\S]*?)<\/script>/)[1];
const systemsDir = path.join(__dirname, 'systems');
const runnable = fs.readdirSync(systemsDir).filter(name =>
    fs.existsSync(path.join(systemsDir, name, 'connector.py')) && fs.existsSync(path.join(systemsDir, name, 'system.json'))).sort();
const systemMetadata = Object.fromEntries(runnable.map(name => [name, JSON.parse(fs.readFileSync(path.join(systemsDir, name, 'system.json'), 'utf8'))]));
const wizardContext = vm.createContext({systems: systemMetadata});
vm.runInContext(wizardScript.slice(0, wizardScript.indexOf('const form =')), wizardContext);
const commandsFor = vm.runInContext('commandsFor', wizardContext);
assert.match(commandsFor(['dolt_mysql'], 'bench-01', ''), /^python -m leaderboard.run dolt_mysql --machine 'bench-01' &&\npython -m leaderboard.site --phase local$/, 'The wizard rebuilds the development site, never production');
assert.match(wizardHtml, /<code>python -m leaderboard\.serve --phase local<\/code>/, 'The fallback names the development server; the production default has no wizard');
assert.doesNotMatch(wizardHtml, /\bCI runs\b/, 'The wizard must not claim that the planned production launcher exists');
assert.match(wizardHtml, /fleet launcher is not implemented yet/, 'The submission path states the production automation boundary');
const mixedCommands = commandsFor(['dolt', 'dolt_mysql'], 'bench-01', '');
assert.equal(mixedCommands.split(' &&\n').length, 3, 'One run per database, then build once');
assert.match(mixedCommands, /^python -m leaderboard.run dolt --machine 'bench-01' &&\n/);
assert.doesNotMatch(mixedCommands, /--client-location|leaderboard\.validate/, 'Local runs need no location; build validates and excludes incompatible history');
assert.deepEqual(Object.keys(systemMetadata).sort(), runnable, 'Wizard metadata must match runnable leaderboard connectors');
assert.doesNotMatch(wizardHtml, /dolt_mysql:\s*\{/, 'The wizard must not duplicate the system registry');
assert.match(wizardHtml, /uv sync --frozen\nuv run --frozen python build_protos\.py/);
assert.match(wizardHtml, /Install uv, protoc/);
assert.doesNotMatch(wizardHtml, /requirements\.lock/);
const macroCommands = commandsFor(['dolt'], 'bench-01', '', {preset: 'mini'});
assert.match(macroCommands, /^status=0\nkeep\(\) \{ \[ "\$status" -ne 0 \] \|\| status=\$1; \}\npython -m leaderboard.run_macro dolt --preset mini --machine 'bench-01' \|\| keep \$\?\npython -m leaderboard.site --phase local \|\| keep \$\?\nexit "\$status"$/, 'All six workflows, no way around validation, the build runs after a failure and the first status survives');
// Execute the generated macro script with a fake python: the first non-zero run status wins, the build always runs, a build failure counts only when every run passed.
const fakeBin = fs.mkdtempSync(path.join(os.tmpdir(), 'bb-fake-python-'));
fs.writeFileSync(path.join(fakeBin, 'python'), '#!/bin/bash\necho "$*" >> "$FAKE_LOG"\nfor a in "$@"; do v="FAIL_${a//[^A-Za-z0-9]/_}"; [ -n "${!v:-}" ] && exit "${!v}"; done\nexit 0\n', {mode: 0o700});
const twoRuns = commandsFor(['dolt', 'dolt_mysql'], 'bench-01', '', {preset: 'mini'});
for (const [failures, expected] of [[{}, 0], [{FAIL_dolt: '3', FAIL_dolt_mysql: '7'}, 3], [{FAIL_leaderboard_site: '2'}, 2], [{FAIL_dolt: '3', FAIL_leaderboard_site: '2'}, 3], [{FAIL_dolt_mysql: '7', FAIL_leaderboard_site: '2'}, 7]]) {
    const log = path.join(fakeBin, 'calls.log'); fs.writeFileSync(log, '');
    const result = spawnSync('/bin/bash', ['-c', twoRuns], {env: {PATH: `${fakeBin}:${process.env.PATH}`, FAKE_LOG: log, ...failures}, encoding: 'utf8'});
    assert.equal(result.status, expected, `${JSON.stringify(failures)}: ${result.stderr}`);
    assert.deepEqual(fs.readFileSync(log, 'utf8').trim().split('\n').map(line => line.split(' ')[1]), ['leaderboard.run_macro', 'leaderboard.run_macro', 'leaderboard.site'], 'every run and the build execute');
}
fs.rmSync(fakeBin, {recursive: true});
assert.doesNotMatch(commandsFor(['dolt', 'dolt_mysql'], 'bench-01', '', {preset: 'full'}), /--allow-dirty|--scenarios/);
assert.throws(() => commandsFor(['dolt'], 'bench-01', '', {preset: 'huge'}), /Quick or Full/);
assert.doesNotMatch(wizardHtml, /name="scenario"|--allow-dirty|smoke/i, 'No workflow subsets, no dirty runs, no smoke wording');
assert.match(wizardHtml, /id="preset-mini"[^>]*\bchecked\b/); assert.match(wizardHtml, /id="suite-micro"[^>]*\bchecked\b/);
assert.doesNotMatch(wizardHtml, /\bETA\b|estimated time/, 'No invented ETA');
assert.throws(() => commandsFor([], 'bench-01', ''), /database/);
assert.throws(() => commandsFor(['dolt'], 'bad; command', ''), /machine/);
assert.throws(() => commandsFor(['unknown'], 'bench-01', ''), /database/);
assert.throws(() => commandsFor(['dolt'], 'a'.repeat(65), ''), /machine/);
// Keep credential/location escaping covered without offering an absent connector.
vm.runInContext("SYSTEMS.neon = {hosted: 'yes', credentials: ['NEON_API_KEY_ORG']}", wizardContext);
assert.throws(() => commandsFor(['neon'], 'bench-01', 'US East', {preset: 'mini'}), /local/, 'Hosted systems never get agent workflows');
assert.ok(commandsFor(['neon'], 'bench-01', "O'Brien $(touch nope)").includes("--client-location 'O'\\''Brien $(touch nope)'"));
assert.throws(() => commandsFor(['neon'], 'bench-01', '   '), /location/);
assert.throws(() => commandsFor(['neon'], 'bench-01', 'US\tEast'), /location/);
vm.runInContext('delete SYSTEMS.neon', wizardContext);
assert.match(wizardHtml, /input\.type = 'radio'; input\.name = 'database'/, 'Wizard selects 1 database per run via generated radio inputs');
const submissionScaffold = vm.runInContext('submissionScaffold', wizardContext);
assert.match(submissionScaffold('AcmeDB', 'acmedb', false, 'dev-box', {preset: 'mini'}), /leaderboard\/systems\/acmedb\/system\.json[\s\S]*"system": "AcmeDB"[\s\S]*"requirements":[\s\S]*"credentials": \[\][\s\S]*python -m leaderboard\.run_macro acmedb --preset mini --machine 'dev-box'/);
assert.throws(() => submissionScaffold('AcmeDB', 'bad-slug!', false, 'dev-box', null), /adapter ID/);
assert.throws(() => submissionScaffold('AcmeDB', '__proto__', false, 'dev-box', null), /adapter ID/);
assert.throws(() => submissionScaffold('AcmeDB', '1db', true, 'dev-box', null), /adapter ID/);
const systemDisplaySlug = vm.runInContext('systemDisplaySlug', wizardContext);
assert.deepEqual(['Dolt', 'Doltgres'].map(system => systemDisplaySlug({system})), ['dolt', 'doltgres'], 'Wizard icons use display identities, not connector keys');


// Selecting data rows changes only the reads/writes score, the board names the subset it counts, and the last ticked
// workload cannot be unticked: there is no silent "none means all".
const selectionData = [entry('A', 0.03, []), entry('B', 0.06, [])];
selectionData[1].result[3] = [0.15, 0.15, 0.15];
const selection = mount(selectionData, [], null, '#microbench'), allScores = tableScores(selection);
const selectRow = (index, checked) => {
    const checkbox = selection.document.getElementById(`row-${index}-no`);
    checkbox.checked = checked; checkbox.change();
    assert.equal(selection.document.activeElement.id, checkbox.id, 'The rebuilt checkbox keeps focus');
};
for (let i = 4; i < suite.rows.length; i++) selectRow(i, false);
assert.deepEqual(tableScores(selection), [[[allScores[0][0][0], '×1.00', allScores[0][0][2]], [allScores[0][1][0], '×4.00', allScores[0][1][2]]]]);
assert.match(classNodes(classNodes(selection.el('breakdowns'), 'heatmap')[0], 'row-meta')[4].textContent, /not in score$/, 'An unticked workload says it is not in the score');
assert.equal(headRow(selection.byClass('summary')[0])[2].textContent, 'Reads & writes (1 of 5)', 'The score names the subset it counts');
assert.equal(selection.run('window.location.hash'), '#microbench?workloads=read', 'and the link carries it');
const last = selection.document.getElementById('row-3-no');
assert.deepEqual([last.checked, last.disabled, last.title], [true, true, 'The score counts at least one workload'], 'The last ticked workload cannot be unticked');
selectRow(4, true);
assert.ok(!selection.document.getElementById('row-3-no').disabled, 'With two counted, either can be unticked again');
for (let i = 4; i < suite.rows.length; i++) selectRow(i, true);
assert.deepEqual(tableScores(selection), allScores);
assert.equal(selection.run('window.location.hash'), '#microbench', 'The default view keeps the plain link');
// A shared link restores the whole view: try, sort, order, counted workloads and filters.
const shared = mount(selectionData, [], null, '#microbench?try=best&sort=data&order=desc&workloads=insert,update&systems=B');
assert.equal(shared.run('[state.mode, state.sort, state.sortDesc, countedData().map(r => r.op), Object.entries(state.system).map(([k, v]) => k + v)].join(" ")'), 'best data true insert,update Afalse,Btrue');
assert.deepEqual(bodyRows(shared.byClass('summary')[0]).map(row => nameOf(row).textContent), ['B'], 'The filter in the link applies');
assert.equal(shared.run('boardHash()'), '#microbench?try=best&sort=data&order=desc&workloads=insert,update&systems=B', 'and the view writes the same link back');
assert.equal(mount(selectionData, [], null, '#microbench?workloads=nope&sort=bogus').run('[countedData().length, state.sort].join()'), '5,branch', 'Unknown values in a link fall back to the defaults');
// Row ids keep a row's identity whole: machines named test.box and test-box never share one, so focus returns to the right row.
const twins = mount([{...entry('A', 0.03, []), machine: 'test.box'}, {...entry('A', 0.05, []), machine: 'test-box'}]);
const twinIds = bodyRows(twins.byClass('summary')[0]).map(row => classNodes(row, 'details-button')[0].id);
assert.notEqual(twinIds[0], twinIds[1], 'Distinct rows never share an id');
twins.document.getElementById(twinIds[1]).click(); twins.el('close-run').click();
assert.equal(twins.document.activeElement.id, twinIds[1], 'Closing returns focus to the row that opened the details');
for (const name of ['constructor', 'toString', '__proto__']) assert.equal(mount(selectionData, [], null, `#microbench?sort=${name}`).run('state.sort'), 'branch', `sort=${name} names no score of its own, so the default holds and nothing breaks`);
// Tied scores share a rank; the next rank skips, as in a competition.
const tied = mount([entry('A', 0.03, []), entry('B', 0.03, []), entry('C', 0.06, [])]);
assert.deepEqual(tied.byClass('rank-number').map(e => [e.textContent, e.className]), [['1', 'rank-number rank-leader'], ['1', 'rank-number rank-leader'], ['3', 'rank-number']]);

data[0].result[0] = [0.03, null, 0.01];
click('choice-mode-best');
const ranking = bodyRows(byClass('summary')[0]);
assert.deepEqual(ranking.map(row => nameOf(row).textContent), ['Measured', 'Tagless'], 'A system without the rank metric is listed last, unranked');
assert.deepEqual(ranking.map(row => classNodes(row, 'rank-number')[0].textContent), ['1', '—']);
assert.match(ranking[1].children[0].title, /Not ranked by First query: missing Time to first query/);
const scoreOf = td => classNodes(td, 'score-value')[0]?.textContent ?? td.textContent;
assert.deepEqual(ranking.map(row => scoreOf(row.children[1])), ['×1.00', '—'], 'A failed first-query repeat removes only that score');
assert.deepEqual(ranking.map(row => scoreOf(row.children[2])), ['×1.75', '×1.00'], 'The other scores stay');
assert.match(elements.get('groups').textContent, /Tagless has no First query score: missing Time to first query/);
assert.deepEqual(classNodes(elements.get('operation-results'), 'cell-missing').map(e => e.textContent), [], 'Only the selected operation is listed');
click('operation-time_to_first_query');
assert.deepEqual(classNodes(elements.get('operation-results'), 'result-bars')[0].children.map(li => li.textContent), ['MMeasured0.060s', 'TTaglessFailed'], 'A failed batch is named after the timed ones, never drawn as a bar');
classNodes(ranking[1], 'details-button')[0].click();
assert.match(classNodes(elements.get('run-body'), 'verdict')[0].textContent, /^Not ranked by First query: missing Time to first query\.$/, 'The details say why there is no rank');
click('close-run');

// Missing measurements never fill or crash a baseline.
data[0].result = data[1].result = suite.rows.map(() => [null, null, null]);
run('render()');
assert.deepEqual(tableScores(page), [[['×1.00'], ['×2.00']]], 'Only the valid loading scores remain');
assert.deepEqual(bodyRows(byClass('summary')[0]).map(row => classNodes(row, 'rank-number')[0].textContent), ['—', '—'], 'Nothing is ranked by a metric nobody has');
assert.equal(heatmapColors(page).filter(Boolean).length, 4, 'Failed operation cells have no heatmap value; load and provisioning do');
assert.match(elements.get('groups').textContent, /First query: no measurements for Time to first query\./);

const stale = [{system: 'Old', machine: 'test', source: 'old.json', reason: 'invalid: suite differs'}];
for (const excluded of [[], stale]) {
    const empty = mount([], excluded);
    assert.equal(empty.elements.get('empty-state').hidden, false);
    assert.equal(empty.elements.get('empty-title').textContent, 'No published results yet');
    assert.equal(empty.elements.get('clear-empty-filters').hidden, true);
    assert.equal(empty.elements.get('shown').textContent, '0 systems');
    assert.equal(empty.elements.get('updated').textContent, 'No published runs yet');
    assert.equal(empty.byClass('summary').length, 0); assert.equal(classNodes(empty.el('breakdowns'), 'panel').length, 0);
    assert.equal(classNodes(empty.el('operation-results'), 'fine')[0].textContent, 'No results yet.', 'The definitions still explain every operation');
    if (excluded.length) {
        assert.match(empty.elements.get('excluded').textContent, /invalid: suite differs/);
        assert.equal(empty.el('excluded-panel').open, true, 'With no result at all, the exclusion reasons start open');
        assert.equal(empty.elements.get('empty-description').textContent, 'Microbenchmark results appear here once they are published. 1 run without a result is listed below.');
    }
}
const grouped = mount([entry('Local', 1, []), {...entry('Hosted', 5, []), hosted: 'yes', region: 'test-region', rtt_ms: 10}]);
assert.equal(grouped.byClass('summary').length, 1, 'One unified ranking table for all systems');
assert.deepEqual(grouped.byClass('group-heading').map(h => h.children[0].textContent), ['Operations']);
assert.deepEqual(grouped.byClass('hosting-badge').map(b => b.textContent), ['Self-hosted', 'Managed service'], 'Each row carries a hosting badge');
assert.equal(classNodes(grouped.el('breakdowns'), 'heatmap').length, 1);
assert.equal(classNodes(grouped.el('breakdowns'), 'breakdown-title').length, 0, 'Unified breakdown needs no group title');
assert.equal(classNodes(grouped.el('operation-results'), 'result-group').length, 1, 'Operation times are shown in one unified list');
{
    const multiProf = mount([
        {...entry('LocalHost', 1, []), execution_profile: 'local_single_host'},
        {...entry('SingleVM', 2, []), execution_profile: 'economic_single_vm'},
    ]);
    assert.equal(multiProf.byClass('summary').length, 2, 'Distinct execution profiles render in separate micro tables');
    multiProf.document.getElementById('sort-data-no-economic_single_vm').click();
    assert.equal(multiProf.document.activeElement.id, 'sort-data-no-economic_single_vm', 'Focus stays on the second profile group sort button');
}
const zeroData = [entry('Zero', 1, []), entry('One', 1, []), entry('Missing', 1, [])];
zeroData[0].result = suite.rows.map(() => [0, 0, 0]);
zeroData[2].result = suite.rows.map(() => [null, null, null]);
const zeros = mount(zeroData);
const timingRows = bodyRows(classNodes(zeros.el('breakdowns'), 'heatmap')[0]);
for (const row of timingRows.slice(0, suite.rows.length)) {
    assert.equal(classNodes(row, 'cell-best')[0].style['--strength'], '0.130', 'Zero is a valid fastest timing');
    assert.equal(classNodes(row, 'cell-missing')[0].style['--strength'], undefined, 'Missing values are not heatmap values');
}
for (const strength of heatmapColors(zeros).filter(Boolean)) {
    const alpha = Number(strength);
    assert.ok(Number.isFinite(alpha) && alpha >= 0 && alpha <= 1);
}
// A slow preview for an old input must never replace the script the user can run.
async function checkLivePreview() {
    const nodes = new Map(), replies = [];
    const context = vm.createContext({AbortSignal,
        el: id => { if (!nodes.has(id)) nodes.set(id, {}); return nodes.get(id); },
        say: (id, text) => { if (!nodes.has(id)) nodes.set(id, {}); nodes.get(id).textContent = text; },
        fetch: () => new Promise(resolve => replies.push(resolve)),
    });
    const prepareSource = wizardScript.slice(wizardScript.indexOf('async function prepareRun('),
        wizardScript.indexOf("el('run-benchmark').addEventListener"));
    vm.runInContext(`let previewVersion = 1, preparedRun = null; ${prepareSource}`, context);
    const prepare = vm.runInContext('prepareRun', context);
    const old = prepare({machine: 'old-machine'}, 1);
    vm.runInContext('previewVersion = 2', context);
    const current = prepare({machine: 'new-machine'}, 2);
    replies[1]({ok: true, json: async () => ({script: 'new script', can_launch: true})});
    await current;
    replies[0]({ok: true, json: async () => ({script: 'old script', can_launch: true})});
    await old;
    assert.equal(nodes.get('run-command').textContent, 'new script');
    assert.equal(vm.runInContext('preparedRun.machine', context), 'new-machine');
    assert.equal(nodes.get('run-benchmark').disabled, false);
}
checkLivePreview().then(() => console.log('Page regression checks passed'))
    .catch(error => { console.error(error); process.exitCode = 1; });
