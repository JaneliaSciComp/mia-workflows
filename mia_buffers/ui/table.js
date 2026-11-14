// A table paged by the server, and its page fetcher.
import { el, num } from "./dom.js";
import { load } from "./api.js";

// ---- a table paged by the server: a header click sorts, the box searches, the pager moves, and each asks the
// server for one page (`?limit&offset&sort&dir&q`). Its state (sort, search, offset) lasts the session, per table id.
export const PAGE_SIZE = 50, TABLE_STATE = new Map();
// a GET list route as a page fetcher; `extra()` adds the view's own filters; quiet404 as in load()
export const pageOf = (path, rowsKey, extra, quiet404) => async state => {
  const query = new URLSearchParams({ limit: state.limit, offset: state.offset, ...(state.sort ? { sort: state.sort, dir: state.dir } : {}), ...(state.q ? { q: state.q } : {}), ...(extra ? extra() : {}) });
  const out = await load(`${path}?${query}`, quiet404);
  return out && { rows: out[rowsKey] || [], total: out.total ?? (out[rowsKey] || []).length, out };
};
export const reloadTables = root => root.querySelectorAll(".paged").forEach(box => box.reload());  // a refresh that rebuilds nothing
// cols: {key (the server's sort name; none: not sortable), label, num, wrap, title, cell}. `select`: {chosen (a Set), id, onchange, can}
// adds a tick per row. `onPage(page)` sees each answer (its `out` is the whole response). The box has reload(reset).
export function pagedTable(id, { source, cols, empty, sort = null, dir = "asc", search = true, rowAttrs, tools = [], select, onPage }) {
  if (!TABLE_STATE.has(id)) TABLE_STATE.set(id, { sort, dir, q: "", offset: 0, limit: PAGE_SIZE });
  const state = TABLE_STATE.get(id);
  const body = el("div", {}, el("div", { class: "empty" }, "loading…")), pager = el("div", { class: "pager" }), box = el("div", { class: "paged" });
  let seq = 0, timer = 0;
  const go = offset => { state.offset = Math.max(0, offset); draw(); };
  const step = (text, off, to) => el("button", { type: "button", class: "btn", disabled: off, onclick: () => go(to) }, text);
  const tick = row => {
    const key = select.id(row), input = el("input", { type: "checkbox", "aria-label": "select " + key });
    input.checked = select.chosen.has(key);
    input.addEventListener("change", () => { if (input.checked) select.chosen.add(key); else select.chosen.delete(key); select.onchange(row, input.checked); });
    return input;
  };
  const tickPage = page => {  // the header's tick: every row of this page, on or off
    const input = el("input", { type: "checkbox", "aria-label": "select every row of this page", title: "tick or untick every row of this page" });
    input.checked = page.rows.length > 0 && page.rows.every(r => select.chosen.has(select.id(r)));
    input.addEventListener("change", () => {
      for (const r of page.rows) { if (input.checked) select.chosen.add(select.id(r)); else select.chosen.delete(select.id(r)); select.onchange(r, input.checked); }
      render(page);
    });
    return input;
  };
  const render = page => {
    const head = el("tr", {}, select ? el("th", {}, tickPage(page)) : null, ...cols.map(c => el("th", { class: (c.num ? "num" : "") + (c.key ? " sortable" : ""), title: c.title || null,
      onclick: c.key ? () => { if (state.sort === c.key) state.dir = state.dir === "asc" ? "desc" : "asc"; else { state.sort = c.key; state.dir = c.num ? "desc" : "asc"; } go(0); } : null },
      c.label + (c.key && state.sort === c.key ? (state.dir === "asc" ? " ▲" : " ▼") : ""))));
    const rows = page.rows.map(r => el("tr", rowAttrs ? rowAttrs(r) : {}, select ? el("td", {}, tick(r)) : null, ...cols.map(c => { const v = c.cell(r); return el("td", { class: c.num ? "num" : c.wrap ? "wrap" : "" }, v == null ? "" : v); })));
    body.replaceChildren(page.total ? el("div", { class: "tablewrap" }, el("table", { class: "plain" }, el("thead", {}, head), el("tbody", {}, ...rows))) : el("div", { class: "empty" }, state.q ? "Nothing matches the search." : empty));
    const last = state.offset + page.rows.length;
    pager.replaceChildren(el("span", {}, page.total ? `${num(state.offset + 1)}–${num(last)} of ${num(page.total)}` : ""),
      ...(page.total > state.limit ? [step("‹ previous", !state.offset, state.offset - state.limit), step("next ›", last >= page.total, state.offset + state.limit)] : []));
  };
  async function draw() {
    const mine = ++seq, page = await source(state);
    if (mine !== seq) return;  // a newer request is on its way
    if (!page) { body.replaceChildren(el("div", { class: "empty" }, "The list did not load.")); pager.replaceChildren(); return; }
    if (state.offset && state.offset >= page.total) return go(Math.floor(Math.max(0, page.total - 1) / state.limit) * state.limit);  // the list shrank under this page
    state.last = page;
    if (onPage) onPage(page);
    render(page);
  }
  if (state.last) render(state.last);  // a rebuilt view shows what the table last showed until the server answers again
  const input = el("input", { type: "search", placeholder: typeof search === "string" ? search : "search", "aria-label": "search this table" });
  input.value = state.q;
  input.addEventListener("input", () => { clearTimeout(timer); timer = setTimeout(() => { state.q = input.value.trim(); go(0); }, 250); });
  if (search || tools.length) box.append(el("div", { class: "tablebar" }, search ? input : null, ...tools));
  box.append(body, pager);
  box.reload = reset => { if (reset) state.offset = 0; draw(); };
  draw();
  return box;
}
