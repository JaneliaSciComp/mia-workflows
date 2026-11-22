// A table paged by the server, its page fetcher, and what the column names shared by the tables mean.
import { el, num } from "./dom.js";
import { load } from "./api.js";

// ---- a table paged by the server: a header click sorts, the box searches, the pager moves, and each asks the
// server for one page (`?limit&offset&sort&dir&q`). Its state (sort, search, offset) lasts the session, per table id.
export const PAGE_SIZE = 50,
  TABLE_STATE = new Map();
// a GET list route as a page fetcher; `extra()` adds the view's own filters; quiet404 as in load()
export const pageOf = (path, rowsKey, extra, quiet404) => async state => {
  const query = new URLSearchParams({
    limit: state.limit,
    offset: state.offset,
    ...(state.sort ? { sort: state.sort, dir: state.dir } : {}),
    ...(state.q ? { q: state.q } : {}),
    ...(extra ? extra() : {}),
  });
  const out = await load(`${path}?${query}`, quiet404);
  return out && { rows: out[rowsKey] || [], total: out.total ?? (out[rowsKey] || []).length, out };
};
export const reloadTables = root => root.querySelectorAll(".paged").forEach(box => box.reload()); // a refresh that rebuilds nothing
// What a column holds, said on its header's hover, for the names that mean the same in every table. A column whose
// meaning is its table's own says it in its `title`.
export const COLUMN_HELP = {
  "dataset": "The dataset's id in the catalog. It opens the dataset's page.",
  "issue": "The GitHub issue that holds a read-only copy of its timeline.",
  "preview": "The overlay of its first saved crop. Hover enlarges it, a click opens it.",
  "dims": "Its dimensionality: 2D, 2D+t, 3D or 3D+t.",
  "modality": "Its imaging modality, from the catalog record.",
  "licence": "The SPDX licence its catalog record names.",
  "size": "Its size upstream, as the catalog record gives it.",
  "raw cache": "What its raw cache holds on disk now: the files a reader took whole.",
  "buffers": "How many buffer versions hold it.",
  "stage": "The stage its workflow is at.",
  "reason": "Why it stands as it does, in the words of the step that left it there.",
  "updated": "When it last changed.",
  "changed": "When it last changed.",
  "when": "When it happened.",
  "request": "The preparation request. It opens the request's page.",
  "job": "The cluster job that is working on it now, if one is.",
  "files": "Opens its directory in the file browser.",
  // a cached asset
  "artifact":
    "The staged artifact's id, a hash of its recipe: binding, levels key, voxel size, harness, environment and plan. Hover it for the whole key.",
  "bytes": "Its size on disk.",
  "last used": "When a round last staged it or took it for a buffer. The keeper evicts the longest unused first.",
  "levels key": "The pyramid geometry it was staged for: the levels and the voxel size a definition asks for.",
  "binding": "The hash of the binding it was staged with: the reader and how it is called.",
  "harness": "The harness commit it was staged with.",
  // a decision
  "gate": "The gate the decision was made at.",
  "about": "What was judged: the kind of thing and its reference.",
  "verdict": "approve, reject, or escalate (handed to a person).",
  "by": "Who decided: a person, the judge or a policy.",
  "confidence": "How sure the judge was, from 0 to 1. A person gives none.",
  "summary": "The note given with the verdict, or the judge's summary.",
};
// cols: {key (the server's sort name; none: not sortable), label, num, wrap, title (else COLUMN_HELP's), cell}. `select`: {chosen (a Set), id, onchange}
// adds a check per row, or per row `select.can(row)` allows. `onPage(page)` sees each answer (its `out` is the whole response). The box has reload(reset).
// `empty`: what a table with no row says, or a function that gives it.
export function pagedTable(
  id,
  { source, cols, empty, sort = null, dir = "asc", search = true, rowAttrs, tools = [], select, onPage },
) {
  if (!TABLE_STATE.has(id)) TABLE_STATE.set(id, { sort, dir, q: "", offset: 0, limit: PAGE_SIZE });
  const state = TABLE_STATE.get(id);
  const body = el("div", {}, el("div", { class: "empty" }, "loading…")),
    pager = el("div", { class: "pager" }),
    box = el("div", { class: "paged" });
  let seq = 0,
    timer = 0;
  const go = offset => {
    state.offset = Math.max(0, offset);
    draw();
  };
  const step = (text, title, off, to) =>
    el("button", { type: "button", class: "btn", title, disabled: off, onclick: () => go(to) }, text);
  const tick = row => {
    const key = select.id(row),
      input = el("input", { type: "checkbox", "aria-label": "select " + key });
    input.checked = select.chosen.has(key);
    input.addEventListener("change", () => {
      if (input.checked) select.chosen.add(key);
      else select.chosen.delete(key);
      select.onchange(row, input.checked);
    });
    return input;
  };
  const canTick = row => !select.can || select.can(row);
  const tickPage = page => {
    // the header's check: every row of this page that has a check, on or off
    const rows = page.rows.filter(canTick);
    const input = el("input", {
      type: "checkbox",
      "aria-label": "select every row of this page",
      title: "check or uncheck every row of this page",
    });
    input.checked = rows.length > 0 && rows.every(r => select.chosen.has(select.id(r)));
    input.addEventListener("change", () => {
      for (const r of rows) {
        if (input.checked) select.chosen.add(select.id(r));
        else select.chosen.delete(select.id(r));
        select.onchange(r, input.checked);
      }
      render(page);
    });
    return input;
  };
  const render = page => {
    const head = el(
      "tr",
      {},
      select ? el("th", {}, tickPage(page)) : null,
      ...cols.map(c =>
        el(
          "th",
          {
            class: (c.num ? "num" : "") + (c.key ? " sortable" : ""),
            title: c.title || COLUMN_HELP[c.label] || null,
            onclick: c.key
              ? () => {
                  if (state.sort === c.key) state.dir = state.dir === "asc" ? "desc" : "asc";
                  else {
                    state.sort = c.key;
                    state.dir = c.num ? "desc" : "asc";
                  }
                  go(0);
                }
              : null,
          },
          c.label + (c.key && state.sort === c.key ? (state.dir === "asc" ? " ▲" : " ▼") : ""),
        ),
      ),
    );
    const rows = page.rows.map(r =>
      el(
        "tr",
        rowAttrs ? rowAttrs(r) : {},
        select ? el("td", {}, canTick(r) ? tick(r) : null) : null,
        ...cols.map(c => {
          const v = c.cell(r);
          return el("td", { class: c.num ? "num" : c.wrap ? "wrap" : "" }, v == null ? "" : v);
        }),
      ),
    );
    body.replaceChildren(
      page.total
        ? el(
            "div",
            { class: "tablewrap" },
            el("table", { class: "plain" }, el("thead", {}, head), el("tbody", {}, ...rows)),
          )
        : el(
            "div",
            { class: "empty" },
            state.q ? "Nothing matches the search." : typeof empty === "function" ? empty() : empty,
          ),
    );
    const last = state.offset + page.rows.length;
    pager.replaceChildren(
      el("span", {}, page.total ? `${num(state.offset + 1)}–${num(last)} of ${num(page.total)}` : ""),
      ...(page.total > state.limit
        ? [
            step("‹ previous", `The ${state.limit} rows before these.`, !state.offset, state.offset - state.limit),
            step("next ›", `The next ${state.limit} rows.`, last >= page.total, state.offset + state.limit),
          ]
        : []),
    );
  };
  async function draw() {
    const mine = ++seq,
      page = await source(state);
    if (mine !== seq) return; // a newer request is on its way
    if (!page) {
      body.replaceChildren(el("div", { class: "empty" }, "The list did not load."));
      pager.replaceChildren();
      return;
    }
    if (state.offset && state.offset >= page.total)
      return go(Math.floor(Math.max(0, page.total - 1) / state.limit) * state.limit); // the list shrank under this page
    state.last = page;
    if (onPage) onPage(page);
    render(page);
  }
  if (state.last) render(state.last); // a rebuilt view shows what the table last showed until the server answers again
  const input = el("input", {
    type: "search",
    placeholder: typeof search === "string" ? search : "search",
    "aria-label": "search this table",
  });
  input.value = state.q;
  input.addEventListener("input", () => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      state.q = input.value.trim();
      go(0);
    }, 250);
  });
  if (search || tools.length) box.append(el("div", { class: "tablebar" }, search ? input : null, ...tools));
  box.append(body, pager);
  box.reload = reset => {
    if (reset) state.offset = 0;
    draw();
  };
  draw();
  return box;
}
