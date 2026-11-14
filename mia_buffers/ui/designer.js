// New buffer: the controls that describe a spec, its plan as numbers, charts and a table, and the buttons that
// create the buffer or prepare what it lacks.
import { DIMS, levelsKey, levelsOf, LIC, SRC, toSpecYaml } from "./spec.js";
import {
  $, assetPill, bufferCount, datasetLink, el, fmt, fmtBytes, ghIcon, hoverable, num, pct, pill, plural, short,
  statePill, svgEl, thumb, tile
} from "./dom.js";
import { api, banner, JSON_HEADERS, newKey, S } from "./api.js";
import { pagedTable, TABLE_STATE } from "./table.js";
import { BUTTON_HELP, prepareButton } from "./actions.js";

const NOT_PREPARED = "not prepared";  // mia_buffers.plan.NOT_PREPARED
const PLAN_DEBOUNCE_MS = 300;

// ---- horizontal bars (the explorer's: single series, sequential hue)
function barPath(x, y, w, h, r) { if (w <= r) return `M${x} ${y}h${w}v${h}h${-w}z`; return `M${x} ${y}h${w-r}a${r} ${r} 0 0 1 ${r} ${r}v${h-2*r}a${r} ${r} 0 0 1 ${-r} ${r}h${-(w-r)}z`; }
function hbars(container, items, opts) {
  if (!items.length) { container.replaceChildren(el("div", { class: "empty" }, opts.empty || "Nothing to show.")); return; }
  const W = 560, labelW = opts.labelW || 170, rowH = 26, barH = 16, pad = 8;
  const H = items.length * rowH + pad * 2;
  const max = Math.max(1e-12, ...items.map(i => i.value));
  const svg = svgEl("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": opts.aria || "" });
  const plotW = W - labelW - 70;
  items.forEach((it, i) => {
    const y = pad + i * rowH;
    const g = svgEl("g");
    const lbl = svgEl("text", { x: labelW - 8, y: y + barH - 3, "text-anchor": "end", class: "lbl" }); lbl.textContent = it.label.length > 26 ? it.label.slice(0, 25) + "…" : it.label; g.append(lbl);
    const w = Math.max(0, it.value / max * plotW);
    g.append(svgEl("path", { d: barPath(labelW, y, w, barH, 4), fill: it.color || "var(--accent)" }));
    const v = svgEl("text", { x: labelW + w + 6, y: y + barH - 3, class: "tick" }); v.textContent = it.text || num(it.value); g.append(v);
    const hit = svgEl("rect", { x: 0, y: y - 4, width: W, height: rowH, fill: "transparent" }); g.append(hit);
    hoverable(hit, [{ b: it.text || num(it.value), t: " " + it.label }, ...(it.sub ? [{ t: it.sub }] : [])]);
    svg.append(g);
  });
  svg.append(svgEl("line", { x1: labelW, x2: labelW, y1: pad - 4, y2: H - pad + 4, class: "base" }));
  container.replaceChildren(svg);
}

// ---- the spec the controls describe
const C = { name: "microscopy-2d-gray", paths: "datasets/**", dims: new Set(["2D"]), lic: new Set(["CC0-1.0","CC-BY-3.0","CC-BY-4.0","MIT","BSD-3-Clause"]),
  allowUnknown: false, annRequired: false, sources: new Set(SRC), minVox: 1e6, patch: 256, level: 4, patchZ: 32, dedup: true, T: 0.5,
  preparedOnly: false, rawCached: false, stagedGeom: false };

// ---- creating the buffer: its spec is posted to the service, which runs it
function createBuffer(container, yaml, blocker) {
  // built once and kept: what a person typed into the box survives the page's re-renders
  let area = container.querySelector("textarea");
  if (!area) {
    area = el("textarea", { class: "specbox", spellcheck: "false", "aria-label": "buffer spec YAML", oninput: () => { area.dataset.edited = "1"; renderActions(); } });  // an edited spec enables Plan
    const status = el("span", { class: "small" });
    const run = el("button", { type: "button", class: "btn", title: BUTTON_HELP.plan, onclick: async () => {
      run.disabled = true; status.textContent = "planning…";
      try {
        const action = await api("/specs", { method: "POST", headers: JSON_HEADERS, body: JSON.stringify({ spec: area.value, action: "go", plan_first: true, key: newKey() }) });
        location.hash = "#find/" + action.number;
      } catch (e) { status.textContent = ""; banner(e.status === 422 ? `The spec was refused: ${e.message}` : e.status === 409 ? `Not started. ${e.message}` : `Not started (${e.status || "network error"}): ${e.message}`); }
      run.disabled = false;
    } }, "Plan");
    const reset = el("button", { type: "button", class: "btn", title: "replace the text with the spec the controls above describe", onclick: () => { delete area.dataset.edited; renderActions(); } }, "Reset to the controls");
    container.replaceChildren(el("details", {}, el("summary", {}, "Create buffer"),
      el("div", { class: "small" }, "The spec the controls describe; edit it here if you need what they do not offer. Plan saves it under its name and starts a round, which stops at its plan: Run on the spec's page starts the jobs."),
      area, el("div", { class: "actions" }, run, reset, status)));
  }
  if (!area.dataset.edited) area.value = yaml;
  const run = container.querySelector("button"), status = container.querySelector(".actions .small");
  run.disabled = !!blocker && !area.dataset.edited;  // an edited spec is judged by the service, not by the controls' plan
  run.title = run.disabled ? blocker : BUTTON_HELP.plan;
  if (!status.textContent || status.dataset.blocker) { status.textContent = run.disabled ? blocker : ""; status.dataset.blocker = "1"; }
}

// ---- the plan's table: the datasets the spec's selection admits, one page at a time (POST /plan with `page`).
// The server joins each row with the registry and says why it is in the pool or not (mia_buffers.explore).
const splitText = x => Object.entries(x.splits || {}).map(([k, v]) => `${k} ${num(v)}`).join(" · ");
const PLAN_COLS = [
  { key: "dataset", label: "dataset", cell: x => { const a = datasetLink(x.id); a.title = x.title || x.id; return a; } },
  { label: "issue", cell: x => ghIcon(S.meta.datasets_repo, x.issue) },
  { label: "preview", cell: x => thumb(x) },
  { key: "decision", label: "decision", cell: x => pill(x.decision.group, x.in_pool ? "pool" : "", x.decision.reason && x.decision.reason !== x.decision.group ? [{ t: x.decision.reason }] : []) },
  { key: "dims", label: "dims", cell: x => x.dims || "" },
  { key: "modality", label: "modality", cell: x => x.modality_group || "" },
  { key: "licence", label: "licence", cell: x => x.licence || "" },
  { key: "state", label: "state", title: "preparation, then the raw cache's state, then \"staged\" when a pyramid of it exists for this geometry",
    cell: x => { const pills = el("span", { class: "pills" }, statePill(x.prepared)); if (x.raw) pills.append(" ", assetPill(x.raw.state)); if (x.staged_here) pills.append(" ", pill("staged", "pool")); if (x.quarantined) pills.append(" ", assetPill("quarantined")); return pills; } },
  { key: "raw", label: "raw cache", num: true, cell: x => x.raw?.bytes ? fmtBytes(x.raw.bytes) : "" },
  { key: "buffers", label: "buffers", num: true, cell: x => bufferCount(x.buffers) },
  { key: "bytes", label: "size", num: true, cell: x => x.bytes ? fmtBytes(x.bytes) : "" },
  { key: "weight", label: "weight", num: true, cell: x => x.weight ? pct(x.weight) : "" },
  { label: "split", cell: splitText },
];

// ---- datasets: controls → YAML → POST /plan (debounced) → KPIs, funnel, charts, table, actions
function chips(container, values, set, onchange) {
  container.replaceChildren(...values.map(v => { const input = el("input", { type: "checkbox" }); input.checked = set.has(v); input.addEventListener("change", () => { if (input.checked) set.add(v); else set.delete(v); onchange(); }); return el("label", { class: "chip" }, input, v); }));
}
let planTimer = 0, planSeq = 0, planTable = null, fitOnly = false;
function schedulePlan(ms) { clearTimeout(planTimer); planTimer = setTimeout(() => { if (planTable) planTable.reload(true); }, ms ?? PLAN_DEBOUNCE_MS); renderActions(); }
async function planPage(state) {  // the table's source: the plan of the spec the controls describe, one page of its rows
  if (!S.ref) return null;
  const seq = ++planSeq;
  $("planStatus").textContent = "planning…";
  const page = { limit: state.limit, offset: state.offset, dir: state.dir, q: state.q, ...(state.sort ? { sort: state.sort } : {}),
    fit_only: fitOnly, prepared_only: C.preparedOnly, raw_cached: C.rawCached, staged_geom: C.stagedGeom };
  let out = null, error = null;
  try { out = await api("/plan", { method: "POST", headers: JSON_HEADERS, body: JSON.stringify({ spec: toSpecYaml(C, S.ref, S.meta.catalog_repo), page }) }); }
  catch (e) { error = e; if (e.status !== 422 && e.status !== 409) banner(`POST /plan → ${e.status || "network error"}: ${e.message}`); }
  if (seq === planSeq) { S.plan = out; S.planError = error; $("planStatus").textContent = ""; renderDesigner(); }
  return { rows: out?.rows || [], total: out?.total || 0, out };
}
const SHOW = [];  // one function per control: make it show what C holds
export function bindControls() {
  const replan = () => schedulePlan();
  const drawChips = () => { chips($("dims"), DIMS, C.dims, replan); chips($("lic"), LIC, C.lic, replan); chips($("src"), SRC, C.sources, replan); };
  drawChips(); SHOW.push(drawChips);
  const bind = (id, key, parse, after) => { const n = $(id), show = () => { if (n.type === "checkbox") n.checked = C[key]; else n.value = String(C[key]); };
    show(); SHOW.push(show);
    n.addEventListener(n.type === "range" || n.type === "text" ? "input" : "change", () => { C[key] = n.type === "checkbox" ? n.checked : parse(n.value); after(); }); };
  const text = v => v, int = v => +v;
  bind("specName", "name", text, renderActions); bind("paths", "paths", text, replan);
  bind("allowUnknown", "allowUnknown", null, replan); bind("annRequired", "annRequired", null, replan);
  bind("minVox", "minVox", int, replan); bind("patch", "patch", int, replan); bind("level", "level", int, replan); bind("patchZ", "patchZ", int, replan);
  bind("dedup", "dedup", null, replan); bind("temp", "T", int, replan);
  for (const id of ["preparedOnly", "rawCached", "stagedGeom"]) bind(id, id, null, () => schedulePlan(0));  // registry state: the same plan, fewer rows
  $("download").addEventListener("click", () => {
    const blob = new Blob([toSpecYaml(C, S.ref, S.meta.catalog_repo)], { type: "application/yaml" });
    const a = el("a", { href: URL.createObjectURL(blob), download: `${C.name.trim() || "buffer"}.yaml` }); document.body.append(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
  });
}
// A spec for chosen datasets: New buffer opens on one that selects exactly them, each by its record's file name
// (a record's path ends `/<id>.yaml`), with every filter that could drop one opened. The plan there says which of
// them are in the pool: a dataset that is not prepared, or that the patch does not fit, still stays out.
export function specFromDatasets(ids) {
  Object.assign(C, { name: "selected-datasets", paths: ids.map(id => `*/${id}.yaml`).join(" "), dims: new Set(DIMS), lic: new Set(LIC), allowUnknown: true, annRequired: false,
    sources: new Set(SRC), minVox: 0, dedup: false, preparedOnly: false, rawCached: false, stagedGeom: false });
  SHOW.forEach(show => show());
  location.hash = "#buffers/new";
  schedulePlan(0);
}
// Why a buffer issue for the current spec would be refused by the planner, or null when the plan is clean.
function bufferBlocker() {
  if (!S.ref) return "no catalog synced";
  if (S.planError) return `the plan failed: ${S.planError.message.split("\n")[0]}`;
  if (!S.plan) return "planning…";
  const errors = S.plan.errors || [];
  if (!errors.length) return null;
  const waiting = S.plan.summary?.not_prepared || 0;
  if (errors.includes("no dataset admitted") && waiting) return `no prepared dataset fits yet; ${num(waiting)} fit but are not prepared: request preparation first`;
  return "the plan has errors: " + errors.join("; ");
}
function renderActions() {
  createBuffer($("bufferIssue"), toSpecYaml(C, S.ref, S.meta.catalog_repo), bufferBlocker());
  // every dataset that fits the spec and is not prepared; one in progress is already on its way
  const todo = S.plan?.summary?.to_prepare || [];
  prepareButton($("prepIssue"), todo, !S.plan ? "no plan yet" : todo.length ? null : TABLE_STATE.get("plan")?.q ? "no dataset that fits and matches the search is left to prepare" : "every dataset that fits is prepared or in progress");
}
function renderPlanMessages() {
  const box = $("planMsgs"); box.replaceChildren();
  const e = S.planError;
  if (e) box.append(el("div", { class: "notice" }, e.status === 422 ? "The service refused the spec (422):" : e.status === 409 ? "The catalog is not synced (409):" : `The plan failed (${e.status || "network error"}):`, el("pre", { class: "err" }, e.message)));
  for (const msg of S.plan?.errors || []) box.append(el("div", { class: "notice" }, "plan error: " + msg));
  for (const msg of S.plan?.warnings || []) box.append(el("div", { class: "notice" }, "warning: " + msg));
}
function renderDesigner() {  // the numbers over the whole plan, as the server summed them (`summary`)
  const F = C.patch * C.level;
  $("footprint").textContent = `footprint ${num(F)} px = ${C.patch} × ${C.level} · levels ${levelsOf(C.level).join(", ")}`;
  $("tVal").textContent = C.T.toFixed(2);
  $("levelsKey").textContent = levelsKey(C);
  renderPlanMessages();
  const plan = S.plan, m = plan?.summary || {}, dims = m.dims || [];
  $("kpis").replaceChildren(
    tile("Datasets that fit", plan ? num(m.fitting) : "—", plan ? `of ${plural(plan.n_seen, "record")} seen at ${short(plan.catalog_ref, 8)}` : "no plan yet", "hero"),
    tile("Prepared", num(m.prepared || 0), "the buffer's pool today"),
    tile("In progress", num(m.in_progress || 0), "mia-data is preparing them"),
    tile("Unprepared", num(m.unprepared || 0), `${num(m.needs_human || 0)} ${m.needs_human === 1 ? "needs" : "need"} a human`),
    tile("Raw bytes", `${fmtBytes(m.bytes_pool || 0)} / ${fmtBytes(m.bytes_fitting || 0)}`, `the prepared pool now / every fitting dataset; ${fmtBytes(m.bytes_to_fetch || 0)} not in the raw cache`),
    tile("Dimensionalities", dims.length ? dims.join(" · ") : "none", dims.length > 1 ? "one buffer serves one: narrow dims" : "one buffer, one model", "text"),
    tile("Capacity", fmt(m.capacity || 0), "non-overlapping coarsest crops in the fitting datasets"),
    tile("Top dataset share", m.pool ? pct(m.top_share) : "—", "of the crops drawn from the prepared pool"));
  // the funnel is the plan's own: it ignores the registry toggles
  const steps = (m.funnel || []).map(step => ({ label: step.label, value: step.value, sub: step.remaining == null ? undefined : `${num(step.remaining)} remain after this rule`,
    color: step.label === NOT_PREPARED ? "var(--s4)" : step.remaining == null ? "var(--s3)" : "var(--accent)" }));
  hbars($("funnel"), steps, { labelW: 165, aria: "Records removed per rule", empty: S.planError ? "no plan: see the message above the actions" : "planning…" });
  hbars($("bymod"), m.by_modality || [], { labelW: 140, aria: "Fitting datasets by modality", empty: "No dataset fits this spec." });
  hbars($("byweight"), (m.weight_by_modality || []).map(bar => ({ ...bar, text: pct(bar.value) })), { labelW: 140, aria: "Weight share by modality", empty: "No prepared dataset in the pool yet: weights cover prepared datasets only." });
  const heavy = m.heaviest || [];
  $("top").replaceChildren(...(heavy.length ? heavy.map(x => el("li", {}, el("b", {}, pct(x.weight || 0)), el("span", { title: x.id }, `${x.id} · ${x.modality_group || "none"} · ${fmt(x.positions || 0)} positions · ${x.channels ?? "?"} ch`))) : [el("li", { class: "empty" }, "No prepared dataset in the pool yet.")]));
  renderActions();
}
export function viewDesigner() {
  $("dsEmpty").hidden = !!S.ref; $("dsBody").hidden = !S.ref;
  if (!S.ref) return;
  if (planTable) return planTable.reload();
  const fit = el("input", { type: "checkbox" });
  fit.addEventListener("change", () => { fitOnly = fit.checked; planTable.reload(true); });
  planTable = pagedTable("plan", { source: planPage, cols: PLAN_COLS, sort: "decision", search: "search id, title, modality, licence",
    tools: [el("label", { class: "inline" }, fit, " fitting only")], empty: "The spec admits no dataset.", rowAttrs: x => ({ class: x.in_pool ? "" : "out" }) });
  $("planTable").replaceChildren(planTable);
}
