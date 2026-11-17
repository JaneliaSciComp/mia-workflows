// Steps 1 and 2 of a buffer: its definition as controls and as YAML, the plan of that definition as numbers,
// charts and a table, and the tick that takes a dataset out.
import { DIMS, fromSpec, levelsKey, levelsOf, LIC, SRC, specDiff, toSpecYaml } from "./spec.js";
import {
  $,
  assetPill,
  bufferCount,
  datasetLink,
  el,
  fmt,
  fmtBytes,
  ghIcon,
  hoverable,
  num,
  pct,
  pill,
  plural,
  short,
  statePill,
  svgEl,
  thumb,
  tile,
} from "./dom.js";
import { api, banner, JSON_HEADERS, S } from "./api.js";
import { pagedTable } from "./table.js";

const NOT_PREPARED = "not prepared"; // mia_buffers.plan.NOT_PREPARED
const PLAN_DEBOUNCE_MS = 300;
const YAML_DEBOUNCE_MS = 400; // a pause in typing before the service is asked to read the YAML

// ---- horizontal bars (single series, sequential hue)
function barPath(x, y, w, h, r) {
  if (w <= r) return `M${x} ${y}h${w}v${h}h${-w}z`;
  return `M${x} ${y}h${w - r}a${r} ${r} 0 0 1 ${r} ${r}v${h - 2 * r}a${r} ${r} 0 0 1 ${-r} ${r}h${-(w - r)}z`;
}
function hbars(container, items, opts) {
  if (!items.length) {
    container.replaceChildren(el("div", { class: "empty" }, opts.empty || "Nothing to show."));
    return;
  }
  const W = 560,
    labelW = opts.labelW || 170,
    rowH = 26,
    barH = 16,
    pad = 8;
  const H = items.length * rowH + pad * 2;
  const max = Math.max(1e-12, ...items.map(i => i.value));
  const svg = svgEl("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": opts.aria || "" });
  const plotW = W - labelW - 70;
  items.forEach((it, i) => {
    const y = pad + i * rowH;
    const g = svgEl("g");
    const lbl = svgEl("text", { x: labelW - 8, y: y + barH - 3, "text-anchor": "end", class: "lbl" });
    lbl.textContent = it.label.length > 26 ? it.label.slice(0, 25) + "…" : it.label;
    g.append(lbl);
    const w = Math.max(0, (it.value / max) * plotW);
    g.append(svgEl("path", { d: barPath(labelW, y, w, barH, 4), fill: it.color || "var(--accent)" }));
    const v = svgEl("text", { x: labelW + w + 6, y: y + barH - 3, class: "tick" });
    v.textContent = it.text || num(it.value);
    g.append(v);
    const hit = svgEl("rect", { x: 0, y: y - 4, width: W, height: rowH, fill: "transparent" });
    g.append(hit);
    hoverable(hit, [{ b: it.text || num(it.value), t: " " + it.label }, ...(it.sub ? [{ t: it.sub }] : [])]);
    svg.append(g);
  });
  svg.append(svgEl("line", { x1: labelW, x2: labelW, y1: pad - 4, y2: H - pad + 4, class: "base" }));
  container.replaceChildren(svg);
}

// ---- the definition. It is one text with two views. While the YAML tab holds a text that is not what the
// controls write (typed or pasted there, or a saved buffer's own text), that text is the definition (`TEXT`);
// once a control is changed, the definition is what the controls write.
const defaults = () => ({
  name: "microscopy-2d-gray",
  paths: "datasets/**",
  dims: new Set(["2D"]),
  lic: new Set(["CC0-1.0", "CC-BY-3.0", "CC-BY-4.0", "MIT", "BSD-3-Clause"]),
  allowUnknown: false,
  annRequired: false,
  sources: new Set(SRC),
  minVox: 1e6,
  patch: 256,
  level: 4,
  patchZ: 32,
  dedup: true,
  T: 0.5,
  cropsTrain: 100000,
  cropsVal: 2000,
  cropsTest: 2000,
  maxCrops: 1024,
  holdout: "",
  exclude: new Set(), // the datasets unticked on step 2
  preparedOnly: false,
  rawCached: false,
  stagedGeom: false,
});
const C = defaults();
let TEXT = null; // the definition as text, while it is not what the controls write
let LOST = []; // what TEXT holds that no control shows: while it holds any, the controls are read-only
let INVALID = null; // why the YAML tab's text is not a definition
let SAVED = null; // the stored text of the saved buffer being shown; null for a buffer that is not saved
let LOADED = null; // whose definition this is: a saved buffer's key, "new" for one not saved, null for none yet
let LOCKED = false; // the buffer cannot change now: a round is open, it serves, or it is shelved
let onDesign = () => {}; // told whenever the definition or its plan changed (the page redraws the step's footer)

export const definition = () => TEXT ?? toSpecYaml(C, S.ref, S.meta.catalog_repo);
// whether a saved buffer's definition differs from what is stored
export const changed = () => SAVED != null && definition() !== SAVED;
// why the way forward is closed, or null
export const problem = () =>
  !S.ref
    ? "no catalog synced"
    : INVALID
      ? `the YAML is not a definition: ${INVALID}`
      : S.planError?.status === 422
        ? `the service refused this definition: ${S.planError.message.split("\n")[0]}`
        : null;
export const planSummary = () => S.plan?.summary || null;
export function setOnDesign(fn) {
  onDesign = fn;
}

const parseSpec = async text =>
  (await api("/specs/parse", { method: "POST", headers: JSON_HEADERS, body: JSON.stringify({ spec: text }) })).spec;
let readSeq = 0;
// Take `text` as the definition: the controls get its values as the service parsed it, and what they cannot show
// is found by parsing the definition the controls would write for it. Text that is no definition changes nothing
// but `INVALID`.
async function readText(text) {
  const seq = ++readSeq;
  let parsed,
    lost = [];
  try {
    parsed = await parseSpec(text);
  } catch (error) {
    if (seq !== readSeq) return;
    TEXT = text;
    INVALID = error.message.split("\n")[0];
    return;
  }
  try {
    lost = specDiff(parsed, await parseSpec(toSpecYaml(fromSpec(parsed), parsed.catalog.ref, parsed.catalog.repo)));
  } catch {
    lost = ["its sampling and splits"]; // the controls' own text is refused: they cannot hold this one at all
  }
  if (seq !== readSeq) return; // a later text was typed while this one was read
  Object.assign(C, fromSpec(parsed));
  TEXT = text;
  INVALID = null;
  LOST = lost;
}
// A control was changed: from here on the definition is what the controls write.
function touched() {
  TEXT = null;
  INVALID = null;
  showText();
  schedulePlan();
  onDesign();
}
// the YAML tab shows the definition, but is never rewritten under someone typing in it
function showText() {
  const box = $("specText");
  if (document.activeElement !== box) box.value = definition();
}
function showLock() {
  $("controlsLock").disabled = LOCKED || LOST.length > 0;
  $("specText").readOnly = LOCKED;
  $("yamlMsg").textContent = INVALID ? `Not a definition: ${INVALID}` : "";
}
const SHOW = []; // one function per control: make it show what C holds
function showAll() {
  SHOW.forEach(show => show());
  showText();
  showLock();
}
// Open a definition: a saved buffer's text, or (null) a new one from the defaults and `preset`.
async function openDefinition(text, preset) {
  Object.assign(C, defaults(), preset || {});
  TEXT = null;
  LOST = [];
  INVALID = null;
  SAVED = text;
  if (text != null) await readText(text);
  showAll();
  schedulePlan(0);
}
// Make the controls and the YAML tab hold this buffer's definition (`subject`: null for one not saved). A
// definition someone is changing is left alone: the stream redraws the page while they type.
export async function ensureDefinition(subject) {
  const key = subject ? subject.key : "new",
    stored = subject ? subject.data?.spec || "" : null;
  if (LOADED !== key) {
    LOADED = key;
    return openDefinition(stored);
  }
  if (stored == null || stored === SAVED) return;
  if (definition() === stored) {
    SAVED = stored; // this page's own change, now stored
    return;
  }
  if (changed()) return; // stored text moved under an edit: the save will be refused by its revision
  return openDefinition(stored);
}
// The buffer can change now, or not (a round is open, it serves, it is shelved).
export function setLocked(locked) {
  LOCKED = locked;
  showLock();
}
// Start a new buffer from the defaults, dropping what was entered.
export function startOver() {
  LOADED = "new";
  return openDefinition(null);
}
// A buffer that is not saved yet, with the definition now shown: of a saved buffer (Clone), or one built by
// `specFromDatasets`. The page then opens New buffer on it.
export function keepAsNew() {
  LOADED = "new";
  SAVED = null;
}
// The buffer that was not saved is saved now: the next New buffer starts from the defaults.
export function forgetNew() {
  if (LOADED === "new") LOADED = null;
}

// ---- the plan's table: the datasets the definition's selection admits, one page at a time (POST /plan with `page`).
// The server joins each row with the registry and says why it is in the pool or not (mia_buffers.explore).
const splitText = x =>
  Object.entries(x.splits || {})
    .map(([k, v]) => `${k} ${num(v)}`)
    .join(" · ");
const PLAN_COLS = [
  {
    label: "selected",
    title: "untick a dataset that fits to take it out of this buffer; it is then listed above the table",
    cell: x => {
      if (!x.fits) return ""; // a rule of the definition keeps it out: there is nothing to tick
      const input = el("input", {
        type: "checkbox",
        "aria-label": "selected: " + x.id,
        disabled: LOCKED || LOST.length > 0,
      });
      input.checked = true;
      input.addEventListener("change", () => {
        C.exclude.add(x.id);
        touched();
      });
      return input;
    },
  },
  {
    key: "dataset",
    label: "dataset",
    cell: x => {
      const a = datasetLink(x.id);
      a.title = x.title || x.id;
      return a;
    },
  },
  { label: "issue", cell: x => ghIcon(S.meta.datasets_repo, x.issue) },
  { label: "preview", cell: x => thumb(x) },
  {
    key: "decision",
    label: "decision",
    cell: x =>
      pill(
        x.decision.group,
        x.in_pool ? "pool" : "",
        x.decision.reason && x.decision.reason !== x.decision.group ? [{ t: x.decision.reason }] : [],
      ),
  },
  { key: "dims", label: "dims", cell: x => x.dims || "" },
  { key: "modality", label: "modality", cell: x => x.modality_group || "" },
  { key: "licence", label: "licence", cell: x => x.licence || "" },
  {
    key: "state",
    label: "state",
    title: 'preparation, then the raw cache\'s state, then "staged" when a pyramid of it exists for this geometry',
    cell: x => {
      const pills = el("span", { class: "pills" }, statePill(x.prepared));
      if (x.raw) pills.append(" ", assetPill(x.raw.state));
      if (x.staged_here) pills.append(" ", pill("staged", "pool"));
      if (x.quarantined) pills.append(" ", assetPill("quarantined"));
      return pills;
    },
  },
  { key: "raw", label: "raw cache", num: true, cell: x => (x.raw?.bytes ? fmtBytes(x.raw.bytes) : "") },
  { key: "buffers", label: "buffers", num: true, cell: x => bufferCount(x.buffers) },
  { key: "bytes", label: "size", num: true, cell: x => (x.bytes ? fmtBytes(x.bytes) : "") },
  { key: "weight", label: "weight", num: true, cell: x => (x.weight ? pct(x.weight) : "") },
  { label: "split", cell: splitText },
];

// ---- controls → the definition → POST /plan (debounced) → numbers, funnel, charts, table
function chips(container, values, set, onchange) {
  container.replaceChildren(
    ...values.map(v => {
      const input = el("input", { type: "checkbox" });
      input.checked = set.has(v);
      input.addEventListener("change", () => {
        if (input.checked) set.add(v);
        else set.delete(v);
        onchange();
      });
      return el("label", { class: "chip" }, input, v);
    }),
  );
}
let planTimer = 0,
  planSeq = 0,
  planTable = null,
  fitOnly = false;
function schedulePlan(ms) {
  clearTimeout(planTimer);
  planTimer = setTimeout(() => {
    if (planTable) planTable.reload(true);
  }, ms ?? PLAN_DEBOUNCE_MS);
}
async function planPage(state) {
  // the table's source: the plan of the definition, one page of its rows
  if (!S.ref) return null;
  if (INVALID) return { rows: S.plan?.rows || [], total: S.plan?.total || 0, out: S.plan }; // nothing to plan: the last plan stays
  const seq = ++planSeq;
  $("planStatus").textContent = "planning…";
  const page = {
    limit: state.limit,
    offset: state.offset,
    dir: state.dir,
    q: state.q,
    ...(state.sort ? { sort: state.sort } : {}),
    fit_only: fitOnly,
    prepared_only: C.preparedOnly,
    raw_cached: C.rawCached,
    staged_geom: C.stagedGeom,
  };
  let out = null,
    error = null;
  try {
    out = await api("/plan", {
      method: "POST",
      headers: JSON_HEADERS,
      body: JSON.stringify({ spec: definition(), page }),
    });
  } catch (e) {
    error = e;
    if (e.status !== 422 && e.status !== 409) banner(`POST /plan → ${e.status || "network error"}: ${e.message}`);
  }
  if (seq === planSeq) {
    S.plan = out;
    S.planError = error;
    $("planStatus").textContent = "";
    renderDesigner();
  }
  return { rows: out?.rows || [], total: out?.total || 0, out };
}
export function bindControls() {
  const drawChips = () => {
    chips($("dims"), DIMS, C.dims, touched);
    chips($("lic"), LIC, C.lic, touched);
    chips($("src"), SRC, C.sources, touched);
  };
  drawChips();
  SHOW.push(drawChips);
  const bind = (id, key, parse, after) => {
    const n = $(id),
      show = () => {
        if (n.type === "checkbox") n.checked = C[key];
        else n.value = String(C[key]);
      };
    show();
    SHOW.push(show);
    n.addEventListener(n.type === "range" || n.type === "text" ? "input" : "change", () => {
      C[key] = n.type === "checkbox" ? n.checked : parse(n.value);
      if (n.type === "number") show(); // a count that was not one reads as the count the definition gets
      after();
    });
  };
  const text = v => v,
    int = v => +v,
    count = v => Math.max(1, Math.floor(+v) || 1);
  bind("specName", "name", text, touched);
  bind("paths", "paths", text, touched);
  bind("allowUnknown", "allowUnknown", null, touched);
  bind("annRequired", "annRequired", null, touched);
  bind("minVox", "minVox", int, touched);
  bind("patch", "patch", int, touched);
  bind("level", "level", int, touched);
  bind("patchZ", "patchZ", int, touched);
  bind("dedup", "dedup", null, touched);
  bind("temp", "T", int, touched);
  for (const [id, key] of [
    ["cropsTrain", "cropsTrain"],
    ["cropsVal", "cropsVal"],
    ["cropsTest", "cropsTest"],
    ["maxCrops", "maxCrops"],
  ])
    bind(id, key, count, touched);
  bind("holdout", "holdout", text, touched);
  // what the registry knows narrows the table: the same definition, fewer rows
  for (const id of ["preparedOnly", "rawCached", "stagedGeom"]) bind(id, id, null, () => schedulePlan(0));
  // the two tabs of the definition
  const tabs = { tabControls: "paneControls", tabYaml: "paneYaml" };
  for (const [tab, pane] of Object.entries(tabs))
    $(tab).addEventListener("click", () => {
      for (const [other, shown] of Object.entries(tabs)) {
        $(other).setAttribute("aria-selected", String(other === tab));
        $(shown).hidden = shown !== pane;
      }
      showText();
    });
  // the YAML tab: after a pause in typing the service reads the text, and the controls follow
  let typing = 0;
  $("specText").addEventListener("input", () => {
    clearTimeout(typing);
    const typed = $("specText").value;
    typing = setTimeout(async () => {
      await readText(typed);
      SHOW.forEach(show => show());
      showLock();
      schedulePlan(0);
      renderPlanMessages();
      onDesign();
    }, YAML_DEBOUNCE_MS);
  });
  $("download").addEventListener("click", () => {
    const blob = new Blob([definition()], { type: "application/yaml" });
    const a = el("a", { href: URL.createObjectURL(blob), download: `${C.name.trim() || "buffer"}.yaml` });
    document.body.append(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
  });
}
// A definition for chosen datasets: New buffer opens on one that selects exactly them, each by its record's file
// name (a record's path ends `/<id>.yaml`), with every filter that could drop one opened. The plan there says
// which of them are selected: a dataset that the patch does not fit still stays out.
export async function specFromDatasets(ids) {
  await openDefinition(null, {
    name: "selected-datasets",
    paths: ids.map(id => `*/${id}.yaml`).join(" "),
    dims: new Set(DIMS),
    lic: new Set(LIC),
    allowUnknown: true,
    annRequired: false,
    sources: new Set(SRC),
    minVox: 0,
    dedup: false,
  });
  keepAsNew();
  location.hash = "#buffers/new";
}
function renderPlanMessages() {
  const box = $("planMsgs");
  box.replaceChildren();
  if (LOST.length)
    box.append(
      el(
        "div",
        { class: "notice" },
        `The YAML holds what the controls do not show (${LOST.join(", ")}). The YAML is the definition, and the controls are read-only while it holds that.`,
      ),
    );
  const e = S.planError;
  if (e)
    box.append(
      el(
        "div",
        { class: "notice" },
        e.status === 422
          ? "The service refused the definition (422):"
          : e.status === 409
            ? "The catalog is not synced (409):"
            : `The plan failed (${e.status || "network error"}):`,
        el("pre", { class: "err" }, e.message),
      ),
    );
  for (const msg of S.plan?.errors || []) box.append(el("div", { class: "notice" }, "plan error: " + msg));
  for (const msg of S.plan?.warnings || []) box.append(el("div", { class: "notice" }, "warning: " + msg));
}
function renderDesigner() {
  // the numbers over the whole plan, as the server summed them (`summary`)
  const F = C.patch * C.level;
  $("footprint").textContent =
    `footprint ${num(F)} px = ${C.patch} × ${C.level} · levels ${levelsOf(C.level).join(", ")}`;
  $("tVal").textContent = C.T.toFixed(2);
  $("levelsKey").textContent = levelsKey(C);
  renderPlanMessages();
  const plan = S.plan,
    m = plan?.summary || {},
    dims = m.dims || [];
  // step 1: the two numbers beside the two cards
  $("kpisFit").replaceChildren(
    tile(
      "Datasets that fit",
      plan ? num(m.fitting) : "—",
      plan ? `of ${plural(plan.n_seen, "record")} at ${short(plan.catalog_ref, 8)}` : "no plan yet",
    ),
    tile("Prepared", num(m.prepared || 0), `${num(m.in_progress || 0)} more in progress`),
  );
  // step 2: the rest
  $("kpis").replaceChildren(
    tile("In progress", num(m.in_progress || 0), "mia-data is preparing them"),
    tile(
      "Unprepared",
      num(m.unprepared || 0),
      `${num(m.needs_human || 0)} ${m.needs_human === 1 ? "needs" : "need"} a human`,
    ),
    tile(
      "Raw bytes",
      `${fmtBytes(m.bytes_pool || 0)} / ${fmtBytes(m.bytes_fitting || 0)}`,
      `the prepared datasets now / every fitting dataset; ${fmtBytes(m.bytes_to_fetch || 0)} not in the raw cache`,
    ),
    tile(
      "Dimensionalities",
      dims.length ? dims.join(" · ") : "none",
      dims.length > 1 ? "one buffer serves one: narrow dims" : "one buffer, one model",
      "text",
    ),
    tile("Capacity", fmt(m.capacity || 0), "non-overlapping coarsest crops in the fitting datasets"),
    tile("Top dataset share", m.pool ? pct(m.top_share) : "—", "of the crops drawn from the prepared datasets"),
  );
  // the funnel is the plan's own: it ignores the registry toggles
  const steps = (m.funnel || []).map(step => ({
    label: step.label,
    value: step.value,
    sub: step.remaining == null ? undefined : `${num(step.remaining)} remain after this rule`,
    color: step.label === NOT_PREPARED ? "var(--s4)" : step.remaining == null ? "var(--s3)" : "var(--accent)",
  }));
  hbars($("funnel"), steps, {
    labelW: 165,
    aria: "Records removed per rule",
    empty: S.planError ? "no plan: see the message on step 1" : "planning…",
  });
  hbars($("bymod"), m.by_modality || [], {
    labelW: 140,
    aria: "Fitting datasets by modality",
    empty: "No dataset fits this definition.",
  });
  hbars(
    $("byweight"),
    (m.weight_by_modality || []).map(bar => ({ ...bar, text: pct(bar.value) })),
    {
      labelW: 140,
      aria: "Weight share by modality",
      empty: "No prepared dataset fits yet: weights cover prepared datasets only.",
    },
  );
  const heavy = m.heaviest || [];
  $("top").replaceChildren(
    ...(heavy.length
      ? heavy.map(x =>
          el(
            "li",
            {},
            el("b", {}, pct(x.weight || 0)),
            el(
              "span",
              { title: x.id },
              `${x.id} · ${x.modality_group || "none"} · ${fmt(x.positions || 0)} positions · ${x.channels ?? "?"} ch`,
            ),
          ),
        )
      : [el("li", { class: "empty" }, "No prepared dataset fits yet.")]),
  );
  // the datasets a person took out: the planner drops them before the table, so they are listed here to put back
  const out = [...C.exclude].sort();
  $("takenOut").replaceChildren(
    ...(out.length
      ? [
          el(
            "div",
            { class: "takenout" },
            el("b", {}, `Taken out (${num(out.length)}):`),
            ...out.map(id =>
              el(
                "span",
                { class: "chosen" },
                id,
                el(
                  "button",
                  {
                    type: "button",
                    class: "btn",
                    title: "select this dataset again",
                    disabled: LOCKED || LOST.length > 0,
                    onclick: () => {
                      C.exclude.delete(id);
                      touched();
                    },
                  },
                  "put back",
                ),
              ),
            ),
          ),
        ]
      : []),
  );
  onDesign();
}
// Draw steps 1 and 2 (the page shows one of them): the table is the one thing that asks for the plan.
export function showDesigner() {
  $("dsEmpty").hidden = !!S.ref;
  $("dsBody").hidden = !S.ref;
  if (!S.ref) return;
  showAll();
  if (planTable) return planTable.reload();
  const fit = el("input", { type: "checkbox" });
  fit.addEventListener("change", () => {
    fitOnly = fit.checked;
    planTable.reload(true);
  });
  planTable = pagedTable("plan", {
    source: planPage,
    cols: PLAN_COLS,
    sort: "decision",
    search: "search id, title, modality, licence",
    tools: [el("label", { class: "inline" }, fit, " fitting only")],
    empty: "The definition admits no dataset.",
    rowAttrs: x => ({ class: x.in_pool ? "" : "out" }),
  });
  $("planTable").replaceChildren(planTable);
}
