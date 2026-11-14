// DOM helpers and the page's presentation primitives: nodes built from text (never parsed HTML), numbers and
// times, links and addresses, pills with their help, the tooltip, cards, and a lifecycle's line.

export const $ = id => document.getElementById(id);
export const fmt = n =>
  n >= 1e6
    ? (n / 1e6).toFixed(n >= 1e7 ? 0 : 1) + "M"
    : n >= 1e3
      ? (n / 1e3).toFixed(n >= 1e4 ? 0 : 1) + "K"
      : String(n);
export const num = n => (n == null || n === "" ? "" : Number(n).toLocaleString());
export const plural = (n, one) => `${num(n)} ${Number(n) === 1 ? one : one + "s"}`; // "1 dataset", "2 datasets"
export const fmtBytes = b => {
  b = Number(b) || 0;
  for (const [s, u] of [
    [1e12, "TB"],
    [1e9, "GB"],
    [1e6, "MB"],
    [1e3, "KB"],
  ])
    if (b >= s) return (b / s).toFixed(1) + " " + u;
  return b + " B";
};
export const short = (s, n) => (s ? String(s).slice(0, n || 8) : "");
export const pct = x => (x * 100).toFixed(x && x < 0.001 ? 3 : 1) + "%";
export const el = (tag, attrs, ...kids) => {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v == null || v === false) continue;
    if (k === "class") n.className = v;
    else if (k.startsWith("on")) n.addEventListener(k.slice(2), v);
    else n.setAttribute(k, v === true ? "" : v);
  }
  for (const k of kids) if (k != null && k !== false) n.append(k);
  return n;
};
export const svgEl = (tag, attrs) => {
  const n = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [k, v] of Object.entries(attrs || {})) n.setAttribute(k, v);
  return n;
};
// catalog records are untrusted: only an http(s) href becomes a link, anything else (javascript:, data:) stays text
// a link to GitHub is never spelled out: it starts with the GitHub mark
const GH_MARK =
  "M8 0c4.42 0 8 3.58 8 8a8.013 8.013 0 0 1-5.45 7.59c-.4.08-.55-.17-.55-.38 0-.27.01-1.13.01-2.2 0-.75-.25-1.23-.54-1.48 1.78-.2 3.65-.88 3.65-3.95 0-.88-.31-1.59-.82-2.15.08-.2.36-1.02-.08-2.12 0 0-.67-.22-2.2.82-.64-.18-1.32-.27-2-.27-.68 0-1.36.09-2 .27-1.53-1.03-2.2-.82-2.2-.82-.44 1.1-.16 1.92-.08 2.12-.51.56-.82 1.28-.82 2.15 0 3.06 1.86 3.75 3.64 3.95-.23.2-.44.55-.51 1.07-.46.21-1.61.55-2.33-.66-.15-.24-.6-.83-1.23-.82-.67.01-.27.38.01.53.34.19.73.9.82 1.13.16.45.68 1.31 2.69.94 0 .67.01 1.3.01 1.49 0 .21-.15.45-.55.38A7.995 7.995 0 0 1 0 8c0-4.42 3.58-8 8-8Z";
const ghMark = () => {
  const svg = svgEl("svg", {
    class: "ghm",
    viewBox: "0 0 16 16",
    width: 14,
    height: 14,
    fill: "currentColor",
    "aria-hidden": "true",
  });
  svg.append(svgEl("path", { d: GH_MARK }));
  return svg;
};
// a link into this page (a job's record in a timeline: `<this origin>/#job/…`) opens here, any other in a new tab
const intoThisPage = href => typeof location !== "undefined" && String(href).startsWith(`${location.origin}/#`);
export const link = (href, text, title) =>
  /^https?:\/\//i.test(String(href || ""))
    ? el(
        "a",
        intoThisPage(href) ? { href, title } : { href, target: "_blank", rel: "noopener", title },
        ...(/^https:\/\/github\.com\//i.test(href) ? [ghMark(), " "] : []),
        ...[].concat(text),
      )
    : text;
// what each pill means, by its text; the tooltip shows it (a decision pill adds the planner's exact reason)
export const PILL_HELP = {
  // the planner's decision for a dataset against the current spec
  "pool": "Fits the spec and is prepared: the dataset is in the plan's pool and would supply crops.",
  "path not selected": "The record's path in the catalog is not under the spec's select.paths globs.",
  "excluded by path": "The record's path matches one of the spec's exclude.paths globs.",
  "excluded by id": "The record's id is listed in the spec's exclude.ids.",
  "dims not selected": "The dataset's dimensionality (2D, 2D+t, 3D, 3D+t) is not in the spec's where.dims.",
  "modality": "None of the dataset's modalities is in the spec's where.modality.",
  "organism": "The dataset's organism is not in the spec's where.organism.",
  "licence unknown": "The record names no SPDX licence and the spec does not allow unknown licences.",
  "licence not allowed": "The record's licence is not in the spec's licence allowlist.",
  "annotations required": "The spec requires annotations and the record has none.",
  "annotation types": "The record's annotation types (masks, boxes, points, …) are not among those the spec asks for.",
  "annotation source":
    "The record's annotation source (manual, proofread, automatic, …) is not among those the spec asks for.",
  "array below minimum": "The record's largest array has fewer voxels than the spec's where.min_size_voxels.",
  "no mia-data folder":
    "The record names no modality the preparation pipeline knows, and a prepared dataset's folder is named after its modality family, so the pipeline cannot place it.",
  "duplicate DOI": "Another record with the same DOI is already in the pool; dedup keeps the first.",
  "record unreadable": "The record's YAML could not be parsed; the planner skipped it.",
  "no patch for dims":
    "The spec has no patch size for this dataset's dimensionality (sampling.patch has no entry for it).",
  "no array shape":
    "The catalog record does not give the shape of its largest array (y and x extents, and z for 3D), so the planner cannot tell whether a crop fits in it.",
  "footprint too large":
    "The coarsest crop (patch size times the largest pyramid level) is bigger than the dataset's largest array.",
  "too few z slices": "The 3D patch's z extent is deeper than the dataset's largest array.",
  "voxel size unknown":
    "The record gives no voxel size and the spec does not allow assuming one (sampling.assume_voxel_size).",
  "not prepared":
    "Fits the spec, but no reader exists for the dataset's files yet; prepare it, and a later round will pick it up.",
  // the preparation pipeline's view of a dataset
  "prepared": "An approved binding and verified crops exist: the dataset can be staged and served.",
  "in-progress":
    "The preparation pipeline is working on it by itself: nothing here waits for a person. The stage says where it is.",
  "unprepared": "Nobody has asked for this dataset to be prepared.",
  "cannot start":
    "The dataset's catalog record lacks what a preparation round needs; the lines below say what. No round can be started for it. It can be started once a catalog sync brings a record that has it.",
  "needs-human":
    "A person has to act. The stage says what: review (its crops wait for a verdict), failed (a step failed; running it again may pass) or blocked (something has to be decided or changed first). The newest entry of its timeline says why.",
  "revoked":
    "Withdrawn by a person after its crops were saved (Revoke): nothing is selected or reused from it; Run again on its page repairs it.",
  "shelved":
    "Set aside by a person, with a reason (Shelve): no preparation request starts it and no buffer plan takes it. Unshelve on its page takes it back as it was.",
  // a cache directory or a staged pyramid
  "cached":
    "The dataset's raw files are in the shared cache and no job is writing them; a buffer checks them against its binding before reusing them.",
  "fetching": "A fetch job is downloading the dataset's raw files into the cache.",
  "staging": "A stage job is converting the raw files into an OME-Zarr pyramid.",
  "waiting-drop":
    "The files cannot be fetched by a job; a person has to place them in the dataset's incoming/ directory.",
  "partial": "A fetch ended before the cache was complete; the next fetch resumes it.",
  "missing": "Expected on disk but not found by the last scan.",
  "evicted": "Removed by the keeper to stay under quota; a buffer that needs it fetches or stages again.",
  "staged": "At least one pyramid of this dataset exists in the staged cache.",
  "quarantined":
    "Kept on disk for autonomy.quarantine_days after a revocation; the keeper does not evict it while an investigation may need it.",
  // a buffer's stage
  "new": "Nothing has run yet.",
  "planning": "Resolving the spec against the catalog.",
  "plan-review": "The plan is in the timeline; the round waits here for Run, and no job has run yet.",
  "sourcing": "Fetching and converting the pool's datasets, several at a time.",
  "ready": "Serving: consumers can draw crops.",
  "idle": "No consumer has drawn for a while; Keep returns it to ready, otherwise the keeper retires it later.",
  "retired": "Its references are released; consumers get 410, and the keeper may reclaim the space.",
  "superseded": "A newer round of the same spec replaced this buffer.",
  // a dataset of a buffer's pool during sourcing (the board)
  "pending": "Not started yet; waits for a sourcing slot.",
  "waiting-dataset": "No approved binding yet; the preparation pipeline is working on the dataset.",
  "waiting-lock": "Another job holds the dataset's raw-cache lock; this one waits for it.",
  "blocked": "The raw cache is over its quota; the fetch waits for the keeper to make room.",
  "servable": "Its volumes are registered and the service can draw crops from them.",
  "failed": "The fetch or stage job failed; the reason is in the row and in the spec's timeline.",
  "dropped": "Left out of the round: still waiting when the day limit passed.",
  // the preparation pipeline's stages (mia-data)
  "matching": "Looking for an existing reader snippet that fits the dataset's files.",
  "developing": "An agent is writing a reader snippet for the dataset.",
  "testing": "Making the sample crops a reviewer will look at.",
  "review": "Sample crops are ready; the dataset's page waits for a reviewer to approve or reject them.",
  "merging": "The crops were approved; the reader's pull request is being merged.",
  "debugging":
    "The round failed. A debugging agent looks into it and, if a new round can pass, runs one; nothing is needed from you.",
  "done": "The dataset is prepared: its crops and binding are saved.",
  // odds and ends
  "stale": "This consumer has not been seen for longer than expected.",
};
export function tipOn(node, lines) {
  // the page's tooltip on hover (no tab stop: tables hold hundreds of pills). A line is {k, b, t, img, full}:
  // a colour key, a bold word, text, a 256 px picture, a picture at its own size (as far as the window has room)
  node.addEventListener("pointerenter", e => showTip(e, lines));
  node.addEventListener("pointermove", moveTip);
  node.addEventListener("pointerleave", hideTip);
  return node;
}
// a pill explains itself on hover: PILL_HELP by its text, plus any extra lines (a count pill passes its own)
export const pill = (text, kind, extra) => {
  const node = el("span", { class: "pill" + (kind ? " " + kind : "") }, text);
  const help = PILL_HELP[text];
  const lines = [...(help ? [{ b: text, t: help }] : []), ...(extra || [])];
  return lines.length ? tipOn(node, lines) : node;
};
const GOOD = new Set(["prepared", "ready", "servable", "pool", "cached"]);
const BAD = new Set(["failed", "needs-human", "missing", "blocked", "evicted", "stale", "revoked", "quarantined"]);
export const statePill = s => (s ? pill(s, GOOD.has(s) ? "pool" : BAD.has(s) ? "warn" : "") : "");
// a subject's stage as a pill. A stage that shares its name with another thing's state says what the stage means.
const STAGE_HELP = {
  "staged": "No job is left to run and the board is in the timeline; the round waits for Approve.",
  "requested": "The request's datasets are handed to the preparation pipeline; it follows them until each has settled.",
  "cancelled": "A person cancelled the round.",
  "failed":
    "A step of the round failed; the newest entry of the timeline says which and why. Running it again may be all it needs.",
  "blocked":
    "The round stopped until a person decides or changes something; the newest entry of the timeline says what.",
};
export const stagePill = s => {
  if (!s) return "";
  const node = el("span", { class: "pill" + (GOOD.has(s) ? " pool" : BAD.has(s) ? " warn" : "") }, s),
    help = STAGE_HELP[s] || PILL_HELP[s];
  return help ? tipOn(node, [{ b: s, t: help }]) : node;
};
// an asset's `ready` only says its directory is there and no job writes it: the page says "cached"
export const assetPill = s => statePill(s === "ready" ? "cached" : s);
// ---- times: the ledger stores epoch seconds; keep ISO strings working too
export function toEpoch(t) {
  if (t == null || t === "") return null;
  if (typeof t === "number") return t;
  const ms = Date.parse(t);
  return Number.isNaN(ms) ? null : ms / 1000;
}
export function ago(t) {
  const s = toEpoch(t);
  if (s == null) return "";
  const d = Date.now() / 1000 - s,
    a = Math.abs(d);
  if (a < 10) return "just now";
  const [v, u] = a < 60 ? [a, "s"] : a < 3600 ? [a / 60, "m"] : a < 86400 ? [a / 3600, "h"] : [a / 86400, "d"];
  return d >= 0 ? `${Math.round(v)}${u} ago` : `in ${Math.round(v)}${u}`;
}
export const when = t => {
  const s = toEpoch(t);
  return s == null ? "" : el("span", { title: new Date(s * 1000).toISOString() }, ago(t));
};
// ---- tooltip (the explorer's)
const tip = $("tip");
function showTip(evt, lines) {
  tip.replaceChildren();
  for (const l of lines) {
    const d = el("div");
    if (l.k) d.append(Object.assign(el("span", { class: "k" }), { style: `background:${l.k}` }));
    if (l.b) d.append(el("b", {}, l.b), " ");
    if (l.t) d.append(l.t);
    if (l.img) d.append(el("img", { src: l.img, alt: "" }));
    if (l.full) d.append(el("img", { class: "full", src: l.full, alt: "" }));
    tip.append(d);
  }
  tip.hidden = false;
  moveTip(evt);
}
function moveTip(evt) {
  const x = Math.min(evt.clientX + 14, window.innerWidth - tip.offsetWidth - 8);
  tip.style.left = x + "px";
  tip.style.top = Math.max(8, Math.min(evt.clientY + 14, window.innerHeight - tip.offsetHeight - 8)) + "px";
}
export function hideTip() {
  tip.hidden = true;
}
export function hoverable(node, lines) {
  node.addEventListener("pointerenter", e => showTip(e, lines));
  node.addEventListener("pointermove", moveTip);
  node.addEventListener("pointerleave", hideTip);
  node.setAttribute("tabindex", "0");
  node.addEventListener("focus", () => {
    const b = node.getBoundingClientRect();
    showTip({ clientX: b.left + b.width / 2, clientY: b.top }, lines);
  });
  node.addEventListener("blur", hideTip);
}
// ---- small helpers for the plain tables, tiles and cards
export const tile = (label, value, sub, kind) =>
  el(
    "div",
    { class: "tile" + (kind ? " " + kind : "") },
    el("div", { class: "label" }, label),
    el("div", { class: "value" }, value),
    sub ? el("div", { class: "sub" }, sub) : null,
  );
export const card = (title, sub, ...kids) =>
  el("div", { class: "card" }, el("h2", {}, title), sub ? el("div", { class: "sub" }, sub) : null, ...kids);
export const kv = pairs =>
  el(
    "dl",
    { class: "kv" },
    ...pairs.filter(p => p).flatMap(([k, v]) => [el("dt", {}, k), el("dd", {}, v == null || v === "" ? "—" : v)]),
  );
export const code = (v, n) => (v ? el("code", { title: String(v) }, n ? short(v, n) : String(v)) : null);
export const bufferLink = id => el("a", { href: "#buffers/" + encodeURIComponent(id) }, id);
// a prepared dataset's first crop overlay, served by buffer-api from the dataset folder (`previews` lists the crops that have one); hover enlarges, click opens
export const cropPng = (d, id, name) => `/crops/${encodeURIComponent(d.folder)}/${encodeURIComponent(id)}/${name}.png`;
export const thumb = d => {
  if (!d?.folder || !d.previews?.length) return "";
  const src = cropPng(d, d.previews[0], "overlay");
  return tipOn(
    el(
      "a",
      { href: src, target: "_blank", rel: "noopener" },
      el("img", { class: "thumb", src, loading: "lazy", alt: "crop overlay" }),
    ),
    [
      { t: `${plural(d.crops || d.previews.length, "crop")} saved · overlay of ${d.previews[0]} · click to open it` },
      { img: src },
    ],
  );
};
// a browse link under one of the roots `/meta` names (each segment encoded), or nothing when that root has no URL
export const folderLink = (root, folder, text, title) =>
  root && folder
    ? link(
        `${root.replace(/\/$/, "")}/${String(folder).split("/").map(encodeURIComponent).join("/")}`,
        text || "files",
        title,
      )
    : "";
// how every table names a dataset: its id links to its page; its GitHub issue (a read-only log) is a small mark
export const datasetLink = record =>
  record ? el("a", { href: "#datasets/" + encodeURIComponent(record) }, record) : "";
export function ghIcon(repo, n, numbered) {
  // in a table the mark alone; on a subject's own page the mark and the number
  if (!repo || !n) return "";
  return el(
    "a",
    {
      class: "gh",
      href: `https://github.com/${repo}/issues/${n}`,
      target: "_blank",
      rel: "noopener",
      title: `GitHub issue #${n}: a read-only copy of the timeline`,
      "aria-label": `GitHub issue ${n}`,
    },
    ghMark(),
    numbered ? `#${n}` : null,
  );
}
export const bufferCount = ids => (ids?.length ? num(ids.length) : ""); // a table says how many buffers; the dataset's page lists them
export const bufferLinks = ids => {
  const span = el("span");
  (ids || []).forEach((b, i) => span.append(i ? " " : "", bufferLink(b)));
  return span;
};
// ---- a lifecycle as boxes and arrows: a kind's stages in order (`lines` of GET /meta), the current one filled.
// It shows where on the line the object is, not the path its round took: a round can skip a step or return to one.
// A stage off the line (failed, blocked, needs-human, cancelled) hangs under the step the round stopped at, which is the newest
// stage of `history` (newest first) that is on the line; with none, it stands apart at the end.
// The line is named for what it is: the workflow that prepares a dataset, builds a buffer, or carries a request.
const FLOW_LABELS = {
  dataset: "Preparation workflow:",
  buffer: "Buffer workflow:",
  prep: "Preparation request workflow:",
};
export const flowRow = (kind, flow) =>
  flow && FLOW_LABELS[kind]
    ? el("div", { class: "flowrow" }, el("span", { class: "flowlabel" }, FLOW_LABELS[kind]), flow)
    : flow;
export function lifecycle(line, stage, history, busy) {
  // `busy`: a round is in progress at the current step
  if (!line || !stage) return null;
  const off = !line.includes(stage),
    at = line.indexOf(off ? (history || []).find(s => line.includes(s)) : stage);
  const hung = el("span", { class: "step off" + (BAD.has(stage) ? " stuck" : ""), "aria-current": "step" }, stage);
  const flow = el("ol", { class: "flow", "aria-label": "lifecycle" });
  const working = i => !!busy && !off && i === at;
  line.forEach((name, i) =>
    flow.append(
      el(
        "li",
        {
          class: i < at || (off && i === at) ? "past" : i === at ? (working(i) ? "now busy" : "now") : null,
          "aria-current": i === at && !off ? "step" : null,
          title: working(i) ? "a round is in progress" : null,
        },
        el("span", { class: "step" }, name),
        off && i === at ? hung : null,
      ),
    ),
  );
  if (off && at < 0) flow.append(el("li", { class: "apart" }, hung));
  return flow;
}

// a verdict's pill, by its word
export const VERDICT_KIND = { approve: "pool", reject: "warn", escalate: "" };

// ---- addresses: a subject's own page by kind, and the view and tab its address belongs to
export const SUBJECT_VIEWS = { datasets: "dataset", specs: "buffer", requests: "prep" }; // #datasets/<record>, #specs/<name>, #requests/<key>
export const SUBJECT_TAB = { datasets: "datasets", specs: "buffers", requests: "preparation" };
export const SUBJECT_HASH = {
  dataset: key => "#datasets/" + encodeURIComponent(key),
  buffer: key => "#specs/" + encodeURIComponent(key),
  prep: key => "#requests/" + encodeURIComponent(key),
};
