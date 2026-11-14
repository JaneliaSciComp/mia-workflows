// The shell: the Docs view, health, the stream, hash routing, the top bar, the theme and boot.
// The page holds no token. It reads with GETs (and with POST /plan and POST /hosts/missing, which write nothing)
// and asks for things through the action routes, which take none. All hosts and repos come from GET /meta.
import { $, el, hideTip, plural, short, SUBJECT_TAB, SUBJECT_VIEWS, toEpoch, when } from "./dom.js";
import { HttpError, load, S, send } from "./api.js";
import { onScreen } from "./actions.js";
import { bindControls, viewDesigner } from "./designer.js";
import { viewBuffers } from "./buffers.js";
import { viewDatasetList } from "./datasets.js";
import { viewCache, viewDecisions, viewPreparation } from "./operations.js";
import { viewFind, viewNeeds, viewSubject } from "./subjects.js";

// ---- the Docs view
// Static: show one of the two documents and mark its sub-tab. A document is a file of the page (guide.html,
// architecture.html), read when it is first shown. It is this package's own HTML, so it is parsed into nodes;
// nothing a job or a record wrote is ever shown this way.
const DOCS_SHOWN = new Set();
async function viewDocs(arg) {
  const doc = arg === "architecture" ? "architecture" : "guide";
  document.querySelectorAll("[data-doc]").forEach(a => { a.hidden = a.dataset.doc !== doc; });
  document.querySelectorAll("[data-doc-tab]").forEach(a => { if (a.dataset.docTab === doc) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current"); });
  if (DOCS_SHOWN.has(doc)) return;
  DOCS_SHOWN.add(doc);
  const article = document.querySelector(`[data-doc="${doc}"]`);
  article.replaceChildren(el("div", { class: "empty" }, "loading…"));
  try {
    const [res, text] = await send(`/ui/${doc}.html`);
    if (!res.ok) throw new HttpError(res.status, res.statusText);
    article.replaceChildren(...new DOMParser().parseFromString(text, "text/html").body.childNodes);
  } catch (e) {  // the next visit asks again
    DOCS_SHOWN.delete(doc);
    article.replaceChildren(el("div", { class: "empty" }, `The document did not load (${e.status || "network error"}): ${e.message}`));
  }
}

// ---- health and the stream
const HEALTH_MS = 60000;  // how often the page asks whether Temporal and every worker are still there
export function downLines(health) {  // what is away, in words; a service's task queue carries its name
  const away = Object.keys(health.queues || {}).filter(queue => !health.queues[queue]);
  return [
    !health.temporal && "Temporal does not answer: nothing can be started or cancelled from this page right now.",
    away.length && `No worker is running for ${away.join(", ")}: nothing that waits on ${away.length > 1 ? "them" : "it"} is moving. On the host: pixi run mia restart ${away.join(" ")}`,
  ].filter(Boolean);
}
async function checkHealth() {  // so that "working" is true: these lines stay until a later check finds everything back
  const health = await load("/health");
  if (health) $("down").replaceChildren(...downLines(health).map(line => el("div", {}, el("span", {}, line))));
}
function followStream() {  // told what changed, the page refetches it; a lost stream reconnects by itself
  const stream = new EventSource("/stream");
  let timer = 0;
  const refresh = onScreen.refresh;
  stream.onopen = () => { checkHealth(); refresh(); };
  stream.onmessage = e => {
    let change; try { change = JSON.parse(e.data); } catch { return; }
    if (onScreen.subject) {
      if (change.kind !== onScreen.subject.kind) return;
      if (onScreen.subject.number != null && change.number !== onScreen.subject.number) return;
    } else if (onScreen.view !== "needs") return;
    clearTimeout(timer); timer = setTimeout(refresh, 150);
  };
}

// ---- hash routing, the top bar, the theme and boot
const VIEWS = { needs: viewNeeds, datasets: viewDatasetList, buffers: viewBuffers, cache: viewCache, preparation: viewPreparation, decisions: viewDecisions, docs: viewDocs };
const REFRESH_MS = 30000;  // the buffer views poll; the others reload when their tab is clicked
let refreshTimer = 0;
function route() {
  const [path, filters] = location.hash.slice(1).split("?");  // #datasets?state=in-progress: a list with its filters set
  const [name, ...rest] = path.split("/");
  let arg = rest.join("/");
  try { arg = decodeURIComponent(arg); } catch { /* a malformed escape: use the raw text */ }
  const designer = name === "buffers" && arg === "new";
  const kind = arg ? SUBJECT_VIEWS[name] : null;  // #datasets/<record>, #specs/<name>, #requests/<key>: its own page
  const view = designer ? "designer" : kind || (name === "find" && arg) ? "subject" : VIEWS[name] ? name : "needs";
  const tab = designer ? "buffers" : kind ? SUBJECT_TAB[name] : view;
  document.querySelectorAll("[data-view]").forEach(s => { s.hidden = s.dataset.view !== view; });
  document.querySelectorAll("[data-tab]").forEach(a => { if (a.dataset.tab === tab) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current"); });
  hideTip();
  clearInterval(refreshTimer);
  onScreen.subject = null; onScreen.view = view;
  if (kind) return viewSubject(kind, arg);
  if (view === "subject") return viewFind(arg);
  if (designer) return viewDesigner();
  if (view === "buffers") refreshTimer = setInterval(() => { hideTip(); VIEWS.buffers(arg, true); }, REFRESH_MS);
  if (view === "datasets") return viewDatasetList(arg, new URLSearchParams(filters || ""));
  VIEWS[view](arg);
}
function renderTopbar() {
  const c = S.catalogs[0];
  $("catref").replaceChildren(c ? el("span", { title: c.ref }, `catalog ${short(c.ref, 8)} · ${plural(c.count, "dataset")} · synced `, when(c.synced_at)) : "no catalog synced");
}
function setupFilterBox() {  // the fold is a per-viewer convenience, remembered in this browser
  const box = $("filterBox");
  try { if (localStorage.getItem("mia-filters") === "closed") box.open = false; } catch { /* storage blocked: open */ }
  box.addEventListener("toggle", () => { try { localStorage.setItem("mia-filters", box.open ? "open" : "closed"); } catch { /* not remembered */ } });
}
function setupTheme() {
  const root = document.documentElement;
  try { const saved = localStorage.getItem("mia-theme"); if (saved) root.dataset.theme = saved; } catch { /* storage blocked: follow the system */ }
  new ResizeObserver(() => root.style.setProperty("--topbar-h", document.querySelector(".topbar").offsetHeight + "px")).observe(document.querySelector(".topbar"));
  $("theme").addEventListener("click", () => {
    const dark = root.dataset.theme ? root.dataset.theme === "dark" : matchMedia("(prefers-color-scheme: dark)").matches;
    root.dataset.theme = dark ? "light" : "dark";
    try { localStorage.setItem("mia-theme", root.dataset.theme); } catch { /* not remembered */ }
  });
}
export async function boot() {
  setupTheme(); setupFilterBox(); bindControls();
  const [meta, catalogs] = await Promise.all([load("/meta"), load("/catalogs")]);
  S.meta = meta || {};
  S.catalogs = (catalogs?.catalogs || []).slice().sort((a, b) => (toEpoch(b.synced_at) || 0) - (toEpoch(a.synced_at) || 0));
  S.ref = S.catalogs[0]?.ref || null;
  renderTopbar();
  window.addEventListener("hashchange", route);
  // a click on the current tab changes no hash: reload the view by hand
  document.querySelectorAll("[data-tab]").forEach(a => a.addEventListener("click", () => { if (a.getAttribute("href") === location.hash) route(); }));
  for (const id of ["subjectBody", "needsBody"]) $(id).addEventListener("input", () => { $(id).dataset.dirty = "1"; });
  $("lookup").addEventListener("keydown", e => { const text = e.target.value.trim(); if (e.key === "Enter" && text) { location.hash = "#find/" + encodeURIComponent(text); e.target.value = ""; } });
  route();
  followStream();  // its first open also reads /health
  setInterval(checkHealth, HEALTH_MS);
}
