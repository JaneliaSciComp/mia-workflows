// The shell: the Docs view, health, the stream, hash routing, the top bar, the theme and boot.
// The page holds no token. It reads with GETs (and with POST /plan and POST /hosts/missing, which write nothing)
// and asks for things through the action routes, which take none. All hosts and repos come from GET /meta.
import { $, el, hideTip, SUBJECT_HASH, SUBJECT_TAB, SUBJECT_VIEWS, toEpoch } from "./dom.js";
import { downLines, HttpError, load, S, send } from "./api.js";
import { onScreen } from "./actions.js";
import { bindControls } from "./designer.js";
import { viewBuffers } from "./buffers.js";
import { viewDatasetList } from "./datasets.js";
import { viewCache, viewDecisions, viewPreparation } from "./operations.js";
import { viewJob } from "./job.js";
import { viewFind, viewSubject } from "./subjects.js";
import { viewOverview } from "./overview.js";
import { viewFlow } from "./flow.js";

// ---- the Docs view
// Static: show one of the two documents and mark its sub-tab. A document is a file of the page (guide.html,
// architecture.html), read when it is first shown. It is this package's own HTML, so it is parsed into nodes;
// nothing a job or a record wrote is ever shown this way.
const docsLoads = new Map();
async function viewDocs(arg) {
  const [name, chapter] = arg.split("/");
  const doc = name === "architecture" ? "architecture" : "guide";
  const routeHash = location.hash;
  document.querySelectorAll("[data-doc]").forEach(a => {
    a.hidden = a.dataset.doc !== doc;
  });
  document.querySelectorAll("[data-doc-tab]").forEach(a => {
    if (a.dataset.docTab === doc) a.setAttribute("aria-current", "page");
    else a.removeAttribute("aria-current");
  });
  const article = document.querySelector(`[data-doc="${doc}"]`);
  if (!docsLoads.has(doc)) docsLoads.set(doc, loadDoc(doc, article));
  await docsLoads.get(doc);
  if (location.hash === routeHash) {
    const target = chapter && document.getElementById(`chapter-${doc}-${chapter}`);
    if (target) target.scrollIntoView();
    else window.scrollTo(0, 0);
  }
}
async function loadDoc(doc, article) {
  article.replaceChildren(el("div", { class: "empty" }, "loading…"));
  try {
    const [res, text] = await send(`/ui/${doc}.html`);
    if (!res.ok) throw new HttpError(res.status, res.statusText);
    article.replaceChildren(...new DOMParser().parseFromString(text, "text/html").body.childNodes);
  } catch (e) {
    // the next visit asks again
    docsLoads.delete(doc);
    article.replaceChildren(
      el("div", { class: "empty" }, `The document did not load (${e.status || "network error"}): ${e.message}`),
    );
  }
}

// ---- health and the stream
const HEALTH_MS = 60000; // how often the page asks whether Temporal and every worker are still there
async function checkHealth() {
  // so that "working" is true: these lines stay until a later check finds everything back
  const [health, sweep] = await Promise.all([load("/health"), load("/cleaner", true)]);
  if (!health) return;
  $("down").replaceChildren(...downLines(health, sweep?.findings).map(line => el("div", {}, el("span", {}, line))));
  // a view that follows the stream also says what is away: a worker that is down writes nothing to the stream
  onScreen.health = health;
  onScreen.streams[onScreen.view]?.();
}
function followStream() {
  // told what changed, the page refetches it; a lost stream reconnects by itself
  const stream = new EventSource("/stream");
  let timer = 0;
  const refresh = onScreen.refresh;
  stream.onopen = () => {
    checkHealth();
    refresh();
  };
  stream.onmessage = e => {
    let change;
    try {
      change = JSON.parse(e.data);
    } catch {
      return;
    }
    if (onScreen.subject) {
      if (change.kind !== onScreen.subject.kind) return;
      if (onScreen.subject.number != null && change.number !== onScreen.subject.number) return;
    } else if (!onScreen.streams[onScreen.view]) return;
    clearTimeout(timer);
    timer = setTimeout(refresh, 150);
  };
}

// ---- hash routing, the top bar, the theme and boot
const VIEWS = {
  overview: viewOverview,
  datasets: viewDatasetList,
  buffers: viewBuffers,
  cache: viewCache,
  preparation: viewPreparation,
  decisions: viewDecisions,
  docs: viewDocs,
  job: viewJob, // #job/<address>: a job's record; the address is everything after the first slash
};
const REFRESH_MS = 30000; // the Buffers list, a version's page and a buffer's page poll
let refreshTimer = 0;
function route() {
  const [path, filters] = location.hash.slice(1).split("?"); // #datasets?state=in-progress: a list with its filters set
  const [name, ...rest] = path.split("/");
  const decoded = text => {
    try {
      return decodeURIComponent(text);
    } catch {
      return text; // a malformed escape: use the raw text
    }
  };
  const arg = decoded(rest.join("/"));
  // #specs/<key> is another address of a buffer's page
  if (name === "specs" && rest.length) return location.replace(SUBJECT_HASH.buffer(arg));
  // a buffer's page: #buffer/<key>[/<step>], and #buffers/new[/<step>] for one that is not saved yet
  const fresh = name === "buffers" && rest[0] === "new",
    flow = fresh || (name === "buffer" && !!rest[0]);
  const kind = arg ? SUBJECT_VIEWS[name] : null; // #datasets/<record>, #requests/<key>: its own page
  const view = flow ? "buffer" : kind || (name === "find" && arg) ? "subject" : VIEWS[name] ? name : "overview";
  const tab = flow ? "buffers" : kind ? SUBJECT_TAB[name] : view === "preparation" ? "datasets" : view;
  document.querySelectorAll("[data-view]").forEach(s => {
    s.hidden = s.dataset.view !== view;
  });
  document.querySelectorAll("[data-tab]").forEach(a => {
    if (a.dataset.tab === tab) a.setAttribute("aria-current", "page");
    else a.removeAttribute("aria-current");
  });
  hideTip();
  clearInterval(refreshTimer);
  onScreen.subject = null;
  onScreen.redraw = null;
  onScreen.view = view;
  if (flow) {
    refreshTimer = setInterval(() => onScreen.redraw?.(), REFRESH_MS);
    return viewFlow(fresh ? null : decoded(rest[0]), rest[1]);
  }
  if (kind) return viewSubject(kind, arg);
  if (view === "subject") return viewFind(arg);
  if (view === "buffers")
    refreshTimer = setInterval(() => {
      hideTip();
      VIEWS.buffers(arg, true);
    }, REFRESH_MS);
  if (view === "datasets") return viewDatasetList(arg, new URLSearchParams(filters || ""));
  VIEWS[view](arg);
}
function setupTheme() {
  const root = document.documentElement;
  try {
    const saved = localStorage.getItem("mia-theme");
    if (saved) root.dataset.theme = saved;
  } catch {
    /* storage blocked: follow the system */
  }
  new ResizeObserver(() =>
    root.style.setProperty("--topbar-h", document.querySelector(".topbar").offsetHeight + "px"),
  ).observe(document.querySelector(".topbar"));
  $("theme").addEventListener("click", () => {
    const dark = root.dataset.theme
      ? root.dataset.theme === "dark"
      : matchMedia("(prefers-color-scheme: dark)").matches;
    root.dataset.theme = dark ? "light" : "dark";
    try {
      localStorage.setItem("mia-theme", root.dataset.theme);
    } catch {
      /* not remembered */
    }
  });
}
export async function boot() {
  setupTheme();
  bindControls();
  const [meta, catalogs] = await Promise.all([load("/meta"), load("/catalogs")]);
  S.meta = meta || {};
  S.catalogs = (catalogs?.catalogs || [])
    .slice()
    .sort((a, b) => (toEpoch(b.synced_at) || 0) - (toEpoch(a.synced_at) || 0));
  S.ref = S.catalogs[0]?.ref || null;
  window.addEventListener("hashchange", route);
  // a click anywhere outside an open menu closes it
  document.addEventListener("click", e => {
    for (const menu of document.querySelectorAll("details.more[open]")) if (!menu.contains(e.target)) menu.open = false;
  });
  // a click on the current tab changes no hash: reload the view by hand
  document.querySelectorAll("[data-tab]").forEach(a =>
    a.addEventListener("click", () => {
      if (a.getAttribute("href") === location.hash) route();
    }),
  );
  for (const id of ["subjectBody", "overviewBody"])
    $(id).addEventListener("input", e => {
      // the Overview reloads its tables in place, so a search typed there holds nothing back; a note does
      if (id === "overviewBody" && e.target.type === "search") return;
      $(id).dataset.dirty = "1";
    });
  $("lookup").addEventListener("keydown", e => {
    const text = e.target.value.trim();
    if (e.key === "Enter" && text) {
      location.hash = "#find/" + encodeURIComponent(text);
      e.target.value = "";
    }
  });
  route();
  followStream(); // its first open also reads /health
  setInterval(checkHealth, HEALTH_MS);
}
