// Overview, the landing page: the whole system in sections that fold (spec overview §5).
import { $, ago, el, KIND_WORDS, kv, num, stageHelp, stagePill, SUBJECT_HASH, when } from "./dom.js";
import { downLines, load, newKey } from "./api.js";
import { pagedTable, pageOf, TABLE_STATE } from "./table.js";
import { actionBar, BUTTON_HELP, onScreen, sendAction, undelivered } from "./actions.js";
import { caseDetail, caseList, caseRows } from "./operations.js";
import { keepTyping } from "./subjects.js";

export const SECTIONS = ["system", "counts", "needs", "repair", "working", "repairs", "shelved"];
const TITLES = {
  system: "System",
  counts: "Counts",
  needs: "Needs human",
  repair: "In repair",
  working: "Working",
  repairs: "Repairs in the last week",
  shelved: "Shelved",
};
const SUBS = {
  counts: "Every dataset, buffer and request by who moves it next. A count opens its list.",
  needs:
    "Nothing automatic will move these: each waits for a person, with what it will take. Open one for its timeline.",
  working: "Rounds in progress.",
  repairs: "What the repair agent ended: repaired, dropped because somebody acted, or given up on.",
  shelved: "What a person set aside, with the reason. Nothing starts it until it is unshelved.",
};
// ---- a section: a fold that stays as the viewer left it
const foldKey = id => "mia-fold-" + id;
export function fold(id, title, count, sub, ...kids) {
  // open unless the viewer closed it; `count(n)` sets the number in its heading
  let open = true;
  try {
    open = localStorage.getItem(foldKey(id)) !== "0";
  } catch {
    /* not remembered */
  }
  const tally = el("span", { class: "count" }, count == null ? "" : num(count));
  const node = el(
    "details",
    { class: "card fold", "data-fold": id, open },
    el("summary", {}, el("h2", {}, title), tally),
    sub ? el("div", { class: "sub" }, sub) : null,
    ...kids,
  );
  node.addEventListener("toggle", () => {
    try {
      localStorage.setItem(foldKey(id), node.open ? "1" : "0");
    } catch {
      /* not remembered */
    }
  });
  node.count = n => tally.replaceChildren(n == null ? "" : num(n));
  return node;
}
// ---- the repair agent's cases, by what a person does with them
const IN_WORK = ["open", "working", "waiting"];
const WEEK_S = 7 * 86400;
export function overviewCases(cases, repairing, acting, now) {
  // `system`: left-behind things the agent could not remove, which are the installation's problem. `repair`: the
  // cases in work; while the agent acts only those of a subject that is in repair (`repairing`) and of a
  // left-behind thing, else all of them, since none is carried out. `ended`: what ended within a week.
  const ofSubject = c => c.kind in SUBJECT_HASH,
    ids = new Set(repairing.map(subject => subject.repair?.id));
  const system = cases.filter(c => c.state === "gave-up" && !ofSubject(c));
  return {
    system,
    repair: cases.filter(c => IN_WORK.includes(c.state) && (!acting || !ofSubject(c) || ids.has(c.id))),
    ended: cases.filter(c => !IN_WORK.includes(c.state) && !system.includes(c) && c.updated >= now - WEEK_S),
  };
}
// why a subject needs a human: what it takes, when the repair agent gave up on it, else its stage's sentence
export const whyOf = subject =>
  subject.repair?.state === "gave-up" && subject.repair.needs ? subject.repair.needs : stageHelp(subject.stage);

// ---- the view
const NEEDS_KINDS = [
  ["", "All"],
  ["dataset", "Datasets"],
  ["buffer", "Buffers"],
  ["prep", "Requests"],
]; // the "what" a person can narrow Needs human to
const KIND_ROWS = { dataset: "Datasets", buffer: "Buffers", prep: "Requests" };
const KIND_LIST = { dataset: "#datasets", buffer: "#buffers", prep: "#preparation" };
const STATUS_ORDER = ["needs human", "in repair", "working", "shelved", "done", "not started"];
const SECTION_OF = { "needs human": "needs", "in repair": "repair", "working": "working", "shelved": "shelved" };
const plain = text => String(text || "").replaceAll("`", "");
let needsKind = ""; // the kind Needs human is narrowed to; kept while the page redraws, set by the address
let parts = null; // what is on screen: the folds, the bodies the cases fill, and the cases by id
const again = () => viewOverview("", true);

function subjectCols() {
  return [
    { key: "kind", label: "what", cell: x => KIND_WORDS[x.kind] },
    { key: "key", label: "name", cell: x => el("a", { href: SUBJECT_HASH[x.kind](x.key) }, x.name || x.key) },
    { key: "status", label: "stage", cell: x => stagePill(x.stage) },
    { key: "updated", label: "since", cell: x => when(x.updated) },
  ];
}
function whyCell(subject) {
  // what the repair agent wrote is text; its case opens under it
  if (subject.repair?.state !== "gave-up") return el("span", { class: "small" }, whyOf(subject));
  const box = el("details", { class: "why" }, el("summary", {}, "Repair agent: " + plain(whyOf(subject))));
  box.addEventListener("toggle", () => {
    const found = parts?.cases.get(subject.repair.id);
    if (box.open && found && !box.querySelector(".casebody")) box.append(caseDetail(caseRows([found])[0]));
  });
  return box;
}
function dropNotices(x) {
  // a buffer's datasets that wait for files only a person can copy in
  return (x.drops || []).map(record =>
    el(
      "div",
      { class: "notice" },
      el("b", {}, record),
      " waits for its files in its incoming/ folder. ",
      el(
        "button",
        {
          type: "button",
          class: "btn",
          title: BUTTON_HELP.dropped,
          disabled: !!x.asked,
          onclick: e => {
            e.target.disabled = true;
            sendAction(x, { action: "dropped", record, key: newKey() }, again);
          },
        },
        "Files are in place",
      ),
    ),
  );
}
function build(root) {
  const body = () => el("div", {});
  const bodies = { system: body(), counts: body(), repair: body(), repairs: body() };
  const failed = el("div");
  const show = kind => {
    // the address keeps the choice across a reload; it is set without a new route
    needsKind = kind;
    if (TABLE_STATE.has("needs")) TABLE_STATE.get("needs").offset = 0;
    history.replaceState(null, "", "#overview" + (kind ? "/" + kind : ""));
    for (const button of kinds.children) button.setAttribute("aria-pressed", String(button.dataset.kind === kind));
    tables.needs.reload();
  };
  const kinds = el(
    "div",
    { class: "toggles", role: "group", "aria-label": "Show" },
    ...NEEDS_KINDS.map(([kind, word]) =>
      el(
        "button",
        {
          type: "button",
          class: "btn",
          "aria-pressed": String(kind === needsKind),
          "data-kind": kind,
          "data-word": word,
          onclick: () => show(kind),
        },
        word,
      ),
    ),
  );
  const tables = {
    needs: pagedTable("needs", {
      source: pageOf("/subjects", "subjects", () => ({
        status: "needs human",
        ...(needsKind ? { kind: needsKind } : {}),
      })),
      sort: "updated",
      dir: "desc",
      empty: "Nothing needs a human.",
      onPage: page => {
        const shares = page.out.kinds || {};
        for (const button of kinds.children) {
          // each button says how many wait under it
          const n = button.dataset.kind
            ? shares[button.dataset.kind]
            : Object.values(shares).reduce((sum, share) => sum + share, 0);
          button.replaceChildren(n == null ? button.dataset.word : `${button.dataset.word} ${num(n)}`);
        }
        failed.replaceChildren(
          ...(page.out.failed || [])
            .filter(a => a.subject)
            .map(a =>
              el(
                "div",
                {},
                el("a", { href: SUBJECT_HASH[a.subject.kind](a.subject.key) }, a.subject.name || a.subject.key),
                undelivered(a.subject, [a], again),
              ),
            ),
        );
      },
      cols: [
        ...subjectCols(),
        { label: "why", wrap: true, cell: whyCell },
        {
          label: "answer",
          wrap: true,
          cell: x => el("div", {}, actionBar(x, again, x.kind === "buffer" ? ["go", "revoke"] : []), ...dropNotices(x)),
        },
      ],
    }),
    working: pagedTable("working", {
      source: pageOf("/subjects", "subjects", () => ({ status: "working" })),
      sort: "updated",
      dir: "desc",
      empty: "Nothing is working.",
      cols: subjectCols(),
    }),
    shelved: pagedTable("shelved", {
      source: pageOf("/subjects", "subjects", () => ({ status: "shelved" })),
      sort: "updated",
      dir: "desc",
      empty: "Nothing is shelved.",
      cols: [
        ...subjectCols(),
        { label: "reason", wrap: true, cell: x => x.shelved?.reason || "" },
        { label: "answer", wrap: true, cell: x => actionBar(x, again) },
      ],
    }),
  };
  const folds = {
    system: fold("system", TITLES.system, null, "", bodies.system),
    counts: fold("counts", TITLES.counts, null, SUBS.counts, bodies.counts),
    needs: fold("needs", TITLES.needs, null, SUBS.needs, kinds, failed, tables.needs),
    repair: fold("repair", TITLES.repair, null, "", bodies.repair),
    working: fold("working", TITLES.working, null, SUBS.working, tables.working),
    repairs: fold("repairs", TITLES.repairs, null, SUBS.repairs, bodies.repairs),
    shelved: fold("shelved", TITLES.shelved, null, SUBS.shelved, tables.shelved),
  };
  root.replaceChildren(...SECTIONS.map(id => folds[id]));
  // a count opens the section it counts, narrowed to its kind where the section can be
  const open = (section, kind) => {
    folds[section].open = true;
    if (section === "needs") show(kind);
    folds[section].scrollIntoView({ block: "start" });
  };
  return { folds, bodies, open, cases: new Map() };
}
function countsTable(counts, open) {
  const cell = (kind, status) => {
    const n = counts?.[kind]?.[status] || 0;
    if (!n) return el("td", { class: "num" }, "—");
    const section = SECTION_OF[status];
    return el(
      "td",
      { class: "num" },
      section
        ? el(
            "a",
            {
              href: "#overview",
              onclick: e => {
                e.preventDefault(); // the address stays: the section opens on this page
                open(section, kind);
              },
            },
            num(n),
          )
        : el("a", { href: KIND_LIST[kind] }, num(n)),
    );
  };
  return el(
    "table",
    { class: "plain" },
    el("thead", {}, el("tr", {}, el("th", {}, ""), ...STATUS_ORDER.map(status => el("th", { class: "num" }, status)))),
    el(
      "tbody",
      {},
      ...Object.keys(KIND_ROWS).map(kind =>
        el("tr", {}, el("td", {}, KIND_ROWS[kind]), ...STATUS_ORDER.map(status => cell(kind, status))),
      ),
    ),
  );
}
// a sweep's report is overdue once three of its periods passed with none after it (`REPORT_STALE` in buffer-api)
export const overdue = (report, now) => !!report && now - report.at > 3 * (report.every_s || 600);
export function systemLines(report, health, now) {
  // what is wrong with the installation: what the health check says is away, what the last sweep found, and a
  // repair agent that has stopped reporting, in which case nothing is being repaired
  const findings = report?.findings || [];
  return [
    ...(health ? downLines(health, findings) : findings.map(finding => plain(finding.text))),
    ...(overdue(report, now)
      ? [`The repair agent last reported ${ago(report.at)}: nothing is being repaired until it reports again.`]
      : []),
  ];
}
function systemBody(report, left, lines) {
  // act mode is the normal one and goes unsaid
  const modes = { off: " The repair agent is off.", report: " Report mode: cases are listed and nothing is done." };
  return [
    el(
      "div",
      { class: "sub" },
      report
        ? `The repair agent last looked ${ago(report.at)}.${modes[report.mode] || ""}${lines.length ? " The subjects behind a problem listed here are left alone until it clears." : ""}`
        : "The repair agent has not reported a sweep yet.",
    ),
    lines.length
      ? el("div", {}, ...lines.map(line => el("div", { class: "notice" }, line)))
      : el("div", { class: "empty" }, "All services operating, no problems found."),
    report?.errors?.length
      ? kv([["errors of the last sweep", el("div", {}, ...report.errors.map(x => el("div", { class: "mono" }, x)))]])
      : null,
    left.length ? el("h3", {}, "Left behind, and the repair agent could not remove it") : null,
    left.length ? caseList(left, "") : null,
  ].filter(Boolean);
}
const CASES_MS = 60000; // how long the cases are kept while only the stream asks: each load reads their job records
async function fill(shown, stream) {
  // what the tables do not load themselves: the last sweep, the cases, and the counts of every status
  const kept = stream && shown.listed && Date.now() - shown.listedAt < CASES_MS;
  const [report, listed, repairing] = await Promise.all([
    load("/cleaner", true),
    kept ? shown.listed : load("/cases?days=7"),
    load(`/subjects?${new URLSearchParams({ status: "in repair" })}`),
  ]);
  if (parts !== shown) return; // another draw took over while this one loaded
  if (!kept) [shown.listed, shown.listedAt] = [listed, Date.now()];
  const now = Date.now() / 1000,
    cases = listed?.cases || [],
    acting = report?.mode === "act" && !overdue(report, now),
    lines = systemLines(report, onScreen.health, now),
    found = overviewCases(cases, repairing?.subjects || [], acting, now),
    counts = repairing?.counts || {},
    total = status => Object.values(counts).reduce((sum, kind) => sum + (kind[status] || 0), 0);
  shown.cases = new Map(cases.map(c => [c.id, c]));
  // a case somebody has open is not redrawn under them
  const set = (id, ...kids) => {
    if (!shown.bodies[id].querySelector("details.case[open]")) shown.bodies[id].replaceChildren(...kids);
  };
  set("system", ...systemBody(report, found.system, lines));
  shown.bodies.counts.replaceChildren(countsTable(counts, shown.open));
  set(
    "repair",
    el(
      "div",
      { class: "sub" },
      acting
        ? "It stopped and the repair agent is working on it. Open one for what was found and each step."
        : "The repair agent carries nothing out: these are what it would repair.",
    ),
    caseList(found.repair, "Nothing is in repair."),
  );
  set("repairs", caseList(found.ended, "Nothing yet."));
  const problems = lines.length + found.system.length;
  shown.folds.system.count(problems || null);
  shown.folds.needs.count(total("needs human"));
  shown.folds.repair.count(found.repair.length);
  shown.folds.working.count(total("working"));
  shown.folds.repairs.count(found.ended.length);
  shown.folds.shelved.count(total("shelved"));
}
export async function viewOverview(arg, refresh) {
  // `arg`: the kind Needs human is narrowed to (`#overview/dataset`). A refresh keeps the sections and what is
  // open in them: a table loads its page again unless a fold in it is open (a case under "why", a menu of
  // actions), and the rest is filled anew.
  const root = $("overviewBody");
  if (refresh === "stream" && keepTyping(root, again)) return;
  delete root.dataset.dirty;
  if (!refresh || !parts) {
    needsKind = NEEDS_KINDS.some(([kind]) => kind === arg) ? arg : "";
    parts = build(root);
  } else for (const box of root.querySelectorAll(".paged")) if (!box.querySelector("details[open]")) box.reload();
  await fill(parts, refresh === "stream");
}
// The stream tells of every stage write: the page follows it at most once in STREAM_MS, at the end of which it
// draws what changed meanwhile. A draw a person asked for (a route, a button) is never held back.
const STREAM_MS = 10000;
let streamAt = 0,
  streamTimer = 0;
onScreen.streams.overview = () => {
  const wait = streamAt + STREAM_MS - Date.now();
  clearTimeout(streamTimer);
  if (wait > 0) {
    streamTimer = setTimeout(onScreen.streams.overview, wait);
    return;
  }
  streamAt = Date.now();
  if (onScreen.view === "overview") viewOverview("", "stream");
};
