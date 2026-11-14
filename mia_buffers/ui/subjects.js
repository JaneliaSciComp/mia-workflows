// A subject's page (a dataset, a buffer spec or a request: its state, buttons and timeline), the lookup box's
// results, and Needs you.
import {
  $,
  bufferLink,
  card,
  datasetLink,
  el,
  flowRow,
  ghIcon,
  lifecycle,
  link,
  num,
  stagePill,
  statePill,
  SUBJECT_HASH,
  when,
} from "./dom.js";
import { api, banner, load, newKey, S } from "./api.js";
import { pagedTable, pageOf, TABLE_STATE } from "./table.js";
import { cropsOf, markdown, mdInline } from "./markdown.js";
import { actionBar, BUTTON_HELP, onScreen, sendAction, undelivered } from "./actions.js";
import { servableText, stageOf } from "./buffers.js";
import { datasetCards } from "./datasets.js";

// ---- a subject's page
let subjectTurn = 0; // the newest load of a subject's page: one that started earlier never paints over it
// the stream, and the end of an asked action's wait, redraw the subject's page or Needs you, whichever is shown
onScreen.refresh = () => {
  if (onScreen.subject) viewSubject(onScreen.subject.kind, onScreen.subject.key, "stream");
  else if (onScreen.view === "needs") viewNeeds("", "stream");
};
// Every link of a subject stands in the row under its name: its GitHub issue, and for a dataset its workflow in
// Temporal and the crops of its newest round.
function subjectLinks(subject, entries) {
  const repo = subject.kind === "dataset" ? S.meta.datasets_repo : S.meta.buffers_repo,
    out = [];
  if (subject.issue) out.push(ghIcon(repo, subject.issue, true) || `#${subject.issue}`);
  if (subject.kind === "dataset" && S.meta.datasets_workflow_url)
    out.push(
      link(S.meta.datasets_workflow_url + subject.number, "temporal", "this dataset's workflow in the Temporal UI"),
    );
  const round = [...entries].reverse().find(e => e.round)?.round;
  if (subject.kind === "dataset" && round && repo && S.meta.gallery_branch)
    out.push(
      link(
        `https://github.com/${repo}/tree/${S.meta.gallery_branch}/galleries/${subject.key}/${round}`,
        "crops of this round",
        "the crop gallery of this round, on GitHub",
      ),
    );
  return out;
}
async function specControls(subject) {
  // what only a buffer spec has: edit and run, its datasets' files, revoke, its buffers
  const box = el("div"),
    again = () => viewSubject(subject.kind, subject.key, true);
  const mine = (await load(`/buffers?limit=1&spec=${subject.number}`))?.buffers || []; // its newest round's buffer
  const board = mine.length
    ? (await load(`/buffers/${encodeURIComponent(mine[0].id)}/datasets`, true))?.datasets || []
    : [];
  if (subject.actions.includes("dropped")) {
    const waiting = board.filter(d => d.status === "waiting-drop");
    for (const d of waiting)
      box.append(
        el(
          "div",
          { class: "notice" },
          el("b", {}, d.record),
          " waits for its files to be copied into its incoming/ folder. ",
          el(
            "button",
            {
              type: "button",
              class: "btn",
              title: BUTTON_HELP.dropped,
              disabled: !!subject.asked,
              onclick: e => {
                e.target.disabled = true;
                sendAction(subject, { action: "dropped", record: d.record, key: newKey() }, again);
              },
            },
            "Files are in place",
          ),
        ),
      );
  }
  if (subject.actions.includes("revoke")) {
    const records = board.filter(d => d.status === "servable").map(d => d.record);
    const ticks = el(
      "div",
      { class: "ticks" },
      ...records.map(r => el("label", { class: "inline" }, el("input", { type: "checkbox", value: r }), " " + r)),
    );
    box.append(
      card(
        "Revoke datasets",
        "The ticked datasets leave this buffer: their volumes are unregistered and their staged pyramids quarantined.",
        records.length ? ticks : el("div", { class: "empty" }, "No dataset is being served."),
        el(
          "div",
          { class: "actions" },
          el(
            "button",
            {
              type: "button",
              class: "btn danger",
              title: BUTTON_HELP.revokeTicked,
              disabled: !!subject.asked,
              onclick: e => {
                const chosen = [...ticks.querySelectorAll("input:checked")].map(i => i.value);
                if (!chosen.length) return banner("Tick the datasets to revoke first.");
                if (
                  !confirm(
                    "Revoke the ticked datasets? Their volumes leave this buffer and their staged pyramids are quarantined; nothing is deleted.",
                  )
                )
                  return;
                e.target.disabled = true;
                sendAction(subject, { action: "revoke", records: chosen, key: newKey() }, again);
              },
            },
            "Revoke the ticked",
          ),
        ),
      ),
    );
  }
  if (subject.actions.includes("go")) {
    const area = el("textarea", { class: "specbox", spellcheck: "false", "aria-label": "buffer spec YAML" });
    area.value = subject.data?.spec || "";
    box.append(
      card(
        subject.stage === "new" ? "Plan" : "Edit and plan again",
        "The spec as it was last saved. Plan starts a new round with this text, which stops at its plan until you press Run; the buffer it builds gets a new id, and training jobs keep the buffer they connected to.",
        area,
        el(
          "div",
          { class: "actions" },
          el(
            "button",
            {
              type: "button",
              class: "btn",
              title: BUTTON_HELP.plan,
              disabled: !!subject.asked,
              onclick: e => {
                e.target.disabled = true;
                sendAction(subject, { action: "go", spec: area.value, plan_first: true, key: newKey() }, again);
              },
            },
            subject.asked?.action === "go" ? "Plan…" : "Plan",
          ),
        ),
      ),
    );
  }
  box.append(
    card(
      "Buffers of this spec",
      "Newest first; each run builds a new one.",
      pagedTable("spec-buffers:" + subject.key, {
        source: pageOf("/buffers", "buffers", () => ({ spec: subject.number })),
        search: false,
        empty: "No buffer yet.",
        cols: [
          { key: "buffer", label: "buffer", cell: b => bufferLink(b.id) },
          {
            key: "updated",
            label: "state",
            cell: b =>
              b.latest_event
                ? el("span", {}, statePill(stageOf(b.latest_event)), " ", when(b.latest_event.at))
                : "no events",
          },
          { key: "volumes", label: "volumes", num: true, cell: b => num(b.volumes) },
          { label: "servable", cell: b => servableText(b.servable) },
        ],
      }),
    ),
  );
  return box;
}
// A view a person is typing into (an edited spec, ticked records) is not rebuilt under them: it says that
// something changed, and waits for them. Any input in the view sets `root.dataset.dirty` and the start of a redraw
// clears it, so a load that ends finds it set when someone typed while it ran. True when the view is to be kept.
export function keepTyping(root, reload) {
  if (!root.dataset.dirty) return false;
  if (!root.querySelector(".stale"))
    root.prepend(
      el(
        "div",
        { class: "notice stale" },
        "This changed while you were typing; what you typed is kept. ",
        el(
          "button",
          { type: "button", class: "btn", title: BUTTON_HELP.showChange, onclick: reload },
          "Show the change",
        ),
      ),
    );
  return true;
}
// A dataset whose catalog record lacks what a round needs (`gaps`, on its row and on its subject) cannot be started:
// the service offers no Start for it and refuses a go. The service reads the record from the synced catalog each
// time, so a record fixed in the catalog can be started after the next catalog sync.
const gapsNotice = gaps =>
  el(
    "div",
    { class: "notice" },
    el("b", {}, "Cannot be started. "),
    "Its catalog record lacks what a round needs: ",
    ...mdInline(gaps.join("; ")),
    ". Start is offered once a catalog sync brings a record that has it; nothing has to be done here.",
  );
// A shelved subject (spec shelved-subjects): who set it aside, when and why. Its only button is Unshelve.
const shelvedNotice = hold =>
  el(
    "div",
    { class: "notice" },
    el("b", {}, "Shelved"),
    ` by ${hold.by}, `,
    when(hold.at),
    `: ${hold.reason} `,
    el(
      "span",
      { class: "small" },
      "Nothing is asked of it, and no request or buffer plan takes it, until it is unshelved.",
    ),
  );
export async function viewSubject(kind, key, refresh) {
  const root = $("subjectBody"),
    hash = location.hash,
    redraw = () => viewSubject(kind, key, true);
  if (refresh === "stream" && keepTyping(root, redraw)) return;
  delete root.dataset.dirty;
  if (!refresh) root.replaceChildren(el("div", { class: "empty" }, "loading…"));
  onScreen.subject = {
    kind,
    key,
    number:
      onScreen.subject && onScreen.subject.kind === kind && onScreen.subject.key === key
        ? onScreen.subject.number
        : null,
  };
  const turn = ++subjectTurn;
  const [subject, detail] = await Promise.all([
    load(`/subjects/${kind}/${encodeURIComponent(key)}`, true),
    kind === "dataset" ? load(`/catalog/datasets/${encodeURIComponent(key)}`, true) : null,
  ]);
  if (location.hash !== hash || turn !== subjectTurn) return; // the reader moved on, or a later load started, while this loaded
  if (keepTyping(root, redraw)) return; // someone typed while this loaded
  const about = detail ? datasetCards(detail) : [];
  const [links = null, ...cards] = about; // a dataset's links stand right under its name, its cards at the end
  const again = !!detail && detail.row.state !== "unprepared"; // Start, or Start again: the same word before and after the click
  const gaps = (subject || detail?.row)?.gaps || [];
  if (!subject) {
    // nothing was ever asked of it on this page: the first Start creates it
    const fresh = {
      kind,
      key,
      stage: "new",
      status: "not started",
      revision: 0,
      actions: kind === "dataset" && !gaps.length ? ["go"] : [],
      again,
    };
    const missing =
      kind === "dataset"
        ? detail
          ? null
          : "No dataset with this id in the newest catalog or the registry."
        : "No such " + (kind === "buffer" ? "buffer spec" : "request") + ".";
    root.replaceChildren(
      ...[
        el("h2", {}, key),
        links,
        missing
          ? null
          : flowRow(
              kind,
              lifecycle(S.meta.lines?.[kind], gaps.length ? "blocked" : detail?.row.stage || "new", ["new"]),
            ),
        missing ? null : actionBar(fresh, redraw),
        gaps.length ? gapsNotice(gaps) : null,
        missing ? el("div", { class: "empty" }, missing) : null,
        ...cards,
      ].filter(Boolean),
    );
    return;
  }
  onScreen.subject.number = subject.number;
  subject.again = again;
  const [timeline, run, extras] = await Promise.all([
    load(`/timeline/${kind}/${subject.number}`),
    api(`/run/${kind}/${subject.number}`).catch(() => null),
    kind === "buffer" ? specControls(subject) : null,
  ]);
  if (location.hash !== hash || turn !== subjectTurn) return;
  if (keepTyping(root, redraw)) return;
  const entries = timeline?.entries || [];
  const flow = lifecycle(
    S.meta.lines?.[kind],
    subject.stage,
    [...entries].reverse().map(e => e.stage),
    subject.status === "working",
  );
  const more = subjectLinks(subject, entries),
    row = links || (more.length ? el("div", { class: "links" }) : null);
  if (row) row.append(...more);
  const parts = [el("h2", {}, key), row, flowRow(kind, flow)].filter(Boolean);
  // "working" must be true: a round the registry calls in progress whose workflow is not running is said so
  if (subject.status === "working" && run && run.status !== "RUNNING")
    parts.push(
      el(
        "div",
        { class: "notice" },
        `The registry says a round is in progress, but its workflow is ${(run.status || "not there").toLowerCase()}. Cancel the round to settle it.`,
      ),
    );
  parts.push(
    actionBar(subject, redraw, kind === "buffer" ? ["go", "revoke"] : []),
    undelivered(subject, timeline?.actions || [], redraw),
  );
  if (subject.shelved) parts.push(shelvedNotice(subject.shelved));
  if (gaps.length) parts.push(gapsNotice(gaps));
  if (extras) parts.push(extras);
  parts.push(...cards);
  if (kind === "prep")
    parts.push(
      card(
        "Datasets of this request",
        "Each dataset the request names, with where its preparation stands.",
        pagedTable("request-datasets:" + key, {
          source: pageOf("/catalog/datasets", "rows", () => ({ request: subject.number })),
          sort: "dataset",
          empty: "This request names no dataset: one that selects by rule lists what it admitted in its timeline.",
          cols: [
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
            { key: "state", label: "state", cell: x => statePill(x.state) },
            { key: "stage", label: "stage", cell: x => x.stage || "" },
            { label: "reason", wrap: true, cell: x => x.reason || "" },
            { key: "updated", label: "updated", cell: x => when(x.updated_at) },
          ],
        }),
      ),
    );
  if (kind === "prep" && subject.data?.spec)
    parts.push(card("The request", "As it was filed.", el("pre", { class: "err" }, subject.data.spec)));
  const log = el(
    "div",
    { class: "card" },
    el("h2", {}, "Timeline"),
    el(
      "div",
      { class: "sub" },
      "What was asked and what each round did, newest first. The issue on GitHub is a copy of this.",
    ),
  );
  // entries are read oldest first, so a judge's findings get the pictures of the crops table before them, and shown
  // newest first; of the folds (crops tables, judges' findings) only the newest starts open
  const crops = {};
  const shown = entries.map(e =>
    el(
      "div",
      { class: "entry" },
      el(
        "div",
        { class: "at" },
        when(e.at),
        e.round ? ` · ${e.round}` : "",
        e.mirrored_at ? "" : " · not on GitHub yet",
      ),
      el("div", { class: "md" }, ...markdown(e.body, cropsOf(e.body, crops))),
    ),
  );
  log.append(...shown.reverse());
  log.querySelectorAll("details").forEach((fold, n) => {
    fold.open = n === 0;
  });
  if (!entries.length) log.append(el("div", { class: "empty" }, "Nothing yet."));
  parts.push(log);
  root.replaceChildren(...parts);
}
export async function viewFind(text) {
  // the lookup box: an issue number, an issue URL, a record id or a spec name
  const root = $("subjectBody"),
    hash = location.hash;
  root.replaceChildren(el("div", { class: "empty" }, "looking…"));
  const found = (await load(`/lookup?q=${encodeURIComponent(text)}`))?.subjects || [];
  if (location.hash !== hash) return;
  if (found.length === 1) {
    location.replace(SUBJECT_HASH[found[0].kind](found[0].key));
    return;
  }
  const list = el("ul");
  for (const s of found)
    list.append(el("li", {}, `${s.kind} `, el("a", { href: SUBJECT_HASH[s.kind](s.key) }, s.key), ` · ${s.stage}`));
  // issue numbers repeat across the two repos, so a bare number can be a dataset and a buffer spec
  root.replaceChildren(
    el("h2", {}, `“${text}”`),
    found.length
      ? list
      : el(
          "div",
          { class: "empty" },
          "Nothing matches. Try an issue number (#234), a pasted issue URL, a record id or a spec name.",
        ),
  );
}

// ---- Needs you: everything that waits on a person, with the button that answers it (spec webapp-actions §8.2)
const KIND_WORDS = { dataset: "dataset", buffer: "buffer spec", prep: "request" };
const NEEDS_KINDS = [
  ["", "All"],
  ["dataset", "Datasets"],
  ["buffer", "Buffer specs"],
  ["prep", "Requests"],
]; // the "what" a person can narrow the list to
let needsKind = ""; // the kind shown; kept while the list redraws itself, cleared by opening the tab
let needsCounts = {}; // how many wait under each kind, as last heard: a redrawn button starts with its number
let needsShelved = false; // whether the list is the shelved subjects (`#needs/shelved`) and not what waits
let shelvedCount = null; // how many subjects are shelved, as last heard
export function viewNeeds(arg, refresh) {
  const root = $("needsBody");
  if (refresh === "stream" && keepTyping(root, () => viewNeeds("", true))) return;
  delete root.dataset.dirty;
  if (!refresh) {
    needsKind = NEEDS_KINDS.some(([kind]) => kind === arg) ? arg : "";
    needsShelved = arg === "shelved";
  }
  const again = () => viewNeeds("", true);
  const failed = el("div");
  const show = (kind, shelved) => {
    // the address keeps the choice across a reload; it is set without a new route
    needsKind = kind;
    needsShelved = shelved;
    if (TABLE_STATE.has("needs")) TABLE_STATE.get("needs").offset = 0;
    history.replaceState(null, "", "#needs" + (shelved ? "/shelved" : kind ? "/" + kind : ""));
    again();
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
          onclick: () => show(kind, needsShelved),
        },
        word,
      ),
    ),
  );
  // beside the kinds: the subjects a person set aside, which the list leaves out until this is pressed
  const shelf = el(
    "button",
    {
      type: "button",
      class: "btn",
      "aria-pressed": String(needsShelved),
      title: "Show what was shelved, with its reason, and not what waits on you.",
      onclick: () => show(needsKind, !needsShelved),
    },
    "Shelved",
  );
  const countShelved = n => {
    shelf.textContent = n == null ? "Shelved" : `Shelved ${num(n)}`;
  };
  countShelved(shelvedCount);
  kinds.append(shelf);
  const count = counts =>
    kinds.querySelectorAll("button").forEach(b => {
      if (b === shelf) return; // it is no kind: it counts for itself
      // each button says how many wait under it
      const all = Object.values(counts),
        n = b.dataset.kind ? counts[b.dataset.kind] : all.length ? all.reduce((a, c) => a + c, 0) : null;
      b.textContent = n == null ? b.dataset.word : `${b.dataset.word} ${num(n)}`;
    });
  count(needsCounts);
  root.replaceChildren(
    el("h2", {}, "Needs you"),
    el(
      "p",
      { class: "small" },
      "Everything that waits on a person or has stopped, newest first, each with what it will take. Open one for its timeline.",
    ),
    kinds,
    failed,
    pagedTable("needs", {
      source: pageOf("/needs", "needs", () => ({
        ...(needsKind ? { kind: needsKind } : {}),
        ...(needsShelved ? { shelved: 1 } : {}),
      })),
      sort: "updated",
      dir: "desc",
      empty: needsShelved
        ? "Nothing is shelved."
        : needsKind
          ? "Nothing of this kind waits on you."
          : "Nothing waits on you.",
      onPage: page => {
        count((needsCounts = page.out.kinds || {}));
        countShelved((shelvedCount = page.out.shelved ?? null));
        failed.replaceChildren(
          ...(page.out.failed || [])
            .filter(a => a.subject)
            .map(a =>
              el(
                "div",
                {},
                el("a", { href: SUBJECT_HASH[a.subject.kind](a.subject.key) }, a.subject.key),
                undelivered(a.subject, [a], again),
              ),
            ),
        );
      },
      cols: [
        { key: "kind", label: "what", cell: x => KIND_WORDS[x.kind] },
        { key: "key", label: "name", cell: x => el("a", { href: SUBJECT_HASH[x.kind](x.key) }, x.key) },
        { key: "status", label: "stage", cell: x => stagePill(x.stage) },
        { key: "updated", label: "since", cell: x => when(x.updated) },
        {
          label: "answer",
          wrap: true,
          cell: x =>
            el(
              "div",
              {},
              actionBar(x, again, x.kind === "buffer" ? ["go", "revoke"] : []),
              x.shelved ? el("div", { class: "small" }, x.shelved.reason) : "",
              ...(x.drops || []).map(record =>
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
              ),
            ),
        },
      ],
    }),
  );
}
