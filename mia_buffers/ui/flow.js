// A buffer's page: its name and state, the strip of its six steps, the step being shown, and its timeline.
import { STEP_TITLES, stepToShow, stripOf } from "./steps.js";
import { $, bufferLink, card, el, ghIcon, num, pill, stagePill, statePill, SUBJECT_HASH, when } from "./dom.js";
import { api, banner, load, newKey, S } from "./api.js";
import { pagedTable, pageOf } from "./table.js";
import { gist, mdInline } from "./markdown.js";
import { actionBar, BUTTON_HELP, onScreen, sendAction, undelivered } from "./actions.js";
import { servableText, stageOf } from "./buffers.js";
import { viewDesigner } from "./designer.js";
import { shelvedNotice, timelineCard } from "./subjects.js";

let turn = 0; // the newest load of the page: one that started earlier never paints over it

// The address of a step: of a saved buffer under its key, of one that is not saved yet under New buffer.
const stepHash = (subject, step) => (subject ? `${SUBJECT_HASH.buffer(subject.key)}/${step}` : `#buffers/new/${step}`);

// ---- the heading: the name, where it stands, its links, and what can be asked of it that is not a step
function head(subject, redraw, skip) {
  const name = subject ? subject.name || subject.key : "New buffer";
  $("bufferHead").replaceChildren(
    el(
      "div",
      { class: "viewhead" },
      el("h2", {}, name, " ", subject ? stagePill(subject.stage) : pill("not saved", "")),
      subject
        ? el("details", { class: "more" }, el("summary", { class: "btn" }, "More"), actionBar(subject, redraw, skip))
        : null,
    ),
    subject
      ? el(
          "div",
          { class: "links" },
          // a name is a label and may repeat: the key is what tells two buffers apart
          subject.name && subject.name !== subject.key ? el("span", { class: "mono" }, subject.key) : null,
          subject.issue ? ghIcon(S.meta.buffers_repo, subject.issue, true) || `#${subject.issue}` : null,
        )
      : null,
  );
}

// ---- the strip: the step shown, the steps that can be opened, the steps not reached
function strip(subject, step) {
  $("bufferSteps").replaceChildren(
    ...stripOf(subject?.stage, step).map(item =>
      el(
        "li",
        { class: item.state, "aria-current": item.state === "here" ? "step" : null },
        item.state === "done"
          ? el("a", { href: stepHash(subject, item.step) }, STEP_TITLES[item.step])
          : STEP_TITLES[item.step],
      ),
    ),
  );
}

// ---- what a person should know before the step: the newest entry, a round that is not running, a hold
function notes(subject, entries, run, actions, redraw) {
  const parts = [];
  const said = gist(entries.at(-1)?.body);
  if (said.text) parts.push(el("div", { class: "latest" }, when(entries.at(-1).at), " · ", ...mdInline(said.text)));
  // "working" must be true: a round the registry calls in progress whose workflow is not running is said so
  if (subject.status === "working" && run && run.status !== "RUNNING")
    parts.push(
      el(
        "div",
        { class: "notice" },
        `The registry says a round is in progress, but its workflow is ${(run.status || "not there").toLowerCase()}. Cancel the round (under More) to settle it.`,
      ),
    );
  parts.push(undelivered(subject, actions, redraw));
  if (subject.shelved) parts.push(shelvedNotice(subject.shelved));
  $("bufferNotes").replaceChildren(...parts);
}

// ---- the newest version of a buffer (its newest round's buffer) and that version's datasets
async function newestVersion(subject) {
  const version = ((await load(`/buffers?limit=1&spec=${subject.number}`))?.buffers || [])[0] || null;
  const board = version
    ? (await load(`/buffers/${encodeURIComponent(version.id)}/datasets`, true))?.datasets || []
    : [];
  return { version, board };
}
// a dataset whose files a person copies in by hand: the round waits until it is told they are in place
const droppedNotices = (subject, board, redraw) =>
  !subject.actions.includes("dropped")
    ? []
    : board
        .filter(d => d.status === "waiting-drop")
        .map(d =>
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
                  sendAction(subject, { action: "dropped", record: d.record, key: newKey() }, redraw);
                },
              },
              "Files are in place",
            ),
          ),
        );
function revokeCard(subject, board, redraw) {
  const records = board.filter(d => d.status === "servable").map(d => d.record);
  const ticks = el(
    "div",
    { class: "ticks" },
    ...records.map(r => el("label", { class: "inline" }, el("input", { type: "checkbox", value: r }), " " + r)),
  );
  return card(
    "Revoke datasets",
    "The ticked datasets leave this version: their volumes are unregistered and their staged pyramids quarantined.",
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
                "Revoke the ticked datasets? Their volumes leave this version and their staged pyramids are quarantined; nothing is deleted.",
              )
            )
              return;
            e.target.disabled = true;
            sendAction(subject, { action: "revoke", records: chosen, key: newKey() }, redraw);
          },
        },
        "Revoke the ticked",
      ),
    ),
  );
}
// every version of the buffer, the newest first: each round that reached its plan made one, under its own id
const versionsCard = subject =>
  card(
    "Versions",
    "Newest first. A training run names a version by its id; open one for its events, datasets, volumes, consumers and usage.",
    pagedTable("buffer-versions:" + subject.key, {
      source: pageOf("/buffers", "buffers", () => ({ spec: subject.number })),
      search: false,
      empty: "No version yet.",
      cols: [
        { key: "buffer", label: "id", cell: b => bufferLink(b.id) },
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
  );

// ---- the steps. Each draws its pane; `turnOf()` tells a step that loaded something whether it is still wanted.
const waiting = text => pane => pane.replaceChildren(el("div", { class: "empty" }, text));
const STEP_VIEWS = {
  define: (_pane, subject) => (subject ? null : viewDesigner()),
  datasets: waiting("The datasets this definition admits."),
  prepare: waiting("The datasets that are left to prepare."),
  plan: waiting("The plan of this round."),
  source: async (pane, subject, redraw, wanted) => {
    const { board } = await newestVersion(subject);
    if (wanted()) pane.replaceChildren(...droppedNotices(subject, board, redraw));
  },
  serve: async (pane, subject, redraw, wanted) => {
    const { board } = await newestVersion(subject);
    if (wanted())
      pane.replaceChildren(
        ...(subject.actions.includes("revoke") ? [revokeCard(subject, board, redraw)] : []),
        versionsCard(subject),
      );
  },
};

function draw(subject, step, entries, run, actions, redraw) {
  head(subject, redraw, ["go", "revoke"]);
  strip(subject, step);
  if (subject) notes(subject, entries, run, actions, redraw);
  else $("bufferNotes").replaceChildren();
  for (const pane of document.querySelectorAll('[data-view="buffer"] [data-step]'))
    pane.hidden = pane.dataset.step !== step;
  const drawn = turn; // a step that loads something paints only while this draw is the newest
  STEP_VIEWS[step](
    document.querySelector(`[data-view="buffer"] [data-step="${step}"]`),
    subject,
    redraw,
    () => drawn === turn,
  );
  $("bufferTimeline").replaceChildren(...(subject ? [timelineCard(entries)] : []));
}

// `key`: the buffer's, or null for one that is not saved yet. `asked`: the step the address names, if any.
export async function viewFlow(key, asked, refresh) {
  const hash = location.hash,
    mine = ++turn,
    redraw = () => viewFlow(key, asked, true);
  onScreen.redraw = redraw;
  if (!key) {
    onScreen.subject = null;
    return draw(null, stepToShow(null, asked), [], null, [], redraw);
  }
  // what the stream refetches: this buffer's changes
  const known = onScreen.subject?.kind === "buffer" && onScreen.subject.key === key ? onScreen.subject.number : null;
  onScreen.subject = { kind: "buffer", key, number: known };
  if (!refresh) $("bufferNotes").replaceChildren(el("div", { class: "empty" }, "loading…"));
  const subject = await load(`/subjects/buffer/${encodeURIComponent(key)}`, true);
  if (location.hash !== hash || mine !== turn) return; // the reader moved on, or a later load started
  if (!subject) {
    $("bufferHead").replaceChildren(el("h2", {}, key));
    $("bufferSteps").replaceChildren();
    $("bufferNotes").replaceChildren(el("div", { class: "empty" }, "No such buffer."));
    return;
  }
  onScreen.subject.number = subject.number;
  const [timeline, run] = await Promise.all([
    load(`/timeline/buffer/${subject.number}`),
    api(`/run/buffer/${subject.number}`).catch(() => null),
  ]);
  if (location.hash !== hash || mine !== turn) return;
  draw(subject, stepToShow(subject.stage, asked), timeline?.entries || [], run, timeline?.actions || [], redraw);
}
